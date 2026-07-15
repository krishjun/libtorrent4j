#include "flow_saf_disk_io.hpp"

#include "flow_saf_storage.hpp"

#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/aux_/disk_buffer_pool.hpp"
#include "libtorrent/disk_interface.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/hex.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/performance_counters.hpp"
#include "libtorrent/session_handle.hpp"
#include "libtorrent/settings_pack.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/error.hpp>
#include <boost/system/errc.hpp>
#include <boost/system/system_error.hpp>

#include <sys/types.h>
#include <unistd.h>

namespace libtorrent {
namespace {

constexpr int bridge_open_read = 0;
constexpr int bridge_open_read_write = 2;

error_code from_bridge_result(std::int64_t result)
{
	if (result >= 0) return {};
	auto const value = int(std::min<std::int64_t>(-result
		, std::numeric_limits<int>::max()));
	return {value, boost::system::generic_category()};
}

storage_error make_error(std::int64_t result, file_index_t const file
	, operation_t const operation)
{
	return {from_bridge_result(result), file, operation};
}

storage_error aborted_error(operation_t const operation)
{
	return {boost::asio::error::operation_aborted, operation};
}

bool safe_relative_path(std::string const& path)
{
	if (path.empty() || path.front() == '/' || path.front() == '\\') return false;
	if (path.find('\\') != std::string::npos || path.find('\0') != std::string::npos)
		return false;
	std::size_t start = 0;
	while (start < path.size())
	{
		auto const end = path.find('/', start);
		auto const size = (end == std::string::npos ? path.size() : end) - start;
		if (size == 0) return false;
		auto const component = path.substr(start, size);
		if (component == "." || component == "..") return false;
		start = end == std::string::npos ? path.size() : end + 1;
	}
	return path.back() != '/';
}

constexpr char flow_session_root[] = ".flow-session-root";

std::string normalized_save_root(std::string_view const path)
{
	if (path == flow_session_root) return {};
	std::string_view const rooted_sentinel = "/.flow-session-root";
	if (path.size() >= rooted_sentinel.size()
		&& path.substr(path.size() - rooted_sentinel.size()) == rooted_sentinel)
		return {};
	return std::string(path);
}

std::string hash_hex(sha1_hash const& hash)
{
	return aux::to_hex({hash.data(), hash.size()});
}

std::string rooted_path(std::string const& root, std::string const& relative)
{
	return root.empty() ? relative : root + "/" + relative;
}

struct torrent_storage
{
	explicit torrent_storage(storage_params const& params, flow_saf_storage& bridge)
		: files(params.files)
		, priorities(params.priorities)
		, save_root(normalized_save_root(params.path))
		, info_hash(params.info_hash)
		, part_path(".flow-parts/" + hash_hex(params.info_hash) + ".parts")
	{
		if (!save_root.empty())
		{
			if (!safe_relative_path(save_root))
				throw std::invalid_argument("Flow SAF save root must be empty or relative");
			int const result = bridge.validate_path(save_root);
			if (result == -EINVAL)
				throw std::invalid_argument("Flow SAF save root was rejected by storage");
			if (result < 0)
				throw boost::system::system_error(from_bridge_result(result)
					, "Flow SAF save root validation failed");
		}
		priorities.resize(std::size_t(files.num_files()), default_priority);
		logical_paths.reserve(std::size_t(files.num_files()));
		paths.reserve(std::size_t(files.num_files()));
		for (file_index_t index{0}; index < files.end_file(); ++index)
		{
			auto path = params.renamed_files.file_path(files, index);
			logical_paths.push_back(path);
			paths.push_back(rooted_path(save_root, path));
		}
	}

	file_storage files;
	aux::vector<download_priority_t, file_index_t> priorities;
	std::vector<std::string> logical_paths;
	std::vector<std::string> paths;
	std::string save_root;
	sha1_hash info_hash;
	std::string part_path;
};

struct open_descriptor
{
	std::uint32_t storage = 0;
	int file = -1;
	std::string path;
	int descriptor = -1;
	bool writable = false;
	std::uint64_t last_use = 0;
	time_point last_use_time;
};

struct scoped_descriptor
{
	explicit scoped_descriptor(int const value = -1) : descriptor(value) {}
	~scoped_descriptor() { reset(); }

	void reset()
	{
		if (descriptor >= 0) ::close(descriptor);
		descriptor = -1;
	}

	int descriptor = -1;
};

class flow_saf_disk_io final : public disk_interface
{
public:
	flow_saf_disk_io(io_context& ios, settings_interface const& settings
		, counters& counters, flow_saf_storage& bridge)
		: m_settings(settings)
		, m_buffer_pool(ios)
		, m_stats_counters(counters)
		, m_ios(ios)
		, m_bridge(bridge)
	{
		settings_updated();
		m_worker = std::thread([this] { worker_loop(); });
	}

	~flow_saf_disk_io() override { abort(true); }

	storage_holder new_torrent(storage_params const& params
		, std::shared_ptr<void> const&) override
	{
		auto storage = std::make_shared<torrent_storage>(params, m_bridge);
		std::lock_guard<std::mutex> lock(m_storage_mutex);
		std::uint32_t index;
		if (m_free_storage.empty())
		{
			index = static_cast<std::uint32_t>(m_torrents.size());
			m_torrents.push_back(std::move(storage));
		}
		else
		{
			index = m_free_storage.back();
			m_free_storage.pop_back();
			m_torrents[index] = std::move(storage);
		}
		return {storage_index_t{index}, *this};
	}

	void remove_torrent(storage_index_t const storage) override
	{
		auto const index = static_cast<std::uint32_t>(storage);
		std::lock_guard<std::mutex> lock(m_storage_mutex);
		if (index >= m_torrents.size() || !m_torrents[index]) return;
		m_torrents[index].reset();
		m_free_storage.push_back(index);
		enqueue([this, index] { close_storage(index); });
	}

	void async_read(storage_index_t const storage, peer_request const& request
		, std::function<void(disk_buffer_holder, storage_error const&)> handler
		, disk_job_flags_t) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post_read_error(std::move(handler), aborted_error(operation_t::file_read));
			return;
		}
		char* raw = m_buffer_pool.allocate_buffer("flow saf read");
		if (raw == nullptr)
		{
			storage_error error(errors::no_memory, operation_t::alloc_cache_piece);
			post_read_error(std::move(handler), error);
			return;
		}
		auto buffer = std::make_shared<disk_buffer_holder>(
			m_buffer_pool, raw, default_block_size);
		auto const storage_number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort(
			[this, torrent = std::move(torrent), storage_number, request
				, buffer, handler]() mutable {
				time_point const start_time = clock_type::now();
				storage_error error;
				int const transferred = read_block(*torrent, storage_number, request
					, {buffer->data(), request.length}, error);
				if (!error && transferred != request.length)
					error = storage_error(boost::asio::error::eof, operation_t::file_read);
				if (!error)
				{
					auto const elapsed = total_microseconds(clock_type::now() - start_time);
					m_stats_counters.inc_stats_counter(counters::num_blocks_read);
					m_stats_counters.inc_stats_counter(counters::num_read_ops);
					m_stats_counters.inc_stats_counter(counters::disk_read_time, elapsed);
					m_stats_counters.inc_stats_counter(counters::disk_job_time, elapsed);
				}
				post(m_ios, [buffer, error, handler = std::move(handler)]() mutable {
					handler(std::move(*buffer), error);
				});
			}
			, [this, buffer, handler]() mutable {
				post(m_ios, [buffer, handler = std::move(handler)]() mutable {
					handler(std::move(*buffer), aborted_error(operation_t::file_read));
				});
			});
	}

	bool async_write(storage_index_t const storage, peer_request const& request
		, char const* bytes, std::shared_ptr<disk_observer>
		, std::function<void(storage_error const&)> handler
		, disk_job_flags_t) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post(m_ios, [handler = std::move(handler)] {
				handler(aborted_error(operation_t::file_write));
			});
			return false;
		}
		std::vector<char> copy(bytes, bytes + request.length);
		auto const storage_number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort(
			[this, torrent = std::move(torrent), storage_number, request
				, copy = std::move(copy), handler]() mutable {
				time_point const start_time = clock_type::now();
				storage_error error;
				write_block(*torrent, storage_number, request, copy, error);
				if (!error)
				{
					auto const elapsed = total_microseconds(clock_type::now() - start_time);
					m_stats_counters.inc_stats_counter(counters::num_blocks_written);
					m_stats_counters.inc_stats_counter(counters::num_write_ops);
					m_stats_counters.inc_stats_counter(counters::disk_write_time, elapsed);
					m_stats_counters.inc_stats_counter(counters::disk_job_time, elapsed);
				}
				post(m_ios, [error, handler = std::move(handler)] { handler(error); });
			}
			, [this, handler]() mutable {
				post(m_ios, [handler = std::move(handler)] {
					handler(aborted_error(operation_t::file_write));
				});
			});
		return false;
	}

	void async_hash(storage_index_t const storage, piece_index_t const piece
		, span<sha256_hash> block_hashes, disk_job_flags_t const flags
		, std::function<void(piece_index_t, sha1_hash const&, storage_error const&)> handler) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post(m_ios, [piece, handler = std::move(handler)] {
				handler(piece, {}, aborted_error(operation_t::file_read));
			});
			return;
		}
		auto const storage_number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort([this, torrent = std::move(torrent), storage_number, piece
			, block_hashes, flags, handler]() mutable {
			time_point const start_time = clock_type::now();
			storage_error error;
			bool const need_v1 = bool(flags & disk_interface::v1_hash);
			bool const need_v2 = !block_hashes.empty();
			int const piece_size = need_v1 ? torrent->files.piece_size(piece) : 0;
			int const piece_size2 = need_v2 ? torrent->files.piece_size2(piece) : 0;
			int const bytes_to_read = std::max(piece_size, piece_size2);
			std::vector<char> bytes(static_cast<std::size_t>(bytes_to_read));
			if (bytes_to_read > 0)
			{
				peer_request request{piece, 0, bytes_to_read};
				read_block(*torrent, storage_number, request, bytes, error);
			}
			hasher v1;
			if (!error && need_v1) v1.update(span<char const>{bytes}.first(piece_size));
			if (!error && need_v2)
			{
				int const blocks = torrent->files.blocks_in_piece2(piece);
				if (int(block_hashes.size()) < blocks)
					error = storage_error(make_error_code(boost::system::errc::invalid_argument)
						, operation_t::file_read);
				for (int block = 0; !error && block < blocks; ++block)
				{
					int const offset = block * default_block_size;
					int const length = std::min(default_block_size, piece_size2 - offset);
					block_hashes[block] = hasher256(span<char const>{bytes}.subspan(offset, length)).final();
				}
			}
			sha1_hash const result = need_v1 && !error ? v1.final() : sha1_hash{};
			if (!error)
			{
				int const blocks = (bytes_to_read + default_block_size - 1) / default_block_size;
				auto const elapsed = total_microseconds(clock_type::now() - start_time);
				m_stats_counters.inc_stats_counter(counters::num_read_back, blocks);
				m_stats_counters.inc_stats_counter(counters::num_blocks_read, blocks);
				m_stats_counters.inc_stats_counter(counters::num_read_ops, blocks);
				m_stats_counters.inc_stats_counter(counters::disk_hash_time, elapsed);
				m_stats_counters.inc_stats_counter(counters::disk_job_time, elapsed);
			}
			post(m_ios, [piece, result, error, handler = std::move(handler)] {
				handler(piece, result, error);
			});
		}, [this, piece, handler]() mutable {
			post(m_ios, [piece, handler = std::move(handler)] {
				handler(piece, {}, aborted_error(operation_t::file_read));
			});
		});
	}

	void async_hash2(storage_index_t const storage, piece_index_t const piece
		, int const offset, disk_job_flags_t
		, std::function<void(piece_index_t, sha256_hash const&, storage_error const&)> handler) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post(m_ios, [piece, handler = std::move(handler)] {
				handler(piece, {}, aborted_error(operation_t::file_read));
			});
			return;
		}
		auto const storage_number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort([this, torrent = std::move(torrent), storage_number, piece, offset
			, handler]() mutable {
			time_point const start_time = clock_type::now();
			storage_error error;
			int const length = std::min(default_block_size
				, torrent->files.piece_size2(piece) - offset);
			std::vector<char> bytes(static_cast<std::size_t>(std::max(0, length)));
			if (length <= 0)
				error = storage_error(make_error_code(boost::system::errc::invalid_argument)
					, operation_t::file_read);
			else
				read_block(*torrent, storage_number, {piece, offset, length}, bytes, error);
			sha256_hash const result = error ? sha256_hash{} : hasher256(bytes).final();
			if (!error)
			{
				auto const elapsed = total_microseconds(clock_type::now() - start_time);
				m_stats_counters.inc_stats_counter(counters::num_read_back);
				m_stats_counters.inc_stats_counter(counters::num_blocks_read);
				m_stats_counters.inc_stats_counter(counters::num_read_ops);
				m_stats_counters.inc_stats_counter(counters::disk_hash_time, elapsed);
				m_stats_counters.inc_stats_counter(counters::disk_job_time, elapsed);
			}
			post(m_ios, [piece, result, error, handler = std::move(handler)] {
				handler(piece, result, error);
			});
		}, [this, piece, handler]() mutable {
			post(m_ios, [piece, handler = std::move(handler)] {
				handler(piece, {}, aborted_error(operation_t::file_read));
			});
		});
	}

	void async_move_storage(storage_index_t const storage, std::string path
		, move_flags_t const flags
		, std::function<void(status_t, std::string const&, storage_error const&)> handler) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post(m_ios, [path = std::move(path), handler = std::move(handler)] {
				handler(disk_status::fatal_disk_error, path
					, aborted_error(operation_t::file_rename));
			});
			return;
		}
		auto const number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort([this, torrent = std::move(torrent), number, path, flags
			, handler]() mutable {
			storage_error error;
			status_t status;
			if (!safe_relative_path(path))
			{
				status |= disk_status::fatal_disk_error;
				error = storage_error(make_error_code(boost::system::errc::invalid_argument)
					, operation_t::file_rename);
			}
			else move_storage(*torrent, number, path, flags, status, error);
			post(m_ios, [status, path = std::move(path), error
				, handler = std::move(handler)] { handler(status, path, error); });
		}, [this, path, handler]() mutable {
			post(m_ios, [path = std::move(path), handler = std::move(handler)] {
				handler(disk_status::fatal_disk_error, path
					, aborted_error(operation_t::file_rename));
			});
		});
	}

	void async_release_files(storage_index_t const storage
		, std::function<void()> handler = {}) override
	{
		auto const number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort([this, number, handler]() mutable {
			close_storage(number);
			if (handler) post(m_ios, std::move(handler));
		}, [this, handler]() mutable {
			if (handler) post(m_ios, std::move(handler));
		});
	}

	void async_check_files(storage_index_t const storage
		, add_torrent_params const* const resume_data
		, aux::vector<std::string, file_index_t> links
		, std::function<void(status_t, storage_error const&)> handler) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post(m_ios, [handler = std::move(handler)] {
				handler(disk_status::fatal_disk_error, aborted_error(operation_t::check_resume));
			});
			return;
		}
		std::shared_ptr<add_torrent_params> resume;
		if (resume_data != nullptr) resume = std::make_shared<add_torrent_params>(*resume_data);
		auto const number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort([this, torrent = std::move(torrent), number, resume = std::move(resume)
			, links = std::move(links), handler]() mutable {
			storage_error error;
			status_t status;
			if (!links.empty())
			{
				status |= disk_status::fatal_disk_error;
				error = storage_error(make_error_code(boost::system::errc::operation_not_supported)
					, operation_t::file_hard_link);
			}
			if (!error)
			{
				bool const validate = resume && (bool(resume->flags & torrent_flags::seed_mode)
					|| resume->have_pieces.size() > 0);
				if (validate) validate_resume(*torrent, *resume, status, error);
				else initialize_files(*torrent, number, status, error);
			}
			post(m_ios, [status, error, handler = std::move(handler)] {
				handler(status, error);
			});
		}, [this, handler]() mutable {
			post(m_ios, [handler = std::move(handler)] {
				handler(disk_status::fatal_disk_error, aborted_error(operation_t::check_resume));
			});
		});
	}

	void async_stop_torrent(storage_index_t const storage
		, std::function<void()> handler = {}) override
	{
		async_release_files(storage, std::move(handler));
	}

	void async_rename_file(storage_index_t const storage, file_index_t const index
		, std::string name
		, std::function<void(std::string const&, file_index_t, storage_error const&)> handler) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post(m_ios, [name = std::move(name), index, handler = std::move(handler)] {
				handler(name, index, aborted_error(operation_t::file_rename));
			});
			return;
		}
		auto const number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort([this, torrent = std::move(torrent), number, index
			, name, handler]() mutable {
			storage_error error;
			if (index < file_index_t{0} || index >= torrent->files.end_file()
				|| !safe_relative_path(name))
				error = storage_error(make_error_code(boost::system::errc::invalid_argument)
					, index, operation_t::file_rename);
			else
			{
				close_file(number, static_cast<int>(index));
				auto const offset = std::size_t(static_cast<int>(index));
				auto& old_name = torrent->paths[offset];
				auto const destination = rooted_path(torrent->save_root, name);
				if (ensure_parent_directories(destination, error, index))
				{
					int const result = m_bridge.rename(old_name, destination);
					if (result < 0) error = make_error(result, index, operation_t::file_rename);
					else
					{
						torrent->logical_paths[offset] = name;
						old_name = destination;
					}
				}
			}
			post(m_ios, [name = std::move(name), index, error
				, handler = std::move(handler)] { handler(name, index, error); });
		}, [this, name, index, handler]() mutable {
			post(m_ios, [name = std::move(name), index, handler = std::move(handler)] {
				handler(name, index, aborted_error(operation_t::file_rename));
			});
		});
	}

	void async_delete_files(storage_index_t const storage, remove_flags_t const options
		, std::function<void(storage_error const&)> handler) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post(m_ios, [handler = std::move(handler)] {
				handler(aborted_error(operation_t::file_remove));
			});
			return;
		}
		auto const number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort([this, torrent = std::move(torrent), number, options
			, handler]() mutable {
			close_storage(number);
			storage_error error;
			if (bool(options & session_handle::delete_files))
			{
				for (file_index_t index{0}; index < torrent->files.end_file(); ++index)
				{
					if (torrent->files.pad_file_at(index)) continue;
					int const result = m_bridge.remove(path_for(*torrent, index), false);
					if (result < 0 && result != -ENOENT && !error)
						error = make_error(result, index, operation_t::file_remove);
				}
			}
			if (bool(options & session_handle::delete_partfile)
				|| bool(options & session_handle::delete_files))
			{
				int const result = m_bridge.remove(torrent->part_path, false);
				if (result < 0 && result != -ENOENT && !error)
					error = storage_error(from_bridge_result(result), operation_t::file_remove);
			}
			post(m_ios, [error, handler = std::move(handler)] { handler(error); });
		}, [this, handler]() mutable {
			post(m_ios, [handler = std::move(handler)] {
				handler(aborted_error(operation_t::file_remove));
			});
		});
	}

	void async_set_file_priority(storage_index_t const storage
		, aux::vector<download_priority_t, file_index_t> priorities
		, std::function<void(storage_error const&
			, aux::vector<download_priority_t, file_index_t>)> handler) override
	{
		auto torrent = find_storage(storage);
		if (!torrent)
		{
			post(m_ios, [priorities = std::move(priorities), handler = std::move(handler)]() mutable {
				handler(aborted_error(operation_t::file_write), std::move(priorities));
			});
			return;
		}
		priorities.resize(std::size_t(torrent->files.num_files()), default_priority);
		auto const number = static_cast<std::uint32_t>(storage);
		enqueue_or_abort([this, torrent = std::move(torrent), number
			, priorities, handler]() mutable {
			storage_error error;
			for (file_index_t index{0}; index < torrent->files.end_file(); ++index)
			{
				bool const was_skipped = torrent->priorities[index] == dont_download;
				bool const now_skipped = priorities[index] == dont_download;
				if (!torrent->files.pad_file_at(index) && was_skipped != now_skipped)
					migrate_priority(*torrent, number, index, now_skipped, error);
				if (error) break;
				torrent->priorities[index] = priorities[index];
			}
			auto current = torrent->priorities;
			if (!error && std::none_of(current.begin(), current.end()
				, [](download_priority_t value) { return value == dont_download; }))
			{
				close_file(number, -1);
				int const result = m_bridge.remove(torrent->part_path, false);
				if (result < 0 && result != -ENOENT)
					error = storage_error(from_bridge_result(result), operation_t::file_remove);
			}
			post(m_ios, [error, current = std::move(current)
				, handler = std::move(handler)]() mutable {
				handler(error, std::move(current));
			});
		}, [this, priorities, handler]() mutable {
			post(m_ios, [priorities = std::move(priorities), handler = std::move(handler)]() mutable {
				handler(aborted_error(operation_t::file_write), std::move(priorities));
			});
		});
	}

	void async_clear_piece(storage_index_t, piece_index_t const piece
		, std::function<void(piece_index_t)> handler) override
	{
		enqueue_or_abort([this, piece, handler]() mutable {
			post(m_ios, [piece, handler = std::move(handler)] { handler(piece); });
		}, [this, piece, handler]() mutable {
			post(m_ios, [piece, handler = std::move(handler)] { handler(piece); });
		});
	}

	void update_stats_counters(counters&) const override {}

	std::vector<open_file_state> get_status(storage_index_t const storage) const override
	{
		std::vector<open_file_state> result;
		auto const number = static_cast<std::uint32_t>(storage);
		std::lock_guard<std::mutex> lock(m_descriptor_mutex);
		for (auto const& descriptor : m_descriptors)
		{
			if (descriptor.storage != number || descriptor.file < 0) continue;
			result.push_back({file_index_t{descriptor.file}
				, descriptor.writable ? file_open_mode::read_write : file_open_mode::read_only
				, descriptor.last_use_time});
		}
		return result;
	}

	void abort(bool) override
	{
		{
			std::lock_guard<std::mutex> lock(m_queue_mutex);
			if (!m_accepting && !m_worker.joinable()) return;
			m_accepting = false;
			m_stopping = true;
		}
		m_queue_condition.notify_all();
		if (m_worker.joinable()) m_worker.join();
		close_all();
	}

	void submit_jobs() override { m_queue_condition.notify_one(); }

	void settings_updated() override
	{
		m_buffer_pool.set_settings(m_settings);
		m_descriptor_limit.store(std::max(1
			, m_settings.get_int(settings_pack::file_pool_size)));
	}

private:
	using job = std::function<void()>;

	std::shared_ptr<torrent_storage> find_storage(storage_index_t const storage) const
	{
		auto const index = static_cast<std::uint32_t>(storage);
		std::lock_guard<std::mutex> lock(m_storage_mutex);
		return index < m_torrents.size() ? m_torrents[index] : nullptr;
	}

	void enqueue(job operation)
	{
		std::lock_guard<std::mutex> lock(m_queue_mutex);
		if (m_accepting) m_jobs.push_back(std::move(operation));
	}

	template <typename Operation, typename Aborted>
	void enqueue_or_abort(Operation operation, Aborted aborted)
	{
		bool accepted;
		{
			std::lock_guard<std::mutex> lock(m_queue_mutex);
			accepted = m_accepting;
			if (accepted) m_jobs.emplace_back(std::move(operation));
		}
		if (!accepted) aborted();
	}

	void worker_loop()
	{
		for (;;)
		{
			job operation;
			{
				std::unique_lock<std::mutex> lock(m_queue_mutex);
				m_queue_condition.wait(lock, [this] { return m_stopping || !m_jobs.empty(); });
				if (m_jobs.empty())
				{
					if (m_stopping) break;
					continue;
				}
				operation = std::move(m_jobs.front());
				m_jobs.pop_front();
			}
			operation();
		}
		close_all();
	}

	void post_read_error(
		std::function<void(disk_buffer_holder, storage_error const&)> handler
		, storage_error const& error)
	{
		post(m_ios, [this, error, handler = std::move(handler)]() mutable {
			handler(disk_buffer_holder(m_buffer_pool, nullptr, 0), error);
		});
	}

	std::string const& path_for(torrent_storage const& torrent, file_index_t const index) const
	{
		return torrent.paths[std::size_t(static_cast<int>(index))];
	}

	bool skipped(torrent_storage const& torrent, file_index_t const index) const
	{
		return index < torrent.priorities.end_index()
			&& torrent.priorities[index] == dont_download;
	}

	bool ensure_parent_directories(std::string const& path, storage_error& error
		, file_index_t const file)
	{
		if (!safe_relative_path(path))
		{
			error = storage_error(make_error_code(boost::system::errc::invalid_argument)
				, file, operation_t::mkdir);
			return false;
		}
		std::size_t separator = path.find('/');
		while (separator != std::string::npos)
		{
			std::string const directory = path.substr(0, separator);
			int const result = m_bridge.create_directory(directory);
			if (result < 0 && result != -EEXIST)
			{
				error = make_error(result, file, operation_t::mkdir);
				return false;
			}
			separator = path.find('/', separator + 1);
		}
		return true;
	}

	int open_file(std::uint32_t const storage, int const file, std::string const& path
		, bool const write, storage_error& error, operation_t const operation)
	{
		std::lock_guard<std::mutex> lock(m_descriptor_mutex);
		for (auto& descriptor : m_descriptors)
		{
			if (descriptor.storage != storage || descriptor.file != file
				|| descriptor.path != path) continue;
			if (!write || descriptor.writable)
			{
				descriptor.last_use = ++m_use_counter;
				descriptor.last_use_time = clock_type::now();
				return descriptor.descriptor;
			}
			::close(descriptor.descriptor);
			m_descriptors.erase(std::find_if(m_descriptors.begin(), m_descriptors.end()
				, [&](open_descriptor const& candidate) { return &candidate == &descriptor; }));
			break;
		}

		if (write && !ensure_parent_directories(path, error, file_index_t{file})) return -1;
		if (!safe_relative_path(path))
		{
			error = storage_error(make_error_code(boost::system::errc::invalid_argument)
				, file_index_t{file}, operation);
			return -1;
		}
		while (int(m_descriptors.size()) >= m_descriptor_limit.load()) evict_one();
		int const result = m_bridge.open_file(path
			, write ? bridge_open_read_write : bridge_open_read);
		if (result < 0)
		{
			error = make_error(result, file_index_t{file}, operation);
			return -1;
		}
		m_descriptors.push_back({storage, file, path, result, write, ++m_use_counter
			, clock_type::now()});
		return result;
	}

	void evict_one()
	{
		if (m_descriptors.empty()) return;
		auto const oldest = std::min_element(m_descriptors.begin(), m_descriptors.end()
			, [](open_descriptor const& lhs, open_descriptor const& rhs) {
				return lhs.last_use < rhs.last_use;
			});
		::close(oldest->descriptor);
		m_descriptors.erase(oldest);
	}

	void close_storage(std::uint32_t const storage)
	{
		std::lock_guard<std::mutex> lock(m_descriptor_mutex);
		for (auto iterator = m_descriptors.begin(); iterator != m_descriptors.end();)
		{
			if (iterator->storage == storage)
			{
				::close(iterator->descriptor);
				iterator = m_descriptors.erase(iterator);
			}
			else ++iterator;
		}
	}

	void close_file(std::uint32_t const storage, int const file)
	{
		std::lock_guard<std::mutex> lock(m_descriptor_mutex);
		for (auto iterator = m_descriptors.begin(); iterator != m_descriptors.end();)
		{
			if (iterator->storage == storage && iterator->file == file)
			{
				::close(iterator->descriptor);
				iterator = m_descriptors.erase(iterator);
			}
			else ++iterator;
		}
	}

	void close_all()
	{
		std::lock_guard<std::mutex> lock(m_descriptor_mutex);
		for (auto const& descriptor : m_descriptors) ::close(descriptor.descriptor);
		m_descriptors.clear();
	}

	int read_block(torrent_storage& torrent, std::uint32_t const storage
		, peer_request const& request, span<char> output, storage_error& error)
	{
		auto const slices = torrent.files.map_block(request.piece, request.start, request.length);
		std::int64_t output_offset = 0;
		for (auto const& slice : slices)
		{
			if (torrent.files.pad_file_at(slice.file_index))
			{
				std::memset(output.data() + output_offset, 0, std::size_t(slice.size));
				output_offset += slice.size;
				continue;
			}
			bool const part = skipped(torrent, slice.file_index);
			std::string const& path = part ? torrent.part_path : path_for(torrent, slice.file_index);
			int const file_key = part ? -1 : static_cast<int>(slice.file_index);
			int const descriptor = open_file(storage, file_key, path, false, error
				, part ? operation_t::partfile_read : operation_t::file_read);
			if (descriptor < 0) return int(output_offset);
			std::int64_t const file_offset = part
				? torrent.files.file_offset(slice.file_index) + slice.offset : slice.offset;
			std::int64_t remaining = slice.size;
			while (remaining > 0)
			{
				auto const result = ::pread(descriptor, output.data() + output_offset
					, std::size_t(remaining), file_offset + (slice.size - remaining));
				if (result < 0 && errno == EINTR) continue;
				if (result < 0)
				{
					error = make_error(-errno, slice.file_index
						, part ? operation_t::partfile_read : operation_t::file_read);
					return int(output_offset);
				}
				if (result == 0)
				{
					error = storage_error(boost::asio::error::eof, slice.file_index
						, part ? operation_t::partfile_read : operation_t::file_read);
					return int(output_offset);
				}
				remaining -= result;
				output_offset += result;
			}
		}
		return int(output_offset);
	}

	int write_block(torrent_storage& torrent, std::uint32_t const storage
		, peer_request const& request, span<char const> input, storage_error& error)
	{
		auto const slices = torrent.files.map_block(request.piece, request.start, request.length);
		std::int64_t input_offset = 0;
		for (auto const& slice : slices)
		{
			if (torrent.files.pad_file_at(slice.file_index))
			{
				input_offset += slice.size;
				continue;
			}
			bool const part = skipped(torrent, slice.file_index);
			std::string const& path = part ? torrent.part_path : path_for(torrent, slice.file_index);
			int const file_key = part ? -1 : static_cast<int>(slice.file_index);
			int const descriptor = open_file(storage, file_key, path, true, error
				, part ? operation_t::partfile_write : operation_t::file_write);
			if (descriptor < 0) return int(input_offset);
			std::int64_t const file_offset = part
				? torrent.files.file_offset(slice.file_index) + slice.offset : slice.offset;
			std::int64_t remaining = slice.size;
			while (remaining > 0)
			{
				auto const result = ::pwrite(descriptor, input.data() + input_offset
					, std::size_t(remaining), file_offset + (slice.size - remaining));
				if (result < 0 && errno == EINTR) continue;
				if (result <= 0)
				{
					error = make_error(result < 0 ? -errno : -EIO, slice.file_index
						, part ? operation_t::partfile_write : operation_t::file_write);
					return int(input_offset);
				}
				remaining -= result;
				input_offset += result;
			}
		}
		return int(input_offset);
	}

	void set_save_root(torrent_storage& torrent, std::string root)
	{
		torrent.save_root = normalized_save_root(root);
		for (std::size_t index = 0; index < torrent.paths.size(); ++index)
			torrent.paths[index] = rooted_path(torrent.save_root, torrent.logical_paths[index]);
	}

	bool copy_public_file(std::string const& source_path, std::string const& destination_path
		, std::int64_t const size, file_index_t const index, storage_error& error)
	{
		if (!safe_relative_path(source_path) || !safe_relative_path(destination_path))
		{
			error = storage_error(make_error_code(boost::system::errc::invalid_argument)
				, index, operation_t::file_rename);
			return false;
		}
		if (!ensure_parent_directories(destination_path, error, index)) return false;
		int const source_result = m_bridge.open_file(source_path, bridge_open_read);
		scoped_descriptor source(source_result);
		if (source_result < 0)
		{
			error = make_error(source_result, index, operation_t::file_read);
			return false;
		}
		int const destination_result = m_bridge.open_file(destination_path
			, bridge_open_read_write);
		scoped_descriptor destination(destination_result);
		if (destination_result < 0)
		{
			error = make_error(destination_result, index, operation_t::file_write);
			return false;
		}
		if (::ftruncate(destination.descriptor, size) != 0)
		{
			error = make_error(-errno, index, operation_t::file_truncate);
			return false;
		}

		std::vector<char> buffer(default_block_size);
		std::int64_t copied = 0;
		while (copied < size)
		{
			std::size_t const length = std::size_t(std::min<std::int64_t>(buffer.size()
				, size - copied));
			auto result = flow_saf_detail::pread_all(source.descriptor, buffer.data()
				, length, copied, [](int const fd, char* const data, std::size_t const bytes
					, std::int64_t const offset) {
					return ::pread(fd, data, bytes, offset);
				});
			if (result < 0)
			{
				error = make_error(result, index, operation_t::file_read);
				return false;
			}
			result = flow_saf_detail::pwrite_all(destination.descriptor, buffer.data()
				, length, copied, [](int const fd, char const* const data
					, std::size_t const bytes, std::int64_t const offset) {
					return ::pwrite(fd, data, bytes, offset);
				});
			if (result < 0)
			{
				error = make_error(result, index, operation_t::file_write);
				return false;
			}
			copied += std::int64_t(length);
		}
		return true;
	}

	void move_storage(torrent_storage& torrent, std::uint32_t const storage
		, std::string const& destination_root, move_flags_t const flags
		, status_t& status, storage_error& error)
	{
		std::string const normalized_destination_root = normalized_save_root(destination_root);
		close_storage(storage);
		std::vector<std::string> destinations;
		destinations.reserve(torrent.logical_paths.size());
		for (std::size_t offset = 0; offset < torrent.logical_paths.size(); ++offset)
		{
			if (!safe_relative_path(torrent.logical_paths[offset])
				|| !safe_relative_path(torrent.paths[offset]))
			{
				status |= disk_status::fatal_disk_error;
				error = storage_error(make_error_code(boost::system::errc::invalid_argument)
					, file_index_t{int(offset)}, operation_t::file_rename);
				return;
			}
			destinations.push_back(rooted_path(normalized_destination_root
				, torrent.logical_paths[offset]));
		}

		struct move_operation
		{
			file_index_t index;
			std::string source;
			std::string destination;
			std::int64_t source_size = -ENOENT;
			std::int64_t destination_size = -ENOENT;
			bool copy = false;
			bool cleanup = false;
		};
		std::vector<move_operation> operations;
		for (file_index_t index{0}; index < torrent.files.end_file(); ++index)
		{
			if (torrent.files.pad_file_at(index)) continue;
			auto const offset = std::size_t(int(index));
			if (torrent.paths[offset] == destinations[offset]) continue;
			operations.push_back({index, torrent.paths[offset], destinations[offset]});
		}

		std::vector<std::size_t> copy_order;
		copy_order.reserve(operations.size());
		for (std::size_t position = 0; position < operations.size(); ++position)
		{
			for (file_index_t source{0}; source < torrent.files.end_file(); ++source)
			{
				if (source == operations[position].index || torrent.files.pad_file_at(source)
					|| operations[position].destination
						!= torrent.paths[std::size_t(int(source))])
					continue;
				status |= disk_status::fatal_disk_error;
				error = storage_error(make_error_code(boost::system::errc::invalid_argument)
					, operations[position].index, operation_t::file_rename);
				return;
			}
			copy_order.push_back(position);
		}

		int const validation = normalized_destination_root.empty()
			? 0 : m_bridge.validate_path(normalized_destination_root);
		if (validation < 0)
		{
			status |= disk_status::fatal_disk_error;
			error = storage_error(validation == -EINVAL
				? make_error_code(boost::system::errc::invalid_argument)
				: from_bridge_result(validation), operation_t::file_rename);
			return;
		}

		if (flags == move_flags_t::reset_save_path
			|| flags == move_flags_t::reset_save_path_unchecked)
		{
			set_save_root(torrent, normalized_destination_root);
			if (flags == move_flags_t::reset_save_path)
				status |= disk_status::need_full_check;
			return;
		}

		if (flags == move_flags_t::fail_if_exist)
		{
			for (file_index_t index{0}; index < torrent.files.end_file(); ++index)
			{
				if (torrent.files.pad_file_at(index)) continue;
				auto const size = m_bridge.file_size(destinations[std::size_t(int(index))]);
				if (size >= 0)
				{
					status |= disk_status::file_exist;
					error = storage_error(make_error_code(boost::system::errc::file_exists)
						, index, operation_t::file_stat);
					return;
				}
				if (size != -ENOENT)
				{
					status |= disk_status::fatal_disk_error;
					error = make_error(size, index, operation_t::file_stat);
					return;
				}
			}
		}

		for (auto& operation : operations)
		{
			operation.destination_size = m_bridge.file_size(operation.destination);
			if (operation.destination_size < 0 && operation.destination_size != -ENOENT)
			{
				status |= disk_status::fatal_disk_error;
				error = make_error(operation.destination_size, operation.index
					, operation_t::file_stat);
				return;
			}
			operation.source_size = m_bridge.file_size(operation.source);
			if (operation.source_size < 0 && operation.source_size != -ENOENT)
			{
				status |= disk_status::fatal_disk_error;
				error = make_error(operation.source_size, operation.index
					, operation_t::file_stat);
				return;
			}
			operation.cleanup = operation.source_size >= 0;
			if (flags == move_flags_t::dont_replace && operation.destination_size >= 0)
			{
				status |= disk_status::need_full_check;
				continue;
			}
			operation.copy = operation.source_size >= 0;
		}

		for (auto const position : copy_order)
		{
			auto const& operation = operations[position];
			if (!operation.copy) continue;
			if (!copy_public_file(operation.source, operation.destination
				, operation.source_size, operation.index, error))
			{
				status |= disk_status::fatal_disk_error;
				return;
			}
		}

		set_save_root(torrent, normalized_destination_root);
		for (auto const& operation : operations)
		{
			if (!operation.cleanup
				|| std::find(destinations.begin(), destinations.end(), operation.source)
					!= destinations.end())
				continue;
			(void)m_bridge.remove(operation.source, false);
		}
	}

	void initialize_files(torrent_storage& torrent, std::uint32_t const storage
		, status_t& status, storage_error& error)
	{
		bool had_file = false;
		for (file_index_t index{0}; index < torrent.files.end_file(); ++index)
		{
			if (torrent.files.pad_file_at(index) || skipped(torrent, index)) continue;
			std::string const& path = path_for(torrent, index);
			auto const existing = m_bridge.file_size(path);
			had_file = had_file || existing >= 0;
			int const descriptor = open_file(storage, static_cast<int>(index), path, true
				, error, operation_t::file_open);
			if (descriptor < 0) { status |= disk_status::fatal_disk_error; return; }
			if (::ftruncate(descriptor, torrent.files.file_size(index)) != 0)
			{
				error = make_error(-errno, index, operation_t::file_truncate);
				status |= disk_status::fatal_disk_error;
				return;
			}
		}
		if (had_file) status |= disk_status::need_full_check;
	}

	void validate_resume(torrent_storage const& torrent
		, add_torrent_params const& resume, status_t& status, storage_error& error)
	{
		bool const seed = bool(resume.flags & torrent_flags::seed_mode);
		bool const full = seed || (resume.have_pieces.size() >= torrent.files.num_pieces()
			&& resume.have_pieces.all_set());
		auto check_file = [&](file_index_t const index, bool const exact_size) {
			if (torrent.files.pad_file_at(index)) return true;
			if (!seed && skipped(torrent, index)) return true;
			auto const size = m_bridge.file_size(path_for(torrent, index));
			if (size < 0 && size != -ENOENT)
			{
				error = make_error(size, index, operation_t::file_stat);
				status |= disk_status::fatal_disk_error;
				return false;
			}
			if (size == -ENOENT || (exact_size && size != torrent.files.file_size(index)))
			{
				status |= disk_status::need_full_check;
				if (size > torrent.files.file_size(index)) status |= disk_status::oversized_file;
			}
			return true;
		};

		if (full)
		{
			for (file_index_t index{0}; index < torrent.files.end_file(); ++index)
				if (!check_file(index, true)) return;
			return;
		}

		std::vector<bool> checked(std::size_t(torrent.files.num_files()), false);
		piece_index_t const end_piece = std::min(resume.have_pieces.end_index()
			, torrent.files.end_piece());
		for (piece_index_t piece{0}; piece < end_piece; ++piece)
		{
			if (!resume.have_pieces.get_bit(piece)) continue;
			auto const slices = torrent.files.map_block(piece, 0
				, torrent.files.piece_size(piece));
			for (auto const& slice : slices)
			{
				auto const offset = std::size_t(static_cast<int>(slice.file_index));
				if (checked[offset]) continue;
				checked[offset] = true;
				if (!check_file(slice.file_index, false)) return;
			}
		}
	}

	void migrate_priority(torrent_storage& torrent, std::uint32_t const storage
		, file_index_t const index, bool const to_part, storage_error& error)
	{
		std::int64_t const size = torrent.files.file_size(index);
		if (size == 0) return;
		std::string const& source_path = to_part ? path_for(torrent, index) : torrent.part_path;
		std::string const& destination_path = to_part ? torrent.part_path : path_for(torrent, index);
		int const source_key = to_part ? static_cast<int>(index) : -1;
		int const destination_key = to_part ? -1 : static_cast<int>(index);
		close_file(storage, source_key);
		close_file(storage, destination_key);

		int source_result = m_bridge.open_file(source_path, bridge_open_read);
		scoped_descriptor source(source_result);
		if (source_result < 0)
		{
			if (source_result != -ENOENT)
			{
				error = make_error(source_result, index
					, to_part ? operation_t::file_read : operation_t::partfile_read);
				return;
			}
			source_result = -1;
		}
		if (!ensure_parent_directories(destination_path, error, index)) return;
		int const destination_result = m_bridge.open_file(destination_path
			, bridge_open_read_write);
		scoped_descriptor destination(destination_result);
		if (destination_result < 0)
		{
			error = make_error(destination_result, index
				, to_part ? operation_t::partfile_write : operation_t::file_write);
			return;
		}
		if (!to_part && ::ftruncate(destination.descriptor, size) != 0)
		{
			error = make_error(-errno, index, operation_t::file_truncate);
			return;
		}

		std::vector<char> buffer(default_block_size, 0);
		std::int64_t copied = 0;
		while (copied < size)
		{
			std::size_t const length = std::size_t(std::min<std::int64_t>(buffer.size()
				, size - copied));
			if (source_result >= 0)
			{
				auto const source_offset = to_part ? copied
					: torrent.files.file_offset(index) + copied;
				auto const result = flow_saf_detail::pread_all(source.descriptor
					, buffer.data(), length, source_offset
					, [](int const fd, char* const data, std::size_t const bytes
						, std::int64_t const offset) {
						return ::pread(fd, data, bytes, offset);
					});
				if (result < 0)
				{
					error = make_error(result, index
						, to_part ? operation_t::file_read : operation_t::partfile_read);
					return;
				}
			}
			else std::fill(buffer.begin(), buffer.begin() + std::ptrdiff_t(length), 0);
			auto const destination_offset = to_part
				? torrent.files.file_offset(index) + copied : copied;
			auto const result = flow_saf_detail::pwrite_all(destination.descriptor
				, buffer.data(), length, destination_offset
				, [](int const fd, char const* const data, std::size_t const bytes
					, std::int64_t const offset) {
					return ::pwrite(fd, data, bytes, offset);
				});
			if (result < 0)
			{
				error = make_error(result, index
					, to_part ? operation_t::partfile_write : operation_t::file_write);
				return;
			}
			copied += std::int64_t(length);
		}
		source.reset();
		destination.reset();
		if (to_part)
		{
			int const result = m_bridge.remove(source_path, false);
			if (result < 0 && result != -ENOENT)
				error = make_error(result, index, operation_t::file_remove);
		}
	}

	settings_interface const& m_settings;
	aux::disk_buffer_pool m_buffer_pool;
	counters& m_stats_counters;
	io_context& m_ios;
	flow_saf_storage& m_bridge;

	mutable std::mutex m_storage_mutex;
	std::vector<std::shared_ptr<torrent_storage>> m_torrents;
	std::vector<std::uint32_t> m_free_storage;

	mutable std::mutex m_descriptor_mutex;
	std::vector<open_descriptor> m_descriptors;
	std::atomic<int> m_descriptor_limit{40};
	std::uint64_t m_use_counter = 0;

	std::mutex m_queue_mutex;
	std::condition_variable m_queue_condition;
	std::deque<job> m_jobs;
	bool m_accepting = true;
	bool m_stopping = false;
	std::thread m_worker;
};

} // anonymous namespace

std::unique_ptr<disk_interface> flow_saf_disk_io_constructor(
	io_context& ios, settings_interface const& settings, counters& counters
	, flow_saf_storage* bridge)
{
	if (bridge == nullptr)
		throw std::invalid_argument("Flow SAF disk I/O requires a non-null storage bridge");
	return std::make_unique<flow_saf_disk_io>(ios, settings, counters, *bridge);
}

} // namespace libtorrent
