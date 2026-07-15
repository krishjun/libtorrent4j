#include "flow_saf_disk_io.hpp"

#include "flow_saf_storage.hpp"

#include "libtorrent/add_torrent_params.hpp"
#include "libtorrent/aux_/session_settings.hpp"
#include "libtorrent/disk_interface.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/hasher.hpp"
#include "libtorrent/hex.hpp"
#include "libtorrent/io_context.hpp"
#include "libtorrent/performance_counters.hpp"
#include "libtorrent/session.hpp"
#include "libtorrent/session_handle.hpp"
#include "libtorrent/settings_pack.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/system/system_error.hpp>

#include <sys/stat.h>
#include <unistd.h>

namespace lt = libtorrent;

namespace {

constexpr int open_read = 0;
constexpr int open_write = 1;
constexpr int open_read_write = 2;

[[noreturn]] void fail(char const* expression, char const* file, int line)
{
	throw std::runtime_error(std::string(file) + ":" + std::to_string(line)
		+ ": assertion failed: " + expression);
}

#define REQUIRE(expression) do { if (!(expression)) fail(#expression, __FILE__, __LINE__); } while (false)

std::string parent_path(std::string const& path)
{
	auto const pos = path.find_last_of('/');
	return pos == std::string::npos ? std::string{} : path.substr(0, pos);
}

bool has_prefix(std::string const& path, std::string const& prefix)
{
	return path == prefix || (path.size() > prefix.size()
		&& path.compare(0, prefix.size(), prefix) == 0
		&& path[prefix.size()] == '/');
}

struct file_closer
{
	void operator()(std::FILE* file) const
	{
		if (file != nullptr) std::fclose(file);
	}
};

struct memory_bridge final : flow_saf_storage
{
	int validate_path(std::string const& path) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		++validation_calls;
		if (auto const error = consume_failure("validate:" + path); error != 0) return error;
		return 0;
	}

	int create_directory(std::string const& path) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		++provider_calls;
		if (auto const error = consume_failure("mkdir:" + path); error != 0) return error;
		directories.insert(path);
		return 0;
	}

	int open_file(std::string const& path, int mode) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		++provider_calls;
		if (auto const error = consume_failure("open:" + path); error != 0) return error;
		auto iter = files.find(path);
		if (iter == files.end())
		{
			if (mode == open_read) return -ENOENT;
			std::unique_ptr<std::FILE, file_closer> file(std::tmpfile());
			if (!file) return -EIO;
			iter = files.emplace(path, std::move(file)).first;
		}
		int const descriptor = ::dup(::fileno(iter->second.get()));
		if (descriptor < 0) return -errno;
		opened_descriptors.push_back(descriptor);
		return descriptor;
	}

	std::int64_t file_size(std::string const& path) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		++provider_calls;
		if (auto const error = consume_failure("size:" + path); error != 0) return error;
		auto const iter = files.find(path);
		if (iter == files.end()) return -ENOENT;
		struct stat status {};
		if (::fstat(::fileno(iter->second.get()), &status) != 0) return -errno;
		return status.st_size;
	}

	int rename(std::string const& old_path, std::string const& new_path) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		++provider_calls;
		if (auto const error = consume_failure("rename:" + old_path); error != 0) return error;
		if (parent_path(old_path) != parent_path(new_path)) return -EXDEV;
		auto node = files.extract(old_path);
		if (node.empty()) return -ENOENT;
		node.key() = new_path;
		files.insert(std::move(node));
		return 0;
	}

	int remove(std::string const& path, bool recursive) override
	{
		std::lock_guard<std::mutex> lock(mutex);
		++provider_calls;
		if (auto const error = consume_failure("remove:" + path); error != 0) return error;
		bool removed = files.erase(path) != 0 || directories.erase(path) != 0;
		if (recursive)
		{
			for (auto iter = files.begin(); iter != files.end();)
				iter = has_prefix(iter->first, path) ? files.erase(iter) : std::next(iter);
			for (auto iter = directories.begin(); iter != directories.end();)
				iter = has_prefix(*iter, path) ? directories.erase(iter) : std::next(iter);
			removed = true;
		}
		return removed ? 0 : -ENOENT;
	}

	void fail_next(std::string operation, int negative_errno)
	{
		std::lock_guard<std::mutex> lock(mutex);
		failures.emplace(std::move(operation), negative_errno);
	}

	bool exists(std::string const& path) const
	{
		std::lock_guard<std::mutex> lock(mutex);
		return files.count(path) != 0 || directories.count(path) != 0;
	}

	std::vector<char> contents(std::string const& path) const
	{
		std::lock_guard<std::mutex> lock(mutex);
		auto const iter = files.find(path);
		REQUIRE(iter != files.end());
		struct stat status {};
		REQUIRE(::fstat(::fileno(iter->second.get()), &status) == 0);
		std::vector<char> result(static_cast<std::size_t>(status.st_size));
		if (!result.empty())
			REQUIRE(::pread(::fileno(iter->second.get()), result.data(), result.size(), 0)
				== static_cast<ssize_t>(result.size()));
		return result;
	}

	void store(std::string const& path, std::vector<char> const& bytes)
	{
		std::lock_guard<std::mutex> lock(mutex);
		std::unique_ptr<std::FILE, file_closer> file(std::tmpfile());
		REQUIRE(file != nullptr);
		if (!bytes.empty())
			REQUIRE(::pwrite(::fileno(file.get()), bytes.data(), bytes.size(), 0)
				== static_cast<ssize_t>(bytes.size()));
		files[path] = std::move(file);
	}
	void resize(std::string const& path, std::int64_t size)
	{
		std::lock_guard<std::mutex> lock(mutex);
		auto const iter = files.find(path);
		REQUIRE(iter != files.end());
		REQUIRE(::ftruncate(::fileno(iter->second.get()), size) == 0);
	}

	int live_descriptor_count() const
	{
		std::lock_guard<std::mutex> lock(mutex);
		std::set<int> live;
		for (int const descriptor : opened_descriptors)
			if (::fcntl(descriptor, F_GETFD) != -1) live.insert(descriptor);
		return static_cast<int>(live.size());
	}

	std::vector<int> opened_descriptor_snapshot() const
	{
		std::lock_guard<std::mutex> lock(mutex);
		return opened_descriptors;
	}

	int provider_call_count() const
	{
		std::lock_guard<std::mutex> lock(mutex);
		return provider_calls;
	}

	int validation_call_count() const
	{
		std::lock_guard<std::mutex> lock(mutex);
		return validation_calls;
	}

private:
	int consume_failure(std::string const& operation)
	{
		auto const iter = failures.find(operation);
		if (iter == failures.end()) return 0;
		int const error = iter->second;
		failures.erase(iter);
		return error;
	}

	mutable std::mutex mutex;
	std::map<std::string, std::unique_ptr<std::FILE, file_closer>> files;
	std::set<std::string> directories;
	std::multimap<std::string, int> failures;
	std::vector<int> opened_descriptors;
	int provider_calls = 0;
	int validation_calls = 0;
};

struct fixture
{
	explicit fixture(int file_pool_size = 4)
	{
		lt::settings_pack pack;
		pack.set_int(lt::settings_pack::file_pool_size, file_pool_size);
		settings = lt::aux::session_settings(pack);
		disk = flow_saf_disk_io_constructor(ioc, settings, counters, &bridge);
	}

	~fixture()
	{
		if (disk) disk->abort(true);
		ioc.restart();
		ioc.poll();
	}

	lt::storage_holder add(lt::file_storage const& files
		, lt::aux::vector<lt::download_priority_t, lt::file_index_t> const& priorities = {}
		, std::string const& save_root = {})
	{
		renamed.emplace_back();
		lt::storage_params params(files, renamed.back(), save_root, ""
			, lt::storage_mode_sparse, priorities, info_hash, true, true);
		return disk->new_torrent(params, {});
	}

	template <typename Predicate>
	void run_until(Predicate predicate)
	{
		auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		while (!predicate())
		{
			ioc.restart();
			ioc.poll();
			if (std::chrono::steady_clock::now() >= deadline)
				throw std::runtime_error("timed out waiting for asynchronous callback");
			std::this_thread::yield();
		}
	}

	lt::storage_error write(lt::storage_index_t storage, lt::peer_request request
		, std::vector<char> const& bytes)
	{
		REQUIRE(request.length == static_cast<int>(bytes.size()));
		std::atomic<bool> complete{false};
		lt::storage_error result;
		disk->async_write(storage, request, bytes.data(), {}, [&](lt::storage_error const& error) {
			result = error;
			complete = true;
		});
		disk->submit_jobs();
		run_until([&] { return complete.load(); });
		return result;
	}

	std::pair<std::vector<char>, lt::storage_error> read(
		lt::storage_index_t storage, lt::peer_request request)
	{
		std::atomic<bool> complete{false};
		std::vector<char> result;
		lt::storage_error error;
		disk->async_read(storage, request
			, [&](lt::disk_buffer_holder buffer, lt::storage_error const& callback_error) {
				error = callback_error;
				if (buffer) result.assign(buffer.data(), buffer.data() + request.length);
				complete = true;
			});
		disk->submit_jobs();
		run_until([&] { return complete.load(); });
		return {std::move(result), error};
	}

	lt::io_context ioc;
	lt::aux::session_settings settings;
	lt::counters counters;
	memory_bridge bridge;
	std::unique_ptr<lt::disk_interface> disk;
	std::vector<lt::renamed_files> renamed;
	lt::sha1_hash info_hash = lt::hasher("flow-saf-test").final();
};

lt::file_storage files(std::initializer_list<std::pair<char const*, std::int64_t>> entries
	, int piece_length = lt::default_block_size)
{
	lt::file_storage result;
	for (auto const& entry : entries) result.add_file(entry.first, entry.second);
	result.set_piece_length(piece_length);
	result.set_num_pieces(lt::aux::calc_num_pieces(result));
	return result;
}

std::vector<char> pattern(int size, char base = 'a')
{
	std::vector<char> result(static_cast<std::size_t>(size));
	for (int i = 0; i < size; ++i) result[static_cast<std::size_t>(i)] = char(base + i % 23);
	return result;
}
std::string hex_hash(lt::sha1_hash const& hash)
{
	return lt::aux::to_hex({hash.data(), hash.size()});
}

void test_single_file_and_cross_file_io()
{
	fixture f;
	auto fs = files({{"payload/one.bin", 20000}, {"payload/two.bin", 20000}});
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);

	auto one_file = pattern(1024);
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 2000, 1024}, one_file).ec);
	auto one_result = f.read(storage, {lt::piece_index_t{0}, 2000, 1024});
	REQUIRE(!one_result.second.ec);
	REQUIRE(one_result.first == one_file);

	auto crossing = pattern(4096, 'A');
	REQUIRE(!f.write(storage, {lt::piece_index_t{1}, 2000, 4096}, crossing).ec);
	auto cross_result = f.read(storage, {lt::piece_index_t{1}, 2000, 4096});
	REQUIRE(!cross_result.second.ec);
	REQUIRE(cross_result.first == crossing);
	REQUIRE(f.bridge.contents("payload/one.bin").size() == 20000);
	auto const second = f.bridge.contents("payload/two.bin");
	REQUIRE(std::equal(crossing.begin() + 1616, crossing.end(), second.begin()));
}

void test_session_root_sentinel_maps_to_granted_tree_root()
{
	fixture f;
	auto fs = files({{"root/payload.bin", 1024}});
	auto holder = f.add(fs, {}, ".flow-session-root");
	auto const storage = static_cast<lt::storage_index_t>(holder);
	auto const payload = pattern(1024);
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 1024}, payload).ec);
	REQUIRE(f.bridge.exists("root/payload.bin"));
	REQUIRE(!f.bridge.exists(".flow-session-root/root/payload.bin"));
	REQUIRE(f.bridge.validation_call_count() == 0);
}

void test_absolute_session_root_sentinel_maps_to_granted_tree_root()
{
	fixture f;
	auto fs = files({{"root/payload.bin", 1024}});
	auto holder = f.add(fs, {}, "/data/user/0/com.xuark.flow/files/.flow-session-root");
	auto const storage = static_cast<lt::storage_index_t>(holder);
	auto const payload = pattern(1024);
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 1024}, payload).ec);
	REQUIRE(f.bridge.exists("root/payload.bin"));
	REQUIRE(f.bridge.validation_call_count() == 0);
}

void test_sparse_write_and_check_files_resize()
{
	fixture f;
	auto fs = files({{"sparse/data.bin", 50000}});
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	auto bytes = pattern(512);
	REQUIRE(!f.write(storage, {lt::piece_index_t{2}, 1000, 512}, bytes).ec);
	auto contents = f.bridge.contents("sparse/data.bin");
	REQUIRE(contents.size() == 33768 + 512);
	REQUIRE(std::all_of(contents.begin(), contents.begin() + 33768, [](char value) { return value == 0; }));

	std::atomic<bool> complete{false};
	lt::storage_error error;
	lt::status_t status;
	f.disk->async_check_files(storage, nullptr, {}, [&](lt::status_t s, lt::storage_error const& e) {
		status = s;
		error = e;
		complete = true;
	});
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(status == lt::status_t{} || bool(status & lt::disk_status::need_full_check));
	REQUIRE(f.bridge.contents("sparse/data.bin").size() == 50000);
}

void test_v1_and_v2_hashes_cross_files()
{
	fixture f;
	auto fs = files({{"hash/first.bin", 7000}, {"hash/second.bin", 9384}});
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	auto bytes = pattern(lt::default_block_size, '0');
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, lt::default_block_size}, bytes).ec);

	std::atomic<bool> complete{false};
	lt::sha1_hash sha1;
	lt::storage_error error;
	std::vector<lt::sha256_hash> v2(1);
	f.disk->async_hash(storage, lt::piece_index_t{0}, v2, lt::disk_interface::v1_hash
		, [&](lt::piece_index_t piece, lt::sha1_hash const& result, lt::storage_error const& e) {
			REQUIRE(piece == lt::piece_index_t{0});
			sha1 = result;
			error = e;
			complete = true;
		});
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(sha1 == lt::hasher(bytes).final());
	auto const first_file = lt::span<char const>{bytes}.first(7000);
	REQUIRE(v2[0] == lt::hasher256(first_file).final());

	complete = false;
	lt::sha256_hash block_hash;
	f.disk->async_hash2(storage, lt::piece_index_t{0}, 0, {}
		, [&](lt::piece_index_t, lt::sha256_hash const& result, lt::storage_error const& e) {
			block_hash = result;
			error = e;
			complete = true;
		});
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(block_hash == lt::hasher256(first_file).final());
}

void test_resume_validation_uses_public_document_sizes()
{
	fixture f;
	auto fs = files({{"resume/a.bin", 4000}, {"resume/b.bin", 8000}});
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 12000}, pattern(12000)).ec);

	lt::add_torrent_params resume;
	resume.flags |= lt::torrent_flags::seed_mode;
	auto check = [&](lt::status_t& status, lt::storage_error& error) {
		std::atomic<bool> complete{false};
		f.disk->async_check_files(storage, &resume, {}, [&](lt::status_t s, lt::storage_error const& e) {
			status = s;
			error = e;
			complete = true;
		});
		f.disk->submit_jobs();
		f.run_until([&] { return complete.load(); });
	};

	lt::status_t status;
	lt::storage_error error;
	check(status, error);
	REQUIRE(!error.ec);
	REQUIRE(!bool(status & lt::disk_status::need_full_check));

	f.bridge.resize("resume/b.bin", 7999);
	check(status, error);
	REQUIRE(!error.ec);
	REQUIRE(bool(status & lt::disk_status::need_full_check));
	f.bridge.remove("resume/a.bin", false);
	check(status, error);
	REQUIRE(bool(status & lt::disk_status::need_full_check));
	f.bridge.fail_next("size:resume/b.bin", -EACCES);
	check(status, error);
	REQUIRE(bool(status & lt::disk_status::fatal_disk_error));
	REQUIRE(error.ec == boost::system::errc::permission_denied);
	REQUIRE(error.file() == lt::file_index_t{1});
	REQUIRE(error.operation == lt::operation_t::file_stat);
}

void test_ordinary_resume_missing_data_rechecks_without_creation()
{
	fixture f;
	auto fs = files({{"resume-missing/data.bin", 4096}});
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	lt::add_torrent_params resume;
	resume.have_pieces.resize(1);
	resume.have_pieces.set_bit(lt::piece_index_t{0});
	std::atomic<bool> complete{false};
	lt::status_t status;
	lt::storage_error error;
	f.disk->async_check_files(storage, &resume, {}
		, [&](lt::status_t result, lt::storage_error const& e) {
			status = result;
			error = e;
			complete = true;
		});
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(bool(status & lt::disk_status::need_full_check));
	REQUIRE(!bool(status & lt::disk_status::fatal_disk_error));
	REQUIRE(!f.bridge.exists("resume-missing/data.bin"));
}
void test_priority_zero_uses_public_flow_part_file()
{
	fixture f(1);
	auto fs = files({{"skip/zero.bin", 10000}, {"skip/keep.bin", 10000}});
	lt::aux::vector<lt::download_priority_t, lt::file_index_t> priorities;
	priorities.resize(2, lt::default_priority);
	priorities[lt::file_index_t{0}] = lt::dont_download;
	auto holder = f.add(fs, priorities);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	auto bytes = pattern(lt::default_block_size);
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, lt::default_block_size}, bytes).ec);

	std::string const part_path = ".flow-parts/" + hex_hash(f.info_hash) + ".parts";
	REQUIRE(f.bridge.exists(part_path));
	REQUIRE(!f.bridge.exists("skip/zero.bin"));
	REQUIRE(f.bridge.exists("skip/keep.bin"));
	auto const part = f.bridge.contents(part_path);
	REQUIRE(part.size() >= 10000);
	REQUIRE(std::equal(bytes.begin(), bytes.begin() + 10000, part.begin()));

	std::atomic<bool> complete{false};
	lt::storage_error error;
	priorities[lt::file_index_t{0}] = lt::default_priority;
	f.disk->async_set_file_priority(storage, priorities
		, [&](lt::storage_error const& e, auto returned) {
			error = e;
			REQUIRE(returned[lt::file_index_t{0}] == lt::default_priority);
			complete = true;
		});
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(f.bridge.exists("skip/zero.bin"));
	REQUIRE(std::equal(bytes.begin(), bytes.begin() + 10000
		, f.bridge.contents("skip/zero.bin").begin()));

	complete = false;
	f.disk->async_delete_files(storage, lt::session_handle::delete_partfile
		, [&](lt::storage_error const& e) { error = e; complete = true; });
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(!f.bridge.exists(part_path));
}

void test_priority_migration_commits_each_file_before_later_failure()
{
	fixture f(1);
	auto fs = files({{"priority/a.bin", 8}, {"priority/b.bin", 8}}, 8);
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	auto const first = pattern(8, 'A');
	auto const second = pattern(8, 'B');
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, first).ec);
	REQUIRE(!f.write(storage, {lt::piece_index_t{1}, 0, 8}, second).ec);

	lt::aux::vector<lt::download_priority_t, lt::file_index_t> requested;
	requested.resize(2, lt::dont_download);
	auto set_priorities = [&](auto const& values) {
		std::atomic<bool> complete{false};
		lt::storage_error error;
		lt::aux::vector<lt::download_priority_t, lt::file_index_t> returned;
		f.disk->async_set_file_priority(storage, values
			, [&](lt::storage_error const& callback_error, auto current) {
				error = callback_error;
				returned = std::move(current);
				complete = true;
			});
		f.disk->submit_jobs();
		f.run_until([&] { return complete.load(); });
		return std::make_pair(error, std::move(returned));
	};

	f.bridge.fail_next("open:priority/b.bin", -EACCES);
	auto result = set_priorities(requested);
	REQUIRE(result.first.ec == boost::system::errc::permission_denied);
	REQUIRE(result.second[lt::file_index_t{0}] == lt::dont_download);
	REQUIRE(result.second[lt::file_index_t{1}] == lt::default_priority);
	REQUIRE(!f.bridge.exists("priority/a.bin"));
	REQUIRE(f.bridge.exists("priority/b.bin"));
	auto read = f.read(storage, {lt::piece_index_t{0}, 0, 8});
	REQUIRE(!read.second.ec);
	REQUIRE(read.first == first);

	result = set_priorities(requested);
	REQUIRE(!result.first.ec);
	REQUIRE(result.second[lt::file_index_t{0}] == lt::dont_download);
	REQUIRE(result.second[lt::file_index_t{1}] == lt::dont_download);
	read = f.read(storage, {lt::piece_index_t{0}, 0, 8});
	REQUIRE(!read.second.ec);
	REQUIRE(read.first == first);
	auto const second_read = f.read(storage, {lt::piece_index_t{1}, 0, 8});
	REQUIRE(!second_read.second.ec);
	REQUIRE(second_read.first == second);
}

void test_rename_move_delete_release_and_stop()
{
	fixture f;
	auto fs = files({{"ops/a.bin", 1024}});
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	auto const payload = pattern(1024);
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 1024}, payload).ec);
	REQUIRE(f.bridge.live_descriptor_count() > 0);

	std::atomic<bool> complete{false};
	lt::storage_error error;
	f.disk->async_release_files(storage, [&] { complete = true; });
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(f.bridge.live_descriptor_count() == 0);

	complete = false;
	f.disk->async_rename_file(storage, lt::file_index_t{0}, "ops/renamed.bin"
		, [&](std::string const& name, lt::file_index_t index, lt::storage_error const& e) {
			REQUIRE(name == "ops/renamed.bin");
			REQUIRE(index == lt::file_index_t{0});
			error = e;
			complete = true;
		});
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(f.bridge.exists("ops/renamed.bin"));

	complete = false;
	lt::status_t move_status;
	std::string move_path;
	f.disk->async_move_storage(storage, "other-parent", lt::move_flags_t::always_replace_files
		, [&](lt::status_t status, std::string const& path, lt::storage_error const& e) {
			move_status = status;
			move_path = path;
			error = e;
			complete = true;
		});
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(!bool(move_status & lt::disk_status::fatal_disk_error));
	REQUIRE(move_path == "other-parent");
	REQUIRE(!f.bridge.exists("ops/renamed.bin"));
	REQUIRE(f.bridge.contents("other-parent/ops/renamed.bin") == payload);

	complete = false;
	f.disk->async_stop_torrent(storage, [&] { complete = true; });
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(f.bridge.live_descriptor_count() == 0);

	complete = false;
	f.disk->async_delete_files(storage, lt::session_handle::delete_files
		, [&](lt::storage_error const& e) { error = e; complete = true; });
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(!f.bridge.exists("other-parent/ops/renamed.bin"));
}

void test_move_storage_flags_and_safe_paths()
{
	struct move_result
	{
		lt::status_t status;
		std::string path;
		lt::storage_error error;
	};
	auto move = [](fixture& f, lt::storage_index_t const storage, std::string path
		, lt::move_flags_t const flags) {
		std::atomic<bool> complete{false};
		move_result result;
		f.disk->async_move_storage(storage, std::move(path), flags
			, [&](lt::status_t status, std::string const& returned_path
				, lt::storage_error const& error) {
				result.status = status;
				result.path = returned_path;
				result.error = error;
				complete = true;
			});
		f.disk->submit_jobs();
		f.run_until([&] { return complete.load(); });
		return result;
	};

	{
		fixture f;
		auto fs = files({{"a.bin", 8}, {"sub/a.bin", 8}}, 8);
		auto holder = f.add(fs);
		auto const storage = static_cast<lt::storage_index_t>(holder);
		auto const first = pattern(8, 'A');
		auto const second = pattern(8, 'B');
		REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, first).ec);
		REQUIRE(!f.write(storage, {lt::piece_index_t{1}, 0, 8}, second).ec);

		auto const provider_calls = f.bridge.provider_call_count();
		auto const result = move(f, storage, "sub"
			, lt::move_flags_t::always_replace_files);
		REQUIRE(result.error.ec == boost::system::errc::invalid_argument);
		REQUIRE(f.bridge.validation_call_count() == 0);
		REQUIRE(f.bridge.provider_call_count() == provider_calls);
		REQUIRE(bool(result.status & lt::disk_status::fatal_disk_error));
		REQUIRE(f.bridge.contents("a.bin") == first);
		REQUIRE(f.bridge.contents("sub/a.bin") == second);
		REQUIRE(!f.bridge.exists("sub/sub/a.bin"));
	}

	{
		fixture f;
		auto fs = files({{"restore/data.bin", 8}}, 8);
		auto holder = f.add(fs);
		auto const storage = static_cast<lt::storage_index_t>(holder);
		auto const payload = pattern(8, 'p');
		REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, payload).ec);
		auto const result = move(f, storage, "persisted"
			, lt::move_flags_t::always_replace_files);
		REQUIRE(!result.error.ec);

		auto const provider_calls = f.bridge.provider_call_count();
		auto const validation_calls = f.bridge.validation_call_count();
		auto restored_holder = f.add(fs, {}, "persisted");
		REQUIRE(f.bridge.provider_call_count() == provider_calls);
		REQUIRE(f.bridge.validation_call_count() == validation_calls + 1);
		auto const restored_storage = static_cast<lt::storage_index_t>(restored_holder);
		auto const restored = f.read(restored_storage, {lt::piece_index_t{0}, 0, 8});
		REQUIRE(!restored.second.ec);
		REQUIRE(restored.first == payload);
	}

	for (std::string const& root : {std::string{"/absolute"}, std::string{" "}
		, std::string{"C:/temp"}, std::string{"%2e%2e"}})
	{
		fixture f;
		auto fs = files({{"root/data.bin", 8}}, 8);
		if (root != "/absolute") f.bridge.fail_next("validate:" + root, -EINVAL);
		bool rejected = false;
		try
		{
			auto unsafe_holder = f.add(fs, {}, root);
			(void)unsafe_holder;
		}
		catch (std::invalid_argument const&)
		{
			rejected = true;
		}
		REQUIRE(rejected);
		REQUIRE(f.bridge.validation_call_count() == (root == "/absolute" ? 0 : 1));
		REQUIRE(f.bridge.provider_call_count() == 0);
		REQUIRE(!f.bridge.exists(root));
		auto valid_holder = f.add(fs);
		REQUIRE(static_cast<std::uint32_t>(static_cast<lt::storage_index_t>(valid_holder)) == 0);
		REQUIRE(f.bridge.provider_call_count() == 0);
	}

	{
		fixture f;
		auto fs = files({{"root/data.bin", 8}}, 8);
		f.bridge.fail_next("validate:", -EINVAL);
		auto holder = f.add(fs);
		REQUIRE(static_cast<std::uint32_t>(static_cast<lt::storage_index_t>(holder)) == 0);
		REQUIRE(f.bridge.validation_call_count() == 0);
		REQUIRE(f.bridge.provider_call_count() == 0);
		REQUIRE(!f.bridge.exists(""));
	}

	{
		fixture f;
		auto fs = files({{"root/data.bin", 8}}, 8);
		f.bridge.fail_next("validate:denied", -EACCES);
		boost::system::error_code validation_error;
		try
		{
			auto denied_holder = f.add(fs, {}, "denied");
			(void)denied_holder;
		}
		catch (boost::system::system_error const& error)
		{
			validation_error = error.code();
		}
		REQUIRE(validation_error == boost::system::errc::permission_denied);
		REQUIRE(f.bridge.validation_call_count() == 1);
		REQUIRE(f.bridge.provider_call_count() == 0);
		auto valid_holder = f.add(fs);
		REQUIRE(static_cast<std::uint32_t>(static_cast<lt::storage_index_t>(valid_holder)) == 0);
	}

	std::vector<lt::move_flags_t> const validation_flags = {
		lt::move_flags_t::always_replace_files,
		lt::move_flags_t::reset_save_path,
		lt::move_flags_t::reset_save_path_unchecked};
	for (auto const flags : validation_flags)
	{
		for (std::string const& root : {std::string{" "}, std::string{"C:/temp"}
			, std::string{"%2e%2e"}})
		{
			fixture f;
			auto fs = files({{"validate/source.bin", 8}}, 8);
			auto holder = f.add(fs);
			auto const storage = static_cast<lt::storage_index_t>(holder);
			auto const source = pattern(8, 'v');
			REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, source).ec);
			auto const provider_calls = f.bridge.provider_call_count();
			f.bridge.fail_next("validate:" + root, -EINVAL);
			auto const result = move(f, storage, root, flags);
			REQUIRE(result.error.ec == boost::system::errc::invalid_argument);
			REQUIRE(bool(result.status & lt::disk_status::fatal_disk_error));
			REQUIRE(f.bridge.validation_call_count() == 1);
			REQUIRE(f.bridge.provider_call_count() == provider_calls);
			REQUIRE(f.bridge.contents("validate/source.bin") == source);
			REQUIRE(!f.bridge.exists(root + "/validate/source.bin"));
		}
	}

	{
		fixture f;
		auto fs = files({{"move/source.bin", 8}}, 8);
		auto holder = f.add(fs);
		auto const storage = static_cast<lt::storage_index_t>(holder);
		auto const source = pattern(8, 's');
		auto const occupied = pattern(8, 'o');
		REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, source).ec);
		f.bridge.store("occupied/move/source.bin", occupied);

		auto result = move(f, storage, "occupied", lt::move_flags_t::fail_if_exist);
		REQUIRE(bool(result.status & lt::disk_status::file_exist));
		REQUIRE(!bool(result.status & lt::disk_status::fatal_disk_error));
		REQUIRE(result.error.ec == boost::system::errc::file_exists);
		REQUIRE(f.bridge.contents("move/source.bin") == source);
		REQUIRE(f.bridge.contents("occupied/move/source.bin") == occupied);

		result = move(f, storage, "occupied", lt::move_flags_t::dont_replace);
		REQUIRE(!result.error.ec);
		REQUIRE(!bool(result.status & lt::disk_status::fatal_disk_error));
		REQUIRE(!f.bridge.exists("move/source.bin"));
		REQUIRE(f.bridge.contents("occupied/move/source.bin") == occupied);
		auto const read = f.read(storage, {lt::piece_index_t{0}, 0, 8});
		REQUIRE(!read.second.ec);
		REQUIRE(read.first == occupied);
	}

	{
		fixture f;
		auto fs = files({{"reset/source.bin", 8}}, 8);
		auto holder = f.add(fs);
		auto const storage = static_cast<lt::storage_index_t>(holder);
		auto const original = pattern(8, 'r');
		REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, original).ec);

		auto validation_calls = f.bridge.validation_call_count();
		auto result = move(f, storage, "reset-target", lt::move_flags_t::reset_save_path);
		REQUIRE(!result.error.ec);
		REQUIRE(f.bridge.validation_call_count() == validation_calls + 1);
		REQUIRE(bool(result.status & lt::disk_status::need_full_check));
		REQUIRE(f.bridge.contents("reset/source.bin") == original);
		auto const checked = pattern(8, 'c');
		REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, checked).ec);
		REQUIRE(f.bridge.contents("reset-target/reset/source.bin") == checked);
		REQUIRE(f.bridge.contents("reset/source.bin") == original);

		validation_calls = f.bridge.validation_call_count();
		result = move(f, storage, "unchecked-target"
			, lt::move_flags_t::reset_save_path_unchecked);
		REQUIRE(!result.error.ec);
		REQUIRE(f.bridge.validation_call_count() == validation_calls + 1);
		REQUIRE(!bool(result.status & lt::disk_status::need_full_check));
		auto const unchecked = pattern(8, 'u');
		REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, unchecked).ec);
		REQUIRE(f.bridge.contents("unchecked-target/reset/source.bin") == unchecked);
	}

	{
		fixture f;
		auto fs = files({{"cleanup/source.bin", 8}}, 8);
		auto holder = f.add(fs);
		auto const storage = static_cast<lt::storage_index_t>(holder);
		auto const source = pattern(8, 'k');
		REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, source).ec);
		f.bridge.fail_next("remove:cleanup/source.bin", -EACCES);
		auto const result = move(f, storage, "cleanup-target"
			, lt::move_flags_t::always_replace_files);
		REQUIRE(!result.error.ec);
		REQUIRE(!bool(result.status & lt::disk_status::fatal_disk_error));
		REQUIRE(f.bridge.contents("cleanup-target/cleanup/source.bin") == source);
		REQUIRE(f.bridge.exists("cleanup/source.bin"));
		auto const read = f.read(storage, {lt::piece_index_t{0}, 0, 8});
		REQUIRE(!read.second.ec);
		REQUIRE(read.first == source);
	}
	{
		fixture f;
		auto fs = files({{"safe/source.bin", 8}}, 8);
		auto holder = f.add(fs);
		auto const storage = static_cast<lt::storage_index_t>(holder);
		auto const source = pattern(8, 'x');
		REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 8}, source).ec);
		auto const result = move(f, storage, "/absolute"
			, lt::move_flags_t::always_replace_files);
		REQUIRE(bool(result.status & lt::disk_status::fatal_disk_error));
		REQUIRE(result.error.ec == boost::system::errc::invalid_argument);
		REQUIRE(f.bridge.contents("safe/source.bin") == source);
	}
}
void test_descriptor_pool_eviction_and_errno_propagation()
{
	fixture f(2);
	auto fs = files({{"pool/a.bin", 10}, {"pool/b.bin", 10}, {"pool/c.bin", 10}}, 10);
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	for (int piece = 0; piece < 3; ++piece)
		REQUIRE(!f.write(storage, {lt::piece_index_t{piece}, 0, 10}, pattern(10, char('a' + piece))).ec);
	REQUIRE(f.disk->get_status(storage).size() <= 2);

	f.disk->async_release_files(storage);
	f.disk->submit_jobs();
	std::atomic<bool> barrier{false};
	f.disk->async_clear_piece(storage, lt::piece_index_t{0}, [&](lt::piece_index_t) { barrier = true; });
	f.disk->submit_jobs();
	f.run_until([&] { return barrier.load(); });
	REQUIRE(f.disk->get_status(storage).empty());

	f.bridge.fail_next("open:pool/a.bin", -EACCES);
	auto const error = f.write(storage, {lt::piece_index_t{0}, 0, 10}, pattern(10));
	REQUIRE(error.ec == boost::system::errc::permission_denied);
	REQUIRE(error.file() == lt::file_index_t{0});
	REQUIRE(error.operation == lt::operation_t::file_write);
}

void test_transfer_helpers_retry_interrupts_and_partial_io()
{
	std::vector<char> const source{'f', 'l', 'o', 'w', '!', '!'};
	std::vector<char> read_back(source.size(), 0);
	int read_calls = 0;
	auto reader = [&](int, char* buffer, std::size_t size, std::int64_t offset) -> ssize_t {
		if (read_calls++ == 0)
		{
			errno = EINTR;
			return -1;
		}
		auto const count = std::min<std::size_t>(size, 2);
		std::memcpy(buffer, source.data() + offset, count);
		return static_cast<ssize_t>(count);
	};
	REQUIRE(lt::flow_saf_detail::pread_all(7, read_back.data(), read_back.size(), 0, reader)
		== static_cast<std::int64_t>(source.size()));
	REQUIRE(read_back == source);

	std::vector<char> written(source.size(), 0);
	int write_calls = 0;
	auto writer = [&](int, char const* buffer, std::size_t size, std::int64_t offset) -> ssize_t {
		if (write_calls++ == 0)
		{
			errno = EINTR;
			return -1;
		}
		auto const count = std::min<std::size_t>(size, 2);
		std::memcpy(written.data() + offset, buffer, count);
		return static_cast<ssize_t>(count);
	};
	REQUIRE(lt::flow_saf_detail::pwrite_all(8, source.data(), source.size(), 0, writer)
		== static_cast<std::int64_t>(source.size()));
	REQUIRE(written == source);

	auto zero_writer = [](int, char const*, std::size_t, std::int64_t) -> ssize_t {
		errno = 0;
		return 0;
	};
	REQUIRE(lt::flow_saf_detail::pwrite_all(9, source.data(), source.size(), 0, zero_writer)
		== -EIO);
}
void test_disk_statistics_are_recorded()
{
	fixture f;
	auto fs = files({{"stats/data.bin", 4096}});
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	auto bytes = pattern(4096);
	REQUIRE(!f.write(storage, {lt::piece_index_t{0}, 0, 4096}, bytes).ec);
	auto const read = f.read(storage, {lt::piece_index_t{0}, 0, 4096});
	REQUIRE(!read.second.ec);
	std::atomic<bool> complete{false};
	lt::storage_error error;
	f.disk->async_hash2(storage, lt::piece_index_t{0}, 0, {}
		, [&](lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const& e) {
			error = e;
			complete = true;
		});
	f.disk->submit_jobs();
	f.run_until([&] { return complete.load(); });
	REQUIRE(!error.ec);
	REQUIRE(f.counters[lt::counters::num_blocks_written] > 0);
	REQUIRE(f.counters[lt::counters::num_write_ops] > 0);
	REQUIRE(f.counters[lt::counters::num_blocks_read] > 0);
	REQUIRE(f.counters[lt::counters::num_read_ops] > 0);
	REQUIRE(f.counters[lt::counters::disk_job_time] >= 0);
	REQUIRE(f.counters[lt::counters::disk_hash_time] >= 0);
}

void test_null_bridge_is_rejected_without_fallback()
{
	lt::session_params params;
	bool rejected = false;
	try
	{
		flow_set_saf_disk_io_constructor(params, nullptr);
	}
	catch (std::invalid_argument const&)
	{
		rejected = true;
	}
	REQUIRE(rejected);
}

void test_full_session_starts_with_saf_disk_constructor()
{
	memory_bridge bridge;
	lt::session_params params;
	flow_set_saf_disk_io_constructor(params, &bridge);
	// The Java binding exposes the const-reference overload, so exercise the
	// same session_params copy path used by SessionManager.start().
	lt::session session(params);
	REQUIRE(session.is_valid());
}

void test_callbacks_run_on_io_context_and_abort_drains_queue()
{
	fixture f;
	auto fs = files({{"shutdown/a.bin", 4096}});
	auto holder = f.add(fs);
	auto const storage = static_cast<lt::storage_index_t>(holder);
	std::thread::id const network_thread = std::this_thread::get_id();
	std::atomic<int> callbacks{0};
	std::thread::id callback_thread;
	for (int i = 0; i < 8; ++i)
	{
		auto bytes = pattern(256, char('a' + i));
		f.disk->async_write(storage, {lt::piece_index_t{0}, i * 256, 256}, bytes.data(), {}
			, [&](lt::storage_error const& error) {
				REQUIRE(!error.ec);
				callback_thread = std::this_thread::get_id();
				++callbacks;
			});
	}
	f.disk->submit_jobs();
	f.disk->abort(true);
	f.run_until([&] { return callbacks.load() == 8; });
	REQUIRE(callback_thread == network_thread);
	REQUIRE(f.bridge.live_descriptor_count() == 0);
	holder.reset();
	f.disk.reset();
	f.ioc.restart();
	REQUIRE(f.ioc.poll() == 0);
}

} // anonymous namespace

int main()
{
	struct test_case { char const* name; void (*function)(); };
	std::vector<test_case> const tests = {
		{"single-file and cross-file I/O", test_single_file_and_cross_file_io},
		{"session root sentinel", test_session_root_sentinel_maps_to_granted_tree_root},
		{"absolute session root sentinel", test_absolute_session_root_sentinel_maps_to_granted_tree_root},
		{"sparse write and check-files resize", test_sparse_write_and_check_files_resize},
		{"v1 and v2 hashing", test_v1_and_v2_hashes_cross_files},
		{"resume validation", test_resume_validation_uses_public_document_sizes},
		{"ordinary resume missing data", test_ordinary_resume_missing_data_rechecks_without_creation},
		{"priority-zero public part file", test_priority_zero_uses_public_flow_part_file},
		{"priority migration transaction", test_priority_migration_commits_each_file_before_later_failure},
		{"rename, move, delete, release and stop", test_rename_move_delete_release_and_stop},
		{"move flags and safe paths", test_move_storage_flags_and_safe_paths},
		{"descriptor pool and errno propagation", test_descriptor_pool_eviction_and_errno_propagation},
		{"partial and interrupted transfer helpers", test_transfer_helpers_retry_interrupts_and_partial_io},
		{"disk statistics", test_disk_statistics_are_recorded},
		{"null bridge rejection", test_null_bridge_is_rejected_without_fallback},
		{"full session startup", test_full_session_starts_with_saf_disk_constructor},
		{"io-context callbacks and abort", test_callbacks_run_on_io_context_and_abort_drains_queue},
	};

	for (auto const& test : tests)
	{
		try
		{
			test.function();
			std::cout << "PASS " << test.name << "\n";
		}
		catch (std::exception const& error)
		{
			std::cerr << "FAIL " << test.name << ": " << error.what() << "\n";
			return 1;
		}
	}
	return 0;
}
