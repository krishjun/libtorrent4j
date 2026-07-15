#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "libtorrent/io_context.hpp"

struct flow_saf_storage;

namespace libtorrent {

struct counters;
struct disk_interface;
struct settings_interface;

namespace flow_saf_detail {

template <typename Reader>
std::int64_t pread_all(int const descriptor, char* const buffer
	, std::size_t const size, std::int64_t const offset, Reader&& reader)
{
	std::size_t transferred = 0;
	while (transferred < size)
	{
		errno = 0;
		auto const result = reader(descriptor, buffer + transferred, size - transferred
			, offset + std::int64_t(transferred));
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) return -(errno == 0 ? EIO : errno);
		if (result == 0 || std::size_t(result) > size - transferred) return -EIO;
		transferred += std::size_t(result);
	}
	return std::int64_t(transferred);
}

template <typename Writer>
std::int64_t pwrite_all(int const descriptor, char const* const buffer
	, std::size_t const size, std::int64_t const offset, Writer&& writer)
{
	std::size_t transferred = 0;
	while (transferred < size)
	{
		errno = 0;
		auto const result = writer(descriptor, buffer + transferred, size - transferred
			, offset + std::int64_t(transferred));
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) return -(errno == 0 ? EIO : errno);
		if (result == 0 || std::size_t(result) > size - transferred) return -EIO;
		transferred += std::size_t(result);
	}
	return std::int64_t(transferred);
}

} // namespace flow_saf_detail

std::unique_ptr<disk_interface> flow_saf_disk_io_constructor(
	io_context& ios, settings_interface const& settings, counters& counters
	, flow_saf_storage* bridge);

} // namespace libtorrent
