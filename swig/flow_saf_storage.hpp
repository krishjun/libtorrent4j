#pragma once

#include <cstdint>
#include <string>

#include "libtorrent/session_params.hpp"

struct flow_saf_storage {
  virtual ~flow_saf_storage() = default;

  // Pure validation only: this callback must not perform provider I/O. It may
  // run on the caller thread and returns 0 or negative errno.
  virtual int validate_path(std::string const& path) = 0;

  // Provider callbacks run on the disk worker thread. Paths are normalized UTF-8
  // relative paths beneath the granted Flow tree; failures return negative errno.
  virtual int create_directory(std::string const& path) = 0;
  virtual int open_file(std::string const& path, int mode) = 0;
  virtual std::int64_t file_size(std::string const& path) = 0;
  virtual int rename(std::string const& old_path, std::string const& new_path) = 0;
  virtual int remove(std::string const& path, bool recursive) = 0;
};

void flow_set_saf_disk_io_constructor(
    libtorrent::session_params& params, flow_saf_storage* bridge);
