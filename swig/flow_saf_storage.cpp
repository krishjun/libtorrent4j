#include "flow_saf_storage.hpp"
#include "flow_saf_disk_io.hpp"

#include "libtorrent/session.hpp"
#include "libtorrent/session_params.hpp"

#if defined(__ANDROID__) && __ANDROID_API__ < 28
#include <cerrno>
#include <cstddef>
#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>

// Some bundled dependencies weak-link these API-28 libc entry points even
// when the target is API 24. They must resolve to callable implementations;
// resolving them to address zero makes libtorrent session startup crash.
extern "C" __attribute__((visibility("default")))
int getentropy(void* buffer, std::size_t length)
{
  if (length > 256)
  {
    errno = EIO;
    return -1;
  }

  int descriptor;
  do descriptor = ::open("/dev/urandom", O_RDONLY | O_CLOEXEC);
  while (descriptor < 0 && errno == EINTR);
  if (descriptor < 0) return -1;

  auto* output = static_cast<unsigned char*>(buffer);
  std::size_t consumed = 0;
  while (consumed < length)
  {
    ssize_t const count = ::read(descriptor, output + consumed, length - consumed);
    if (count > 0)
    {
      consumed += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) continue;
    int const read_error = count == 0 ? EIO : errno;
    ::close(descriptor);
    errno = read_error;
    return -1;
  }

  ::close(descriptor);
  return 0;
}

extern "C" __attribute__((visibility("default")))
int memfd_create(char const* name, unsigned int flags)
{
#if defined(__NR_memfd_create)
  return static_cast<int>(::syscall(__NR_memfd_create, name, flags));
#else
  (void)name;
  (void)flags;
  errno = ENOSYS;
  return -1;
#endif
}
#endif

#include <stdexcept>

void flow_set_saf_disk_io_constructor(
    libtorrent::session_params& params, flow_saf_storage* bridge) {
  if (bridge == nullptr)
    throw std::invalid_argument("Flow SAF disk I/O requires a non-null storage bridge");

  params.disk_io_constructor =
      [bridge](libtorrent::io_context& ios,
               libtorrent::settings_interface const& settings,
               libtorrent::counters& counters) {
        return libtorrent::flow_saf_disk_io_constructor(
            ios, settings, counters, bridge);
      };
}
