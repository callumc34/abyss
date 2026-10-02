#ifndef _WIN32

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <span>
#include <string>
#include <system_error>

#include "abyss/platform/random.h"

#ifdef __linux__
#include <sys/random.h>
#endif

namespace abyss::platform {

core::Result<void> RandomBytes(std::span<std::byte> out) {
#ifdef __linux__
  std::size_t filled = 0;
  while (filled < out.size()) {
    const ssize_t n = ::getrandom(out.data() + filled, out.size() - filled, 0);
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                         "getrandom: " + std::generic_category().message(errno)});
    }
    filled += static_cast<std::size_t>(n);
  }
  return {};
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
  ::arc4random_buf(out.data(), out.size());
  return {};
#else
#error "RandomBytes needs an OS CSPRNG on this platform"
#endif
}

}  // namespace abyss::platform

#endif  // _WIN32
