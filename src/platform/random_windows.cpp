#ifdef _WIN32

// clang-format off
#include <windows.h>
#include <bcrypt.h>
// clang-format on

#include <algorithm>
#include <cstddef>
#include <limits>
#include <span>
#include <string>

#include "abyss/platform/random.h"

namespace abyss::platform {

core::Result<void> RandomBytes(std::span<std::byte> out) {
  std::size_t filled = 0;
  while (filled < out.size()) {
    const auto n = static_cast<ULONG>(
        std::min<std::size_t>(out.size() - filled, std::numeric_limits<ULONG>::max()));
    const NTSTATUS status = ::BCryptGenRandom(
        nullptr, reinterpret_cast<PUCHAR>(out.data() + filled), n, BCRYPT_USE_SYSTEM_PREFERRED_RNG);
    if (status < 0) {
      return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                         "BCryptGenRandom failed: " + std::to_string(status)});
    }
    filled += n;
  }
  return {};
}

}  // namespace abyss::platform

#endif  // _WIN32
