#include "abyss/core/resp_format.h"

#include <array>
#include <charconv>
#include <cmath>
#include <string>

namespace abyss::core {

std::string FormatRespDouble(double value) {
  if (std::isnan(value)) return "nan";
  if (std::isinf(value)) return value > 0 ? "inf" : "-inf";

  std::array<char, 32> buf{};
  auto [ptr, ec] =
      std::to_chars(buf.data(), buf.data() + buf.size(), value, std::chars_format::general, 17);
  (void)ec;  // 32 bytes always fits a %.17g double representation.
  return {buf.data(), ptr};
}

}  // namespace abyss::core
