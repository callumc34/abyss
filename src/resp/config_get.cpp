#include <cctype>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/resp/admin_handlers.h"
#include "abyss/resp/config_provider.h"

namespace abyss::resp {
namespace {

using core::ErrorPrefix;
using core::RespValue;

// Iterative glob match for '*' and '?' (case-insensitive). Bounded by
// O(|pattern| + |text|) amortised — no recursion, safe against adversarial
// patterns that could otherwise cause exponential backtracking.
bool GlobMatch(std::string_view pattern, std::string_view text) noexcept {
  size_t pi = 0;
  size_t ti = 0;
  size_t star_p = std::string_view::npos;
  size_t star_t = 0;

  auto eq_ci = [](char a, char b) {
    return std::tolower(static_cast<unsigned char>(a)) ==
           std::tolower(static_cast<unsigned char>(b));
  };

  while (ti < text.size()) {
    if (pi < pattern.size() && (pattern[pi] == '?' || eq_ci(pattern[pi], text[ti]))) {
      ++pi;
      ++ti;
    } else if (pi < pattern.size() && pattern[pi] == '*') {
      star_p = pi++;
      star_t = ti;
    } else if (star_p != std::string_view::npos) {
      pi = star_p + 1;
      ti = ++star_t;
    } else {
      return false;
    }
  }
  while (pi < pattern.size() && pattern[pi] == '*') ++pi;
  return pi == pattern.size();
}

RespValue HandleGet(const core::RespCommand& cmd, const ConfigProvider& config) {
  const std::string_view pattern = cmd.args[2];
  std::vector<RespValue> out;
  for (const auto& [key, value] : config.Entries()) {
    if (GlobMatch(pattern, key)) {
      out.push_back(RespValue::BulkString(std::string(key)));
      out.push_back(RespValue::BulkString(value));
    }
  }
  return RespValue::Array(std::move(out));
}

}  // namespace

core::RespValue HandleConfig(std::string_view subcommand, const core::RespCommand& cmd,
                             const ConfigProvider& config) {
  if (subcommand == "GET") return HandleGet(cmd, config);

  return RespValue::Error(
      ErrorPrefix::kErr,
      std::string("internal: unhandled CONFIG subcommand '").append(subcommand).append("'"));
}

}  // namespace abyss::resp
