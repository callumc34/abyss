#include <cctype>
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

std::string Uppercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return out;
}

// Recursive glob match for '*' and '?' (case-insensitive).
// TODO: Full implementation, potentially using external package if required.
bool GlobMatch(std::string_view pattern, std::string_view text) {
  if (pattern.empty()) return text.empty();
  if (pattern[0] == '*') {
    while (!pattern.empty() && pattern[0] == '*') pattern.remove_prefix(1);
    if (pattern.empty()) return true;
    for (size_t i = 0; i <= text.size(); ++i) {
      if (GlobMatch(pattern, text.substr(i))) return true;
    }
    return false;
  }
  if (text.empty()) return false;
  if (pattern[0] == '?' || std::tolower(static_cast<unsigned char>(pattern[0])) ==
                               std::tolower(static_cast<unsigned char>(text[0]))) {
    return GlobMatch(pattern.substr(1), text.substr(1));
  }
  return false;
}

RespValue HandleGet(const core::RespCommand& cmd, const ConfigProvider& config) {
  if (cmd.ArgCount() != 3) {
    return RespValue::Error(ErrorPrefix::kErr, "Wrong number of arguments for 'CONFIG GET'");
  }
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

core::RespValue HandleConfig(const core::RespCommand& cmd, const ConfigProvider& config) {
  if (cmd.ArgCount() < 2) {
    return RespValue::Error(ErrorPrefix::kErr, "Wrong number of arguments for 'CONFIG'");
  }
  const auto sub = Uppercase(cmd.args[1]);
  if (sub == "GET") return HandleGet(cmd, config);

  std::string msg = "Unknown CONFIG subcommand or wrong number of arguments for '";
  msg.append(cmd.args[1]);
  msg.push_back('\'');
  return RespValue::Error(ErrorPrefix::kErr, std::move(msg));
}

}  // namespace abyss::resp
