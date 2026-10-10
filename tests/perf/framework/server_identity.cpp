#include "server_identity.h"

#include <optional>
#include <string>
#include <string_view>

namespace abyss::perf {

namespace {

constexpr std::string_view kRedis = "redis";

}  // namespace

std::optional<std::string> InfoField(std::string_view info, std::string_view key) {
  while (!info.empty()) {
    const auto eol = info.find('\n');
    std::string_view line = info.substr(0, eol);
    info = eol == std::string_view::npos ? std::string_view{} : info.substr(eol + 1);
    if (line.ends_with('\r')) line.remove_suffix(1);
    if (line.size() > key.size() && line.starts_with(key) && line[key.size()] == ':') {
      return std::string{line.substr(key.size() + 1)};
    }
  }
  return std::nullopt;
}

bool NeedsInfo(const std::optional<HelloFields>& hello) {
  return !hello.has_value() || hello->server.empty() || hello->server == kRedis;
}

ServerIdentity ResolveServerIdentity(const std::optional<HelloFields>& hello,
                                     std::string_view info) {
  if (hello.has_value() && !NeedsInfo(hello)) {
    return {.kind = hello->server, .version = hello->version};
  }
  if (auto v = InfoField(info, "valkey_version")) return {.kind = "valkey", .version = *v};
  if (auto v = InfoField(info, "dragonfly_version")) return {.kind = "dragonfly", .version = *v};
  if (hello.has_value() && !hello->server.empty()) {
    return {.kind = hello->server, .version = hello->version};
  }
  if (auto v = InfoField(info, "redis_version")) {
    return {.kind = std::string{kRedis}, .version = *v};
  }
  return {.kind = "unknown", .version = {}};
}

}  // namespace abyss::perf
