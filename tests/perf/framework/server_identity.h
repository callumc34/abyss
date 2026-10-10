#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace abyss::perf {

struct ServerIdentity {
  std::string kind;
  std::string version;
};

// The "server" and "version" fields of a HELLO reply.
struct HelloFields {
  std::string server;
  std::string version;
};

// Value of `key` in an INFO reply's "key:value" lines.
std::optional<std::string> InfoField(std::string_view info, std::string_view key);

// Valkey and Dragonfly may answer HELLO as "redis", so only INFO's
// *_version fields can tell them apart.
bool NeedsInfo(const std::optional<HelloFields>& hello);

// `info` is the INFO server section, or empty when it was not fetched.
ServerIdentity ResolveServerIdentity(const std::optional<HelloFields>& hello,
                                     std::string_view info);

}  // namespace abyss::perf
