#include "abyss/resp/command_registry.h"

#include <array>
#include <cctype>
#include <string>

namespace abyss::resp {
namespace {

// The canonical command table.
constexpr auto kCommandTable = std::to_array<CommandSpec>({
    // ── Admin: stateless ──────────────────────────────────────────────────
    {"PING", -1, CommandClass::kAdmin, Dispatch::kStateless},
    {"ECHO", 2, CommandClass::kAdmin, Dispatch::kStateless},
    {"QUIT", 1, CommandClass::kAdmin, Dispatch::kStateless},
    {"HELLO", -1, CommandClass::kAdmin, Dispatch::kStateless},
    {"CLIENT", -2, CommandClass::kAdmin, Dispatch::kStateless},
    {"RESET", 1, CommandClass::kAdmin, Dispatch::kStateless},
    {"TIME", 1, CommandClass::kAdmin, Dispatch::kStateless},
    {"COMMAND", -1, CommandClass::kAdmin, Dispatch::kStateless},
    {"CONFIG", -3, CommandClass::kAdmin, Dispatch::kStateless},

    // ── Admin: consumer RPC ───────────────────────────────────────────────
    {"DBSIZE", 1, CommandClass::kAdmin, Dispatch::kConsumerRpc},
    {"INFO", -1, CommandClass::kAdmin, Dispatch::kConsumerRpc},

    // ── Admin: cluster ──────────────────────────────────────────────────────
    {"CLUSTER", -2, CommandClass::kAdmin, Dispatch::kStateless},

    // ── Strings ───────────────────────────────────────────────────────────
    {"GET", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"SET", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"SETNX", 3, CommandClass::kWrite, Dispatch::kConditionalWrite},
    {"SETEX", 4, CommandClass::kWrite, Dispatch::kWritePath},
    {"PSETEX", 4, CommandClass::kWrite, Dispatch::kWritePath},
    {"GETSET", 3, CommandClass::kWrite, Dispatch::kWritePath},
    {"GETDEL", 2, CommandClass::kWrite, Dispatch::kWritePath},
    {"APPEND", 3, CommandClass::kWrite, Dispatch::kWritePath},
    {"STRLEN", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"INCR", 2, CommandClass::kWrite, Dispatch::kWritePath},
    {"DECR", 2, CommandClass::kWrite, Dispatch::kWritePath},
    {"INCRBY", 3, CommandClass::kWrite, Dispatch::kWritePath},
    {"DECRBY", 3, CommandClass::kWrite, Dispatch::kWritePath},
    {"INCRBYFLOAT", 3, CommandClass::kWrite, Dispatch::kWritePath},
    {"MGET", -2, CommandClass::kRead, Dispatch::kTieredRead},
    {"MSET", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"MSETNX", -3, CommandClass::kWrite, Dispatch::kConditionalWrite},

    // ── Sets ──────────────────────────────────────────────────────────────
    {"SADD", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"SREM", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"SMEMBERS", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"SISMEMBER", 3, CommandClass::kRead, Dispatch::kTieredRead},
    {"SMISMEMBER", -3, CommandClass::kRead, Dispatch::kTieredRead},
    {"SCARD", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"SPOP", -2, CommandClass::kWrite, Dispatch::kWritePath},
    {"SRANDMEMBER", -2, CommandClass::kRead, Dispatch::kTieredRead},

    // ── Sorted sets ───────────────────────────────────────────────────────
    {"ZADD", -4, CommandClass::kWrite, Dispatch::kWritePath},
    {"ZREM", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"ZSCORE", 3, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZMSCORE", -3, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZCARD", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZRANK", -3, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZREVRANK", -3, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZINCRBY", 4, CommandClass::kWrite, Dispatch::kWritePath},
    {"ZRANGE", -4, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZRANGEBYSCORE", -4, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZRANGEBYLEX", -4, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZCOUNT", 4, CommandClass::kRead, Dispatch::kTieredRead},
    {"ZLEXCOUNT", 4, CommandClass::kRead, Dispatch::kTieredRead},

    // ── Generic / key management ──────────────────────────────────────────
    {"DEL", -2, CommandClass::kWrite, Dispatch::kWritePath},
    {"UNLINK", -2, CommandClass::kWrite, Dispatch::kWritePath},
    {"EXISTS", -2, CommandClass::kRead, Dispatch::kTieredRead},
    {"EXPIRE", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"PEXPIRE", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"EXPIREAT", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"PEXPIREAT", -3, CommandClass::kWrite, Dispatch::kWritePath},
    {"PERSIST", 2, CommandClass::kWrite, Dispatch::kWritePath},
    {"TTL", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"PTTL", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"EXPIRETIME", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"PEXPIRETIME", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"TYPE", 2, CommandClass::kRead, Dispatch::kTieredRead},
    {"RENAME", 3, CommandClass::kWrite, Dispatch::kWritePath},
    {"RENAMENX", 3, CommandClass::kWrite, Dispatch::kConditionalWrite},
    {"COPY", -3, CommandClass::kWrite, Dispatch::kConditionalWrite},
    {"OBJECT", -3, CommandClass::kRead, Dispatch::kTieredRead},
});

std::string Uppercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return out;
}

bool ArityMatches(const CommandSpec& spec, size_t arg_count) {
  const auto arg_count_int = static_cast<int>(arg_count);
  if (spec.arity >= 0) {
    return arg_count_int == spec.arity;
  }
  return arg_count_int >= -spec.arity;
}

}  // namespace

CommandRegistry::CommandRegistry() {
  by_name_.reserve(kCommandTable.size());
  for (const auto& spec : kCommandTable) {
    by_name_.emplace(std::string(spec.name), &spec);
  }
}

const CommandSpec* CommandRegistry::Find(std::string_view name) const {
  auto upper = Uppercase(name);
  auto it = by_name_.find(upper);
  if (it == by_name_.end()) {
    return nullptr;
  }
  return it->second;
}

core::Result<const CommandSpec*> CommandRegistry::Classify(const core::RespCommand& cmd) const {
  if (cmd.ArgCount() == 0) {
    return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument, "empty command"));
  }
  const auto* spec = Find(cmd.Name());
  if (spec == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, std::string(cmd.Name())));
  }
  if (!ArityMatches(*spec, cmd.ArgCount())) {
    return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument, std::string(spec->name)));
  }
  return spec;
}

const CommandRegistry& GlobalRegistry() {
  static const CommandRegistry registry;
  return registry;
}

}  // namespace abyss::resp
