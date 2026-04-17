#include "abyss/resp/command_registry.h"

#include <array>
#include <cctype>
#include <string>

namespace abyss::resp {
namespace {

// The canonical command table.
// Fields: {name, arity, class, dispatch, first_key, last_key, key_step}
// Key positions mirror Redis COMMAND INFO. 0 = no keys.
constexpr auto kCommandTable = std::to_array<CommandSpec>({
    // ── Admin: stateless (no keys) ───────────────────────────────────────
    {"PING", -1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"ECHO", 2, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"QUIT", 1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"HELLO", -1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"CLIENT", -2, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"RESET", 1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"TIME", 1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"COMMAND", -1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"CONFIG", -3, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},

    {"FLUSHALL", -1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"FLUSHDB", -1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},
    {"INFO", -1, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},

    // ── Admin: consumer RPC (no keys) ────────────────────────────────────
    {"DBSIZE", 1, CommandClass::kAdmin, Dispatch::kConsumerRpc, 0, 0, 0},

    // ── Admin: cluster (no keys) ─────────────────────────────────────────
    {"CLUSTER", -2, CommandClass::kAdmin, Dispatch::kStateless, 0, 0, 0},

    // ── Strings ──────────────────────────────────────────────────────────
    {"GET", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"SET", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"SETNX", 3, CommandClass::kWrite, Dispatch::kConditionalWrite, 1, 1, 1},
    {"SETEX", 4, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"PSETEX", 4, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"GETSET", 3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"GETDEL", 2, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"APPEND", 3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"STRLEN", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"INCR", 2, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"DECR", 2, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"INCRBY", 3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"DECRBY", 3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"INCRBYFLOAT", 3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"MGET", -2, CommandClass::kRead, Dispatch::kTieredRead, 1, -1, 1},
    {"MSET", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, -1, 2},
    {"MSETNX", -3, CommandClass::kWrite, Dispatch::kConditionalWrite, 1, -1, 2},

    // ── Sets ─────────────────────────────────────────────────────────────
    {"SADD", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"SREM", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"SMEMBERS", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"SISMEMBER", 3, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"SMISMEMBER", -3, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"SCARD", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"SPOP", -2, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"SRANDMEMBER", -2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},

    // ── Sorted sets ──────────────────────────────────────────────────────
    {"ZADD", -4, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"ZREM", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"ZSCORE", 3, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZMSCORE", -3, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZCARD", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZRANK", -3, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZREVRANK", -3, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZINCRBY", 4, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"ZRANGE", -4, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZRANGEBYSCORE", -4, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZRANGEBYLEX", -4, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZCOUNT", 4, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"ZLEXCOUNT", 4, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},

    // ── Generic / key management ─────────────────────────────────────────
    {"DEL", -2, CommandClass::kWrite, Dispatch::kWritePath, 1, -1, 1},
    {"UNLINK", -2, CommandClass::kWrite, Dispatch::kWritePath, 1, -1, 1},
    {"EXISTS", -2, CommandClass::kRead, Dispatch::kTieredRead, 1, -1, 1},
    {"EXPIRE", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"PEXPIRE", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"EXPIREAT", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"PEXPIREAT", -3, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"PERSIST", 2, CommandClass::kWrite, Dispatch::kWritePath, 1, 1, 1},
    {"TTL", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"PTTL", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"EXPIRETIME", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"PEXPIRETIME", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"TYPE", 2, CommandClass::kRead, Dispatch::kTieredRead, 1, 1, 1},
    {"RENAME", 3, CommandClass::kWrite, Dispatch::kWritePath, 1, 2, 1},
    {"RENAMENX", 3, CommandClass::kWrite, Dispatch::kConditionalWrite, 1, 2, 1},
    {"COPY", -3, CommandClass::kWrite, Dispatch::kConditionalWrite, 1, 2, 1},
    {"OBJECT", -3, CommandClass::kRead, Dispatch::kTieredRead, 2, 2, 1},
});

std::string Uppercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
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
