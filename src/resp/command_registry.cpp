#include "abyss/resp/command_registry.h"

#include <array>
#include <cstddef>
#include <span>
#include <string>

#include "abyss/core/ascii.h"

namespace abyss::resp {
namespace {

using C = CommandClass;
using D = Dispatch;

constexpr std::array<SubcommandSpec, 4> kClientSubs{{
    {.name = "ID",
     .arity = 2,
     .dispatch = D::kStateless,
     .docs = {.summary = "Returns the connection's client ID.",
              .since = "5.0.0",
              .group = "connection",
              .complexity = "O(1)"}},
    {.name = "GETNAME",
     .arity = 2,
     .dispatch = D::kStateless,
     .docs = {.summary = "Returns the connection's name.",
              .since = "2.6.9",
              .group = "connection",
              .complexity = "O(1)"}},
    {.name = "SETNAME",
     .arity = 3,
     .dispatch = D::kStateless,
     .docs = {.summary = "Sets the connection's name.",
              .since = "2.6.9",
              .group = "connection",
              .complexity = "O(1)"}},
    {.name = "NO-EVICT",
     .arity = 3,
     .dispatch = D::kStateless,
     .docs = {.summary = "Records the connection's no-evict preference.",
              .since = "7.0.0",
              .group = "connection",
              .complexity = "O(1)"}},
}};

constexpr std::array<SubcommandSpec, 7> kClusterSubs{{
    {.name = "SLOTS",
     .arity = 2,
     .dispatch = D::kStateless,
     .loading_safe = true,
     .docs = {.summary = "Returns the cluster slot map.",
              .since = "3.0.0",
              .group = "cluster",
              .complexity = "O(N) over slot ranges"}},
    {.name = "SHARDS",
     .arity = 2,
     .dispatch = D::kStateless,
     .docs = {.summary = "Returns the cluster shard descriptors.",
              .since = "7.0.0",
              .group = "cluster",
              .complexity = "O(N) over shards"}},
    {.name = "NODES",
     .arity = 2,
     .dispatch = D::kStateless,
     .docs = {.summary = "Returns the cluster nodes table.",
              .since = "3.0.0",
              .group = "cluster",
              .complexity = "O(N) over nodes"}},
    {.name = "INFO",
     .arity = 2,
     .dispatch = D::kStateless,
     .loading_safe = true,
     .docs = {.summary = "Returns cluster state information.",
              .since = "3.0.0",
              .group = "cluster",
              .complexity = "O(1)"}},
    {.name = "MYID",
     .arity = 2,
     .dispatch = D::kStateless,
     .loading_safe = true,
     .docs = {.summary = "Returns this node's UUID.",
              .since = "3.0.0",
              .group = "cluster",
              .complexity = "O(1)"}},
    {.name = "KEYSLOT",
     .arity = 3,
     .dispatch = D::kStateless,
     .docs = {.summary = "Returns the slot a key hashes to.",
              .since = "3.0.0",
              .group = "cluster",
              .complexity = "O(N) in key length"}},
    {.name = "COUNTKEYSINSLOT",
     .arity = 3,
     .dispatch = D::kStateless,
     .docs = {.summary = "Returns the count of keys in a slot.",
              .since = "3.0.0",
              .group = "cluster",
              .complexity = "O(1)"}},
}};

constexpr std::array<SubcommandSpec, 1> kConfigSubs{{
    {.name = "GET",
     .arity = 3,
     .dispatch = D::kStateless,
     .docs = {.summary = "Returns config values matching a glob pattern.",
              .since = "2.0.0",
              .group = "server",
              .complexity = "O(N) over config entries"}},
}};

constexpr std::array<SubcommandSpec, 3> kCommandSubs{{
    {.name = "COUNT",
     .arity = 2,
     .dispatch = D::kStateless,
     .loading_safe = true,
     .docs = {.summary = "Returns the number of registered commands.",
              .since = "2.8.13",
              .group = "server",
              .complexity = "O(1)"}},
    {.name = "INFO",
     .arity = -2,
     .dispatch = D::kStateless,
     .loading_safe = true,
     .docs = {.summary = "Returns metadata for the requested commands.",
              .since = "2.8.13",
              .group = "server",
              .complexity = "O(N) over commands requested"}},
    {.name = "DOCS",
     .arity = -2,
     .dispatch = D::kStateless,
     .loading_safe = true,
     .docs = {.summary = "Returns documentation for the requested commands.",
              .since = "7.0.0",
              .group = "server",
              .complexity = "O(N) over commands requested"}},
}};

constexpr std::array<SubcommandSpec, 2> kObjectSubs{{
    {.name = "ENCODING",
     .arity = 3,
     .dispatch = D::kTieredRead,
     .docs = {.summary = "Returns the internal encoding of a key's value.",
              .since = "2.2.3",
              .group = "generic",
              .complexity = "O(1)"}},
    {.name = "IDLETIME",
     .arity = 3,
     .dispatch = D::kConsumerRpc,
     .docs = {.summary = "Returns the seconds since the key was last accessed.",
              .since = "2.2.3",
              .group = "generic",
              .complexity = "O(1)"}},
}};

constexpr CommandSpec Admin(std::string_view name, int arity, Dispatch dispatch, bool loading_safe,
                            CommandDocs docs, std::span<const SubcommandSpec> subs = {}) {
  return {.name = name,
          .arity = arity,
          .cls = C::kAdmin,
          .dispatch = dispatch,
          .first_key = 0,
          .last_key = 0,
          .key_step = 1,
          .loading_safe = loading_safe,
          .docs = docs,
          .subcommands = subs};
}

constexpr CommandSpec Read(std::string_view name, int arity, int first_key, int last_key,
                           int key_step, CommandDocs docs,
                           std::span<const SubcommandSpec> subs = {}) {
  return {.name = name,
          .arity = arity,
          .cls = C::kRead,
          .dispatch = D::kTieredRead,
          .first_key = first_key,
          .last_key = last_key,
          .key_step = key_step,
          .loading_safe = false,
          .docs = docs,
          .subcommands = subs};
}

constexpr CommandSpec Write(std::string_view name, int arity, Dispatch dispatch, int first_key,
                            int last_key, int key_step, CommandDocs docs,
                            PredicateExtractor predicate = nullptr) {
  return {.name = name,
          .arity = arity,
          .cls = C::kWrite,
          .dispatch = dispatch,
          .first_key = first_key,
          .last_key = last_key,
          .key_step = key_step,
          .loading_safe = false,
          .docs = docs,
          .predicate = predicate};
}

constexpr auto kCommandTable = std::to_array<CommandSpec>({
    // Admin — stateless
    Admin("PING", -1, D::kStateless, true,
          {"Returns the server's liveliness response.", "1.0.0", "connection", "O(1)"}),
    Admin("ECHO", 2, D::kStateless, false,
          {"Returns the given string.", "1.0.0", "connection", "O(1)"}),
    Admin("QUIT", 1, D::kStateless, true,
          {"Closes the connection.", "1.0.0", "connection", "O(1)"}),
    Admin("HELLO", -1, D::kStateless, true,
          {"Handshakes with the server and negotiates protocol version.", "6.0.0", "connection",
           "O(1)"}),
    Admin("CLIENT", -2, D::kStateless, false,
          {"A container for client-connection commands.", "2.4.0", "connection",
           "Depends on subcommand."},
          kClientSubs),
    Admin("RESET", 1, D::kStateless, false,
          {"Resets the connection.", "6.2.0", "connection", "O(1)"}),
    Admin("TIME", 1, D::kStateless, false, {"Returns the server time.", "2.6.0", "server", "O(1)"}),
    Admin("COMMAND", -1, D::kStateless, true,
          {"A container for command metadata inspection.", "2.8.13", "server",
           "Depends on subcommand."},
          kCommandSubs),
    Admin("CONFIG", -3, D::kStateless, false,
          {"A container for server configuration.", "2.0.0", "server", "Depends on subcommand."},
          kConfigSubs),
    Admin("INFO", -1, D::kStateless, true,
          {"Returns information and statistics about the server.", "1.0.0", "server", "O(1)"}),
    Admin("CLUSTER", -2, D::kStateless, false,
          {"A container for cluster-introspection commands.", "3.0.0", "cluster",
           "Depends on subcommand."},
          kClusterSubs),

    // Admin — consumer RPC
    Admin("DBSIZE", 1, D::kConsumerRpc, false,
          {"Returns the number of keys in the database.", "1.0.0", "server", "O(1)"}),

    // Strings
    Read("GET", 2, 1, 1, 1, {"Returns the string value of a key.", "1.0.0", "string", "O(1)"}),
    Write("SET", -3, D::kWritePath, 1, 1, 1,
          {"Sets the string value of a key. Supports TTL and conditional options.", "1.0.0",
           "string", "O(1)"},
          ExtractSetFlags),
    Write("SETNX", 3, D::kConditionalWrite, 1, 1, 1,
          {"Sets a string value only if the key does not exist.", "1.0.0", "string", "O(1)"},
          ExtractSetNxFlags),
    Write("SETEX", 4, D::kWritePath, 1, 1, 1,
          {"Sets the value and expiration in seconds.", "2.0.0", "string", "O(1)"}),
    Write("PSETEX", 4, D::kWritePath, 1, 1, 1,
          {"Sets the value and expiration in milliseconds.", "2.6.0", "string", "O(1)"}),
    Write("GETSET", 3, D::kWritePath, 1, 1, 1,
          {"Sets a new value and returns the prior one.", "1.0.0", "string", "O(1)"}),
    Write("GETDEL", 2, D::kWritePath, 1, 1, 1,
          {"Returns the value and deletes the key.", "6.2.0", "string", "O(1)"}),
    Write("APPEND", 3, D::kWritePath, 1, 1, 1,
          {"Appends a value to the key; creates the key if absent.", "2.0.0", "string",
           "O(1) amortised"}),
    Read("STRLEN", 2, 1, 1, 1,
         {"Returns the length of the string value of a key.", "2.2.0", "string", "O(1)"}),
    Write("INCR", 2, D::kWritePath, 1, 1, 1,
          {"Increments the integer value of a key by one.", "1.0.0", "string", "O(1)"}),
    Write("DECR", 2, D::kWritePath, 1, 1, 1,
          {"Decrements the integer value of a key by one.", "1.0.0", "string", "O(1)"}),
    Write("INCRBY", 3, D::kWritePath, 1, 1, 1,
          {"Increments the integer value of a key by a given amount.", "1.0.0", "string", "O(1)"}),
    Write("DECRBY", 3, D::kWritePath, 1, 1, 1,
          {"Decrements the integer value of a key by a given amount.", "1.0.0", "string", "O(1)"}),
    Write("INCRBYFLOAT", 3, D::kWritePath, 1, 1, 1,
          {"Increments the float value of a key by a given amount.", "2.6.0", "string", "O(1)"}),
    Read("MGET", -2, 1, -1, 1,
         {"Returns the values of multiple keys.", "1.0.0", "string", "O(N) over keys requested"}),
    Write("MSET", -3, D::kWritePath, 1, -1, 2,
          {"Sets the values of multiple keys.", "1.0.1", "string", "O(N) over keys set"}),
    Write("MSETNX", -3, D::kConditionalWrite, 1, -1, 2,
          {"Atomically sets multiple keys only if none exist.", "1.0.1", "string",
           "O(N) over keys set"},
          ExtractMsetNxFlags),

    // Sets
    Write("SADD", -3, D::kWritePath, 1, 1, 1,
          {"Adds members to a set.", "1.0.0", "set", "O(N) over members added"}),
    Write("SREM", -3, D::kWritePath, 1, 1, 1,
          {"Removes members from a set.", "1.0.0", "set", "O(N) over members removed"}),
    Read("SMEMBERS", 2, 1, 1, 1,
         {"Returns all members of a set.", "1.0.0", "set", "O(N) over members"}),
    Read("SISMEMBER", 3, 1, 1, 1,
         {"Tests whether a value is a member of a set.", "1.0.0", "set", "O(1)"}),
    Read("SMISMEMBER", -3, 1, 1, 1,
         {"Tests whether each value is a member of a set.", "6.2.0", "set", "O(N) over arguments"}),
    Read("SCARD", 2, 1, 1, 1, {"Returns the number of members of a set.", "1.0.0", "set", "O(1)"}),
    Write("SPOP", -2, D::kWritePath, 1, 1, 1,
          {"Removes and returns one or more random members of a set.", "1.0.0", "set",
           "O(N) for count"}),
    Read("SRANDMEMBER", -2, 1, 1, 1,
         {"Returns one or more random members of a set.", "1.0.0", "set", "O(N) for count"}),

    // Sorted sets
    Write("ZADD", -4, D::kWritePath, 1, 1, 1,
          {"Adds members to a sorted set or updates their scores.", "1.2.0", "sorted-set",
           "O(log N) per member"},
          ExtractZAddFlags),
    Write("ZREM", -3, D::kWritePath, 1, 1, 1,
          {"Removes members from a sorted set.", "1.2.0", "sorted-set", "O(log N) per member"}),
    Read("ZSCORE", 3, 1, 1, 1,
         {"Returns the score of a member in a sorted set.", "1.2.0", "sorted-set", "O(1)"}),
    Read("ZMSCORE", -3, 1, 1, 1,
         {"Returns the scores of multiple members in a sorted set.", "6.2.0", "sorted-set",
          "O(N) over members requested"}),
    Read("ZCARD", 2, 1, 1, 1,
         {"Returns the number of members of a sorted set.", "1.2.0", "sorted-set", "O(1)"}),
    Read("ZRANK", -3, 1, 1, 1,
         {"Returns the index of a member in a sorted set.", "2.0.0", "sorted-set", "O(log N)"}),
    Read("ZREVRANK", -3, 1, 1, 1,
         {"Returns the reverse index of a member in a sorted set.", "2.0.0", "sorted-set",
          "O(log N)"}),
    Write("ZINCRBY", 4, D::kWritePath, 1, 1, 1,
          {"Increments a member's score in a sorted set.", "1.2.0", "sorted-set", "O(log N)"}),
    Read("ZRANGE", -4, 1, 1, 1,
         {"Returns members of a sorted set by range.", "1.2.0", "sorted-set", "O(log N + M)"}),
    Read(
        "ZRANGEBYSCORE", -4, 1, 1, 1,
        {"Returns members of a sorted set by score range.", "1.0.5", "sorted-set", "O(log N + M)"}),
    Read("ZRANGEBYLEX", -4, 1, 1, 1,
         {"Returns members of a sorted set by lex range.", "2.8.9", "sorted-set", "O(log N + M)"}),
    Read("ZCOUNT", 4, 1, 1, 1,
         {"Counts members of a sorted set in a score range.", "2.0.0", "sorted-set", "O(log N)"}),
    Read("ZLEXCOUNT", 4, 1, 1, 1,
         {"Counts members of a sorted set in a lex range.", "2.8.9", "sorted-set", "O(log N)"}),

    // Hashes
    Write("HSET", -4, D::kWritePath, 1, 1, 1,
          {"Sets fields in a hash.", "2.0.0", "hash", "O(N) over fields set"}),
    Write("HDEL", -3, D::kWritePath, 1, 1, 1,
          {"Deletes fields from a hash.", "2.0.0", "hash", "O(N) over fields deleted"}),
    Read("HGET", 3, 1, 1, 1, {"Returns the value of a hash field.", "2.0.0", "hash", "O(1)"}),
    Read("HGETALL", 2, 1, 1, 1,
         {"Returns all fields and values of a hash.", "2.0.0", "hash", "O(N) over fields"}),

    // Generic / key management
    Write("DEL", -2, D::kWritePath, 1, -1, 1,
          {"Deletes one or more keys.", "1.0.0", "generic", "O(N) over keys deleted"}),
    Write("UNLINK", -2, D::kWritePath, 1, -1, 1,
          {"Deletes one or more keys; treated as a synonym of DEL.", "4.0.0", "generic",
           "O(N) over keys deleted"}),
    Read("EXISTS", -2, 1, -1, 1,
         {"Tests whether keys exist.", "1.0.0", "generic", "O(N) over keys requested"}),
    Write("EXPIRE", -3, D::kWritePath, 1, 1, 1,
          {"Sets a key's time-to-live in seconds.", "1.0.0", "generic", "O(1)"},
          ExtractExpireFlags),
    Write("PEXPIRE", -3, D::kWritePath, 1, 1, 1,
          {"Sets a key's time-to-live in milliseconds.", "2.6.0", "generic", "O(1)"},
          ExtractExpireFlags),
    Write("EXPIREAT", -3, D::kWritePath, 1, 1, 1,
          {"Sets the expiration of a key to a Unix timestamp.", "1.2.0", "generic", "O(1)"},
          ExtractExpireFlags),
    Write("PEXPIREAT", -3, D::kWritePath, 1, 1, 1,
          {"Sets the expiration of a key to a Unix millisecond timestamp.", "2.6.0", "generic",
           "O(1)"},
          ExtractExpireFlags),
    Write("PERSIST", 2, D::kWritePath, 1, 1, 1,
          {"Removes the expiration from a key.", "2.2.0", "generic", "O(1)"}),
    Read("TTL", 2, 1, 1, 1,
         {"Returns a key's time-to-live in seconds.", "1.0.0", "generic", "O(1)"}),
    Read("PTTL", 2, 1, 1, 1,
         {"Returns a key's time-to-live in milliseconds.", "2.6.0", "generic", "O(1)"}),
    Read("EXPIRETIME", 2, 1, 1, 1,
         {"Returns the expiration Unix timestamp of a key.", "7.0.0", "generic", "O(1)"}),
    Read("PEXPIRETIME", 2, 1, 1, 1,
         {"Returns the expiration Unix millisecond timestamp of a key.", "7.0.0", "generic",
          "O(1)"}),
    Read("TYPE", 2, 1, 1, 1, {"Returns the type of a key.", "1.0.0", "generic", "O(1)"}),
    Write("RENAME", 3, D::kWritePath, 1, 2, 1, {"Renames a key.", "1.0.0", "generic", "O(1)"}),
    Write("RENAMENX", 3, D::kConditionalWrite, 1, 2, 1,
          {"Renames a key only if the destination does not exist.", "1.0.0", "generic", "O(1)"},
          ExtractRenameNxFlags),
    Write("COPY", -3, D::kConditionalWrite, 1, 2, 1,
          {"Copies the value of a key to another key.", "6.2.0", "generic", "O(N)"},
          ExtractCopyFlags),
    Write("HSETNX", 4, D::kConditionalWrite, 1, 1, 1,
          {"Sets a hash field only if the field does not already exist.", "2.0.0", "hash", "O(1)"},
          ExtractHsetNxFlags),
    Read("OBJECT", -3, 2, 2, 1,
         {"A container for object-introspection commands.", "2.2.3", "generic",
          "Depends on subcommand."},
         kObjectSubs),
});

bool ArityMatches(int arity, size_t arg_count) {
  const auto arg_count_int = static_cast<int>(arg_count);
  if (arity >= 0) return arg_count_int == arity;
  return arg_count_int >= -arity;
}

}  // namespace

// NOLINTNEXTLINE(modernize-use-equals-default)
CommandRegistry::CommandRegistry() {
  by_name_.reserve(kCommandTable.size());
  for (const auto& spec : kCommandTable) {
    by_name_.emplace(std::string(spec.name), &spec);
  }
}

const CommandSpec* CommandRegistry::Find(std::string_view name) const {
  auto upper = core::AsciiUpper(name);
  auto it = by_name_.find(upper);
  if (it == by_name_.end()) {
    return nullptr;
  }
  return it->second;
}

const SubcommandSpec* CommandRegistry::FindSubcommand(const CommandSpec& parent,
                                                      std::string_view name) const {
  if (parent.subcommands.empty()) return nullptr;
  const auto upper = core::AsciiUpper(name);
  for (const auto& sub : parent.subcommands) {
    if (sub.name == upper) return &sub;
  }
  return nullptr;
}

CommandRegistry::ResolveResult CommandRegistry::Resolve(const core::RespCommand& cmd) const {
  if (cmd.ArgCount() == 0) {
    return {.status = ResolveStatus::kUnknownCommand, .resolved = {nullptr, nullptr}};
  }
  const auto* parent = Find(cmd.Name());
  if (parent == nullptr) {
    return {.status = ResolveStatus::kUnknownCommand, .resolved = {nullptr, nullptr}};
  }

  // Enforce parent arity first; failure here matches Redis's "wrong number of
  // arguments for '<cmd>' command" path.
  if (!ArityMatches(parent->arity, cmd.ArgCount())) {
    return {.status = ResolveStatus::kArityMismatch, .resolved = {parent, nullptr}};
  }

  if (!parent->subcommands.empty() && cmd.ArgCount() >= 2) {
    const auto* sub = FindSubcommand(*parent, cmd.args[1]);
    if (sub == nullptr) {
      return {.status = ResolveStatus::kUnknownSubcommand, .resolved = {parent, nullptr}};
    }
    if (!ArityMatches(sub->arity, cmd.ArgCount())) {
      return {.status = ResolveStatus::kArityMismatch, .resolved = {parent, sub}};
    }
    return {.status = ResolveStatus::kOk, .resolved = {parent, sub}};
  }

  return {.status = ResolveStatus::kOk, .resolved = {parent, nullptr}};
}

std::span<const CommandSpec> CommandRegistry::All() const { return kCommandTable; }

const CommandRegistry& GlobalRegistry() {
  static const CommandRegistry registry;
  return registry;
}

}  // namespace abyss::resp
