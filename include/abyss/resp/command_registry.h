#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::resp {

// What the command does to state.
enum class CommandClass : uint8_t {
  kRead,
  kWrite,
  kAdmin,
};

// How the frontend executes the command.
enum class Dispatch : uint8_t {
  kStateless,
  kTieredRead,
  kWritePath,
  kConditionalWrite,
  kConsumerRpc,
};

struct CommandDocs {
  std::string_view summary;
  std::string_view since;
  std::string_view group;
  std::string_view complexity;
};

struct CommandSpec {
  std::string_view name;  // Canonical uppercase name (e.g. "GET", "ZADD").

  int arity;
  CommandClass cls;
  Dispatch dispatch;

  // Key position metadata.
  int first_key = 0;  // 0 = no keys, 1 = args[1] is first key
  int last_key = 0;   // 0 = same as first_key, -1 = last arg is a key
  int key_step = 1;   // step between keys (2 for MSET key val key val)

  // Subcommand handlers may narrow this further during loading.
  bool loading_safe = false;

  CommandDocs docs;
};

// Data-driven command table loaded from a static list at construction time.
class CommandRegistry {
 public:
  CommandRegistry();
  ~CommandRegistry() = default;

  CommandRegistry(const CommandRegistry&) = delete;
  CommandRegistry& operator=(const CommandRegistry&) = delete;
  CommandRegistry(CommandRegistry&&) = delete;
  CommandRegistry& operator=(CommandRegistry&&) = delete;

  // Case-insensitive lookup. Returns nullptr if the name is not registered.
  const CommandSpec* Find(std::string_view name) const;

  // kNotFound on unknown name, kInvalidArgument on arity mismatch.
  core::Result<const CommandSpec*> Classify(const core::RespCommand& cmd) const;

  size_t Size() const { return by_name_.size(); }

  std::span<const CommandSpec> All() const;

 private:
  // Values point into a process-lifetime static table; lookup-stable.
  std::unordered_map<std::string, const CommandSpec*> by_name_;
};

// Process-wide registry singleton.
const CommandRegistry& GlobalRegistry();

}  // namespace abyss::resp
