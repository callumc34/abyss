#pragma once

#include <cstdint>
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

// Per-command metadata.
struct CommandSpec {
  std::string_view name;  // Canonical uppercase name (e.g. "GET", "ZADD").

  int arity;
  CommandClass cls;
  Dispatch dispatch;
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

  // Classify a parsed command.
  //
  // Returns:
  //   kNotFound            — unknown command name.
  //   kInvalidArgument     — arity mismatch.
  //   CommandSpec*         — on success.
  core::Result<const CommandSpec*> Classify(const core::RespCommand& cmd) const;

  size_t Size() const { return by_name_.size(); }

 private:
  // Map of UPPERCASE name to spec. Spec objects are backed by a process-lifetime
  // static array, so pointers into them are stable for the registry's lifetime.
  std::unordered_map<std::string, const CommandSpec*> by_name_;
};

// Process-wide registry singleton.
const CommandRegistry& GlobalRegistry();

}  // namespace abyss::resp
