#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/resp_types.h"
#include "abyss/resp/predicate_extractor.h"

namespace abyss::resp {

enum class CommandClass : uint8_t {
  kRead,
  kWrite,
  kAdmin,
};

enum class Dispatch : uint8_t {
  kStateless,
  kTieredRead,
  kWritePath,
  kConditionalWrite,
  kConsumerRpc,
  kFlush,
};

struct CommandDocs {
  std::string_view summary;
  std::string_view since;
  std::string_view group;
  std::string_view complexity;
};

// Arity is the total RESP arg count including parent and sub tokens
// (`CLUSTER KEYSLOT foo` is arity 3).
struct SubcommandSpec {
  std::string_view name;
  int arity;
  Dispatch dispatch;
  bool loading_safe = false;
  CommandDocs docs{};
};

struct CommandSpec {
  std::string_view name;
  int arity;
  CommandClass cls;
  Dispatch dispatch;

  // Key positions apply to the parent only; per-subcommand keys aren't modelled.
  int first_key = 0;
  int last_key = 0;
  int key_step = 1;

  // For container commands the subcommand's loading_safe overrides this.
  bool loading_safe = false;

  // Non-kNone signals the frontend to route through DispatchFanOut instead of
  // DispatchRead/DispatchWrite. The kind picks the aggregator (array vs sum vs
  // OK). Decomposition itself lives in the engine.
  core::MultiKeyKind multi_key_kind = core::MultiKeyKind::kNone;

  CommandDocs docs{};

  std::span<const SubcommandSpec> subcommands{};

  // nullptr if the command is never conditional. See ADP-011.
  PredicateExtractor predicate = nullptr;
};

// `subcommand` is null when the parent applies directly.
struct ResolvedCommand {
  const CommandSpec* parent;
  const SubcommandSpec* subcommand;

  int Arity() const { return subcommand != nullptr ? subcommand->arity : parent->arity; }
  Dispatch DispatchClass() const {
    return subcommand != nullptr ? subcommand->dispatch : parent->dispatch;
  }
  bool LoadingSafe() const {
    return subcommand != nullptr ? subcommand->loading_safe : parent->loading_safe;
  }
};

class CommandRegistry {
 public:
  CommandRegistry();
  ~CommandRegistry() = default;

  CommandRegistry(const CommandRegistry&) = delete;
  CommandRegistry& operator=(const CommandRegistry&) = delete;
  CommandRegistry(CommandRegistry&&) = delete;
  CommandRegistry& operator=(CommandRegistry&&) = delete;

  // Case-insensitive; returns nullptr if absent.
  const CommandSpec* Find(std::string_view name) const;
  const SubcommandSpec* FindSubcommand(const CommandSpec& parent, std::string_view name) const;

  enum class ResolveStatus : uint8_t {
    kOk,
    kUnknownCommand,
    kArityMismatch,
    kUnknownSubcommand,
  };

  struct ResolveResult {
    ResolveStatus status;
    ResolvedCommand resolved;
  };

  ResolveResult Resolve(const core::RespCommand& cmd) const;

  size_t Size() const { return by_name_.size(); }
  std::span<const CommandSpec> All() const;

 private:
  std::unordered_map<std::string, const CommandSpec*> by_name_;
};

const CommandRegistry& GlobalRegistry();

}  // namespace abyss::resp
