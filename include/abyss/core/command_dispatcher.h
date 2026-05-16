#pragma once

#include <cstdint>

#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::core {

// Identifies multi-key Redis commands that the engine decomposes into per-key
// sub-commands before queueing. The dispatch class (read vs write) is still
// carried by the registry; this is the orthogonal aggregator selector.
enum class MultiKeyKind : uint8_t {
  kNone,
  kMget,
  kExists,
  kMset,
  kDelete,
};

class CommandDispatcher {
 public:
  CommandDispatcher() = default;
  virtual ~CommandDispatcher() = default;
  CommandDispatcher(const CommandDispatcher&) = delete;
  CommandDispatcher& operator=(const CommandDispatcher&) = delete;
  CommandDispatcher(CommandDispatcher&&) = delete;
  CommandDispatcher& operator=(CommandDispatcher&&) = delete;

  virtual Result<RespValue> DispatchRead(std::string_view name, const RespCommand& cmd) = 0;
  virtual Result<RespValue> DispatchWrite(std::string_view name, RespCommand cmd) = 0;
  virtual Result<RespValue> DispatchConditional(std::string_view name, RespCommand cmd,
                                                PredicateFlags flags) = 0;
  virtual Result<RespValue> DispatchFanOut(MultiKeyKind kind, RespCommand cmd) = 0;
};

}  // namespace abyss::core
