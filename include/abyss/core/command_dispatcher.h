#pragma once

#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::core {

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
};

}  // namespace abyss::core
