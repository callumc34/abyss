#pragma once

#include <string_view>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/sequencer.h"

namespace abyss::engine {

// Dispatch: every read goes through the read path, every write
// through the sequencer.
class TieringEngine : public core::CommandDispatcher {
 public:
  TieringEngine(ReadPath& reads, Sequencer& sequencer);

  core::Result<core::RespValue> DispatchRead(std::string_view name,
                                             const core::RespCommand& cmd) override;
  core::Result<core::RespValue> DispatchWrite(std::string_view name,
                                              core::RespCommand cmd) override;
  core::Result<core::RespValue> DispatchConditional(std::string_view name, core::RespCommand cmd,
                                                    core::PredicateFlags flags) override;
  core::Result<core::RespValue> DispatchFanOut(core::MultiKeyKind kind,
                                               core::RespCommand cmd) override;
  core::Result<core::RespValue> DispatchFlush(core::FlushTarget target) override;

 private:
  ReadPath& reads_;
  Sequencer& sequencer_;
};

}  // namespace abyss::engine
