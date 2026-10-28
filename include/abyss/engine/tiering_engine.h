#pragma once

#include <memory>

#include "abyss/core/command_dispatcher.h"
#include "abyss/engine/command_task.h"
#include "abyss/engine/io_pool.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/sequencer.h"

namespace abyss::engine {

// Dispatch: every read goes through the read path, every write
// through the sequencer, and every command gets the sequencer's
// command timeout as its deadline. A hit, and a write that needs no
// wait, reply on the caller's thread; a miss and a write's loads go
// to the I/O pool.
class TieringEngine : public core::CommandDispatcher {
 public:
  TieringEngine(ReadPath& reads, Sequencer& sequencer, IoPool& pool);

  core::Outcome Dispatch(core::Request request, core::Slot& slot,
                         std::unique_ptr<core::Task>& task) override;

 private:
  core::Outcome FanOut(core::Request request, core::Slot& slot, std::unique_ptr<core::Task>& task);
  CommandTask& TaskOf(std::unique_ptr<core::Task>& task);

  ReadPath& reads_;
  Sequencer& sequencer_;
  IoPool& pool_;
};

}  // namespace abyss::engine
