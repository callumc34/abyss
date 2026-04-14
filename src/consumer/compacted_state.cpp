#include "abyss/consumer/compacted_state.h"

namespace abyss::consumer {

void CompactedState::Absorb(const core::RespCommand& /*cmd*/) {
  // Will implement merge logic: scalar last-write-wins, set merge-accumulate
}

std::vector<core::RespCommand> CompactedState::Emit() const { return {}; }

void CompactedState::Reset() {
  latest_set_.reset();
  pending_adds_.clear();
  pending_removes_.clear();
  is_tombstone_ = false;
}

}  // namespace abyss::consumer
