#include "abyss/engine/write_promise.h"

namespace abyss::engine {

std::future<core::Result<void>> WritePromiseMap::Register(core::SequenceId seq) {
  std::lock_guard lock(mutex_);
  auto [it, inserted] = pending_.emplace(seq, std::promise<core::Result<void>>{});
  return it->second.get_future();
}

void WritePromiseMap::Fulfill(core::SequenceId seq, core::Result<void> result) {
  std::lock_guard lock(mutex_);
  if (auto it = pending_.find(seq); it != pending_.end()) {
    it->second.set_value(std::move(result));
    pending_.erase(it);
  }
}

}  // namespace abyss::engine
