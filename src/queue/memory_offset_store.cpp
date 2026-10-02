#include "abyss/queue/memory_offset_store.h"

namespace abyss::queue {

std::optional<core::SequenceId> MemoryOffsetStore::Get(core::ConsumerId consumer,
                                                       core::ShardId shard) const {
  const std::scoped_lock lock(mu_);
  auto it = offsets_.find(Key(consumer, shard));
  if (it == offsets_.end()) return std::nullopt;
  return it->second;
}

core::Result<void> MemoryOffsetStore::Set(core::ConsumerId consumer, core::ShardId shard,
                                          core::SequenceId seq) {
  const std::scoped_lock lock(mu_);
  offsets_[Key(consumer, shard)] = seq;
  return {};
}

}  // namespace abyss::queue
