#include "abyss/core/apply_notifier.h"

#include <algorithm>
#include <future>
#include <memory>

namespace abyss::core {

ApplyNotifier::ApplyNotifier(ApplyNotifierConfig config) {
  const auto count = std::max<uint32_t>(1, config.registry_shard_count);
  shards_.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    auto shard = std::make_unique<Shard>();
    shard->recent_fired.assign(late_window_size_per_shard_, 0);
    shards_.push_back(std::move(shard));
  }
}

ApplyNotifier::Shard& ApplyNotifier::ShardFor(RpcId id) const {
  return *shards_[id % shards_.size()];
}

std::future<void> ApplyNotifier::AwaitApplied(RpcId id) {
  auto& shard = ShardFor(id);
  const std::scoped_lock lock(shard.mu);

  // Late-arrival: return ready if already fired within the recent window.
  for (auto fired : shard.recent_fired) {
    if (fired == id) {
      std::promise<void> p;
      p.set_value();
      return p.get_future();
    }
  }

  auto [it, inserted] = shard.pending.try_emplace(id);
  if (!inserted) {
    // Duplicate Await: original waiter keeps its future; duplicate gets a broken one.
    std::promise<void> broken;
    return broken.get_future();
  }
  return it->second.get_future();
}

void ApplyNotifier::NotifyApplied(RpcId id) {
  auto& shard = ShardFor(id);
  std::promise<void> to_fulfill;
  bool fulfill = false;
  {
    const std::scoped_lock lock(shard.mu);
    auto it = shard.pending.find(id);
    if (it != shard.pending.end()) {
      to_fulfill = std::move(it->second);
      shard.pending.erase(it);
      fulfill = true;
    }
    shard.recent_fired[shard.recent_cursor] = id;
    shard.recent_cursor = (shard.recent_cursor + 1) % shard.recent_fired.size();
  }
  if (fulfill) to_fulfill.set_value();
}

bool ApplyNotifier::Cancel(RpcId id) {
  auto& shard = ShardFor(id);
  const std::scoped_lock lock(shard.mu);
  return shard.pending.erase(id) > 0;
}

size_t ApplyNotifier::PendingCount() const {
  size_t total = 0;
  for (const auto& shard : shards_) {
    const std::scoped_lock lock(shard->mu);
    total += shard->pending.size();
  }
  return total;
}

}  // namespace abyss::core
