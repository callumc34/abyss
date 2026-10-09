#include "abyss/core/apply_notifier.h"

#include <algorithm>
#include <future>
#include <memory>
#include <utility>
#include <vector>

#include "abyss/core/fatal.h"
#include "abyss/core/types.h"

namespace abyss::core {

AppliedSeqNotifier::AppliedSeqNotifier(AppliedSeqNotifierConfig config) {
  const auto count = std::max<uint32_t>(1, config.shard_count);
  shards_.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    shards_.push_back(std::make_unique<Shard>());
  }
}

AppliedSeqNotifier::Shard& AppliedSeqNotifier::ShardFor(ShardId shard) const {
  return *shards_[shard % shards_.size()];
}

std::future<void> AppliedSeqNotifier::AwaitApplied(ShardId shard, SequenceId seq) {
  auto& s = ShardFor(shard);
  const std::scoped_lock lock(s.mu);

  // Already applied at or past seq: ready immediately.
  if (s.applied_seq.load(std::memory_order_acquire) >= seq) {
    std::promise<void> p;
    p.set_value();
    return p.get_future();
  }

  std::promise<void> p;
  auto fut = p.get_future();
  s.waiters.emplace(seq, std::move(p));
  return fut;
}

void AppliedSeqNotifier::NotifyApplied(ShardId shard, SequenceId seq) {
  ABYSS_DCHECK(seq >= kFirstSeq, "applied at seq 0, which names no entry");
  auto& s = ShardFor(shard);
  std::vector<std::promise<void>> to_fulfill;
  {
    const std::scoped_lock lock(s.mu);

    // All advances happen under the lock, so a plain monotonic max suffices.
    const auto high = std::max(s.applied_seq.load(std::memory_order_relaxed), seq);
    s.applied_seq.store(high, std::memory_order_release);

    auto end = s.waiters.upper_bound(high);
    for (auto it = s.waiters.begin(); it != end; ++it) {
      to_fulfill.push_back(std::move(it->second));
    }
    s.waiters.erase(s.waiters.begin(), end);
  }
  for (auto& p : to_fulfill) p.set_value();
}

bool AppliedSeqNotifier::Cancel(ShardId shard, SequenceId seq) {
  auto& s = ShardFor(shard);
  const std::scoped_lock lock(s.mu);
  return s.waiters.erase(seq) > 0;
}

SequenceId AppliedSeqNotifier::AppliedSeq(ShardId shard) const {
  return ShardFor(shard).applied_seq.load(std::memory_order_acquire);
}

size_t AppliedSeqNotifier::PendingCount() const {
  size_t total = 0;
  for (const auto& s : shards_) {
    const std::scoped_lock lock(s->mu);
    total += s->waiters.size();
  }
  return total;
}

}  // namespace abyss::core
