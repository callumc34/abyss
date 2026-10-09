#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct AppliedSeqNotifierConfig {
  uint32_t shard_count = 1;
};

// Hot signals "applied at seq"; the resolver awaits before fulfilling the
// client RPC. Required for read-your-write on conditional writes.
//
// Per shard, the applied seq is a monotonic high-water (CAS-max advanced), not
// a bounded recent-fired ring: a fired seq is remembered forever, so a late
// AwaitApplied(shard, seq) whose seq is at or below the high-water resolves
// immediately no matter how many notifies followed it. Keyed by (shard, seq)
// rather than a packed RpcId.
class AppliedSeqNotifier {
 public:
  explicit AppliedSeqNotifier(AppliedSeqNotifierConfig config = {});
  ~AppliedSeqNotifier() = default;

  AppliedSeqNotifier(const AppliedSeqNotifier&) = delete;
  AppliedSeqNotifier& operator=(const AppliedSeqNotifier&) = delete;
  AppliedSeqNotifier(AppliedSeqNotifier&&) = delete;
  AppliedSeqNotifier& operator=(AppliedSeqNotifier&&) = delete;

  // Ready iff applied_seq >= seq, else returns a future fulfilled by a
  // later NotifyApplied(shard, s) with s >= seq. applied_seq is 0 until
  // something applies, so an await of 0, which names no entry, is ready.
  std::future<void> AwaitApplied(ShardId shard, SequenceId seq);
  void NotifyApplied(ShardId shard, SequenceId seq);
  bool Cancel(ShardId shard, SequenceId seq);

  SequenceId AppliedSeq(ShardId shard) const;
  size_t PendingCount() const;

 private:
  struct Shard {
    std::atomic<SequenceId> applied_seq{0};
    mutable std::mutex mu;
    std::multimap<SequenceId, std::promise<void>> waiters ABYSS_GUARDED_BY(mu);
  };

  Shard& ShardFor(ShardId shard) const;

  std::vector<std::unique_ptr<Shard>> shards_;
};

// Transitional alias: pool/server forwarding sites still spell the old name.
using ApplyNotifier = AppliedSeqNotifier;

}  // namespace abyss::core
