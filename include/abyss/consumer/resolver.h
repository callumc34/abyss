#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/consumer/existence_cache.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

// Per-shard consumer that resolves Conditional queue entries. See ADP-011.
class Resolver {
 public:
  struct Config {
    core::ShardId shard = 0;
    size_t read_batch_size = 256;
    core::Duration read_timeout{100};
    std::chrono::milliseconds cold_lookup_timeout{100};
    uint32_t stripe_count = 64;
    std::chrono::milliseconds hot_apply_wait{1000};
    ExistenceCache::Config cache;
  };

  Resolver(core::Queue& queue, core::ColdStore& cold, CompactionBufferRouter& buffer_router,
           core::ConsumerRpc& rpc, core::ApplyNotifier& apply_notifier, Config config);
  ~Resolver();

  Resolver(const Resolver&) = delete;
  Resolver& operator=(const Resolver&) = delete;
  Resolver(Resolver&&) = delete;
  Resolver& operator=(Resolver&&) = delete;

  // Idempotent. Must run before Start() and before cold/hot consumers start.
  core::Result<void> ReplayForRecovery();

  void Start();
  void RequestStop();
  void Join();
  void Stop();

  bool Running() const { return running_.load(std::memory_order_acquire); }
  core::ShardId shard() const { return config_.shard; }
  ExistenceCache& Cache() { return cache_; }
  const ExistenceCache& Cache() const { return cache_; }

  struct Snapshot {
    uint64_t conditionals_resolved = 0;
    uint64_t decisions_apply = 0;
    uint64_t decisions_skip = 0;
    uint64_t cache_hits = 0;
    uint64_t buffer_hits = 0;
    uint64_t cold_hits = 0;
    uint64_t cold_timeouts = 0;
    uint64_t cold_errors = 0;
    uint64_t apply_wait_timeouts = 0;
    uint64_t append_failures = 0;
    uint64_t parse_failures = 0;
    uint64_t replayed_resolveds_emitted = 0;
    core::SequenceId latest_drained_seq = 0;
    core::SequenceId last_ack_seq = 0;
    size_t cache_entries = 0;
    size_t cache_bytes = 0;
  };

  Snapshot GetSnapshot() const;

 private:
  void Run();
  void ProcessEntry(const core::QueueEntry& entry);

  // Pure function of cache + buffer + cold + entry.appended_at — required
  // for replay determinism (ADP-011 §Decision determinism).
  core::entry::Resolved Decide(const core::QueueEntry& entry, const core::entry::Conditional& cond);

  void UpdateCacheFromResolved(core::SequenceId seq, const core::entry::Resolved& resolved);
  void UpdateCacheFromWrite(core::SequenceId seq, const core::RespCommand& cmd);
  bool WaitForHotApply(core::SequenceId seq);

  // Sorted ascending; dedupes colliding stripes for deadlock-free multi-key.
  std::vector<uint32_t> StripeIndicesFor(const std::vector<std::string_view>& keys) const;

  core::Queue& queue_;
  core::ColdStore& cold_;
  CompactionBufferRouter& buffer_router_;
  core::ConsumerRpc& rpc_;
  core::ApplyNotifier& apply_notifier_;
  Config config_;

  std::vector<std::mutex> stripes_;
  ExistenceCache cache_;

  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> running_{false};
  std::thread thread_;

  std::atomic<core::SequenceId> latest_drained_seq_{0};
  std::atomic<core::SequenceId> last_ack_seq_{0};

  std::atomic<uint64_t> conditionals_resolved_{0};
  std::atomic<uint64_t> decisions_apply_{0};
  std::atomic<uint64_t> decisions_skip_{0};
  std::atomic<uint64_t> cache_hits_{0};
  std::atomic<uint64_t> buffer_hits_{0};
  std::atomic<uint64_t> cold_hits_{0};
  std::atomic<uint64_t> cold_timeouts_{0};
  std::atomic<uint64_t> cold_errors_{0};
  std::atomic<uint64_t> apply_wait_timeouts_{0};
  std::atomic<uint64_t> append_failures_{0};
  std::atomic<uint64_t> parse_failures_{0};
  std::atomic<uint64_t> replayed_resolveds_emitted_{0};
};

}  // namespace abyss::consumer
