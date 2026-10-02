#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
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
    // Used by ReplayForRecovery(); larger than read_batch_size to amortise
    // queue reads while scanning a long catch-up backlog.
    size_t replay_batch_size = 5000;
    core::Duration read_timeout{100};
    std::chrono::milliseconds cold_lookup_timeout{100};
    uint32_t stripe_count = 64;
    std::chrono::milliseconds hot_apply_wait{1000};
    // Bound on the wait for a self-emitted Resolved's WAL fsync before the
    // client conditional ack is fulfilled. Mirrors the write path's
    // write_timeout so the conditional fulfil path has the same durability
    // latency budget as DispatchConditional. On timeout the client gets an
    // error (never the success value); the Resolved stays durable in the WAL
    // and applies on catch-up. The hot-apply wait that follows uses
    // hot_apply_wait.
    std::chrono::milliseconds durable_wait_timeout{5000};
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
  // If `cancel` flips true mid-replay, the call returns kUnavailable; partial
  // progress is still committed and every call rescans from the committed
  // offset, so a subsequent invocation resumes cleanly.
  core::Result<void> ReplayForRecovery(const std::atomic<bool>& cancel);

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
    uint64_t durable_wait_timeouts = 0;
    uint64_t append_failures = 0;
    uint64_t commit_failures = 0;
    uint64_t parse_failures = 0;
    uint64_t replayed_resolveds_emitted = 0;
    uint64_t flushes_observed = 0;
    uint64_t flush_skip_resolveds_emitted = 0;
    core::SequenceId latest_drained_seq = 0;
    core::SequenceId last_commit_seq = 0;
    core::SequenceId resolver_durable_floor = 0;
    core::SequenceId latest_flush_seq = 0;
    size_t cache_entries = 0;
    size_t cache_bytes = 0;
  };

  Snapshot GetSnapshot() const;

 private:
  void Run();
  // Points the read cursor just past the committed offset.
  core::Result<void> SeedCursor();
  // Commits `seq`; on success it becomes committed_.
  void Commit(core::SequenceId seq);
  // The reaper deleted entries above the persisted offset: fail-stop.
  [[noreturn]] void FailOutOfRange(core::SequenceId requested);
  // False when a Conditional's Resolved append failed: retry the entry.
  [[nodiscard]] bool ProcessEntry(const core::QueueEntry& entry);
  void HandleFlush(const core::QueueEntry& entry);

  // Pure function of cache + buffer + cold + entry.appended_at — required
  // for replay determinism (ADP-011 §Decision determinism).
  core::entry::Resolved Decide(const core::QueueEntry& entry, const core::entry::Conditional& cond);

  void UpdateCacheFromResolved(core::SequenceId seq, core::WallTime appended_at,
                               const core::entry::Resolved& resolved);
  // The single deterministic cache-apply path (A3). Parses `cmd` through the
  // canonical core::ops::ParseWriteOp with wall_now = WallMs(appended_at) — the
  // same clock the hot store uses — so the existence cache is a pure function
  // of (entry bytes, appended_at) and replay reproduces every decision
  // bit-for-bit. Never reads WallClock::now().
  void ApplyToCache(core::SequenceId seq, core::WallTime appended_at, const core::RespCommand& cmd);
  bool WaitForHotApply(core::SequenceId seq, std::chrono::milliseconds timeout);
  // Blocks until the self-emitted Resolved at `resolved_seq` is fsynced on this
  // shard, or `timeout` elapses. Returns true iff durable. The single idiom
  // shared by the steady-state Fulfill path and the recovery barrier: a
  // Resolver-emitted Resolved is treated as durable ONLY after its WAL fsync
  // confirms, never on publish. On timeout/error increments
  // durable_wait_timeouts_.
  bool AwaitResolvedDurable(core::SequenceId resolved_seq, std::chrono::milliseconds timeout);

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
  // Wakes the append-retry backoff on RequestStop.
  std::mutex stop_mu_;
  std::condition_variable stop_cv_;
  std::atomic<bool> running_{false};
  std::thread thread_;

  // Replay or Run thread state, never both at once: the next seq to read and
  // the last committed offset.
  bool cursor_seeded_ = false;
  core::SequenceId next_read_seq_ = 0;
  std::optional<core::SequenceId> committed_;

  std::atomic<core::SequenceId> latest_drained_seq_{0};
  // Mirror of committed_ for GetSnapshot(); 0 when nothing is committed.
  std::atomic<core::SequenceId> last_commit_seq_{0};
  // High-watermark (Kafka HW vs LEO): the highest Conditional seq X such
  // that every Resolved this resolver emitted for Conditionals <= X is
  // confirmed fsynced. The committed offset is clamped to this so a
  // Conditional is never committed past until its emitted Resolved is
  // durable (XDUR-2). `highest_emitted_resolved_seq_` is the durability
  // target the floor advances behind: the max Resolved seq emitted for any
  // drained Conditional.
  std::atomic<core::SequenceId> resolver_durable_floor_{0};
  std::atomic<core::SequenceId> highest_emitted_resolved_seq_{0};
  // Highest seq of an observed `entry::Flush`. During replay, gates the
  // cache-only lookup path for post-Flush danglings (cold replay runs after
  // resolver replay, so cold is still pre-Flush).
  std::atomic<core::SequenceId> latest_flush_seq_{0};
  std::atomic<bool> replay_mode_{false};

  std::atomic<uint64_t> conditionals_resolved_{0};
  std::atomic<uint64_t> decisions_apply_{0};
  std::atomic<uint64_t> decisions_skip_{0};
  std::atomic<uint64_t> cache_hits_{0};
  std::atomic<uint64_t> buffer_hits_{0};
  std::atomic<uint64_t> cold_hits_{0};
  std::atomic<uint64_t> cold_timeouts_{0};
  std::atomic<uint64_t> cold_errors_{0};
  std::atomic<uint64_t> apply_wait_timeouts_{0};
  std::atomic<uint64_t> durable_wait_timeouts_{0};
  std::atomic<uint64_t> append_failures_{0};
  std::atomic<uint64_t> commit_failures_{0};
  std::atomic<uint64_t> parse_failures_{0};
  std::atomic<uint64_t> replayed_resolveds_emitted_{0};
  std::atomic<uint64_t> flushes_observed_{0};
  std::atomic<uint64_t> flush_skip_resolveds_emitted_{0};
};

}  // namespace abyss::consumer
