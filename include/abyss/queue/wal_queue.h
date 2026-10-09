#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue.h"
#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/metrics/metrics.h"
#include "abyss/queue/durability_window.h"
#include "abyss/queue/group_commit.h"
#include "abyss/queue/offset_checkpoint.h"
#include "abyss/queue/reservation.h"
#include "abyss/queue/segment_reaper.h"
#include "abyss/queue/segment_registry.h"

namespace abyss::queue {

class ShardStream;
struct LogUnit;

// Runs in each flush of `log` after its filled-prefix snapshot, before
// its syncs. May block; an error is a failed sync.
using FlushHook = std::function<core::Result<void>(uint32_t log)>;

// Where a log's power-durable prefix ends: its segment file and the
// file offset of that position.
struct DurableExtent {
  std::string path;
  uint64_t offset = 0;
};

struct WalConfig {
  std::string wal_path;
  size_t segment_size_bytes = 134217728;
  // Largest single encoded entry accepted; decoupled from segment_size_bytes.
  size_t max_value_size_bytes = 67108864;
  size_t shard_count = 1;
  // Physical logs, a power of two <= shard_count; shard s is on log
  // s % log_count.
  uint32_t log_count = 1;
  // Per-shard offset ring slots, a power of two.
  size_t ring_entries = 65536;
  // The class append futures resolve at.
  core::Durability durability = core::Durability::kProcessCrash;
  // Bounds on filled but not yet power-durable entries: bytes across
  // logs, and the oldest entry's age per log.
  uint64_t durability_window_bytes = uint64_t{64} * 1024 * 1024;
  std::chrono::milliseconds durability_window{1000};
  // Admission wait for the appends that take no deadline.
  std::chrono::milliseconds admission_timeout{5000};
  std::chrono::seconds min_retention{86400};
  // Consumers that commit offsets; their persisted offsets gate retention.
  std::vector<core::ConsumerId> retention_consumers;
  // Cadence of the background committed-offset checkpoint.
  std::chrono::milliseconds offset_fsync_interval{1000};
};

// NOLINTNEXTLINE(misc-multiple-inheritance)
class WalQueue : public core::Queue, public SegmentRegistry {
 public:
  static core::Result<std::unique_ptr<WalQueue>> Open(WalConfig config);
  ~WalQueue() override;

  WalQueue(const WalQueue&) = delete;
  WalQueue& operator=(const WalQueue&) = delete;
  WalQueue(WalQueue&&) = delete;
  WalQueue& operator=(WalQueue&&) = delete;

  core::Result<PendingAppend> BeginAppend(core::ShardId shard, core::QueueEntry entry,
                                          core::SteadyTime admit_by) override;
  core::Result<PendingBatchAppend> BeginAppendBatch(core::ShardId shard,
                                                    std::span<const core::QueueEntry> entries,
                                                    core::SteadyTime admit_by) override;
  core::Result<AppendResult> Append(core::ShardId shard, core::QueueEntry entry,
                                    core::SteadyTime admit_by) override;
  core::Result<AppendBatchResult> AppendBatch(core::ShardId shard,
                                              std::span<const core::QueueEntry> entries,
                                              core::SteadyTime admit_by) override;

  core::Result<void> Admit(core::ShardId shard, core::SteadyTime admit_by) override;
  bool WaitForSpare(core::ShardId shard, core::SteadyTime deadline) override;
  core::Result<Reservation> Reserve(std::span<const ShardEntries> parts) override;
  core::Result<Reservation> ReserveFlush(std::span<const ShardEntries> parts) override;
  uint32_t LogOf(core::ShardId shard) const override { return shard % config_.log_count; }
  DurableFutures Complete(Reservation&& reservation) override;

  // As above, admitting within WalConfig::admission_timeout.
  core::Result<PendingAppend> BeginAppend(core::ShardId shard, core::QueueEntry entry);
  core::Result<PendingBatchAppend> BeginAppendBatch(core::ShardId shard,
                                                    std::span<const core::QueueEntry> entries);
  core::Result<AppendResult> Append(core::ShardId shard, core::QueueEntry entry);
  core::Result<AppendBatchResult> AppendBatch(core::ShardId shard,
                                              std::span<const core::QueueEntry> entries);
  core::Result<std::vector<core::QueueEntry>> Read(core::ShardId shard, core::SequenceId from_seq,
                                                   size_t max_count, core::Duration timeout,
                                                   core::Durability visible) override;
  core::Result<void> CommitOffset(core::ConsumerId consumer, core::ShardId shard,
                                  core::SequenceId seq) override;
  core::Result<std::optional<core::SequenceId>> CommittedOffset(core::ConsumerId consumer,
                                                                core::ShardId shard) override;
  core::Durability AckDurability() const override { return config_.durability; }
  core::Result<core::SequenceId> DurableEnd(core::ShardId shard,
                                            core::Durability durability) override;
  core::Result<bool> AwaitDurable(core::ShardId shard, core::SequenceId seq,
                                  core::Durability durability, core::Duration timeout) override;
  core::Result<core::SequenceId> FirstSeq(core::ShardId shard) override;
  core::Result<core::SequenceId> OldestRetained(core::ShardId shard) override;
  core::Result<core::SequenceId> TailSeq(core::ShardId shard) override;
  core::Result<core::QueueStats> Stats() override;

  // A log-structured scan: one header-only walker per log feeds
  // shard-affine decode workers. Retention is held off meanwhile.
  core::Result<void> Scan(std::span<const core::SequenceId> from,
                          std::span<const core::SequenceId> end, uint32_t parallelism,
                          const ScanSink& sink, const std::atomic<bool>& cancel) override;

  uint32_t LogCount() const override { return config_.log_count; }
  std::vector<SegmentRegistry::SealedSegmentInfo> ListSealedSegments(
      uint32_t log, std::size_t max_count) const override;
  core::Result<void> RemoveSegment(uint32_t log, uint64_t ordinal) override;
  // Every log's sealed segments.
  std::vector<SegmentRegistry::SealedSegmentInfo> ListSealedSegments() const;

  // Persists every committed offset now, then sweeps retention. The same
  // round the background persister runs each offset_fsync_interval.
  core::Result<void> FlushOffsets();

  // The offset the reaper honours: last checkpointed, not last committed.
  core::Result<std::optional<core::SequenceId>> PersistedOffset(core::ConsumerId consumer,
                                                                core::ShardId shard) const;

  // True while Open is in progress; false once all shards have been loaded.
  bool IsRecovering() const { return recovering_.load(std::memory_order_acquire); }

  uint64_t ReaperFailures() const { return reaper_failures_.load(std::memory_order_relaxed); }

  // Time since the oldest segment the last sweep found eligible but
  // could not remove was sealed; nullopt once retention reclaims
  // everything it is allowed to.
  [[nodiscard]] std::optional<core::Duration> OldestEligibleUnreapedAge() const;

  // Reservations on `log` not yet completed.
  uint64_t ReadyToComplete(uint32_t log) const;

  // Filled bytes not yet power-durable, across logs.
  uint64_t UnflushedBytes() const { return window_.UnflushedBytes(); }
  // Age bound of the oldest entry not yet power-durable, across logs.
  core::Duration DurabilityLag() const;

  // Test seams. A fault returned here fails persist rounds like an I/O
  // error would; skipping the final persist models a crash at teardown.
  void SetOffsetPersistFaultForTesting(std::function<core::Result<void>()> fault);
  void SkipFinalOffsetPersistForTesting();
  // A blocking hook stalls flushes; an error is a fatal flush failure.
  void SetFlushHookForTesting(const FlushHook& hook);
  DurableExtent DurableExtentForTesting(uint32_t log) const;
  // The data syncs `log`'s flushes have run.
  uint64_t SyncCountForTesting(uint32_t log) const;
  // Close without the final flush, as a power loss at teardown would.
  void SkipFinalFlushForTesting();
  // Runs inside a batch append or reservation after each of its frames
  // but the last is committed, with the count committed so far. It may
  // block.
  void SetBatchCommitHookForTesting(const std::function<void(std::size_t committed)>& hook);
  // While paused, `log` prepares no spare segments.
  void PauseSegmentPreparerForTesting(uint32_t log, bool paused);
  // The next removal of a reclaimed segment's file in `log` fails.
  void InjectSegmentRemoveErrorForTesting(uint32_t log, core::Error error);
  // For tests that look at how a shard's reads locate frames.
  const ShardStream& StreamForTesting(core::ShardId shard) const;
  // The log position `seq`'s frame was reserved at, while the shard's
  // offset ring still holds it.
  std::optional<uint64_t> PositionForTesting(core::ShardId shard, core::SequenceId seq) const;

 private:
  static constexpr int64_t kNoUnreapedEpochMs = std::numeric_limits<int64_t>::min();

  explicit WalQueue(WalConfig config);
  core::Result<void> Initialize();

  core::Result<void> OpenLogs();
  core::Result<void> RecoverOffsets();
  core::Result<GroupCommitter::Extent> FlushLog(LogUnit& unit);
  void Flushed(LogUnit& unit);

  core::Result<void> ValidateShard(core::ShardId shard) const;
  core::SteadyTime DefaultAdmitBy() const;
  // Index into committed_ for a retention consumer, or nullopt.
  std::optional<size_t> OffsetIndex(core::ConsumerId consumer, core::ShardId shard) const;

  core::Result<void> PersistOffsets() ABYSS_EXCLUDES(persist_mu_);
  void RunPersister();
  void StopPersister();

  void RunReaper() ABYSS_EXCLUDES(reaper_mu_);
  void RecordOldestEligibleUnreaped(std::optional<core::WallTime> sealed_at);

  WalConfig config_;
  std::atomic<bool> recovering_{true};
  std::atomic<uint64_t> reaper_failures_{0};
  std::atomic<int64_t> oldest_eligible_unreaped_epoch_ms_{kNoUnreapedEpochMs};
  const uint64_t frame_space_;
  // Outlives the logs, whose commit threads release into it.
  DurabilityWindow window_;
  std::vector<std::unique_ptr<LogUnit>> logs_;
  std::vector<std::unique_ptr<ShardStream>> streams_;
  std::atomic<bool> skip_final_flush_{false};
  metrics::CounterHandle scan_bytes_;
  std::unique_ptr<OffsetCheckpoint> checkpoint_;

  // In-memory committed offsets, OffsetCheckpoint::Encode()d, indexed
  // consumer-major like the checkpoint.
  std::vector<std::atomic<uint64_t>> committed_;
  // Bumped on every commit; a persist round with no change is skipped.
  std::atomic<uint64_t> commit_generation_{0};

  std::mutex persist_mu_;
  uint64_t persisted_generation_ ABYSS_GUARDED_BY(persist_mu_) = 0;
  // The generation written to both checkpoint slots.
  uint64_t settled_generation_ ABYSS_GUARDED_BY(persist_mu_) = 0;
  std::function<core::Result<void>()> persist_fault_ ABYSS_GUARDED_BY(persist_mu_);
  bool persist_on_close_ ABYSS_GUARDED_BY(persist_mu_) = true;
  metrics::HistogramHandle persist_duration_;
  metrics::CounterHandle persist_failures_;

  std::mutex persister_mu_;
  std::condition_variable persister_cv_;
  bool persister_stop_ ABYSS_GUARDED_BY(persister_mu_) = false;
  std::thread persister_;

  // Sweeps run from the persister, one at a time, and never during a
  // Scan.
  std::mutex reaper_mu_;
  std::unique_ptr<SegmentReaper> reaper_ ABYSS_GUARDED_BY(reaper_mu_);
  uint32_t scans_ ABYSS_GUARDED_BY(reaper_mu_) = 0;
};

}  // namespace abyss::queue
