#include "abyss/queue/wal_queue.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string_view>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/platform/fs.h"
#include "shard_state.h"

ABYSS_LOG_COMPONENT("abyss.queue.wal")

namespace abyss::queue {

namespace {

constexpr int kShardNameWidth = 4;

std::string ShardDirName(core::ShardId shard) {
  std::ostringstream oss;
  oss << "shard-" << std::setw(kShardNameWidth) << std::setfill('0') << shard;
  return oss.str();
}

std::string OffsetsDirName() { return "offsets"; }

}  // namespace

core::Result<std::unique_ptr<WalQueue>> WalQueue::Open(WalConfig config) {
  if (config.shard_count == 0) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "shard_count must be >= 1"});
  }
  if (config.offset_fsync_interval <= std::chrono::milliseconds::zero()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "offset_fsync_interval must be > 0"});
  }
  if (config.durability_window_bytes == 0 ||
      config.durability_window <= std::chrono::milliseconds::zero()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "durability window bounds must be > 0"});
  }

  std::error_code ec;
  std::filesystem::create_directories(config.wal_path, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "create wal_path: " + ec.message()});
  }

  // Surface the volume's real durability posture before any ack is given
  // (invariant 5). A volume that cannot make directory entries durable
  // could lose whole segments, not just the unflushed window, on a power
  // loss: refuse to start.
  if (auto cap = platform::fs::ProbeDurability(config.wal_path); cap.has_value()) {
    metrics::Registry::Instance()
        .Gauge(metrics::names::kFsDurableDirSupported)
        .Set(cap->dir_sync_supported ? 1.0 : 0.0);
    ABYSS_LOG_INFO("WAL durability probe", {"path", std::string_view{config.wal_path}},
                   {"dir_sync_supported", cap->dir_sync_supported},
                   {"fsync_backend", static_cast<int64_t>(cap->backend)});
    if (!cap->dir_sync_supported) {
      ABYSS_LOG_CRITICAL("data volume cannot make directory entries durable",
                         {"path", std::string_view{config.wal_path}});
      return std::unexpected(
          core::Error{core::ErrorCode::kFailedPrecondition,
                      "data volume does not support durable directory fsync; refusing to start"});
    }
  } else {
    return std::unexpected(cap.error());
  }

  std::unique_ptr<WalQueue> queue(new WalQueue(std::move(config)));
  auto init = queue->Initialize();
  if (!init.has_value()) return std::unexpected(init.error());
  queue->recovering_.store(false, std::memory_order_release);
  queue->persister_ = std::thread(&WalQueue::RunPersister, queue.get());

  core::SequenceId max_head = 0;
  for (const auto& shard : queue->shards_) {
    max_head = std::max(max_head, shard->head_seq());
  }
  ABYSS_LOG_INFO("WAL opened", {"path", std::string_view{queue->config_.wal_path}},
                 {"shard_count", static_cast<int64_t>(queue->config_.shard_count)},
                 {"durability", core::DurabilityName(queue->config_.durability)},
                 {"segment_size_bytes", static_cast<uint64_t>(queue->config_.segment_size_bytes)},
                 {"min_retention_s", static_cast<int64_t>(queue->config_.min_retention.count())},
                 {"offset_fsync_interval_ms",
                  static_cast<int64_t>(queue->config_.offset_fsync_interval.count())},
                 {"head_seq", static_cast<uint64_t>(max_head)});
  return queue;
}

WalQueue::WalQueue(WalConfig config)
    : config_(std::move(config)),
      window_(config_.durability_window_bytes, config_.durability_window),
      committed_(config_.retention_consumers.size() * config_.shard_count),
      persist_duration_(metrics::Registry::Instance().Histogram(
          metrics::names::kQueueOffsetPersistDurationSeconds)),
      persist_failures_(
          metrics::Registry::Instance().Counter(metrics::names::kQueueOffsetPersistFailuresTotal)) {
}

// Only allocation can throw here, and an OOM at teardown may terminate.
// NOLINTNEXTLINE(bugprone-exception-escape)
WalQueue::~WalQueue() {
  StopPersister();
  bool persist = false;
  {
    const std::scoped_lock lock(persist_mu_);
    persist = persist_on_close_ && checkpoint_ != nullptr;
  }
  // Consumers have stopped committing by now; anything they committed that
  // the last round missed is persisted here. A failure is already logged.
  if (persist) (void)PersistOffsets();  // NOLINT(bugprone-unused-return-value)
  window_.Shutdown();
  for (auto& shard : shards_) {
    if (shard) shard->Shutdown();
  }
}

core::Result<void> WalQueue::Initialize() {
  auto checkpoint = OffsetCheckpoint::Open(OffsetCheckpointConfig{
      .dir = std::filesystem::path(config_.wal_path) / OffsetsDirName(),
      .shard_count = static_cast<uint32_t>(config_.shard_count),
      .consumers = config_.retention_consumers,
  });
  if (!checkpoint.has_value()) return std::unexpected(checkpoint.error());
  checkpoint_ = std::move(*checkpoint);

  shards_.reserve(config_.shard_count);
  for (core::ShardId shard = 0; shard < config_.shard_count; ++shard) {
    const auto shard_dir = std::filesystem::path(config_.wal_path) / ShardDirName(shard);
    auto state = ShardState::Open({
        .shard = shard,
        .directory = shard_dir.string(),
        .segment_size_bytes = config_.segment_size_bytes,
        .max_value_size_bytes = config_.max_value_size_bytes,
        .ack_durability = config_.durability,
        .window = &window_,
        .on_rotate = [this] { RunReaper(); },
    });
    if (!state.has_value()) return std::unexpected(state.error());

    for (size_t c = 0; c < config_.retention_consumers.size(); ++c) {
      const core::ConsumerId consumer = config_.retention_consumers[c];
      const auto persisted = checkpoint_->Get(consumer, shard);
      if (!persisted.has_value()) continue;
      // Commits are gated on the power-durable end, so a persisted offset
      // names an entry that survives any crash: it is below head.
      if (*persisted >= (*state)->head_seq()) {
        ABYSS_LOG_CRITICAL("persisted offset exceeds WAL head",
                           {"consumer", static_cast<uint64_t>(consumer)},
                           {"shard", static_cast<int64_t>(shard)},
                           {"persisted", static_cast<uint64_t>(*persisted)},
                           {"head_seq", static_cast<uint64_t>((*state)->head_seq())});
        return std::unexpected(core::Error{core::ErrorCode::kCorruption,
                                           "persisted offset exceeds WAL head for consumer/shard"});
      }
      committed_[(c * config_.shard_count) + shard].store(OffsetCheckpoint::Encode(persisted),
                                                          std::memory_order_relaxed);
    }

    shards_.push_back(std::move(*state));
  }

  const std::scoped_lock lock(reaper_mu_);
  reaper_ = std::make_unique<SegmentReaper>(*this, *checkpoint_,
                                            SegmentReaperConfig{
                                                .consumers = config_.retention_consumers,
                                                .min_retention = config_.min_retention,
                                            });
  return {};
}

std::optional<size_t> WalQueue::OffsetIndex(core::ConsumerId consumer, core::ShardId shard) const {
  const auto& consumers = config_.retention_consumers;
  const auto it = std::ranges::find(consumers, consumer);
  if (it == consumers.end()) return std::nullopt;
  return (static_cast<size_t>(it - consumers.begin()) * config_.shard_count) + shard;
}

core::Result<void> WalQueue::PersistOffsets() {
  const std::scoped_lock lock(persist_mu_);
  const uint64_t generation = commit_generation_.load(std::memory_order_acquire);
  if (generation == persisted_generation_) return {};

  std::vector<uint64_t> snapshot(committed_.size());
  for (size_t i = 0; i < committed_.size(); ++i) {
    snapshot[i] = committed_[i].load(std::memory_order_acquire);
  }

  const auto start = std::chrono::steady_clock::now();
  core::Result<void> written = persist_fault_ ? persist_fault_() : core::Result<void>{};
  if (written.has_value()) written = checkpoint_->Write(snapshot);
  persist_duration_.Observe(
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
  if (!written.has_value()) {
    persist_failures_.Increment();
    ABYSS_LOG_ERROR("offset checkpoint persist failed; retention holds until a retry succeeds",
                    {"err", std::string_view{written.error().message()}});
    return written;
  }
  persisted_generation_ = generation;
  return {};
}

core::Result<void> WalQueue::FlushOffsets() {
  auto persisted = PersistOffsets();
  if (!persisted.has_value()) return persisted;
  RunReaper();
  return {};
}

void WalQueue::RunPersister() {
  std::unique_lock lock(persister_mu_);
  while (!persister_stop_) {
    if (persister_cv_.wait_for(lock, config_.offset_fsync_interval,
                               [this] { return persister_stop_; })) {
      break;
    }
    lock.unlock();
    (void)FlushOffsets();  // NOLINT(bugprone-unused-return-value): logged and counted
    lock.lock();
  }
}

void WalQueue::StopPersister() {
  {
    const std::scoped_lock lock(persister_mu_);
    persister_stop_ = true;
  }
  persister_cv_.notify_all();
  if (persister_.joinable()) persister_.join();
}

void WalQueue::SetOffsetPersistFaultForTesting(std::function<core::Result<void>()> fault) {
  const std::scoped_lock lock(persist_mu_);
  persist_fault_ = std::move(fault);
}

void WalQueue::SkipFinalOffsetPersistForTesting() {
  const std::scoped_lock lock(persist_mu_);
  persist_on_close_ = false;
}

void WalQueue::SetFlushHookForTesting(const FlushHook& hook) {
  for (auto& shard : shards_) shard->SetFlushHookForTesting(hook);
}

FlushedExtent WalQueue::FlushedExtentForTesting(core::ShardId shard) const {
  return shards_.at(shard)->FlushedExtentForTesting();
}

void WalQueue::SkipFinalFlushForTesting() {
  for (auto& shard : shards_) shard->SkipFinalFlushForTesting();
}

core::Duration WalQueue::DurabilityLag() const {
  core::Duration lag = core::Duration::zero();
  for (const auto& shard : shards_) lag = std::max(lag, shard->DurabilityLag());
  return lag;
}

void WalQueue::RunReaper() {
  const std::scoped_lock lock(reaper_mu_);
  if (!reaper_) return;
  auto result = reaper_->RunOnce();
  if (!result.has_value()) {
    reaper_failures_.fetch_add(1, std::memory_order_relaxed);
    metrics::Registry::Instance().Counter(metrics::names::kQueueReaperFailuresTotal).Increment();
    ABYSS_LOG_WARN("segment reaper failed", {"err", std::string_view{result.error().message()}});
    return;
  }

  const auto& outcome = *result;
  if (outcome.failed > 0) {
    reaper_failures_.fetch_add(outcome.failed, std::memory_order_relaxed);
    metrics::Registry::Instance()
        .Counter(metrics::names::kQueueReaperFailuresTotal)
        .Increment(static_cast<double>(outcome.failed));
    const std::string_view err = outcome.first_error.has_value()
                                     ? std::string_view{outcome.first_error->message()}
                                     : std::string_view{"unknown"};
    ABYSS_LOG_WARN("segment reaper could not reclaim every eligible segment",
                   {"failed", static_cast<uint64_t>(outcome.failed)},
                   {"deleted", static_cast<uint64_t>(outcome.deleted)}, {"err", err});
  }
  RecordOldestEligibleUnreaped(outcome.oldest_eligible_unreaped);
}

void WalQueue::RecordOldestEligibleUnreaped(std::optional<core::WallTime> created_at) {
  int64_t epoch_ms = kNoUnreapedEpochMs;
  if (created_at.has_value()) {
    const auto since_epoch = created_at->time_since_epoch();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(since_epoch);
    epoch_ms = static_cast<int64_t>(ms.count());
  }
  oldest_eligible_unreaped_epoch_ms_.store(epoch_ms, std::memory_order_relaxed);
}

std::optional<core::Duration> WalQueue::OldestEligibleUnreapedAge() const {
  const int64_t epoch_ms = oldest_eligible_unreaped_epoch_ms_.load(std::memory_order_relaxed);
  if (epoch_ms == kNoUnreapedEpochMs) return std::nullopt;
  const auto created_at = core::WallTime{
      std::chrono::duration_cast<core::WallClock::duration>(std::chrono::milliseconds{epoch_ms})};
  const auto age = std::chrono::duration_cast<core::Duration>(core::WallClock::now() - created_at);
  return std::max(core::Duration::zero(), age);
}

core::Result<void> WalQueue::ValidateShard(core::ShardId shard) const {
  if (shard >= config_.shard_count) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument,
                                       "shard " + std::to_string(shard) + " out of range"});
  }
  return {};
}

core::SteadyTime WalQueue::DefaultAdmitBy() const {
  return core::SteadyClock::now() + config_.admission_timeout;
}

core::Result<PendingAppend> WalQueue::BeginAppend(core::ShardId shard, core::QueueEntry entry,
                                                  core::SteadyTime admit_by) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->BeginAppend(std::move(entry), admit_by);
}

core::Result<PendingBatchAppend> WalQueue::BeginAppendBatch(
    core::ShardId shard, std::span<const core::QueueEntry> entries, core::SteadyTime admit_by) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->BeginAppendBatch(entries, admit_by);
}

core::Result<AppendResult> WalQueue::Append(core::ShardId shard, core::QueueEntry entry,
                                            core::SteadyTime admit_by) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->Append(std::move(entry), admit_by);
}

core::Result<AppendBatchResult> WalQueue::AppendBatch(core::ShardId shard,
                                                      std::span<const core::QueueEntry> entries,
                                                      core::SteadyTime admit_by) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->AppendBatch(entries, admit_by);
}

core::Result<PendingAppend> WalQueue::BeginAppend(core::ShardId shard, core::QueueEntry entry) {
  return BeginAppend(shard, std::move(entry), DefaultAdmitBy());
}

core::Result<PendingBatchAppend> WalQueue::BeginAppendBatch(
    core::ShardId shard, std::span<const core::QueueEntry> entries) {
  return BeginAppendBatch(shard, entries, DefaultAdmitBy());
}

core::Result<AppendResult> WalQueue::Append(core::ShardId shard, core::QueueEntry entry) {
  return Append(shard, std::move(entry), DefaultAdmitBy());
}

core::Result<AppendBatchResult> WalQueue::AppendBatch(core::ShardId shard,
                                                      std::span<const core::QueueEntry> entries) {
  return AppendBatch(shard, entries, DefaultAdmitBy());
}

core::Result<std::vector<core::QueueEntry>> WalQueue::Read(core::ShardId shard,
                                                           core::SequenceId from_seq,
                                                           size_t max_count, core::Duration timeout,
                                                           core::Durability visible) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->Read(from_seq, max_count, timeout, visible);
}

core::Result<void> WalQueue::CommitOffset(core::ConsumerId consumer, core::ShardId shard,
                                          core::SequenceId seq) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  const auto index = OffsetIndex(consumer, shard);
  if (!index.has_value()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument,
                    "consumer " + std::to_string(consumer) + " does not commit offsets"});
  }
  // Fail-closed durability gate (QUEUE-2/XERR-2/XDUR-1/XDUR-2/HOTC-5): a
  // committed offset never passes the power-durable log, under either
  // class. Consumers clamp or await before committing; this is the
  // backstop.
  const core::SequenceId durable_end = shards_[shard]->DurableEnd(core::Durability::kPowerLoss);
  if (seq >= durable_end) {
    return std::unexpected(core::Error{
        core::ErrorCode::kFailedPrecondition,
        "commit seq " + std::to_string(seq) + " is not below the power-durable WAL end " +
            std::to_string(durable_end) + " for shard " + std::to_string(shard)});
  }

  const uint64_t want = OffsetCheckpoint::Encode(seq);
  auto& slot = committed_[*index];
  uint64_t current = slot.load(std::memory_order_acquire);
  do {
    if (current == want) return {};
    if (current > want) {
      return std::unexpected(
          core::Error{core::ErrorCode::kInvalidArgument,
                      "commit seq " + std::to_string(seq) + " is below committed offset " +
                          std::to_string(current - 1) + " for consumer " +
                          std::to_string(consumer) + " on shard " + std::to_string(shard)});
    }
  } while (!slot.compare_exchange_weak(current, want, std::memory_order_acq_rel));
  commit_generation_.fetch_add(1, std::memory_order_release);
  return {};
}

core::Result<std::optional<core::SequenceId>> WalQueue::CommittedOffset(core::ConsumerId consumer,
                                                                        core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  const auto index = OffsetIndex(consumer, shard);
  if (!index.has_value()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument,
                    "consumer " + std::to_string(consumer) + " does not commit offsets"});
  }
  return OffsetCheckpoint::Decode(committed_[*index].load(std::memory_order_acquire));
}

core::Result<std::optional<core::SequenceId>> WalQueue::PersistedOffset(core::ConsumerId consumer,
                                                                        core::ShardId shard) const {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  if (!OffsetIndex(consumer, shard).has_value()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument,
                    "consumer " + std::to_string(consumer) + " does not commit offsets"});
  }
  return checkpoint_->Get(consumer, shard);
}

core::Result<core::SequenceId> WalQueue::DurableEnd(core::ShardId shard,
                                                    core::Durability durability) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->DurableEnd(durability);
}

core::Result<bool> WalQueue::AwaitDurable(core::ShardId shard, core::SequenceId seq,
                                          core::Durability durability, core::Duration timeout) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->AwaitDurable(seq, durability, timeout);
}

core::Result<core::SequenceId> WalQueue::FirstSeq(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->first_seq();
}

core::Result<core::SequenceId> WalQueue::OldestRetained(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());

  core::SequenceId min_offset = shards_[shard]->head_seq();
  for (auto consumer : config_.retention_consumers) {
    const auto persisted = checkpoint_->Get(consumer, shard);
    if (!persisted.has_value()) return shards_[shard]->first_seq();
    min_offset = std::min(min_offset, *persisted);
  }
  return min_offset;
}

core::Result<core::SequenceId> WalQueue::TailSeq(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  // head_seq is the next seq to assign; the highest assigned (matching what
  // a consumer's HighestSettledSeq will reach once caught up) is one less.
  const auto head = shards_[shard]->head_seq();
  return head > 0 ? head - 1 : 0;
}

core::Result<core::QueueStats> WalQueue::Stats() {
  core::QueueStats stats;  // NOLINT(misc-const-correctness)
  bool first = true;
  for (const auto& shard : shards_) {
    stats.total_entries += shard->total_entries();
    stats.total_bytes += shard->total_bytes();
    stats.head_seq = std::max(stats.head_seq, shard->head_seq());
    const core::SequenceId shard_first = shard->first_seq();
    stats.first_seq = first ? shard_first : std::min(stats.first_seq, shard_first);
    first = false;
  }
  return stats;
}

std::vector<SegmentRegistry::SealedSegmentInfo> WalQueue::ListSealedSegments() const {
  std::vector<SegmentRegistry::SealedSegmentInfo> result;
  for (const auto& shard : shards_) {
    auto sealed = shard->ListSealedSegments();
    for (auto& info : sealed) {
      result.push_back(std::move(info));
    }
  }
  return result;
}

core::Result<void> WalQueue::RemoveSegment(core::ShardId shard, core::SequenceId base_seq) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->RemoveSegment(base_seq);
}

}  // namespace abyss::queue
