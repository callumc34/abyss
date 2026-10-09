#include "abyss/queue/wal_queue.h"

#include <algorithm>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/fatal.h"
#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/platform/fs.h"
#include "segment_header_v2.h"
#include "shard_stream.h"

ABYSS_LOG_COMPONENT("abyss.queue.wal")

namespace abyss::queue {

namespace {

constexpr std::size_t kMaxRingEntries = std::size_t{1} << 24;
constexpr std::size_t kLogDigits = 4;
constexpr std::size_t kOrdinalDigits = 20;
constexpr std::string_view kLogPrefix = "log-";
constexpr std::string_view kFormat1Prefix = "shard-";

std::string Padded(uint64_t value, std::size_t width) {
  const std::string digits = std::to_string(value);
  return std::string(width > digits.size() ? width - digits.size() : 0, '0') + digits;
}

std::string LogDirName(uint32_t log) { return std::string(kLogPrefix) + Padded(log, kLogDigits); }

std::string OffsetsDirName() { return "offsets"; }

core::Error Invalid(const std::string& what) { return {core::ErrorCode::kInvalidArgument, what}; }

// Format 1 kept a directory per shard; there is no migration from it.
// A log directory past log_count holds shards routed elsewhere now.
core::Result<void> CheckLayout(const std::filesystem::path& wal_path, uint32_t log_count) {
  std::error_code ec;
  std::filesystem::directory_iterator it(wal_path, ec);
  if (ec) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                       "list wal_path " + wal_path.string() + ": " + ec.message()});
  }
  for (const auto& entry : it) {
    if (!entry.is_directory()) continue;
    const std::string name = entry.path().filename().string();
    if (name.starts_with(kFormat1Prefix)) {
      return std::unexpected(
          core::Error{core::ErrorCode::kFailedPrecondition,
                      "wal_path " + wal_path.string() + " holds a WAL format 1 layout (" + name +
                          "); format 2 has no migration from it, so start from an empty wal_path"});
    }
    if (!name.starts_with(kLogPrefix)) continue;
    const std::string_view digits = std::string_view(name).substr(kLogPrefix.size());
    uint32_t log = 0;
    const auto [ptr, err] = std::from_chars(digits.data(), digits.data() + digits.size(), log);
    if (err == std::errc{} && ptr == digits.data() + digits.size() && log >= log_count) {
      return std::unexpected(core::Error{
          core::ErrorCode::kFailedPrecondition,
          "wal_path " + wal_path.string() + " holds " + name + ", but queue.log_count is " +
              std::to_string(log_count) + "; a WAL keeps the log count it was created with"});
    }
  }
  return {};
}

}  // namespace

core::Result<std::unique_ptr<WalQueue>> WalQueue::Open(WalConfig config) {
  if (config.shard_count == 0) return std::unexpected(Invalid("shard_count must be >= 1"));
  if (config.log_count == 0 || !std::has_single_bit(config.log_count) ||
      config.log_count > config.shard_count) {
    return std::unexpected(Invalid("log_count must be a power of two <= shard_count"));
  }
  if (config.ring_entries == 0 || !std::has_single_bit(config.ring_entries) ||
      config.ring_entries > kMaxRingEntries) {
    return std::unexpected(Invalid("ring_entries must be a power of two <= 2^24"));
  }
  if (config.offset_fsync_interval <= std::chrono::milliseconds::zero()) {
    return std::unexpected(Invalid("offset_fsync_interval must be > 0"));
  }
  if (config.durability_window_bytes == 0 ||
      config.durability_window <= std::chrono::milliseconds::zero()) {
    return std::unexpected(Invalid("durability window bounds must be > 0"));
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
  if (auto layout = CheckLayout(config.wal_path, config.log_count); !layout) {
    return std::unexpected(layout.error());
  }

  std::unique_ptr<WalQueue> queue(new WalQueue(std::move(config)));
  auto init = queue->Initialize();
  if (!init.has_value()) return std::unexpected(init.error());
  queue->recovering_.store(false, std::memory_order_release);
  queue->persister_ = std::thread(&WalQueue::RunPersister, queue.get());

  core::SequenceId max_head = 0;
  for (const auto& stream : queue->streams_) max_head = std::max(max_head, stream->next_seq());
  ABYSS_LOG_INFO(
      "WAL opened", {"path", std::string_view{queue->config_.wal_path}},
      {"shard_count", static_cast<int64_t>(queue->config_.shard_count)},
      {"log_count", static_cast<int64_t>(queue->config_.log_count)},
      {"ring_bytes", static_cast<uint64_t>(ShardStream::RingBytes(queue->config_.ring_entries) *
                                           queue->config_.shard_count)},
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
      frame_space_(config_.segment_size_bytes > kLogSegmentHeaderBytes
                       ? config_.segment_size_bytes - kLogSegmentHeaderBytes
                       : 0),
      window_(config_.durability_window_bytes, config_.durability_window),
      scan_bytes_(metrics::Registry::Instance().Counter(metrics::names::kWalScanBytesTotal)),
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
  for (auto& stream : streams_) stream->Shutdown();
  const bool final_flush = !skip_final_flush_.load(std::memory_order_acquire);
  for (auto& unit : logs_) {
    if (unit->committer == nullptr) continue;
    // A publisher may not have reported its end yet; the final flush
    // must still cover it.
    if (final_flush) unit->committer->Published(unit->log->FilledPrefix());
    unit->committer->Stop(final_flush);
  }
  for (auto& stream : streams_) stream->CommitterStopped();
  for (auto& unit : logs_) {
    if (unit->log != nullptr) unit->log->Shutdown();
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

  logs_.reserve(config_.log_count);
  for (uint32_t id = 0; id < config_.log_count; ++id) {
    auto unit = std::make_unique<LogUnit>();
    unit->id = id;
    unit->is_touched.assign(config_.shard_count, false);
    logs_.push_back(std::move(unit));
  }
  streams_.reserve(config_.shard_count);
  for (core::ShardId shard = 0; shard < config_.shard_count; ++shard) {
    streams_.push_back(std::make_unique<ShardStream>(ShardStreamConfig{
        .shard = shard,
        .ring_entries = config_.ring_entries,
        .max_value_size_bytes = config_.max_value_size_bytes,
        .ack_durability = config_.durability,
        .window = &window_,
        .unit = logs_[shard % config_.log_count].get(),
    }));
  }
  if (auto opened = OpenLogs(); !opened) return opened;
  if (auto recovered = RecoverOffsets(); !recovered) return recovered;

  for (auto& owned : logs_) {
    LogUnit* unit = owned.get();
    unit->committer = std::make_unique<GroupCommitter>(
        unit->log->DurablePrefix(), [this, unit] { return FlushLog(*unit); },
        [this, unit](LogPosition, GroupCommitter::Extent) { Flushed(*unit); });
  }

  const std::scoped_lock lock(reaper_mu_);
  reaper_ = std::make_unique<SegmentReaper>(*this, *checkpoint_,
                                            SegmentReaperConfig{
                                                .consumers = config_.retention_consumers,
                                                .min_retention = config_.min_retention,
                                            });
  return {};
}

// One pass per log hands every recovered frame to its shard's stream.
core::Result<void> WalQueue::OpenLogs() {
  for (auto& unit : logs_) {
    const uint32_t id = unit->id;
    std::optional<core::Error> failed;
    auto log = Log::Open(
        LogConfig{
            .dir = std::filesystem::path(config_.wal_path) / LogDirName(id),
            .log_id = id,
            .shard_count = static_cast<uint32_t>(config_.shard_count),
            .segment_size_bytes = config_.segment_size_bytes,
            .durability_window_bytes = config_.durability_window_bytes,
        },
        [this, id, &failed](const RecoveredFrame& frame) {
          if (failed.has_value()) return;
          const core::ShardId shard = frame.header.shard;
          if (shard % config_.log_count != id) {
            failed = core::Error{core::ErrorCode::kFailedPrecondition,
                                 "WAL log " + std::to_string(id) + " holds shard " +
                                     std::to_string(shard) + ", which queue.log_count " +
                                     std::to_string(config_.log_count) + " routes elsewhere"};
            return;
          }
          if (auto recovered = streams_[shard]->Recover(frame); !recovered) {
            failed = recovered.error();
          }
        });
    if (!log.has_value()) return std::unexpected(log.error());
    unit->log = std::move(*log);
    if (failed.has_value()) return std::unexpected(std::move(*failed));
  }
  return {};
}

// A shard's next seq survives reclamation of all its frames: reclaiming
// needs every consumer persisted past them, and persisted offsets stay
// below the power-durable end, so they bound what was ever assigned.
core::Result<void> WalQueue::RecoverOffsets() {
  for (core::ShardId shard = 0; shard < config_.shard_count; ++shard) {
    ShardStream& stream = *streams_[shard];
    const std::optional<core::SequenceId> head = stream.recovered_next();
    const std::optional<core::SequenceId> first = stream.recovered_first();
    core::SequenceId next = head.value_or(core::kFirstSeq);
    for (size_t c = 0; c < config_.retention_consumers.size(); ++c) {
      const core::ConsumerId consumer = config_.retention_consumers[c];
      const auto persisted = checkpoint_->Get(consumer, shard);
      // Commits are gated on the power-durable end, so a persisted offset
      // names an entry that survives any crash: it is below head.
      if (persisted.has_value() && head.has_value() && *persisted >= *head) {
        ABYSS_LOG_CRITICAL("persisted offset exceeds WAL head",
                           {"consumer", static_cast<uint64_t>(consumer)},
                           {"shard", static_cast<int64_t>(shard)},
                           {"persisted", static_cast<uint64_t>(*persisted)},
                           {"head_seq", static_cast<uint64_t>(*head)});
        return std::unexpected(core::Error{core::ErrorCode::kCorruption,
                                           "persisted offset exceeds WAL head for consumer/shard"});
      }
      // Only frames every consumer persisted are ever reclaimed.
      if (first.has_value() && *first > core::kFirstSeq &&
          (!persisted.has_value() || *persisted + 1 < *first)) {
        ABYSS_LOG_CRITICAL("WAL frames missing below the first retained frame",
                           {"consumer", static_cast<uint64_t>(consumer)},
                           {"shard", static_cast<int64_t>(shard)},
                           {"first_retained", static_cast<uint64_t>(*first)});
        return std::unexpected(
            core::Error{core::ErrorCode::kCorruption,
                        "shard " + std::to_string(shard) + " starts at seq " +
                            std::to_string(*first) + ", but consumer " + std::to_string(consumer) +
                            (persisted.has_value() ? " persisted only " + std::to_string(*persisted)
                                                   : std::string(" persisted nothing")) +
                            ": the entries in between are lost"});
      }
      if (persisted.has_value()) next = std::max(next, *persisted + 1);
      committed_[(c * config_.shard_count) + shard].store(OffsetCheckpoint::Encode(persisted),
                                                          std::memory_order_relaxed);
    }
    stream.FinishRecovery(next);
  }
  return {};
}

// A batch is visible at power_loss only once its last frame is
// durable, and then on every shard it spans.
core::Result<GroupCommitter::Extent> WalQueue::FlushLog(LogUnit& unit) {
  const auto snapshot_at = DurabilityWindow::Clock::now();
  auto flushed = unit.log->Flush([this, &unit](const frame::Header& header, uint32_t size) {
    auto& batch = unit.batch;
    if (!batch.empty() && batch.back().first == header.shard) {
      batch.back().second = header.seq;
    } else {
      batch.emplace_back(header.shard, header.seq);
    }
    if (header.batch_rest != size) return;
    for (const auto& [shard, seq] : batch) {
      if (streams_[shard]->Durable(seq + 1) && !unit.is_touched[shard]) {
        unit.is_touched[shard] = true;
        unit.touched.push_back(shard);
      }
    }
    batch.clear();
  });
  if (!flushed.has_value()) return std::unexpected(flushed.error());
  unit.age.Flushed(snapshot_at, flushed->to, [&unit] { return unit.log->ReservedTail(); });
  window_.Release(flushed->entry_bytes);
  return GroupCommitter::Extent{.end = flushed->to, .entries = flushed->entries};
}

void WalQueue::Flushed(LogUnit& unit) {
  for (const core::ShardId shard : unit.touched) {
    streams_[shard]->PowerAdvanced();
    unit.is_touched[shard] = false;
  }
  unit.touched.clear();
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
  // Retention reclaims only what both checkpoint slots hold, so a round
  // after the last commit writes it once more to the other slot.
  if (generation == persisted_generation_ && generation == settled_generation_) return {};

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
  if (generation == persisted_generation_) settled_generation_ = generation;
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
  for (auto& unit : logs_) {
    std::function<core::Result<void>()> bound;
    if (hook) bound = [hook, id = unit->id] { return hook(id); };
    unit->log->SetFlushHookForTesting(std::move(bound));
  }
}

DurableExtent WalQueue::DurableExtentForTesting(uint32_t log) const {
  const LogPosition durable = logs_.at(log)->log->DurablePrefix();
  const uint64_t ordinal = durable / frame_space_;
  const auto path = std::filesystem::path(config_.wal_path) / LogDirName(log) /
                    (Padded(ordinal, kOrdinalDigits) + ".seg");
  return DurableExtent{.path = path.string(),
                       .offset = kLogSegmentHeaderBytes + (durable - (ordinal * frame_space_))};
}

uint64_t WalQueue::SyncCountForTesting(uint32_t log) const {
  return logs_.at(log)->log->SyncCountForTesting();
}

void WalQueue::SkipFinalFlushForTesting() {
  skip_final_flush_.store(true, std::memory_order_release);
}

void WalQueue::SetBatchCommitHookForTesting(
    const std::function<void(std::size_t committed)>& hook) {
  for (auto& stream : streams_) stream->SetBatchCommitHookForTesting(hook);
}

void WalQueue::PauseSegmentPreparerForTesting(uint32_t log, bool paused) {
  Log& target = *logs_.at(log)->log;
  if (paused) {
    target.PausePreparerForTesting();
  } else {
    target.ResumePreparerForTesting();
  }
}

void WalQueue::InjectSegmentRemoveErrorForTesting(uint32_t log, core::Error error) {
  logs_.at(log)->log->InjectRemoveErrorForTesting(std::move(error));
}

const ShardStream& WalQueue::StreamForTesting(core::ShardId shard) const {
  return *streams_.at(shard);
}

std::optional<uint64_t> WalQueue::PositionForTesting(core::ShardId shard,
                                                     core::SequenceId seq) const {
  return streams_.at(shard)->RingPositionForTesting(seq);
}

core::Duration WalQueue::DurabilityLag() const {
  const auto now = DurabilityWindow::Clock::now();
  DurabilityWindow::Clock::duration lag = DurabilityWindow::Clock::duration::zero();
  for (const auto& unit : logs_) lag = std::max(lag, unit->age.Age(now));
  return std::chrono::duration_cast<core::Duration>(lag);
}

void WalQueue::RunReaper() {
  const std::scoped_lock lock(reaper_mu_);
  if (!reaper_ || scans_ > 0) return;
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

void WalQueue::RecordOldestEligibleUnreaped(std::optional<core::WallTime> sealed_at) {
  int64_t epoch_ms = kNoUnreapedEpochMs;
  if (sealed_at.has_value()) {
    const auto since_epoch = sealed_at->time_since_epoch();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(since_epoch);
    epoch_ms = static_cast<int64_t>(ms.count());
  }
  oldest_eligible_unreaped_epoch_ms_.store(epoch_ms, std::memory_order_relaxed);
}

std::optional<core::Duration> WalQueue::OldestEligibleUnreapedAge() const {
  const int64_t epoch_ms = oldest_eligible_unreaped_epoch_ms_.load(std::memory_order_relaxed);
  if (epoch_ms == kNoUnreapedEpochMs) return std::nullopt;
  const auto sealed_at = core::WallTime{
      std::chrono::duration_cast<core::WallClock::duration>(std::chrono::milliseconds{epoch_ms})};
  const auto age = std::chrono::duration_cast<core::Duration>(core::WallClock::now() - sealed_at);
  return std::max(core::Duration::zero(), age);
}

core::Result<void> WalQueue::ValidateShard(core::ShardId shard) const {
  if (shard >= config_.shard_count) {
    return std::unexpected(Invalid("shard " + std::to_string(shard) + " out of range"));
  }
  return {};
}

core::SteadyTime WalQueue::DefaultAdmitBy() const {
  return core::SteadyClock::now() + config_.admission_timeout;
}

core::Result<PendingAppend> WalQueue::BeginAppend(core::ShardId shard, core::QueueEntry entry,
                                                  core::SteadyTime admit_by) {
  ABYSS_DCHECK(ReservationsHeld() == 0, "WAL append by a thread holding a reservation");
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return streams_[shard]->BeginAppend(std::move(entry), admit_by);
}

core::Result<PendingBatchAppend> WalQueue::BeginAppendBatch(
    core::ShardId shard, std::span<const core::QueueEntry> entries, core::SteadyTime admit_by) {
  ABYSS_DCHECK(ReservationsHeld() == 0, "WAL append by a thread holding a reservation");
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return streams_[shard]->BeginAppendBatch(entries, admit_by);
}

core::Result<void> WalQueue::Admit(core::ShardId shard, core::SteadyTime admit_by) {
  ABYSS_DCHECK(ReservationsHeld() == 0, "WAL admission wait by a thread holding a reservation");
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return window_.Admit(logs_[shard % config_.log_count]->age, admit_by);
}

bool WalQueue::WaitForSpare(core::ShardId shard, core::SteadyTime deadline) {
  ABYSS_DCHECK(ReservationsHeld() == 0, "WAL spare wait by a thread holding a reservation");
  // A stopping queue refuses Reserve, so a ready spare must not invite
  // another attempt.
  if (shard >= config_.shard_count || streams_[shard]->stopping()) return false;
  return logs_[shard % config_.log_count]->log->WaitForSpare(deadline);
}

core::Result<Reservation> WalQueue::Reserve(std::span<const ShardEntries> parts) {
  ABYSS_DCHECK(ReservationsHeld() == 0, "WAL reservation by a thread holding one");
  if (parts.empty()) return std::unexpected(Invalid("reservation has no parts"));
  thread_local std::vector<ShardStream*> streams;
  thread_local std::vector<uint32_t> sizes;
  streams.clear();
  sizes.clear();
  const uint32_t log = parts.front().shard % config_.log_count;
  uint64_t total = 0;
  for (std::size_t p = 0; p < parts.size(); ++p) {
    const ShardEntries& part = parts[p];
    if (auto v = ValidateShard(part.shard); !v.has_value()) return std::unexpected(v.error());
    if (part.entries.empty()) {
      return std::unexpected(
          Invalid("reservation part for shard " + std::to_string(part.shard) + " has no entries"));
    }
    if (p > 0 && part.shard <= parts[p - 1].shard) {
      return std::unexpected(Invalid("reservation parts must be sorted by shard and distinct"));
    }
    if (part.shard % config_.log_count != log) {
      return std::unexpected(Invalid("CROSSSLOT a write's shards span WAL logs: shard " +
                                     std::to_string(parts.front().shard) + " is on log " +
                                     std::to_string(log) + ", shard " + std::to_string(part.shard) +
                                     " on log " + std::to_string(part.shard % config_.log_count)));
    }
    streams.push_back(streams_[part.shard].get());
    for (const core::QueueEntry& entry : part.entries) {
      const std::size_t size = frame::EntryFrameSize(entry);
      if (size > config_.max_value_size_bytes) {
        return std::unexpected(core::Error{core::ErrorCode::kValueTooLarge,
                                           "entry of " + std::to_string(size) +
                                               " bytes exceeds queue.max_value_size_bytes (" +
                                               std::to_string(config_.max_value_size_bytes) + ")"});
      }
      sizes.push_back(static_cast<uint32_t>(size));
      total += size;
    }
  }
  if (total > frame_space_) {
    return std::unexpected(core::Error{core::ErrorCode::kValueTooLarge,
                                       "batch of " + std::to_string(total) +
                                           " bytes exceeds the segment frame space of " +
                                           std::to_string(frame_space_) + " bytes"});
  }
  // Re-checked without waiting: the caller holds its hot shard locks.
  if (auto admitted = window_.Admit(logs_[log]->age, core::SteadyTime{}); !admitted) {
    return std::unexpected(admitted.error());
  }
  const ShardStream::LogParts group{.streams = streams, .parts = parts, .sizes = sizes};
  return ShardStream::Reserve(std::span(&group, 1));
}

core::Result<Reservation> WalQueue::ReserveFlush(std::span<const ShardEntries> parts) {
  ABYSS_DCHECK(ReservationsHeld() == 0, "WAL reservation by a thread holding one");
  if (parts.size() != config_.shard_count) {
    return std::unexpected(Invalid("a flush reservation covers every shard"));
  }
  // Grouped by log, then shard: the order Reserve locks streams in.
  std::vector<std::vector<ShardEntries>> by_log(config_.log_count);
  std::vector<std::vector<ShardStream*>> streams(config_.log_count);
  std::vector<std::vector<uint32_t>> sizes(config_.log_count);
  for (std::size_t p = 0; p < parts.size(); ++p) {
    const ShardEntries& part = parts[p];
    if (part.shard != p) {
      return std::unexpected(Invalid("flush reservation parts must be every shard, in order"));
    }
    for (const core::QueueEntry& entry : part.entries) {
      if (!std::holds_alternative<core::entry::Flush>(entry.payload)) {
        return std::unexpected(Invalid("a flush reservation holds only Flush entries"));
      }
      sizes[part.shard % config_.log_count].push_back(
          static_cast<uint32_t>(frame::EntryFrameSize(entry)));
    }
    by_log[part.shard % config_.log_count].push_back(part);
    streams[part.shard % config_.log_count].push_back(streams_[part.shard].get());
  }
  std::vector<ShardStream::LogParts> groups;
  groups.reserve(config_.log_count);
  for (uint32_t log = 0; log < config_.log_count; ++log) {
    if (by_log[log].empty()) continue;
    if (auto admitted = window_.Admit(logs_[log]->age, core::SteadyTime{}); !admitted) {
      return std::unexpected(admitted.error());
    }
    groups.push_back(
        ShardStream::LogParts{.streams = streams[log], .parts = by_log[log], .sizes = sizes[log]});
  }
  return ShardStream::Reserve(groups);
}

DurableFutures WalQueue::Complete(Reservation&& reservation) {
  Reservation owned = std::move(reservation);
  return owned.Finish();
}

uint64_t WalQueue::ReadyToComplete(uint32_t log) const {
  return logs_.at(log)->ready_to_complete.load(std::memory_order_acquire);
}

core::Result<AppendResult> WalQueue::Append(core::ShardId shard, core::QueueEntry entry,
                                            core::SteadyTime admit_by) {
  auto pending = BeginAppend(shard, std::move(entry), admit_by);
  if (!pending.has_value()) return std::unexpected(pending.error());
  const core::SequenceId seq = pending->seq();
  DurabilityFuture durable = std::move(pending->durable());
  pending->Publish();
  return AppendResult{.seq = seq, .durable = std::move(durable)};
}

core::Result<AppendBatchResult> WalQueue::AppendBatch(core::ShardId shard,
                                                      std::span<const core::QueueEntry> entries,
                                                      core::SteadyTime admit_by) {
  auto pending = BeginAppendBatch(shard, entries, admit_by);
  if (!pending.has_value()) return std::unexpected(pending.error());
  const core::SequenceId first = pending->first_seq();
  const core::SequenceId last = pending->last_seq();
  DurabilityFuture durable = std::move(pending->durable());
  pending->Publish();
  return AppendBatchResult{.first_seq = first, .last_seq = last, .durable = std::move(durable)};
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
  return streams_[shard]->Read(from_seq, max_count, timeout, visible);
}

core::Result<void> WalQueue::CommitOffset(core::ConsumerId consumer, core::ShardId shard,
                                          core::SequenceId seq) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  const auto index = OffsetIndex(consumer, shard);
  if (!index.has_value()) {
    return std::unexpected(
        Invalid("consumer " + std::to_string(consumer) + " does not commit offsets"));
  }
  // Fail-closed durability gate (QUEUE-2/XERR-2/XDUR-1/XDUR-2/HOTC-5): a
  // committed offset never passes the power-durable log, under either
  // class. Consumers clamp or await before committing; this is the
  // backstop.
  const core::SequenceId durable_end = streams_[shard]->DurableEnd(core::Durability::kPowerLoss);
  if (seq >= durable_end) {
    return std::unexpected(core::Error{
        core::ErrorCode::kFailedPrecondition,
        "commit seq " + std::to_string(seq) + " is not below the power-durable WAL end " +
            std::to_string(durable_end) + " for shard " + std::to_string(shard)});
  }

  ABYSS_DCHECK(seq >= core::kFirstSeq, "commit of seq 0, which names no entry");
  const uint64_t want = OffsetCheckpoint::Encode(seq);
  auto& slot = committed_[*index];
  uint64_t current = slot.load(std::memory_order_acquire);
  do {
    if (current == want) return {};
    if (current > want) {
      return std::unexpected(Invalid("commit seq " + std::to_string(seq) +
                                     " is below committed offset " + std::to_string(current) +
                                     " for consumer " + std::to_string(consumer) + " on shard " +
                                     std::to_string(shard)));
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
        Invalid("consumer " + std::to_string(consumer) + " does not commit offsets"));
  }
  return OffsetCheckpoint::Decode(committed_[*index].load(std::memory_order_acquire));
}

core::Result<std::optional<core::SequenceId>> WalQueue::PersistedOffset(core::ConsumerId consumer,
                                                                        core::ShardId shard) const {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  if (!OffsetIndex(consumer, shard).has_value()) {
    return std::unexpected(
        Invalid("consumer " + std::to_string(consumer) + " does not commit offsets"));
  }
  return checkpoint_->Get(consumer, shard);
}

core::Result<core::SequenceId> WalQueue::DurableEnd(core::ShardId shard,
                                                    core::Durability durability) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return streams_[shard]->DurableEnd(durability);
}

core::Result<bool> WalQueue::AwaitDurable(core::ShardId shard, core::SequenceId seq,
                                          core::Durability durability, core::Duration timeout) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return streams_[shard]->AwaitDurable(seq, durability, timeout);
}

core::Result<core::SequenceId> WalQueue::FirstSeq(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return streams_[shard]->first_seq();
}

core::Result<core::SequenceId> WalQueue::OldestRetained(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());

  // The offsets retention actually honours. A floor is a committed
  // seq, inclusive, so the frame it names is kept: one processed
  // frame more than needed, never one fewer.
  core::SequenceId min_offset = streams_[shard]->next_seq();
  for (auto consumer : config_.retention_consumers) {
    const auto floor = checkpoint_->ReclaimFloor(consumer, shard);
    if (!floor.has_value()) return streams_[shard]->first_seq();
    min_offset = std::min(min_offset, *floor);
  }
  return min_offset;
}

core::Result<core::SequenceId> WalQueue::TailSeq(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  // next_seq is the next seq to assign; the highest assigned is one
  // less, and 0 when nothing was.
  return streams_[shard]->next_seq() - 1;
}

core::Result<core::QueueStats> WalQueue::Stats() {
  core::QueueStats stats;  // NOLINT(misc-const-correctness)
  bool first = true;
  for (const auto& stream : streams_) {
    const core::SequenceId head = stream->next_seq();
    const core::SequenceId shard_first = stream->first_seq();
    stats.total_entries += head - shard_first;
    stats.head_seq = std::max(stats.head_seq, head);
    stats.first_seq = first ? shard_first : std::min(stats.first_seq, shard_first);
    first = false;
  }
  // Segments are fixed-size files: the retained run up to the active
  // one, the spares past it and the free pool.
  for (const auto& unit : logs_) {
    const Log& log = *unit->log;
    const uint64_t active = log.ReservedTail() / frame_space_;
    const auto oldest = log.SealedSegments(1);
    const uint64_t from = oldest.empty() ? active : oldest.front().ordinal;
    const uint64_t files = (active - from + 1) + log.spare_count() + log.free_count();
    stats.total_bytes += files * config_.segment_size_bytes;
  }
  return stats;
}

std::vector<SegmentRegistry::SealedSegmentInfo> WalQueue::ListSealedSegments(
    uint32_t log, std::size_t max_count) const {
  std::vector<SealedSegmentInfo> out;
  if (log >= logs_.size()) return out;
  for (auto& segment : logs_[log]->log->SealedSegments(max_count)) {
    out.push_back(SealedSegmentInfo{.log = log,
                                    .ordinal = segment.ordinal,
                                    .shards = std::move(segment.shards),
                                    .sealed_at = segment.sealed_at});
  }
  return out;
}

std::vector<SegmentRegistry::SealedSegmentInfo> WalQueue::ListSealedSegments() const {
  std::vector<SealedSegmentInfo> out;
  for (uint32_t log = 0; log < config_.log_count; ++log) {
    auto segments = ListSealedSegments(log, std::numeric_limits<std::size_t>::max());
    std::ranges::move(segments, std::back_inserter(out));
  }
  return out;
}

// Readers that find the segment gone wait on reclaim_mu, so they see
// the streams' new floors once this returns.
core::Result<void> WalQueue::RemoveSegment(uint32_t log, uint64_t ordinal) {
  if (log >= logs_.size()) {
    return std::unexpected(Invalid("log " + std::to_string(log) + " out of range"));
  }
  LogUnit& unit = *logs_[log];
  const std::scoped_lock lock(unit.reclaim_mu);
  const auto oldest = unit.log->SealedSegments(1);
  if (oldest.empty() || oldest.front().ordinal != ordinal) {
    return std::unexpected(core::Error{core::ErrorCode::kFailedPrecondition,
                                       "segment " + std::to_string(ordinal) +
                                           " is not the oldest sealed segment of log " +
                                           std::to_string(log)});
  }
  auto reclaimed = unit.log->Reclaim(ordinal);
  // A file that could not be removed yet has still left the table.
  const auto after = unit.log->SealedSegments(1);
  if (!after.empty() && after.front().ordinal == ordinal) return reclaimed;

  std::vector<std::optional<core::SequenceId>> max_seq(config_.shard_count);
  for (const auto& range : oldest.front().shards) max_seq[range.shard] = range.max_seq;
  const LogPosition retained_from = (ordinal + 1) * frame_space_;
  for (core::ShardId shard = log; shard < config_.shard_count; shard += config_.log_count) {
    streams_[shard]->Reclaimed(max_seq[shard], retained_from);
  }
  unit.reclaims.fetch_add(1, std::memory_order_release);
  return reclaimed;
}

}  // namespace abyss::queue
