#include "abyss/queue/wal_queue.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <utility>

#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/platform/fs.h"
#include "abyss/queue/file_offset_store.h"
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

  std::error_code ec;
  std::filesystem::create_directories(config.wal_path, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "create wal_path: " + ec.message()});
  }

  // Surface the volume's real durability posture before any ack is given
  // (invariant 5). A retention-bearing policy on a volume that cannot make
  // directory renames durable is a refuse-to-start condition: the persisted-ack
  // <= durable-tail contract A1 relies on cannot hold there.
  const bool durability_required = config.commit.policy != FsyncPolicy::kNone;
  if (auto cap = platform::fs::ProbeDurability(config.wal_path); cap.has_value()) {
    metrics::Registry::Instance()
        .Gauge(metrics::names::kFsDurableDirSupported)
        .Set(cap->dir_sync_supported ? 1.0 : 0.0);
    ABYSS_LOG_INFO("WAL durability probe", {"path", std::string_view{config.wal_path}},
                   {"dir_sync_supported", cap->dir_sync_supported},
                   {"fsync_backend", static_cast<int64_t>(cap->backend)});
    if (durability_required && !cap->dir_sync_supported) {
      ABYSS_LOG_CRITICAL("data volume cannot make directory entries durable",
                         {"path", std::string_view{config.wal_path}});
      return std::unexpected(core::Error{
          core::ErrorCode::kFailedPrecondition,
          "data volume does not support durable directory fsync; refusing to start with a "
          "retention fsync policy (set fsync_policy=none to override at the cost of durability)"});
    }
  } else {
    return std::unexpected(cap.error());
  }

  std::unique_ptr<WalQueue> queue(new WalQueue(std::move(config)));
  auto init = queue->Initialize();
  if (!init.has_value()) return std::unexpected(init.error());
  queue->recovering_.store(false, std::memory_order_release);

  core::SequenceId max_head = 0;
  for (const auto& shard : queue->shards_) {
    max_head = std::max(max_head, shard->head_seq());
  }
  ABYSS_LOG_INFO("WAL opened", {"path", std::string_view{queue->config_.wal_path}},
                 {"shard_count", static_cast<int64_t>(queue->config_.shard_count)},
                 {"segment_size_bytes", static_cast<uint64_t>(queue->config_.segment_size_bytes)},
                 {"min_retention_s", static_cast<int64_t>(queue->config_.min_retention.count())},
                 {"head_seq", static_cast<uint64_t>(max_head)});
  return queue;
}

WalQueue::WalQueue(WalConfig config) : config_(std::move(config)) {}

// NOLINTNEXTLINE(modernize-use-equals-default)
WalQueue::~WalQueue() {
  for (auto& shard : shards_) {
    if (shard) shard->Shutdown();
  }
}

core::Result<void> WalQueue::Initialize() {
  const auto offsets_dir = std::filesystem::path(config_.wal_path) / OffsetsDirName();
  auto offsets = FileOffsetStore::Open({.directory = offsets_dir.string()});
  if (!offsets.has_value()) return std::unexpected(offsets.error());
  offsets_ = std::move(*offsets);

  shards_.reserve(config_.shard_count);
  for (core::ShardId shard = 0; shard < config_.shard_count; ++shard) {
    const auto shard_dir = std::filesystem::path(config_.wal_path) / ShardDirName(shard);
    auto state = ShardState::Open({
        .shard = shard,
        .directory = shard_dir.string(),
        .segment_size_bytes = config_.segment_size_bytes,
        .max_value_size_bytes = config_.max_value_size_bytes,
        .commit = config_.commit,
        .on_rotate = [this] { RunReaper(); },
    });
    if (!state.has_value()) return std::unexpected(state.error());

    for (auto consumer : config_.retention_consumers) {
      if (auto persisted = offsets_->Get(consumer, shard); persisted.has_value()) {
        if (*persisted > (*state)->head_seq()) {
          ABYSS_LOG_CRITICAL("persisted offset exceeds WAL head",
                             {"consumer", static_cast<uint64_t>(consumer)},
                             {"shard", static_cast<int64_t>(shard)},
                             {"persisted", static_cast<uint64_t>(*persisted)},
                             {"head_seq", static_cast<uint64_t>((*state)->head_seq())});
          return std::unexpected(
              core::Error{core::ErrorCode::kCorruption,
                          "persisted offset exceeds WAL head for consumer/shard"});
        }
      }
    }

    shards_.push_back(std::move(*state));
  }

  reaper_ = std::make_unique<SegmentReaper>(*this, *offsets_,
                                            SegmentReaperConfig{
                                                .consumers = config_.retention_consumers,
                                                .min_retention = config_.min_retention,
                                            });
  return {};
}

void WalQueue::RunReaper() {
  if (!reaper_) return;
  auto result = reaper_->RunOnce();
  if (!result.has_value()) {
    reaper_failures_.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_WARN("segment reaper failed", {"err", std::string_view{result.error().message()}});
  }
}

core::Result<void> WalQueue::ValidateShard(core::ShardId shard) const {
  if (shard >= config_.shard_count) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument,
                                       "shard " + std::to_string(shard) + " out of range"});
  }
  return {};
}

core::Result<PendingAppend> WalQueue::BeginAppend(core::ShardId shard, core::QueueEntry entry) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->BeginAppend(std::move(entry));
}

core::Result<PendingBatchAppend> WalQueue::BeginAppendBatch(
    core::ShardId shard, std::span<const core::QueueEntry> entries) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->BeginAppendBatch(entries);
}

core::Result<AppendResult> WalQueue::Append(core::ShardId shard, core::QueueEntry entry) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->Append(std::move(entry));
}

core::Result<AppendBatchResult> WalQueue::AppendBatch(core::ShardId shard,
                                                      std::span<const core::QueueEntry> entries) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->AppendBatch(entries);
}

core::Result<std::vector<core::QueueEntry>> WalQueue::Read(core::ConsumerId consumer,
                                                           core::ShardId shard, size_t max_count,
                                                           core::Duration timeout) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  const auto offset = GetOffset(consumer, shard);
  const core::SequenceId from_seq = offset.has_value() ? *offset + 1 : 0;
  return shards_[shard]->Read(from_seq, max_count, timeout);
}

core::Result<void> WalQueue::Ack(core::ConsumerId consumer, core::ShardId shard,
                                 core::SequenceId seq) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  if (IsVolatile(consumer)) {
    SetVolatileOffset(consumer, shard, seq);
    return {};
  }
  // Fail-closed durability gate (QUEUE-2/XERR-2/XDUR-1/XDUR-2/HOTC-5): a
  // retention consumer's persisted offset can never advance past the durable
  // WAL tail. Consumers (cold/resolver) clamp to DurableSeq or AwaitDurable
  // before acking; this is the backstop. Under fsync_none durable_seq tracks
  // the published seq so the gate is a correct no-op (Decision 1). HasDurable
  // disambiguates the seq-0 edge: a 0 watermark with nothing durable must
  // reject ack(0), but once seq 0 is durable the same ack is accepted.
  const bool any_durable = shards_[shard]->HasDurable();
  const core::SequenceId durable = shards_[shard]->DurableSeq();
  if (!any_durable || seq > durable) {
    return std::unexpected(core::Error{core::ErrorCode::kFailedPrecondition,
                                       "ack seq " + std::to_string(seq) +
                                           " exceeds durable WAL tail " + std::to_string(durable) +
                                           " for shard " + std::to_string(shard)});
  }
  auto set = offsets_->Set(consumer, shard, seq);
  if (!set.has_value()) return set;
  RunReaper();
  return {};
}

core::Result<core::SequenceId> WalQueue::DurableSeq(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->DurableSeq();
}

core::Result<bool> WalQueue::AwaitDurable(core::ShardId shard, core::SequenceId seq,
                                          core::Duration timeout) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return shards_[shard]->AwaitDurable(seq, timeout);
}

core::Result<core::SequenceId> WalQueue::OldestRetained(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());

  core::SequenceId min_ack = shards_[shard]->head_seq();
  for (auto consumer : config_.retention_consumers) {
    auto ack = offsets_->Get(consumer, shard);
    if (!ack.has_value()) {
      return shards_[shard]->tail_seq();
    }
    min_ack = std::min(min_ack, *ack);
  }
  return min_ack;
}

core::Result<core::SequenceId> WalQueue::TailSeq(core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  // head_seq is the next seq to assign; the highest assigned (matching what
  // a consumer's HighestSettledSeq will reach once caught up) is one less.
  const auto head = shards_[shard]->head_seq();
  return head > 0 ? head - 1 : 0;
}

core::Result<core::SequenceId> WalQueue::AckOffset(core::ConsumerId consumer, core::ShardId shard) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  return GetOffset(consumer, shard).value_or(0);
}

bool WalQueue::IsVolatile(core::ConsumerId consumer) const {
  return std::ranges::find(config_.volatile_consumers, consumer) !=
         config_.volatile_consumers.end();
}

namespace {
constexpr uint64_t kShardKeyShift = 32;
uint64_t MakeOffsetKey(core::ConsumerId consumer, core::ShardId shard) {
  return (static_cast<uint64_t>(consumer) << kShardKeyShift) | static_cast<uint64_t>(shard);
}
}  // namespace

std::optional<core::SequenceId> WalQueue::GetOffset(core::ConsumerId consumer,
                                                    core::ShardId shard) const {
  if (IsVolatile(consumer)) {
    const std::scoped_lock lock(volatile_mu_);
    auto it = volatile_offsets_.find(MakeOffsetKey(consumer, shard));
    if (it == volatile_offsets_.end()) return std::nullopt;
    return it->second;
  }
  return offsets_->Get(consumer, shard);
}

void WalQueue::SetVolatileOffset(core::ConsumerId consumer, core::ShardId shard,
                                 core::SequenceId seq) {
  const std::scoped_lock lock(volatile_mu_);
  volatile_offsets_[MakeOffsetKey(consumer, shard)] = seq;
}

core::Result<core::QueueStats> WalQueue::Stats() {
  core::QueueStats stats;  // NOLINT(misc-const-correctness)
  for (const auto& shard : shards_) {
    stats.total_entries += shard->total_entries();
    stats.total_bytes += shard->total_bytes();
    stats.head_seq = std::max(stats.head_seq, shard->head_seq());
    stats.tail_seq = std::max(stats.tail_seq, shard->tail_seq());
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
