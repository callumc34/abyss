#include "abyss/queue/wal_queue.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <utility>

#include "abyss/core/fire_and_forget.h"
#include "abyss/queue/file_offset_store.h"
#include "shard_state.h"

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

  std::unique_ptr<WalQueue> queue(new WalQueue(std::move(config)));
  auto init = queue->Initialize();
  if (!init.has_value()) return std::unexpected(init.error());
  queue->recovering_.store(false, std::memory_order_release);
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
        .commit = config_.commit,
        .on_rotate = [this] { RunReaper(); },
    });
    if (!state.has_value()) return std::unexpected(state.error());

    for (auto consumer : config_.retention_consumers) {
      if (auto persisted = offsets_->Get(consumer, shard); persisted.has_value()) {
        if (*persisted > (*state)->head_seq()) {
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
  // Reaper runs on a timer; record failures rather than let them go silent.
  core::FireAndForget(reaper_->RunOnce(), reaper_failures_);
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
  const auto persisted = offsets_->Get(consumer, shard);
  const core::SequenceId from_seq = persisted.has_value() ? *persisted + 1 : 0;
  return shards_[shard]->Read(from_seq, max_count, timeout);
}

core::Result<void> WalQueue::Ack(core::ConsumerId consumer, core::ShardId shard,
                                 core::SequenceId seq) {
  if (auto v = ValidateShard(shard); !v.has_value()) return std::unexpected(v.error());
  auto set = offsets_->Set(consumer, shard, seq);
  if (!set.has_value()) return set;
  RunReaper();
  return {};
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
