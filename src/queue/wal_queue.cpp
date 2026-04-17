#include "abyss/queue/wal_queue.h"

#include <utility>

namespace abyss::queue {

WalQueue::WalQueue(WalConfig config) : config_(std::move(config)) {}
WalQueue::~WalQueue() = default;

core::Result<core::SequenceId> WalQueue::Append(core::ShardId /*shard*/,
                                                core::QueueEntry /*entry*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::Append requires WAL segment writer"));
}

core::Result<core::SequenceId> WalQueue::AppendBatch(
    core::ShardId /*shard*/, std::span<const core::QueueEntry> /*entries*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::AppendBatch requires WAL segment writer"));
}

core::Result<std::vector<core::QueueEntry>> WalQueue::Read(core::ConsumerId /*consumer*/,
                                                           core::ShardId /*shard*/,
                                                           size_t /*max_count*/,
                                                           core::Duration /*timeout*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::Read requires WAL segment reader"));
}

core::Result<void> WalQueue::Ack(core::ConsumerId /*consumer*/, core::ShardId /*shard*/,
                                 core::SequenceId /*seq*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::Ack requires offset persistence"));
}

core::Result<core::SequenceId> WalQueue::OldestRetained(core::ShardId /*shard*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::OldestRetained requires segment GC"));
}

core::Result<core::QueueStats> WalQueue::Stats() {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::Stats requires segment metadata"));
}

}  // namespace abyss::queue
