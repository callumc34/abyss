#include "abyss/queue/wal_queue.h"

#include <utility>

namespace abyss::queue {

WalQueue::WalQueue(WalConfig config) : config_(std::move(config)) {}
WalQueue::~WalQueue() = default;

core::Result<core::SequenceId> WalQueue::Append(core::ShardId /*shard*/,
                                                core::RespCommand /*cmd*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::Append not implemented"));
}

core::Result<core::SequenceId> WalQueue::AppendBatch(core::ShardId /*shard*/,
                                                     std::span<const core::RespCommand> /*cmds*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::AppendBatch not implemented"));
}

core::Result<std::vector<core::LogEntry>> WalQueue::Read(core::ConsumerId /*consumer*/,
                                                         core::ShardId /*shard*/,
                                                         size_t /*max_count*/,
                                                         core::Duration /*timeout*/) {
  return std::unexpected(core::Error(core::ErrorCode::kInternal, "WalQueue::Read not implemented"));
}

core::Result<void> WalQueue::Ack(core::ConsumerId /*consumer*/, core::ShardId /*shard*/,
                                 core::SequenceId /*seq*/) {
  return std::unexpected(core::Error(core::ErrorCode::kInternal, "WalQueue::Ack not implemented"));
}

core::Result<core::SequenceId> WalQueue::OldestRetained(core::ShardId /*shard*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::OldestRetained not implemented"));
}

core::Result<core::QueueStats> WalQueue::Stats() {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "WalQueue::Stats not implemented"));
}

}  // namespace abyss::queue
