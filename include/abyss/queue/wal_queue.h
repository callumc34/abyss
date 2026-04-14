#pragma once

#include <string>

#include "abyss/core/queue.h"

namespace abyss::queue {

struct WalConfig {
  std::string wal_path;
  size_t segment_size_bytes = 67108864;
};

class WalQueue : public core::Queue {
 public:
  explicit WalQueue(WalConfig config);
  ~WalQueue() override;

  core::Result<core::SequenceId> Append(core::ShardId shard, core::RespCommand cmd) override;
  core::Result<core::SequenceId> AppendBatch(core::ShardId shard,
                                             std::span<const core::RespCommand> cmds) override;
  core::Result<std::vector<core::LogEntry>> Read(core::ConsumerId consumer, core::ShardId shard,
                                                 size_t max_count, core::Duration timeout) override;
  core::Result<void> Ack(core::ConsumerId consumer, core::ShardId shard,
                         core::SequenceId seq) override;
  core::Result<core::SequenceId> OldestRetained(core::ShardId shard) override;
  core::Result<core::QueueStats> Stats() override;

 private:
  WalConfig config_;
};

}  // namespace abyss::queue
