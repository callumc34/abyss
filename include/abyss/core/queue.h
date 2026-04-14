#pragma once

#include <span>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct LogEntry {
  SequenceId seq;
  RespCommand cmd;
  SteadyTime appended_at;
};

struct QueueStats {
  uint64_t total_entries = 0;
  uint64_t total_bytes = 0;
  SequenceId head_seq = 0;
  SequenceId tail_seq = 0;
};

class Queue {
 public:
  virtual ~Queue() = default;

  virtual Result<SequenceId> Append(ShardId shard, RespCommand cmd) = 0;
  virtual Result<SequenceId> AppendBatch(ShardId shard, std::span<const RespCommand> cmds) = 0;

  virtual Result<std::vector<LogEntry>> Read(ConsumerId consumer, ShardId shard, size_t max_count,
                                             Duration timeout) = 0;

  virtual Result<void> Ack(ConsumerId consumer, ShardId shard, SequenceId seq) = 0;
  virtual Result<SequenceId> OldestRetained(ShardId shard) = 0;
  virtual Result<QueueStats> Stats() = 0;
};

}  // namespace abyss::core
