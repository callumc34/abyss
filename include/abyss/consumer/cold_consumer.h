#pragma once

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

class ColdConsumer {
 public:
  ColdConsumer(core::Queue& queue, core::ColdStore& store, CompactionBuffer& buffer,
               core::ShardId shard);

  void Start();
  void Stop();

  const CompactionBuffer& Buffer() const { return buffer_; }

 private:
  core::Queue& queue_;
  core::ColdStore& store_;
  CompactionBuffer& buffer_;
  core::ShardId shard_;
};

}  // namespace abyss::consumer
