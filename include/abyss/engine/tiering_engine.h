#pragma once

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/engine/write_promise.h"

namespace abyss::engine {

class TieringEngine {
 public:
  TieringEngine(core::Queue& queue, core::HotStore& hot_store, core::ColdStore& cold_store,
                consumer::CompactionBuffer& buffer);

  core::Result<core::RespValue> HandleRead(const core::RespCommand& cmd);
  core::Result<core::RespValue> HandleWrite(core::ShardId shard, core::RespCommand cmd);

  WritePromiseMap& Promises() { return promises_; }

 private:
  core::Queue& queue_;
  core::HotStore& hot_store_;
  core::ColdStore& cold_store_;
  consumer::CompactionBuffer& buffer_;
  WritePromiseMap promises_;
};

}  // namespace abyss::engine
