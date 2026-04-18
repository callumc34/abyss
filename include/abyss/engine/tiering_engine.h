#pragma once

#include <cstdint>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/command_dispatcher.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::engine {

class TieringEngine : public core::CommandDispatcher {
 public:
  TieringEngine(core::Queue& queue, core::HotStore& hot_store, core::ColdStore& cold_store,
                consumer::CompactionBufferRouter& buffer_router, core::ConsumerRpc& rpc,
                uint32_t shard_count);

  core::Result<core::RespValue> DispatchRead(std::string_view name,
                                             const core::RespCommand& cmd) override;
  core::Result<core::RespValue> DispatchWrite(std::string_view name,
                                              core::RespCommand cmd) override;

 private:
  core::Queue& queue_;
  core::HotStore& hot_store_;
  core::ColdStore& cold_store_;
  consumer::CompactionBufferRouter& buffer_router_;
  [[maybe_unused]] core::ConsumerRpc& rpc_;
  uint32_t shard_count_;
};

}  // namespace abyss::engine
