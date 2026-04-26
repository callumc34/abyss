#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string_view>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/command_dispatcher.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/hot_store.h"
#include "abyss/core/queue.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::engine {

struct TieringEngineConfig {
  uint32_t shard_count = 1;

  // Maximum time DispatchWrite waits for a write to be durable AND applied by
  // the responsible consumer before returning to the client. The write remains
  // durable in the queue and will eventually apply; the client just did not
  // observe the outcome within this bound.
  std::chrono::milliseconds write_timeout{5000};

  // Lower bound on the consumer-apply budget after the durable wait completes.
  // Keeps a slow fsync from leaving ~0ms for the RPC wait. Total latency is
  // bounded by write_timeout * (1 + min_rpc_wait_fraction).
  double min_rpc_wait_fraction = 0.5;
};

struct TieringEngineMetrics {
  uint64_t promotion_append_failures = 0;
};

class TieringEngine : public core::CommandDispatcher {
 public:
  TieringEngine(core::Queue& queue, core::HotStore& hot_store, core::ColdStore& cold_store,
                consumer::CompactionBufferRouter& buffer_router, core::ConsumerRpc& rpc,
                TieringEngineConfig config);

  core::Result<core::RespValue> DispatchRead(std::string_view name,
                                             const core::RespCommand& cmd) override;
  core::Result<core::RespValue> DispatchWrite(std::string_view name,
                                              core::RespCommand cmd) override;
  core::Result<core::RespValue> DispatchConditional(std::string_view name, core::RespCommand cmd,
                                                    core::PredicateFlags flags) override;

  TieringEngineMetrics Snapshot() const;

 private:
  void PromoteThroughQueue(std::string_view key);

  core::Queue& queue_;
  core::HotStore& hot_store_;
  core::ColdStore& cold_store_;
  consumer::CompactionBufferRouter& buffer_router_;
  core::ConsumerRpc& rpc_;
  TieringEngineConfig config_;

  std::atomic<uint64_t> promotion_append_failures_{0};
};

}  // namespace abyss::engine
