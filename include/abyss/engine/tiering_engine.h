#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string_view>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/consumer/hot_consumer_progress.h"
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

  // How long DispatchHashRead waits for the cold consumer to catch up to hot
  // before consulting the buffer overlay.
  std::chrono::milliseconds buffer_consistency_wait_timeout{100};
};

struct TieringEngineMetrics {
  uint64_t promotion_append_failures = 0;
  uint64_t flush_total = 0;
  uint64_t flush_durable_failures = 0;
  uint64_t flush_consumer_timeouts = 0;
  uint64_t flush_append_failures = 0;
  // Reads that fell through hot but timed out waiting for the cold consumer
  // to catch up to hot's settled seq — surface as an error rather than serving
  // a buffer/cold snapshot that lags hot.
  uint64_t read_buffer_wait_timeouts = 0;
};

class TieringEngine : public core::CommandDispatcher {
 public:
  TieringEngine(core::Queue& queue, core::HotStore& hot_store, core::ColdStore& cold_store,
                consumer::CompactionBufferRouter& buffer_router,
                const consumer::HotConsumerProgress& hot_progress, core::ConsumerRpc& rpc,
                TieringEngineConfig config);

  core::Result<core::RespValue> DispatchRead(std::string_view name,
                                             const core::RespCommand& cmd) override;
  core::Result<core::RespValue> DispatchWrite(std::string_view name,
                                              core::RespCommand cmd) override;
  core::Result<core::RespValue> DispatchConditional(std::string_view name, core::RespCommand cmd,
                                                    core::PredicateFlags flags) override;
  core::Result<core::RespValue> DispatchFanOut(core::MultiKeyKind kind,
                                               core::RespCommand cmd) override;
  core::Result<core::RespValue> DispatchFlush(core::FlushTarget target) override;

  TieringEngineMetrics Snapshot() const;

 private:
  void PromoteThroughQueue(std::string_view key);
  core::Result<core::RespValue> DispatchHashRead(const core::ops::ReadOp& op);
  core::Result<core::RespValue> DispatchSingleKeyRead(const core::ops::ReadOp& op);
  core::Result<core::RespValue> DispatchSingleKeyWrite(core::RespCommand cmd);

  core::Result<core::RespValue> FanOutMget(const core::RespCommand& cmd);
  core::Result<core::RespValue> FanOutExists(const core::RespCommand& cmd);
  core::Result<core::RespValue> FanOutMset(const core::RespCommand& cmd);
  core::Result<core::RespValue> FanOutDel(std::string_view name, const core::RespCommand& cmd);

  // RespValue::Array on success (per-sub results), RespValue::Error on
  // whole-fan-out timeout. Structural failures surface as Result errors.
  core::Result<core::RespValue> FanOutWrite(const std::vector<core::RespCommand>& subs);

  core::Result<bool> ProbeKeyExists(std::string_view key);

  // Returns true if the per-shard cold consumer has caught up to hot's settled
  // seq within the configured budget; false on timeout. Callers that surface
  // staleness through the buffer overlay (single-key reads after hot miss,
  // ProbeKeyExists) gate on this so they never merge against an overlay older
  // than hot's view. See ADP-006 §Read Path.
  bool WaitForBufferConsistency(std::string_view key);

  core::Queue& queue_;
  core::HotStore& hot_store_;
  core::ColdStore& cold_store_;
  consumer::CompactionBufferRouter& buffer_router_;
  const consumer::HotConsumerProgress& hot_progress_;
  core::ConsumerRpc& rpc_;
  TieringEngineConfig config_;

  std::atomic<uint64_t> promotion_append_failures_{0};
  std::atomic<uint64_t> flush_total_{0};
  std::atomic<uint64_t> flush_durable_failures_{0};
  std::atomic<uint64_t> flush_consumer_timeouts_{0};
  std::atomic<uint64_t> flush_append_failures_{0};
  std::atomic<uint64_t> read_buffer_wait_timeouts_{0};
};

}  // namespace abyss::engine
