#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/command_dispatcher.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/engine/sequencer.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"

namespace abyss::engine {

struct TieringEngineConfig {
  uint32_t shard_count = 1;

  // Bounds a read's wait for the writes it saw to become durable at the
  // ack class (the sequencer bounds writes by its own).
  std::chrono::milliseconds write_timeout{5000};

  // Deadline carried into every cold point-read at the engine→cold edge. The
  // 5ms default is the ADP-003 <5ms p99 cold-read SLA: a point read that cannot
  // be served within the bound fails closed (kTimeout) rather than blocking a
  // reactor thread (invariant 5).
  std::chrono::milliseconds cold_read_deadline{5};

  // Deadline carried into cold collection scans (SMEMBERS/ZRANGE/HGETALL/…).
  // Scans over a large cold-resident collection legitimately take longer than a
  // point read, so this knob is separate and larger; operators tune it for
  // their largest collections. A scan that exceeds it surfaces a deadline error
  // and the cold-scan-deadline-exceeded metric — never a silently truncated
  // result (decision 4 / invariant 5). See docs/operations/failure-modes.md.
  std::chrono::milliseconds cold_scan_deadline{50};
};

// Dispatch: every write goes through the sequencer; reads answer from
// hot, fenced on what they saw, or fall through to buffer and cold.
class TieringEngine : public core::CommandDispatcher {
 public:
  TieringEngine(hot::ShardedHotStore& hot_store, core::ColdStore& cold_store,
                consumer::CompactionBufferRouter& buffer_router, Sequencer& sequencer,
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

 private:
  core::Result<core::RespValue> DispatchHashRead(const core::ops::ReadOp& op);
  core::Result<core::RespValue> DispatchSingleKeyRead(const core::ops::ReadOp& op);
  // A hot answer once the writes it saw are durable at the ack class.
  core::Result<core::RespValue> Fenced(core::ShardId shard, hot::ShardedHotStore::HotRead read);

  // Uniform Hot → Buffer → Cold overlay for set/zset SCALAR reads
  // (SISMEMBER/SCARD/ZSCORE/ZCARD): the buffer overlay (Exec) is consulted
  // FIRST so a buffered partial SREM/ZREM against a cold-resident collection
  // wins (read-after-write), and a true buffer miss (kNotFound) falls through
  // to cold WITH the cold-read deadline. No collection read shape may skip the
  // buffer tier (ADP-006 invariant 1 — fixes ENGINE-2).
  core::Result<core::RespValue> MergeCollectionScalar(const core::ops::ReadOp& op);

  // Cold deadline appropriate to the op shape: scans (SMEMBERS/ZRANGE/HGETALL/
  // HKEYS/HVALS) get cold_scan_deadline, everything else the cold_read_deadline
  // point-read SLA. Threaded into every engine→cold Exec so no cold path is
  // unbounded (COLD-2).
  core::Duration ColdDeadlineFor(const core::ops::ReadOp& op) const;

  // The single engine→cold read edge: runs cold_store_.Exec with the
  // shape-appropriate deadline (COLD-2). A deadline-exceeded scan (kTimeout)
  // bumps the cold-scan-deadline-exceeded metric and surfaces the error
  // verbatim — never a silently truncated partial result (decision 4).
  core::Result<core::RespValue> ColdExec(const core::ops::ReadOp& op);

  core::Result<core::RespValue> FanOutMget(const core::RespCommand& cmd);
  core::Result<core::RespValue> FanOutExists(const core::RespCommand& cmd);

  core::Result<bool> ProbeKeyExists(std::string_view key);

  hot::ShardedHotStore& hot_store_;
  core::ColdStore& cold_store_;
  consumer::CompactionBufferRouter& buffer_router_;
  Sequencer& sequencer_;
  TieringEngineConfig config_;

  metrics::CounterHandle hits_hot_;
  metrics::CounterHandle hits_buffer_;
  metrics::CounterHandle hits_cold_;
  metrics::CounterHandle misses_;
  metrics::CounterHandle cold_scan_deadline_exceeded_;
};

}  // namespace abyss::engine
