#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <string_view>

#include "histogram.h"
#include "key_dist.h"
#include "workload.h"

namespace abyss::perf {

struct RunLoopConfig {
  int workers = 1;
  std::chrono::seconds duration{30};
  std::chrono::seconds warmup{0};
  // Total requests/s across workers (0 = closed loop), split exactly by
  // WorkerRate.
  uint64_t target_rate_ops = 0;
  uint64_t key_count = 1;
  KeyDistConfig key_distribution;
  uint64_t value_size_bytes = 64;
  OperationMix mix;
};

// Samples belong to the measured window by intended send time (open
// loop) or send time (closed loop), never by completion time.
struct RunLoopResult {
  std::map<std::string, Histogram> per_op_histograms;
  // Successful measured ops; a failed op is counted, never timed.
  std::map<std::string, uint64_t> per_op_counts;
  std::map<std::string, uint64_t> per_op_errors;
  // Open loop only: actual minus intended send time, one per slot.
  Histogram send_lag;
  bool open_loop = false;
  // CPU of the driver threads over the measured window.
  std::chrono::nanoseconds driver_cpu{0};
  // Until the later of run end and the last measured completion.
  std::chrono::nanoseconds measured_duration{0};
};

// Operation callback. Invoked once per scheduled iteration on the chosen op
// name with the chosen key index; returns false when the op failed. The
// function must not throw.
using OpFn = std::function<bool(int worker_id, std::string_view op_name, uint64_t key_index)>;

// Drives workers according to config and invokes op() once per iteration.
// Open loop issues every slot before run end, late if necessary, and
// measures it from its intended send time (wrk2); closed loop measures
// from the send. Per-worker histograms are merged into the result.
RunLoopResult RunLoop(const RunLoopConfig& config, const OpFn& op);

// A connection holding several requests in flight; replies arrive in
// send order.
class PipelinedConnection {
 public:
  // kError: the oldest request was answered with an error reply.
  enum class Await : uint8_t { kReply, kError, kTimeout, kFailed };

  PipelinedConnection() = default;
  virtual ~PipelinedConnection() = default;
  PipelinedConnection(const PipelinedConnection&) = delete;
  PipelinedConnection& operator=(const PipelinedConnection&) = delete;
  PipelinedConnection(PipelinedConnection&&) = delete;
  PipelinedConnection& operator=(PipelinedConnection&&) = delete;

  // Queues one request; it reaches the server on the next Flush.
  virtual bool Send(std::string_view op_name, uint64_t key_index) = 0;
  virtual bool Flush() = 0;
  // Waits until `deadline` for the oldest outstanding reply; a deadline
  // already passed polls without blocking.
  virtual Await AwaitReply(std::chrono::steady_clock::time_point deadline) = 0;
};

// One worker per connection, each with up to `pipeline_depth` requests
// in flight; config.workers is not consulted. Open loop measures every
// request from its slot's intended send time (wrk2), so a request held
// back by a full window carries that wait. A burst slot carries
// `pipeline_depth` requests at the same request rate. Closed loop keeps
// the window full and measures from the actual send. A failed
// connection ends its worker.
RunLoopResult RunPipelinedLoop(const RunLoopConfig& config, int pipeline_depth, Arrival arrival,
                               std::span<PipelinedConnection* const> connections);

// Worker `index`'s share of `total_ops`: the first total % workers
// workers take one extra request/s, so the shares sum to the total.
uint64_t WorkerRate(uint64_t total_ops, size_t workers, size_t index);

uint64_t TotalErrors(const RunLoopResult& result);

// Successful measured ops per second of the measured window.
double AchievedOps(const RunLoopResult& result);

// Materialise a RunLoopConfig from a WorkloadConfig. The total target rate is
// divided across workers; the workload's mix and key distribution flow through.
RunLoopConfig RunLoopConfigFromWorkload(const WorkloadConfig& workload);

}  // namespace abyss::perf
