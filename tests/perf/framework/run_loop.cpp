#include "run_loop.h"

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include <algorithm>
#include <chrono>
#include <ctime>
#include <deque>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "scheduler.h"

namespace abyss::perf {

namespace {

constexpr std::chrono::milliseconds kSetupSlack{100};

struct OpSelector {
  std::vector<std::string> names;
  std::vector<double> cumulative;

  size_t PickIndex(double u) const {
    for (size_t i = 0; i < cumulative.size(); ++i) {
      if (u <= cumulative[i]) {
        return i;
      }
    }
    return names.size() - 1;
  }

  std::string_view Pick(double u) const { return names[PickIndex(u)]; }
};

OpSelector BuildSelector(const OperationMix& mix) {
  OpSelector sel;
  double running = 0.0;
  for (const auto& [name, weight] : mix.weights) {
    running += weight;
    sel.names.push_back(name);
    sel.cumulative.push_back(running);
  }
  if (!sel.cumulative.empty()) {
    sel.cumulative.back() = 1.0;
  }
  return sel;
}

struct WorkerState {
  std::map<std::string, Histogram> histograms;
  std::map<std::string, uint64_t> counts;
  std::map<std::string, uint64_t> errors;
  Histogram send_lag;
  std::chrono::steady_clock::time_point last_done{};
  std::chrono::nanoseconds cpu{0};
};

std::chrono::nanoseconds ThreadCpuNow() {
  timespec ts{};
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec};
}

// This thread's CPU from the first moment it sees the measured window
// until it exits.
class ThreadCpuWindow {
 public:
  explicit ThreadCpuWindow(std::chrono::nanoseconds& out) : out_(out) {}
  ~ThreadCpuWindow() {
    if (start_.has_value()) out_ = ThreadCpuNow() - *start_;
  }
  ThreadCpuWindow(const ThreadCpuWindow&) = delete;
  ThreadCpuWindow& operator=(const ThreadCpuWindow&) = delete;
  ThreadCpuWindow(ThreadCpuWindow&&) = delete;
  ThreadCpuWindow& operator=(ThreadCpuWindow&&) = delete;

  void StartAt(std::chrono::steady_clock::time_point now,
               std::chrono::steady_clock::time_point warmup_end) {
    if (!start_.has_value() && now >= warmup_end) start_ = ThreadCpuNow();
  }

 private:
  std::chrono::nanoseconds& out_;
  std::optional<std::chrono::nanoseconds> start_;
};

std::vector<WorkerState> MakeWorkerStates(size_t workers, const OpSelector& selector) {
  std::vector<WorkerState> states(workers);
  for (auto& state : states) {
    for (const auto& name : selector.names) {
      state.histograms.try_emplace(name);
      state.counts[name] = 0;
      state.errors[name] = 0;
    }
  }
  return states;
}

RunLoopResult MergeWorkerStates(const OpSelector& selector, std::vector<WorkerState>& states,
                                bool open_loop, std::chrono::steady_clock::time_point warmup_end,
                                std::chrono::steady_clock::time_point run_end) {
  RunLoopResult result;
  for (const auto& name : selector.names) {
    result.per_op_histograms.try_emplace(name);
    result.per_op_counts[name] = 0;
    result.per_op_errors[name] = 0;
  }
  auto end = run_end;
  for (auto& state : states) {
    for (auto& [name, hist] : state.histograms) {
      result.per_op_histograms.at(name).Merge(hist);
      result.per_op_counts[name] += state.counts[name];
      result.per_op_errors[name] += state.errors[name];
    }
    result.send_lag.Merge(state.send_lag);
    result.driver_cpu += state.cpu;
    end = std::max(end, state.last_done);
  }
  result.open_loop = open_loop;
  result.measured_duration = end - warmup_end;
  return result;
}

// Default Linux timer slack (50us) would dominate a sub-100us schedule.
void TightenTimerSlack() {
#ifdef __linux__
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  prctl(PR_SET_TIMERSLACK, 1UL, 0UL, 0UL, 0UL);
#endif
}

void RecordOutcome(WorkerState& state, const std::string& op_name, bool ok,
                   std::chrono::steady_clock::time_point reference,
                   std::chrono::steady_clock::time_point done) {
  state.last_done = std::max(state.last_done, done);
  if (!ok) {
    ++state.errors[op_name];
    return;
  }
  state.histograms.at(op_name).Record(
      std::chrono::duration_cast<std::chrono::nanoseconds>(done - reference).count());
  ++state.counts[op_name];
}

// Waits for a reply until the next scheduled send, polling rather than
// sleeping inside the spin window so the send is not late.
PipelinedConnection::Await AwaitBeforeSend(PipelinedConnection& conn, const CoScheduler& scheduler,
                                           std::chrono::steady_clock::time_point send_at) {
  using Clock = std::chrono::steady_clock;
  using Await = PipelinedConnection::Await;
  while (const auto wake = scheduler.SleepStepEnd(Clock::now(), send_at)) {
    if (const auto status = conn.AwaitReply(*wake); status != Await::kTimeout) return status;
  }
  while (Clock::now() < send_at) {
    if (const auto status = conn.AwaitReply(Clock::now()); status != Await::kTimeout) {
      return status;
    }
    CoScheduler::CpuRelax();
  }
  return Await::kTimeout;
}

int64_t ElapsedNs(std::chrono::steady_clock::time_point from,
                  std::chrono::steady_clock::time_point to) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(to - from).count();
}

}  // namespace

uint64_t WorkerRate(uint64_t total_ops, size_t workers, size_t index) {
  const auto n = static_cast<uint64_t>(std::max<size_t>(workers, 1));
  return (total_ops / n) + (static_cast<uint64_t>(index) < total_ops % n ? 1 : 0);
}

uint64_t TotalErrors(const RunLoopResult& result) {
  uint64_t total = 0;
  for (const auto& [_, errors] : result.per_op_errors) total += errors;
  return total;
}

double AchievedOps(const RunLoopResult& result) {
  uint64_t ok = 0;
  for (const auto& [_, count] : result.per_op_counts) ok += count;
  const double seconds = std::chrono::duration<double>(result.measured_duration).count();
  return seconds > 0.0 ? static_cast<double>(ok) / seconds : 0.0;
}

RunLoopConfig RunLoopConfigFromWorkload(const WorkloadConfig& workload) {
  RunLoopConfig out;
  out.workers = workload.workers;
  out.duration = workload.duration;
  out.warmup = workload.warmup;
  out.target_rate_ops = workload.target_rate_ops;
  out.key_count = workload.key_count;
  out.key_distribution = workload.key_distribution;
  out.value_size_bytes = workload.value_size_bytes;
  out.mix = workload.mix;
  return out;
}

RunLoopResult RunLoop(const RunLoopConfig& config, const OpFn& op) {
  using Clock = std::chrono::steady_clock;
  const auto start_time = Clock::now() + kSetupSlack;
  const auto warmup_end = start_time + config.warmup;
  const auto run_end = warmup_end + config.duration;
  const bool open_loop = config.target_rate_ops > 0;

  const OpSelector selector = BuildSelector(config.mix);
  auto worker_states = MakeWorkerStates(static_cast<size_t>(config.workers), selector);

  std::vector<std::thread> threads;
  threads.reserve(static_cast<size_t>(config.workers));
  for (int w = 0; w < config.workers; ++w) {
    threads.emplace_back([&, w]() {
      TightenTimerSlack();
      auto& state = worker_states[static_cast<size_t>(w)];
      ThreadCpuWindow cpu{state.cpu};
      const uint64_t rate = WorkerRate(config.target_rate_ops, static_cast<size_t>(config.workers),
                                       static_cast<size_t>(w));
      // Fewer requests/s than workers leaves some workers idle.
      if (open_loop && rate == 0) return;
      const CoScheduler scheduler{rate, start_time, 1, static_cast<size_t>(config.workers),
                                  static_cast<size_t>(w)};
      KeyDistConfig kd = config.key_distribution;
      kd.seed = kd.seed + static_cast<uint64_t>(w);
      auto key_dist = MakeKeyDistribution(kd, config.key_count);
      std::mt19937_64 op_rng{kd.seed ^ 0x9E3779B97F4A7C15ULL};
      std::uniform_real_distribution<double> op_uniform{0.0, 1.0};

      for (uint64_t op_index = 0;; ++op_index) {
        Clock::time_point intended_send_time = start_time;
        if (open_loop) {
          intended_send_time = scheduler.IntendedSendTime(op_index);
          if (intended_send_time >= run_end) break;
          scheduler.SleepUntil(intended_send_time);
        } else if (Clock::now() >= run_end) {
          break;
        }
        cpu.StartAt(Clock::now(), warmup_end);

        const double u = op_uniform(op_rng);
        const std::string op_name{selector.Pick(u)};
        const uint64_t key_index = key_dist->Next();

        const auto t_send = Clock::now();
        const bool ok = op(w, op_name, key_index);
        const auto t_done = Clock::now();

        // Every slot is issued, so a late op carries its own wait.
        const auto reference = open_loop ? intended_send_time : t_send;
        if (reference < warmup_end) continue;
        RecordOutcome(state, op_name, ok, reference, t_done);
        if (open_loop) state.send_lag.Record(ElapsedNs(intended_send_time, t_send));
      }
    });
  }

  for (auto& t : threads) t.join();

  return MergeWorkerStates(selector, worker_states, open_loop, warmup_end, run_end);
}

RunLoopResult RunPipelinedLoop(const RunLoopConfig& config, int pipeline_depth, Arrival arrival,
                               std::span<PipelinedConnection* const> connections) {
  using Clock = std::chrono::steady_clock;
  const auto depth = static_cast<size_t>(std::max(pipeline_depth, 1));
  const auto start_time = Clock::now() + kSetupSlack;
  const auto warmup_end = start_time + config.warmup;
  const auto run_end = warmup_end + config.duration;
  const bool open_loop = config.target_rate_ops > 0;
  const bool burst = open_loop && arrival == Arrival::kBurst;

  const OpSelector selector = BuildSelector(config.mix);
  auto worker_states = MakeWorkerStates(connections.size(), selector);

  std::vector<std::thread> threads;
  threads.reserve(connections.size());
  for (size_t w = 0; w < connections.size(); ++w) {
    threads.emplace_back([&, w]() {
      TightenTimerSlack();
      PipelinedConnection& conn = *connections[w];
      auto& state = worker_states[w];
      ThreadCpuWindow cpu{state.cpu};
      const uint64_t rate = WorkerRate(config.target_rate_ops, connections.size(), w);
      if (open_loop && rate == 0) return;
      const CoScheduler scheduler{rate, start_time, burst ? depth : 1, connections.size(), w};
      KeyDistConfig kd = config.key_distribution;
      kd.seed = kd.seed + static_cast<uint64_t>(w);
      auto key_dist = MakeKeyDistribution(kd, config.key_count);
      std::mt19937_64 op_rng{kd.seed ^ 0x9E3779B97F4A7C15ULL};
      std::uniform_real_distribution<double> op_uniform{0.0, 1.0};

      // A burst's requests share its first request's slot.
      const auto slot_of = [&](uint64_t index) { return burst ? index - (index % depth) : index; };
      const auto intended = [&](uint64_t index) {
        return scheduler.IntendedSendTime(slot_of(index));
      };

      // Measured from the intended send time (open) or the send (closed).
      struct InFlight {
        size_t op;
        Clock::time_point reference;
      };
      std::deque<InFlight> in_flight;
      std::vector<Clock::time_point> slots_sent;
      uint64_t next_index = 0;
      bool sending = true;

      while (true) {
        const auto now = Clock::now();
        cpu.StartAt(now, warmup_end);
        slots_sent.clear();
        bool queued = false;
        while (sending && in_flight.size() < depth) {
          Clock::time_point reference = now;
          if (open_loop) {
            reference = intended(next_index);
            if (reference >= run_end) {
              sending = false;
              break;
            }
            if (reference > now) break;
            if (slot_of(next_index) == next_index) slots_sent.push_back(reference);
          } else if (now >= run_end) {
            sending = false;
            break;
          }
          const size_t op = selector.PickIndex(op_uniform(op_rng));
          if (!conn.Send(selector.names[op], key_dist->Next())) return;
          in_flight.push_back({.op = op, .reference = reference});
          ++next_index;
          queued = true;
        }
        if (queued) {
          if (!conn.Flush()) return;
          const auto t_sent = Clock::now();
          for (const auto slot : slots_sent) {
            if (slot >= warmup_end) state.send_lag.Record(ElapsedNs(slot, t_sent));
          }
        }

        if (in_flight.empty()) {
          if (!sending) return;
          scheduler.SleepUntil(intended(next_index));
          continue;
        }

        // With room in the window, wake for the next scheduled send.
        const bool room = sending && open_loop && in_flight.size() < depth;
        const auto status = room ? AwaitBeforeSend(conn, scheduler, intended(next_index))
                                 : conn.AwaitReply(Clock::time_point::max());
        if (status == PipelinedConnection::Await::kFailed) return;
        if (status == PipelinedConnection::Await::kTimeout) continue;

        const auto t_done = Clock::now();
        const InFlight done = in_flight.front();
        in_flight.pop_front();
        if (done.reference < warmup_end) continue;
        RecordOutcome(state, selector.names[done.op], status == PipelinedConnection::Await::kReply,
                      done.reference, t_done);
      }
    });
  }

  for (auto& t : threads) t.join();

  return MergeWorkerStates(selector, worker_states, open_loop, warmup_end, run_end);
}

}  // namespace abyss::perf
