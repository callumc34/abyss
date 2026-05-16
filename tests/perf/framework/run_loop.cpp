#include "run_loop.h"

#include <chrono>
#include <random>
#include <thread>
#include <vector>

#include "scheduler.h"

namespace abyss::perf {

namespace {

struct OpSelector {
  std::vector<std::string> names;
  std::vector<double> cumulative;

  std::string_view Pick(double u) const {
    for (size_t i = 0; i < cumulative.size(); ++i) {
      if (u <= cumulative[i]) {
        return names[i];
      }
    }
    return names.back();
  }
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
};

}  // namespace

RunLoopConfig RunLoopConfigFromWorkload(const WorkloadConfig& workload) {
  RunLoopConfig out;
  out.workers = workload.workers;
  out.duration = workload.duration;
  out.warmup = workload.warmup;
  out.target_rate_ops_per_worker =
      workload.workers > 0 ? workload.target_rate_ops / static_cast<uint64_t>(workload.workers) : 0;
  out.key_count = workload.key_count;
  out.key_distribution = workload.key_distribution;
  out.value_size_bytes = workload.value_size_bytes;
  out.mix = workload.mix;
  return out;
}

RunLoopResult RunLoop(const RunLoopConfig& config, const OpFn& op) {
  using Clock = std::chrono::steady_clock;
  const auto setup_slack = std::chrono::milliseconds{100};
  const auto start_time = Clock::now() + setup_slack;
  const auto warmup_end = start_time + config.warmup;
  const auto run_end = warmup_end + config.duration;

  const OpSelector selector = BuildSelector(config.mix);
  std::vector<WorkerState> worker_states(static_cast<size_t>(config.workers));
  for (auto& state : worker_states) {
    for (const auto& name : selector.names) {
      state.histograms.try_emplace(name);
      state.counts[name] = 0;
    }
  }

  std::vector<std::thread> threads;
  threads.reserve(static_cast<size_t>(config.workers));
  for (int w = 0; w < config.workers; ++w) {
    threads.emplace_back([&, w]() {
      const CoScheduler scheduler{config.target_rate_ops_per_worker, start_time};
      KeyDistConfig kd = config.key_distribution;
      kd.seed = kd.seed + static_cast<uint64_t>(w);
      auto key_dist = MakeKeyDistribution(kd, config.key_count);
      std::mt19937_64 op_rng{kd.seed ^ 0x9E3779B97F4A7C15ULL};
      std::uniform_real_distribution<double> op_uniform{0.0, 1.0};

      uint64_t op_index = 0;
      while (true) {
        Clock::time_point intended_send_time = start_time;
        if (scheduler.IsOpenLoop()) {
          intended_send_time = scheduler.IntendedSendTime(op_index);
          if (intended_send_time >= run_end) break;
          CoScheduler::SleepUntil(intended_send_time);
        } else if (Clock::now() >= run_end) {
          break;
        }

        const double u = op_uniform(op_rng);
        const std::string op_name{selector.Pick(u)};
        const uint64_t key_index = key_dist->Next();

        const auto t_send = Clock::now();
        op(w, op_name, key_index);
        const auto t_done = Clock::now();

        if (t_done >= warmup_end) {
          const auto reference = scheduler.IsOpenLoop() ? intended_send_time : t_send;
          const auto latency_ns =
              std::chrono::duration_cast<std::chrono::nanoseconds>(t_done - reference).count();
          auto& state = worker_states[static_cast<size_t>(w)];
          if (scheduler.IsOpenLoop()) {
            state.histograms.at(op_name).RecordCorrected(latency_ns,
                                                         scheduler.ExpectedIntervalNs());
          } else {
            state.histograms.at(op_name).Record(latency_ns);
          }
          state.counts[op_name]++;
        }

        if (Clock::now() >= run_end) break;
        ++op_index;
      }
    });
  }

  for (auto& t : threads) t.join();

  RunLoopResult result;
  for (const auto& name : selector.names) {
    result.per_op_histograms.try_emplace(name);
    result.per_op_counts[name] = 0;
  }
  for (auto& state : worker_states) {
    for (auto& [name, hist] : state.histograms) {
      result.per_op_histograms.at(name).Merge(hist);
      result.per_op_counts[name] += state.counts[name];
    }
  }
  result.measured_duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(run_end - warmup_end);
  return result;
}

}  // namespace abyss::perf
