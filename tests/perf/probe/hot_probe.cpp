#include <CLI/CLI.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "common.h"
#include "workload.h"

namespace {

constexpr std::string_view kOpGet = "hot_get";
constexpr std::string_view kOpApply = "hot_apply";

std::string KeyFor(uint64_t idx) {
  std::array<char, 32> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "k%020llu", static_cast<unsigned long long>(idx));
  return buf.data();
}

abyss::perf::OperationMix DefaultMix() {
  abyss::perf::OperationMix mix;
  mix.weights[std::string{kOpGet}] = 0.5;
  mix.weights[std::string{kOpApply}] = 0.5;
  return mix;
}

abyss::perf::WorkloadTargets DefaultTargets() {
  abyss::perf::WorkloadTargets t;
  abyss::perf::TargetSpec get_spec;
  get_spec.p99_us = 100;
  abyss::perf::TargetSpec apply_spec;
  apply_spec.p99_us = 5;
  t.per_op[std::string{kOpGet}] = get_spec;
  t.per_op[std::string{kOpApply}] = apply_spec;
  return t;
}

}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
  CLI::App app{"In-process hot store probe (ADP-013)"};
  abyss::perf::probe::ProbeArgs args;
  abyss::perf::probe::RegisterCliOptions(app, args);

  size_t max_memory_bytes = 256ULL * 1024ULL * 1024ULL;
  uint32_t shard_count = 4;
  app.add_option("--max-memory-bytes", max_memory_bytes, "Hot store memory budget");
  app.add_option("--shards", shard_count, "Hot store shard count");

  CLI11_PARSE(app, argc, argv);

  const auto mix_or = args.mix_spec.empty()
                          ? abyss::core::Result<abyss::perf::OperationMix>{DefaultMix()}
                          : abyss::perf::probe::ParseMixSpec(args.mix_spec);
  if (!mix_or.has_value()) {
    std::cerr << "invalid mix: " << mix_or.error().message() << '\n';
    return 2;
  }
  const auto& mix = *mix_or;
  for (const auto& [name, weight] : mix.weights) {
    if (name != kOpGet && name != kOpApply) {
      std::cerr << "unknown op in mix: " << name << " (expected " << kOpGet << " or " << kOpApply
                << ")\n";
      return 2;
    }
  }

  abyss::core::EvictionPolicy eviction{std::chrono::seconds{86400}};
  abyss::hot::ShardedHotStore hot{abyss::hot::ShardedHotStoreConfig{
      .max_memory_bytes = max_memory_bytes,
      .shard_count = shard_count,
      .eviction_policy = &eviction,
  }};

  std::vector<std::string> values_pool(static_cast<size_t>(args.workers));
  for (auto& v : values_pool) v.assign(args.value_size_bytes, 'x');

  // Preload: SET every key once so GETs hit.
  for (uint64_t i = 0; i < args.key_count; ++i) {
    const auto key = KeyFor(i);
    abyss::core::ops::StringSet set_op{
        .key = key,
        .value = values_pool[0],
        .abs_ttl_ms = 0,
    };
    abyss::core::ops::WriteOp op = set_op;
    auto rc = hot.Apply(op, /*seq=*/abyss::core::kFirstSeq);
    if (!rc.has_value()) {
      std::cerr << "preload failed at key " << i << ": " << rc.error().message() << '\n';
      return 1;
    }
  }

  auto cfg = abyss::perf::probe::MakeRunLoopConfig(args, mix);

  abyss::perf::OpFn op_fn = [&](int worker_id, std::string_view op_name, uint64_t key_index) {
    const auto key = KeyFor(key_index);
    if (op_name == kOpGet) {
      abyss::core::ops::StringGet read{.key = key};
      return hot.Exec(read).has_value();
    }
    {
      const auto& val = values_pool[static_cast<size_t>(worker_id)];
      abyss::core::ops::StringSet set_op{
          .key = key,
          .value = val,
          .abs_ttl_ms = 0,
      };
      abyss::core::ops::WriteOp write = set_op;
      return hot.Apply(write, /*seq=*/abyss::core::kFirstSeq).has_value();
    }
  };

  const auto result = abyss::perf::RunLoop(cfg, op_fn);
  const auto workload = abyss::perf::probe::MakeWorkloadConfig(args, mix, DefaultTargets());
  const auto report = abyss::perf::probe::BuildReport(args, workload, result);

  if (!abyss::perf::probe::WriteOutputs(report, args, result)) {
    return 1;
  }

  if (const auto errors = abyss::perf::TotalErrors(result); errors > 0) {
    std::cerr << "hot_probe: " << errors << " op errors during run\n";
    return abyss::perf::probe::kExitOpErrors;
  }
  if (args.gate && !report.pass) {
    std::cerr << "hot_probe: one or more targets failed\n";
    return 3;
  }
  return 0;
}
