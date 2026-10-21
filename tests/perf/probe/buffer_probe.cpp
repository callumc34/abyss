#include <CLI/CLI.hpp>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <string_view>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/ops.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "common.h"
#include "workload.h"

namespace {

constexpr std::string_view kOpRead = "buffer_read";

std::string KeyFor(uint64_t idx) {
  std::array<char, 32> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "k%020llu", static_cast<unsigned long long>(idx));
  return buf.data();
}

abyss::perf::OperationMix DefaultMix() {
  abyss::perf::OperationMix mix;
  mix.weights[std::string{kOpRead}] = 1.0;
  return mix;
}

abyss::perf::WorkloadTargets DefaultTargets() {
  abyss::perf::WorkloadTargets t;
  abyss::perf::TargetSpec spec;
  spec.p99_us = 50;
  t.per_op[std::string{kOpRead}] = spec;
  return t;
}

}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
  CLI::App app{"In-process compaction buffer probe (ADP-013)"};
  abyss::perf::probe::ProbeArgs args;
  abyss::perf::probe::RegisterCliOptions(app, args);
  CLI11_PARSE(app, argc, argv);

  const auto mix_or = args.mix_spec.empty()
                          ? abyss::core::Result<abyss::perf::OperationMix>{DefaultMix()}
                          : abyss::perf::probe::ParseMixSpec(args.mix_spec);
  if (!mix_or.has_value()) {
    std::cerr << "invalid mix: " << mix_or.error().message() << '\n';
    return 2;
  }
  const auto& mix = *mix_or;

  abyss::consumer::CompactionBuffer buffer;

  const std::string value_str(args.value_size_bytes, 'x');

  // Preload: absorb a SET for every key so reads can land on real entries.
  for (uint64_t i = 0; i < args.key_count; ++i) {
    const auto key = KeyFor(i);
    abyss::core::ops::StringSet set_op{
        .key = key,
        .value = value_str,
        .abs_ttl_ms = 0,
    };
    abyss::core::ops::WriteOp op = set_op;
    const auto seq = abyss::core::kFirstSeq + static_cast<abyss::core::SequenceId>(i);
    buffer.Absorb(key, op, abyss::core::EvictionTTL{86400}, seq, 0);
  }

  auto cfg = abyss::perf::probe::MakeRunLoopConfig(args, mix);

  // Every key was preloaded, so a miss is a failed read.
  abyss::perf::OpFn op_fn = [&](int /*worker_id*/, std::string_view /*op_name*/,
                                uint64_t key_index) {
    return buffer.Snapshot(KeyFor(key_index)).has_value();
  };

  const auto result = abyss::perf::RunLoop(cfg, op_fn);
  const auto workload = abyss::perf::probe::MakeWorkloadConfig(args, mix, DefaultTargets());
  const auto report = abyss::perf::probe::BuildReport(args, workload, result);

  if (!abyss::perf::probe::WriteOutputs(report, args, result)) {
    return 1;
  }

  if (const auto errors = abyss::perf::TotalErrors(result); errors > 0) {
    std::cerr << "buffer_probe: " << errors << " read misses during run\n";
    return abyss::perf::probe::kExitOpErrors;
  }
  if (args.gate && !report.pass) {
    std::cerr << "buffer_probe: one or more targets failed\n";
    return 3;
  }
  return 0;
}
