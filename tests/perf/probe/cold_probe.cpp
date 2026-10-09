#include <CLI/CLI.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/result.h"
#include "common.h"
#include "temp_dir.h"
#include "workload.h"

namespace {

constexpr std::string_view kOpGet = "cold_get";
constexpr std::string_view kOpSet = "cold_set";

std::string KeyFor(uint64_t idx) {
  std::array<char, 32> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "k%020llu", static_cast<unsigned long long>(idx));
  return buf.data();
}

abyss::perf::OperationMix DefaultMix() {
  abyss::perf::OperationMix mix;
  mix.weights[std::string{kOpGet}] = 1.0;
  return mix;
}

abyss::perf::WorkloadTargets DefaultTargets() {
  abyss::perf::WorkloadTargets t;
  abyss::perf::TargetSpec spec;
  spec.p99_us = 5000;
  t.per_op[std::string{kOpGet}] = spec;
  return t;
}

}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
  CLI::App app{"In-process cold store probe (ADP-013)"};
  abyss::perf::probe::ProbeArgs args;
  abyss::perf::probe::RegisterCliOptions(app, args);

  std::string data_path;
  size_t write_buffer_bytes = 64ULL * 1024ULL * 1024ULL;
  app.add_option("--data-path", data_path, "RocksDB data directory (default: temp)");
  app.add_option("--write-buffer-bytes", write_buffer_bytes, "RocksDB write buffer size");
  CLI11_PARSE(app, argc, argv);

  const auto mix_or = args.mix_spec.empty()
                          ? abyss::core::Result<abyss::perf::OperationMix>{DefaultMix()}
                          : abyss::perf::probe::ParseMixSpec(args.mix_spec);
  if (!mix_or.has_value()) {
    std::cerr << "invalid mix: " << mix_or.error().message() << '\n';
    return 2;
  }
  const auto& mix = *mix_or;

  std::unique_ptr<abyss::testing::TempDir> tmp_owner;
  if (data_path.empty()) {
    tmp_owner = std::make_unique<abyss::testing::TempDir>("cold_probe");
    data_path = tmp_owner->String();
  }

  auto store_or = abyss::cold::backends::RocksdbStore::Create({
      .data_path = data_path,
      .write_buffer_size_bytes = write_buffer_bytes,
  });
  if (!store_or.has_value()) {
    std::cerr << "cold store open failed: " << store_or.error().message() << '\n';
    return 1;
  }
  auto& cold = **store_or;
  if (auto rc = cold.Start(); !rc.has_value()) {
    std::cerr << "cold start failed: " << rc.error().message() << '\n';
    return 1;
  }

  const std::string value_str(args.value_size_bytes, 'x');

  // Preload: ApplyBatch of one StringSet per key. Batches of ~10k for speed.
  constexpr size_t kBatchSize = 10'000;
  std::vector<std::string> key_storage;
  key_storage.reserve(kBatchSize);
  std::vector<abyss::core::ops::WriteOp> batch;
  batch.reserve(kBatchSize);
  for (uint64_t i = 0; i < args.key_count; ++i) {
    key_storage.push_back(KeyFor(i));
    abyss::core::ops::StringSet set_op{
        .key = key_storage.back(),
        .value = value_str,
        .abs_ttl_ms = 0,
    };
    batch.emplace_back(set_op);
    if (batch.size() >= kBatchSize) {
      auto rc = cold.ApplyBatch(std::span<const abyss::core::ops::WriteOp>{batch}, 0);
      if (!rc.has_value()) {
        std::cerr << "preload ApplyBatch failed: " << rc.error().message() << '\n';
        return 1;
      }
      batch.clear();
      key_storage.clear();
    }
  }
  if (!batch.empty()) {
    auto rc = cold.ApplyBatch(std::span<const abyss::core::ops::WriteOp>{batch}, 0);
    if (!rc.has_value()) {
      std::cerr << "preload final ApplyBatch failed: " << rc.error().message() << '\n';
      return 1;
    }
    batch.clear();
    key_storage.clear();
  }

  auto cfg = abyss::perf::probe::MakeRunLoopConfig(args, mix);

  abyss::perf::OpFn op_fn = [&](int /*worker_id*/, std::string_view op_name, uint64_t key_index) {
    const auto key = KeyFor(key_index);
    if (op_name == kOpGet) {
      // What a read that misses hot and the buffer costs: a typed load.
      return cold
          .LoadKeyAs(key, abyss::core::KeyType::kString,
                     abyss::core::SteadyClock::now() + std::chrono::seconds(5))
          .has_value();
    }
    abyss::core::ops::StringSet set_op{
        .key = key,
        .value = value_str,
        .abs_ttl_ms = 0,
    };
    std::array<abyss::core::ops::WriteOp, 1> ops{set_op};
    return cold.ApplyBatch(std::span<const abyss::core::ops::WriteOp>{ops}, 0).has_value();
  };

  const auto result = abyss::perf::RunLoop(cfg, op_fn);

  abyss::perf::WorkloadTargets targets = DefaultTargets();
  if (mix.weights.contains(std::string{kOpSet})) {
    abyss::perf::TargetSpec set_spec;
    set_spec.p99_us = 50'000;
    targets.per_op[std::string{kOpSet}] = set_spec;
  }
  const auto workload = abyss::perf::probe::MakeWorkloadConfig(args, mix, targets);
  const auto report = abyss::perf::probe::BuildReport(args, workload, result);

  if (!abyss::perf::probe::WriteOutputs(report, args, result)) {
    return 1;
  }

  if (auto rc = cold.Stop(); !rc.has_value()) {
    std::cerr << "cold_probe: stop failed: " << rc.error().message() << '\n';
  }
  if (const auto errors = abyss::perf::TotalErrors(result); errors > 0) {
    std::cerr << "cold_probe: " << errors << " op errors during run\n";
    return abyss::perf::probe::kExitOpErrors;
  }
  if (args.gate && !report.pass) {
    std::cerr << "cold_probe: one or more targets failed\n";
    return 3;
  }
  return 0;
}
