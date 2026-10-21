#include <benchmark/benchmark.h>

#include "perf_source_tree.h"

// BENCHMARK_MAIN, plus the tree the binary was built from in the JSON's
// context, as the probes report it in build.source_tree.
// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
  benchmark::AddCustomContext("source_tree", kPerfSourceTree);
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
