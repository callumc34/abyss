add_executable(abyss_bench
  bench_main.cpp
  frame_bench.cpp
  wal_queue_bench.cpp
  recovery_bench.cpp
  eviction_policy_bench.cpp
  hot_store_bench.cpp
  compaction_buffer_bench.cpp
  write_validation_bench.cpp
  loader_bench.cpp
  read_path_bench.cpp
)

target_link_libraries(abyss_bench PRIVATE
  abyss::core
  abyss::queue
  abyss::cold
  abyss::consumer
  abyss::engine
  abyss::hot
  abyss::resp
  benchmark::benchmark
)
add_dependencies(abyss_bench abyss_perf_source_tree)
target_include_directories(abyss_bench PRIVATE
  ${ABYSS_PERF_GENERATED_DIR}
  ${PROJECT_SOURCE_DIR}/tests/support
)
