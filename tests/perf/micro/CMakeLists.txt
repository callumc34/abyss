add_executable(abyss_bench
  frame_bench.cpp
  wal_queue_bench.cpp
  recovery_bench.cpp
  eviction_policy_bench.cpp
  hot_store_bench.cpp
  compaction_buffer_bench.cpp
  write_validation_bench.cpp
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
