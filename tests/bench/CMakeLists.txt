add_executable(abyss_bench
  wal_entry_bench.cpp
  wal_queue_bench.cpp
  recovery_bench.cpp
)

target_link_libraries(abyss_bench PRIVATE
  abyss::core
  abyss::queue
  abyss::cold
  abyss::consumer
  abyss::engine
  abyss::hot
  benchmark::benchmark
)
