add_executable(abyss_bench
  wal_entry_bench.cpp
)

target_link_libraries(abyss_bench PRIVATE
  abyss::core
  abyss::queue
  benchmark::benchmark
)
