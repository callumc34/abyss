find_package(benchmark CONFIG REQUIRED)
find_package(Crc32c CONFIG REQUIRED)

set_package_properties(benchmark PROPERTIES
  TYPE OPTIONAL
  DESCRIPTION "Google micro-benchmark framework"
  URL "https://github.com/google/benchmark"
  PURPOSE "Encode/decode throughput measurements"
)

add_executable(abyss_bench
  wal_entry_bench.cpp
)

target_link_libraries(abyss_bench PRIVATE
  abyss::core
  abyss::queue
  benchmark::benchmark
)
