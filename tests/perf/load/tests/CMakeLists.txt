add_executable(abyss_perf_load_tests
  loadgen_smoke_test.cpp
)

target_link_libraries(abyss_perf_load_tests PRIVATE
  abyss::compiler_options
  abyss::test_support
  abyss_system_test_framework
  GTest::gtest_main
  yaml-cpp::yaml-cpp
)

target_compile_definitions(abyss_perf_load_tests PRIVATE
  ABYSS_LOADGEN_BINARY="$<TARGET_FILE:abyss_loadgen>"
  ABYSS_LOAD_WORKLOAD_DIR="${CMAKE_SOURCE_DIR}/tests/perf/workloads"
)

add_dependencies(abyss_perf_load_tests abyss_loadgen abyss-server)

abyss_discover_tests(abyss_perf_load_tests "perf-load")
