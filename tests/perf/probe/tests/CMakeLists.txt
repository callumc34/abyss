add_executable(abyss_perf_probe_tests
  probe_smoke_test.cpp
)

target_link_libraries(abyss_perf_probe_tests PRIVATE
  abyss::compiler_options
  abyss::test_support
  GTest::gtest_main
  yaml-cpp::yaml-cpp
)

target_compile_definitions(abyss_perf_probe_tests PRIVATE
  ABYSS_HOT_PROBE_BINARY="$<TARGET_FILE:abyss_hot_probe>"
  ABYSS_BUFFER_PROBE_BINARY="$<TARGET_FILE:abyss_buffer_probe>"
)

add_dependencies(abyss_perf_probe_tests abyss_hot_probe abyss_buffer_probe)

if(ABYSS_WITH_ROCKSDB)
  target_compile_definitions(abyss_perf_probe_tests PRIVATE
    ABYSS_COLD_PROBE_BINARY="$<TARGET_FILE:abyss_cold_probe>"
    ABYSS_HAVE_COLD_PROBE=1
  )
  add_dependencies(abyss_perf_probe_tests abyss_cold_probe)
endif()

abyss_discover_tests(abyss_perf_probe_tests "perf-probe")
