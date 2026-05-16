add_executable(abyss_perf_framework_tests
  histogram_test.cpp
  scheduler_co_test.cpp
  key_dist_test.cpp
  workload_test.cpp
  reporter_test.cpp
  metrics_scraper_test.cpp
)

target_link_libraries(abyss_perf_framework_tests PRIVATE
  abyss::compiler_options
  abyss::perf_framework
  GTest::gtest_main
)

abyss_discover_tests(abyss_perf_framework_tests "perf-framework")
