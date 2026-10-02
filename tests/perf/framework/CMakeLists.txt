add_library(abyss_perf_framework
  histogram.cpp
  scheduler.cpp
  key_dist.cpp
  workload.cpp
  reporter.cpp
  metrics_scraper.cpp
  run_loop.cpp
  server_identity.cpp
  sweep.cpp
)
add_library(abyss::perf_framework ALIAS abyss_perf_framework)

target_include_directories(abyss_perf_framework PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_perf_framework PUBLIC
  abyss::compiler_options
  abyss::core
  yaml-cpp::yaml-cpp
)

target_link_libraries(abyss_perf_framework PRIVATE
  httplib::httplib
  $<IF:$<TARGET_EXISTS:hdr_histogram::hdr_histogram>,hdr_histogram::hdr_histogram,hdr_histogram::hdr_histogram_static>
)
