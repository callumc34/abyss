add_library(abyss_perf_probe_common STATIC
  common.cpp
)
add_library(abyss::perf_probe_common ALIAS abyss_perf_probe_common)

target_include_directories(abyss_perf_probe_common PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_perf_probe_common PUBLIC
  abyss::compiler_options
  abyss::core
  abyss::perf_framework
  CLI11::CLI11
)

add_executable(abyss_hot_probe hot_probe.cpp)
target_link_libraries(abyss_hot_probe PRIVATE
  abyss::compiler_options
  abyss::core
  abyss::hot
  abyss::perf_framework
  abyss::perf_probe_common
  CLI11::CLI11
)

add_executable(abyss_buffer_probe buffer_probe.cpp)
target_link_libraries(abyss_buffer_probe PRIVATE
  abyss::compiler_options
  abyss::consumer
  abyss::core
  abyss::perf_framework
  abyss::perf_probe_common
  CLI11::CLI11
)

if(ABYSS_WITH_ROCKSDB)
  add_executable(abyss_cold_probe cold_probe.cpp)
  target_link_libraries(abyss_cold_probe PRIVATE
    abyss::compiler_options
    abyss::cold
    abyss::core
    abyss::perf_framework
    abyss::perf_probe_common
    abyss::test_support
    CLI11::CLI11
  )
endif()

if(ABYSS_BUILD_TESTS)
  add_subdirectory(tests)
endif()
