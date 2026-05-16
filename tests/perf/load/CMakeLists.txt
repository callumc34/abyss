add_executable(abyss_loadgen
  loadgen.cpp
)

target_link_libraries(abyss_loadgen PRIVATE
  abyss::compiler_options
  abyss::core
  abyss::perf_framework
  CLI11::CLI11
  hiredis::hiredis
)

if(ABYSS_BUILD_TESTS)
  add_subdirectory(tests)
endif()
