if(ABYSS_BUILD_TESTS)
  add_subdirectory(support)
  add_subdirectory(unit)
endif()

if(ABYSS_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()
