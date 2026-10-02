if(ABYSS_BUILD_TESTS)
  add_subdirectory(support)
  add_subdirectory(unit)
  add_subdirectory(component)
  if(ABYSS_WITH_ROCKSDB)
    add_subdirectory(integration)
  endif()
  add_subdirectory(system)

  # Concurrent file-I/O tests that exceed the 30s default on Windows runners.
  abyss_mark_slow_tests(component "${CMAKE_CURRENT_SOURCE_DIR}/component"
    WalQueueTest.TwoPhaseWritePathEliminatesFulfillBeforeRegisterRace
    HotConsumerTest.ConcurrentWritersAlwaysSeeRegisteredEntries)
endif()

if(ABYSS_BUILD_BENCHMARKS OR ABYSS_BUILD_PERF)
  add_subdirectory(perf)
endif()

if(ABYSS_BUILD_FUZZ)
  add_subdirectory(fuzz)
endif()
