add_executable(abyss_acceptance_tests
  adp001_durable_before_ack_test.cpp
  adp002_hot_eviction_refresh_test.cpp
  adp004_quiet_dominates_over_deadline_test.cpp
  adp004_cold_retention_under_stall_test.cpp
  adp006_write_path_both_complete_test.cpp
)

target_link_libraries(abyss_acceptance_tests PRIVATE
  abyss::engine
  abyss::consumer
  abyss::hot
  abyss::cold
  abyss::queue
  abyss::test_support
  GTest::gtest_main
  GTest::gmock
)

target_include_directories(abyss_acceptance_tests SYSTEM PRIVATE
  $<TARGET_PROPERTY:RocksDB::rocksdb,INTERFACE_INCLUDE_DIRECTORIES>
)

abyss_discover_tests(abyss_acceptance_tests "acceptance")
