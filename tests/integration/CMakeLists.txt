add_executable(abyss_integration_tests
  tiering_integration_test.cpp
)

target_link_libraries(abyss_integration_tests PRIVATE
  abyss::engine
  abyss::consumer
  abyss::hot
  abyss::cold
  abyss::queue
  abyss::test_support
  GTest::gtest_main
)

target_include_directories(abyss_integration_tests SYSTEM PRIVATE
  $<TARGET_PROPERTY:RocksDB::rocksdb,INTERFACE_INCLUDE_DIRECTORIES>
)

abyss_discover_tests(abyss_integration_tests "integration")
