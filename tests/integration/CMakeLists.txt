find_package(RocksDB CONFIG REQUIRED)

add_executable(abyss_integration_tests
  tiering_integration_test.cpp
)

target_include_directories(abyss_integration_tests PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_integration_tests PRIVATE
  abyss::engine
  abyss::consumer
  abyss::hot
  abyss::cold
  abyss::test_support
  GTest::gtest_main
)

target_include_directories(abyss_integration_tests SYSTEM PRIVATE
  $<TARGET_PROPERTY:RocksDB::rocksdb,INTERFACE_INCLUDE_DIRECTORIES>
)

include(GoogleTest)
if(CMAKE_CXX_FLAGS MATCHES "fsanitize")
  gtest_discover_tests(abyss_integration_tests
    DISCOVERY_MODE PRE_TEST
    DISCOVERY_TIMEOUT 120
    PROPERTIES LABELS "integration"
  )
else()
  gtest_discover_tests(abyss_integration_tests
    PROPERTIES LABELS "integration"
  )
endif()
