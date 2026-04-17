find_package(Crc32c CONFIG REQUIRED)

add_executable(abyss_unit_tests
  core/result_test.cpp
  core/resp_types_test.cpp
  core/consumer_rpc_test.cpp
  core/ops_test.cpp
  queue/wal_entry_test.cpp
  queue/segment_header_test.cpp
  resp/command_registry_test.cpp
  resp/parser_test.cpp
  resp/serializer_test.cpp
  resp/request_pipeline_test.cpp
  cold/format/key_codec_test.cpp
  hot/shard_router_test.cpp
  hot/single_shard_store_test.cpp
  hot/sharded_hot_store_test.cpp
)

target_include_directories(abyss_unit_tests PRIVATE
  ${PROJECT_SOURCE_DIR}/src/queue
  $<TARGET_PROPERTY:Crc32c::crc32c,INTERFACE_INCLUDE_DIRECTORIES>
)

target_link_libraries(abyss_unit_tests PRIVATE
  abyss::queue
  abyss::resp
  abyss::cold
  abyss::hot
  abyss::test_support
  GTest::gtest_main
)

if(ABYSS_WITH_ROCKSDB)
  find_package(RocksDB CONFIG REQUIRED)
  target_sources(abyss_unit_tests PRIVATE
    cold/backends/rocksdb_store_lifecycle_test.cpp
    cold/backends/rocksdb_store_strings_test.cpp
  )
  target_include_directories(abyss_unit_tests SYSTEM PRIVATE
    $<TARGET_PROPERTY:RocksDB::rocksdb,INTERFACE_INCLUDE_DIRECTORIES>
  )
endif()

include(GoogleTest)
if(CMAKE_CXX_FLAGS MATCHES "fsanitize")
  gtest_discover_tests(abyss_unit_tests DISCOVERY_MODE PRE_TEST DISCOVERY_TIMEOUT 120)
else()
  gtest_discover_tests(abyss_unit_tests)
endif()
