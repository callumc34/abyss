add_executable(abyss_core_tests
  core/result_test.cpp
  core/resp_types_test.cpp
  core/consumer_rpc_test.cpp
  core/ops_test.cpp
)

target_link_libraries(abyss_core_tests PRIVATE
  abyss::core
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_queue_tests
  queue/wal_entry_test.cpp
  queue/segment_header_test.cpp
)

target_include_directories(abyss_queue_tests PRIVATE
  ${PROJECT_SOURCE_DIR}/src/queue
  $<TARGET_PROPERTY:Crc32c::crc32c,INTERFACE_INCLUDE_DIRECTORIES>
)

target_link_libraries(abyss_queue_tests PRIVATE
  abyss::queue
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_resp_tests
  resp/parser_test.cpp
  resp/serializer_test.cpp
  resp/command_registry_test.cpp
  resp/request_pipeline_test.cpp
)

target_link_libraries(abyss_resp_tests PRIVATE
  abyss::resp
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_hot_tests
  hot/shard_router_test.cpp
  hot/single_shard_store_test.cpp
  hot/sharded_hot_store_test.cpp
)

target_link_libraries(abyss_hot_tests PRIVATE
  abyss::hot
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_cold_tests
  cold/format/key_codec_test.cpp
)

target_link_libraries(abyss_cold_tests PRIVATE
  abyss::cold
  abyss::test_support
  GTest::gtest_main
)

if(ABYSS_WITH_ROCKSDB)
  target_sources(abyss_cold_tests PRIVATE
    cold/backends/rocksdb_store_lifecycle_test.cpp
    cold/backends/rocksdb_store_strings_test.cpp
  )
  target_include_directories(abyss_cold_tests SYSTEM PRIVATE
    $<TARGET_PROPERTY:RocksDB::rocksdb,INTERFACE_INCLUDE_DIRECTORIES>
  )
endif()

add_executable(abyss_consumer_tests
  consumer/compacted_state_test.cpp
  consumer/flush_strategy_test.cpp
  consumer/compaction_buffer_test.cpp
)

target_link_libraries(abyss_consumer_tests PRIVATE
  abyss::consumer
  abyss::test_support
  GTest::gtest_main
)

abyss_discover_tests(abyss_core_tests     "unit\;unit-core")
abyss_discover_tests(abyss_queue_tests    "unit\;unit-queue")
abyss_discover_tests(abyss_resp_tests     "unit\;unit-resp")
abyss_discover_tests(abyss_hot_tests      "unit\;unit-hot")
abyss_discover_tests(abyss_cold_tests     "unit\;unit-cold")
abyss_discover_tests(abyss_consumer_tests "unit\;unit-consumer")
