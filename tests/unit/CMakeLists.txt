add_executable(abyss_core_tests
  core/result_test.cpp
  core/resp_types_test.cpp
  core/consumer_rpc_test.cpp
  core/apply_notifier_test.cpp
  core/fire_and_forget_test.cpp
  core/ops_test.cpp
  core/varint_test.cpp
  core/eviction_policy_test.cpp
  core/queue_entry_test.cpp
  core/shard_router_test.cpp
  core/slot_test.cpp
  core/atomic_file_test.cpp
)

target_link_libraries(abyss_core_tests PRIVATE
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_queue_tests
  queue/wal_entry_test.cpp
  queue/segment_header_test.cpp
  queue/segment_test.cpp
  queue/group_commit_test.cpp
  queue/offset_store_test.cpp
  queue/segment_reaper_test.cpp
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
  resp/node_identity_test.cpp
)

target_link_libraries(abyss_resp_tests PRIVATE
  abyss::resp
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_hot_tests
  hot/single_shard_store_test.cpp
  hot/sharded_hot_store_test.cpp
  hot/eviction_worker_test.cpp
)

target_link_libraries(abyss_hot_tests PRIVATE
  abyss::hot
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_cold_tests
  cold/format/key_codec_test.cpp
  cold/ttl_scanner_test.cpp
  cold/ttl_scanner_thread_test.cpp
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
    cold/backends/rocksdb_store_collections_test.cpp
    cold/backends/rocksdb_store_ttl_test.cpp
    cold/backends/rocksdb_store_sweep_test.cpp
    cold/backends/rocksdb_store_wipe_test.cpp
  )
  target_include_directories(abyss_cold_tests SYSTEM PRIVATE
    $<TARGET_PROPERTY:RocksDB::rocksdb,INTERFACE_INCLUDE_DIRECTORIES>
  )
endif()

add_executable(abyss_consumer_tests
  consumer/compacted_state_test.cpp
  consumer/flush_strategy_test.cpp
  consumer/compaction_buffer_test.cpp
  consumer/cold_consumer_test.cpp
  consumer/cold_consumer_pool_test.cpp
  consumer/existence_cache_test.cpp
  consumer/resolver_test.cpp
)

target_link_libraries(abyss_consumer_tests PRIVATE
  abyss::consumer
  abyss::queue
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_config_tests
  config/defaults_test.cpp
  config/parse_test.cpp
  config/validate_test.cpp
  config/unknown_key_test.cpp
  config/env_override_test.cpp
  config/example_file_test.cpp
  config/log_config_test.cpp
)

target_compile_definitions(abyss_config_tests PRIVATE
  ABYSS_EXAMPLE_CONFIG_PATH="${PROJECT_SOURCE_DIR}/config/abyss.example.yaml"
)

target_link_libraries(abyss_config_tests PRIVATE
  abyss::config
  GTest::gtest_main
)

add_executable(abyss_support_tests
  support/temp_dir_test.cpp
)

target_link_libraries(abyss_support_tests PRIVATE
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_log_tests
  log/level_test.cpp
  log/logger_test.cpp
  log/json_sink_test.cpp
  log/text_sink_test.cpp
  log/pre_init_test.cpp
  log/capturing_sink_test.cpp
  log/key_hash_test.cpp
  log/threaded_test.cpp
  log/reset_test.cpp
)

target_link_libraries(abyss_log_tests PRIVATE
  abyss::log
  abyss::config
  GTest::gtest_main
)

add_executable(abyss_metrics_tests
  metrics/register_test.cpp
  metrics/idempotent_test.cpp
  metrics/conflict_test.cpp
  metrics/scrape_test.cpp
  metrics/enabled_test.cpp
  metrics/names_test.cpp
  metrics/threaded_test.cpp
  metrics/reset_test.cpp
)

target_link_libraries(abyss_metrics_tests PRIVATE
  abyss::metrics
  GTest::gtest_main
)

add_executable(abyss_net_tests
  net/poller_test.cpp
  net/connection_backpressure_test.cpp
  net/connection_idle_test.cpp
  net/connection_lazy_arm_test.cpp
  net/connection_oversize_test.cpp
)

target_include_directories(abyss_net_tests PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/net
)

target_link_libraries(abyss_net_tests PRIVATE
  abyss::net
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_admin_tests
  admin/http_server_test.cpp
  admin/health_handler_test.cpp
  admin/ready_handler_test.cpp
  admin/metrics_handler_test.cpp
  admin/status_handler_test.cpp
  admin/json_writer_test.cpp
)

target_include_directories(abyss_admin_tests PRIVATE
  ${PROJECT_SOURCE_DIR}/src/admin
)

target_link_libraries(abyss_admin_tests PRIVATE
  abyss::admin
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_engine_tests
  engine/shard_scheduler_test.cpp
)

target_link_libraries(abyss_engine_tests PRIVATE
  abyss::engine
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_branding_tests
  branding/banner_test.cpp
)

target_link_libraries(abyss_branding_tests PRIVATE
  abyss::branding
  abyss::test_support
  GTest::gtest_main
)

add_executable(abyss_platform_tests
  platform/fs_test.cpp
)

target_link_libraries(abyss_platform_tests PRIVATE
  abyss::platform
  abyss::test_support
  GTest::gtest_main
)

abyss_discover_tests(abyss_core_tests     "unit\;unit-core")
abyss_discover_tests(abyss_queue_tests    "unit\;unit-queue")
abyss_discover_tests(abyss_resp_tests     "unit\;unit-resp")
abyss_discover_tests(abyss_hot_tests      "unit\;unit-hot")
abyss_discover_tests(abyss_cold_tests     "unit\;unit-cold")
abyss_discover_tests(abyss_consumer_tests "unit\;unit-consumer")
abyss_discover_tests(abyss_config_tests   "unit\;unit-config")
abyss_discover_tests(abyss_support_tests  "unit\;unit-support")
abyss_discover_tests(abyss_log_tests      "unit\;unit-log")
abyss_discover_tests(abyss_metrics_tests  "unit\;unit-metrics")
abyss_discover_tests(abyss_net_tests      "unit\;unit-net")
abyss_discover_tests(abyss_admin_tests    "unit\;unit-admin")
abyss_discover_tests(abyss_engine_tests   "unit\;unit-engine")
abyss_discover_tests(abyss_branding_tests "unit\;unit-branding")
abyss_discover_tests(abyss_platform_tests "unit\;unit-platform")
