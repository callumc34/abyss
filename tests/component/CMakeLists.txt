add_executable(abyss_component_tests
  tiering_engine_test.cpp
  wal_queue_test.cpp
  wal_queue_crash_test.cpp
  wal_queue_streams_test.cpp
  wal_queue_reserve_test.cpp
  cold_durability_test.cpp
  cold_log_clock_test.cpp
  cold_persistence_gate_test.cpp
  recovery_coordinator_test.cpp
  hot_replayer_test.cpp
  regression_test.cpp
  property_regression_test.cpp
  loader_test.cpp
  read_path_test.cpp
  sequencer_test.cpp
)

target_link_libraries(abyss_component_tests PRIVATE
  abyss::engine
  abyss::consumer
  abyss::hot
  abyss::cold
  abyss::queue
  abyss::test_support
  GTest::gtest_main
)

abyss_discover_tests(abyss_component_tests "component")

# Property tests: a differential stream against a reference model,
# linearizability per key group, and crash and power-loss recovery.
add_executable(abyss_property_tests
  differential_property_test.cpp
  linearizability_test.cpp
  crash_property_test.cpp
)

target_link_libraries(abyss_property_tests PRIVATE
  abyss::engine
  abyss::consumer
  abyss::hot
  abyss::cold
  abyss::queue
  abyss::resp
  abyss::test_support
  GTest::gtest_main
)

# ASan and TSan slow every step 5 to 15 times, and Windows pays a real
# FlushFileBuffers per power_loss ack, so there the bounded tiers shrink
# and take the slow timeout; UBSan only takes the slow timeout.
if(ABYSS_SANITIZER MATCHES "^(address|thread)" OR WIN32)
  target_compile_definitions(abyss_property_tests PRIVATE ABYSS_PROPERTY_REDUCED=1)
endif()

if(ABYSS_SANITIZER_ACTIVE OR WIN32)
  abyss_discover_slow_tests(abyss_property_tests "component")
else()
  abyss_discover_tests(abyss_property_tests "component")
endif()

# A million keys in one shard; its own binary so it can carry its own label.
add_executable(abyss_hot_hold_cap_tests
  hot_hold_cap_test.cpp
)

target_link_libraries(abyss_hot_hold_cap_tests PRIVATE
  abyss::hot
  abyss::test_support
  GTest::gtest_main
)

abyss_discover_tests(abyss_hot_hold_cap_tests "component")

add_executable(abyss_component_net_tests
  net/tcp_server_test.cpp
  net/sync_redis_client.cpp
)

target_include_directories(abyss_component_net_tests PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/net
)

target_link_libraries(abyss_component_net_tests PRIVATE
  abyss::net
  GTest::gtest_main
)

abyss_discover_tests(abyss_component_net_tests "component\;component-net")
