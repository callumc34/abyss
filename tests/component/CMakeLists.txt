add_executable(abyss_component_tests
  tiering_engine_test.cpp
  wal_queue_test.cpp
  wal_queue_crash_test.cpp
  wal_queue_streams_test.cpp
  hot_consumer_test.cpp
  cold_durability_test.cpp
  cold_persistence_gate_test.cpp
  resolver_recovery_test.cpp
  recovery_coordinator_test.cpp
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
