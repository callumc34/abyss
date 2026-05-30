add_library(abyss_system_test_framework STATIC
  framework/redis_client.cpp
  framework/server_fixture.cpp
)

target_include_directories(abyss_system_test_framework PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/framework
)

target_link_libraries(abyss_system_test_framework PUBLIC
  abyss::test_support
  GTest::gtest
)

target_compile_definitions(abyss_system_test_framework PRIVATE
  ABYSS_SERVER_BINARY="$<TARGET_FILE:abyss-server>"
)

add_executable(abyss_system_tests
  commands_test.cpp
  protocol_test.cpp
  tiering_test.cpp
  tiered_read_test.cpp
  compaction_efficiency_test.cpp
  compaction_remove_resurrection_test.cpp
  durability_test.cpp
  operational_test.cpp
  health_test.cpp
  two_ttl_test.cpp
)

target_link_libraries(abyss_system_tests PRIVATE
  abyss_system_test_framework
  GTest::gtest_main
)

add_dependencies(abyss_system_tests abyss-server)

abyss_add_binary_test(abyss_system_tests "system")

if(ABYSS_WITH_ROCKSDB)
  add_executable(abyss_system_net_tests
    net/tcp_server_smoke_test.cpp
    ${CMAKE_SOURCE_DIR}/tests/component/net/sync_redis_client.cpp
  )

  target_include_directories(abyss_system_net_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/tests/component/net
  )

  target_link_libraries(abyss_system_net_tests PRIVATE
    abyss::net
    abyss::engine
    abyss::queue
    abyss::cold_rocksdb
    abyss::test_support
    GTest::gtest_main
  )

  abyss_discover_tests(abyss_system_net_tests "system\;system-net")
endif()
