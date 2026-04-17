add_library(abyss_system_test_framework STATIC
  framework/redis_client.cpp
  framework/server_fixture.cpp
)

target_include_directories(abyss_system_test_framework PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/framework
)

target_link_libraries(abyss_system_test_framework PUBLIC
  GTest::gtest
)

target_compile_definitions(abyss_system_test_framework PRIVATE
  ABYSS_SERVER_BINARY="$<TARGET_FILE:abyss-server>"
)

add_executable(abyss_system_tests
  acceptance_test.cpp
  protocol_test.cpp
  tiering_test.cpp
  durability_test.cpp
  operational_test.cpp
)

target_link_libraries(abyss_system_tests PRIVATE
  abyss_system_test_framework
  GTest::gtest_main
)

add_dependencies(abyss_system_tests abyss-server)

include(GoogleTest)
if(CMAKE_CXX_FLAGS MATCHES "fsanitize")
  gtest_discover_tests(abyss_system_tests
    DISCOVERY_MODE PRE_TEST
    DISCOVERY_TIMEOUT 120
    PROPERTIES LABELS "system"
  )
else()
  gtest_discover_tests(abyss_system_tests
    PROPERTIES LABELS "system"
  )
endif()
