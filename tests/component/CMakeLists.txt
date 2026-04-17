add_executable(abyss_component_tests
  tiering_engine_test.cpp
)

target_link_libraries(abyss_component_tests PRIVATE
  abyss::engine
  abyss::consumer
  abyss::test_support
  GTest::gtest_main
)

include(GoogleTest)
if(CMAKE_CXX_FLAGS MATCHES "fsanitize")
  gtest_discover_tests(abyss_component_tests
    DISCOVERY_MODE PRE_TEST
    DISCOVERY_TIMEOUT 120
    PROPERTIES LABELS "component"
  )
else()
  gtest_discover_tests(abyss_component_tests
    PROPERTIES LABELS "component"
  )
endif()
