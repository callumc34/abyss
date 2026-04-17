add_executable(abyss_component_tests
  tiering_engine_test.cpp
)

target_link_libraries(abyss_component_tests PRIVATE
  abyss::engine
  abyss::consumer
  abyss::test_support
  GTest::gtest_main
)

abyss_discover_tests(abyss_component_tests "component")
