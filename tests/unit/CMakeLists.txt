add_executable(abyss_unit_tests
  core/result_test.cpp
  core/resp_types_test.cpp
)

target_link_libraries(abyss_unit_tests PRIVATE
  abyss::core
  abyss::test_support
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(abyss_unit_tests)
