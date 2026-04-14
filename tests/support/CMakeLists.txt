add_library(abyss_test_support INTERFACE)
add_library(abyss::test_support ALIAS abyss_test_support)

target_include_directories(abyss_test_support INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_test_support INTERFACE
  abyss::core
  GTest::gmock
)
