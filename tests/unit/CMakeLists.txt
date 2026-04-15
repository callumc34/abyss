find_package(Crc32c CONFIG REQUIRED)

add_executable(abyss_unit_tests
  core/result_test.cpp
  core/resp_types_test.cpp
  queue/wal_entry_test.cpp
  queue/segment_header_test.cpp
)

target_include_directories(abyss_unit_tests PRIVATE
  ${PROJECT_SOURCE_DIR}/src/queue
)

target_link_libraries(abyss_unit_tests PRIVATE
  abyss::core
  abyss::queue
  abyss::test_support
  Crc32c::crc32c
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(abyss_unit_tests)
