find_package(Crc32c CONFIG REQUIRED)

add_executable(abyss_unit_tests
  core/result_test.cpp
  core/resp_types_test.cpp
  queue/wal_entry_test.cpp
  queue/segment_header_test.cpp
  cold/format/key_codec_test.cpp
)

target_include_directories(abyss_unit_tests PRIVATE
  ${PROJECT_SOURCE_DIR}/src/queue
)

target_link_libraries(abyss_unit_tests PRIVATE
  abyss::core
  abyss::queue
  abyss::cold
  abyss::test_support
  Crc32c::crc32c
  GTest::gtest_main
)

if(ABYSS_WITH_ROCKSDB)
  find_package(RocksDB CONFIG REQUIRED)
  target_sources(abyss_unit_tests PRIVATE
    cold/backends/rocksdb_store_lifecycle_test.cpp
    cold/backends/rocksdb_store_strings_test.cpp
  )
  target_link_libraries(abyss_unit_tests PRIVATE RocksDB::rocksdb)
endif()

include(GoogleTest)
gtest_discover_tests(abyss_unit_tests)
