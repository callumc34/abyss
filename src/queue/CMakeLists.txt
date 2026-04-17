find_package(Crc32c CONFIG REQUIRED)
set_package_properties(Crc32c PROPERTIES
  TYPE REQUIRED
  DESCRIPTION "Hardware-accelerated CRC32C integrity checksums"
  URL "https://github.com/google/crc32c"
  PURPOSE "WAL segment and entry integrity verification"
)

add_library(abyss_queue
  wal_queue.cpp
  segment.cpp
  group_commit.cpp
  crc32c.cpp
  wal_entry.cpp
  segment_header.cpp
)
add_library(abyss::queue ALIAS abyss_queue)

target_include_directories(abyss_queue
  PUBLIC ${PROJECT_SOURCE_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_queue
  PUBLIC abyss::core
  PRIVATE Crc32c::crc32c abyss::resp
)
