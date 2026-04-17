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
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_queue
  PUBLIC abyss::core
  PRIVATE Crc32c::crc32c abyss::resp
)
