add_library(abyss_queue
  wal_queue.cpp
  wal_scan.cpp
  shard_stream.cpp
  group_commit.cpp
  crc32c.cpp
  entry_payload.cpp
  frame.cpp
  log.cpp
  segment_header_v2.cpp
  durability_window.cpp
  memory_offset_store.cpp
  offset_checkpoint.cpp
  segment_reaper.cpp
  pending_append.cpp
  reservation.cpp
)
add_library(abyss::queue ALIAS abyss_queue)

target_include_directories(abyss_queue
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_queue
  PUBLIC abyss::core abyss::metrics
  PRIVATE Crc32c::crc32c abyss::resp abyss::log abyss::platform
)
