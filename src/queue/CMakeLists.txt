add_library(abyss_queue
  wal_queue.cpp
  segment.cpp
  group_commit.cpp
)
add_library(abyss::queue ALIAS abyss_queue)

target_include_directories(abyss_queue
  PUBLIC ${PROJECT_SOURCE_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_queue PUBLIC
  abyss::core
)
