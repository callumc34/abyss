add_library(abyss_consumer
  hot_consumer.cpp
  cold_consumer.cpp
  compaction_buffer.cpp
  compacted_state.cpp
  flush_strategy.cpp
)
add_library(abyss::consumer ALIAS abyss_consumer)

target_include_directories(abyss_consumer PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(abyss_consumer PUBLIC
  abyss::core
)
