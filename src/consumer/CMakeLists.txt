add_library(abyss_consumer
  hot_consumer.cpp
  cold_consumer.cpp
  compaction_buffer.cpp
  compacted_state.cpp
  flush_strategy.cpp
)
add_library(abyss::consumer ALIAS abyss_consumer)

target_link_libraries(abyss_consumer PUBLIC
  abyss::core
)
