add_library(abyss_consumer
  cold_consumer.cpp
  cold_consumer_pool.cpp
  compaction_buffer.cpp
  compacted_state.cpp
  flush_strategy.cpp
)
add_library(abyss::consumer ALIAS abyss_consumer)

target_link_libraries(abyss_consumer
  PUBLIC abyss::core abyss::metrics
  PRIVATE abyss::log
)
