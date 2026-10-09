add_library(abyss_hot
  single_shard_store.cpp
  sharded_hot_store.cpp
  eviction_worker.cpp
)
add_library(abyss::hot ALIAS abyss_hot)

target_link_libraries(abyss_hot
  PUBLIC abyss::core abyss::metrics
  PRIVATE abyss::log abyss::queue
)
