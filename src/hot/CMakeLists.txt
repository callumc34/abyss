add_library(abyss_hot
  shard_router.cpp
  single_shard_store.cpp
  sharded_hot_store.cpp
)
add_library(abyss::hot ALIAS abyss_hot)

target_link_libraries(abyss_hot
  PUBLIC abyss::core
  PRIVATE xxHash::xxhash
)
