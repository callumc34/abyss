find_package(xxHash CONFIG REQUIRED)

add_library(abyss_hot
  shard_router.cpp
  single_shard_store.cpp
  sharded_hot_store.cpp
)
add_library(abyss::hot ALIAS abyss_hot)

target_include_directories(abyss_hot PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(abyss_hot PUBLIC
  abyss::core
  xxHash::xxhash
)
