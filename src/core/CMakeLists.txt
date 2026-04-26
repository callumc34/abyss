add_library(abyss_core
  resp_types.cpp
  resp_format.cpp
  consumer_rpc.cpp
  apply_notifier.cpp
  ops.cpp
  eviction_policy.cpp
  shard_router.cpp
  slot.cpp
  atomic_file.cpp
)
add_library(abyss::core ALIAS abyss_core)

target_link_libraries(abyss_core
  PUBLIC abyss::compiler_options
  PRIVATE xxHash::xxhash abyss::log abyss::platform
)
