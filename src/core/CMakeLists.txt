add_library(abyss_core
  resp_types.cpp
  resp_format.cpp
  ops.cpp
  eviction_policy.cpp
  shard_router.cpp
  slot.cpp
  slot_shard_map.cpp
  topology_manifest.cpp
  atomic_file.cpp
  fatal.cpp
  durability.cpp
  queue.cpp
)
add_library(abyss::core ALIAS abyss_core)

target_link_libraries(abyss_core
  PUBLIC abyss::compiler_options
  PRIVATE abyss::log abyss::platform
)
