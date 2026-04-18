add_library(abyss_core
  resp_types.cpp
  resp_format.cpp
  consumer_rpc.cpp
  ops.cpp
  eviction_policy.cpp
)
add_library(abyss::core ALIAS abyss_core)

target_link_libraries(abyss_core PUBLIC
  abyss::compiler_options
)
