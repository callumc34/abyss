add_library(abyss_resp
  parser.cpp
  serializer.cpp
  command_registry.cpp
  request_pipeline.cpp
  metrics.cpp
  node_identity.cpp
  info.cpp
  cluster.cpp
  config_get.cpp
  command_info.cpp
)
add_library(abyss::resp ALIAS abyss_resp)

target_link_libraries(abyss_resp
  PUBLIC abyss::core abyss::metrics
  PRIVATE abyss::log
)
