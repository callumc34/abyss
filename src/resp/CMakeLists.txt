add_library(abyss_resp
  parser.cpp
  serializer.cpp
  command_registry.cpp
  request_pipeline.cpp
)
add_library(abyss::resp ALIAS abyss_resp)

target_include_directories(abyss_resp PUBLIC
  ${PROJECT_SOURCE_DIR}/include
  ${CMAKE_BINARY_DIR}/generated
)

target_link_libraries(abyss_resp PUBLIC
  abyss::core
)
