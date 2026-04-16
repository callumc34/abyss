add_library(abyss_core
  resp_types.cpp
  consumer_rpc.cpp
)
add_library(abyss::core ALIAS abyss_core)

target_include_directories(abyss_core PUBLIC
  ${PROJECT_SOURCE_DIR}/include
  ${CMAKE_BINARY_DIR}/generated
)

target_link_libraries(abyss_core PUBLIC
  abyss::compiler_options
)
