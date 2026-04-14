add_library(abyss_config
  config.cpp
)
add_library(abyss::config ALIAS abyss_config)

target_include_directories(abyss_config PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(abyss_config PUBLIC
  abyss::compiler_options
)
