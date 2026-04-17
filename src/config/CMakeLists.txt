add_library(abyss_config
  config.cpp
)
add_library(abyss::config ALIAS abyss_config)

target_link_libraries(abyss_config PUBLIC
  abyss::compiler_options
)
