add_library(abyss_config
  config.cpp
  yaml_decode.cpp
  validator.cpp
)
add_library(abyss::config ALIAS abyss_config)

target_link_libraries(abyss_config PUBLIC
  abyss::compiler_options
  abyss::core
)

target_link_libraries(abyss_config PRIVATE
  yaml-cpp::yaml-cpp
)
