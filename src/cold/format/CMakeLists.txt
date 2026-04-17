add_library(abyss_cold_format
  key_codec.cpp
)
add_library(abyss::cold_format ALIAS abyss_cold_format)

target_link_libraries(abyss_cold_format PUBLIC
  abyss::core
)
