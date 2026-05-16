add_library(abyss_branding
  banner.cpp
)
add_library(abyss::branding ALIAS abyss_branding)

target_link_libraries(abyss_branding
  PUBLIC abyss::compiler_options
)
