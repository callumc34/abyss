add_library(abyss_metrics
  metrics.cpp
)
add_library(abyss::metrics ALIAS abyss_metrics)

target_link_libraries(abyss_metrics PUBLIC
  abyss::compiler_options
)
