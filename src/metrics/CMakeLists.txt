add_library(abyss_metrics
  metrics.cpp
)
add_library(abyss::metrics ALIAS abyss_metrics)

target_include_directories(abyss_metrics PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(abyss_metrics PUBLIC
  abyss::compiler_options
)
