add_library(abyss_metrics
  metrics.cpp
  testing.cpp
)
add_library(abyss::metrics ALIAS abyss_metrics)

target_include_directories(abyss_metrics PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_metrics PUBLIC
  abyss::compiler_options
)

if(ABYSS_WITH_METRICS)
  target_compile_definitions(abyss_metrics PRIVATE ABYSS_WITH_METRICS=1)
  target_link_libraries(abyss_metrics PRIVATE
    prometheus-cpp::core
  )
endif()
