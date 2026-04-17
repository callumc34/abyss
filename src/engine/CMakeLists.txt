add_library(abyss_engine
  tiering_engine.cpp
)
add_library(abyss::engine ALIAS abyss_engine)

target_include_directories(abyss_engine PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(abyss_engine PUBLIC
  abyss::core
  abyss::consumer
  abyss::hot
)
