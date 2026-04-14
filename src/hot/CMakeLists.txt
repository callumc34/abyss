add_library(abyss_hot
  hashmap_store.cpp
)
add_library(abyss::hot ALIAS abyss_hot)

target_include_directories(abyss_hot PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(abyss_hot PUBLIC
  abyss::core
)
