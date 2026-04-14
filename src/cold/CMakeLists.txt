add_library(abyss_cold
  rocksdb_store.cpp
)
add_library(abyss::cold ALIAS abyss_cold)

target_include_directories(abyss_cold PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(abyss_cold PUBLIC
  abyss::core
)
