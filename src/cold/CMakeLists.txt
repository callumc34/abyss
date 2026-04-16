add_subdirectory(format)
add_subdirectory(backends)

add_library(abyss_cold INTERFACE)
add_library(abyss::cold ALIAS abyss_cold)

target_link_libraries(abyss_cold INTERFACE abyss::cold_format)

if(ABYSS_WITH_ROCKSDB)
  target_link_libraries(abyss_cold INTERFACE abyss::cold_rocksdb)
endif()
