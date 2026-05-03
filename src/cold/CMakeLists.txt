add_subdirectory(format)
add_subdirectory(backends)

add_library(abyss_cold_ttl_scanner
  ttl_scanner.cpp
)
add_library(abyss::cold_ttl_scanner ALIAS abyss_cold_ttl_scanner)

target_link_libraries(abyss_cold_ttl_scanner
  PUBLIC abyss::core abyss::metrics
  PRIVATE abyss::log
)

add_library(abyss_cold INTERFACE)
add_library(abyss::cold ALIAS abyss_cold)

target_link_libraries(abyss_cold INTERFACE abyss::cold_format abyss::cold_ttl_scanner)

if(ABYSS_WITH_ROCKSDB)
  target_link_libraries(abyss_cold INTERFACE abyss::cold_rocksdb)
endif()
