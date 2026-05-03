if(ABYSS_WITH_ROCKSDB)
  add_library(abyss_cold_rocksdb
    rocksdb_store.cpp
  )
  add_library(abyss::cold_rocksdb ALIAS abyss_cold_rocksdb)

  target_link_libraries(abyss_cold_rocksdb
    PUBLIC abyss::core abyss::cold_format abyss::cold_ttl_scanner
    PRIVATE RocksDB::rocksdb abyss::log
  )

  target_compile_definitions(abyss_cold_rocksdb PUBLIC ABYSS_HAVE_ROCKSDB)
endif()
