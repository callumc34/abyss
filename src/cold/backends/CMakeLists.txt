if(ABYSS_WITH_ROCKSDB)
  find_package(RocksDB CONFIG REQUIRED)
  set_package_properties(RocksDB PROPERTIES
    TYPE REQUIRED
    DESCRIPTION "Embedded LSM-based KV store"
    URL "https://rocksdb.org"
    PURPOSE "Built-in cold store backend"
  )

  add_library(abyss_cold_rocksdb
    rocksdb_store.cpp
  )
  add_library(abyss::cold_rocksdb ALIAS abyss_cold_rocksdb)

  target_include_directories(abyss_cold_rocksdb PUBLIC
    ${PROJECT_SOURCE_DIR}/include
  )

  target_link_libraries(abyss_cold_rocksdb
    PUBLIC abyss::core abyss::cold_format
    PRIVATE RocksDB::rocksdb
  )

  target_compile_definitions(abyss_cold_rocksdb PUBLIC ABYSS_HAVE_ROCKSDB)
endif()
