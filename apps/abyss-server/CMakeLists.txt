add_executable(abyss-server main.cpp server.cpp)

target_link_libraries(abyss-server PRIVATE
  abyss::config
  abyss::consumer
  abyss::core
  abyss::engine
  abyss::hot
  abyss::metrics
  abyss::queue
  abyss::resp
  CLI11::CLI11
)

if(ABYSS_WITH_ROCKSDB)
  target_link_libraries(abyss-server PRIVATE abyss::cold_rocksdb)
endif()
