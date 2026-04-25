add_executable(abyss-server main.cpp server.cpp providers.cpp)

target_link_libraries(abyss-server PRIVATE
  abyss::config
  abyss::engine
  abyss::log
  abyss::metrics
  abyss::net
  abyss::queue
  abyss::resp
  CLI11::CLI11
)

if(ABYSS_WITH_ROCKSDB)
  target_link_libraries(abyss-server PRIVATE abyss::cold_rocksdb)
endif()
