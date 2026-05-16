add_executable(abyss-server main.cpp server.cpp providers.cpp)

target_link_libraries(abyss-server PRIVATE
  abyss::admin
  abyss::branding
  abyss::config
  abyss::engine
  abyss::net
  abyss::queue
  abyss::resp
  CLI11::CLI11
)

if(ABYSS_WITH_ROCKSDB)
  target_link_libraries(abyss-server PRIVATE abyss::cold_rocksdb)
endif()
