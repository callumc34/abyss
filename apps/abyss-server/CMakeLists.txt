find_package(CLI11 CONFIG REQUIRED)
set_package_properties(CLI11 PROPERTIES
  TYPE REQUIRED
  DESCRIPTION "Command line parser for C++17+"
  URL "https://github.com/CLIUtils/CLI11"
  PURPOSE "Server CLI argument parsing"
)

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
