# The server, apart from main: the binary links it, and so can a test
# that runs a server in its own process.
add_library(abyss_server_core STATIC server.cpp providers.cpp)

target_include_directories(abyss_server_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(abyss_server_core PUBLIC
  abyss::admin
  abyss::branding
  abyss::config
  abyss::engine
  abyss::net
  abyss::queue
  abyss::resp
)

if(ABYSS_WITH_ROCKSDB)
  target_link_libraries(abyss_server_core PUBLIC abyss::cold_rocksdb)
endif()

add_executable(abyss-server main.cpp)

target_link_libraries(abyss-server PRIVATE
  abyss_server_core
  CLI11::CLI11
)
