add_executable(abyss-server main.cpp)

target_link_libraries(abyss-server PRIVATE
  abyss::core
  abyss::resp
  abyss::queue
  abyss::hot
  abyss::cold
  abyss::consumer
  abyss::engine
  abyss::config
  abyss::metrics
)
