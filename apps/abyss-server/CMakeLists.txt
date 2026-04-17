add_executable(abyss-server main.cpp)

target_link_libraries(abyss-server PRIVATE
  abyss::engine
  abyss::queue
  abyss::cold
  abyss::config
  abyss::metrics
)
