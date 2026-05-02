add_library(abyss_admin
  health_handler.cpp
  http_server.cpp
  json_writer.cpp
  metrics_handler.cpp
  ready_handler.cpp
  status_handler.cpp
)
add_library(abyss::admin ALIAS abyss_admin)

target_include_directories(abyss_admin PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_admin PUBLIC
  abyss::compiler_options
  abyss::core
  abyss::log
)

target_link_libraries(abyss_admin PRIVATE
  httplib::httplib
)
