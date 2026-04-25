set(ABYSS_NET_SOURCES
  socket_ops.cpp
  connection.cpp
  reactor.cpp
  tcp_server.cpp
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  list(APPEND ABYSS_NET_SOURCES poller_epoll.cpp)
elseif(APPLE OR CMAKE_SYSTEM_NAME MATCHES "BSD")
  list(APPEND ABYSS_NET_SOURCES poller_kqueue.cpp)
else()
  message(FATAL_ERROR
    "abyss::net has no Poller backend for ${CMAKE_SYSTEM_NAME}. "
    "Windows IOCP is tracked in a follow-up issue.")
endif()

add_library(abyss_net ${ABYSS_NET_SOURCES})
add_library(abyss::net ALIAS abyss_net)

target_include_directories(abyss_net PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_net
  PUBLIC abyss::core abyss::resp abyss::metrics abyss::log
)
