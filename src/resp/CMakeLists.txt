add_library(abyss_resp
  parser.cpp
  serializer.cpp
)
add_library(abyss::resp ALIAS abyss_resp)

target_include_directories(abyss_resp PUBLIC
  ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(abyss_resp PUBLIC
  abyss::core
)
