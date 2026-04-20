add_library(abyss_log
  log.cpp
  testing.cpp
)
add_library(abyss::log ALIAS abyss_log)

target_include_directories(abyss_log PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(abyss_log PUBLIC
  abyss::compiler_options
)

if(ABYSS_WITH_LOGGING)
  target_compile_definitions(abyss_log PRIVATE ABYSS_WITH_LOGGING=1)
  target_link_libraries(abyss_log PRIVATE
    spdlog::spdlog
    xxHash::xxhash
  )
else()
  target_link_libraries(abyss_log PRIVATE
    xxHash::xxhash
  )
endif()
