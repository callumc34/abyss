add_library(abyss_platform
  fs_posix.cpp
  fs_windows.cpp
  net_posix.cpp
  net_windows.cpp
  random_posix.cpp
  random_windows.cpp
)
add_library(abyss::platform ALIAS abyss_platform)

target_link_libraries(abyss_platform
  PUBLIC abyss::compiler_options
)

if(WIN32)
  target_link_libraries(abyss_platform PUBLIC ws2_32 bcrypt)
endif()
