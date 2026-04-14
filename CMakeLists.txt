cmake_minimum_required(VERSION 3.25)
project(abyss
  VERSION 0.1.0
  LANGUAGES CXX
  DESCRIPTION "Redis-compatible hot-cold tiered KV store"
  HOMEPAGE_URL "https://github.com/callumc34/abyss"
)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(cmake/abyss_compiler_options.cmake)

option(ABYSS_BUILD_TESTS "Build tests" ON)
option(ABYSS_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(ABYSS_WERROR "Treat warnings as errors" OFF)
option(ABYSS_STRICT_WARNINGS "Enable additional warning flags" OFF)

configure_file(
  cmake/version.h.in
  ${CMAKE_BINARY_DIR}/generated/abyss/version.h
  @ONLY
)

add_subdirectory(src/core)
add_subdirectory(src/resp)
add_subdirectory(src/queue)
add_subdirectory(src/hot)
add_subdirectory(src/cold)
add_subdirectory(src/consumer)
add_subdirectory(src/engine)
add_subdirectory(src/config)
add_subdirectory(src/metrics)

add_subdirectory(apps/abyss-server)

if(ABYSS_BUILD_TESTS)
  enable_testing()
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
  )
  FetchContent_MakeAvailable(googletest)
  add_subdirectory(tests)
endif()
