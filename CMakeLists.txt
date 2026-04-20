cmake_minimum_required(VERSION 3.25)

if(NOT DEFINED CMAKE_TOOLCHAIN_FILE AND DEFINED ENV{VCPKG_ROOT})
  set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
      CACHE STRING "Vcpkg toolchain file")
endif()

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

include(FeatureSummary)
include(cmake/abyss_compiler_options.cmake)

option(ABYSS_BUILD_TESTS "Build tests" ON)
option(ABYSS_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(ABYSS_WERROR "Treat warnings as errors" OFF)
option(ABYSS_STRICT_WARNINGS "Enable additional warning flags" OFF)
option(ABYSS_WITH_ROCKSDB "Build the built-in RocksDB cold store backend" ON)
option(ABYSS_BUILD_FUZZ "Build libFuzzer fuzz targets" OFF)
option(ABYSS_USE_CCACHE "Use ccache as compiler launcher if available" ON)

if(ABYSS_USE_CCACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  find_program(CCACHE_PROGRAM ccache)
  if(CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    message(STATUS "ccache enabled: ${CCACHE_PROGRAM}")
  endif()
endif()

find_package(Crc32c CONFIG REQUIRED)
set_package_properties(Crc32c PROPERTIES
  TYPE REQUIRED
  DESCRIPTION "Hardware-accelerated CRC32C integrity checksums"
  URL "https://github.com/google/crc32c"
  PURPOSE "WAL segment and entry integrity verification"
)

find_package(xxHash CONFIG REQUIRED)
set_package_properties(xxHash PROPERTIES
  TYPE REQUIRED
  DESCRIPTION "Extremely fast hash algorithm"
  URL "https://github.com/Cyan4973/xxHash"
  PURPOSE "Key hashing for shard routing"
)

find_package(CLI11 CONFIG REQUIRED)
set_package_properties(CLI11 PROPERTIES
  TYPE REQUIRED
  DESCRIPTION "Command line parser for C++17+"
  URL "https://github.com/CLIUtils/CLI11"
  PURPOSE "Server CLI argument parsing"
)

find_package(yaml-cpp CONFIG REQUIRED)
set_package_properties(yaml-cpp PROPERTIES
  TYPE REQUIRED
  DESCRIPTION "YAML parser for C++"
  URL "https://github.com/jbeder/yaml-cpp"
  PURPOSE "Configuration file parsing"
)

if(ABYSS_WITH_ROCKSDB)
  find_package(RocksDB CONFIG REQUIRED)
  set_package_properties(RocksDB PROPERTIES
    TYPE REQUIRED
    DESCRIPTION "Embedded LSM-based KV store"
    URL "https://rocksdb.org"
    PURPOSE "Built-in cold store backend"
  )
endif()

add_feature_info(BuildTests      ABYSS_BUILD_TESTS      "Build unit and integration tests")
add_feature_info(BuildBenchmarks ABYSS_BUILD_BENCHMARKS "Build micro-benchmarks")
add_feature_info(StrictWarnings  ABYSS_STRICT_WARNINGS  "Enable additional compiler warning flags")
add_feature_info(Werror          ABYSS_WERROR           "Treat compiler warnings as errors")
add_feature_info(WithRocksDB     ABYSS_WITH_ROCKSDB     "Build the built-in RocksDB cold store backend")
add_feature_info(BuildFuzz       ABYSS_BUILD_FUZZ       "Build libFuzzer fuzz targets")
add_feature_info(UseCcache       ABYSS_USE_CCACHE       "Use ccache as compiler launcher if available")

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

if(ABYSS_BUILD_BENCHMARKS)
  find_package(benchmark CONFIG REQUIRED)
  set_package_properties(benchmark PROPERTIES
    TYPE OPTIONAL
    DESCRIPTION "Google micro-benchmark framework"
    URL "https://github.com/google/benchmark"
    PURPOSE "Encode/decode throughput measurements"
  )
endif()

if(ABYSS_BUILD_TESTS)
  enable_testing()
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
  )
  FetchContent_MakeAvailable(googletest)
  include(cmake/abyss_testing.cmake)
endif()

if(ABYSS_BUILD_TESTS OR ABYSS_BUILD_BENCHMARKS)
  add_subdirectory(tests)
endif()

feature_summary(WHAT ALL)
