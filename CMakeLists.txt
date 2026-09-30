cmake_minimum_required(VERSION 3.25)

if(NOT DEFINED CMAKE_TOOLCHAIN_FILE AND DEFINED ENV{VCPKG_ROOT})
  set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
      CACHE STRING "Vcpkg toolchain file")
endif()

# The vcpkg toolchain resolves the manifest during project(), so the test
# framework has to be requested before that. The presets set
# VCPKG_MANIFEST_FEATURES in the cache; appending here shadows it for this
# configure without disturbing the preset value. Mirrors the ABYSS_BUILD_TESTS
# default (ON) for the case where the cache has not been primed yet.
if(NOT DEFINED ABYSS_BUILD_TESTS OR ABYSS_BUILD_TESTS)
  list(APPEND VCPKG_MANIFEST_FEATURES "tests")
endif()

include(cmake/abyss_tsan_triplet.cmake)

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
include(cmake/abyss_sanitizers.cmake)

option(ABYSS_BUILD_TESTS "Build tests" ON)
option(ABYSS_BUILD_BENCHMARKS "Build benchmarks" OFF)
option(ABYSS_BUILD_PERF "Build performance harness (probes, load generator, framework)" OFF)
option(ABYSS_WERROR "Treat warnings as errors" OFF)
option(ABYSS_STRICT_WARNINGS "Enable additional warning flags" OFF)
option(ABYSS_WITH_ROCKSDB "Build the built-in RocksDB cold store backend" ON)
option(ABYSS_WITH_LOGGING "Enable spdlog-backed structured logging" ON)
option(ABYSS_WITH_METRICS "Enable prometheus-cpp-backed metrics" ON)
option(ABYSS_BUILD_FUZZ "Build libFuzzer fuzz targets" OFF)
option(ABYSS_USE_CCACHE "Use ccache as compiler launcher if available" ON)

if(ABYSS_USE_CCACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  if(WIN32)
    find_program(SCCACHE_PROGRAM sccache)
    if(SCCACHE_PROGRAM)
      set(CMAKE_C_COMPILER_LAUNCHER "${SCCACHE_PROGRAM}")
      set(CMAKE_CXX_COMPILER_LAUNCHER "${SCCACHE_PROGRAM}")
      message(STATUS "sccache enabled: ${SCCACHE_PROGRAM}")
    endif()
  else()
    find_program(CCACHE_PROGRAM ccache)
    if(CCACHE_PROGRAM)
      set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
      set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
      message(STATUS "ccache enabled: ${CCACHE_PROGRAM}")
    endif()
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
  PURPOSE "Key hashing for log redaction"
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

find_package(httplib CONFIG REQUIRED)
set_package_properties(httplib PROPERTIES
  TYPE REQUIRED
  DESCRIPTION "Header-only C++ HTTP/HTTPS server and client library"
  URL "https://github.com/yhirose/cpp-httplib"
  PURPOSE "Admin and metrics HTTP endpoints"
)

if(ABYSS_WITH_LOGGING)
  find_package(spdlog CONFIG REQUIRED)
  set_package_properties(spdlog PROPERTIES
    TYPE REQUIRED
    DESCRIPTION "Fast C++ logging library"
    URL "https://github.com/gabime/spdlog"
    PURPOSE "Structured logging backend"
  )
endif()

if(ABYSS_WITH_METRICS)
  find_package(prometheus-cpp CONFIG REQUIRED)
  set_package_properties(prometheus-cpp PROPERTIES
    TYPE REQUIRED
    DESCRIPTION "Prometheus metrics client library for C++"
    URL "https://github.com/jupp0r/prometheus-cpp"
    PURPOSE "Metrics registry and text-format exposition"
  )
endif()

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
add_feature_info(BuildPerf       ABYSS_BUILD_PERF       "Build performance harness")
add_feature_info(StrictWarnings  ABYSS_STRICT_WARNINGS  "Enable additional compiler warning flags")
add_feature_info(Werror          ABYSS_WERROR           "Treat compiler warnings as errors")
add_feature_info(WithRocksDB     ABYSS_WITH_ROCKSDB     "Build the built-in RocksDB cold store backend")
add_feature_info(WithLogging     ABYSS_WITH_LOGGING     "Enable spdlog-backed structured logging")
add_feature_info(WithMetrics     ABYSS_WITH_METRICS     "Enable prometheus-cpp-backed metrics")
add_feature_info(BuildFuzz       ABYSS_BUILD_FUZZ       "Build libFuzzer fuzz targets")
add_feature_info(UseCcache       ABYSS_USE_CCACHE       "Use ccache as compiler launcher if available")

find_package(Git QUIET)
if(Git_FOUND)
  execute_process(
    COMMAND ${GIT_EXECUTABLE} rev-parse --short=12 HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE ABYSS_BUILD_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE _git_status
  )
  if(NOT _git_status EQUAL 0)
    set(ABYSS_BUILD_COMMIT "unknown")
  endif()
else()
  set(ABYSS_BUILD_COMMIT "unknown")
endif()

string(TIMESTAMP ABYSS_BUILD_DATE "%Y-%m-%dT%H:%M:%SZ" UTC)

if(CMAKE_BUILD_TYPE)
  string(TOLOWER "${CMAKE_BUILD_TYPE}" ABYSS_BUILD_TYPE)
else()
  set(ABYSS_BUILD_TYPE "unknown")
endif()

configure_file(
  cmake/version.h.in
  ${CMAKE_BINARY_DIR}/generated/abyss/version.h
  @ONLY
)

add_subdirectory(src/platform)
add_subdirectory(src/core)
add_subdirectory(src/log)
add_subdirectory(src/branding)
add_subdirectory(src/metrics)
add_subdirectory(src/resp)
add_subdirectory(src/queue)
add_subdirectory(src/hot)
add_subdirectory(src/cold)
add_subdirectory(src/consumer)
add_subdirectory(src/engine)
add_subdirectory(src/net)
add_subdirectory(src/config)
add_subdirectory(src/admin)

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

if(ABYSS_BUILD_PERF)
  find_package(hdr_histogram CONFIG REQUIRED)
  set_package_properties(hdr_histogram PROPERTIES
    TYPE OPTIONAL
    DESCRIPTION "C port of HdrHistogram (High Dynamic Range)"
    URL "https://github.com/HdrHistogram/HdrHistogram_c"
    PURPOSE "Lossless latency recording for the performance harness"
  )
  find_package(hiredis CONFIG REQUIRED)
  set_package_properties(hiredis PROPERTIES
    TYPE OPTIONAL
    DESCRIPTION "Minimalistic C client for Redis"
    URL "https://github.com/redis/hiredis"
    PURPOSE "RESP client for the TCP load generator"
  )
endif()

if(ABYSS_BUILD_TESTS)
  enable_testing()
  find_package(GTest CONFIG REQUIRED)
  set_package_properties(GTest PROPERTIES
    TYPE REQUIRED
    DESCRIPTION "Google testing and mocking framework"
    URL "https://github.com/google/googletest"
    PURPOSE "Unit, component, integration and system test suites"
  )
  include(cmake/abyss_testing.cmake)
endif()

if(ABYSS_BUILD_TESTS OR ABYSS_BUILD_BENCHMARKS OR ABYSS_BUILD_PERF OR ABYSS_BUILD_FUZZ)
  add_subdirectory(tests)
endif()

feature_summary(WHAT ALL)
