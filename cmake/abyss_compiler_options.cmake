add_library(abyss_compiler_options INTERFACE)
add_library(abyss::compiler_options ALIAS abyss_compiler_options)

target_compile_features(abyss_compiler_options INTERFACE cxx_std_23)

target_include_directories(abyss_compiler_options INTERFACE
  ${PROJECT_SOURCE_DIR}/include
  ${CMAKE_BINARY_DIR}/generated
)

set(ABYSS_GCC_CLANG_WARNINGS
  -Wall
  -Wextra
  -Wpedantic
  # GCC warns on C++20 designated init with omitted class-type members; Clang doesn't.
  -Wno-missing-field-initializers
)

set(ABYSS_MSVC_WARNINGS
  /W4
  /permissive-
)

target_compile_options(abyss_compiler_options INTERFACE
  $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:${ABYSS_GCC_CLANG_WARNINGS}>
  $<$<CXX_COMPILER_ID:MSVC>:${ABYSS_MSVC_WARNINGS}>
)

target_compile_options(abyss_compiler_options INTERFACE
  $<$<CXX_COMPILER_ID:Clang>:-Wthread-safety>
)

if(ABYSS_STRICT_WARNINGS)
  set(ABYSS_GCC_CLANG_STRICT
    -Wconversion
    -Wsign-conversion
    -Wcast-align
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
    -Wimplicit-fallthrough
  )
  set(ABYSS_MSVC_STRICT
    /w14242
    /w14254
    /w14263
    /w14265
    /w14287
    /w14296
    /w14311
    /w14545
    /w14546
    /w14547
    /w14549
    /w14555
    /w14619
    /w14640
    /w14826
    /w14905
    /w14906
  )
  target_compile_options(abyss_compiler_options INTERFACE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:${ABYSS_GCC_CLANG_STRICT}>
    $<$<CXX_COMPILER_ID:MSVC>:${ABYSS_MSVC_STRICT}>
  )
endif()

if(ABYSS_WERROR)
  target_compile_options(abyss_compiler_options INTERFACE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Werror>
    $<$<CXX_COMPILER_ID:MSVC>:/WX>
  )
endif()
