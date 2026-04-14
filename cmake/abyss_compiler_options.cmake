add_library(abyss_compiler_options INTERFACE)
add_library(abyss::compiler_options ALIAS abyss_compiler_options)

target_compile_features(abyss_compiler_options INTERFACE cxx_std_23)

target_compile_options(abyss_compiler_options INTERFACE
  -Wall
  -Wextra
  -Wpedantic
)

target_compile_options(abyss_compiler_options INTERFACE
  $<$<CXX_COMPILER_ID:Clang>:-Wthread-safety>
)

if(ABYSS_STRICT_WARNINGS)
  target_compile_options(abyss_compiler_options INTERFACE
    -Wconversion
    -Wsign-conversion
    -Wcast-align
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
    -Wimplicit-fallthrough
  )
endif()

if(ABYSS_WERROR)
  target_compile_options(abyss_compiler_options INTERFACE -Werror)
endif()
