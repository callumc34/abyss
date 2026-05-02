# Sanitizer wiring.
#
# Selection is driven by the ABYSS_SANITIZER cache variable. Flags are attached
# to the abyss::compiler_options INTERFACE library so they propagate to every
# target that transitively links it.
#
# Supported values:
#   none                no sanitizer (default).
#   address             AddressSanitizer.
#   thread              ThreadSanitizer.
#   undefined           UndefinedBehaviorSanitizer (extended check set, clang only).
#   address+undefined   ASan + UBSan combined (default UBSan check group, GCC or clang).

set(ABYSS_SANITIZER_VALUES none address thread undefined address+undefined)
set(ABYSS_SANITIZER "none" CACHE STRING
  "Sanitizer to enable (one of: ${ABYSS_SANITIZER_VALUES})")
set_property(CACHE ABYSS_SANITIZER PROPERTY STRINGS ${ABYSS_SANITIZER_VALUES})

if(NOT ABYSS_SANITIZER IN_LIST ABYSS_SANITIZER_VALUES)
  message(FATAL_ERROR
    "ABYSS_SANITIZER='${ABYSS_SANITIZER}' is not one of: ${ABYSS_SANITIZER_VALUES}")
endif()

if(ABYSS_SANITIZER STREQUAL "none")
  return()
endif()

if(NOT TARGET abyss_compiler_options)
  message(FATAL_ERROR
    "abyss_compiler_options must be defined before sanitizer wiring; "
    "include abyss_compiler_options.cmake first")
endif()

if(NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang|AppleClang)$")
  message(FATAL_ERROR
    "Sanitizers require GCC or Clang (got ${CMAKE_CXX_COMPILER_ID})")
endif()

set(_abyss_san_compile)
set(_abyss_san_link)

list(APPEND _abyss_san_compile -fno-omit-frame-pointer -g)

if(ABYSS_SANITIZER STREQUAL "address")
  list(APPEND _abyss_san_compile -fsanitize=address)
  list(APPEND _abyss_san_link    -fsanitize=address)

elseif(ABYSS_SANITIZER STREQUAL "thread")
  list(APPEND _abyss_san_compile -fsanitize=thread)
  list(APPEND _abyss_san_link    -fsanitize=thread)

elseif(ABYSS_SANITIZER STREQUAL "address+undefined")
  list(APPEND _abyss_san_compile
    -fsanitize=address,undefined
    -fno-sanitize-recover=undefined
  )
  list(APPEND _abyss_san_link -fsanitize=address,undefined)

elseif(ABYSS_SANITIZER STREQUAL "undefined")
  # The extended UBSan check set relies on clang-only groups (nullability,
  # local-bounds) and the modern -fsanitize-ignorelist semantics. GCC's UBSan
  # has known parity gaps in both. The asan preset's address+undefined mode
  # covers GCC with the default check group.
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
    message(FATAL_ERROR
      "ABYSS_SANITIZER=undefined requires Clang (got ${CMAKE_CXX_COMPILER_ID}); "
      "use ABYSS_SANITIZER=address+undefined for the default UBSan check group on GCC")
  endif()

  # Extended check set beyond the default `undefined` group:
  #   nullability           - null passed to _Nonnull-annotated parameters.
  #   local-bounds          - stack-array OOB the default array-bounds misses.
  #   float-divide-by-zero  - finite/zero on float (not in `undefined`).
  #   integer               - signed + unsigned overflow + shifts. Highest-value
  #                           check for offset, sequence, and TTL math.
  #   implicit-conversion   - narrowing/sign-changing implicit conversions.
  set(_ubsan_on undefined,nullability,local-bounds,float-divide-by-zero,integer,implicit-conversion)
  # Sub-checks inside the groups above that are pure noise on idiomatic C++:
  #   implicit-integer-sign-change       - fires on int -> size_t indexing.
  #   implicit-signed-integer-truncation - fires on `uint8_t x = 0xFF`.
  set(_ubsan_off implicit-integer-sign-change,implicit-signed-integer-truncation)

  list(APPEND _abyss_san_compile
    -fsanitize=${_ubsan_on}
    -fno-sanitize=${_ubsan_off}
    -fno-sanitize-recover=all
  )
  list(APPEND _abyss_san_link -fsanitize=${_ubsan_on})

  set(_ubsan_ignorelist "${PROJECT_SOURCE_DIR}/cmake/ubsan_ignorelist.txt")
  if(EXISTS "${_ubsan_ignorelist}")
    list(APPEND _abyss_san_compile "-fsanitize-ignorelist=${_ubsan_ignorelist}")
  endif()
endif()

target_compile_options(abyss_compiler_options INTERFACE ${_abyss_san_compile})
target_link_options(abyss_compiler_options INTERFACE ${_abyss_san_link})

set(ABYSS_SANITIZER_ACTIVE TRUE CACHE INTERNAL
  "True if any sanitizer is enabled. Used to gate test discovery mode.")

message(STATUS "Sanitizer enabled: ${ABYSS_SANITIZER}")
