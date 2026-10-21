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
#   undefined           UndefinedBehaviorSanitizer (default check group + float-divide-by-zero).
#   address+undefined   ASan + UBSan combined (default UBSan check group).

set(ABYSS_SANITIZER_VALUES none address thread undefined address+undefined)
set(ABYSS_SANITIZER "none" CACHE STRING
  "Sanitizer to enable (one of: ${ABYSS_SANITIZER_VALUES})")
set_property(CACHE ABYSS_SANITIZER PROPERTY STRINGS ${ABYSS_SANITIZER_VALUES})

if(NOT ABYSS_SANITIZER IN_LIST ABYSS_SANITIZER_VALUES)
  message(FATAL_ERROR
    "ABYSS_SANITIZER='${ABYSS_SANITIZER}' is not one of: ${ABYSS_SANITIZER_VALUES}")
endif()

# Reset on every configure, so a build dir reconfigured to none drops it.
set(ABYSS_SANITIZER_ACTIVE FALSE CACHE INTERNAL
  "True if any sanitizer is enabled. Used to gate test discovery mode.")

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
  set(_ubsan_on undefined,float-divide-by-zero)
  list(APPEND _abyss_san_compile
    -fsanitize=${_ubsan_on}
    -fno-sanitize-recover=all
  )
  list(APPEND _abyss_san_link -fsanitize=${_ubsan_on})

  # -fsanitize-ignorelist is clang-only; GCC has no equivalent.
  if(CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
    set(_ubsan_ignorelist "${PROJECT_SOURCE_DIR}/cmake/ubsan_ignorelist.txt")
    if(EXISTS "${_ubsan_ignorelist}")
      list(APPEND _abyss_san_compile "-fsanitize-ignorelist=${_ubsan_ignorelist}")
    endif()
  endif()
endif()

target_compile_options(abyss_compiler_options INTERFACE ${_abyss_san_compile})
target_link_options(abyss_compiler_options INTERFACE ${_abyss_san_link})

set(ABYSS_SANITIZER_ACTIVE TRUE CACHE INTERNAL
  "True if any sanitizer is enabled. Used to gate test discovery mode.")

message(STATUS "Sanitizer enabled: ${ABYSS_SANITIZER}")
