include(GoogleTest)

set(ABYSS_TEST_TIMEOUT_DEFAULT 30)
set(ABYSS_TEST_TIMEOUT_SLOW 300)

function(_abyss_discover_tests_impl target labels timeout)
  if(ABYSS_SANITIZER_ACTIVE)
    gtest_discover_tests(${target}
      DISCOVERY_MODE PRE_TEST
      DISCOVERY_TIMEOUT 120
      PROPERTIES
        LABELS "${labels}"
        TIMEOUT ${timeout}
    )
  else()
    gtest_discover_tests(${target}
      PROPERTIES
        LABELS "${labels}"
        TIMEOUT ${timeout}
    )
  endif()
endfunction()

macro(abyss_discover_tests target labels)
  _abyss_discover_tests_impl(${target} "${labels}" ${ABYSS_TEST_TIMEOUT_DEFAULT})
endmacro()

macro(abyss_discover_slow_tests target labels)
  _abyss_discover_tests_impl(${target} "${labels}\;slow" ${ABYSS_TEST_TIMEOUT_SLOW})
endmacro()

# A whole suite as one test, which sanitizers slow several times over.
macro(abyss_add_binary_test target labels)
  add_test(NAME ${target} COMMAND ${target})
  if(ABYSS_SANITIZER_ACTIVE)
    set(_abyss_binary_timeout 900)
  else()
    set(_abyss_binary_timeout ${ABYSS_TEST_TIMEOUT_SLOW})
  endif()
  set_tests_properties(${target} PROPERTIES
    LABELS "${labels}"
    TIMEOUT ${_abyss_binary_timeout}
  )
endmacro()

# Per-test timeout override for tests discovered via gtest_discover_tests.
function(abyss_mark_slow_tests tag dir)
  set(_overrides_file "${CMAKE_BINARY_DIR}/abyss_slow_${tag}_overrides.cmake")
  set(_content "")
  foreach(_test IN LISTS ARGN)
    string(APPEND _content
      "set_tests_properties(${_test} PROPERTIES TIMEOUT ${ABYSS_TEST_TIMEOUT_SLOW})\n")
  endforeach()
  file(WRITE "${_overrides_file}" "${_content}")
  set_property(DIRECTORY "${dir}" APPEND PROPERTY TEST_INCLUDE_FILES "${_overrides_file}")
endfunction()

# Runs the named discovered tests alone: they assert wall-clock rates
# that a busy host's other tests would distort.
function(abyss_mark_serial_tests tag dir)
  set(_overrides_file "${CMAKE_BINARY_DIR}/abyss_serial_${tag}_overrides.cmake")
  set(_content "")
  foreach(_test IN LISTS ARGN)
    string(APPEND _content "set_tests_properties(${_test} PROPERTIES RUN_SERIAL TRUE)\n")
  endforeach()
  file(WRITE "${_overrides_file}" "${_content}")
  set_property(DIRECTORY "${dir}" APPEND PROPERTY TEST_INCLUDE_FILES "${_overrides_file}")
endfunction()
