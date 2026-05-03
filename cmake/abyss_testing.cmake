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

macro(abyss_add_binary_test target labels)
  add_test(NAME ${target} COMMAND ${target})
  set_tests_properties(${target} PROPERTIES
    LABELS "${labels}"
    TIMEOUT ${ABYSS_TEST_TIMEOUT_SLOW}
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
