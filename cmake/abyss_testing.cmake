include(GoogleTest)

macro(abyss_discover_tests target labels)
  set(_labels "${labels}")
  if(CMAKE_CXX_FLAGS MATCHES "fsanitize")
    gtest_discover_tests(${target}
      DISCOVERY_MODE PRE_TEST
      DISCOVERY_TIMEOUT 120
      PROPERTIES LABELS "${_labels}"
    )
  else()
    gtest_discover_tests(${target}
      PROPERTIES LABELS "${_labels}"
    )
  endif()
endmacro()
