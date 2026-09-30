# Selects an instrumented vcpkg triplet for ThreadSanitizer builds. Included
# before project(), because the vcpkg toolchain resolves the triplet there.
#
# TSAN only sees synchronisation performed by instrumented code. Linking a
# dependency built without -fsanitize=thread turns its internal locking into
# false race reports, hides genuine races that pass through it, and drops its
# frames from reported stacks, so every dependency must be instrumented too.

if(NOT ABYSS_SANITIZER STREQUAL "thread")
  return()
endif()

if(DEFINED VCPKG_TARGET_TRIPLET)
  if(NOT VCPKG_TARGET_TRIPLET MATCHES "-tsan$")
    message(FATAL_ERROR
      "ABYSS_SANITIZER=thread requires an instrumented vcpkg triplet (*-tsan), but "
      "VCPKG_TARGET_TRIPLET is '${VCPKG_TARGET_TRIPLET}'. Configure a fresh build "
      "directory with the tsan preset.")
  endif()
else()
  cmake_host_system_information(RESULT _abyss_host_os QUERY OS_NAME)
  cmake_host_system_information(RESULT _abyss_host_arch QUERY OS_PLATFORM)
  if(NOT _abyss_host_os STREQUAL "Linux")
    message(FATAL_ERROR
      "ABYSS_SANITIZER=thread needs instrumented dependencies, which are provided "
      "for Linux only (cmake/triplets/*-linux-tsan.cmake); host is '${_abyss_host_os}'.")
  endif()
  if(_abyss_host_arch MATCHES "^(x86_64|amd64|AMD64)$")
    set(_abyss_tsan_triplet x64-linux-tsan)
  elseif(_abyss_host_arch MATCHES "^(aarch64|arm64)$")
    set(_abyss_tsan_triplet arm64-linux-tsan)
  else()
    message(FATAL_ERROR
      "No instrumented vcpkg triplet for host architecture '${_abyss_host_arch}'.")
  endif()
  set(VCPKG_TARGET_TRIPLET "${_abyss_tsan_triplet}" CACHE STRING "vcpkg target triplet")
endif()

list(APPEND VCPKG_OVERLAY_TRIPLETS "${CMAKE_CURRENT_LIST_DIR}/triplets")
