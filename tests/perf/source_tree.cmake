# Writes OUTPUT, a header naming the tree a perf binary is built from:
# HEAD, plus a hash of its uncommitted and untracked changes when there
# are any. Run at build time, so the name is never older than the build.
# Inputs: GIT, SOURCE_DIR, OUTPUT.
set(tree "unknown")
if(GIT)
  execute_process(
    COMMAND "${GIT}" rev-parse HEAD
    WORKING_DIRECTORY "${SOURCE_DIR}"
    OUTPUT_VARIABLE head
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE head_status)
  if(head_status EQUAL 0)
    execute_process(
      COMMAND "${GIT}" diff HEAD --binary
      WORKING_DIRECTORY "${SOURCE_DIR}"
      OUTPUT_VARIABLE changes
      ERROR_QUIET)
    execute_process(
      COMMAND "${GIT}" ls-files --others --exclude-standard
      WORKING_DIRECTORY "${SOURCE_DIR}"
      OUTPUT_VARIABLE untracked
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET)
    if(untracked)
      string(REPLACE "\n" ";" untracked "${untracked}")
      foreach(path IN LISTS untracked)
        file(SHA256 "${SOURCE_DIR}/${path}" digest)
        string(APPEND changes "${path} ${digest}\n")
      endforeach()
    endif()
    if(changes STREQUAL "")
      set(tree "${head}")
    else()
      string(SHA256 digest "${changes}")
      string(SUBSTRING "${digest}" 0 12 digest)
      set(tree "${head}+dirty.${digest}")
    endif()
  endif()
endif()

set(content "#pragma once\n\ninline constexpr const char* kPerfSourceTree = \"${tree}\";\n")
set(old "")
if(EXISTS "${OUTPUT}")
  file(READ "${OUTPUT}" old)
endif()
if(NOT old STREQUAL content)
  file(WRITE "${OUTPUT}" "${content}")
endif()
