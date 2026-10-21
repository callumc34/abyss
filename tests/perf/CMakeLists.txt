# The tree a perf binary is built from, for the probes' reports and the
# benchmarks' context. Regenerated on every build; rewritten only when
# the tree changed.
set(ABYSS_PERF_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
add_custom_target(abyss_perf_source_tree
  COMMAND ${CMAKE_COMMAND} -DGIT=${GIT_EXECUTABLE} -DSOURCE_DIR=${PROJECT_SOURCE_DIR}
          -DOUTPUT=${ABYSS_PERF_GENERATED_DIR}/perf_source_tree.h
          -P ${CMAKE_CURRENT_SOURCE_DIR}/source_tree.cmake
  BYPRODUCTS ${ABYSS_PERF_GENERATED_DIR}/perf_source_tree.h
  VERBATIM
)

if(ABYSS_BUILD_PERF)
  add_subdirectory(framework)
  if(ABYSS_BUILD_TESTS)
    add_subdirectory(framework/tests)
  endif()
  add_subdirectory(probe)
  add_subdirectory(load)
endif()

if(ABYSS_BUILD_BENCHMARKS)
  add_subdirectory(micro)
endif()
