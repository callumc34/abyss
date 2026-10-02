if(WIN32)
  message(FATAL_ERROR
    "ABYSS_BUILD_FUZZ requires libFuzzer (-fsanitize=fuzzer) which is not wired for MSVC. "
    "Disable ABYSS_BUILD_FUZZ on Windows.")
endif()

if(NOT CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
  message(FATAL_ERROR
    "ABYSS_BUILD_FUZZ requires Clang's libFuzzer (got '${CMAKE_CXX_COMPILER_ID}'). "
    "Configure with clang, or disable ABYSS_BUILD_FUZZ.")
endif()

# Sanitizer-coverage instrumentation resolves its runtime hooks out of the ASan
# runtime; without it the instrumented libraries below do not link.
if(NOT ABYSS_SANITIZER MATCHES "address")
  message(FATAL_ERROR
    "ABYSS_BUILD_FUZZ requires an AddressSanitizer build "
    "(-DABYSS_SANITIZER=address or address+undefined); got '${ABYSS_SANITIZER}'.")
endif()

# Instrument the libraries under test, not just the harness translation units.
# Without coverage on abyss::resp / abyss::queue / abyss::net libFuzzer has no
# feedback signal and degrades to uniform random input generation.
target_compile_options(abyss_compiler_options INTERFACE -fsanitize=fuzzer-no-link)

add_executable(fuzz_resp_parser resp_parser_fuzz.cpp)
target_link_libraries(fuzz_resp_parser PRIVATE
  abyss::resp
  -fsanitize=fuzzer
)
target_compile_options(fuzz_resp_parser PRIVATE -fsanitize=fuzzer)

# Streaming counterpart to fuzz_resp_parser: chunked feed through the pipeline's
# accumulate/consume loop, covering the kIncomplete/resume path.
add_executable(fuzz_resp_stream resp_stream_fuzz.cpp)
target_link_libraries(fuzz_resp_stream PRIVATE
  abyss::resp
  -fsanitize=fuzzer
)
target_compile_options(fuzz_resp_stream PRIVATE -fsanitize=fuzzer)

# The format 2 frame codec: Inspect with and without CRC verification,
# then DecodeEntry. Its CRC oracle uses the queue's private CRC32C and
# byte helpers, hence the source-tree include path.
add_executable(fuzz_frame_decoder frame_decoder_fuzz.cpp)
target_include_directories(fuzz_frame_decoder PRIVATE ${PROJECT_SOURCE_DIR}/src/queue)
target_link_libraries(fuzz_frame_decoder PRIVATE
  abyss::queue
  -fsanitize=fuzzer
)
target_compile_options(fuzz_frame_decoder PRIVATE -fsanitize=fuzzer)

# Log::Open over a real segment whose frame space carries the input, so
# end-of-log, torn-tail and CRC-valid-but-corrupt recovery are all
# exercised, then a walk of what recovery kept.
add_executable(fuzz_log_recovery log_recovery_fuzz.cpp)
target_include_directories(fuzz_log_recovery PRIVATE ${PROJECT_SOURCE_DIR}/src/queue)
target_link_libraries(fuzz_log_recovery PRIVATE
  abyss::queue
  -fsanitize=fuzzer
)
target_compile_options(fuzz_log_recovery PRIVATE -fsanitize=fuzzer)

# Regenerates corpus/frame_decoder and corpus/log_recovery with the
# real encoder: `./fuzz_make_corpus <source dir>/tests/fuzz/corpus`.
add_executable(fuzz_make_corpus make_corpus.cpp)
target_include_directories(fuzz_make_corpus PRIVATE ${PROJECT_SOURCE_DIR}/src/queue)
target_link_libraries(fuzz_make_corpus PRIVATE abyss::queue)

add_executable(fuzz_net_connection net/connection_fuzz.cpp)
target_link_libraries(fuzz_net_connection PRIVATE
  abyss::net
  -fsanitize=fuzzer
)
target_compile_options(fuzz_net_connection PRIVATE -fsanitize=fuzzer)

# Seed corpora land beside the binaries so a local run is
# `./fuzz_resp_stream corpus/resp`.
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/corpus DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
