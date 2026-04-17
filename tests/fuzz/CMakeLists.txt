add_executable(fuzz_resp_parser resp_parser_fuzz.cpp)
target_link_libraries(fuzz_resp_parser PRIVATE
  abyss::resp
  -fsanitize=fuzzer
)
target_compile_options(fuzz_resp_parser PRIVATE -fsanitize=fuzzer)

add_executable(fuzz_wal_decoder wal_decoder_fuzz.cpp)
target_link_libraries(fuzz_wal_decoder PRIVATE
  abyss::queue
  -fsanitize=fuzzer
)
target_compile_options(fuzz_wal_decoder PRIVATE -fsanitize=fuzzer)
