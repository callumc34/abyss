// libFuzzer harness for the INCREMENTAL RESP path.
//
// resp_parser_fuzz hands the parser one complete buffer, so it never reaches the
// kIncomplete/resume transitions the real server lives in. Here the input is cut
// into content-derived chunks and pushed through the same accumulate/Process/
// compact loop Connection runs, so a frame that straddles a chunk boundary is
// parsed exactly the way a frame straddling a socket read is.
//
// POSIX/Clang only: the libFuzzer entrypoint depends on -fsanitize=fuzzer.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/parser.h"
#include "abyss/resp/request_pipeline.h"

namespace {

class NoopDispatcher : public abyss::core::CommandDispatcher {
 public:
  abyss::core::Result<abyss::core::RespValue> DispatchRead(
      std::string_view /*name*/, const abyss::core::RespCommand& /*cmd*/) override {
    return abyss::core::RespValue::BulkString("v");
  }
  abyss::core::Result<abyss::core::RespValue> DispatchWrite(
      std::string_view /*name*/, abyss::core::RespCommand /*cmd*/) override {
    return abyss::core::RespValue::SimpleString("OK");
  }
  abyss::core::Result<abyss::core::RespValue> DispatchConditional(
      std::string_view /*name*/, abyss::core::RespCommand /*cmd*/,
      abyss::core::PredicateFlags /*flags*/) override {
    return abyss::core::RespValue::SimpleString("OK");
  }
  abyss::core::Result<abyss::core::RespValue> DispatchFanOut(
      abyss::core::MultiKeyKind /*kind*/, abyss::core::RespCommand /*cmd*/) override {
    return abyss::core::RespValue::SimpleString("OK");
  }
  abyss::core::Result<abyss::core::RespValue> DispatchFlush(
      abyss::core::FlushTarget /*target*/) override {
    return abyss::core::RespValue::SimpleString("OK");
  }
};

// Mirrors Connection's read cap: a frame that cannot complete inside it is a
// fail-closed teardown, never an unbounded buffer.
constexpr size_t kMaxReadBufferBytes = 64 * 1024;
constexpr size_t kMaxChunkBytes = 37;

// Chunk widths come from the input itself, so libFuzzer steers the split with
// ordinary byte mutations and the committed corpus stays plain RESP bytes.
size_t NextChunkSize(std::span<const uint8_t> input, size_t offset) {
  const size_t width = 1 + ((static_cast<size_t>(input[offset]) ^ (offset * 31)) % kMaxChunkBytes);
  return std::min(width, input.size() - offset);
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size == 0) return 0;
  const std::span<const uint8_t> input(data, size);

  NoopDispatcher dispatcher;
  abyss::resp::RequestPipeline pipeline(
      abyss::resp::GlobalRegistry(), abyss::resp::ConnectionState{.client_id = 1},
      abyss::resp::PipelineDependencies{.dispatcher = &dispatcher});

  std::vector<uint8_t> read_buf;
  std::vector<uint8_t> write_buf;

  for (size_t offset = 0; offset < input.size();) {
    const size_t chunk = NextChunkSize(input, offset);
    read_buf.insert(read_buf.end(), input.begin() + static_cast<ptrdiff_t>(offset),
                    input.begin() + static_cast<ptrdiff_t>(offset + chunk));
    offset += chunk;

    const auto result = pipeline.Process(read_buf, write_buf);
    read_buf.erase(read_buf.begin(),
                   read_buf.begin() + static_cast<ptrdiff_t>(result.bytes_consumed));
    write_buf.clear();

    if (result.close_reason != abyss::resp::RequestPipeline::ProcessCloseReason::kNone) break;
    if (read_buf.size() >= kMaxReadBufferBytes) break;
  }

  // Whatever survives the stream must still classify: an incomplete frame or a
  // protocol error, never a state the connection could wait on forever.
  if (!read_buf.empty()) {
    // NOLINTNEXTLINE(bugprone-unused-return-value)
    (void)abyss::resp::Parser::ParseCommand(read_buf);
  }
  return 0;
}
