// libFuzzer harness — POSIX-only because socketpair(AF_UNIX) and the
// libFuzzer entrypoint both depend on Clang's `-fsanitize=fuzzer` runtime,
// which is not currently wired for MSVC. The Windows preset doesn't enable
// ABYSS_BUILD_FUZZ.

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/net/connection.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"
#include "abyss/platform/types.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/request_pipeline.h"

namespace {

class NoopPoller : public abyss::net::Poller {
 public:
  abyss::core::Result<void> Add(abyss::platform::Socket /*fd*/, abyss::net::EventKind /*interest*/,
                                void* /*user_data*/) override {
    return {};
  }
  abyss::core::Result<void> Modify(abyss::platform::Socket /*fd*/,
                                   abyss::net::EventKind /*interest*/,
                                   void* /*user_data*/) override {
    return {};
  }
  abyss::core::Result<void> Remove(abyss::platform::Socket /*fd*/) override { return {}; }
  abyss::core::Result<std::span<const abyss::net::Event>> Wait(
      std::chrono::milliseconds /*timeout*/) override {
    return std::span<const abyss::net::Event>{};
  }
  abyss::core::Result<void> Wake() override { return {}; }
};

class NoopDispatcher : public abyss::core::CommandDispatcher {
 public:
  abyss::core::Result<abyss::core::RespValue> DispatchRead(
      std::string_view /*name*/, const abyss::core::RespCommand& /*cmd*/) override {
    return abyss::core::RespValue::BulkString("v");
  }
  abyss::core::Result<abyss::core::RespValue> DispatchWrite(
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

void SetNonBlocking(int fd) {
  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
  const int flags = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  // NOLINTEND(cppcoreguidelines-pro-type-vararg)
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  int sv[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) return 0;
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);

  NoopPoller poller;
  NoopDispatcher dispatcher;
  abyss::net::NetMetrics metrics;
  abyss::net::ConnectionConfig config{
      .max_read_buffer_bytes = 65536,
      .write_backpressure_bytes = 4096,
      .write_resume_bytes = 1024,
      .write_hard_limit_bytes = 16384,
      .idle_timeout = std::chrono::seconds{60},
  };

  abyss::net::Connection conn(
      abyss::net::Fd(sv[0]), 0, 0, /*client_id=*/1, poller, abyss::resp::GlobalRegistry(),
      abyss::resp::PipelineDependencies{.dispatcher = &dispatcher}, config, metrics);
  // NOLINTNEXTLINE(bugprone-unused-return-value)
  (void)conn.Arm();

  constexpr size_t kChunk = 64;
  size_t off = 0;
  while (off < size && !conn.IsClosed()) {
    const size_t n = std::min(kChunk, size - off);
    const ssize_t w = ::send(sv[1], data + off, n, 0);
    if (w <= 0) break;
    off += static_cast<size_t>(w);
    conn.OnReadable();
    conn.OnWritable();
  }

  ::close(sv[1]);
  return 0;
}
