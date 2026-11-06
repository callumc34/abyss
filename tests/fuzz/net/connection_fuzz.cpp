// libFuzzer harness — POSIX-only because socketpair(AF_UNIX) and the
// libFuzzer entrypoint both depend on Clang's `-fsanitize=fuzzer` runtime,
// which is not currently wired for MSVC. The Windows preset doesn't enable
// ABYSS_BUILD_FUZZ.
//
// The input is sent to a Connection in chunks, over a socketpair, with a
// fake async engine (fake_async_engine.h) behind its pipeline. Between
// chunks the input resumes tasks, releases durable waits, passes
// deadlines, reads replies or not, and closes the connection, as its
// reactor would see them; a closed connection's pipeline and a given-up
// command's task go to the orphans, as the reactor keeps them. Replies
// must leave in command order, none twice, and none from a command
// after it was given up.

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/net/connection.h"
#include "abyss/net/poller.h"
#include "abyss/net/socket_ops.h"
#include "abyss/platform/types.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/orphans.h"
#include "abyss/resp/request_pipeline.h"
#include "fake_async_engine.h"

namespace {

using abyss::fuzz::Check;

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

void SetNonBlocking(int fd) {
  // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
  const int flags = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  // NOLINTEND(cppcoreguidelines-pro-type-vararg)
}

constexpr uint64_t kClient = 1;
constexpr size_t kChunk = 64;
constexpr int kSettleRounds = 64;

class Harness {
 public:
  Harness(std::span<const uint8_t> input, int server_fd, int client_fd)
      : client_fd_(client_fd),
        tape_(input),
        engine_(tape_, inbox_),
        conn_(std::make_unique<abyss::net::Connection>(
            abyss::net::Fd(server_fd), 0, 0, kClient, poller_, abyss::resp::GlobalRegistry(),
            abyss::resp::PipelineDependencies{.dispatcher = &engine_},
            abyss::net::ConnectionConfig{
                .max_read_buffer_bytes = 65536,
                .write_backpressure_bytes = 4096,
                .write_resume_bytes = 1024,
                .write_hard_limit_bytes = 16384,
                .idle_timeout = std::chrono::seconds{60},
                .max_inflight_commands = 1 + tape_.Below(8),
            },
            metrics_, abyss::core::DefaultSteadyClock,
            abyss::resp::PipelineHost{.sink = &inbox_})) {
    // NOLINTNEXTLINE(bugprone-unused-return-value)
    (void)conn_->Arm();
  }

  Harness(const Harness&) = delete;
  Harness& operator=(const Harness&) = delete;
  Harness(Harness&&) = delete;
  Harness& operator=(Harness&&) = delete;

  ~Harness() = default;

  // Lands every owed post, then checks what is left.
  void Finish() {
    Settle();
    Check(orphans_.empty(), "orphans left once everything landed");
    if (!closed_) {
      Check(conn_->Pipeline() == nullptr || conn_->Pipeline()->Quiescent(),
            "a pipeline owed a post at the end");
      Check(replies_ == sent_.count(), "a command never replied, or replied twice");
    }
  }

  // Sends a chunk; false once the connection takes no more.
  bool Send(std::span<const uint8_t> chunk) {
    if (closed_) return false;
    const ssize_t w = ::send(client_fd_, chunk.data(), chunk.size(), 0);
    if (w <= 0) return false;
    sent_.Add(chunk.first(static_cast<size_t>(w)));
    conn_->OnReadable();
    conn_->OnWritable();
    Step();
    return !closed_;
  }

 private:
  // What the reactor might see before the next read, in the input's
  // order.
  void Step() {
    for (int steps = 1 + static_cast<int>(tape_.Below(4)); steps > 0 && !closed_; --steps) {
      switch (tape_.Below(7)) {
        case 0:
          engine_.ResumeOne();
          break;
        case 1:
          engine_.ReleaseOne();
          break;
        case 2:
          engine_.Advance(std::chrono::milliseconds{tape_.Below(3) * 60});
          Expire();
          break;
        case 3:
          if (tape_.Below(16) == 0) {
            conn_->Close(tape_.Below(2) == 0 ? abyss::metrics::CloseReason::kServerShutdown
                                             : abyss::metrics::CloseReason::kClient);
          }
          break;
        case 4:
        case 5:
          ReadClient();
          break;
        default:
          break;
      }
      Route();
    }
  }

  void Expire() {
    if (!closed_) {
      if (auto orphan = conn_->OnTimer(engine_.now())) orphans_.Adopt(std::move(orphan));
    }
    orphans_.Expire(kClient, engine_.now());
  }

  void Route() {
    while (!inbox_.empty()) {
      for (const auto& event : inbox_.Take()) {
        (void)orphans_.Route(event, closed_ ? nullptr : conn_->Pipeline());
      }
      if (!closed_) conn_->Pump();
      Reap();
    }
    Reap();
  }

  // A connection that closed, by the input's choice or its own, hands
  // its pipeline to the orphans.
  void Reap() {
    if (closed_ || !conn_->IsClosed()) return;
    closed_ = true;
    if (auto pipeline = conn_->ReleasePipeline()) orphans_.Adopt(std::move(pipeline));
  }

  void ReadClient() {
    std::array<uint8_t, 4096> buf{};
    for (;;) {
      const ssize_t n = ::recv(client_fd_, buf.data(), buf.size(), 0);
      if (n <= 0) break;
      received_.insert(received_.end(), buf.begin(), buf.begin() + n);
    }
    replies_ += abyss::fuzz::ReadReplies(received_, parsed_, engine_);
    if (!closed_) conn_->OnWritable();
    Reap();
  }

  // Every owed post lands, every deadline passes, and the client reads
  // everything.
  void Settle() {
    for (int round = 0; round < kSettleRounds; ++round) {
      engine_.ResumeAll();
      engine_.ReleaseAll();
      Route();
      engine_.Advance(std::chrono::milliseconds{1000});
      Expire();
      Route();
      if (!closed_) conn_->OnReadable();
      Route();
      // Last, so a reply written in this round is read in it.
      const size_t replies = replies_;
      ReadClient();
      const bool quiet = replies_ == replies && orphans_.empty() && !engine_.Owes() &&
                         inbox_.empty() &&
                         (closed_ || (conn_->Pipeline()->Quiescent() && !conn_->Busy()));
      if (quiet) return;
    }
  }

  int client_fd_;
  abyss::fuzz::Tape tape_;
  NoopPoller poller_;
  abyss::net::NetMetrics metrics_;
  // Declared in the order they must outlive one another.
  abyss::fuzz::Inbox inbox_;
  abyss::fuzz::FakeEngine engine_;
  abyss::resp::Orphans orphans_;
  std::unique_ptr<abyss::net::Connection> conn_;
  bool closed_ = false;
  abyss::fuzz::CommandCount sent_;
  std::vector<uint8_t> received_;
  size_t parsed_ = 0;
  size_t replies_ = 0;
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  int sv[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) return 0;
  SetNonBlocking(sv[0]);
  SetNonBlocking(sv[1]);
  {
    const std::span<const uint8_t> input(data, size);
    Harness harness(input, sv[0], sv[1]);
    for (size_t off = 0; off < size;) {
      const size_t n = std::min(kChunk, size - off);
      if (!harness.Send(input.subspan(off, n))) break;
      off += n;
    }
    harness.Finish();
  }
  ::close(sv[1]);
  return 0;
}
