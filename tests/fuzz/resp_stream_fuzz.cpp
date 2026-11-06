// libFuzzer harness for the INCREMENTAL RESP path, through an async engine.
//
// resp_parser_fuzz hands the parser one complete buffer, so it never reaches the
// kIncomplete/resume transitions the real server lives in. Here the input is cut
// into content-derived chunks and pushed through the same accumulate/Feed/Drain/
// compact loop Connection runs, so a frame that straddles a chunk boundary is
// parsed exactly the way a frame straddling a socket read is. A fake engine
// (fake_async_engine.h) replies, sequences or pends each command, and between
// chunks the input resumes tasks, releases durable waits, passes deadlines and
// closes the client, as a reactor would see them: replies must leave in order,
// none twice, and none from a command after it was given up.
//
// POSIX/Clang only: the libFuzzer entrypoint depends on -fsanitize=fuzzer.

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/orphans.h"
#include "abyss/resp/parser.h"
#include "abyss/resp/request_pipeline.h"
#include "fake_async_engine.h"

namespace {

using abyss::fuzz::Check;

// Mirrors Connection's read cap: a frame that cannot complete inside it is a
// fail-closed teardown, never an unbounded buffer.
constexpr size_t kMaxReadBufferBytes = size_t{64} * 1024;
constexpr size_t kMaxChunkBytes = 37;
// Steps that settle what is left once the input is spent.
constexpr int kSettleRounds = 64;

// Chunk widths come from the input itself, so libFuzzer steers the split with
// ordinary byte mutations and the committed corpus stays plain RESP bytes.
size_t NextChunkSize(std::span<const uint8_t> input, size_t offset) {
  const size_t width = 1 + ((static_cast<size_t>(input[offset]) ^ (offset * 31)) % kMaxChunkBytes);
  return std::min(width, input.size() - offset);
}

class Harness {
 public:
  explicit Harness(std::span<const uint8_t> input)
      : tape_(input),
        engine_(tape_, inbox_),
        pipeline_(std::make_unique<abyss::resp::RequestPipeline>(
            abyss::resp::GlobalRegistry(), abyss::resp::ConnectionState{.client_id = 1},
            abyss::resp::PipelineDependencies{.dispatcher = &engine_},
            abyss::resp::PipelineHost{.sink = &inbox_},
            abyss::resp::PipelineLimits{.max_inflight = 1 + tape_.Below(8),
                                        .reply_bytes = 64 + (tape_.Below(16) * 64)})) {}

  Harness(const Harness&) = delete;
  Harness& operator=(const Harness&) = delete;
  Harness(Harness&&) = delete;
  Harness& operator=(Harness&&) = delete;

  ~Harness() = default;

  // Lands every owed post, then checks what is left.
  void Finish() {
    Settle();
    Check(orphans_.empty(), "orphans left once everything landed");
    Check(pipeline_ == nullptr || pipeline_->Quiescent(), "a pipeline owed a post at the end");
    // Open, it replied to everything it framed, QUIT and a protocol
    // error included.
    if (pipeline_ != nullptr) {
      Check(replies_ == commands_.count(), "a command never replied, or replied twice");
    }
  }

  bool open() const { return pipeline_ != nullptr; }

  // Takes a chunk; false once nothing more is read.
  bool Read(std::span<const uint8_t> chunk) {
    read_buf_.insert(read_buf_.end(), chunk.begin(), chunk.end());
    Step();
    if (!open() ||
        pipeline_->close_request() != abyss::resp::RequestPipeline::CloseRequest::kNone) {
      return false;
    }
    return read_buf_.size() < kMaxReadBufferBytes;
  }

  // Whatever survives the stream must still classify: an incomplete frame or
  // a protocol error, never a state the connection could wait on forever.
  void Classify() const {
    if (read_buf_.empty()) return;
    // NOLINTNEXTLINE(bugprone-unused-return-value)
    (void)abyss::resp::Parser::ParseCommand(read_buf_);
  }

 private:
  // What a reactor might see before the next read, in the input's order.
  void Step() {
    for (int steps = 1 + static_cast<int>(tape_.Below(4)); steps > 0 && open(); --steps) {
      switch (tape_.Below(6)) {
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
          if (tape_.Below(16) == 0) Close(tape_.Below(2) == 0);
          break;
        default:
          break;
      }
      Route();
      Feed();
    }
  }

  void Feed() {
    if (!open()) return;
    const auto fed = pipeline_->Feed(read_buf_, unwritten_);
    commands_.Add(std::span(read_buf_).first(fed.consumed));
    read_buf_.erase(read_buf_.begin(), read_buf_.begin() + static_cast<ptrdiff_t>(fed.consumed));
    pipeline_->Drain(write_buf_);
    replies_ += abyss::fuzz::ReadReplies(write_buf_, parsed_, engine_);
    // Written now, or left unwritten for the reply-byte cap to see.
    unwritten_ = !settling_ && tape_.Below(4) == 0 ? write_buf_.size() - parsed_ : 0;
  }

  void Expire() {
    if (open()) {
      if (auto orphan = pipeline_->Expire(engine_.now())) orphans_.Adopt(std::move(orphan));
    }
    orphans_.Expire(1, engine_.now());
  }

  void Route() {
    while (!inbox_.empty()) {
      for (const auto& event : inbox_.Take()) (void)orphans_.Route(event, pipeline_.get());
    }
    if (open()) Feed();
  }

  // The client goes: replies are dropped, and at shutdown its pending
  // command is given up.
  void Close(bool shutdown) {
    pipeline_->Close(shutdown);
    orphans_.Adopt(std::move(pipeline_));
  }

  // Every owed post lands, and every deadline passes.
  void Settle() {
    settling_ = true;
    unwritten_ = 0;
    for (int round = 0; round < kSettleRounds; ++round) {
      engine_.ResumeAll();
      engine_.ReleaseAll();
      Route();
      engine_.Advance(kSettle);
      Expire();
      Route();
      const bool quiet = (!open() || pipeline_->Quiescent()) && orphans_.empty() &&
                         !engine_.Owes() && inbox_.empty();
      if (quiet) return;
    }
  }

  static constexpr std::chrono::milliseconds kSettle{1000};

  abyss::fuzz::Tape tape_;
  // Declared in the order they must outlive one another.
  abyss::fuzz::Inbox inbox_;
  abyss::fuzz::FakeEngine engine_;
  abyss::resp::Orphans orphans_;
  std::unique_ptr<abyss::resp::RequestPipeline> pipeline_;
  std::vector<uint8_t> read_buf_;
  std::vector<uint8_t> write_buf_;
  size_t parsed_ = 0;
  size_t unwritten_ = 0;
  bool settling_ = false;
  abyss::fuzz::CommandCount commands_;
  size_t replies_ = 0;
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size == 0) return 0;
  const std::span<const uint8_t> input(data, size);
  Harness harness(input);
  for (size_t offset = 0; offset < input.size();) {
    const size_t chunk = NextChunkSize(input, offset);
    const bool more = harness.Read(input.subspan(offset, chunk));
    offset += chunk;
    if (!more) break;
  }
  harness.Classify();
  harness.Finish();
  return 0;
}
