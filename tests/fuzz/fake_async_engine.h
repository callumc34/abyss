#pragma once

// A fake async engine for the stream fuzzers: the input decides, per
// command, whether it replies, is sequenced or pends, and when pending
// tasks resume, durable waits release, deadlines pass and the client
// goes. Each engine reply names its command's dispatch number, which
// is its order among the commands the engine saw, so the replies the
// client reads are checked as they arrive: in command order, none
// twice, none from a command after it was given up. A broken
// invariant traps, for libFuzzer to keep the input.

#include <gmock/gmock.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/completion.h"
#include "abyss/core/durable_wait.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/resp/orphans.h"
#include "abyss/resp/parser.h"
#include "abyss/resp/request_pipeline.h"
#include "mock_queue.h"

namespace abyss::fuzz {

[[noreturn]] inline void Broken(const char* what) {
  std::fputs("fuzz invariant broken: ", stderr);
  std::fputs(what, stderr);
  std::fputc('\n', stderr);
  std::abort();
}

inline void Check(bool holds, const char* what) {
  if (!holds) Broken(what);
}

// Choices drawn from the input itself, so libFuzzer steers them with
// the same mutations it applies to the stream.
class Tape {
 public:
  explicit Tape(std::span<const uint8_t> input) : input_(input) {}
  uint8_t Next() {
    if (input_.empty()) return 0;
    const auto byte = static_cast<uint8_t>(input_[at_ % input_.size()] ^ (at_ * 131U));
    ++at_;
    return byte;
  }
  size_t Below(size_t n) { return n == 0 ? 0 : Next() % n; }

 private:
  std::span<const uint8_t> input_;
  size_t at_ = 0;
};

// Collects posts, for the harness to route on its own thread.
class Inbox final : public core::CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Post(std::span<const core::CompletionEvent> events) noexcept override {
    events_.insert(events_.end(), events.begin(), events.end());
  }
  std::vector<core::CompletionEvent> Take() { return std::exchange(events_, {}); }
  bool empty() const { return events_.empty(); }

 private:
  std::vector<core::CompletionEvent> events_;
};

inline constexpr core::ShardId kShards = 4;
inline constexpr auto kTimeout = std::chrono::milliseconds{100};

class FakeEngine final : public core::CommandDispatcher {
 public:
  FakeEngine(Tape& tape, Inbox& inbox) : tape_(tape), inbox_(inbox) {
    ON_CALL(queue_, WhenDurable(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault([this](core::ShardId shard, core::Durability, core::SeqWaiterNode& node) {
          return queue_.ParkDurable(shard, node);
        });
  }

  core::Outcome Dispatch(core::Request /*request*/, core::Slot& slot,
                         std::unique_ptr<core::Task>& task) override {
    Check(task == nullptr || task->Quiescent(), "a command dispatched while one was pending");
    slot.deadline = now_ + kTimeout;
    const uint64_t tag = next_tag_++;
    if (tape_.Below(3) != 0) return Decide(slot, tag);
    if (task == nullptr) task = std::make_unique<Task>(*this);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): made here.
    static_cast<Task&>(*task).Pend(slot, tag);
    return {.kind = core::Outcome::Kind::kPending};
  }

  // One owed resume, the input's pick, posted.
  void ResumeOne() {
    if (owed_.empty()) return;
    const size_t at = tape_.Below(owed_.size());
    const core::Completion to = owed_[at];
    owed_.erase(owed_.begin() + static_cast<std::ptrdiff_t>(at));
    const core::CompletionEvent event{.client_id = to.client_id,
                                      .slot_seq = to.slot_seq,
                                      .kind = core::CompletionKind::kResume,
                                      .result = {}};
    inbox_.Post(std::span(&event, 1));
  }
  void ReleaseOne() { queue_.ReleaseParked(static_cast<core::ShardId>(tape_.Below(kShards))); }
  void ReleaseAll() {
    for (core::ShardId shard = 0; shard < kShards; ++shard) queue_.ReleaseParked(shard);
  }
  void ResumeAll() {
    while (!owed_.empty()) ResumeOne();
  }
  bool Owes() const { return !owed_.empty(); }

  core::SteadyTime now() const { return now_; }
  void Advance(std::chrono::milliseconds by) { now_ += by; }

  // A reply the client read: an engine reply names its command.
  void Read(const core::RespValue& reply) {
    std::string_view text;
    if (reply.IsBulkString()) {
      text = reply.AsString();
    } else if (reply.IsError()) {
      text = reply.AsString();
      if (text.starts_with("ERR ")) text.remove_prefix(4);
    }
    Check(!text.starts_with("late "), "a reply from a command after it was given up");
    std::optional<uint64_t> tag;
    if (text.starts_with("r ")) tag = Number(text.substr(2));
    if (text.starts_with("abandoned ")) tag = Number(text.substr(10));
    if (!tag.has_value()) return;
    Check(!last_read_.has_value() || *tag > *last_read_, "replies out of order, or one twice");
    last_read_ = tag;
  }

 private:
  class Task final : public core::Task {
   public:
    explicit Task(FakeEngine& engine) : engine_(engine) {}
    ~Task() override { Check(Quiescent(), "a task freed while busy"); }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&&) = delete;
    Task& operator=(Task&&) = delete;

    void Pend(const core::Slot& slot, uint64_t tag) {
      Check(!busy_, "a busy task given a command");
      tag_ = tag;
      busy_ = true;
      abandoned_ = false;
      engine_.owed_.push_back(slot.completion);
    }
    core::Outcome Run(core::Slot& slot, core::Result<void> /*wake*/) override {
      Check(busy_, "an idle task resumed");
      busy_ = false;
      // Given up: it only cleans up, and what it would answer is no
      // one's; it shows if a pipeline lets it through.
      if (abandoned_) {
        return {.reply = core::RespValue::BulkString("late " + std::to_string(tag_))};
      }
      if (engine_.tape_.Below(4) == 0) {
        Pend(slot, tag_);
        return {.kind = core::Outcome::Kind::kPending};
      }
      return engine_.Decide(slot, tag_);
    }
    core::Result<core::RespValue> Abandon(core::Slot& slot, core::AbandonReason reason) override {
      Check(busy_ && !abandoned_, "a task abandoned while not pending");
      // Taken back while queued, or already running and owed its post.
      if (engine_.tape_.Below(2) == 0) {
        engine_.Forget(slot.completion);
        busy_ = false;
      } else {
        abandoned_ = true;
      }
      const std::string why = reason == core::AbandonReason::kShutdown ? " shutdown" : "";
      return std::unexpected(
          core::Error{core::ErrorCode::kTimeout, "abandoned " + std::to_string(tag_) + why});
    }
    bool Quiescent() const override { return !busy_; }

   private:
    FakeEngine& engine_;
    uint64_t tag_ = 0;
    bool busy_ = false;
    bool abandoned_ = false;
  };

  // Replied now, or sequenced with its reply held until durable.
  core::Outcome Decide(core::Slot& slot, uint64_t tag) {
    core::RespValue reply = core::RespValue::BulkString("r " + std::to_string(tag));
    if (tape_.Below(2) == 0) return {.reply = std::move(reply)};
    slot.durable.Begin(queue_, core::DurableWait::Kind::kWrite, slot.completion, 1,
                       std::move(reply));
    slot.durable.Add(static_cast<core::ShardId>(tape_.Below(kShards)), 1);
    if (auto now = slot.durable.Ready()) return {.reply = *std::move(now)};
    return {.kind = core::Outcome::Kind::kSequenced};
  }

  void Forget(const core::Completion& completion) {
    std::erase_if(owed_, [&completion](const core::Completion& owed) {
      return owed.client_id == completion.client_id && owed.slot_seq == completion.slot_seq;
    });
  }

  static std::optional<uint64_t> Number(std::string_view text) {
    uint64_t value = 0;
    size_t digits = 0;
    for (const char c : text) {
      if (c < '0' || c > '9') break;
      value = (value * 10) + static_cast<uint64_t>(c - '0');
      ++digits;
    }
    if (digits == 0) return std::nullopt;
    return value;
  }

  Tape& tape_;
  Inbox& inbox_;
  ::testing::NiceMock<testing::MockQueue> queue_;
  core::SteadyTime now_{};
  uint64_t next_tag_ = 0;
  std::vector<core::Completion> owed_;
  std::optional<uint64_t> last_read_;
};

// Parses what the client has read from `parsed` on, checking each
// reply. Returns the replies read.
inline size_t ReadReplies(std::span<const uint8_t> bytes, size_t& parsed, FakeEngine& engine) {
  size_t replies = 0;
  while (parsed < bytes.size()) {
    auto reply = resp::Parser::Parse(bytes.subspan(parsed));
    if (!reply.has_value()) {
      Check(reply.error().code() == core::ErrorCode::kIncomplete, "an unparsable reply");
      break;
    }
    parsed += reply->bytes_consumed;
    engine.Read(reply->value);
    ++replies;
  }
  return replies;
}

// Counts the commands a stream holds as the pipeline frames them; an
// unframable tail is one more, its protocol error.
class CommandCount {
 public:
  void Add(std::span<const uint8_t> bytes) {
    pending_.insert(pending_.end(), bytes.begin(), bytes.end());
    size_t at = 0;
    while (!unframable_ && at < pending_.size()) {
      auto parsed = resp::Parser::ParseCommand(std::span(pending_).subspan(at));
      if (!parsed.has_value()) {
        if (parsed.error().code() != core::ErrorCode::kIncomplete) {
          unframable_ = true;
          ++count_;
        }
        break;
      }
      at += parsed->bytes_consumed;
      ++count_;
    }
    pending_.erase(pending_.begin(), pending_.begin() + static_cast<std::ptrdiff_t>(at));
  }
  size_t count() const { return count_; }

 private:
  std::vector<uint8_t> pending_;
  size_t count_ = 0;
  bool unframable_ = false;
};

}  // namespace abyss::fuzz
