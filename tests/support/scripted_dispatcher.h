#pragma once

#include <gmock/gmock.h>

#include <chrono>
#include <map>
#include <memory>
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
#include "mock_queue.h"

namespace abyss::testing {

// A dispatcher whose outcome is its command's key. "s<n>" is sequenced
// on shard n until the queue's ReleaseParked(n); "p<...>" is pending
// until its kResume, then replies with its key, or "ps<n>" is sequenced
// on shard n; anything else replies with its key at once. A pending
// command's Abandon replies `timeout_text`; with `cancel_fails` its
// wake is already on its way, and its task stays busy until it lands.
// Counts, by key, the commands applied and the late wakes that only
// cleaned up.
class ScriptedDispatcher : public core::CommandDispatcher {
 public:
  explicit ScriptedDispatcher(::testing::NiceMock<MockQueue>& queue) : queue_(queue) {
    ON_CALL(queue_, WhenDurable(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault([this](core::ShardId shard, core::Durability, core::SeqWaiterNode& node) {
          return queue_.ParkDurable(shard, node);
        });
  }

  core::Outcome Dispatch(core::Request request, core::Slot& slot,
                         std::unique_ptr<core::Task>& task) override {
    slot.deadline = core::SteadyClock::now() + timeout;
    std::string key = request.cmd.args.size() > 1 ? request.cmd.args[1] : std::string();
    dispatched.push_back(key);
    if (key.starts_with("p")) {
      if (task == nullptr) task = std::make_unique<Task>(*this);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): made here.
      static_cast<Task&>(*task).Begin(std::move(key));
      last_pending = slot.completion;
      return {.kind = core::Outcome::Kind::kPending};
    }
    ++applied[key];
    if (key.starts_with("s")) return Sequence(slot, key);
    return {.reply = core::RespValue::BulkString(key)};
  }

  // Posts kResume for the command `client_id` dispatched as `slot_seq`.
  void Wake(uint64_t client_id, uint64_t slot_seq, core::Result<void> result = {}) const {
    const core::CompletionEvent event{.client_id = client_id,
                                      .slot_seq = slot_seq,
                                      .kind = core::CompletionKind::kResume,
                                      .result = std::move(result)};
    last_pending.sink->Post(std::span(&event, 1));
  }
  void WakeLast(core::Result<void> result = {}) const {
    Wake(last_pending.client_id, last_pending.slot_seq, std::move(result));
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::vector<std::string> dispatched;
  std::map<std::string, int> applied;
  std::map<std::string, int> cleanups;
  bool cancel_fails = false;
  std::string timeout_text = "write timed out waiting for admission";
  std::chrono::milliseconds timeout{10000};
  core::Completion last_pending;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

 private:
  class Task final : public core::Task {
   public:
    explicit Task(ScriptedDispatcher& owner) : owner_(owner) {}

    void Begin(std::string key) {
      key_ = std::move(key);
      busy_ = true;
    }
    core::Outcome Run(core::Slot& slot, core::Result<void> wake) override {
      busy_ = false;
      if (std::exchange(abandoned_, false)) {
        ++owner_.cleanups[key_];
        return {.reply = std::unexpected(core::Error{core::ErrorCode::kTimeout, "late"})};
      }
      if (!wake.has_value()) return {.reply = std::unexpected(wake.error())};
      ++owner_.applied[key_];
      if (key_.starts_with("ps")) return owner_.Sequence(slot, key_);
      return {.reply = core::RespValue::BulkString(key_)};
    }
    core::Result<core::RespValue> Abandon(core::Slot& /*slot*/) override {
      if (owner_.cancel_fails) {
        abandoned_ = true;
      } else {
        busy_ = false;
      }
      return std::unexpected(core::Error{core::ErrorCode::kTimeout, owner_.timeout_text});
    }
    bool Quiescent() const override { return !busy_; }

   private:
    ScriptedDispatcher& owner_;
    std::string key_;
    bool busy_ = false;
    bool abandoned_ = false;
  };

  core::Outcome Sequence(core::Slot& slot, std::string_view key) {
    const auto shard =
        static_cast<core::ShardId>(std::stoul(std::string(key.substr(key.find('s') + 1))));
    slot.durable.Begin(queue_, core::DurableWait::Kind::kWrite, slot.completion, 1,
                       core::RespValue::BulkString(std::string(key)));
    slot.durable.Add(shard, 1);
    if (auto now = slot.durable.Ready()) return {.reply = *std::move(now)};
    return {.kind = core::Outcome::Kind::kSequenced};
  }

  ::testing::NiceMock<MockQueue>& queue_;
};

}  // namespace abyss::testing
