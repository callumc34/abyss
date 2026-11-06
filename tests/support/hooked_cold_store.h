#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/cold_store.h"
#include "on_exit.h"

namespace abyss::testing {

// Forwards to a cold store, running a hook before each load, and one
// after it has read: a test interleaves evictions, flushes or writes
// with a load in flight. A hook may be set while loads run: setting
// one returns once no call of the hook it replaced is still running,
// so none runs after. A hook must therefore never wait on a thread
// that sets one, nor set one itself.
class HookedColdStore : public core::ColdStore {
 public:
  using Hook = std::function<void(std::string_view key)>;

  // `cold` names the store to forward to at each call, so it may be
  // reopened beneath the wrapper.
  explicit HookedColdStore(std::function<core::ColdStore&()> cold) : cold_(std::move(cold)) {}

  void SetHook(Hook hook) { Set(before_, std::move(hook)); }
  void SetAfterHook(Hook hook) { Set(after_, std::move(hook)); }

  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::SequenceId highest_wal_seq) override {
    return cold_().ApplyBatch(ops, highest_wal_seq);
  }
  core::Result<void> Checkpoint(core::ShardId shard, core::SequenceId up_to_wal_seq) override {
    return cold_().Checkpoint(shard, up_to_wal_seq);
  }
  core::Result<void> Wipe(core::ShardId shard) override { return cold_().Wipe(shard); }
  core::Result<core::StorageStats> Stats() override { return cold_().Stats(); }
  core::Result<void> Compact() override { return cold_().Compact(); }
  core::Result<void> Start() override { return cold_().Start(); }
  core::Result<void> Stop() override { return cold_().Stop(); }

  core::Result<std::optional<core::ColdKeyState>> LoadKey(std::string_view key,
                                                          core::SteadyTime deadline) override {
    Call(before_, key);
    auto loaded = cold_().LoadKey(key, deadline);
    Call(after_, key);
    return loaded;
  }
  core::Result<std::optional<core::LoadedAs>> LoadKeyAs(std::string_view key, core::KeyType type,
                                                        core::SteadyTime deadline) override {
    Call(before_, key);
    auto loaded = cold_().LoadKeyAs(key, type, deadline);
    Call(after_, key);
    return loaded;
  }
  core::Result<std::optional<core::KeyMeta>> ProbeKey(std::string_view key,
                                                      core::SteadyTime deadline) override {
    Call(before_, key);
    auto loaded = cold_().ProbeKey(key, deadline);
    Call(after_, key);
    return loaded;
  }
  core::Result<std::vector<std::optional<core::MemberValue>>> LoadMembers(
      std::string_view key, core::KeyType type, std::span<const std::string_view> members,
      core::SteadyTime deadline) override {
    Call(before_, key);
    auto loaded = cold_().LoadMembers(key, type, members, deadline);
    Call(after_, key);
    return loaded;
  }

 private:
  struct Slot {
    Hook hook;
    uint64_t generation = 0;
    // Calls running the current hook, and those running one replaced.
    uint64_t running = 0;
    uint64_t replaced = 0;
  };

  void Set(Slot& slot, Hook hook) {
    std::unique_lock lock(mu_);
    slot.hook.swap(hook);
    ++slot.generation;
    slot.replaced += std::exchange(slot.running, 0);
    returned_.wait(lock, [&slot] { return slot.replaced == 0; });
  }

  // Not re-entered on a thread: what a hook does loads nothing
  // through it. Run unlocked, so a hook may block.
  void Call(Slot& slot, std::string_view key) {
    thread_local bool in_hook = false;
    if (in_hook) return;
    Hook hook;
    uint64_t generation = 0;
    {
      const std::scoped_lock lock(mu_);
      if (!slot.hook) return;
      hook = slot.hook;
      generation = slot.generation;
      ++slot.running;
    }
    in_hook = true;
    const OnExit done([&] {
      in_hook = false;
      const std::scoped_lock lock(mu_);
      --(generation == slot.generation ? slot.running : slot.replaced);
      // Under mu_: a setter that sees no call left may destroy the
      // store as soon as it relocks.
      returned_.notify_all();
    });
    hook(key);
    hook = nullptr;
  }

  std::function<core::ColdStore&()> cold_;
  std::mutex mu_;
  std::condition_variable returned_;
  Slot before_;
  Slot after_;
};

}  // namespace abyss::testing
