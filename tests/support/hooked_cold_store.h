#pragma once

#include <functional>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/cold_store.h"

namespace abyss::testing {

// Forwards to a cold store, running a hook before each load: a test
// interleaves evictions, flushes or writes with a load in flight.
// SetHook only while no load runs: the hook is not synchronised.
class HookedColdStore : public core::ColdStore {
 public:
  using Hook = std::function<void(std::string_view key)>;

  // `cold` names the store to forward to at each call, so it may be
  // reopened beneath the wrapper.
  explicit HookedColdStore(std::function<core::ColdStore&()> cold) : cold_(std::move(cold)) {}

  void SetHook(Hook hook) { hook_ = std::move(hook); }

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
    Before(key);
    return cold_().LoadKey(key, deadline);
  }
  core::Result<std::optional<core::LoadedAs>> LoadKeyAs(std::string_view key, core::KeyType type,
                                                        core::SteadyTime deadline) override {
    Before(key);
    return cold_().LoadKeyAs(key, type, deadline);
  }
  core::Result<std::optional<core::KeyMeta>> ProbeKey(std::string_view key,
                                                      core::SteadyTime deadline) override {
    Before(key);
    return cold_().ProbeKey(key, deadline);
  }
  core::Result<std::vector<std::optional<core::MemberValue>>> LoadMembers(
      std::string_view key, core::KeyType type, std::span<const std::string_view> members,
      core::SteadyTime deadline) override {
    Before(key);
    return cold_().LoadMembers(key, type, members, deadline);
  }

 private:
  // Not re-entered on a thread: what a hook does loads nothing
  // through it.
  void Before(std::string_view key) {
    thread_local bool in_hook = false;
    if (!hook_ || in_hook) return;
    struct Guard {
      bool& flag;
      explicit Guard(bool& f) : flag(f) { flag = true; }
      Guard(const Guard&) = delete;
      Guard& operator=(const Guard&) = delete;
      Guard(Guard&&) = delete;
      Guard& operator=(Guard&&) = delete;
      ~Guard() { flag = false; }
    } guard(in_hook);
    hook_(key);
  }

  std::function<core::ColdStore&()> cold_;
  Hook hook_;
};

}  // namespace abyss::testing
