#include "abyss/engine/recovery_coordinator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "abyss/engine/bounded_thread_shard_scheduler.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/wal_queue.h"
#include "temp_dir.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

constexpr std::size_t kSegment = std::size_t{64} << 10;
constexpr std::size_t kFrameSpace = kSegment - 4096;

std::string Prefix(core::ShardId shard) { return "s" + std::to_string(shard) + ":"; }

// Keys name their WAL shard, "s<shard>:...", and route to it as the
// engine routes keys, so a Flush on one shard wipes exactly that
// shard's keys from hot and from this cold store.
std::string Key(core::ShardId shard, const std::string& name, uint32_t shards) {
  for (uint32_t salt = 0; salt < (uint32_t{1} << 20); ++salt) {
    std::string key = Prefix(shard) + name + "#" + std::to_string(salt);
    if (core::ComputeShard(key, shards) == shard) return key;
  }
  ADD_FAILURE() << "no key routes to shard " << shard;
  return Prefix(shard) + name;
}

// A cold store in memory: per key, the last op applied and, for a
// string, its value.
class MemoryColdStore : public core::ColdStore {
 public:
  struct Value {
    std::string op;
    std::string text;
    bool operator==(const Value&) const = default;
  };

  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::SequenceId /*highest_wal_seq*/) override {
    const std::scoped_lock lock(mu_);
    for (const auto& op : ops) {
      if (const auto* del = std::get_if<core::ops::Del>(&op)) {
        for (const auto key : del->keys) state_.erase(std::string(key));
        continue;
      }
      Value value;
      for (const auto& arg : core::ops::CanonicalCommand(op).args) value.op += arg + " ";
      if (const auto* set = std::get_if<core::ops::StringSet>(&op)) value.text = set->value;
      state_[std::string(core::ops::PrimaryKey(op))] = std::move(value);
    }
    return {};
  }

  core::Result<void> Checkpoint(core::ShardId /*shard*/, core::SequenceId /*up_to*/) override {
    return {};
  }

  core::Result<void> Wipe(core::ShardId shard) override {
    if (fail_wipes_.load()) {
      return std::unexpected(core::Error{core::ErrorCode::kInternal, "cold store unwritable"});
    }
    const std::scoped_lock lock(mu_);
    const std::string prefix = Prefix(shard);
    std::erase_if(state_, [&](const auto& kv) { return kv.first.starts_with(prefix); });
    return {};
  }

  core::Result<core::StorageStats> Stats() override { return core::StorageStats{}; }
  core::Result<void> Compact() override { return {}; }
  // Every key held here loads as its last string.
  core::Result<std::optional<core::ColdKeyState>> LoadKey(std::string_view key,
                                                          core::SteadyTime /*deadline*/) override {
    const std::scoped_lock lock(mu_);
    const auto it = state_.find(std::string(key));
    if (it == state_.end()) return std::nullopt;
    return core::ColdKeyState{.type = core::KeyType::kString, .value = it->second.text};
  }
  core::Result<std::optional<core::KeyMeta>> ProbeKey(std::string_view key,
                                                      core::SteadyTime /*deadline*/) override {
    const std::scoped_lock lock(mu_);
    if (!state_.contains(std::string(key))) return std::nullopt;
    return core::KeyMeta{.type = core::KeyType::kString, .cardinality = 1};
  }
  core::Result<std::optional<core::LoadedAs>> LoadKeyAs(std::string_view key, core::KeyType type,
                                                        core::SteadyTime deadline) override {
    auto loaded = LoadKey(key, deadline);
    if (!loaded.has_value() || !loaded->has_value()) return std::nullopt;
    if (type != core::KeyType::kString) {
      return core::LoadedAs{core::KeyMeta{.type = core::KeyType::kString, .cardinality = 1}};
    }
    return core::LoadedAs{**std::move(loaded)};
  }
  core::Result<std::vector<std::optional<core::MemberValue>>> LoadMembers(
      std::string_view /*key*/, core::KeyType /*type*/, std::span<const std::string_view> members,
      core::SteadyTime /*deadline*/) override {
    return std::vector<std::optional<core::MemberValue>>(members.size());
  }

  std::map<std::string, Value> State() const {
    const std::scoped_lock lock(mu_);
    return state_;
  }
  void FailWipes() { fail_wipes_.store(true); }

 private:
  mutable std::mutex mu_;
  std::map<std::string, Value> state_;
  std::atomic<bool> fail_wipes_{false};
};

queue::WalConfig WalConfigFor(const std::filesystem::path& dir, uint32_t shards) {
  return queue::WalConfig{
      .wal_path = dir.string(),
      .segment_size_bytes = kSegment,
      .shard_count = shards,
      .ring_entries = 4096,
      .durability = core::Durability::kProcessCrash,
      .min_retention = 0s,
      .retention_consumers = {core::kColdConsumer},
      .offset_fsync_interval = std::chrono::hours{1},
  };
}

// Everything one recovery runs over a WAL directory, in teardown order.
struct Node {
  Node(const std::filesystem::path& dir, uint32_t shard_count,
       const consumer::ColdConsumer::Config& cold_config = {})
      : shards(shard_count) {
    auto opened = queue::WalQueue::Open(WalConfigFor(dir, shards));
    if (!opened.has_value()) {
      ADD_FAILURE() << opened.error().message();
      return;
    }
    queue = std::move(*opened);
    hot = std::make_unique<hot::ShardedHotStore>(
        hot::ShardedHotStoreConfig{.shard_count = shards, .eviction_policy = &policy});
    cold_pool = std::make_unique<consumer::ColdConsumerPool>(
        *queue, cold,
        consumer::ColdConsumerPool::Config{.shard_count = shards, .consumer = cold_config}, policy);
  }

  // Runs the coordinator, cancelling it if it outlives `bound`, so a
  // hang fails the test instead of stalling the suite.
  core::Result<void> Recover(uint32_t parallelism, std::chrono::seconds bound = 30s) const {
    BoundedThreadShardScheduler scheduler(16);
    RecoveryCoordinator coordinator(*queue, *cold_pool, *hot, scheduler,
                                    RecoveryConfig{.replay_parallelism = parallelism});
    std::atomic<bool> cancel{false};
    auto run = std::async(std::launch::async, [&] { return coordinator.Run(cancel); });
    if (run.wait_for(bound) != std::future_status::ready) {
      ADD_FAILURE() << "recovery did not return within " << bound.count() << " s";
      cancel.store(true);
    }
    return run.get();
  }

  std::optional<std::string> HotGet(const std::string& key) const {
    auto got = hot->Exec(core::ops::ReadOp{core::ops::StringGet{.key = key}});
    if (!got.has_value() || !got->IsBulkString()) return std::nullopt;
    return got->AsString();
  }

  uint32_t shards;
  std::unique_ptr<queue::WalQueue> queue;
  MemoryColdStore cold;
  core::EvictionPolicy policy{86400s};
  std::unique_ptr<hot::ShardedHotStore> hot;
  std::unique_ptr<consumer::ColdConsumerPool> cold_pool;
};

// A SET replaces its key's state, so the sequencer flags it.
core::QueueEntry WriteEntry(const std::string& key, const std::string& value) {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = core::RespCommand{{"SET", key, value}}},
      .replaces_state = true,
  };
}

core::QueueEntry FlushEntry() {
  return core::QueueEntry{.appended_at = core::WallClock::now(), .payload = core::entry::Flush{}};
}

class RecoveryCoordinatorScanTest : public ::testing::Test {
 protected:
  void SetUp() override { metrics::testing::Reset(); }
  void TearDown() override { metrics::testing::Reset(); }

  // The seq assigned; a failed append fails the test.
  core::SequenceId Append(queue::WalQueue& queue, core::ShardId shard, core::QueueEntry entry) {
    auto appended = queue.Append(shard, std::move(entry));
    EXPECT_TRUE(appended.has_value()) << appended.error().message();
    return appended.has_value() ? appended->seq : 0;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::TempDir dir_{"recovery_scan"};
  std::set<std::string> keys_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// 64 shards on one log. A reclaimed prefix leaves FirstSeq above the
// first seq and cold's commit just past it; then writes, and a FLUSHDB
// on shard 0. One scan must rebuild cold to the log's last word on
// every key, hot to it on every key written since FirstSeq, and read
// the retained log once.
TEST_F(RecoveryCoordinatorScanTest, OneScanRebuildsHotAndColdToTheLog) {
  constexpr uint32_t kShards = 64;
  const auto shard_of = [](std::size_t i) {
    return static_cast<core::ShardId>(((i * 37) + (i / kShards)) % kShards);
  };
  // Each key's last value and seq, or nullopt once a Flush wiped it.
  std::map<std::string, std::pair<std::optional<std::string>, core::SequenceId>> model;
  const auto wal = dir_.Sub("wal");
  {
    auto opened = queue::WalQueue::Open(WalConfigFor(wal, kShards));
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    auto& queue = **opened;
    const auto write = [&](std::size_t i, const std::string& value) {
      const core::ShardId shard = shard_of(i);
      const std::string key = Key(shard, "k" + std::to_string(i % 40), kShards);
      model[key] = {value, Append(queue, shard, WriteEntry(key, value))};
    };
    for (std::size_t i = 0; i < 3000; ++i) write(i, "old" + std::to_string(i));
    for (core::ShardId s = 0; s < kShards; ++s) {
      const core::SequenceId tail = queue.TailSeq(s).value();
      ASSERT_TRUE(queue.AwaitDurable(s, tail, core::Durability::kPowerLoss, 10s).value());
      ASSERT_TRUE(queue.CommitOffset(core::kColdConsumer, s, tail).has_value());
    }
    // Retention honours an offset once both checkpoint slots hold it.
    ASSERT_TRUE(queue.FlushOffsets().has_value());
    ASSERT_TRUE(queue.FlushOffsets().has_value());

    for (std::size_t i = 0; i < 15000; ++i) {
      write(i, std::string(i % 97, 'v') + std::to_string(i));
      if (i == 7500) {
        const core::SequenceId flush = Append(queue, 0, FlushEntry());
        for (auto& [key, last] : model) {
          if (core::ComputeShard(key, kShards) == 0) last = {std::nullopt, flush};
        }
      }
    }
  }

  // Cold committed the prefix without applying it; this cold starts
  // with what that commit stood for.
  Node scan(wal, kShards);
  ASSERT_TRUE(scan.queue != nullptr);
  std::vector<core::SequenceId> first(kShards);
  uint32_t trimmed = 0;
  for (core::ShardId s = 0; s < kShards; ++s) {
    first[s] = scan.queue->FirstSeq(s).value();
    trimmed += first[s] > core::kFirstSeq ? 1 : 0;
  }
  ASSERT_GT(trimmed, kShards / 2) << "retention reclaimed too little to test against";
  // Workers interleave shards, and each shard's Flush wipes only its
  // own hot shard, so the rebuilt state cannot depend on the order.
  auto recovered = scan.Recover(4);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().message();

  // The scan walked the retained log once: from the oldest retained
  // segment's start to the last frame, less at most the padding that
  // sealed the recovered tail.
  for (core::ShardId s = 0; s < kShards; ++s) {
    const core::SequenceId tail = scan.queue->TailSeq(s).value();
    ASSERT_TRUE(scan.queue->AwaitDurable(s, tail, core::Durability::kPowerLoss, 10s).value());
  }
  const auto extent = scan.queue->DurableExtentForTesting(0);
  const uint64_t durable_ordinal = std::stoull(std::filesystem::path(extent.path).stem().string());
  const auto sealed = scan.queue->ListSealedSegments();
  ASSERT_FALSE(sealed.empty());
  const uint64_t retained = ((durable_ordinal - sealed.front().ordinal) * kFrameSpace) +
                            (extent.offset - (kSegment - kFrameSpace));
  ASSERT_GT(retained, 10 * kFrameSpace);
  const double walked =
      metrics::testing::GetCounterValue(metrics::names::kWalScanBytesTotal).value_or(0);
  EXPECT_LE(walked, static_cast<double>(retained));
  EXPECT_GE(walked, static_cast<double>(retained - kFrameSpace));

  const auto cold_state = scan.cold.State();
  std::size_t in_hot = 0;
  for (const auto& [key, last] : model) {
    const auto& [value, seq] = last;
    const core::ShardId shard = core::ComputeShard(key, kShards);
    const auto got = scan.HotGet(key);
    in_hot += got.has_value() ? 1 : 0;
    if (seq >= first[shard]) {
      EXPECT_EQ(got, value) << key;
    } else {
      // Its last write was reclaimed: hot never held it, cold does.
      EXPECT_EQ(got, std::nullopt) << key;
    }
    if (!value.has_value()) {
      EXPECT_FALSE(cold_state.contains(key)) << key;
    } else if (seq >= first[shard]) {
      ASSERT_TRUE(cold_state.contains(key)) << key;
      EXPECT_EQ(cold_state.at(key).text, *value) << key;
    }
  }
  EXPECT_GT(in_hot, model.size() / 2);
}

// A wipe that never succeeds fails recovery once drain_grace is spent.
TEST_F(RecoveryCoordinatorScanTest, AColdWipeThatAlwaysFailsFailsRecoveryWithinItsBudget) {
  constexpr uint32_t kShards = 4;
  const auto wal = dir_.Sub("wal");
  {
    auto opened = queue::WalQueue::Open(WalConfigFor(wal, kShards));
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    for (core::ShardId s = 0; s < kShards; ++s)
      Append(**opened, s, WriteEntry(Key(s, "k", kShards), "v"));
    Append(**opened, 3, FlushEntry());
    Append(**opened, 3, WriteEntry(Key(3, "after", kShards), "v"));
  }

  consumer::ColdConsumer::Config cold_config;
  cold_config.drain_grace = 100ms;
  cold_config.loop_max_backoff = 20ms;
  Node node(wal, kShards, cold_config);
  ASSERT_TRUE(node.queue != nullptr);
  node.cold.FailWipes();

  auto recovered = node.Recover(2);
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), core::ErrorCode::kTimeout);
  EXPECT_NE(recovered.error().message().find("shard 3"), std::string::npos)
      << recovered.error().message();
}

}  // namespace
}  // namespace abyss::engine
