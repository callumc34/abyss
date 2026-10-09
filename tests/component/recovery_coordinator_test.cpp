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
#include "abyss/consumer/hot_consumer_pool.h"
#include "abyss/consumer/resolver_pool.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/consumer_rpc.h"
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
#include "abyss/log/testing.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/wal_queue.h"
#include "temp_dir.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

constexpr std::size_t kSegment = std::size_t{64} << 10;
constexpr std::size_t kFrameSpace = kSegment - 4096;
constexpr std::string_view kMissed = "replay after the recovery scan found entries it missed";

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

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op,
                                     std::optional<core::Duration> /*deadline*/) override {
    const std::scoped_lock lock(mu_);
    if (const auto* exists = std::get_if<core::ops::Exists>(&op)) {
      int64_t found = 0;
      for (const auto key : exists->keys) found += state_.contains(std::string(key)) ? 1 : 0;
      return core::RespValue::Integer(found);
    }
    if (const auto* get = std::get_if<core::ops::StringGet>(&op)) {
      const auto it = state_.find(std::string(get->key));
      if (it == state_.end()) return core::RespValue::Null();
      return core::RespValue::BulkString(it->second.text);
    }
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "unsupported read (test)"});
  }

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
  core::Result<std::optional<core::RespCommand>> GetPromotionCommand(
      std::string_view /*key*/) override {
    return std::nullopt;
  }
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
  core::Result<std::optional<core::MemberValue>> LoadMember(
      std::string_view /*key*/, core::KeyType /*type*/, std::string_view /*member*/,
      core::SteadyTime /*deadline*/) override {
    return std::nullopt;
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
      .retention_consumers = {core::kColdConsumer, core::kResolverConsumer},
      .offset_fsync_interval = std::chrono::hours{1},
  };
}

// Everything one recovery runs over a WAL directory, in teardown order.
struct Node {
  Node(const std::filesystem::path& dir, uint32_t shard_count,
       const consumer::ColdConsumer::Config& cold_config = {})
      : shards(shard_count), notifier(core::AppliedSeqNotifierConfig{.shard_count = shard_count}) {
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
        consumer::ColdConsumerPool::Config{.shard_count = shards, .consumer = cold_config}, policy,
        rpc);
    hot_pool = std::make_unique<consumer::HotConsumerPool>(
        *queue, *hot, rpc, notifier, consumer::HotConsumerPool::Config{.shard_count = shards},
        policy);
    resolver_pool = std::make_unique<consumer::ResolverPool>(
        *queue, cold, *cold_pool, rpc, notifier,
        consumer::ResolverPool::Config{.shard_count = shards});
  }

  // Runs the coordinator, cancelling it if it outlives `bound`, so a
  // hang fails the test instead of stalling the suite.
  core::Result<void> Recover(uint32_t parallelism, std::chrono::seconds bound = 30s) const {
    BoundedThreadShardScheduler scheduler(16);
    RecoveryCoordinator coordinator(*queue, *resolver_pool, *cold_pool, *hot_pool, scheduler,
                                    RecoveryConfig{.replay_parallelism = parallelism});
    std::atomic<bool> cancel{false};
    auto run = std::async(std::launch::async, [&] { return coordinator.Run(cancel); });
    if (run.wait_for(bound) != std::future_status::ready) {
      ADD_FAILURE() << "recovery did not return within " << bound.count() << " s";
      cancel.store(true);
    }
    return run.get();
  }

  // The per-shard path the scan replaced: every resolver, then each
  // shard's cold and hot ReplayUntil in shard order.
  void RecoverShardByShard() {
    const std::atomic<bool> cancel{false};
    {
      std::vector<std::jthread> resolvers;
      resolvers.reserve(shards);
      for (core::ShardId s = 0; s < shards; ++s) {
        resolvers.emplace_back([this, s, &cancel] {
          auto replayed = resolver_pool->ConsumerFor(s).ReplayForRecovery(cancel);
          EXPECT_TRUE(replayed.has_value()) << replayed.error().message();
        });
      }
    }
    for (core::ShardId s = 0; s < shards; ++s) {
      const core::SequenceId tail = queue->TailSeq(s).value();
      auto cold_replayed = cold_pool->ConsumerFor(s).ReplayUntil(tail, cancel);
      ASSERT_TRUE(cold_replayed.has_value()) << cold_replayed.error().message();
      auto hot_replayed = hot_pool->ConsumerFor(s).ReplayUntil(tail, cancel);
      ASSERT_TRUE(hot_replayed.has_value()) << hot_replayed.error().message();
    }
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
  core::ConsumerRpc rpc;
  core::ApplyNotifier notifier;
  std::unique_ptr<consumer::ColdConsumerPool> cold_pool;
  std::unique_ptr<consumer::HotConsumerPool> hot_pool;
  std::unique_ptr<consumer::ResolverPool> resolver_pool;
};

core::QueueEntry WriteEntry(const std::string& key, const std::string& value) {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = core::RespCommand{{"SET", key, value}}},
  };
}

core::QueueEntry ConditionalEntry(const std::string& key) {
  return core::QueueEntry{
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Conditional{.cmd = core::RespCommand{{"SETNX", key, "v"}},
                                          .flags = core::PredicateFlags::kNx},
  };
}

core::QueueEntry ResolvedEntry(core::SequenceId ref, const std::string& key, bool apply) {
  core::entry::Resolved resolved{.ref = ref,
                                 .decision = apply ? core::Decision::kApply : core::Decision::kSkip,
                                 .return_value = core::RespValue::Integer(apply ? 1 : 0)};
  if (apply) resolved.materialised_ops.push_back(core::RespCommand{{"SET", key, "v"}});
  return core::QueueEntry{.appended_at = core::WallClock::now(), .payload = std::move(resolved)};
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

// 64 shards on one log. A reclaimed prefix leaves FirstSeq above 0 and
// cold's commit just past it; then writes, conditionals with their
// Resolveds, a FLUSHDB on shard 0 and one Conditional left dangling for
// the resolver. One scan must leave hot and cold as the per-shard path
// does, read the retained log once, and leave ReplayUntil nothing.
TEST_F(RecoveryCoordinatorScanTest, OneScanRebuildsWhatShardByShardReplayDid) {
  constexpr uint32_t kShards = 64;
  const auto shard_of = [](std::size_t i) {
    return static_cast<core::ShardId>(((i * 37) + (i / kShards)) % kShards);
  };
  const auto wal_a = dir_.Sub("a");
  {
    auto opened = queue::WalQueue::Open(WalConfigFor(wal_a, kShards));
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    auto& queue = **opened;
    for (std::size_t i = 0; i < 3000; ++i) {
      const core::ShardId shard = shard_of(i);
      const std::string key = Key(shard, "k" + std::to_string(i % 40), kShards);
      keys_.insert(key);
      Append(queue, shard, WriteEntry(key, "old" + std::to_string(i)));
    }
    for (core::ShardId s = 0; s < kShards; ++s) {
      const core::SequenceId tail = queue.TailSeq(s).value();
      ASSERT_TRUE(queue.AwaitDurable(s, tail, core::Durability::kPowerLoss, 10s).value());
      ASSERT_TRUE(queue.CommitOffset(core::kColdConsumer, s, tail).has_value());
      ASSERT_TRUE(queue.CommitOffset(core::kResolverConsumer, s, tail).has_value());
    }
    // Retention honours an offset once both checkpoint slots hold it.
    ASSERT_TRUE(queue.FlushOffsets().has_value());
    ASSERT_TRUE(queue.FlushOffsets().has_value());

    for (std::size_t i = 0; i < 15000; ++i) {
      const core::ShardId shard = shard_of(i);
      const std::string key = Key(shard, "k" + std::to_string(i % 40), kShards);
      keys_.insert(key);
      Append(queue, shard, WriteEntry(key, std::string(i % 97, 'v') + std::to_string(i)));
      if (i % 25 == 0) {
        const std::string cond = Key(shard, "c" + std::to_string(i), kShards);
        keys_.insert(cond);
        const core::SequenceId ref = Append(queue, shard, ConditionalEntry(cond));
        Append(queue, shard, ResolvedEntry(ref, cond, i % 50 == 0));
      }
      if (i == 7500) Append(queue, 0, FlushEntry());
    }
    keys_.insert(Key(9, "dangling", kShards));
    Append(queue, 9, ConditionalEntry(Key(9, "dangling", kShards)));
  }
  const auto wal_b = dir_.Sub("b");
  std::filesystem::copy(wal_a, wal_b, std::filesystem::copy_options::recursive);

  Node scan(wal_a, kShards);
  ASSERT_TRUE(scan.queue != nullptr);
  uint32_t trimmed = 0;
  for (core::ShardId s = 0; s < kShards; ++s) trimmed += scan.queue->FirstSeq(s).value() > 0;
  ASSERT_GT(trimmed, kShards / 2) << "retention reclaimed too little to test against";
  const log::testing::CapturingSink logs;
  // Workers interleave shards, and each shard's Flush wipes only its
  // own hot shard, so the rebuilt state cannot depend on the order.
  auto recovered = scan.Recover(4);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().message();
  for (const auto& record : logs.Records()) EXPECT_NE(record.msg, kMissed);

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

  Node per_shard(wal_b, kShards);
  ASSERT_TRUE(per_shard.queue != nullptr);
  ASSERT_NO_FATAL_FAILURE(per_shard.RecoverShardByShard());

  for (core::ShardId s = 0; s < kShards; ++s) {
    EXPECT_EQ(scan.queue->TailSeq(s).value(), per_shard.queue->TailSeq(s).value()) << s;
    EXPECT_EQ(scan.queue->CommittedOffset(core::kColdConsumer, s).value(),
              per_shard.queue->CommittedOffset(core::kColdConsumer, s).value())
        << "shard " << s;
  }
  const auto cold_state = scan.cold.State();
  EXPECT_EQ(cold_state, per_shard.cold.State());
  EXPECT_GT(cold_state.size(), keys_.size() / 2);
  std::size_t in_hot = 0;
  for (const auto& key : keys_) {
    const auto got = scan.HotGet(key);
    EXPECT_EQ(got, per_shard.HotGet(key)) << key;
    in_hot += got.has_value() ? 1 : 0;
  }
  EXPECT_GT(in_hot, keys_.size() / 2);
  EXPECT_EQ(scan.HotGet(Key(9, "dangling", kShards)), "v");
  EXPECT_TRUE(cold_state.contains(Key(9, "dangling", kShards)));
}

// The resolver re-emits a Resolved for a dangling Conditional. The
// scan, whose ends are captured after it, replays it to hot and cold,
// and the trailing ReplayUntil finds nothing.
TEST_F(RecoveryCoordinatorScanTest, AResolvedTheResolverReEmitsIsReplayedByTheScan) {
  constexpr uint32_t kShards = 4;
  const auto wal = dir_.Sub("wal");
  core::SequenceId before = 0;
  {
    auto opened = queue::WalQueue::Open(WalConfigFor(wal, kShards));
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    Append(**opened, 2, WriteEntry(Key(2, "k", kShards), "v"));
    before = Append(**opened, 2, ConditionalEntry(Key(2, "c", kShards)));
  }

  Node node(wal, kShards);
  ASSERT_TRUE(node.queue != nullptr);
  const log::testing::CapturingSink logs;
  auto recovered = node.Recover(2);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().message();

  EXPECT_EQ(node.queue->TailSeq(2).value(), before + 1) << "no Resolved was re-emitted";
  EXPECT_EQ(node.HotGet(Key(2, "c", kShards)), "v");
  EXPECT_TRUE(node.cold.State().contains(Key(2, "c", kShards)));
  for (const auto& record : logs.Records()) EXPECT_NE(record.msg, kMissed);
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
