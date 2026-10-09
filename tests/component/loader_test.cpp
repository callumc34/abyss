#include "abyss/engine/loader.h"

#include <gtest/gtest.h>

#include "abyss/core/string_hash.h"

#ifdef ABYSS_HAVE_ROCKSDB

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/compacted_state.h"
#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/engine/decide.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"
#include "latch.h"
#include "on_exit.h"
#include "temp_dir.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
namespace ops = core::ops;
using Members = core::StringSet;
using Fields = core::StringMap<std::string>;
using Scores = core::StringMap<double>;

constexpr core::EvictionTTL kEviction{3600};

// Forwards to RocksDB, counting the loads; LoadKey may be held.
class CountingColdStore : public core::ColdStore {
 public:
  explicit CountingColdStore(core::ColdStore& inner) : inner_(inner) {}

  core::Result<core::RespValue> Exec(const ops::ReadOp& op,
                                     std::optional<core::Duration> deadline) override {
    return inner_.Exec(op, deadline);
  }
  core::Result<void> ApplyBatch(std::span<const ops::WriteOp> ops,
                                core::SequenceId highest_wal_seq) override {
    return inner_.ApplyBatch(ops, highest_wal_seq);
  }
  core::Result<void> Checkpoint(core::ShardId shard, core::SequenceId up_to) override {
    return inner_.Checkpoint(shard, up_to);
  }
  core::Result<void> Wipe(core::ShardId shard) override { return inner_.Wipe(shard); }
  core::Result<core::StorageStats> Stats() override { return inner_.Stats(); }
  core::Result<void> Compact() override { return inner_.Compact(); }
  core::Result<std::optional<core::ColdKeyState>> LoadKey(std::string_view key,
                                                          core::SteadyTime deadline) override {
    ++loads;
    if (hold_loads_) {
      entered.Open();
      release.Wait();
    }
    return inner_.LoadKey(key, deadline);
  }
  core::Result<std::optional<core::KeyMeta>> ProbeKey(std::string_view key,
                                                      core::SteadyTime deadline) override {
    ++probes;
    return inner_.ProbeKey(key, deadline);
  }
  core::Result<std::optional<core::LoadedAs>> LoadKeyAs(std::string_view key, core::KeyType type,
                                                        core::SteadyTime deadline) override {
    ++loads;
    if (hold_loads_) {
      entered.Open();
      release.Wait();
    }
    return inner_.LoadKeyAs(key, type, deadline);
  }
  core::Result<std::vector<std::optional<core::MemberValue>>> LoadMembers(
      std::string_view key, core::KeyType type, std::span<const std::string_view> members,
      core::SteadyTime deadline) override {
    return inner_.LoadMembers(key, type, members, deadline);
  }

  void HoldLoads() { hold_loads_ = true; }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::atomic<int> loads{0};
  std::atomic<int> probes{0};
  abyss::testing::Latch entered;
  abyss::testing::Latch release;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

 private:
  core::ColdStore& inner_;
  std::atomic<bool> hold_loads_{false};
};

class OneBufferRouter : public consumer::CompactionBufferRouter {
 public:
  explicit OneBufferRouter(consumer::CompactionBuffer& buffer) : buffer_(buffer) {}
  core::Result<core::RespValue> Exec(const ops::ReadOp& op,
                                     std::optional<core::Duration> /*deadline*/) override {
    return buffer_.Exec(op);
  }
  std::optional<consumer::CompactedState> Snapshot(core::ShardId /*shard*/,
                                                   std::string_view key) const override {
    return buffer_.Snapshot(key);
  }
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*target_seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    return true;
  }

 private:
  consumer::CompactionBuffer& buffer_;
};

std::unique_ptr<cold::backends::RocksdbStore> OpenCold(const abyss::testing::TempDir& dir) {
  auto created = cold::backends::RocksdbStore::Create({.data_path = dir.String()});
  if (!created.has_value()) {
    ADD_FAILURE() << created.error().message();
    return nullptr;
  }
  return std::move(*created);
}

const hot::LoadedFull& Full(const core::Result<hot::LoadResult>& loaded) {
  static const hot::LoadedFull kNone;
  EXPECT_TRUE(loaded.has_value()) << loaded.error().message();
  const auto* full = loaded.has_value() ? std::get_if<hot::LoadedFull>(&*loaded) : nullptr;
  EXPECT_NE(full, nullptr) << "not a full load";
  return full != nullptr ? *full : kNone;
}

class LoaderComponentTest : public ::testing::Test {
 protected:
  // Applies `batch` to cold, as a flush does.
  void Flush(std::vector<ops::WriteOp> batch) {
    ASSERT_TRUE(rocks_->ApplyBatch(batch, ++seq_).has_value());
  }
  // Leaves `op` in the buffer, unflushed.
  void Buffer(std::string_view key, const ops::WriteOp& op) {
    const core::SequenceId seq = ++seq_;
    buffer_.Absorb(std::string(key), op, kEviction, seq, seq, 0);
  }
  core::Result<hot::LoadResult> Load(std::string_view key, Need need = Need::kState) {
    return loader_.Load(0, key, need, Deadline());
  }
  static core::SteadyTime Deadline() { return core::SteadyClock::now() + 10s; }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  core::SequenceId seq_ = 0;
  abyss::testing::TempDir dir_{"loader"};
  std::unique_ptr<cold::backends::RocksdbStore> rocks_ = OpenCold(dir_);
  CountingColdStore cold_{*rocks_};
  consumer::CompactionBuffer buffer_;
  OneBufferRouter router_{buffer_};
  hot::ShardedHotStore hot_{hot::ShardedHotStoreConfig{.shard_count = 1}};
  Loader loader_{hot_, router_, cold_};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(LoaderComponentTest, AnUnflushedDeltaMergesOverAFlushedBase) {
  Flush({ops::SetAdd{.key = "set", .members = {"a", "b", "c"}},
         ops::ZsetAdd{.key = "zset",
                      .entries = {{.score = 1, .member = "m"}, {.score = 2, .member = "n"}}},
         ops::HashSet{.key = "hash",
                      .fields = {{.field = "f", .value = "1"}, {.field = "g", .value = "2"}}},
         ops::Expire{.key = "hash", .abs_ttl_ms = 4'000'000'000'000},
         ops::StringSet{.key = "str", .value = "old", .abs_ttl_ms = 4'000'000'000'000}});
  Buffer("set", ops::SetRem{.key = "set", .members = {"b"}});
  Buffer("set", ops::SetAdd{.key = "set", .members = {"d"}});
  Buffer("zset",
         ops::ZsetAdd{.key = "zset",
                      .entries = {{.score = 5, .member = "m"}, {.score = 3, .member = "o"}}});
  Buffer("zset", ops::ZsetRem{.key = "zset", .members = {"n"}});
  Buffer("hash", ops::HashSet{.key = "hash", .fields = {{.field = "f", .value = "9"}}});
  Buffer("hash", ops::HashDel{.key = "hash", .fields = {"g"}});
  Buffer("str", ops::StringSet{.key = "str", .value = "new"});

  const auto set = Load("set");
  EXPECT_EQ(std::get<hot::SetValue>(Full(set).value).members, (Members{"a", "c", "d"}));
  const auto zset = Load("zset");
  EXPECT_EQ(std::get<hot::ZsetValue>(Full(zset).value).member_scores, (Scores{{"m", 5}, {"o", 3}}));
  const auto hash = Load("hash");
  EXPECT_EQ(std::get<hot::HashValue>(Full(hash).value).fields, (Fields{{"f", "9"}}));
  EXPECT_EQ(Full(hash).abs_ttl_ms, 4'000'000'000'000) << "an unchanged TTL is cold's";
  const auto str = Load("str");
  EXPECT_EQ(std::get<std::string>(Full(str).value), "new");
  EXPECT_EQ(Full(str).abs_ttl_ms, 0);

  // The same once the delta is flushed too: the merge is idempotent.
  Flush({ops::SetRem{.key = "set", .members = {"b"}}, ops::SetAdd{.key = "set", .members = {"d"}}});
  const auto again = Load("set");
  EXPECT_EQ(std::get<hot::SetValue>(Full(again).value).members, (Members{"a", "c", "d"}));
}

TEST_F(LoaderComponentTest, PointReadsMergeTheDeltaOverCold) {
  Flush({ops::SetAdd{.key = "set", .members = {"a", "b"}},
         ops::ZsetAdd{.key = "zset",
                      .entries = {{.score = 1, .member = "m"}, {.score = 2, .member = "n"}}}});
  Buffer("set", ops::SetRem{.key = "set", .members = {"b"}});
  Buffer("set", ops::SetAdd{.key = "set", .members = {"c"}});
  Buffer("zset", ops::ZsetRem{.key = "zset", .members = {"n"}});
  Buffer("zset", ops::ZsetAdd{.key = "zset", .entries = {{.score = 3, .member = "o"}}});

  const auto is_member = [&](std::string_view member) {
    auto r = loader_.IsMember(0, "set", member, Deadline());
    EXPECT_TRUE(r.has_value());
    return r.value_or(false);
  };
  EXPECT_TRUE(is_member("a"));
  EXPECT_FALSE(is_member("b"));
  EXPECT_TRUE(is_member("c"));
  const auto score = loader_.Score(0, "zset", "m", Deadline());
  ASSERT_TRUE(score.has_value());
  EXPECT_EQ(score->value_or(0), 1);
  const auto set = Load("set");
  const auto zset = Load("zset");
  EXPECT_EQ(std::get<hot::SetValue>(Full(set).value).members.size(), 2U);
  EXPECT_EQ(std::get<hot::ZsetValue>(Full(zset).value).member_scores.size(), 2U);
}

TEST_F(LoaderComponentTest, TheDeadlineIsHonoured) {
  Flush({ops::SetAdd{.key = "set", .members = {"a"}}});
  const auto past = core::SteadyClock::now() - 1ms;
  for (const Need need : {Need::kState, Need::kExistence}) {
    auto loaded = loader_.Load(0, "set", need, past);
    ASSERT_FALSE(loaded.has_value());
    EXPECT_EQ(loaded.error().code(), core::ErrorCode::kTimeout);
  }
  auto member = loader_.IsMember(0, "set", "a", past);
  ASSERT_FALSE(member.has_value());
  EXPECT_EQ(member.error().code(), core::ErrorCode::kTimeout);
}

TEST_F(LoaderComponentTest, AnInstallRacedByABlindWriteDiscards) {
  Flush({ops::StringSet{.key = "k", .value = "cold"}});
  cold_.HoldLoads();
  auto install = std::async(
      std::launch::async, [&] { return loader_.Install("k", core::KeyType::kString, Deadline()); });
  const abyss::testing::OnExit release([&] { cold_.release.Open(); });
  ASSERT_TRUE(cold_.entered.Wait());

  // A blind write replaces the placeholder.
  ASSERT_TRUE(
      hot_.Apply(ops::WriteOp{ops::StringSet{.key = "k", .value = "blind"}}, 7).has_value());
  cold_.release.Open();
  ASSERT_EQ(install.wait_for(10s), std::future_status::ready);
  const auto filled = install.get();

  ASSERT_TRUE(filled.has_value()) << filled.error().message();
  EXPECT_EQ(filled->fill, Loader::Fill::kDiscarded);
  ASSERT_TRUE(filled->result.has_value());
  const auto* full =
      filled->result.has_value() ? std::get_if<hot::LoadedFull>(&*filled->result) : nullptr;
  ASSERT_NE(full, nullptr);
  EXPECT_EQ(std::get<std::string>(full->value), "cold") << "what was read, for the reply";
  auto get = hot_.Exec(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_TRUE(get.has_value());
  EXPECT_EQ(get->AsString(), "blind");
  EXPECT_EQ(hot_.Stats()->load_discards, 1U);
}

TEST_F(LoaderComponentTest, DelOfAColdOnlyKeyCountsItWithoutAFullLoad) {
  std::vector<std::string> members;
  members.reserve(1000);
  for (int i = 0; i < 1000; ++i) members.push_back("m" + std::to_string(i));
  Flush({ops::SetAdd{.key = "k", .members = {members.begin(), members.end()}}});

  // The sequencer's flow on one shard: decide, load off the lock,
  // complete and decide again in one hold, apply.
  constexpr uint64_t kNow = 1'000'000;
  hot::SingleShardStore shard{hot::SingleShardConfig{.stub_max_entries = 16}};
  const core::EvictionPolicy policy;
  core::RespCommand del{.args = {"DEL", "k"}};
  const auto decide = [&] {
    return Decide(del, core::PredicateFlags::kNone, kNow,
                  [&](std::string_view key) { return shard.View(key, hot::kAllDrained, kNow); });
  };
  const Decision first = decide();
  ASSERT_EQ(first.needs_load, (std::vector<KeyLoad>{{"k", Need::kExistence}}));
  const auto token = shard.BeginLoad("k");
  ASSERT_TRUE(token.has_value());
  auto loaded = loader_.Load(0, "k", Need::kExistence, Deadline());
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  std::vector<hot::LoadCompletion> completions;
  completions.push_back(
      {.key = "k", .token = token.value_or(hot::LoadToken{}), .result = *std::move(loaded)});
  ASSERT_EQ(shard.CompleteLoads(completions, policy), 1U);
  Decision second = decide();
  ASSERT_TRUE(second.needs_load.empty());
  ASSERT_TRUE(second.reply.has_value());
  EXPECT_EQ(second.reply.value_or(core::RespValue::Integer(0)).AsInteger(), 1);
  shard.ApplyEffects(second.effects, 9, core::WallTime{std::chrono::milliseconds{kNow}}, policy,
                     hot::kAllDrained);

  const hot::KeyView after = shard.View("k", hot::kAllDrained, kNow);
  EXPECT_EQ(after.presence, hot::KeyView::Presence::kTombstoned);
  EXPECT_EQ(after.latest_seq, 9U);
  EXPECT_EQ(cold_.loads.load(), 0) << "no full load";
  EXPECT_EQ(cold_.probes.load(), 1);
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
