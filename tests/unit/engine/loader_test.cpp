#include "abyss/engine/loader.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

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
#include "decide_fixture.h"
#include "latch.h"
#include "mock_cold_store.h"
#include "on_exit.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
using ::testing::_;
using ::testing::Return;
namespace ops = core::ops;
using core::KeyType;
using Presence = hot::KeyView::Presence;

using ProbeResult = core::Result<std::optional<core::KeyMeta>>;
using LoadKeyResult = core::Result<std::optional<core::ColdKeyState>>;
using MemberResult = core::Result<std::optional<core::MemberValue>>;

constexpr core::EvictionTTL kEviction{3600};
constexpr int64_t kNowMs = 1'000'000;

template <typename T>
T Ok(core::Result<T> result) {
  EXPECT_TRUE(result.has_value()) << result.error().message();
  return result.has_value() ? *std::move(result) : T{};
}

// Every key is on shard 0.
class OneBufferRouter : public consumer::CompactionBufferRouter {
 public:
  explicit OneBufferRouter(consumer::CompactionBuffer& buffer) : buffer_(buffer) {}
  core::Result<core::RespValue> Exec(const ops::ReadOp& op,
                                     std::optional<core::Duration> /*deadline*/) override {
    return buffer_.Exec(op);
  }
  core::Result<core::RespValue> Read(std::string_view key) const override {
    return buffer_.Read(std::string(key));
  }
  consumer::BufferKeyPresence Probe(std::string_view key) const override {
    return buffer_.Probe(key);
  }
  consumer::HashOverlay HashOverlayFor(std::string_view key) const override {
    return buffer_.HashOverlayFor(key);
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

class LoaderTest : public ::testing::Test {
 protected:
  void Buffer(std::string_view key, const ops::WriteOp& op) {
    const core::SequenceId seq = ++seq_;
    buffer_.Absorb(std::string(key), op, kEviction, seq, seq, 0);
  }

  core::Result<hot::LoadResult> Load(std::string_view key, Need need) const {
    return loader_.Load(0, key, need, Deadline());
  }

  static core::SteadyTime Deadline() { return core::SteadyClock::now() + 10s; }

  static void ExpectExists(const core::Result<hot::LoadResult>& result, hot::Entry::Type type,
                           int64_t abs_ttl_ms) {
    ASSERT_TRUE(result.has_value()) << result.error().message();
    const auto* exists = std::get_if<hot::LoadedExists>(&*result);
    ASSERT_NE(exists, nullptr);
    EXPECT_EQ(*exists, (hot::LoadedExists{.type = type, .abs_ttl_ms = abs_ttl_ms}));
  }

  static bool IsAbsent(const core::Result<hot::LoadResult>& result) {
    return result.has_value() && std::holds_alternative<hot::LoadedAbsent>(*result);
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  core::SequenceId seq_ = 0;
  consumer::CompactionBuffer buffer_;
  OneBufferRouter router_{buffer_};
  ::testing::StrictMock<abyss::testing::MockColdStore> cold_;
  hot::ShardedHotStore hot_{hot::ShardedHotStoreConfig{.shard_count = 1}};
  Loader loader_{hot_, router_, cold_,
                 [] { return core::WallTime{std::chrono::milliseconds{kNowMs}}; }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- Existence ---

TEST_F(LoaderTest, AnExistenceLoadOfAColdCollectionReadsNoMembers) {
  EXPECT_CALL(cold_, ProbeKey(std::string_view{"big"}, _))
      .WillOnce(Return(ProbeResult{
          core::KeyMeta{.type = KeyType::kSet, .abs_ttl_ms = 50, .cardinality = 1'000'000}}));
  EXPECT_CALL(cold_, LoadKey(_, _)).Times(0);
  ExpectExists(Load("big", Need::kExistence), hot::Entry::Type::kSet, 50);
}

TEST_F(LoaderTest, AnExistenceLoadOfAnAbsentKey) {
  EXPECT_CALL(cold_, ProbeKey(_, _)).WillOnce(Return(ProbeResult{std::nullopt}));
  EXPECT_TRUE(IsAbsent(Load("never", Need::kExistence)));
}

TEST_F(LoaderTest, TheDeltaDecidesExistenceWithoutCold) {
  Buffer("deleted", ops::Del{.keys = {"deleted"}});
  Buffer("str", ops::StringSet{.key = "str", .value = "v", .abs_ttl_ms = 70});
  Buffer("recreated", ops::Del{.keys = {"recreated"}});
  Buffer("recreated", ops::SetAdd{.key = "recreated", .members = {"a"}});
  Buffer("timed", ops::ZsetAdd{.key = "timed", .entries = {{.score = 1, .member = "m"}}});
  Buffer("timed", ops::Expire{.key = "timed", .abs_ttl_ms = 90});
  EXPECT_CALL(cold_, ProbeKey(_, _)).Times(0);
  EXPECT_CALL(cold_, LoadKey(_, _)).Times(0);

  EXPECT_TRUE(IsAbsent(Load("deleted", Need::kExistence)));
  ExpectExists(Load("str", Need::kExistence), hot::Entry::Type::kString, 70);
  ExpectExists(Load("recreated", Need::kExistence), hot::Entry::Type::kSet, 0);
  ExpectExists(Load("timed", Need::kExistence), hot::Entry::Type::kZset, 90);
}

TEST_F(LoaderTest, AnUnchangedTtlComesFromCold) {
  Buffer("s", ops::SetAdd{.key = "s", .members = {"x"}});
  EXPECT_CALL(cold_, ProbeKey(_, _))
      .WillOnce(Return(
          ProbeResult{core::KeyMeta{.type = KeyType::kSet, .abs_ttl_ms = 50, .cardinality = 3}}));
  ExpectExists(Load("s", Need::kExistence), hot::Entry::Type::kSet, 50);
}

TEST_F(LoaderTest, RemovalsThatMayEmptyTheKeyLoadItInFull) {
  Buffer("small", ops::SetRem{.key = "small", .members = {"a", "b"}});
  Buffer("large", ops::SetRem{.key = "large", .members = {"a", "b"}});
  EXPECT_CALL(cold_, ProbeKey(std::string_view{"small"}, _))
      .WillOnce(Return(ProbeResult{core::KeyMeta{.type = KeyType::kSet, .cardinality = 2}}));
  EXPECT_CALL(cold_, LoadKey(std::string_view{"small"}, _))
      .WillOnce(Return(LoadKeyResult{core::ColdKeyState{
          .type = KeyType::kSet, .value = std::unordered_set<std::string>{"a", "b"}}}));
  EXPECT_CALL(cold_, ProbeKey(std::string_view{"large"}, _))
      .WillOnce(Return(ProbeResult{core::KeyMeta{.type = KeyType::kSet, .cardinality = 3}}));

  EXPECT_TRUE(IsAbsent(Load("small", Need::kExistence)));
  ExpectExists(Load("large", Need::kExistence), hot::Entry::Type::kSet, 0);
}

TEST(LoaderNoStubsTest, WithoutStubsExistenceLoadsInFull) {
  consumer::CompactionBuffer buffer;
  OneBufferRouter router{buffer};
  ::testing::StrictMock<abyss::testing::MockColdStore> cold;
  hot::ShardedHotStore hot{hot::ShardedHotStoreConfig{.max_memory_bytes = 0, .shard_count = 1}};
  const Loader loader{hot, router, cold};
  EXPECT_CALL(cold, LoadKey(_, _))
      .WillOnce(Return(LoadKeyResult{core::ColdKeyState{.type = KeyType::kString, .value = "v"}}));
  auto loaded = loader.Load(0, "k", Need::kExistence, core::SteadyClock::now() + 10s);
  ASSERT_TRUE(loaded.has_value());
  EXPECT_TRUE(std::holds_alternative<hot::LoadedFull>(*loaded));
}

// --- Full loads ---

TEST_F(LoaderTest, AFullLoadReadsNoColdAfterADelAll) {
  Buffer("s", ops::Del{.keys = {"s"}});
  Buffer("s", ops::SetAdd{.key = "s", .members = {"c"}});
  auto loaded = Load("s", Need::kState);
  ASSERT_TRUE(loaded.has_value());
  const auto* full = std::get_if<hot::LoadedFull>(&*loaded);
  ASSERT_NE(full, nullptr);
  EXPECT_EQ(std::get<hot::SetValue>(full->value).members, (std::unordered_set<std::string>{"c"}));
}

TEST_F(LoaderTest, AColdErrorIsReturned) {
  EXPECT_CALL(cold_, LoadKey(_, _))
      .WillOnce(
          Return(LoadKeyResult{std::unexpected(core::Error{core::ErrorCode::kTimeout, "late"})}));
  auto loaded = Load("k", Need::kState);
  ASSERT_FALSE(loaded.has_value());
  EXPECT_EQ(loaded.error().code(), core::ErrorCode::kTimeout);
}

// --- Installed results, as decide reads them ---

class InstalledResultTest : public ::testing::Test {
 protected:
  static constexpr uint64_t kNow = 1'000'000;

  Decision DecideOn(std::vector<std::string> args) {
    core::RespCommand cmd{.args = std::move(args)};
    return Decide(cmd, core::PredicateFlags::kNone, kNow, [this](std::string_view key) {
      return store_.View(key, hot::kAllDrained, kNow);
    });
  }

  void Install(std::string_view key, hot::LoadResult result) {
    const auto token = store_.BeginLoad(key);
    ASSERT_TRUE(token.has_value());
    ASSERT_TRUE(store_.CompleteLoad(key, token.value_or(hot::LoadToken{}), std::move(result),
                                    kEviction, hot::kAllDrained));
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  hot::SingleShardStore store_{hot::SingleShardConfig{.stub_max_entries = 16}};
  core::EvictionPolicy policy_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(InstalledResultTest, AnAbsentResultIsATombstoneDecideReadsAsAbsent) {
  EXPECT_EQ(DecideOn({"SADD", "k", "a"}).needs_load, (std::vector<KeyLoad>{{"k", Need::kState}}));
  Install("k", hot::LoadedAbsent{});

  const Decision sadd = DecideOn({"SADD", "k", "a"});
  EXPECT_TRUE(sadd.needs_load.empty());
  ASSERT_EQ(sadd.effects.size(), 1U);
  EXPECT_TRUE(sadd.effects.front().replaces_state) << "it creates the key";
  const Decision del = DecideOn({"DEL", "k"});
  EXPECT_TRUE(del.needs_load.empty());
  EXPECT_TRUE(del.effects.empty());
  EXPECT_EQ(testing::Describe(del.reply), ":0");
}

TEST_F(InstalledResultTest, AnExistsResultIsAStubDelCounts) {
  EXPECT_EQ(DecideOn({"DEL", "k"}).needs_load, (std::vector<KeyLoad>{{"k", Need::kExistence}}));
  Install("k", hot::LoadedExists{.type = hot::Entry::Type::kSet});
  ASSERT_EQ(store_.View("k", hot::kAllDrained, kNow).presence, Presence::kStub);

  Decision del = DecideOn({"DEL", "k"});
  EXPECT_TRUE(del.needs_load.empty());
  EXPECT_EQ(testing::Describe(del.reply), ":1");
  ASSERT_EQ(del.effects.size(), 1U);
  store_.ApplyEffects(del.effects, 5, core::WallTime{std::chrono::milliseconds{kNow}}, policy_,
                      hot::kAllDrained);
  const hot::KeyView after = store_.View("k", hot::kAllDrained, kNow);
  EXPECT_EQ(after.presence, Presence::kTombstoned);
  EXPECT_EQ(after.latest_seq, 5U);
}

TEST_F(InstalledResultTest, AnExpiredExistsResultIsAnExpiry) {
  Install("k", hot::LoadedExists{.type = hot::Entry::Type::kHash, .abs_ttl_ms = 1});
  const Decision del = DecideOn({"DEL", "k"});
  EXPECT_TRUE(del.needs_load.empty());
  EXPECT_EQ(testing::Describe(del.reply), ":0");
  ASSERT_EQ(del.effects.size(), 1U) << "decide logs the expiry";
  EXPECT_TRUE(del.effects.front().observed_expiry);
}

// --- Point reads ---

TEST_F(LoaderTest, APointReadTheDeltaDecidesReadsNoCold) {
  Buffer("s", ops::SetAdd{.key = "s", .members = {"a"}});
  Buffer("s", ops::Expire{.key = "s", .abs_ttl_ms = kNowMs * 2});
  Buffer("gone", ops::Del{.keys = {"gone"}});
  Buffer("h", ops::Del{.keys = {"h"}});
  Buffer("h", ops::HashSet{.key = "h", .fields = {{.field = "f", .value = "1"}}});
  EXPECT_CALL(cold_, ProbeKey(_, _)).Times(0);
  EXPECT_CALL(cold_, LoadMember(_, _, _, _)).Times(0);

  EXPECT_TRUE(Ok(loader_.IsMember(0, "s", "a", Deadline())));
  EXPECT_FALSE(Ok(loader_.IsMember(0, "gone", "a", Deadline())));
  EXPECT_EQ(Ok(loader_.HashField(0, "h", "f", Deadline())), "1");
  EXPECT_EQ(Ok(loader_.HashField(0, "h", "g", Deadline())), std::nullopt)
      << "after a DEL-all, only the delta counts";
}

TEST_F(LoaderTest, ARemovedMemberIsAbsentWithoutAMemberRead) {
  Buffer("s", ops::SetRem{.key = "s", .members = {"a"}});
  EXPECT_CALL(cold_, ProbeKey(_, _))
      .WillOnce(Return(ProbeResult{core::KeyMeta{.type = KeyType::kSet, .cardinality = 5}}));
  EXPECT_CALL(cold_, LoadMember(_, _, _, _)).Times(0);
  EXPECT_FALSE(Ok(loader_.IsMember(0, "s", "a", Deadline())));
}

TEST_F(LoaderTest, ColdAnswersWhatTheDeltaDoesNot) {
  Buffer("z", ops::ZsetAdd{.key = "z", .entries = {{.score = 7, .member = "new"}}});
  EXPECT_CALL(cold_, ProbeKey(std::string_view{"z"}, _))
      .WillOnce(Return(ProbeResult{core::KeyMeta{.type = KeyType::kZset, .cardinality = 9}}));
  EXPECT_CALL(cold_, LoadMember(std::string_view{"z"}, KeyType::kZset, std::string_view{"old"}, _))
      .WillOnce(Return(MemberResult{core::MemberValue{2.5}}));
  EXPECT_EQ(Ok(loader_.Score(0, "z", "old", Deadline())), 2.5);
}

TEST_F(LoaderTest, APointReadOfAnotherTypeIsWrongType) {
  EXPECT_CALL(cold_, ProbeKey(_, _))
      .WillOnce(Return(ProbeResult{core::KeyMeta{.type = KeyType::kString, .cardinality = 1}}));
  auto member = loader_.IsMember(0, "str", "a", Deadline());
  ASSERT_FALSE(member.has_value());
  EXPECT_EQ(member.error().code(), core::ErrorCode::kWrongType);
}

// Removals that may have emptied a key of another type make it absent,
// not WRONGTYPE; only a load, bounded by the removals, can tell.
TEST_F(LoaderTest, AnEmptiedKeyOfAnotherTypeIsAbsentNotWrongType) {
  Buffer("s", ops::SetRem{.key = "s", .members = {"a", "b"}});
  EXPECT_CALL(cold_, ProbeKey(std::string_view{"s"}, _))
      .WillRepeatedly(Return(ProbeResult{core::KeyMeta{.type = KeyType::kSet, .cardinality = 2}}));
  EXPECT_CALL(cold_, LoadKey(std::string_view{"s"}, _))
      .WillOnce(Return(LoadKeyResult{core::ColdKeyState{
          .type = KeyType::kSet, .value = std::unordered_set<std::string>{"a", "b"}}}));
  EXPECT_EQ(Ok(loader_.HashField(0, "s", "f", Deadline())), std::nullopt);
}

TEST_F(LoaderTest, AKeyOfAnotherTypeTheRemovalsCannotEmptyIsWrongType) {
  Buffer("s", ops::SetRem{.key = "s", .members = {"a"}});
  EXPECT_CALL(cold_, ProbeKey(std::string_view{"s"}, _))
      .WillOnce(Return(ProbeResult{core::KeyMeta{.type = KeyType::kSet, .cardinality = 2}}));
  EXPECT_CALL(cold_, LoadKey(_, _)).Times(0);
  auto field = loader_.HashField(0, "s", "f", Deadline());
  ASSERT_FALSE(field.has_value());
  EXPECT_EQ(field.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(LoaderTest, APointReadOfAnExpiredKeyIsAbsent) {
  EXPECT_CALL(cold_, ProbeKey(_, _))
      .WillOnce(Return(ProbeResult{
          core::KeyMeta{.type = KeyType::kHash, .abs_ttl_ms = kNowMs, .cardinality = 1}}));
  EXPECT_CALL(cold_, LoadMember(_, _, _, _)).Times(0);
  EXPECT_EQ(Ok(loader_.HashField(0, "h", "f", Deadline())), std::nullopt);
}

TEST_F(LoaderTest, APointReadNeverWaitsOnAPlaceholder) {
  ASSERT_TRUE(hot_.BeginLoad("s").started());
  EXPECT_CALL(cold_, ProbeKey(_, _))
      .WillOnce(Return(ProbeResult{core::KeyMeta{.type = KeyType::kSet, .cardinality = 1}}));
  EXPECT_CALL(cold_, LoadMember(_, _, _, _)).WillOnce(Return(MemberResult{core::MemberValue{}}));
  EXPECT_TRUE(Ok(loader_.IsMember(0, "s", "a", core::SteadyClock::now() + 1s)));
}

// --- Install, the read path's cache fill ---

TEST_F(LoaderTest, InstallFillsHotOnceThenFindsItResident) {
  EXPECT_CALL(cold_, LoadKey(std::string_view{"k"}, _))
      .WillOnce(Return(LoadKeyResult{core::ColdKeyState{.type = KeyType::kString, .value = "v"}}));
  auto first = loader_.Install("k", Deadline());
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->fill, Loader::Fill::kInstalled);
  auto get = hot_.Exec(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_TRUE(get.has_value());
  EXPECT_EQ(get->AsString(), "v");

  auto second = loader_.Install("k", Deadline());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->fill, Loader::Fill::kResident);
}

TEST(LoaderFlushFloorTest, InstallUnderTheFlushFloorIsFlushed) {
  consumer::CompactionBuffer buffer;
  OneBufferRouter router{buffer};
  ::testing::StrictMock<abyss::testing::MockColdStore> cold;
  hot::ShardedHotStore hot{hot::ShardedHotStoreConfig{
      .shard_count = 1, .drained = [](core::ShardId) { return core::SequenceId{0}; }}};
  ASSERT_TRUE(hot.Wipe(0, 10).has_value());
  Loader loader{hot, router, cold};
  EXPECT_CALL(cold, LoadKey(_, _)).Times(0);
  auto filled = loader.Install("k", core::SteadyClock::now() + 10s);
  ASSERT_TRUE(filled.has_value());
  EXPECT_EQ(filled->fill, Loader::Fill::kFlushed) << "cold may still hold what the Flush removed";
}

TEST_F(LoaderTest, ConcurrentInstallsShareOneColdRead) {
  constexpr int kReaders = 8;
  std::atomic<int> loads{0};
  abyss::testing::Latch release;
  EXPECT_CALL(cold_, LoadKey(std::string_view{"set"}, _)).WillOnce([&](auto, auto) {
    ++loads;
    release.Wait();
    return LoadKeyResult{core::ColdKeyState{.type = KeyType::kSet,
                                            .value = std::unordered_set<std::string>{"a", "b"}}};
  });

  std::vector<Loader::Fill> fills(kReaders, Loader::Fill::kDiscarded);
  std::vector<std::thread> readers;
  readers.reserve(kReaders);
  const abyss::testing::OnExit join([&] {
    release.Open();
    for (auto& reader : readers) reader.join();
  });
  for (int i = 0; i < kReaders; ++i) {
    readers.emplace_back([&, i] {
      auto filled = loader_.Install("set", Deadline());
      ASSERT_TRUE(filled.has_value());
      fills[static_cast<size_t>(i)] = filled->fill;
    });
  }
  // Every reader has either begun the load or found its placeholder.
  for (int spins = 0; loads.load() == 0 && spins < 1000; ++spins) std::this_thread::sleep_for(1ms);
  std::this_thread::sleep_for(20ms);
  release.Open();
  for (auto& reader : readers) reader.join();
  readers.clear();

  EXPECT_EQ(loads.load(), 1);
  EXPECT_EQ(std::ranges::count(fills, Loader::Fill::kInstalled), 1);
  EXPECT_EQ(std::ranges::count(fills, Loader::Fill::kResident), kReaders - 1);
  auto card = hot_.Exec(ops::ReadOp{ops::SetCard{.key = "set"}});
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 2);
}

TEST_F(LoaderTest, AFailedInstallAbortsItsPlaceholder) {
  EXPECT_CALL(cold_, LoadKey(_, _))
      .WillOnce(
          Return(LoadKeyResult{std::unexpected(core::Error{core::ErrorCode::kUnavailable, "io"})}));
  auto filled = loader_.Install("k", Deadline());
  ASSERT_FALSE(filled.has_value());
  EXPECT_EQ(filled.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_FALSE(hot_.LoadPending("k"));
}

TEST_F(LoaderTest, InstallAwaitingAnotherLoadTimesOut) {
  ASSERT_TRUE(hot_.BeginLoad("k").started());
  auto filled = loader_.Install("k", core::SteadyClock::now() + 20ms);
  ASSERT_FALSE(filled.has_value());
  EXPECT_EQ(filled.error().code(), core::ErrorCode::kTimeout);
}

}  // namespace
}  // namespace abyss::engine
