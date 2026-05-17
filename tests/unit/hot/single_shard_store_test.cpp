#include "abyss/hot/single_shard_store.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <string>
#include <vector>

#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;

class SingleShardStoreTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TestClock clock_;
  SingleShardStore store_{SingleShardConfig{
      .max_memory_bytes = 1024UL * 1024,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
  static constexpr core::EvictionTTL kEviction{86400};

  void SetString(std::string_view key, std::string_view value, uint64_t ttl_ms = 0) {
    core::ops::StringSet op{.key = key, .value = value, .abs_ttl_ms = ttl_ms};
    auto result = store_.Apply(core::ops::WriteOp{op}, kEviction);
    ASSERT_TRUE(result.has_value()) << result.error().message();
  }

  core::Result<core::RespValue> GetString(std::string_view key) {
    core::ops::StringGet op{.key = key};
    return store_.Exec(core::ops::ReadOp{op});
  }
};

// --- String operations ---

TEST_F(SingleShardStoreTest, StringSetThenGet) {
  SetString("key1", "value1");
  auto result = GetString("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "value1");
}

TEST_F(SingleShardStoreTest, StringGetMissing) {
  auto result = GetString("nonexistent");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(SingleShardStoreTest, StringOverwrite) {
  SetString("key1", "old");
  SetString("key1", "new");
  auto result = GetString("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "new");
}

TEST_F(SingleShardStoreTest, StringEmptyValue) {
  SetString("key1", "");
  auto result = GetString("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "");
}

// --- Set operations ---

TEST_F(SingleShardStoreTest, SetAddAndIsMember) {
  core::ops::SetAdd add_op{.key = "myset", .members = {"a", "b", "c"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::SetIsMember is_op{.key = "myset", .member = "b"};
  auto result = store_.Exec(core::ops::ReadOp{is_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 1);
}

TEST_F(SingleShardStoreTest, SetIsMemberNonMember) {
  core::ops::SetAdd add_op{.key = "myset", .members = {"a"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::SetIsMember is_op{.key = "myset", .member = "z"};
  auto result = store_.Exec(core::ops::ReadOp{is_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 0);
}

TEST_F(SingleShardStoreTest, SetIsMemberMissingKey) {
  core::ops::SetIsMember is_op{.key = "nosuchkey", .member = "a"};
  auto result = store_.Exec(core::ops::ReadOp{is_op});
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(SingleShardStoreTest, SetMembersReturnsAll) {
  core::ops::SetAdd add_op{.key = "myset", .members = {"x", "y"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::SetMembers op{.key = "myset"};
  auto result = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  EXPECT_EQ(result->AsArray().size(), 2U);
}

TEST_F(SingleShardStoreTest, SetCard) {
  core::ops::SetAdd add_op{.key = "myset", .members = {"a", "b", "c"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::SetCard op{.key = "myset"};
  auto result = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 3);
}

TEST_F(SingleShardStoreTest, SetRemRemovesMember) {
  core::ops::SetAdd add_op{.key = "myset", .members = {"a", "b"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::SetRem rem_op{.key = "myset", .members = {"a"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{rem_op}, kEviction).has_value());

  core::ops::SetCard card_op{.key = "myset"};
  auto result = store_.Exec(core::ops::ReadOp{card_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 1);
}

TEST_F(SingleShardStoreTest, SetRemLastMemberRemovesKey) {
  core::ops::SetAdd add_op{.key = "myset", .members = {"only"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::SetRem rem_op{.key = "myset", .members = {"only"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{rem_op}, kEviction).has_value());

  core::ops::SetCard card_op{.key = "myset"};
  auto result = store_.Exec(core::ops::ReadOp{card_op});
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

// --- Sorted set operations ---

TEST_F(SingleShardStoreTest, ZsetAddAndScore) {
  core::ops::ZsetAdd add_op{.key = "zs", .entries = {{.score = 1.5, .member = "m1"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::ZsetScore score_op{.key = "zs", .member = "m1"};
  auto result = store_.Exec(core::ops::ReadOp{score_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "1.5");
}

TEST_F(SingleShardStoreTest, ZsetScoreNonMember) {
  core::ops::ZsetAdd add_op{.key = "zs", .entries = {{.score = 1.0, .member = "m1"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::ZsetScore score_op{.key = "zs", .member = "nonexistent"};
  auto result = store_.Exec(core::ops::ReadOp{score_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(SingleShardStoreTest, ZsetCard) {
  core::ops::ZsetAdd add_op{
      .key = "zs", .entries = {{.score = 1.0, .member = "a"}, {.score = 2.0, .member = "b"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::ZsetCard card_op{.key = "zs"};
  auto result = store_.Exec(core::ops::ReadOp{card_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(SingleShardStoreTest, ZsetAddUpdatesScore) {
  core::ops::ZsetAdd add1{.key = "zs", .entries = {{.score = 1.0, .member = "m"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add1}, kEviction).has_value());

  core::ops::ZsetAdd add2{.key = "zs", .entries = {{.score = 5.0, .member = "m"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add2}, kEviction).has_value());

  core::ops::ZsetScore score_op{.key = "zs", .member = "m"};
  auto result = store_.Exec(core::ops::ReadOp{score_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "5");

  core::ops::ZsetCard card_op{.key = "zs"};
  auto card = store_.Exec(core::ops::ReadOp{card_op});
  EXPECT_EQ(card->AsInteger(), 1);
}

TEST_F(SingleShardStoreTest, ZsetRangeByScore) {
  core::ops::ZsetAdd add_op{.key = "zs",
                            .entries = {{.score = 1.0, .member = "a"},
                                        {.score = 2.0, .member = "b"},
                                        {.score = 3.0, .member = "c"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::ZsetRange range_op{.key = "zs", .min = "1", .max = "2", .by_score = true};
  auto result = store_.Exec(core::ops::ReadOp{range_op});
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  ASSERT_EQ(result->AsArray().size(), 2U);
  EXPECT_EQ(result->AsArray()[0].AsString(), "a");
  EXPECT_EQ(result->AsArray()[1].AsString(), "b");
}

TEST_F(SingleShardStoreTest, ZsetRemRemovesMember) {
  core::ops::ZsetAdd add_op{
      .key = "zs", .entries = {{.score = 1.0, .member = "a"}, {.score = 2.0, .member = "b"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  core::ops::ZsetRem rem_op{.key = "zs", .members = {"a"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{rem_op}, kEviction).has_value());

  core::ops::ZsetCard card_op{.key = "zs"};
  auto result = store_.Exec(core::ops::ReadOp{card_op});
  EXPECT_EQ(result->AsInteger(), 1);
}

// --- Hash operations ---

TEST_F(SingleShardStoreTest, HashSetAndGet) {
  core::ops::HashSet set_op{.key = "h", .fields = {{.field = "f1", .value = "v1"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set_op}, kEviction).has_value());

  core::ops::HashGet get_op{.key = "h", .field = "f1"};
  auto result = store_.Exec(core::ops::ReadOp{get_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v1");
}

TEST_F(SingleShardStoreTest, HashGetMissingField) {
  core::ops::HashSet set_op{.key = "h", .fields = {{.field = "f1", .value = "v1"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set_op}, kEviction).has_value());

  core::ops::HashGet get_op{.key = "h", .field = "no_such_field"};
  auto result = store_.Exec(core::ops::ReadOp{get_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(SingleShardStoreTest, HashGetAll) {
  core::ops::HashSet set_op{.key = "h",
                            .fields = {{.field = "a", .value = "1"}, {.field = "b", .value = "2"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set_op}, kEviction).has_value());

  core::ops::HashGetAll get_op{.key = "h"};
  auto result = store_.Exec(core::ops::ReadOp{get_op});
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  EXPECT_EQ(result->AsArray().size(), 4U);
}

TEST_F(SingleShardStoreTest, HashDelRemovesField) {
  core::ops::HashSet set_op{.key = "h",
                            .fields = {{.field = "a", .value = "1"}, {.field = "b", .value = "2"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set_op}, kEviction).has_value());

  core::ops::HashDel del_op{.key = "h", .fields = {"a"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{del_op}, kEviction).has_value());

  core::ops::HashGet get_op{.key = "h", .field = "a"};
  auto result = store_.Exec(core::ops::ReadOp{get_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(SingleShardStoreTest, HsetReturnsNewFieldCount) {
  core::ops::HashSet first{.key = "h",
                           .fields = {{.field = "a", .value = "1"}, {.field = "b", .value = "2"}}};
  auto r1 = store_.Apply(core::ops::WriteOp{first}, kEviction);
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(r1->AsInteger(), 2);

  core::ops::HashSet second{
      .key = "h", .fields = {{.field = "a", .value = "1b"}, {.field = "c", .value = "3"}}};
  auto r2 = store_.Apply(core::ops::WriteOp{second}, kEviction);
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(r2->AsInteger(), 1);
}

TEST_F(SingleShardStoreTest, HmsetReturnsOk) {
  core::ops::HashMSet op{.key = "h",
                         .fields = {{.field = "a", .value = "1"}, {.field = "b", .value = "2"}}};
  auto r = store_.Apply(core::ops::WriteOp{op}, kEviction);
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(r->IsSimpleString());
  EXPECT_EQ(r->AsString(), "OK");

  // HMSET persists field-by-field like HSET; subsequent HGET sees the values.
  core::ops::HashGet get_op{.key = "h", .field = "a"};
  auto got = store_.Exec(core::ops::ReadOp{get_op});
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->AsString(), "1");
}

TEST_F(SingleShardStoreTest, HmgetReturnsArrayWithNullsForMissing) {
  core::ops::HashSet set_op{.key = "h",
                            .fields = {{.field = "a", .value = "1"}, {.field = "b", .value = "2"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set_op}, kEviction).has_value());

  core::ops::HashMultiGet op{.key = "h", .fields = {"a", "missing", "b"}};
  auto r = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(r->IsArray());
  const auto& arr = r->AsArray();
  ASSERT_EQ(arr.size(), 3U);
  EXPECT_EQ(arr[0].AsString(), "1");
  EXPECT_TRUE(arr[1].IsNull());
  EXPECT_EQ(arr[2].AsString(), "2");
}

TEST_F(SingleShardStoreTest, HexistsReturns1Or0) {
  core::ops::HashSet set_op{.key = "h", .fields = {{.field = "a", .value = "1"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set_op}, kEviction).has_value());

  core::ops::HashFieldExists yes{.key = "h", .field = "a"};
  EXPECT_EQ(store_.Exec(core::ops::ReadOp{yes})->AsInteger(), 1);
  core::ops::HashFieldExists no{.key = "h", .field = "no_such"};
  EXPECT_EQ(store_.Exec(core::ops::ReadOp{no})->AsInteger(), 0);
}

TEST_F(SingleShardStoreTest, HkeysHvalsHlenReturnCollection) {
  core::ops::HashSet set_op{.key = "h",
                            .fields = {{.field = "a", .value = "1"}, {.field = "b", .value = "2"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set_op}, kEviction).has_value());

  auto keys = store_.Exec(core::ops::ReadOp{core::ops::HashKeys{.key = "h"}});
  ASSERT_TRUE(keys.has_value());
  ASSERT_TRUE(keys->IsArray());
  EXPECT_EQ(keys->AsArray().size(), 2U);

  auto vals = store_.Exec(core::ops::ReadOp{core::ops::HashVals{.key = "h"}});
  ASSERT_TRUE(vals.has_value());
  ASSERT_TRUE(vals->IsArray());
  EXPECT_EQ(vals->AsArray().size(), 2U);

  auto len = store_.Exec(core::ops::ReadOp{core::ops::HashLen{.key = "h"}});
  ASSERT_TRUE(len.has_value());
  EXPECT_EQ(len->AsInteger(), 2);
}

TEST_F(SingleShardStoreTest, HashReadsOnMissingKeyReportNotFound) {
  // FindTypedEntry-on-absent returns kNotFound so the engine can fall through
  // to buffer + cold. This contract is load-bearing for the merge path.
  for (const auto& op : std::array<core::ops::ReadOp, 5>{
           core::ops::HashKeys{.key = "missing"},
           core::ops::HashVals{.key = "missing"},
           core::ops::HashLen{.key = "missing"},
           core::ops::HashFieldExists{.key = "missing", .field = "f"},
           core::ops::HashMultiGet{.key = "missing", .fields = {"f"}},
       }) {
    auto r = store_.Exec(op);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), core::ErrorCode::kNotFound);
  }
}

TEST_F(SingleShardStoreTest, HashReadsOnWrongTypeReturnWrongType) {
  SetString("k", "scalar");
  for (const auto& op : std::array<core::ops::ReadOp, 5>{
           core::ops::HashKeys{.key = "k"},
           core::ops::HashVals{.key = "k"},
           core::ops::HashLen{.key = "k"},
           core::ops::HashFieldExists{.key = "k", .field = "f"},
           core::ops::HashMultiGet{.key = "k", .fields = {"f"}},
       }) {
    auto r = store_.Exec(op);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), core::ErrorCode::kWrongType);
  }
}

// --- DEL ---

TEST_F(SingleShardStoreTest, DelRemovesKey) {
  SetString("key1", "val");
  core::ops::Del del_op{.keys = {"key1"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{del_op}, kEviction).has_value());

  auto result = GetString("key1");
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(SingleShardStoreTest, DelNonExistentIsNoop) {
  core::ops::Del del_op{.keys = {"nonexistent"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{del_op}, kEviction).has_value());
}

// --- EXISTS ---

TEST_F(SingleShardStoreTest, ExistsCountsPresent) {
  SetString("a", "1");
  SetString("b", "2");

  core::ops::Exists op{.keys = {"a", "b", "c"}};
  auto result = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

// --- Type errors ---

TEST_F(SingleShardStoreTest, WrongTypeSetOnString) {
  SetString("key1", "value");

  core::ops::SetAdd add_op{.key = "key1", .members = {"m"}};
  auto result = store_.Apply(core::ops::WriteOp{add_op}, kEviction);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(SingleShardStoreTest, WrongTypeStringGetOnSet) {
  core::ops::SetAdd add_op{.key = "myset", .members = {"a"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add_op}, kEviction).has_value());

  auto result = GetString("myset");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kWrongType);
}

// --- Eviction ---

TEST_F(SingleShardStoreTest, EvictExpiredRemovesOldKeys) {
  core::ops::StringSet op{.key = "k", .value = "v"};
  auto short_eviction = core::EvictionTTL{1};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{op}, short_eviction).has_value());

  auto before = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(before.Total(), 0U);

  clock_.Advance(1100ms);
  auto after = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(after.Total(), 1U);
  EXPECT_EQ(after.by_deadline, 1U);
  EXPECT_EQ(after.by_ttl, 0U);

  auto result = GetString("k");
  EXPECT_FALSE(result.has_value());
}

TEST_F(SingleShardStoreTest, RefreshAccessExtendsDeadline) {
  core::ops::StringSet op{.key = "k", .value = "v"};
  auto short_eviction = core::EvictionTTL{1};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{op}, short_eviction).has_value());

  clock_.Advance(500ms);
  store_.RefreshAccess("k", clock_.SteadyNow());

  clock_.Advance(700ms);
  auto evicted = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(evicted.Total(), 0U);
}

TEST_F(SingleShardStoreTest, EvictExpiredAttributesTtlReason) {
  // Long eviction so the deadline never fires; rely on abs_ttl_ms to drive
  // removal and assert the by_ttl counter, not by_deadline.
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  SetString("k", "v", static_cast<uint64_t>(now_ms + 1000));

  auto before = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(before.by_ttl, 0U);
  EXPECT_EQ(before.by_deadline, 0U);

  clock_.Advance(1500ms);
  auto after = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(after.by_ttl, 1U) << "abs TTL drove removal";
  EXPECT_EQ(after.by_deadline, 0U) << "eviction deadline is far in the future";
}

TEST_F(SingleShardStoreTest, EvictExpiredTtlWinsWhenBothApply) {
  // Short eviction + short TTL. The deadline check is `<=`, the TTL check
  // is `>=` on wall-ms — at clock advance both fire on the same entry. The
  // metric must attribute to TTL because the semantic outcome is "deleted
  // entirely" not "moved tier".
  core::ops::StringSet op{.key = "k", .value = "v"};
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  op.abs_ttl_ms = static_cast<uint64_t>(now_ms + 500);
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{op}, core::EvictionTTL{1}).has_value());

  clock_.Advance(1500ms);
  auto report = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(report.by_ttl, 1U);
  EXPECT_EQ(report.by_deadline, 0U);
}

TEST_F(SingleShardStoreTest, EvictLruRemovesOldest) {
  SetString("old", std::string(512, 'x'));
  store_.RefreshAccess("old", clock_.SteadyNow() - 100s);

  SetString("new", std::string(512, 'y'));

  auto target = store_.Stats().used_bytes * 3 / 4;
  auto evicted = store_.EvictLru(target);
  EXPECT_GE(evicted, 1U);

  auto old_result = GetString("old");
  EXPECT_FALSE(old_result.has_value());

  auto new_result = GetString("new");
  EXPECT_TRUE(new_result.has_value());
}

// --- TTL ---

TEST_F(SingleShardStoreTest, TtlExpiredKeyNotFound) {
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  SetString("k", "v", static_cast<uint64_t>(now_ms - 1000));

  auto result = GetString("k");
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(SingleShardStoreTest, TtlFutureKeyFound) {
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  SetString("k", "v", static_cast<uint64_t>(now_ms + 60000));

  auto result = GetString("k");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v");
}

// --- Memory tracking ---

TEST_F(SingleShardStoreTest, StatsTrackInsertAndDelete) {
  auto before = store_.Stats();
  EXPECT_EQ(before.key_count, 0U);
  EXPECT_EQ(before.used_bytes, 0U);

  SetString("k", "v");
  auto after_insert = store_.Stats();
  EXPECT_EQ(after_insert.key_count, 1U);
  EXPECT_GT(after_insert.used_bytes, 0U);

  core::ops::Del del_op{.keys = {"k"}};
  auto del_result = store_.Apply(core::ops::WriteOp{del_op}, kEviction);
  auto after_delete = store_.Stats();
  EXPECT_EQ(after_delete.key_count, 0U);
  EXPECT_EQ(after_delete.used_bytes, 0U);
}

// --- Wipe ---

TEST_F(SingleShardStoreTest, WipeClearsAll) {
  SetString("a", "1");
  SetString("b", "2");
  store_.Wipe();

  EXPECT_EQ(store_.Stats().key_count, 0U);
  auto result = GetString("a");
  EXPECT_FALSE(result.has_value());
}

// --- ApplyBatch ---

TEST_F(SingleShardStoreTest, ApplyBatchMultipleOps) {
  std::vector<core::ops::WriteOp> ops;
  ops.emplace_back(core::ops::StringSet{.key = "a", .value = "1"});
  ops.emplace_back(core::ops::StringSet{.key = "b", .value = "2"});

  auto result = store_.ApplyBatch(ops, kEviction);
  ASSERT_TRUE(result.has_value());

  auto a = GetString("a");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->AsString(), "1");

  auto b = GetString("b");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->AsString(), "2");
}

}  // namespace
}  // namespace abyss::hot
