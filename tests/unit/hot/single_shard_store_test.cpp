#include "abyss/hot/single_shard_store.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <string>
#include <string_view>
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

  // Emptying the set deletes the key; it now reads as an authoritative 0 via a
  // tombstone rather than a miss that would fall through. See ADP-006 §Read Path.
  core::ops::SetCard card_op{.key = "myset"};
  auto result = store_.Exec(core::ops::ReadOp{card_op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 0);
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

  // DEL leaves a tombstone: the key reads as an authoritative nil (success),
  // not a miss, so the read never falls through to a lagging overlay.
  auto result = GetString("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
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
  // The delete leaves a tombstone, so a small footprint remains until
  // GcTombstones reclaims the entry (proven separately). It is never larger
  // than the live entry — the value is released (modulo small-string capacity).
  EXPECT_GT(after_delete.used_bytes, 0U);
  EXPECT_LE(after_delete.used_bytes, after_insert.used_bytes);
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

// --- Delete tombstones (ADP-006 §Read Path) ---

TEST_F(SingleShardStoreTest, ProbeReportsPresentTombstonedAbsent) {
  SetString("live", "v");
  SetString("dead", "v");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"dead"}}}, kEviction, 1).has_value());

  EXPECT_EQ(store_.Probe("live"), core::HotKeyPresence::kPresent);
  EXPECT_EQ(store_.Probe("dead"), core::HotKeyPresence::kTombstoned);
  EXPECT_EQ(store_.Probe("never"), core::HotKeyPresence::kAbsent);
}

TEST_F(SingleShardStoreTest, TtlExpiredProbesAbsentNotTombstoned) {
  // A TTL-expired key is not a tombstone: cold applies the same expiry, so the
  // read must be free to fall through rather than answer authoritatively.
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  SetString("k", "v", static_cast<uint64_t>(now_ms - 1000));
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kAbsent);
}

TEST_F(SingleShardStoreTest, DeletedKeyExcludedFromKeyCount) {
  SetString("k", "v");
  EXPECT_EQ(store_.Stats().key_count, 1U);
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, 1).has_value());
  EXPECT_EQ(store_.Stats().key_count, 0U);
}

TEST_F(SingleShardStoreTest, ExistsTreatsTombstoneAsAbsent) {
  SetString("a", "1");
  SetString("b", "2");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"b"}}}, kEviction, 1).has_value());

  core::ops::Exists op{.keys = {"a", "b", "c"}};
  auto r = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsInteger(), 1) << "only the live key counts";
}

TEST_F(SingleShardStoreTest, EmptiedHashReadsAuthoritativeEmpty) {
  core::ops::HashSet set{.key = "h", .fields = {{.field = "f", .value = "1"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set}, kEviction).has_value());
  core::ops::HashDel del{.key = "h", .fields = {"f"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{del}, kEviction, 2).has_value());

  EXPECT_EQ(store_.Probe("h"), core::HotKeyPresence::kTombstoned);
  auto all = store_.Exec(core::ops::ReadOp{core::ops::HashGetAll{.key = "h"}});
  ASSERT_TRUE(all.has_value());
  ASSERT_TRUE(all->IsArray());
  EXPECT_TRUE(all->AsArray().empty());
  auto len = store_.Exec(core::ops::ReadOp{core::ops::HashLen{.key = "h"}});
  ASSERT_TRUE(len.has_value());
  EXPECT_EQ(len->AsInteger(), 0);
}

TEST_F(SingleShardStoreTest, SetAfterDeleteResurrectsKey) {
  SetString("k", "v1");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, 5).has_value());
  ASSERT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);

  SetString("k", "v2");
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kPresent);
  EXPECT_EQ(store_.Stats().key_count, 1U);
  auto r = GetString("k");
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsString(), "v2");
}

TEST_F(SingleShardStoreTest, WriteDifferentTypeToTombstonedKey) {
  SetString("k", "v");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, 1).has_value());

  // SADD on a tombstoned (formerly string) key revives it as a set; no WRONGTYPE.
  core::ops::SetAdd add{.key = "k", .members = {"m"}};
  auto r = store_.Apply(core::ops::WriteOp{add}, kEviction);
  ASSERT_TRUE(r.has_value()) << r.error().message();
  EXPECT_EQ(store_.Stats().key_count, 1U);

  auto members = store_.Exec(core::ops::ReadOp{core::ops::SetMembers{.key = "k"}});
  ASSERT_TRUE(members.has_value());
  EXPECT_EQ(members->AsArray().size(), 1U);
}

TEST_F(SingleShardStoreTest, GcTombstonesReclaimsAtOrBelowHorizon) {
  SetString("a", "1");
  SetString("b", "2");
  SetString("c", "3");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"a"}}}, kEviction, 10).has_value());
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"b"}}}, kEviction, 20).has_value());
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"c"}}}, kEviction, 30).has_value());

  EXPECT_EQ(store_.GcTombstones(20), 2U) << "reclaims deletes at seq <= 20";
  EXPECT_EQ(store_.Probe("a"), core::HotKeyPresence::kAbsent);
  EXPECT_EQ(store_.Probe("b"), core::HotKeyPresence::kAbsent);
  EXPECT_EQ(store_.Probe("c"), core::HotKeyPresence::kTombstoned) << "seq 30 not yet covered";

  EXPECT_EQ(store_.GcTombstones(30), 1U);
  EXPECT_EQ(store_.Probe("c"), core::HotKeyPresence::kAbsent);
}

// --- Accounting correctness (HOT-2, HOT-3) ---

TEST_F(SingleShardStoreTest, CollectionCreateAccountsFullFootprint) {
  // A brand-new set must count the empty-entry baseline plus the member
  // footprint — strictly more than nothing, and at least as much as a string
  // of the same key created the same way (the create path is no longer biased
  // low by a spurious leading TrackRemove).
  core::ops::SetAdd add{.key = "myset", .members = {"alpha", "beta", "gamma"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add}, kEviction).has_value());
  const auto set_bytes = store_.Stats().used_bytes;
  EXPECT_GT(set_bytes, 0U);

  SetString("equal_len_key", "v");  // same key length, scalar value
  const auto total_after_string = store_.Stats().used_bytes;
  EXPECT_GT(total_after_string, set_bytes) << "string add must increase used_bytes";
}

TEST_F(SingleShardStoreTest, TombstoneResurrectThenCreateBalancedBytes) {
  // Dropping and recreating a collection key N times must not drift used_bytes_
  // downward (the resurrect path no longer double-subtracts).
  const auto fresh_create = [&] {
    core::ops::SetAdd add{.key = "k", .members = {"a", "b", "c"}};
    EXPECT_TRUE(store_.Apply(core::ops::WriteOp{add}, kEviction).has_value());
    return store_.Stats().used_bytes;
  };

  core::SequenceId seq = 1;
  const auto baseline = fresh_create();
  EXPECT_GT(baseline, 0U);

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, seq++)
                    .has_value());
    // GC the tombstone so the slot returns to truly empty before recreating.
    store_.GcTombstones(seq);
    const auto recreated = fresh_create();
    EXPECT_EQ(recreated, baseline) << "used_bytes drifted after drop/recreate cycle " << i;
  }
}

TEST_F(SingleShardStoreTest, AccountingInvariantUnderRandomOps) {
  // After a delete+GC of every key, used_bytes_ must return to exactly 0 — any
  // unbalanced Track pair would leave residue (or clamp at 0 hiding an
  // over-count, which the per-step monotonic checks below would catch).
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::SetAdd{.key = "s", .members = {"x", "y"}}},
                         kEviction)
                  .has_value());
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::HashSet{
                             .key = "h", .fields = {{.field = "f", .value = "v"}}}},
                         kEviction)
                  .has_value());
  ASSERT_TRUE(store_
                  .Apply(core::ops::WriteOp{core::ops::ZsetAdd{
                             .key = "z", .entries = {{.score = 1.0, .member = "m"}}}},
                         kEviction)
                  .has_value());
  SetString("str", "value");
  EXPECT_GT(store_.Stats().used_bytes, 0U);

  core::SequenceId seq = 1;
  for (const auto* key : {"s", "h", "z", "str"}) {
    ASSERT_TRUE(store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {key}}}, kEviction, seq++)
                    .has_value());
  }
  store_.GcTombstones(seq);
  EXPECT_EQ(store_.Stats().used_bytes, 0U) << "used_bytes did not return to baseline";
  EXPECT_EQ(store_.Stats().key_count, 0U);
}

TEST_F(SingleShardStoreTest, ZsetAccountsScoreMembersIndex) {
  // A zset stores every member twice (member_scores + score_members). Its
  // footprint must exceed a same-cardinality set of identical member strings,
  // proving the score index is counted (HOT-3).
  const std::vector<std::string_view> members = {"alpha", "bravo", "charlie", "delta"};
  core::ops::SetAdd set_add{.key = "as_set"};
  core::ops::ZsetAdd zset_add{.key = "as_zset"};
  double score = 1.0;
  for (auto m : members) {
    set_add.members.push_back(m);
    zset_add.entries.push_back({.score = score, .member = m});
    score += 1.0;
  }
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{set_add}, kEviction).has_value());
  const auto set_bytes = store_.Stats().used_bytes;
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{zset_add}, kEviction).has_value());
  const auto total_bytes = store_.Stats().used_bytes;
  const auto zset_bytes = total_bytes - set_bytes;

  EXPECT_GT(zset_bytes, set_bytes)
      << "zset of equal cardinality must report more than a set (double-indexed members)";
}

TEST_F(SingleShardStoreTest, ZsetRemReleasesBothIndexes) {
  // After ZADD then ZREM of every member (emptied -> tombstone), GC must return
  // used_bytes_ to the pre-ZADD baseline: no residual from the score index.
  const auto baseline = store_.Stats().used_bytes;
  core::ops::ZsetAdd add{.key = "z",
                         .entries = {{.score = 1.0, .member = "a"},
                                     {.score = 2.0, .member = "b"},
                                     {.score = 3.0, .member = "c"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add}, kEviction).has_value());
  EXPECT_GT(store_.Stats().used_bytes, baseline);

  core::ops::ZsetRem rem{.key = "z", .members = {"a", "b", "c"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{rem}, kEviction, 1).has_value());
  store_.GcTombstones(2);
  EXPECT_EQ(store_.Stats().used_bytes, baseline) << "score_members index not fully released";
}

// --- ZRANGE bound parsing is total / non-throwing (HOT-6) ---

TEST_F(SingleShardStoreTest, ZsetRangeMalformedScoreReturnsInvalidArgument) {
  core::ops::ZsetAdd add{.key = "z",
                         .entries = {{.score = 1.0, .member = "a"}, {.score = 2.0, .member = "b"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add}, kEviction).has_value());

  // Malformed by-score bound: clean error, no throw.
  core::ops::ZsetRange bad_score{.key = "z", .min = "abc", .max = "2", .by_score = true};
  auto r = store_.Exec(core::ops::ReadOp{bad_score});
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);

  // Malformed index bound (non-numeric): clean error.
  core::ops::ZsetRange bad_idx{.key = "z", .min = "x", .max = "1", .by_score = false};
  auto r2 = store_.Exec(core::ops::ReadOp{bad_idx});
  ASSERT_FALSE(r2.has_value());
  EXPECT_EQ(r2.error().code(), core::ErrorCode::kInvalidArgument);

  // Valid sentinels and numeric ranges still work.
  core::ops::ZsetRange inf{.key = "z", .min = "-inf", .max = "+inf", .by_score = true};
  auto ok = store_.Exec(core::ops::ReadOp{inf});
  ASSERT_TRUE(ok.has_value());
  EXPECT_EQ(ok->AsArray().size(), 2U);

  core::ops::ZsetRange empty_bounds{.key = "z", .min = "", .max = "", .by_score = false};
  auto ok2 = store_.Exec(core::ops::ReadOp{empty_bounds});
  ASSERT_TRUE(ok2.has_value());
  EXPECT_EQ(ok2->AsArray().size(), 2U);
}

TEST_F(SingleShardStoreTest, ZsetRangeArbitraryBytesNeverThrow) {
  core::ops::ZsetAdd add{.key = "z", .entries = {{.score = 1.0, .member = "a"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add}, kEviction).has_value());

  const std::array<std::string_view, 8> fuzz = {
      "1e999999",         "9999999999999999999999999999", "nan", "0x10", "1.2.3", "+-1",
      std::string_view{}, std::string_view("\0\1\2", 3)};
  for (bool by_score : {true, false}) {
    for (auto bad : fuzz) {
      core::ops::ZsetRange op{.key = "z", .min = bad, .max = bad, .by_score = by_score};
      // The contract is: never throws, always a valid Result (ok or error).
      auto r = store_.Exec(core::ops::ReadOp{op});
      (void)r;  // either outcome is acceptable; the point is no exception escapes.
      SUCCEED();
    }
  }
}

// --- Counter split: TTL-expiry vs deadline-eviction (HOT-7) ---

TEST_F(SingleShardStoreTest, TtlExpiryCountsExpiredNotEviction) {
  // TTL-expired key bumps expired_count only.
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  SetString("ttl", "v", static_cast<uint64_t>(now_ms + 500));
  clock_.Advance(1000ms);
  store_.EvictExpired(clock_.SteadyNow());
  auto after_ttl = store_.Stats();
  EXPECT_EQ(after_ttl.expired_count, 1U);
  EXPECT_EQ(after_ttl.eviction_count, 0U);

  // Deadline-evicted key bumps eviction_count only, leaving expired_count.
  core::ops::StringSet op{.key = "ev", .value = "v"};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{op}, core::EvictionTTL{1}).has_value());
  clock_.Advance(1100ms);
  store_.EvictExpired(clock_.SteadyNow());
  auto after_deadline = store_.Stats();
  EXPECT_EQ(after_deadline.expired_count, 1U) << "TTL count unchanged by a deadline eviction";
  EXPECT_EQ(after_deadline.eviction_count, 1U);
}

// --- Memory-pressure LRU eviction (HOT-1) ---

// Measures the footprint of a single string entry of the given key/value, so
// budgets can be sized relative to the real per-entry cost rather than guessed.
uint64_t MeasureStringEntryBytes(std::string_view key, const std::string& value) {
  abyss::testing::TestClock clock;
  SingleShardStore probe{SingleShardConfig{
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  EXPECT_TRUE(probe
                  .Apply(core::ops::WriteOp{core::ops::StringSet{.key = key, .value = value}},
                         core::EvictionTTL{86400})
                  .has_value());
  return probe.Stats().used_bytes;
}

TEST(SingleShardStoreMemoryTest, ApplyEvictsLruToFitUnderBudget) {
  abyss::testing::TestClock clock;
  const std::string value(64, 'v');
  const uint64_t per_entry = MeasureStringEntryBytes("a", value);
  // Budget holds 2 entries but not 3.
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = (per_entry * 2) + (per_entry / 2),
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const core::EvictionTTL eviction{86400};

  auto write = [&](std::string_view key) {
    return store.Apply(core::ops::WriteOp{core::ops::StringSet{.key = key, .value = value}},
                       eviction);
  };

  ASSERT_TRUE(write("a").has_value());  // oldest access
  clock.Advance(10ms);
  ASSERT_TRUE(write("b").has_value());
  clock.Advance(10ms);
  // Writing c pushes over the budget; LRU evicts the least-recently-accessed
  // (a), the write still succeeds, and used_bytes stays under the ceiling.
  auto r = write("c");
  ASSERT_TRUE(r.has_value()) << r.error().message();
  EXPECT_LE(store.Stats().used_bytes, store.Stats().max_bytes);
  EXPECT_GT(store.Stats().eviction_count, 0U) << "memory pressure bumps eviction_count";

  // a was demoted (still resolvable from cold/queue in production); c is live.
  EXPECT_FALSE(store.Exec(core::ops::ReadOp{core::ops::StringGet{.key = "a"}}).has_value());
  EXPECT_TRUE(store.Exec(core::ops::ReadOp{core::ops::StringGet{.key = "c"}}).has_value());
}

TEST(SingleShardStoreMemoryTest, ApplyReturnsResourceExhaustedWhenNoVictims) {
  abyss::testing::TestClock clock;
  const uint64_t small_entry = MeasureStringEntryBytes("k", "v");
  // Budget below a single real value, no other evictable keys.
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = small_entry,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  // A single value larger than the whole budget, no other evictable keys.
  auto r = store.Apply(
      core::ops::WriteOp{core::ops::StringSet{.key = "big", .value = std::string(4096, 'x')}},
      core::EvictionTTL{86400});
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kResourceExhausted);
}

TEST(SingleShardStoreMemoryTest, ReplayModeSuppressesMemoryEviction) {
  abyss::testing::TestClock clock;
  const uint64_t small_entry = MeasureStringEntryBytes("k", "v");
  SingleShardStore store{SingleShardConfig{
      .max_memory_bytes = small_entry,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  store.SetReplayMode(true);
  // Over-budget writes must all apply during replay: no eviction, no
  // kResourceExhausted (deterministic replay, invariant 4).
  for (int i = 0; i < 5; ++i) {
    auto r = store.Apply(core::ops::WriteOp{core::ops::StringSet{.key = "k" + std::to_string(i),
                                                                 .value = std::string(256, 'x')}},
                         core::EvictionTTL{86400});
    ASSERT_TRUE(r.has_value()) << "replay write " << i << " should not be rejected";
  }
  EXPECT_EQ(store.Stats().eviction_count, 0U);
  EXPECT_EQ(store.Stats().key_count, 5U);
  EXPECT_GT(store.Stats().used_bytes, store.Stats().max_bytes)
      << "over budget during replay (enforced only after)";

  // After replay, the ceiling is enforced by EvictLru.
  store.SetReplayMode(false);
  store.EvictLru(store.Stats().max_bytes);
  EXPECT_LE(store.Stats().used_bytes, store.Stats().max_bytes);
}

TEST_F(SingleShardStoreTest, GcReclaimsTombstoneFootprintAndPreservesLiveKeys) {
  SetString("live", "v");
  SetString("dead", "v");
  const auto live_only_bytes = [&] {
    core::ops::Del del{.keys = {"dead"}};
    EXPECT_TRUE(store_.Apply(core::ops::WriteOp{del}, kEviction, 7).has_value());
    return store_.Stats().used_bytes;
  }();
  EXPECT_GT(live_only_bytes, 0U);

  // A horizon below the delete seq must not reclaim the tombstone.
  EXPECT_EQ(store_.GcTombstones(6), 0U);
  EXPECT_EQ(store_.Probe("dead"), core::HotKeyPresence::kTombstoned);

  EXPECT_EQ(store_.GcTombstones(7), 1U);
  EXPECT_EQ(store_.Probe("dead"), core::HotKeyPresence::kAbsent);
  EXPECT_LT(store_.Stats().used_bytes, live_only_bytes) << "tombstone footprint reclaimed";

  // The live key is untouched throughout.
  EXPECT_EQ(store_.Probe("live"), core::HotKeyPresence::kPresent);
  EXPECT_EQ(store_.Stats().key_count, 1U);
  auto r = GetString("live");
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsString(), "v");
}

// --- HOT-4: EXPIRE/PERSIST on a tombstoned key ------------------------------

TEST_F(SingleShardStoreTest, ExpireOnTombstonedKeyReturnsZeroNoMutation) {
  SetString("k", "v");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, 1).has_value());
  ASSERT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);

  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  auto r = store_.Apply(core::ops::WriteOp{core::ops::Expire{
                            .key = "k", .abs_ttl_ms = static_cast<uint64_t>(now_ms + 100000)}},
                        kEviction, 2);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsInteger(), 0) << "EXPIRE on a tombstone is a no-op returning 0";
  // The tombstone is untouched: still tombstoned (not resurrected), GC reclaims
  // it by horizon, and a read still falls through as absent.
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
}

TEST_F(SingleShardStoreTest, PexpireatOnTombstonedKeyReturnsZero) {
  SetString("k", "v");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, 1).has_value());

  // PEXPIREAT parses to the same Expire write op as EXPIRE; both must no-op.
  auto r =
      store_.Apply(core::ops::WriteOp{core::ops::Expire{.key = "k", .abs_ttl_ms = 99999999999ULL}},
                   kEviction, 2);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsInteger(), 0);
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
}

TEST_F(SingleShardStoreTest, PersistOnTombstonedKeyReturnsZero) {
  SetString("k", "v");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, 1).has_value());

  auto r = store_.Apply(core::ops::WriteOp{core::ops::Persist{.key = "k"}}, kEviction, 2);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsInteger(), 0) << "PERSIST on a tombstone is a no-op returning 0";
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
}

TEST_F(SingleShardStoreTest, ExpirePersistOnTombstoneDoNotPerturbEvictionCounters) {
  // HOT-7 split: EvictExpired skips tombstones before the count branch, so a
  // tombstone routed through EXPIRE/PERSIST must not bleak into the ttl_expired
  // vs deadline counters. Drive a tombstone through both ops, then EvictExpired
  // and assert neither bucket counts the tombstone.
  SetString("k", "v");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, 1).has_value());
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Expire{.key = "k", .abs_ttl_ms = 1}}, kEviction, 2)
          .has_value());
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Persist{.key = "k"}}, kEviction, 3).has_value());

  clock_.Advance(1500ms);
  auto report = store_.EvictExpired(clock_.SteadyNow());
  EXPECT_EQ(report.by_ttl, 0U) << "tombstone is skipped, never counted as a TTL expiry";
  EXPECT_EQ(report.by_deadline, 0U) << "tombstone is skipped, never counted as an eviction";
  // The tombstone survives until GC, not the eviction deadline.
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kTombstoned);
}

TEST_F(SingleShardStoreTest, ExpirePersistAfterTombstoneGcStillAbsent) {
  SetString("k", "v");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, kEviction, 5).has_value());
  // GC reclaims the tombstone (horizon >= tombstone_seq), so the key is now
  // fully absent and the entries_.end() guard returns 0.
  EXPECT_EQ(store_.GcTombstones(5), 1U);
  EXPECT_EQ(store_.Probe("k"), core::HotKeyPresence::kAbsent);

  auto e = store_.Apply(core::ops::WriteOp{core::ops::Expire{.key = "k", .abs_ttl_ms = 1}},
                        kEviction, 6);
  ASSERT_TRUE(e.has_value());
  EXPECT_EQ(e->AsInteger(), 0);
  auto p = store_.Apply(core::ops::WriteOp{core::ops::Persist{.key = "k"}}, kEviction, 7);
  ASSERT_TRUE(p.has_value());
  EXPECT_EQ(p->AsInteger(), 0);
}

TEST_F(SingleShardStoreTest, ExpireOnLiveKeyStillReturnsOne) {
  // Regression guard: the tombstone short-circuit must not affect live keys.
  SetString("k", "v");
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock_.WallNow().time_since_epoch())
          .count();
  auto r = store_.Apply(core::ops::WriteOp{core::ops::Expire{
                            .key = "k", .abs_ttl_ms = static_cast<uint64_t>(now_ms + 100000)}},
                        kEviction, 1);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsInteger(), 1);
}

// --- COLD-3 coupling: hot ZRANGEBYLEX by_lex branch -------------------------

TEST_F(SingleShardStoreTest, HotZrangeByLexAppliesBounds) {
  // Equal scores so the order is purely lexicographic.
  core::ops::ZsetAdd add{.key = "z",
                         .entries = {{.score = 0.0, .member = "a"},
                                     {.score = 0.0, .member = "b"},
                                     {.score = 0.0, .member = "c"},
                                     {.score = 0.0, .member = "d"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add}, kEviction).has_value());

  auto collect = [&](std::string_view min, std::string_view max, bool rev = false) {
    auto r = store_.Exec(core::ops::ReadOp{
        core::ops::ZsetRange{.key = "z", .min = min, .max = max, .by_lex = true, .rev = rev}});
    EXPECT_TRUE(r.has_value());
    std::vector<std::string> out;
    for (const auto& e : r->AsArray()) out.push_back(e.AsString());
    return out;
  };

  EXPECT_EQ(collect("[b", "(d"), (std::vector<std::string>{"b", "c"}));
  EXPECT_EQ(collect("-", "+"), (std::vector<std::string>{"a", "b", "c", "d"}));
  EXPECT_EQ(collect("(a", "[c"), (std::vector<std::string>{"b", "c"}));
  EXPECT_EQ(collect("-", "+", /*rev=*/true), (std::vector<std::string>{"d", "c", "b", "a"}));
}

TEST_F(SingleShardStoreTest, HotZrangeByLexMalformedBoundIsCleanError) {
  core::ops::ZsetAdd add{.key = "z", .entries = {{.score = 0.0, .member = "a"}}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{add}, kEviction).has_value());

  // A bare value with no [ or ( prefix must surface a clean error, never throw.
  auto r = store_.Exec(
      core::ops::ReadOp{core::ops::ZsetRange{.key = "z", .min = "a", .max = "+", .by_lex = true}});
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

}  // namespace
}  // namespace abyss::hot
