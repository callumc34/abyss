#include <gtest/gtest.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::cold::backends {
namespace {

class CollectionsFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<int> counter{0};
    auto base = std::filesystem::temp_directory_path();
#ifdef _WIN32
    path_ = base / ("abyss_cold_collections_test_" + std::to_string(GetCurrentProcessId()) + "_" +
                    std::to_string(counter.fetch_add(1)));
#else
    path_ = base / ("abyss_cold_collections_test_" + std::to_string(getpid()) + "_" +
                    std::to_string(counter.fetch_add(1)));
#endif
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  std::unique_ptr<RocksdbStore> OpenStore() {
    RocksdbConfig config;
    config.data_path = path_.string();
    auto store = RocksdbStore::Create(config);
    EXPECT_TRUE(store.has_value()) << (store.has_value() ? "" : store.error().message());
    return std::move(*store);
  }

  static std::vector<std::string> ArrayToStrings(const core::RespValue& v) {
    std::vector<std::string> out;
    for (const auto& e : v.AsArray()) {
      out.push_back(e.AsString());
    }
    std::ranges::sort(out);
    return out;
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::filesystem::path path_;
};

// --- SET round-trip ---------------------------------------------------------

TEST_F(CollectionsFixture, SetAddThenMembersReturnsAll) {
  auto store = OpenStore();
  std::string key = "myset";
  std::vector<std::string> members = {"a", "b", "c"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto result = store->Exec(core::ops::SetMembers{.key = key});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(ArrayToStrings(*result), (std::vector<std::string>{"a", "b", "c"}));

  auto card = store->Exec(core::ops::SetCard{.key = key});
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 3);
}

TEST_F(CollectionsFixture, SetIsMemberReturnsCorrectly) {
  auto store = OpenStore();
  std::string key = "s";
  std::vector<std::string> members = {"present"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto hit = store->Exec(core::ops::SetIsMember{.key = key, .member = "present"});
  ASSERT_TRUE(hit.has_value());
  EXPECT_EQ(hit->AsInteger(), 1);

  auto miss = store->Exec(core::ops::SetIsMember{.key = key, .member = "absent"});
  ASSERT_TRUE(miss.has_value());
  EXPECT_EQ(miss->AsInteger(), 0);
}

TEST_F(CollectionsFixture, SetRemOnNonMemberIsNoop) {
  auto store = OpenStore();
  std::string key = "s";
  std::vector<std::string> members = {"a", "b"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  std::vector<std::string_view> rem = {"nope"};
  std::vector<core::ops::WriteOp> rem_ops = {core::ops::SetRem{.key = key, .members = rem}};
  ASSERT_TRUE(store->ApplyBatch(rem_ops, 0).has_value());

  auto card = store->Exec(core::ops::SetCard{.key = key});
  EXPECT_EQ(card->AsInteger(), 2);
}

TEST_F(CollectionsFixture, SetRemLastMemberDeletesMeta) {
  auto store = OpenStore();
  std::string key = "s";
  std::vector<std::string> members = {"only"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  std::vector<std::string_view> rem = {"only"};
  std::vector<core::ops::WriteOp> rem_ops = {core::ops::SetRem{.key = key, .members = rem}};
  ASSERT_TRUE(store->ApplyBatch(rem_ops, 0).has_value());

  auto card = store->Exec(core::ops::SetCard{.key = key});
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 0);
  auto members_result = store->Exec(core::ops::SetMembers{.key = key});
  ASSERT_TRUE(members_result.has_value());
  EXPECT_TRUE(members_result->IsArray());
  EXPECT_TRUE(members_result->AsArray().empty());
}

TEST_F(CollectionsFixture, SetAddDuplicatesDoNotBumpCardinality) {
  auto store = OpenStore();
  std::string key = "s";
  std::vector<std::string> first = {"a", "b"};
  std::vector<std::string> second = {"a", "c"};  // "a" is already present
  std::vector<std::string_view> v1(first.begin(), first.end());
  std::vector<std::string_view> v2(second.begin(), second.end());
  std::vector<core::ops::WriteOp> ops1 = {core::ops::SetAdd{.key = key, .members = v1}};
  std::vector<core::ops::WriteOp> ops2 = {core::ops::SetAdd{.key = key, .members = v2}};
  ASSERT_TRUE(store->ApplyBatch(ops1, 0).has_value());
  ASSERT_TRUE(store->ApplyBatch(ops2, 0).has_value());

  auto card = store->Exec(core::ops::SetCard{.key = key});
  EXPECT_EQ(card->AsInteger(), 3);
}

// --- HASH round-trip --------------------------------------------------------

TEST_F(CollectionsFixture, HashSetThenGetAllReturnsPairs) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {
      {.field = "f1", .value = "v1"},
      {.field = "f2", .value = "v2"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto result = store->Exec(core::ops::HashGetAll{.key = key});
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  ASSERT_EQ(result->AsArray().size(), 4U);
}

TEST_F(CollectionsFixture, HashGetFieldOrNull) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "present", .value = "yes"}};
  std::vector<core::ops::WriteOp> ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto hit = store->Exec(core::ops::HashGet{.key = key, .field = "present"});
  ASSERT_TRUE(hit.has_value());
  EXPECT_EQ(hit->AsString(), "yes");

  auto miss = store->Exec(core::ops::HashGet{.key = key, .field = "absent"});
  ASSERT_TRUE(miss.has_value());
  EXPECT_TRUE(miss->IsNull());
}

TEST_F(CollectionsFixture, HashSetOverwritesFieldWithoutCardinalityBump) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs1 = {{.field = "f", .value = "v1"}};
  std::vector<core::ops::HashSet::FieldValue> fvs2 = {{.field = "f", .value = "v2"}};
  std::vector<core::ops::WriteOp> ops1 = {core::ops::HashSet{.key = key, .fields = fvs1}};
  std::vector<core::ops::WriteOp> ops2 = {core::ops::HashSet{.key = key, .fields = fvs2}};
  ASSERT_TRUE(store->ApplyBatch(ops1, 0).has_value());
  ASSERT_TRUE(store->ApplyBatch(ops2, 0).has_value());

  auto get = store->Exec(core::ops::HashGet{.key = key, .field = "f"});
  EXPECT_EQ(get->AsString(), "v2");
  auto all = store->Exec(core::ops::HashGetAll{.key = key});
  EXPECT_EQ(all->AsArray().size(), 2U);  // one pair
}

TEST_F(CollectionsFixture, HashMSetRoundTripsLikeHashSet) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {
      {.field = "a", .value = "1"},
      {.field = "b", .value = "2"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::HashMSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto all = store->Exec(core::ops::HashGetAll{.key = key});
  ASSERT_TRUE(all.has_value());
  ASSERT_EQ(all->AsArray().size(), 4U);
}

TEST_F(CollectionsFixture, HashLenReturnsCachedCardinality) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {
      {.field = "a", .value = "1"},
      {.field = "b", .value = "2"},
      {.field = "c", .value = "3"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto len = store->Exec(core::ops::HashLen{.key = key});
  ASSERT_TRUE(len.has_value());
  EXPECT_EQ(len->AsInteger(), 3);

  auto missing = store->Exec(core::ops::HashLen{.key = "nope"});
  ASSERT_TRUE(missing.has_value());
  EXPECT_EQ(missing->AsInteger(), 0);
}

TEST_F(CollectionsFixture, HashKeysAndValsReturnProjections) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {
      {.field = "a", .value = "1"},
      {.field = "b", .value = "2"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto keys = store->Exec(core::ops::HashKeys{.key = key});
  ASSERT_TRUE(keys.has_value());
  EXPECT_EQ(ArrayToStrings(*keys), (std::vector<std::string>{"a", "b"}));

  auto vals = store->Exec(core::ops::HashVals{.key = key});
  ASSERT_TRUE(vals.has_value());
  EXPECT_EQ(ArrayToStrings(*vals), (std::vector<std::string>{"1", "2"}));

  auto empty_keys = store->Exec(core::ops::HashKeys{.key = "nope"});
  ASSERT_TRUE(empty_keys.has_value());
  EXPECT_TRUE(empty_keys->AsArray().empty());
}

TEST_F(CollectionsFixture, HashMultiGetReturnsArrayWithNulls) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {
      {.field = "a", .value = "1"},
      {.field = "b", .value = "2"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  std::vector<std::string_view> req = {"a", "missing", "b"};
  auto r = store->Exec(core::ops::HashMultiGet{.key = key, .fields = req});
  ASSERT_TRUE(r.has_value());
  ASSERT_TRUE(r->IsArray());
  const auto& arr = r->AsArray();
  ASSERT_EQ(arr.size(), 3U);
  EXPECT_EQ(arr[0].AsString(), "1");
  EXPECT_TRUE(arr[1].IsNull());
  EXPECT_EQ(arr[2].AsString(), "2");

  // Missing-key short-circuits with all nulls.
  std::vector<std::string_view> two_fields = {"x", "y"};
  auto miss = store->Exec(core::ops::HashMultiGet{.key = "nope", .fields = two_fields});
  ASSERT_TRUE(miss.has_value());
  ASSERT_EQ(miss->AsArray().size(), 2U);
  EXPECT_TRUE(miss->AsArray()[0].IsNull());
  EXPECT_TRUE(miss->AsArray()[1].IsNull());
}

TEST_F(CollectionsFixture, HashFieldExistsReturns1Or0) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "a", .value = "1"}};
  std::vector<core::ops::WriteOp> ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  EXPECT_EQ(store->Exec(core::ops::HashFieldExists{.key = key, .field = "a"})->AsInteger(), 1);
  EXPECT_EQ(store->Exec(core::ops::HashFieldExists{.key = key, .field = "missing"})->AsInteger(),
            0);
  EXPECT_EQ(store->Exec(core::ops::HashFieldExists{.key = "no_key", .field = "a"})->AsInteger(), 0);
}

TEST_F(CollectionsFixture, HashDelRemovesOnlyNamedFields) {
  auto store = OpenStore();
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {
      {.field = "a", .value = "1"},
      {.field = "b", .value = "2"},
      {.field = "c", .value = "3"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  std::vector<std::string_view> to_del = {"a", "c", "nope"};
  std::vector<core::ops::WriteOp> del_ops = {core::ops::HashDel{.key = key, .fields = to_del}};
  ASSERT_TRUE(store->ApplyBatch(del_ops, 0).has_value());

  auto all = store->Exec(core::ops::HashGetAll{.key = key});
  ASSERT_EQ(all->AsArray().size(), 2U);
  EXPECT_EQ(all->AsArray()[0].AsString(), "b");
  EXPECT_EQ(all->AsArray()[1].AsString(), "2");
}

// --- ZSET round-trip --------------------------------------------------------

TEST_F(CollectionsFixture, ZsetAddThenScore) {
  auto store = OpenStore();
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> entries = {
      {.score = 1.5, .member = "m1"},
      {.score = 2.0, .member = "m2"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto s1 = store->Exec(core::ops::ZsetScore{.key = key, .member = "m1"});
  ASSERT_TRUE(s1.has_value());
  EXPECT_EQ(s1->AsString(), "1.5");

  auto s2 = store->Exec(core::ops::ZsetScore{.key = key, .member = "m2"});
  EXPECT_EQ(s2->AsString(), "2");

  auto card = store->Exec(core::ops::ZsetCard{.key = key});
  EXPECT_EQ(card->AsInteger(), 2);
}

TEST_F(CollectionsFixture, ZsetAddOverwriteReplacesScoreIndex) {
  auto store = OpenStore();
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> e1 = {{.score = 1.0, .member = "m"}};
  std::vector<core::ops::ZsetAdd::Entry> e2 = {{.score = 99.0, .member = "m"}};
  std::vector<core::ops::WriteOp> ops1 = {core::ops::ZsetAdd{.key = key, .entries = e1}};
  std::vector<core::ops::WriteOp> ops2 = {core::ops::ZsetAdd{.key = key, .entries = e2}};
  ASSERT_TRUE(store->ApplyBatch(ops1, 0).has_value());
  ASSERT_TRUE(store->ApplyBatch(ops2, 0).has_value());

  auto score = store->Exec(core::ops::ZsetScore{.key = key, .member = "m"});
  EXPECT_EQ(score->AsString(), "99");

  // Range BY score over [0, 10] must no longer see the old score 1.0 entry.
  auto range =
      store->Exec(core::ops::ZsetRange{.key = key, .min = "0", .max = "10", .by_score = true});
  ASSERT_TRUE(range.has_value());
  EXPECT_TRUE(range->AsArray().empty());

  auto range2 =
      store->Exec(core::ops::ZsetRange{.key = key, .min = "0", .max = "100", .by_score = true});
  ASSERT_EQ(range2->AsArray().size(), 1U);
  EXPECT_EQ(range2->AsArray()[0].AsString(), "m");
}

TEST_F(CollectionsFixture, ZsetRangeByScoreExclusiveBounds) {
  auto store = OpenStore();
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> entries = {
      {.score = 1.0, .member = "a"},
      {.score = 2.0, .member = "b"},
      {.score = 3.0, .member = "c"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto exclusive_min =
      store->Exec(core::ops::ZsetRange{.key = key, .min = "(1", .max = "3", .by_score = true});
  ASSERT_EQ(exclusive_min->AsArray().size(), 2U);
  EXPECT_EQ(exclusive_min->AsArray()[0].AsString(), "b");
  EXPECT_EQ(exclusive_min->AsArray()[1].AsString(), "c");

  auto exclusive_both =
      store->Exec(core::ops::ZsetRange{.key = key, .min = "(1", .max = "(3", .by_score = true});
  ASSERT_EQ(exclusive_both->AsArray().size(), 1U);
  EXPECT_EQ(exclusive_both->AsArray()[0].AsString(), "b");
}

TEST_F(CollectionsFixture, ZsetRangeByScoreWithScoresAndRev) {
  auto store = OpenStore();
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> entries = {
      {.score = 1.0, .member = "a"},
      {.score = 2.0, .member = "b"},
      {.score = 3.0, .member = "c"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto result = store->Exec(core::ops::ZsetRange{.key = key,
                                                 .min = "-inf",
                                                 .max = "+inf",
                                                 .by_score = true,
                                                 .rev = true,
                                                 .with_scores = true});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->AsArray().size(), 6U);
  EXPECT_EQ(result->AsArray()[0].AsString(), "c");
  EXPECT_EQ(result->AsArray()[1].AsString(), "3");
  EXPECT_EQ(result->AsArray()[2].AsString(), "b");
  EXPECT_EQ(result->AsArray()[3].AsString(), "2");
  EXPECT_EQ(result->AsArray()[4].AsString(), "a");
  EXPECT_EQ(result->AsArray()[5].AsString(), "1");
}

TEST_F(CollectionsFixture, ZsetRangeByScoreOffsetCount) {
  auto store = OpenStore();
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> entries = {
      {.score = 1.0, .member = "a"}, {.score = 2.0, .member = "b"}, {.score = 3.0, .member = "c"},
      {.score = 4.0, .member = "d"}, {.score = 5.0, .member = "e"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto result = store->Exec(core::ops::ZsetRange{
      .key = key, .min = "-inf", .max = "+inf", .by_score = true, .offset = 1, .count = 2});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->AsArray().size(), 2U);
  EXPECT_EQ(result->AsArray()[0].AsString(), "b");
  EXPECT_EQ(result->AsArray()[1].AsString(), "c");
}

TEST_F(CollectionsFixture, ZsetRangeIndexBased) {
  auto store = OpenStore();
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> entries = {
      {.score = 1.0, .member = "a"},
      {.score = 2.0, .member = "b"},
      {.score = 3.0, .member = "c"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  // ZRANGE z 0 -1 — all members in score order.
  auto all = store->Exec(core::ops::ZsetRange{.key = key, .min = "0", .max = "-1"});
  ASSERT_EQ(all->AsArray().size(), 3U);
  EXPECT_EQ(all->AsArray()[0].AsString(), "a");
  EXPECT_EQ(all->AsArray()[1].AsString(), "b");
  EXPECT_EQ(all->AsArray()[2].AsString(), "c");

  // Slice [1..2].
  auto slice = store->Exec(core::ops::ZsetRange{.key = key, .min = "1", .max = "2"});
  ASSERT_EQ(slice->AsArray().size(), 2U);
  EXPECT_EQ(slice->AsArray()[0].AsString(), "b");
  EXPECT_EQ(slice->AsArray()[1].AsString(), "c");
}

TEST_F(CollectionsFixture, ZsetRemRemovesScoreIndex) {
  auto store = OpenStore();
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> entries = {
      {.score = 1.0, .member = "a"},
      {.score = 2.0, .member = "b"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  std::vector<std::string_view> rem = {"a"};
  std::vector<core::ops::WriteOp> rem_ops = {core::ops::ZsetRem{.key = key, .members = rem}};
  ASSERT_TRUE(store->ApplyBatch(rem_ops, 0).has_value());

  auto card = store->Exec(core::ops::ZsetCard{.key = key});
  EXPECT_EQ(card->AsInteger(), 1);
  auto score = store->Exec(core::ops::ZsetScore{.key = key, .member = "a"});
  EXPECT_TRUE(score->IsNull());

  auto full_range =
      store->Exec(core::ops::ZsetRange{.key = key, .min = "-inf", .max = "+inf", .by_score = true});
  ASSERT_EQ(full_range->AsArray().size(), 1U);
  EXPECT_EQ(full_range->AsArray()[0].AsString(), "b");
}

// --- Missing-key semantics --------------------------------------------------

TEST_F(CollectionsFixture, ReadsOnMissingKeyReturnDefault) {
  auto store = OpenStore();
  EXPECT_TRUE(store->Exec(core::ops::SetMembers{.key = "nope"})->AsArray().empty());
  EXPECT_EQ(store->Exec(core::ops::SetCard{.key = "nope"})->AsInteger(), 0);
  EXPECT_EQ(store->Exec(core::ops::SetIsMember{.key = "nope", .member = "x"})->AsInteger(), 0);
  EXPECT_TRUE(store->Exec(core::ops::HashGet{.key = "nope", .field = "f"})->IsNull());
  EXPECT_TRUE(store->Exec(core::ops::HashGetAll{.key = "nope"})->AsArray().empty());
  EXPECT_TRUE(store->Exec(core::ops::ZsetScore{.key = "nope", .member = "m"})->IsNull());
  EXPECT_EQ(store->Exec(core::ops::ZsetCard{.key = "nope"})->AsInteger(), 0);
  EXPECT_TRUE(
      store->Exec(core::ops::ZsetRange{.key = "nope", .min = "0", .max = "-1"})->AsArray().empty());
}

// --- DEL with collections ---------------------------------------------------

TEST_F(CollectionsFixture, DelRemovesCollectionSubKeys) {
  auto store = OpenStore();

  std::vector<std::string> members = {"a", "b", "c"};
  std::vector<std::string_view> member_views(members.begin(), members.end());
  std::vector<core::ops::ZsetAdd::Entry> zentries = {
      {.score = 1.0, .member = "x"},
      {.score = 2.0, .member = "y"},
  };
  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "f", .value = "v"}};
  std::vector<core::ops::WriteOp> ops = {
      core::ops::SetAdd{.key = "s", .members = member_views},
      core::ops::ZsetAdd{.key = "z", .entries = zentries},
      core::ops::HashSet{.key = "h", .fields = fvs},
  };
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  core::ops::Del del_op;
  del_op.keys = {"s", "z", "h"};
  auto del = store->ExecDel(del_op);
  ASSERT_TRUE(del.has_value());
  EXPECT_EQ(del->AsInteger(), 3);

  EXPECT_EQ(store->Exec(core::ops::SetCard{.key = "s"})->AsInteger(), 0);
  EXPECT_EQ(store->Exec(core::ops::ZsetCard{.key = "z"})->AsInteger(), 0);
  EXPECT_TRUE(store->Exec(core::ops::HashGetAll{.key = "h"})->AsArray().empty());

  // Range over the zset must find nothing — score index must have been purged.
  auto range =
      store->Exec(core::ops::ZsetRange{.key = "z", .min = "-inf", .max = "+inf", .by_score = true});
  EXPECT_TRUE(range->AsArray().empty());
}

TEST_F(CollectionsFixture, DelCountsStringAndCollectionMix) {
  auto store = OpenStore();
  std::string sv = "string_val";
  std::vector<std::string> members = {"m"};
  std::vector<std::string_view> member_views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {
      core::ops::StringSet{.key = "str", .value = sv},
      core::ops::SetAdd{.key = "set", .members = member_views},
  };
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  core::ops::Del del_op;
  del_op.keys = {"str", "set", "never-set"};
  auto result = store->ExecDel(del_op);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

// --- EXISTS / MGET ----------------------------------------------------------

TEST_F(CollectionsFixture, ExistsCountsStringsAndCollections) {
  auto store = OpenStore();
  std::string sv = "v";
  std::vector<std::string> members = {"m"};
  std::vector<std::string_view> member_views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {
      core::ops::StringSet{.key = "str", .value = sv},
      core::ops::SetAdd{.key = "set", .members = member_views},
  };
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  core::ops::Exists exists_op;
  exists_op.keys = {"str", "set", "missing"};
  auto result = store->Exec(exists_op);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

// --- Mixed batch ------------------------------------------------------------

TEST_F(CollectionsFixture, MixedBatchLandsAllOps) {
  auto store = OpenStore();
  std::string sv = "v";
  std::vector<std::string> members = {"a", "b"};
  std::vector<std::string_view> member_views(members.begin(), members.end());
  std::vector<core::ops::ZsetAdd::Entry> zentries = {{.score = 1.0, .member = "m"}};
  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "f", .value = "fv"}};
  std::vector<core::ops::WriteOp> ops = {
      core::ops::StringSet{.key = "str", .value = sv},
      core::ops::SetAdd{.key = "set", .members = member_views},
      core::ops::ZsetAdd{.key = "zset", .entries = zentries},
      core::ops::HashSet{.key = "hash", .fields = fvs},
  };
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  EXPECT_EQ(store->Exec(core::ops::StringGet{.key = "str"})->AsString(), "v");
  EXPECT_EQ(store->Exec(core::ops::SetCard{.key = "set"})->AsInteger(), 2);
  EXPECT_EQ(store->Exec(core::ops::ZsetScore{.key = "zset", .member = "m"})->AsString(), "1");
  EXPECT_EQ(store->Exec(core::ops::HashGet{.key = "hash", .field = "f"})->AsString(), "fv");
}

// --- COLD-3: ZRANGEBYLEX bounds --------------------------------------------

TEST_F(CollectionsFixture, ZrangeByLexAppliesInclusiveAndExclusiveBounds) {
  auto store = OpenStore();
  std::string key = "z";
  // Equal scores so the order is purely lexicographic (Redis ZRANGEBYLEX).
  std::vector<core::ops::ZsetAdd::Entry> entries = {
      {.score = 0.0, .member = "a"},
      {.score = 0.0, .member = "b"},
      {.score = 0.0, .member = "c"},
      {.score = 0.0, .member = "d"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  // [b (d -> {b, c}.
  auto r1 = store->Exec(core::ops::ZsetRange{.key = key, .min = "[b", .max = "(d", .by_lex = true});
  ASSERT_TRUE(r1.has_value());
  EXPECT_EQ(ArrayToStrings(*r1), (std::vector<std::string>{"b", "c"}));

  // - + -> all.
  auto r2 = store->Exec(core::ops::ZsetRange{.key = key, .min = "-", .max = "+", .by_lex = true});
  ASSERT_TRUE(r2.has_value());
  EXPECT_EQ(ArrayToStrings(*r2), (std::vector<std::string>{"a", "b", "c", "d"}));

  // (a [c -> {b, c}.
  auto r3 = store->Exec(core::ops::ZsetRange{.key = key, .min = "(a", .max = "[c", .by_lex = true});
  ASSERT_TRUE(r3.has_value());
  EXPECT_EQ(ArrayToStrings(*r3), (std::vector<std::string>{"b", "c"}));
}

TEST_F(CollectionsFixture, ZrangeByLexEmptyReversedAndMalformedBounds) {
  auto store = OpenStore();
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> entries = {
      {.score = 0.0, .member = "a"},
      {.score = 0.0, .member = "b"},
      {.score = 0.0, .member = "c"},
      {.score = 0.0, .member = "d"},
  };
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  // min lexically > max -> empty.
  auto empty =
      store->Exec(core::ops::ZsetRange{.key = key, .min = "[d", .max = "[a", .by_lex = true});
  ASSERT_TRUE(empty.has_value());
  EXPECT_TRUE(empty->AsArray().empty());

  // rev reverses the lex slice.
  auto rev = store->Exec(
      core::ops::ZsetRange{.key = key, .min = "-", .max = "+", .by_lex = true, .rev = true});
  ASSERT_TRUE(rev.has_value());
  ASSERT_EQ(rev->AsArray().size(), 4U);
  EXPECT_EQ(rev->AsArray()[0].AsString(), "d");
  EXPECT_EQ(rev->AsArray()[3].AsString(), "a");

  // offset/count narrow within the lex slice.
  auto narrowed = store->Exec(core::ops::ZsetRange{
      .key = key, .min = "-", .max = "+", .by_lex = true, .offset = 1, .count = 2});
  ASSERT_TRUE(narrowed.has_value());
  ASSERT_EQ(narrowed->AsArray().size(), 2U);
  EXPECT_EQ(narrowed->AsArray()[0].AsString(), "b");
  EXPECT_EQ(narrowed->AsArray()[1].AsString(), "c");

  // A bare value with no [ or ( prefix is a clean error, never a throw.
  auto bad = store->Exec(core::ops::ZsetRange{.key = key, .min = "b", .max = "+", .by_lex = true});
  ASSERT_FALSE(bad.has_value());
  EXPECT_EQ(bad.error().code(), core::ErrorCode::kInvalidArgument);
}

// --- COLD-2: scan deadline --------------------------------------------------

TEST_F(CollectionsFixture, ScanHandlersHonorGenerousDeadline) {
  auto store = OpenStore();
  std::string key = "s";
  std::vector<std::string> members = {"a", "b", "c"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  // A generous deadline returns the full result.
  auto full = store->Exec(core::ops::SetMembers{.key = key}, std::chrono::milliseconds{1000});
  ASSERT_TRUE(full.has_value());
  EXPECT_EQ(ArrayToStrings(*full), (std::vector<std::string>{"a", "b", "c"}));
}

TEST_F(CollectionsFixture, LargeCollectionScanAbortsOnZeroDeadline) {
  auto store = OpenStore();
  std::string key = "big";
  // A large set so the scan does real iterator work and the elapsed deadline is
  // checked at iterator-step granularity.
  std::vector<std::string> members;
  members.reserve(5000);
  for (int i = 0; i < 5000; ++i) members.push_back("member_" + std::to_string(i));
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  // An already-elapsed deadline (zero) fails closed at the Exec guard with
  // kTimeout — the scan is bounded, never unbounded (COLD-2 / invariant 5).
  auto timed_out = store->Exec(core::ops::SetMembers{.key = key}, std::chrono::milliseconds{0});
  ASSERT_FALSE(timed_out.has_value());
  EXPECT_EQ(timed_out.error().code(), core::ErrorCode::kTimeout);
}

// --- Cross-window type change drops stale slices (COLDC-6) -------------------
//
// Each test establishes a key as one type in one ApplyBatch, then re-establishes
// it as a different type in a SEPARATE ApplyBatch — the cold analogue of two
// compaction windows where the later window's CompactedState emits no leading
// Del because it has no in-window signal that cold already holds the prior type.
// The store must drop the prior type's slices on the type-establishing apply, or
// the prior type resurrects on read.

TEST_F(CollectionsFixture, HashThenStringDropsStaleHash) {
  auto store = OpenStore();
  std::string key = "k";

  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "f", .value = "hv"}};
  std::vector<core::ops::WriteOp> hash_ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(hash_ops, 0).has_value());

  std::vector<core::ops::WriteOp> str_ops = {core::ops::StringSet{.key = key, .value = "sv"}};
  ASSERT_TRUE(store->ApplyBatch(str_ops, 0).has_value());

  auto get = store->Exec(core::ops::StringGet{.key = key});
  ASSERT_TRUE(get.has_value());
  EXPECT_EQ(get->AsString(), "sv");

  // No stale hash fields: HLEN/HGETALL must see nothing — but the key is now a
  // string, so the read-side surfaces WRONGTYPE rather than a resurrected count.
  // A resurrected hash would instead return a positive integer here.
  auto hlen = store->Exec(core::ops::HashLen{.key = key});
  ASSERT_FALSE(hlen.has_value()) << "stale hash resurrected after SET";
  EXPECT_EQ(hlen.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(CollectionsFixture, StringThenHashDropsStaleString) {
  auto store = OpenStore();
  std::string key = "k";

  std::vector<core::ops::WriteOp> str_ops = {core::ops::StringSet{.key = key, .value = "sv"}};
  ASSERT_TRUE(store->ApplyBatch(str_ops, 0).has_value());

  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "f", .value = "hv"}};
  std::vector<core::ops::WriteOp> hash_ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(hash_ops, 0).has_value());

  auto hget = store->Exec(core::ops::HashGet{.key = key, .field = "f"});
  ASSERT_TRUE(hget.has_value());
  EXPECT_EQ(hget->AsString(), "hv");

  // The string slice must be gone; GET on the now-hash key sees no resurrected
  // string. (GET returns null for a non-string key in the cold layer.)
  auto get = store->Exec(core::ops::StringGet{.key = key});
  ASSERT_TRUE(get.has_value());
  EXPECT_TRUE(get->IsNull()) << "stale string resurrected after HSET";
}

TEST_F(CollectionsFixture, SetThenZsetDropsStaleSet) {
  auto store = OpenStore();
  std::string key = "k";

  std::vector<std::string> members = {"a", "b"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> set_ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(set_ops, 0).has_value());

  std::vector<core::ops::ZsetAdd::Entry> entries = {{.score = 1.0, .member = "z"}};
  std::vector<core::ops::WriteOp> zset_ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(zset_ops, 0).has_value());

  auto zcard = store->Exec(core::ops::ZsetCard{.key = key});
  ASSERT_TRUE(zcard.has_value());
  EXPECT_EQ(zcard->AsInteger(), 1);

  // The set is now a zset: SCARD must not report the stale set cardinality.
  auto scard = store->Exec(core::ops::SetCard{.key = key});
  ASSERT_FALSE(scard.has_value()) << "stale set resurrected after ZADD";
  EXPECT_EQ(scard.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(CollectionsFixture, StringThenSetDropsStaleString) {
  auto store = OpenStore();
  std::string key = "k";

  std::vector<core::ops::WriteOp> str_ops = {core::ops::StringSet{.key = key, .value = "sv"}};
  ASSERT_TRUE(store->ApplyBatch(str_ops, 0).has_value());

  std::vector<std::string> members = {"x"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> set_ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(set_ops, 0).has_value());

  auto scard = store->Exec(core::ops::SetCard{.key = key});
  ASSERT_TRUE(scard.has_value());
  EXPECT_EQ(scard->AsInteger(), 1);

  auto get = store->Exec(core::ops::StringGet{.key = key});
  ASSERT_TRUE(get.has_value());
  EXPECT_TRUE(get->IsNull()) << "stale string resurrected after SADD";
}

// The zset score index is a separate column family; a set->zset change must not
// leave the prior set's member slices behind to corrupt a later ZRANGE.
TEST_F(CollectionsFixture, ZsetAfterSetHasNoStaleMembers) {
  auto store = OpenStore();
  std::string key = "k";

  std::vector<std::string> members = {"a", "b", "c"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> set_ops = {core::ops::SetAdd{.key = key, .members = views}};
  ASSERT_TRUE(store->ApplyBatch(set_ops, 0).has_value());

  std::vector<core::ops::ZsetAdd::Entry> entries = {{.score = 2.0, .member = "z"}};
  std::vector<core::ops::WriteOp> zset_ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  ASSERT_TRUE(store->ApplyBatch(zset_ops, 0).has_value());

  auto range = store->Exec(core::ops::ZsetRange{.key = key, .min = "0", .max = "-1"});
  ASSERT_TRUE(range.has_value());
  ASSERT_TRUE(range->IsArray());
  ASSERT_EQ(range->AsArray().size(), 1U);
  EXPECT_EQ(range->AsArray()[0].AsString(), "z");
}

// --- Read-side WRONGTYPE for a foreign cold type (COLDC-6 read facet) --------

TEST_F(CollectionsFixture, CollectionReadOnStringKeyReturnsWrongType) {
  auto store = OpenStore();
  std::string key = "k";
  std::vector<core::ops::WriteOp> str_ops = {core::ops::StringSet{.key = key, .value = "sv"}};
  ASSERT_TRUE(store->ApplyBatch(str_ops, 0).has_value());

  auto hlen = store->Exec(core::ops::HashLen{.key = key});
  ASSERT_FALSE(hlen.has_value());
  EXPECT_EQ(hlen.error().code(), core::ErrorCode::kWrongType);

  auto scard = store->Exec(core::ops::SetCard{.key = key});
  ASSERT_FALSE(scard.has_value());
  EXPECT_EQ(scard.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(CollectionsFixture, CollectionReadOnForeignCollectionReturnsWrongType) {
  auto store = OpenStore();
  std::string key = "k";
  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "f", .value = "v"}};
  std::vector<core::ops::WriteOp> hash_ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(hash_ops, 0).has_value());

  auto scard = store->Exec(core::ops::SetCard{.key = key});
  ASSERT_FALSE(scard.has_value());
  EXPECT_EQ(scard.error().code(), core::ErrorCode::kWrongType);

  auto zcard = store->Exec(core::ops::ZsetCard{.key = key});
  ASSERT_FALSE(zcard.has_value());
  EXPECT_EQ(zcard.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(CollectionsFixture, MatchingCollectionReadIsNotWrongType) {
  auto store = OpenStore();
  std::string key = "k";
  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "f", .value = "v"}};
  std::vector<core::ops::WriteOp> hash_ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  ASSERT_TRUE(store->ApplyBatch(hash_ops, 0).has_value());

  // A missing field on the right-typed key is a clean null, never WRONGTYPE.
  auto hget = store->Exec(core::ops::HashGet{.key = key, .field = "absent"});
  ASSERT_TRUE(hget.has_value());
  EXPECT_TRUE(hget->IsNull());

  // A read of an entirely absent key is a clean empty/0, never WRONGTYPE.
  auto absent = store->Exec(core::ops::HashLen{.key = "no_such_key"});
  ASSERT_TRUE(absent.has_value());
  EXPECT_EQ(absent->AsInteger(), 0);
}

}  // namespace
}  // namespace abyss::cold::backends
