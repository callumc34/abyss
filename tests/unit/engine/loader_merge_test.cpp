#include <gtest/gtest.h>

#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/consumer/compacted_state.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/ops.h"
#include "abyss/engine/loader.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::engine {
namespace {

namespace ops = core::ops;
using core::ColdKeyState;
using core::KeyType;
using Members = std::unordered_set<std::string>;
using Fields = std::unordered_map<std::string, std::string>;
using Scores = std::unordered_map<std::string, double>;

ColdKeyState Str(std::string value, int64_t ttl = 0) {
  return {.type = KeyType::kString, .value = std::move(value), .abs_ttl_ms = ttl};
}
ColdKeyState SetOf(Members members, int64_t ttl = 0) {
  return {.type = KeyType::kSet, .value = std::move(members), .abs_ttl_ms = ttl};
}
ColdKeyState HashOf(Fields fields, int64_t ttl = 0) {
  return {.type = KeyType::kHash, .value = std::move(fields), .abs_ttl_ms = ttl};
}
ColdKeyState ZsetOf(Scores scores, int64_t ttl = 0) {
  return {.type = KeyType::kZset, .value = std::move(scores), .abs_ttl_ms = ttl};
}

// A load result in cold's terms, checking the hot value's invariants.
std::optional<ColdKeyState> AsCold(const hot::LoadResult& result) {
  EXPECT_FALSE(std::holds_alternative<hot::LoadedExists>(result)) << "a full load never probes";
  const auto* full = std::get_if<hot::LoadedFull>(&result);
  if (full == nullptr) return std::nullopt;
  EXPECT_EQ(full->bytes, hot::ApproximateBytes(full->value));
  ColdKeyState state{.type = static_cast<KeyType>(full->value.index()),
                     .abs_ttl_ms = full->abs_ttl_ms};
  if (const auto* str = std::get_if<std::string>(&full->value)) {
    state.value = *str;
  } else if (const auto* set = std::get_if<hot::SetValue>(&full->value)) {
    state.value = set->members;
  } else if (const auto* hash = std::get_if<hot::HashValue>(&full->value)) {
    state.value = hash->fields;
  } else if (const auto* zset = std::get_if<hot::ZsetValue>(&full->value)) {
    size_t indexed = 0;
    for (const auto& [score, members] : zset->score_members) {
      for (const auto& member : members) {
        EXPECT_EQ(zset->member_scores.at(member), score) << member;
        ++indexed;
      }
    }
    EXPECT_EQ(indexed, zset->member_scores.size());
    state.value = zset->member_scores;
  }
  return state;
}

struct MergeCase {
  std::string name;
  std::optional<ColdKeyState> base;
  // Absorbed in order; none means no delta.
  std::vector<ops::WriteOp> delta;
  // Nullopt: absent.
  std::optional<ColdKeyState> expected;
};

void PrintTo(const MergeCase& c, std::ostream* os) { *os << c.name; }

class MergeLoadTest : public ::testing::TestWithParam<MergeCase> {};

TEST_P(MergeLoadTest, MergesAsColdWillHoldIt) {
  const MergeCase& c = GetParam();
  std::optional<consumer::CompactedState> delta;
  if (!c.delta.empty()) {
    delta.emplace();
    for (const auto& op : c.delta) delta->Absorb(op);
  }
  const consumer::CompactedState* changes = delta.has_value() ? &*delta : nullptr;

  const auto merged = AsCold(MergeLoad(c.base, changes));
  EXPECT_EQ(merged, c.expected);
  // Idempotent: a base the delta already reached merges to the same.
  EXPECT_EQ(AsCold(MergeLoad(merged, changes)), merged);
}

const std::vector<MergeCase>& Cases() {
  static const std::vector<MergeCase> cases = {
      // --- string ---
      {"StringBaseOnly", Str("v", 5), {}, Str("v", 5)},
      {"StringDeltaOnly",
       std::nullopt,
       {ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = 7}},
       Str("v", 7)},
      {"StringDeltaOverBase",
       Str("old", 5),
       {ops::StringSet{.key = "k", .value = "new"}},
       Str("new")},
      {"StringDelAllThenReAdd",
       Str("old", 5),
       {ops::Del{.keys = {"k"}}, ops::StringSet{.key = "k", .value = "new"}},
       Str("new")},
      {"StringTtlSet", Str("v", 5), {ops::Expire{.key = "k", .abs_ttl_ms = 9}}, Str("v", 9)},
      {"StringTtlCleared", Str("v", 5), {ops::Persist{.key = "k"}}, Str("v")},
      {"StringTtlUnchanged",
       Str("old", 5),
       {ops::StringSet{.key = "k", .value = "new", .abs_ttl_ms = 6}},
       Str("new", 6)},
      {"StringTombstone", Str("v", 5), {ops::Del{.keys = {"k"}}}, std::nullopt},
      {"StringRetypesASet", SetOf({"a"}, 5), {ops::StringSet{.key = "k", .value = "v"}}, Str("v")},
      // --- set ---
      {"SetBaseOnly", SetOf({"a", "b"}, 5), {}, SetOf({"a", "b"}, 5)},
      {"SetDeltaOnly",
       std::nullopt,
       {ops::SetAdd{.key = "k", .members = {"a", "b"}}},
       SetOf({"a", "b"})},
      {"SetDeltaOverBase",
       SetOf({"a", "b"}, 5),
       {ops::SetAdd{.key = "k", .members = {"c"}}, ops::SetRem{.key = "k", .members = {"a"}}},
       SetOf({"b", "c"}, 5)},
      {"SetDelAllThenReAdd",
       SetOf({"a", "b"}, 5),
       {ops::Del{.keys = {"k"}}, ops::SetAdd{.key = "k", .members = {"c"}}},
       SetOf({"c"})},
      {"SetTtlSet",
       SetOf({"a"}, 5),
       {ops::SetAdd{.key = "k", .members = {"b"}}, ops::Expire{.key = "k", .abs_ttl_ms = 9}},
       SetOf({"a", "b"}, 9)},
      {"SetTtlCleared", SetOf({"a"}, 5), {ops::Persist{.key = "k"}}, SetOf({"a"})},
      {"SetTtlUnchanged",
       SetOf({"a"}, 5),
       {ops::SetRem{.key = "k", .members = {"missing"}}},
       SetOf({"a"}, 5)},
      {"SetTombstone", SetOf({"a"}), {ops::Del{.keys = {"k"}}}, std::nullopt},
      {"SetEmptied",
       SetOf({"a", "b"}, 5),
       {ops::SetRem{.key = "k", .members = {"a", "b"}}},
       std::nullopt},
      {"SetAddsRetypeAString",
       Str("v", 5),
       {ops::SetAdd{.key = "k", .members = {"a"}}},
       SetOf({"a"})},
      {"SetRemovalsLeaveAString",
       Str("v", 5),
       {ops::SetRem{.key = "k", .members = {"a"}}},
       Str("v", 5)},
      // --- hash ---
      {"HashBaseOnly", HashOf({{"f", "1"}}, 5), {}, HashOf({{"f", "1"}}, 5)},
      {"HashDeltaOnly",
       std::nullopt,
       {ops::HashSet{.key = "k", .fields = {{.field = "f", .value = "1"}}}},
       HashOf({{"f", "1"}})},
      {"HashDeltaOverBase",
       HashOf({{"f", "1"}, {"g", "2"}}, 5),
       {ops::HashSet{.key = "k",
                     .fields = {{.field = "f", .value = "9"}, {.field = "h", .value = "3"}}},
        ops::HashDel{.key = "k", .fields = {"g"}}},
       HashOf({{"f", "9"}, {"h", "3"}}, 5)},
      {"HashDelAllThenReAdd",
       HashOf({{"f", "1"}}, 5),
       {ops::Del{.keys = {"k"}},
        ops::HashSet{.key = "k", .fields = {{.field = "g", .value = "2"}}}},
       HashOf({{"g", "2"}})},
      {"HashTtlSet",
       HashOf({{"f", "1"}}, 5),
       {ops::Expire{.key = "k", .abs_ttl_ms = 9}},
       HashOf({{"f", "1"}}, 9)},
      {"HashTtlCleared", HashOf({{"f", "1"}}, 5), {ops::Persist{.key = "k"}}, HashOf({{"f", "1"}})},
      {"HashTtlUnchanged",
       HashOf({{"f", "1"}}, 5),
       {ops::HashSet{.key = "k", .fields = {{.field = "f", .value = "2"}}}},
       HashOf({{"f", "2"}}, 5)},
      {"HashTombstone", HashOf({{"f", "1"}}), {ops::Del{.keys = {"k"}}}, std::nullopt},
      {"HashEmptied",
       HashOf({{"f", "1"}}, 5),
       {ops::HashDel{.key = "k", .fields = {"f"}}},
       std::nullopt},
      // --- zset ---
      {"ZsetBaseOnly", ZsetOf({{"m", 1}}, 5), {}, ZsetOf({{"m", 1}}, 5)},
      {"ZsetDeltaOnly",
       std::nullopt,
       {ops::ZsetAdd{.key = "k",
                     .entries = {{.score = 1, .member = "m"}, {.score = 1, .member = "n"}}}},
       ZsetOf({{"m", 1}, {"n", 1}})},
      {"ZsetDeltaOverBase",
       ZsetOf({{"m", 1}, {"n", 2}}, 5),
       {ops::ZsetAdd{.key = "k",
                     .entries = {{.score = 5, .member = "m"}, {.score = 3, .member = "o"}}},
        ops::ZsetRem{.key = "k", .members = {"n"}}},
       ZsetOf({{"m", 5}, {"o", 3}}, 5)},
      {"ZsetDelAllThenReAdd",
       ZsetOf({{"m", 1}}, 5),
       {ops::Del{.keys = {"k"}},
        ops::ZsetAdd{.key = "k", .entries = {{.score = 2, .member = "n"}}}},
       ZsetOf({{"n", 2}})},
      {"ZsetTtlSet",
       ZsetOf({{"m", 1}}, 5),
       {ops::Expire{.key = "k", .abs_ttl_ms = 9}},
       ZsetOf({{"m", 1}}, 9)},
      {"ZsetTtlCleared", ZsetOf({{"m", 1}}, 5), {ops::Persist{.key = "k"}}, ZsetOf({{"m", 1}})},
      {"ZsetTtlUnchanged",
       ZsetOf({{"m", 1}}, 5),
       {ops::ZsetAdd{.key = "k", .entries = {{.score = -4, .member = "m"}}}},
       ZsetOf({{"m", -4}}, 5)},
      {"ZsetTombstone", ZsetOf({{"m", 1}}), {ops::Del{.keys = {"k"}}}, std::nullopt},
      {"ZsetEmptied",
       ZsetOf({{"m", 1}}, 5),
       {ops::ZsetRem{.key = "k", .members = {"m"}}},
       std::nullopt},
      // --- no base ---
      {"Absent", std::nullopt, {}, std::nullopt},
      {"TtlOfAnAbsentKey", std::nullopt, {ops::Expire{.key = "k", .abs_ttl_ms = 9}}, std::nullopt},
      {"RemovalsOfAnAbsentKey",
       std::nullopt,
       {ops::SetRem{.key = "k", .members = {"a"}}},
       std::nullopt},
  };
  return cases;
}

INSTANTIATE_TEST_SUITE_P(Merge, MergeLoadTest, ::testing::ValuesIn(Cases()),
                         [](const ::testing::TestParamInfo<MergeCase>& info) {
                           return info.param.name;
                         });

TEST(MergeLoadTtlTest, AnExpiredTtlIsKept) {
  // Decide logs the expiry; the load reports it as it is.
  const auto merged = AsCold(MergeLoad(SetOf({"a"}, 1), nullptr));
  EXPECT_EQ(merged, SetOf({"a"}, 1));
}

}  // namespace
}  // namespace abyss::engine
