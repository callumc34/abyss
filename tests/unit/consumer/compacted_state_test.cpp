#include "abyss/consumer/compacted_state.h"

#include <gtest/gtest.h>

namespace abyss::consumer {
namespace {

using core::ops::Del;
using core::ops::HashDel;
using core::ops::HashMSet;
using core::ops::HashSet;
using core::ops::SetAdd;
using core::ops::SetRem;
using core::ops::StringSet;
using core::ops::WriteOp;
using core::ops::ZsetAdd;
using core::ops::ZsetRem;

class CompactedStateTest : public ::testing::Test {
 protected:
  CompactedState state_;
};

// --- String operations ---

TEST_F(CompactedStateTest, StringSetEmitsStringSet) {
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kString);
  EXPECT_EQ(state_.StringValue(), "v");
  EXPECT_FALSE(state_.IsTombstone());

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<StringSet>(ops[0]));
  EXPECT_EQ(std::get<StringSet>(ops[0]).value, "v");
}

TEST_F(CompactedStateTest, StringSetLastWriteWins) {
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v1"}});
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v2"}});
  EXPECT_EQ(state_.StringValue(), "v2");

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  EXPECT_EQ(std::get<StringSet>(ops[0]).value, "v2");
}

TEST_F(CompactedStateTest, StringSetPreservesTtl) {
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v", .abs_ttl_ms = 99000}});
  EXPECT_EQ(state_.StringTtlMs(), 99000);

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  EXPECT_EQ(std::get<StringSet>(ops[0]).abs_ttl_ms, 99000);
}

TEST_F(CompactedStateTest, StringSetOverwritesClearsOldTtl) {
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v", .abs_ttl_ms = 99000}});
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v2", .abs_ttl_ms = 0}});
  EXPECT_EQ(state_.StringTtlMs(), 0);
}

// --- DEL operations ---

TEST_F(CompactedStateTest, DelSetsTombstone) {
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});
  state_.Absorb(WriteOp{Del{.keys = {"k"}}});
  EXPECT_TRUE(state_.IsTombstone());
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kNone);
  EXPECT_TRUE(state_.Emit().empty());
}

TEST_F(CompactedStateTest, DelThenStringSetClearsTombstone) {
  state_.Absorb(WriteOp{Del{.keys = {"k"}}});
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "after"}});
  EXPECT_FALSE(state_.IsTombstone());
  EXPECT_EQ(state_.StringValue(), "after");

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  EXPECT_EQ(std::get<StringSet>(ops[0]).value, "after");
}

TEST_F(CompactedStateTest, DelOnFreshStateIsTombstone) {
  state_.Absorb(WriteOp{Del{.keys = {"k"}}});
  EXPECT_TRUE(state_.IsTombstone());
  EXPECT_TRUE(state_.Emit().empty());
}

// --- Set operations ---

TEST_F(CompactedStateTest, SetAddEmitsSetAdd) {
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a", "b", "c"}}});
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kSet);

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<SetAdd>(ops[0]));
  EXPECT_EQ(std::get<SetAdd>(ops[0]).members.size(), 3);
}

TEST_F(CompactedStateTest, SetAddThenRemDisjoint) {
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a", "b", "c"}}});
  state_.Absorb(WriteOp{SetRem{.key = "k", .members = {"d"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  EXPECT_EQ(std::get<SetAdd>(ops[0]).members.size(), 3);
}

TEST_F(CompactedStateTest, SetAddThenRemSameMember) {
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a", "b"}}});
  state_.Absorb(WriteOp{SetRem{.key = "k", .members = {"b"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& members = std::get<SetAdd>(ops[0]).members;
  ASSERT_EQ(members.size(), 1);
  EXPECT_EQ(members[0], "a");
}

TEST_F(CompactedStateTest, SetRemThenAddSameMember) {
  state_.Absorb(WriteOp{SetRem{.key = "k", .members = {"a"}}});
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& members = std::get<SetAdd>(ops[0]).members;
  ASSERT_EQ(members.size(), 1);
  EXPECT_EQ(members[0], "a");
}

TEST_F(CompactedStateTest, SetRemAllMembersEmitsNothing) {
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a"}}});
  state_.Absorb(WriteOp{SetRem{.key = "k", .members = {"a"}}});

  auto ops = state_.Emit();
  EXPECT_TRUE(ops.empty());
}

TEST_F(CompactedStateTest, DelThenSetAddClearsTombstone) {
  state_.Absorb(WriteOp{Del{.keys = {"k"}}});
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"x"}}});
  EXPECT_FALSE(state_.IsTombstone());
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kSet);

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
}

// --- Sorted set operations ---

TEST_F(CompactedStateTest, ZsetAddEmitsZsetAdd) {
  state_.Absorb(WriteOp{ZsetAdd{.key = "k", .entries = {{.score = 1.0, .member = "a"}}}});
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kZset);

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<ZsetAdd>(ops[0]));
  EXPECT_EQ(std::get<ZsetAdd>(ops[0]).entries.size(), 1);
}

TEST_F(CompactedStateTest, ZsetAddSameMemberLatestScoreWins) {
  state_.Absorb(WriteOp{ZsetAdd{.key = "k", .entries = {{.score = 1.0, .member = "a"}}}});
  state_.Absorb(WriteOp{ZsetAdd{.key = "k", .entries = {{.score = 5.0, .member = "a"}}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& entries = std::get<ZsetAdd>(ops[0]).entries;
  ASSERT_EQ(entries.size(), 1);
  EXPECT_DOUBLE_EQ(entries[0].score, 5.0);
}

TEST_F(CompactedStateTest, ZsetAddThenRemRemovesMember) {
  state_.Absorb(WriteOp{ZsetAdd{
      .key = "k", .entries = {{.score = 1.0, .member = "a"}, {.score = 2.0, .member = "b"}}}});
  state_.Absorb(WriteOp{ZsetRem{.key = "k", .members = {"a"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& entries = std::get<ZsetAdd>(ops[0]).entries;
  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].member, "b");
}

TEST_F(CompactedStateTest, ZsetRemThenAddSameMember) {
  state_.Absorb(WriteOp{ZsetRem{.key = "k", .members = {"a"}}});
  state_.Absorb(WriteOp{ZsetAdd{.key = "k", .entries = {{.score = 3.0, .member = "a"}}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& entries = std::get<ZsetAdd>(ops[0]).entries;
  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].member, "a");
  EXPECT_DOUBLE_EQ(entries[0].score, 3.0);
}

// --- Hash operations ---

TEST_F(CompactedStateTest, HashSetEmitsHashSet) {
  state_.Absorb(WriteOp{HashSet{.key = "k", .fields = {{.field = "f1", .value = "v1"}}}});
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kHash);

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<HashSet>(ops[0]));
}

TEST_F(CompactedStateTest, HashSetSameFieldLatestValueWins) {
  state_.Absorb(WriteOp{HashSet{.key = "k", .fields = {{.field = "f", .value = "old"}}}});
  state_.Absorb(WriteOp{HashSet{.key = "k", .fields = {{.field = "f", .value = "new"}}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& fields = std::get<HashSet>(ops[0]).fields;
  ASSERT_EQ(fields.size(), 1);
  EXPECT_EQ(fields[0].value, "new");
}

TEST_F(CompactedStateTest, HashSetThenDelRemovesField) {
  state_.Absorb(WriteOp{HashSet{
      .key = "k", .fields = {{.field = "f1", .value = "v1"}, {.field = "f2", .value = "v2"}}}});
  state_.Absorb(WriteOp{HashDel{.key = "k", .fields = {"f1"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& fields = std::get<HashSet>(ops[0]).fields;
  ASSERT_EQ(fields.size(), 1);
  EXPECT_EQ(fields[0].field, "f2");
}

TEST_F(CompactedStateTest, HashMSetAbsorbsLikeHashSet) {
  state_.Absorb(WriteOp{HashMSet{
      .key = "k", .fields = {{.field = "f1", .value = "v1"}, {.field = "f2", .value = "v2"}}}});
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kHash);
  EXPECT_FALSE(state_.IsTombstone());

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  // Cold flush always emits HashSet — the +OK vs count distinction lives on
  // the hot reply path only.
  ASSERT_TRUE(std::holds_alternative<HashSet>(ops[0]));
  EXPECT_EQ(std::get<HashSet>(ops[0]).fields.size(), 2);
}

TEST_F(CompactedStateTest, HashMSetAndHashSetInterleave) {
  state_.Absorb(WriteOp{HashMSet{.key = "k", .fields = {{.field = "a", .value = "1"}}}});
  state_.Absorb(WriteOp{HashSet{.key = "k", .fields = {{.field = "b", .value = "2"}}}});
  state_.Absorb(WriteOp{HashMSet{.key = "k", .fields = {{.field = "a", .value = "1b"}}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  const auto& fields = std::get<HashSet>(ops[0]).fields;
  ASSERT_EQ(fields.size(), 2);
  // Both fields present, 'a' reflects the later HMSet value.
  bool saw_a = false;
  bool saw_b = false;
  for (const auto& fv : fields) {
    if (fv.field == "a") {
      EXPECT_EQ(fv.value, "1b");
      saw_a = true;
    } else if (fv.field == "b") {
      EXPECT_EQ(fv.value, "2");
      saw_b = true;
    }
  }
  EXPECT_TRUE(saw_a);
  EXPECT_TRUE(saw_b);
}

TEST_F(CompactedStateTest, HashOverlayAccessorsReflectAbsorbs) {
  state_.Absorb(WriteOp{HashSet{
      .key = "k", .fields = {{.field = "keep", .value = "v"}, {.field = "gone", .value = "v"}}}});
  state_.Absorb(WriteOp{HashDel{.key = "k", .fields = {"gone"}}});

  EXPECT_TRUE(state_.HashFields().contains("keep"));
  EXPECT_FALSE(state_.HashFields().contains("gone"));
  EXPECT_TRUE(state_.HashRemovedFields().contains("gone"));
}

TEST_F(CompactedStateTest, HashDelThenSetSameField) {
  state_.Absorb(WriteOp{HashDel{.key = "k", .fields = {"f"}}});
  state_.Absorb(WriteOp{HashSet{.key = "k", .fields = {{.field = "f", .value = "v"}}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& fields = std::get<HashSet>(ops[0]).fields;
  ASSERT_EQ(fields.size(), 1);
  EXPECT_EQ(fields[0].value, "v");
}

// --- Type conflicts ---

TEST_F(CompactedStateTest, SetAddOnStringTypeIgnored) {
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"m"}}});
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kString);
  EXPECT_EQ(state_.StringValue(), "v");
}

TEST_F(CompactedStateTest, StringSetAfterSetAddChangesType) {
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a", "b"}}});
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kString);
  EXPECT_EQ(state_.StringValue(), "v");

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<StringSet>(ops[0]));
}

TEST_F(CompactedStateTest, ZsetAddOnHashTypeIgnored) {
  state_.Absorb(WriteOp{HashSet{.key = "k", .fields = {{.field = "f", .value = "v"}}}});
  state_.Absorb(WriteOp{ZsetAdd{.key = "k", .entries = {{.score = 1.0, .member = "a"}}}});
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kHash);
}

// --- Reset ---

TEST_F(CompactedStateTest, ResetClearsAllState) {
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});
  state_.Reset();
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kNone);
  EXPECT_FALSE(state_.IsTombstone());
  EXPECT_TRUE(state_.Emit().empty());
}

TEST_F(CompactedStateTest, ResetClearsTombstone) {
  state_.Absorb(WriteOp{Del{.keys = {"k"}}});
  state_.Reset();
  EXPECT_FALSE(state_.IsTombstone());
}

// --- Emit edge cases ---

TEST_F(CompactedStateTest, EmitOnFreshStateReturnsEmpty) { EXPECT_TRUE(state_.Emit().empty()); }

TEST_F(CompactedStateTest, EmitOnTombstoneReturnsEmpty) {
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});
  state_.Absorb(WriteOp{Del{.keys = {"k"}}});
  EXPECT_TRUE(state_.Emit().empty());
}

}  // namespace
}  // namespace abyss::consumer
