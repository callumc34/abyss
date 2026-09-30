#include "abyss/consumer/compacted_state.h"

#include <gtest/gtest.h>

#include <string>
#include <variant>

#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"

namespace abyss::consumer {
namespace {

using core::ops::Del;
using core::ops::Expire;
using core::ops::HashDel;
using core::ops::HashMSet;
using core::ops::HashSet;
using core::ops::Persist;
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

  // COLDC-3: a DEL before the re-add emits a leading destructive Del so cold
  // wipes any prior slices for the key before the new string lands.
  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 2);
  ASSERT_TRUE(std::holds_alternative<Del>(ops[0]));
  EXPECT_EQ(std::get<StringSet>(ops[1]).value, "after");
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
  ASSERT_EQ(ops.size(), 2);
  EXPECT_EQ(std::get<SetAdd>(ops[0]).members.size(), 3);
  auto& removed = std::get<SetRem>(ops[1]).members;
  ASSERT_EQ(removed.size(), 1);
  EXPECT_EQ(removed[0], "d");
}

TEST_F(CompactedStateTest, SetAddThenRemSameMember) {
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a", "b"}}});
  state_.Absorb(WriteOp{SetRem{.key = "k", .members = {"b"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 2);
  auto& members = std::get<SetAdd>(ops[0]).members;
  ASSERT_EQ(members.size(), 1);
  EXPECT_EQ(members[0], "a");
  auto& removed = std::get<SetRem>(ops[1]).members;
  ASSERT_EQ(removed.size(), 1);
  EXPECT_EQ(removed[0], "b");
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

TEST_F(CompactedStateTest, SetRemAllMembersStillEmitsRem) {
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a"}}});
  state_.Absorb(WriteOp{SetRem{.key = "k", .members = {"a"}}});

  // The member may exist in cold from an earlier window; the net removal must
  // still be emitted even though the add cancelled within this window.
  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  auto& removed = std::get<SetRem>(ops[0]).members;
  ASSERT_EQ(removed.size(), 1);
  EXPECT_EQ(removed[0], "a");
}

TEST_F(CompactedStateTest, DelThenSetAddClearsTombstone) {
  state_.Absorb(WriteOp{Del{.keys = {"k"}}});
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"x"}}});
  EXPECT_FALSE(state_.IsTombstone());
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kSet);

  // COLDC-3: leading Del clears prior cold members before the re-add.
  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 2);
  ASSERT_TRUE(std::holds_alternative<Del>(ops[0]));
  ASSERT_TRUE(std::holds_alternative<SetAdd>(ops[1]));
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
  ASSERT_EQ(ops.size(), 2);
  auto& entries = std::get<ZsetAdd>(ops[0]).entries;
  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].member, "b");
  auto& removed = std::get<ZsetRem>(ops[1]).members;
  ASSERT_EQ(removed.size(), 1);
  EXPECT_EQ(removed[0], "a");
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
  ASSERT_EQ(ops.size(), 2);
  auto& fields = std::get<HashSet>(ops[0]).fields;
  ASSERT_EQ(fields.size(), 1);
  EXPECT_EQ(fields[0].field, "f2");
  auto& removed = std::get<HashDel>(ops[1]).fields;
  ASSERT_EQ(removed.size(), 1);
  EXPECT_EQ(removed[0], "f1");
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

  // COLDC-3: a type change (set -> string) emits a leading Del so cold drops
  // the prior set's slices before the string Put.
  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 2);
  ASSERT_TRUE(std::holds_alternative<Del>(ops[0]));
  ASSERT_TRUE(std::holds_alternative<StringSet>(ops[1]));
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

// --- Multi-window removals ---
// The state is reset (or freshly created) after every flush, so a removal in a
// later window has no matching addition to cancel against. Emit must still
// surface the removal so cold drops the member flushed in the earlier window.

TEST_F(CompactedStateTest, SetRemOnFreshStateEmitsRem) {
  state_.Absorb(WriteOp{SetRem{.key = "k", .members = {"a", "b"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<SetRem>(ops[0]));
  EXPECT_EQ(std::get<SetRem>(ops[0]).members.size(), 2);
}

TEST_F(CompactedStateTest, ZsetRemOnFreshStateEmitsRem) {
  state_.Absorb(WriteOp{ZsetRem{.key = "k", .members = {"a"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<ZsetRem>(ops[0]));
  ASSERT_EQ(std::get<ZsetRem>(ops[0]).members.size(), 1);
  EXPECT_EQ(std::get<ZsetRem>(ops[0]).members[0], "a");
}

TEST_F(CompactedStateTest, HashDelOnFreshStateEmitsDel) {
  state_.Absorb(WriteOp{HashDel{.key = "k", .fields = {"f"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<HashDel>(ops[0]));
  ASSERT_EQ(std::get<HashDel>(ops[0]).fields.size(), 1);
  EXPECT_EQ(std::get<HashDel>(ops[0]).fields[0], "f");
}

TEST_F(CompactedStateTest, AddThenResetThenRemEmitsRem) {
  state_.Absorb(WriteOp{HashSet{.key = "k", .fields = {{.field = "f", .value = "v"}}}});
  ASSERT_FALSE(state_.Emit().empty());

  state_.Reset();
  state_.Absorb(WriteOp{HashDel{.key = "k", .fields = {"f"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<HashDel>(ops[0]));
  EXPECT_EQ(std::get<HashDel>(ops[0]).fields[0], "f");
}

// --- COLDC-2: EXPIRE/PERSIST-only and TTL-intent windows ---------------------

TEST_F(CompactedStateTest, ExpireOnlyWindowEmitsExpire) {
  // A standalone EXPIRE on a key whose value already lives in cold (fresh,
  // non-tombstone state this window) must still flush the TTL change.
  state_.Absorb(WriteOp{Expire{.key = "k", .abs_ttl_ms = 99000}});
  EXPECT_FALSE(state_.IsTombstone());
  EXPECT_EQ(state_.Type(), CompactedState::DataType::kNone);

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<Expire>(ops[0]));
  EXPECT_EQ(std::get<Expire>(ops[0]).abs_ttl_ms, 99000U);
}

TEST_F(CompactedStateTest, PersistOnlyWindowEmitsPersist) {
  state_.Absorb(WriteOp{Persist{.key = "k"}});
  EXPECT_FALSE(state_.IsTombstone());

  // Cleared semantics, not Expire{0}.
  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<Persist>(ops[0]));
}

TEST_F(CompactedStateTest, SetAddThenPersistEmitsSetAddAndPersist) {
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a", "b"}}});
  state_.Absorb(WriteOp{Persist{.key = "k"}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 2);
  ASSERT_TRUE(std::holds_alternative<SetAdd>(ops[0]));
  // Trailing TTL op is Persist (cleared), not Expire.
  ASSERT_TRUE(std::holds_alternative<Persist>(ops[1]));
}

TEST_F(CompactedStateTest, TtlIntentLastWriteWinsAndStringFolds) {
  // PERSIST then EXPIRE{T} on a collection -> kSetTo wins -> trailing Expire{T}.
  {
    CompactedState s;
    s.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a"}}});
    s.Absorb(WriteOp{Persist{.key = "k"}});
    s.Absorb(WriteOp{Expire{.key = "k", .abs_ttl_ms = 5000}});
    auto ops = s.Emit();
    ASSERT_EQ(ops.size(), 2);
    ASSERT_TRUE(std::holds_alternative<SetAdd>(ops[0]));
    ASSERT_TRUE(std::holds_alternative<Expire>(ops[1]));
    EXPECT_EQ(std::get<Expire>(ops[1]).abs_ttl_ms, 5000U);
  }
  // EXPIRE{T} then PERSIST on a collection -> kCleared wins -> trailing Persist.
  {
    CompactedState s;
    s.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a"}}});
    s.Absorb(WriteOp{Expire{.key = "k", .abs_ttl_ms = 5000}});
    s.Absorb(WriteOp{Persist{.key = "k"}});
    auto ops = s.Emit();
    ASSERT_EQ(ops.size(), 2);
    ASSERT_TRUE(std::holds_alternative<Persist>(ops[1]));
  }
  // For kString the TTL folds into StringSet.abs_ttl_ms; no duplicate trailing op.
  {
    CompactedState s;
    s.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});
    s.Absorb(WriteOp{Expire{.key = "k", .abs_ttl_ms = 7000}});
    auto ops = s.Emit();
    ASSERT_EQ(ops.size(), 1);
    ASSERT_TRUE(std::holds_alternative<StringSet>(ops[0]));
    EXPECT_EQ(std::get<StringSet>(ops[0]).abs_ttl_ms, 7000U);
  }
  // SET EX then PERSIST folds the clear into StringSet (abs_ttl_ms back to 0).
  {
    CompactedState s;
    s.Absorb(WriteOp{StringSet{.key = "k", .value = "v", .abs_ttl_ms = 7000}});
    s.Absorb(WriteOp{Persist{.key = "k"}});
    auto ops = s.Emit();
    ASSERT_EQ(ops.size(), 1);
    ASSERT_TRUE(std::holds_alternative<StringSet>(ops[0]));
    EXPECT_EQ(std::get<StringSet>(ops[0]).abs_ttl_ms, 0U);
  }
}

TEST_F(CompactedStateTest, ExpireThenSetSuppressesTrailingTtlOp) {
  // EXPIRE then SET: the SET resets TTL intent and carries its own (none here),
  // so no stale trailing Expire leaks onto the new string value.
  state_.Absorb(WriteOp{Expire{.key = "k", .abs_ttl_ms = 9000}});
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<StringSet>(ops[0]));
  EXPECT_EQ(std::get<StringSet>(ops[0]).abs_ttl_ms, 0U);
}

// --- COLDC-3: destructive leading Del on DEL-then-readd / type change --------

TEST_F(CompactedStateTest, DelThenSetAddEmitsLeadingDel) {
  state_.Absorb(WriteOp{Del{.keys = {"k"}}});
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"x"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 2);
  ASSERT_TRUE(std::holds_alternative<Del>(ops[0]));
  ASSERT_TRUE(std::holds_alternative<SetAdd>(ops[1]));
  EXPECT_FALSE(state_.IsTombstone());
}

TEST_F(CompactedStateTest, TypeChangeStringOverHashEmitsLeadingDel) {
  state_.Absorb(WriteOp{HashSet{.key = "k", .fields = {{.field = "f", .value = "v"}}}});
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v"}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 2);
  ASSERT_TRUE(std::holds_alternative<Del>(ops[0]));
  ASSERT_TRUE(std::holds_alternative<StringSet>(ops[1]));
}

TEST_F(CompactedStateTest, FreshAddDoesNotEmitLeadingDel) {
  // No prior DEL / type change this window -> no over-emission of a Del on
  // every collection flush (perf + idempotency guard).
  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"x"}}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<SetAdd>(ops[0]));
}

TEST_F(CompactedStateTest, StringOverStringDoesNotEmitLeadingDel) {
  // Same-type overwrite: the Put replaces the prior value in place; no Del.
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v1"}});
  state_.Absorb(WriteOp{StringSet{.key = "k", .value = "v2"}});

  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<StringSet>(ops[0]));
}

// --- Determinism (invariant 4): no second TTL clock --------------------------
// Compaction must reproduce the SAME absolute TTL the hot apply derives from the
// same WAL slice. Both read the parser-computed abs_ttl_ms from the SAME
// ParseWriteOp(appended_at) path; compaction never recomputes relative->absolute.

TEST_F(CompactedStateTest, EmitReproducesParserAbsTtlForCollection) {
  // Hot applies the parser output directly; compaction absorbs the SAME parsed
  // op. Parse a PEXPIREAT with a fixed appended_at so the absolute TTL is
  // unambiguous, then assert Emit carries it through unchanged.
  constexpr uint64_t kAppendedAtMs = 1'700'000'000'000ULL;
  constexpr uint64_t kAbsTtlMs = 1'700'000'099'000ULL;
  core::RespCommand expire_cmd{.args = {"PEXPIREAT", "k", std::to_string(kAbsTtlMs)}};
  auto parsed = core::ops::ParseWriteOp(expire_cmd.Name(), expire_cmd, kAppendedAtMs);
  ASSERT_TRUE(parsed.has_value());
  ASSERT_TRUE(std::holds_alternative<Expire>(*parsed));
  const uint64_t hot_abs_ttl = std::get<Expire>(*parsed).abs_ttl_ms;
  EXPECT_EQ(hot_abs_ttl, kAbsTtlMs);

  state_.Absorb(WriteOp{SetAdd{.key = "k", .members = {"a"}}});
  state_.Absorb(*parsed);
  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 2);
  ASSERT_TRUE(std::holds_alternative<Expire>(ops[1]));
  // The compaction-emitted TTL must equal hot's applied TTL bit-for-bit.
  EXPECT_EQ(std::get<Expire>(ops[1]).abs_ttl_ms, hot_abs_ttl);
}

TEST_F(CompactedStateTest, EmitReproducesParserAbsTtlForStringFold) {
  // For strings the TTL folds into StringSet.abs_ttl_ms; it must still equal the
  // parser-derived absolute TTL (no second clock).
  constexpr uint64_t kAppendedAtMs = 1'700'000'000'000ULL;
  constexpr uint64_t kAbsTtlMs = 1'700'000'042'000ULL;
  core::RespCommand set_cmd{.args = {"SET", "k", "v", "PXAT", std::to_string(kAbsTtlMs)}};
  auto parsed = core::ops::ParseWriteOp(set_cmd.Name(), set_cmd, kAppendedAtMs);
  ASSERT_TRUE(parsed.has_value());
  ASSERT_TRUE(std::holds_alternative<StringSet>(*parsed));
  const uint64_t hot_abs_ttl = std::get<StringSet>(*parsed).abs_ttl_ms;
  EXPECT_EQ(hot_abs_ttl, kAbsTtlMs);

  state_.Absorb(*parsed);
  auto ops = state_.Emit();
  ASSERT_EQ(ops.size(), 1);
  ASSERT_TRUE(std::holds_alternative<StringSet>(ops[0]));
  EXPECT_EQ(std::get<StringSet>(ops[0]).abs_ttl_ms, hot_abs_ttl);
}

}  // namespace
}  // namespace abyss::consumer
