#include "abyss/engine/decide.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "abyss/core/predicate.h"
#include "abyss/core/result.h"
#include "decide_fixture.h"

namespace abyss::engine {
namespace {

using core::PredicateFlags;
using testing::DecideOn;
using testing::Describe;
using testing::ErrorOf;
using testing::Expired;
using testing::ExpiredStub;
using testing::FakeKey;
using testing::FlushAbsent;
using testing::HashOf;
using testing::Keys;
using testing::kNow;
using testing::kTtl;
using testing::Loads;
using testing::LookupOf;
using testing::Presence;
using testing::SetOf;
using testing::Str;
using testing::StubOf;
using testing::Tombstone;
using testing::Type;
using testing::Want;
using testing::ZsetOf;

constexpr auto kNone = PredicateFlags::kNone;
constexpr auto kNx = PredicateFlags::kNx;
constexpr auto kXx = PredicateFlags::kXx;
constexpr auto kGet = PredicateFlags::kGet;
constexpr auto kKeepTtl = PredicateFlags::kKeepTtl;
constexpr auto kGt = PredicateFlags::kZAddGt;
constexpr auto kLt = PredicateFlags::kZAddLt;
constexpr auto kCh = PredicateFlags::kZAddCh;
constexpr auto kExpireGt = PredicateFlags::kExpireGt;
constexpr auto kExpireLt = PredicateFlags::kExpireLt;

std::string Ms(int64_t ms) { return std::to_string(ms); }

std::vector<std::string> Concat(std::vector<std::string> head,
                                const std::vector<std::string>& tail) {
  head.insert(head.end(), tail.begin(), tail.end());
  return head;
}

struct DecideCase {
  std::string name;
  FakeKey key;
  std::vector<std::string> cmd;
  PredicateFlags flags = kNone;
  std::vector<Want> effects;
  // Describe()'s form; "" leaves the reply to apply.
  std::string reply;
  std::optional<core::ErrorCode> error;
  std::string message;
};

// Names the case in ctest, which lists a parameter by printing it.
void PrintTo(const DecideCase& c, std::ostream* os) { *os << c.name; }

void ExpectDecision(const Decision& d, const DecideCase& c) {
  if (c.error.has_value()) {
    EXPECT_EQ(ErrorOf(d), c.error);
    EXPECT_EQ(d.error.has_value() ? d.error->message() : "", c.message);
    EXPECT_TRUE(d.effects.empty());
    EXPECT_FALSE(d.reply.has_value());
    return;
  }
  ASSERT_FALSE(d.error.has_value()) << (d.error.has_value() ? d.error->message() : "");
  EXPECT_TRUE(d.needs_load.empty());
  testing::ExpectEffects(d.effects, c.effects);
  EXPECT_EQ(Describe(d.reply), c.reply);
}

// --- SET, every predicate combination ---

struct SetRow {
  std::string name;
  PredicateFlags flags;
  std::vector<std::string> tokens;
  // Against a live "old" with kTtl.
  bool present_sets;
  std::string present_reply;
  // Against an absent key.
  bool absent_sets;
  std::string absent_reply;
};

std::vector<SetRow> SetRows() {
  return {
      {"Plain", kNone, {}, true, "", true, ""},
      {"Nx", kNx, {"NX"}, false, "nil", true, ""},
      {"NxGet", kNx | kGet, {"NX", "GET"}, false, "$old", true, "nil"},
      {"NxKeepTtl", kNx | kKeepTtl, {"NX", "KEEPTTL"}, false, "nil", true, ""},
      {"NxGetKeepTtl", kNx | kGet | kKeepTtl, {"NX", "GET", "KEEPTTL"}, false, "$old", true, "nil"},
      {"Xx", kXx, {"XX"}, true, "", false, "nil"},
      {"XxGet", kXx | kGet, {"XX", "GET"}, true, "", false, "nil"},
      {"XxKeepTtl", kXx | kKeepTtl, {"XX", "KEEPTTL"}, true, "", false, "nil"},
      {"XxGetKeepTtl", kXx | kGet | kKeepTtl, {"XX", "GET", "KEEPTTL"}, true, "", false, "nil"},
      {"Get", kGet, {"GET"}, true, "", true, "nil"},
      {"KeepTtl", kKeepTtl, {"KEEPTTL"}, true, "", true, ""},
      {"GetKeepTtl", kGet | kKeepTtl, {"GET", "KEEPTTL"}, true, "", true, "nil"},
  };
}

std::vector<DecideCase> SetCases() {
  std::vector<DecideCase> out;
  for (const auto& row : SetRows()) {
    const bool get = core::HasFlag(row.flags, kGet);
    const bool keep_ttl = core::HasFlag(row.flags, kKeepTtl);
    std::vector<std::string> kept = {"SET", "k", "v"};
    if (keep_ttl) kept = {"SET", "k", "v", "PXAT", Ms(kTtl)};
    out.push_back(
        {.name = "Set" + row.name + "Present",
         .key = Str("old", kTtl),
         .cmd = Concat({"SET", "k", "v"}, row.tokens),
         .flags = row.flags,
         .effects =
             row.present_sets
                 ? std::vector<Want>{{.args = kept, .replaces_state = true, .reply_old_value = get}}
                 : std::vector<Want>{},
         .reply = row.present_reply});
    out.push_back({.name = "Set" + row.name + "Absent",
                   .key = Tombstone(),
                   .cmd = Concat({"SET", "k", "v"}, row.tokens),
                   .flags = row.flags,
                   .effects = row.absent_sets ? std::vector<Want>{{.args = {"SET", "k", "v"},
                                                                   .replaces_state = true}}
                                              : std::vector<Want>{},
                   .reply = row.absent_reply});
  }
  const std::vector<std::pair<std::string, PredicateFlags>> invalid = {
      {"", kNone}, {"Get", kGet}, {"KeepTtl", kKeepTtl}, {"GetKeepTtl", kGet | kKeepTtl}};
  for (const auto& [suffix, extra] : invalid) {
    out.push_back({.name = "SetNxXx" + suffix,
                   .key = Str("old"),
                   .cmd = {"SET", "k", "v", "NX", "XX"},
                   .flags = kNx | kXx | extra,
                   .error = core::ErrorCode::kInvalidArgument,
                   .message = "syntax error"});
  }
  const std::vector<std::pair<std::string, std::vector<std::string>>> malformed = {
      {"UnknownOption", {"SET", "k", "v", "BOGUS"}},
      {"ExWithoutTime", {"SET", "k", "v", "EX"}},
      {"TwoExpiries", {"SET", "k", "v", "EX", "1", "PX", "1"}},
      {"KeepTtlWithExpiry", {"SET", "k", "v", "KEEPTTL", "EX", "1"}}};
  for (const auto& [name, cmd] : malformed) {
    out.push_back({.name = "Set" + name,
                   .key = Str("old"),
                   .cmd = cmd,
                   .error = core::ErrorCode::kInvalidArgument,
                   .message = "syntax error"});
  }
  out.push_back({.name = "SetExNotAnInteger",
                 .key = Str("old"),
                 .cmd = {"SET", "k", "v", "EX", "ten"},
                 .error = core::ErrorCode::kInvalidArgument,
                 .message = "value is not an integer or out of range"});
  const std::vector<Want> set_ex = {
      {.args = {"SET", "k", "v", "PXAT", Ms(kNow + 10'000)}, .replaces_state = true}};
  out.push_back({.name = "SetExIsAbsolute",
                 .key = Tombstone(),
                 .cmd = {"set", "k", "v", "EX", "10"},
                 .effects = set_ex});
  out.push_back({.name = "SetexIsAbsolute",
                 .key = Tombstone(),
                 .cmd = {"SETEX", "k", "10", "v"},
                 .effects = set_ex});
  out.push_back(
      {.name = "PsetexIsAbsolute",
       .key = Tombstone(),
       .cmd = {"PSETEX", "k", "500", "v"},
       .effects = {{.args = {"SET", "k", "v", "PXAT", Ms(kNow + 500)}, .replaces_state = true}}});
  out.push_back({.name = "SetXxKeepTtlWithoutTtlSetsNone",
                 .key = Str("old"),
                 .cmd = {"SET", "k", "v", "XX", "KEEPTTL"},
                 .flags = kXx | kKeepTtl,
                 .effects = {{.args = {"SET", "k", "v"}, .replaces_state = true}}});
  out.push_back({.name = "SetGetOnSetIsWrongType",
                 .key = SetOf({"a"}),
                 .cmd = {"SET", "k", "v", "GET"},
                 .flags = kGet,
                 .error = core::ErrorCode::kWrongType,
                 .message = std::string(testing::kWrongTypeText)});
  out.push_back({.name = "SetNxOnSetIsNil",
                 .key = SetOf({"a"}),
                 .cmd = {"SET", "k", "v", "NX"},
                 .flags = kNx,
                 .reply = "nil"});
  out.push_back({.name = "PlainSetOverwritesSet",
                 .key = SetOf({"a"}),
                 .cmd = {"SET", "k", "v"},
                 .effects = {{.args = {"SET", "k", "v"}, .replaces_state = true}}});
  out.push_back({.name = "SetnxPresent",
                 .key = Str("old"),
                 .cmd = {"SETNX", "k", "v"},
                 .flags = kNx,
                 .reply = ":0"});
  out.push_back({.name = "SetnxAbsent",
                 .key = Tombstone(),
                 .cmd = {"SETNX", "k", "v"},
                 .flags = kNx,
                 .effects = {{.args = {"SET", "k", "v"}, .replaces_state = true}},
                 .reply = ":1"});
  return out;
}

// --- ZADD, every predicate combination ---

struct ZaddRow {
  std::string name;
  PredicateFlags flags;
  std::vector<std::string> tokens;
  // Of 3 a (up from 1), 1 b (down from 2), 5 c (new), against {a:1, b:2}.
  std::vector<std::string> kept;
  std::string present_reply;
  // Against an absent key.
  bool absent_adds;
  std::string absent_reply;
};

std::vector<ZaddRow> ZaddRows() {
  const std::vector<std::string> all = {"3", "a", "1", "b", "5", "c"};
  return {
      {"Plain", kNone, {}, all, ":1", true, ":3"},
      {"Ch", kCh, {"CH"}, all, ":3", true, ":3"},
      {"Nx", kNx, {"NX"}, {"5", "c"}, ":1", true, ":3"},
      {"NxCh", kNx | kCh, {"NX", "CH"}, {"5", "c"}, ":1", true, ":3"},
      {"Xx", kXx, {"XX"}, {"3", "a", "1", "b"}, ":0", false, ":0"},
      {"XxCh", kXx | kCh, {"XX", "CH"}, {"3", "a", "1", "b"}, ":2", false, ":0"},
      {"Gt", kGt, {"GT"}, {"3", "a", "5", "c"}, ":1", true, ":3"},
      {"GtCh", kGt | kCh, {"GT", "CH"}, {"3", "a", "5", "c"}, ":2", true, ":3"},
      {"Lt", kLt, {"LT"}, {"1", "b", "5", "c"}, ":1", true, ":3"},
      {"LtCh", kLt | kCh, {"LT", "CH"}, {"1", "b", "5", "c"}, ":2", true, ":3"},
      {"XxGt", kXx | kGt, {"XX", "GT"}, {"3", "a"}, ":0", false, ":0"},
      {"XxGtCh", kXx | kGt | kCh, {"XX", "GT", "CH"}, {"3", "a"}, ":1", false, ":0"},
      {"XxLt", kXx | kLt, {"XX", "LT"}, {"1", "b"}, ":0", false, ":0"},
      {"XxLtCh", kXx | kLt | kCh, {"XX", "LT", "CH"}, {"1", "b"}, ":1", false, ":0"},
  };
}

std::vector<DecideCase> ZaddCases() {
  std::vector<DecideCase> out;
  const std::vector<std::string> pairs = {"3", "a", "1", "b", "5", "c"};
  for (const auto& row : ZaddRows()) {
    const auto cmd = Concat(Concat({"ZADD", "k"}, row.tokens), pairs);
    out.push_back({.name = "Zadd" + row.name + "Present",
                   .key = ZsetOf({{"a", 1}, {"b", 2}}),
                   .cmd = cmd,
                   .flags = row.flags,
                   .effects = {{.args = Concat({"ZADD", "k"}, row.kept)}},
                   .reply = row.present_reply});
    out.push_back({.name = "Zadd" + row.name + "Absent",
                   .key = Tombstone(),
                   .cmd = cmd,
                   .flags = row.flags,
                   .effects = row.absent_adds
                                  ? std::vector<Want>{{.args = Concat({"ZADD", "k"}, pairs),
                                                       .replaces_state = true}}
                                  : std::vector<Want>{},
                   .reply = row.absent_reply});
  }
  const std::string nx_xx = "XX and NX options at the same time are not compatible";
  const std::string gt_lt_nx = "GT, LT, and/or NX options at the same time are not compatible";
  const std::vector<std::tuple<std::string, PredicateFlags, std::string>> invalid = {
      {"NxXx", kNx | kXx, nx_xx},           {"GtLt", kGt | kLt, gt_lt_nx},
      {"NxGt", kNx | kGt, gt_lt_nx},        {"NxLt", kNx | kLt, gt_lt_nx},
      {"NxXxCh", kNx | kXx | kCh, nx_xx},   {"NxXxGt", kNx | kXx | kGt, nx_xx},
      {"GtLtCh", kGt | kLt | kCh, gt_lt_nx}};
  for (const auto& [name, flags, message] : invalid) {
    out.push_back({.name = "Zadd" + name,
                   .key = ZsetOf({{"a", 1}}),
                   .cmd = {"ZADD", "k", "1", "a"},
                   .flags = flags,
                   .error = core::ErrorCode::kInvalidArgument,
                   .message = message});
  }
  for (const auto& [name, cmd] : std::vector<std::pair<std::string, std::vector<std::string>>>{
           {"OddPairs", {"ZADD", "k", "1", "a", "2"}}, {"NoPairs", {"ZADD", "k", "CH", "NX"}}}) {
    out.push_back({.name = "Zadd" + name,
                   .key = ZsetOf({{"a", 1}}),
                   .cmd = cmd,
                   .error = core::ErrorCode::kInvalidArgument,
                   .message = "syntax error"});
  }
  out.push_back({.name = "ZaddScoreIsCanonical",
                 .key = ZsetOf({{"a", 1}}),
                 .cmd = {"ZADD", "k", "1.50", "a", "2e0", "b"},
                 .effects = {{.args = {"ZADD", "k", "1.5", "a", "2", "b"}}},
                 .reply = ":1"});
  out.push_back({.name = "ZaddNxRepeatedMemberSeesItsEarlierPair",
                 .key = Tombstone(),
                 .cmd = {"ZADD", "k", "NX", "1", "a", "2", "a"},
                 .flags = kNx,
                 .effects = {{.args = {"ZADD", "k", "1", "a"}, .replaces_state = true}},
                 .reply = ":1"});
  out.push_back({.name = "ZaddChRepeatedMemberCountsOnce",
                 .key = Tombstone(),
                 .cmd = {"ZADD", "k", "CH", "1", "a", "2", "a"},
                 .flags = kCh,
                 .effects = {{.args = {"ZADD", "k", "1", "a", "2", "a"}, .replaces_state = true}},
                 .reply = ":2"});
  out.push_back({.name = "ZaddChUnchangedScoreIsNotCounted",
                 .key = ZsetOf({{"a", 1}}),
                 .cmd = {"ZADD", "k", "CH", "1", "a"},
                 .flags = kCh,
                 .reply = ":0"});
  out.push_back({.name = "ZaddOnStringIsWrongType",
                 .key = Str("v"),
                 .cmd = {"ZADD", "k", "1", "a"},
                 .error = core::ErrorCode::kWrongType,
                 .message = std::string(testing::kWrongTypeText)});
  out.push_back({.name = "ZaddNanScoreIsRejected",
                 .key = ZsetOf({{"a", 1}}),
                 .cmd = {"ZADD", "k", "nan", "a"},
                 .error = core::ErrorCode::kInvalidArgument,
                 .message = "value is not a valid float"});
  out.push_back({.name = "ZaddTakesALeadingPlus",
                 .key = ZsetOf({{"a", 1}}),
                 .cmd = {"ZADD", "k", "+2", "a", "+inf", "b"},
                 .effects = {{.args = {"ZADD", "k", "2", "a", "inf", "b"}}},
                 .reply = ":1"});
  out.push_back({.name = "ZaddRejectsPlusMinus",
                 .key = ZsetOf({{"a", 1}}),
                 .cmd = {"ZADD", "k", "+-2", "a"},
                 .error = core::ErrorCode::kInvalidArgument,
                 .message = "value is not a valid float"});
  out.push_back({.name = "ZaddGtNanScoreIsRejected",
                 .key = ZsetOf({{"a", 1}}),
                 .cmd = {"ZADD", "k", "GT", "NaN", "a"},
                 .flags = kGt,
                 .error = core::ErrorCode::kInvalidArgument,
                 .message = "value is not a valid float"});
  return out;
}

// --- The EXPIRE family, every predicate combination ---

struct ExpireRow {
  std::string name;
  PredicateFlags flags;
  std::vector<std::string> tokens;
  // Against kTtl, asking for an earlier then a later one; then a key
  // with no TTL, asking for the earlier.
  bool sooner;
  bool later;
  bool no_ttl;
};

std::vector<ExpireRow> ExpireRows() {
  return {
      {"Plain", kNone, {}, true, true, true},
      {"Nx", kNx, {"NX"}, false, false, true},
      {"Xx", kXx, {"XX"}, true, true, false},
      {"Gt", kExpireGt, {"GT"}, false, true, false},
      {"Lt", kExpireLt, {"LT"}, true, false, true},
      {"XxGt", kXx | kExpireGt, {"XX", "GT"}, false, true, false},
      {"XxLt", kXx | kExpireLt, {"XX", "LT"}, true, false, false},
  };
}

std::vector<DecideCase> ExpireCases() {
  std::vector<DecideCase> out;
  const auto apply = [](int64_t at) {
    return std::vector<Want>{{.args = {"PEXPIREAT", "k", Ms(at)}}};
  };
  const int64_t sooner = static_cast<int64_t>(kNow) + 50'000;
  const int64_t later = static_cast<int64_t>(kNow) + 200'000;
  for (const auto& row : ExpireRows()) {
    const auto cmd_sooner = Concat({"PEXPIRE", "k", "50000"}, row.tokens);
    const auto cmd_later = Concat({"PEXPIRE", "k", "200000"}, row.tokens);
    out.push_back({.name = "Pexpire" + row.name + "Sooner",
                   .key = Str("v", kTtl),
                   .cmd = cmd_sooner,
                   .flags = row.flags,
                   .effects = row.sooner ? apply(sooner) : std::vector<Want>{},
                   .reply = row.sooner ? "" : ":0"});
    out.push_back({.name = "Pexpire" + row.name + "Later",
                   .key = Str("v", kTtl),
                   .cmd = cmd_later,
                   .flags = row.flags,
                   .effects = row.later ? apply(later) : std::vector<Want>{},
                   .reply = row.later ? "" : ":0"});
    out.push_back({.name = "Pexpire" + row.name + "NoTtl",
                   .key = Str("v"),
                   .cmd = cmd_sooner,
                   .flags = row.flags,
                   .effects = row.no_ttl ? apply(sooner) : std::vector<Want>{},
                   .reply = row.no_ttl ? "" : ":0"});
    out.push_back({.name = "Pexpire" + row.name + "Absent",
                   .key = Tombstone(),
                   .cmd = cmd_sooner,
                   .flags = row.flags,
                   .reply = ":0"});
    // A past time is the soonest of all, and deletes.
    const auto cmd_past = Concat({"PEXPIRE", "k", "-1"}, row.tokens);
    const std::vector<Want> del = {{.args = {"DEL", "k"}, .replaces_state = true}};
    out.push_back({.name = "Pexpire" + row.name + "Past",
                   .key = Str("v", kTtl),
                   .cmd = cmd_past,
                   .flags = row.flags,
                   .effects = row.sooner ? del : std::vector<Want>{},
                   .reply = row.sooner ? ":1" : ":0"});
    out.push_back({.name = "Pexpire" + row.name + "PastNoTtl",
                   .key = Str("v"),
                   .cmd = cmd_past,
                   .flags = row.flags,
                   .effects = row.no_ttl ? del : std::vector<Want>{},
                   .reply = row.no_ttl ? ":1" : ":0"});
  }
  out.push_back({.name = "ExpireatZeroDeletes",
                 .key = SetOf({"m"}),
                 .cmd = {"EXPIREAT", "k", "0"},
                 .effects = {{.args = {"DEL", "k"}, .replaces_state = true}},
                 .reply = ":1"});
  out.push_back({.name = "PexpireNowDeletes",
                 .key = Str("v"),
                 .cmd = {"PEXPIRE", "k", "0"},
                 .effects = {{.args = {"DEL", "k"}, .replaces_state = true}},
                 .reply = ":1"});
  const std::string nx_any = "NX and XX, GT or LT options at the same time are not compatible";
  const std::string gt_lt = "GT and LT options at the same time are not compatible";
  const std::vector<std::tuple<std::string, PredicateFlags, std::string>> invalid = {
      {"NxXx", kNx | kXx, nx_any},
      {"GtLt", kExpireGt | kExpireLt, gt_lt},
      {"NxGt", kNx | kExpireGt, nx_any},
      {"NxLt", kNx | kExpireLt, nx_any},
      {"NxGtLt", kNx | kExpireGt | kExpireLt, nx_any},
      {"XxGtLt", kXx | kExpireGt | kExpireLt, gt_lt}};
  for (const auto& [name, flags, message] : invalid) {
    out.push_back({.name = "Expire" + name,
                   .key = Str("v"),
                   .cmd = {"EXPIRE", "k", "10"},
                   .flags = flags,
                   .error = core::ErrorCode::kInvalidArgument,
                   .message = message});
  }
  out.push_back({.name = "ExpireNotAnInteger",
                 .key = Str("v"),
                 .cmd = {"EXPIRE", "k", "ten"},
                 .error = core::ErrorCode::kInvalidArgument,
                 .message = "value is not an integer or out of range"});
  out.push_back({.name = "ExpireSecondsIsAbsolute",
                 .key = SetOf({"a"}),
                 .cmd = {"EXPIRE", "k", "10"},
                 .effects = apply(static_cast<int64_t>(kNow) + 10'000)});
  out.push_back({.name = "ExpireatIsAbsolute",
                 .key = HashOf({{"f", "v"}}),
                 .cmd = {"EXPIREAT", "k", "2000000"},
                 .effects = apply(2'000'000'000)});
  out.push_back({.name = "PexpireatIsAbsolute",
                 .key = ZsetOf({{"a", 1}}),
                 .cmd = {"PEXPIREAT", "k", Ms(later)},
                 .effects = apply(later)});
  out.push_back({.name = "ExpireNegativeTtlDeletes",
                 .key = Str("v"),
                 .cmd = {"EXPIRE", "k", "-1"},
                 .effects = {{.args = {"DEL", "k"}, .replaces_state = true}},
                 .reply = ":1"});
  out.push_back({.name = "ExpireOverflowIsRejected",
                 .key = Str("v"),
                 .cmd = {"EXPIRE", "k", "9223372036854775807"},
                 .error = core::ErrorCode::kInvalidArgument,
                 .message = "invalid expire time in 'expire' command"});
  out.push_back({.name = "ExpireUnknownOptionIsRejected",
                 .key = Str("v"),
                 .cmd = {"EXPIRE", "k", "10", "bogus"},
                 .error = core::ErrorCode::kInvalidArgument,
                 .message = "Unsupported option bogus"});
  out.push_back({.name = "PersistWithTtl",
                 .key = Str("v", kTtl),
                 .cmd = {"PERSIST", "k"},
                 .effects = {{.args = {"PERSIST", "k"}}}});
  out.push_back(
      {.name = "PersistWithoutTtl", .key = Str("v"), .cmd = {"PERSIST", "k"}, .reply = ":0"});
  out.push_back(
      {.name = "PersistAbsent", .key = Tombstone(), .cmd = {"PERSIST", "k"}, .reply = ":0"});
  return out;
}

// --- Collections ---

std::vector<DecideCase> CollectionCases() {
  return {
      {.name = "SaddPresent",
       .key = SetOf({"a"}),
       .cmd = {"SADD", "k", "a", "b"},
       .effects = {{.args = {"SADD", "k", "a", "b"}}}},
      {.name = "SaddAbsentCreates",
       .key = Tombstone(),
       .cmd = {"SADD", "k", "a"},
       .effects = {{.args = {"SADD", "k", "a"}, .replaces_state = true}}},
      {.name = "SaddOnStringIsWrongType",
       .key = Str("v"),
       .cmd = {"SADD", "k", "a"},
       .error = core::ErrorCode::kWrongType,
       .message = std::string(testing::kWrongTypeText)},
      {.name = "SremPresent",
       .key = SetOf({"a"}),
       .cmd = {"SREM", "k", "a", "z"},
       .effects = {{.args = {"SREM", "k", "a", "z"}}}},
      {.name = "SremAbsent", .key = Tombstone(), .cmd = {"SREM", "k", "a"}, .reply = ":0"},
      {.name = "SremOnHashIsWrongType",
       .key = HashOf({{"f", "v"}}),
       .cmd = {"SREM", "k", "a"},
       .error = core::ErrorCode::kWrongType,
       .message = std::string(testing::kWrongTypeText)},
      {.name = "ZremPresent",
       .key = ZsetOf({{"a", 1}}),
       .cmd = {"ZREM", "k", "a"},
       .effects = {{.args = {"ZREM", "k", "a"}}}},
      {.name = "ZremAbsent", .key = Tombstone(), .cmd = {"ZREM", "k", "a"}, .reply = ":0"},
      {.name = "ZremOnSetIsWrongType",
       .key = SetOf({"a"}),
       .cmd = {"ZREM", "k", "a"},
       .error = core::ErrorCode::kWrongType,
       .message = std::string(testing::kWrongTypeText)},
      {.name = "HsetPresent",
       .key = HashOf({{"f", "v"}}),
       .cmd = {"HSET", "k", "f", "w", "g", "x"},
       .effects = {{.args = {"HSET", "k", "f", "w", "g", "x"}}}},
      {.name = "HsetAbsentCreates",
       .key = Tombstone(),
       .cmd = {"hset", "k", "f", "v"},
       .effects = {{.args = {"HSET", "k", "f", "v"}, .replaces_state = true}}},
      {.name = "HsetOddFieldsIsRejected",
       .key = HashOf({{"f", "v"}}),
       .cmd = {"HSET", "k", "f", "v", "g"},
       .error = core::ErrorCode::kInvalidArgument,
       .message = "wrong number of arguments for 'hset' command"},
      {.name = "HmsetOddFieldsIsRejected",
       .key = HashOf({{"f", "v"}}),
       .cmd = {"hmset", "k", "f", "v", "g"},
       .error = core::ErrorCode::kInvalidArgument,
       .message = "wrong number of arguments for 'hmset' command"},
      {.name = "HsetOnZsetIsWrongType",
       .key = ZsetOf({{"a", 1}}),
       .cmd = {"HSET", "k", "f", "v"},
       .error = core::ErrorCode::kWrongType,
       .message = std::string(testing::kWrongTypeText)},
      {.name = "HmsetPresent",
       .key = HashOf({{"f", "v"}}),
       .cmd = {"HMSET", "k", "f", "w"},
       .effects = {{.args = {"HMSET", "k", "f", "w"}}}},
      {.name = "HmsetAbsentCreates",
       .key = Tombstone(),
       .cmd = {"HMSET", "k", "f", "w"},
       .effects = {{.args = {"HMSET", "k", "f", "w"}, .replaces_state = true}}},
      {.name = "HdelPresent",
       .key = HashOf({{"f", "v"}}),
       .cmd = {"HDEL", "k", "f"},
       .effects = {{.args = {"HDEL", "k", "f"}}}},
      {.name = "HdelAbsent", .key = Tombstone(), .cmd = {"HDEL", "k", "f"}, .reply = ":0"},
      {.name = "HsetnxFieldPresent",
       .key = HashOf({{"f", "v"}}),
       .cmd = {"HSETNX", "k", "f", "w"},
       .flags = kNx,
       .reply = ":0"},
      {.name = "HsetnxFieldAbsent",
       .key = HashOf({{"g", "v"}}),
       .cmd = {"HSETNX", "k", "f", "w"},
       .flags = kNx,
       .effects = {{.args = {"HSET", "k", "f", "w"}}},
       .reply = ":1"},
      {.name = "HsetnxKeyAbsentCreates",
       .key = Tombstone(),
       .cmd = {"HSETNX", "k", "f", "w"},
       .flags = kNx,
       .effects = {{.args = {"HSET", "k", "f", "w"}, .replaces_state = true}},
       .reply = ":1"},
      {.name = "HsetnxOnStringIsWrongType",
       .key = Str("v"),
       .cmd = {"HSETNX", "k", "f", "w"},
       .flags = kNx,
       .error = core::ErrorCode::kWrongType,
       .message = std::string(testing::kWrongTypeText)},
  };
}

class DecideTableTest : public ::testing::TestWithParam<DecideCase> {};

TEST_P(DecideTableTest, Decides) {
  const DecideCase& c = GetParam();
  const Keys keys = {{"k", c.key}};
  ExpectDecision(DecideOn(keys, c.cmd, c.flags), c);
}

// Restore gives back every argument decide moved, so the request
// decides the same again: the sequencer's retry after a failed reserve.
TEST_P(DecideTableTest, RestoreThenDecideAgainIsIdentical) {
  const DecideCase& c = GetParam();
  const Keys keys = {{"k", c.key}};
  core::RespCommand cmd{.args = c.cmd};
  Decision first = Decide(cmd, c.flags, kNow, LookupOf(keys));
  if (first.effects.empty()) {
    EXPECT_EQ(cmd.args, c.cmd) << "nothing emitted, so nothing moved";
    EXPECT_TRUE(first.moved.empty());
    return;
  }
  const std::vector<core::Effect> effects = first.effects;
  const std::string reply = Describe(first.reply);
  Restore(std::move(first), cmd);
  EXPECT_EQ(cmd.args, c.cmd);

  const Decision again = Decide(cmd, c.flags, kNow, LookupOf(keys));
  ASSERT_EQ(again.effects.size(), effects.size());
  for (size_t i = 0; i < effects.size(); ++i) {
    EXPECT_EQ(again.effects[i].key, effects[i].key);
    EXPECT_EQ(again.effects[i].cmd.args, effects[i].cmd.args);
    EXPECT_EQ(again.effects[i].replaces_state, effects[i].replaces_state);
    EXPECT_EQ(again.effects[i].reply_old_value, effects[i].reply_old_value);
  }
  EXPECT_EQ(Describe(again.reply), reply);
}

std::vector<DecideCase> AllCases() {
  std::vector<DecideCase> out;
  for (auto cases : {SetCases(), ZaddCases(), ExpireCases(), CollectionCases()}) {
    for (auto& c : cases) out.push_back(std::move(c));
  }
  return out;
}

INSTANTIATE_TEST_SUITE_P(Commands, DecideTableTest, ::testing::ValuesIn(AllCases()),
                         [](const auto& info) { return info.param.name; });

// --- Every single-key command against every state ---

// Each case's live-key decision is pinned above; here the other states
// are checked against it and against each other.
class DecideStateTest : public ::testing::TestWithParam<DecideCase> {};

Decision RunOn(const FakeKey& key, const DecideCase& c) {
  return DecideOn(Keys{{"k", key}}, c.cmd, c.flags);
}

bool IsDelOfK(const core::Effect& e) {
  return e.cmd.args == std::vector<std::string>{"DEL", "k"} && e.replaces_state &&
         e.observed_expiry;
}

TEST_P(DecideStateTest, StatesAgree) {
  const DecideCase& c = GetParam();
  const Decision tombstoned = RunOn(Tombstone(4), c);
  ASSERT_TRUE(tombstoned.needs_load.empty());
  const bool blind = DecideOn(Keys{}, c.cmd, c.flags).needs_load.empty();

  {
    SCOPED_TRACE("flush floor reads as a tombstone at the Flush's seq");
    const Decision flushed = RunOn(FlushAbsent(9), c);
    EXPECT_EQ(Describe(flushed.reply), Describe(tombstoned.reply));
    EXPECT_EQ(flushed.effects.size(), tombstoned.effects.size());
    EXPECT_EQ(flushed.error.has_value(), tombstoned.error.has_value());
    if (!blind && !flushed.error.has_value()) {
      ASSERT_EQ(flushed.observed.size(), 1U);
      EXPECT_EQ(flushed.observed[0].second, 9U);
    }
  }
  for (const FakeKey& expired : {Expired(c.key), ExpiredStub(c.key.type)}) {
    SCOPED_TRACE(expired.value.has_value() ? "expired entry" : "expired stub");
    const Decision d = RunOn(expired, c);
    EXPECT_TRUE(d.needs_load.empty());
    EXPECT_EQ(Describe(d.reply), Describe(tombstoned.reply));
    EXPECT_EQ(d.error.has_value(), tombstoned.error.has_value());
    if (blind || tombstoned.error.has_value()) {
      EXPECT_EQ(d.effects.size(), tombstoned.effects.size());
      continue;
    }
    ASSERT_EQ(d.effects.size(), tombstoned.effects.size() + 1);
    EXPECT_TRUE(IsDelOfK(d.effects[0])) << "observed expiry is logged first";
    for (size_t i = 0; i < tombstoned.effects.size(); ++i) {
      EXPECT_EQ(d.effects[i + 1].cmd.args, tombstoned.effects[i].cmd.args);
      EXPECT_EQ(d.effects[i + 1].replaces_state, tombstoned.effects[i].replaces_state);
    }
  }
  const Decision non_resident = RunOn(FakeKey{.presence = Presence::kNonResident}, c);
  const bool answered = blind || tombstoned.error.has_value();
  const bool existence =
      !non_resident.needs_load.empty() && non_resident.needs_load.front().need == Need::kExistence;
  const auto expect_load = [](const Decision& d, Need need) {
    EXPECT_EQ(d.needs_load, Loads({"k"}, need));
    EXPECT_TRUE(d.effects.empty());
    EXPECT_FALSE(d.reply.has_value());
    EXPECT_TRUE(d.observed.empty());
    EXPECT_FALSE(d.error.has_value());
  };
  {
    SCOPED_TRACE("non-resident");
    if (answered) {
      EXPECT_TRUE(non_resident.needs_load.empty());
    } else {
      expect_load(non_resident, existence ? Need::kExistence : Need::kState);
    }
  }
  {
    SCOPED_TRACE("stub of the live key's type and TTL");
    const Decision d = RunOn(StubOf(c.key.type, c.key.abs_ttl_ms), c);
    if (answered) {
      EXPECT_TRUE(d.needs_load.empty());
    } else if (existence) {
      // Existence, type and TTL are all such a decision reads.
      const Decision live = RunOn(c.key, c);
      EXPECT_TRUE(d.needs_load.empty());
      EXPECT_EQ(Describe(d.reply), Describe(live.reply));
      ASSERT_EQ(d.effects.size(), live.effects.size());
      for (size_t i = 0; i < d.effects.size(); ++i) {
        EXPECT_EQ(d.effects[i].cmd.args, live.effects[i].cmd.args);
      }
    } else {
      expect_load(d, Need::kState);
    }
  }
  {
    SCOPED_TRACE("a live key is observed at its seq");
    FakeKey live = c.key;
    live.latest_seq = 7;
    const Decision d = RunOn(live, c);
    if (!blind && !d.error.has_value()) {
      ASSERT_EQ(d.observed.size(), 1U);
      EXPECT_EQ(d.observed[0], (std::pair<std::string, core::SequenceId>{"k", 7}));
    }
  }
}

std::vector<DecideCase> LiveCases() {
  std::vector<DecideCase> out;
  for (auto& c : AllCases()) {
    if (c.key.presence == Presence::kLive) out.push_back(std::move(c));
  }
  return out;
}

INSTANTIATE_TEST_SUITE_P(Commands, DecideStateTest, ::testing::ValuesIn(LiveCases()),
                         [](const auto& info) { return info.param.name; });

TEST(DecideTest, CollectionWritesThatChangeNothingLogNothing) {
  const Keys keys = {{"s", SetOf({"a"})}, {"z", ZsetOf({{"a", 1}})}, {"h", HashOf({{"f", "v"}})}};
  for (const auto& args : std::vector<std::vector<std::string>>{{"SADD", "s", "a", "a"},
                                                                {"SREM", "s", "x"},
                                                                {"ZREM", "z", "x"},
                                                                {"HDEL", "h", "x"},
                                                                {"ZADD", "z", "1", "a"}}) {
    SCOPED_TRACE(args[0]);
    const Decision d = DecideOn(keys, args);
    EXPECT_TRUE(d.effects.empty());
    EXPECT_EQ(Describe(d.reply), ":0");
    EXPECT_EQ(d.observed.size(), 1U) << "the reply fences on what it read";
  }
  EXPECT_EQ(DecideOn(keys, {"HSET", "h", "f", "v"}).effects.size(), 1U)
      << "Redis counts every pair HSET sets";
}

TEST(DecideTest, ZaddIncrIsRefused) {
  const Decision d = DecideOn(Keys{{"k", ZsetOf({{"a", 1}})}}, {"ZADD", "k", "INCR", "1", "a"});
  EXPECT_EQ(d.error.has_value() ? d.error->message() : "", "ZADD INCR is not supported");
}

TEST(DecideTest, PlainSetNeedsNoState) {
  const Decision d = DecideOn(Keys{}, {"SET", "k", "v"});
  EXPECT_TRUE(d.needs_load.empty());
  EXPECT_TRUE(d.observed.empty()) << "a blind write reads nothing";
  testing::ExpectEffects(d.effects, {{.args = {"SET", "k", "v"}, .replaces_state = true}});
}

TEST(DecideTest, ExpiredKeyKeepsNoTtl) {
  const Decision d =
      DecideOn(Keys{{"k", Expired(Str("old", kTtl))}}, {"SET", "k", "v", "KEEPTTL"}, kKeepTtl);
  testing::ExpectEffects(d.effects, {{.args = {"DEL", "k"}, .replaces_state = true},
                                     {.args = {"SET", "k", "v"}, .replaces_state = true}});
}

// Large values move from the request into the effect: the request's
// argument is left empty and the effect holds the very same buffer.
struct MoveCase {
  std::string name;
  Keys keys;
  std::vector<std::string> args;
  PredicateFlags flags = kNone;
  // Request arguments the effects take, in order; the large ones are
  // checked for the same buffer.
  std::vector<uint32_t> moved;
  std::vector<uint32_t> large;
};

std::vector<MoveCase> MoveCases() {
  const std::string a(1024, 'a');
  const std::string b(1024, 'b');
  return {
      {.name = "SetBlind", .args = {"SET", "k", a}, .moved = {2}, .large = {2}},
      {.name = "SetXx",
       .keys = {{"k", Str("old")}},
       .args = {"SET", "k", a, "XX"},
       .flags = kXx,
       .moved = {2},
       .large = {2}},
      {.name = "Mset", .args = {"MSET", "x", a, "y", b}, .moved = {2, 4}, .large = {2, 4}},
      {.name = "Sadd",
       .keys = {{"k", Tombstone()}},
       .args = {"SADD", "k", a, b},
       .moved = {1, 2, 3},
       .large = {2, 3}},
      {.name = "Hset",
       .keys = {{"k", HashOf({{"f", "v"}})}},
       .args = {"HSET", "k", "f", a},
       .moved = {1, 2, 3},
       .large = {3}},
      {.name = "Hsetnx",
       .keys = {{"k", HashOf({{"g", "v"}})}},
       .args = {"HSETNX", "k", "f", a},
       .flags = kNx,
       .moved = {2, 3},
       .large = {3}},
      {.name = "Zadd",
       .keys = {{"k", Tombstone()}},
       .args = {"ZADD", "k", "1", a},
       .moved = {3},
       .large = {3}},
      // a stays at 5: GT refuses it, so only b's member moves.
      {.name = "ZaddGt",
       .keys = {{"k", ZsetOf({{a, 5}})}},
       .args = {"ZADD", "k", "GT", "1", a, "2", b},
       .flags = kGt,
       .moved = {6},
       .large = {6}},
  };
}

TEST(DecideTest, LargeValuesMoveNotCopy) {
  for (const MoveCase& c : MoveCases()) {
    SCOPED_TRACE(c.name);
    core::RespCommand cmd{.args = c.args};
    std::vector<const char*> buffers;
    buffers.reserve(c.large.size());
    for (const uint32_t i : c.large) buffers.push_back(cmd.args[i].data());

    const Decision d = Decide(cmd, c.flags, kNow, LookupOf(c.keys));
    ASSERT_FALSE(d.error.has_value());
    ASSERT_TRUE(d.needs_load.empty());
    std::vector<uint32_t> moved;
    for (const Moved& m : d.moved) {
      moved.push_back(m.request_arg);
      EXPECT_EQ(d.effects.at(m.effect).cmd.args.at(m.arg), c.args.at(m.request_arg));
    }
    EXPECT_EQ(moved, c.moved);
    for (size_t j = 0; j < c.large.size(); ++j) {
      const uint32_t i = c.large[j];
      EXPECT_TRUE(cmd.args[i].empty()) << "argument " << i << " was copied";
      const auto m = std::ranges::find(d.moved, i, &Moved::request_arg);
      ASSERT_NE(m, d.moved.end());
      EXPECT_EQ(d.effects.at(m->effect).cmd.args.at(m->arg).data(), buffers[j]);
    }
  }
}

TEST(DecideTest, RejectsUnknownCommandsAndBadArity) {
  for (const auto& args : std::vector<std::vector<std::string>>{{},
                                                                {"GET", "k"},
                                                                {"SET", "k"},
                                                                {"SETNX", "k", "v", "x"},
                                                                {"HSETNX", "k", "f"},
                                                                {"PERSIST"},
                                                                {"ZADD", "k", "1"},
                                                                {"MSET", "a", "1", "b"},
                                                                {"MSETNX", "a"}}) {
    SCOPED_TRACE(args.empty() ? "" : args[0]);
    const Decision d = DecideOn(Keys{{"k", Str("v")}}, args);
    EXPECT_EQ(ErrorOf(d), core::ErrorCode::kInvalidArgument);
    EXPECT_TRUE(d.effects.empty());
  }
}

}  // namespace
}  // namespace abyss::engine
