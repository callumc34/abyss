#pragma once

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <initializer_list>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/effect.h"
#include "abyss/core/ops.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/engine/decide.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::engine::testing {

using Presence = hot::KeyView::Presence;
using Type = hot::Entry::Type;

inline constexpr uint64_t kNow = 1'000'000'000;
inline constexpr int64_t kTtl = static_cast<int64_t>(kNow) + 100'000;
inline constexpr std::string_view kWrongTypeText =
    "Operation against a key holding the wrong kind of value";

// One key as a fake lookup reports it.
struct FakeKey {
  Presence presence = Presence::kLive;
  Type type = Type::kString;
  std::optional<hot::Value> value;
  int64_t abs_ttl_ms = 0;
  core::SequenceId latest_seq = 1;
  bool flush_floor = false;
  core::ShardId shard = 0;
};

inline FakeKey Str(std::string value, int64_t abs_ttl_ms = 0, core::SequenceId seq = 1) {
  return {.type = Type::kString,
          .value = hot::Value{std::move(value)},
          .abs_ttl_ms = abs_ttl_ms,
          .latest_seq = seq};
}

inline FakeKey SetOf(std::initializer_list<std::string> members, int64_t abs_ttl_ms = 0) {
  return {.type = Type::kSet,
          .value = hot::Value{hot::SetValue{.members = members}},
          .abs_ttl_ms = abs_ttl_ms};
}

inline FakeKey ZsetOf(std::initializer_list<std::pair<std::string, double>> entries,
                      int64_t abs_ttl_ms = 0) {
  hot::ZsetValue zset;
  for (const auto& [member, score] : entries) {
    zset.member_scores[member] = score;
    zset.score_members[score].insert(member);
  }
  return {.type = Type::kZset, .value = hot::Value{std::move(zset)}, .abs_ttl_ms = abs_ttl_ms};
}

inline FakeKey HashOf(std::initializer_list<std::pair<const std::string, std::string>> fields,
                      int64_t abs_ttl_ms = 0) {
  return {.type = Type::kHash,
          .value = hot::Value{hot::HashValue{.fields = fields}},
          .abs_ttl_ms = abs_ttl_ms};
}

inline FakeKey Tombstone(core::SequenceId seq = 1) {
  return {.presence = Presence::kTombstoned, .latest_seq = seq};
}

inline FakeKey FlushAbsent(core::SequenceId flush_seq = 9) {
  return {.presence = Presence::kTombstoned, .latest_seq = flush_seq, .flush_floor = true};
}

// Past its TTL at kNow.
inline FakeKey Expired(FakeKey key) {
  key.presence = Presence::kExpired;
  key.abs_ttl_ms = static_cast<int64_t>(kNow) - 1;
  return key;
}

inline FakeKey StubOf(Type type = Type::kString, int64_t abs_ttl_ms = 0) {
  return {.presence = Presence::kStub, .type = type, .abs_ttl_ms = abs_ttl_ms};
}

inline FakeKey ExpiredStub(Type type = Type::kString) {
  return {
      .presence = Presence::kExpired, .type = type, .abs_ttl_ms = static_cast<int64_t>(kNow) - 1};
}

using Keys = std::map<std::string, FakeKey, std::less<>>;

inline hot::KeyView ViewOf(const FakeKey& key) {
  return {.presence = key.presence,
          .flush_floor = key.flush_floor,
          .type = key.type,
          .abs_ttl_ms = key.abs_ttl_ms,
          .latest_seq = key.latest_seq,
          .value = key.value.has_value() ? &*key.value : nullptr,
          .shard = key.shard};
}

// A key missing from `keys` is non-resident.
inline KeyLookup LookupOf(const Keys& keys) {
  return [&keys](std::string_view key) {
    const auto it = keys.find(key);
    return it == keys.end() ? hot::KeyView{} : ViewOf(it->second);
  };
}

inline Decision DecideOn(const Keys& keys, std::vector<std::string> args,
                         core::PredicateFlags flags = core::PredicateFlags::kNone) {
  core::RespCommand cmd{.args = std::move(args)};
  return Decide(cmd, flags, kNow, LookupOf(keys));
}

inline std::vector<KeyLoad> Loads(std::initializer_list<std::string> keys,
                                  Need need = Need::kState) {
  std::vector<KeyLoad> out;
  for (const auto& key : keys) out.push_back({.key = key, .need = need});
  return out;
}

// "" for no reply; otherwise its type and content.
inline std::string Describe(const std::optional<core::RespValue>& reply) {
  if (!reply.has_value()) return "";
  switch (reply->type()) {
    case core::RespValue::Type::kNull:
      return "nil";
    case core::RespValue::Type::kInteger:
      return ":" + std::to_string(reply->AsInteger());
    case core::RespValue::Type::kSimpleString:
      return "+" + reply->AsString();
    case core::RespValue::Type::kBulkString:
      return "$" + reply->AsString();
    case core::RespValue::Type::kError:
      return "-" + reply->AsString();
    default:
      return "?";
  }
}

inline std::optional<core::ErrorCode> ErrorOf(const Decision& d) {
  if (!d.error.has_value()) return std::nullopt;
  return d.error->code();
}

struct Want {
  std::vector<std::string> args;
  bool replaces_state = false;
  bool reply_old_value = false;
};

// Parsing the effect and writing it back out changes nothing.
inline void ExpectCanonical(const core::Effect& effect) {
  auto op = core::ops::ParseWriteOp(effect.cmd.Name(), effect.cmd, kNow);
  ASSERT_TRUE(op.has_value()) << op.error().message();
  EXPECT_EQ(core::ops::CanonicalCommand(*op).args, effect.cmd.args);
}

inline void ExpectEffects(const std::vector<core::Effect>& got, const std::vector<Want>& want) {
  ASSERT_EQ(got.size(), want.size());
  for (size_t i = 0; i < got.size(); ++i) {
    SCOPED_TRACE("effect " + std::to_string(i));
    ExpectCanonical(got[i]);
    EXPECT_EQ(got[i].cmd.args, want[i].args);
    EXPECT_EQ(got[i].key, want[i].args.at(1));
    EXPECT_EQ(got[i].replaces_state, want[i].replaces_state);
    EXPECT_EQ(got[i].reply_old_value, want[i].reply_old_value);
  }
}

}  // namespace abyss::engine::testing
