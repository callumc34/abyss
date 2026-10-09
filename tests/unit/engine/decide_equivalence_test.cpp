#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/consumer/resolver.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/ascii.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/effect.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/predicate.h"
#include "abyss/engine/decide.h"
#include "abyss/hot/single_shard_store.h"
#include "decide_fixture.h"
#include "mock_cold_store.h"
#include "mock_queue.h"

// Today's Resolver::Decide and the new Decide, given the same history,
// make the same decision. Pinned here before the resolver goes.
namespace abyss::engine {
namespace {

using core::PredicateFlags;
using ::testing::_;
using testing::Describe;
using ::testing::Return;
namespace ops = core::ops;

constexpr uint64_t kAt = 50'000'000;

core::WallTime At(uint64_t ms) { return core::WallTime{std::chrono::milliseconds{ms}}; }

// The buffer is empty, so the resolver reads its cache, then cold.
class EmptyBufferRouter : public consumer::CompactionBufferRouter {
 public:
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& /*op*/,
                                     std::optional<core::Duration> /*deadline*/) override {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "empty buffer (test)"));
  }
  core::Result<core::RespValue> Read(std::string_view /*key*/) const override {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "empty buffer (test)"));
  }
  consumer::BufferKeyPresence Probe(std::string_view /*key*/) const override {
    return consumer::BufferKeyPresence::kAbsent;
  }
  consumer::HashOverlay HashOverlayFor(std::string_view /*key*/) const override {
    return consumer::HashOverlay{};
  }
  std::optional<consumer::CompactedState> Snapshot(core::ShardId /*shard*/,
                                                   std::string_view /*key*/) const override {
    return std::nullopt;
  }
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*target_seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    return true;
  }
};

struct Step {
  std::vector<std::string> args;
  uint64_t at_ms = kAt;
};

struct EquivalenceCase {
  std::string name;
  std::vector<Step> history;
  std::vector<std::string> cmd;
  PredicateFlags flags = PredicateFlags::kNone;
  uint64_t at_ms = kAt;
};

// Names the case in ctest, which lists a parameter by printing it.
void PrintTo(const EquivalenceCase& c, std::ostream* os) { *os << c.name; }

struct Outcome {
  // Canonical, so both sides' spellings compare.
  std::vector<std::vector<std::string>> effects;
  std::string reply;
};

std::vector<std::string> Canonical(const core::RespCommand& cmd, uint64_t at_ms) {
  auto op = ops::ParseWriteOp(core::AsciiUpper(cmd.args[0]), cmd, at_ms);
  EXPECT_TRUE(op.has_value()) << cmd.args[0];
  if (!op.has_value()) return cmd.args;
  return ops::CanonicalCommand(*op).args;
}

class EquivalenceHarness : public ::testing::Test {
 protected:
  Outcome ResolverOutcome(const EquivalenceCase& c) {
    std::vector<core::QueueEntry> entries;
    entries.reserve(c.history.size() + 1);
    core::SequenceId seq = 10;
    for (const auto& step : c.history) {
      entries.push_back({.seq = seq++,
                         .appended_at = At(step.at_ms),
                         .payload = core::entry::Write{.cmd = core::RespCommand{step.args}}});
    }
    const core::SequenceId ref = seq;
    entries.push_back(
        {.seq = ref,
         .appended_at = At(c.at_ms),
         .payload = core::entry::Conditional{.cmd = core::RespCommand{c.cmd}, .flags = c.flags}});
    auto remaining = std::make_shared<std::vector<core::QueueEntry>>(std::move(entries));
    EXPECT_CALL(queue_, Read(0, _, _, _, _))
        .WillRepeatedly(
            [remaining](core::ShardId, core::SequenceId, size_t, core::Duration,
                        core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
              return std::exchange(*remaining, {});
            });
    std::vector<core::QueueEntry> appended;
    EXPECT_CALL(queue_, Append(_, _, _))
        .WillRepeatedly([&appended](core::ShardId, core::QueueEntry entry,
                                    core::SteadyTime) -> core::Result<queue::AppendResult> {
          std::promise<core::Result<void>> durable;
          durable.set_value(core::Result<void>{});
          entry.seq = 1000 + appended.size();
          appended.push_back(std::move(entry));
          return queue::AppendResult{.seq = appended.back().seq, .durable = durable.get_future()};
        });
    EXPECT_CALL(queue_, CommitOffset(_, _, _)).WillRepeatedly(Return(core::Result<void>{}));
    // Every key the history leaves unwritten is absent in cold.
    EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Null()));

    consumer::Resolver::Config config;
    config.shard = 0;
    config.read_batch_size = 64;
    config.read_timeout = core::Duration{10};
    config.cold_lookup_timeout = std::chrono::milliseconds{50};
    config.stripe_count = 4;
    config.hot_apply_wait = std::chrono::milliseconds{50};
    consumer::Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config);
    const std::atomic<bool> cancel{false};
    EXPECT_TRUE(resolver.ReplayForRecovery(cancel).has_value());

    Outcome out;
    for (const auto& e : appended) {
      const auto* resolved = std::get_if<core::entry::Resolved>(&e.payload);
      if (resolved == nullptr || resolved->ref != ref) continue;
      for (const auto& cmd : resolved->materialised_ops) {
        out.effects.push_back(Canonical(cmd, c.at_ms));
      }
      out.reply = Describe(resolved->return_value);
      return out;
    }
    ADD_FAILURE() << "the resolver decided nothing";
    return out;
  }

  // Leaves out the DELs of keys that had expired: they are the new
  // path's observed expiries, which the resolver did not log.
  static Outcome NewOutcome(const EquivalenceCase& c) {
    hot::SingleShardStore store{hot::SingleShardConfig{}};
    // Every key the history leaves unwritten is known absent.
    store.Wipe(1);
    const core::EvictionPolicy policy;
    core::SequenceId seq = 2;
    for (const auto& step : c.history) {
      const core::RespCommand cmd{step.args};
      std::vector<core::Effect> effects = {
          {.key = cmd.args[1], .cmd = core::RespCommand{Canonical(cmd, step.at_ms)}}};
      store.ApplyEffects(effects, seq++, At(step.at_ms), policy, 0);
    }
    const auto lookup = [&store, &c](std::string_view key) { return store.View(key, 0, c.at_ms); };
    core::RespCommand cmd{c.cmd};
    Decision d = Decide(cmd, c.flags, c.at_ms, lookup);
    EXPECT_TRUE(d.needs_load.empty());
    Outcome out;
    if (d.error.has_value()) {
      out.reply = "error " + d.error->message();
      return out;
    }
    for (const auto& e : d.effects) {
      const bool expiry =
          e.cmd.args[0] == "DEL" && lookup(e.key).presence == testing::Presence::kExpired;
      if (!expiry) out.effects.push_back(e.cmd.args);
    }
    const auto replies = store.ApplyEffects(d.effects, seq, At(c.at_ms), policy, 0);
    if (d.reply.has_value()) {
      out.reply = Describe(d.reply);
    } else if (!replies.empty()) {
      out.reply = Describe(replies.back());
    }
    return out;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ::testing::NiceMock<abyss::testing::MockQueue> queue_;
  ::testing::NiceMock<abyss::testing::MockColdStore> cold_;
  EmptyBufferRouter buffer_router_;
  core::ConsumerRpc rpc_;
  core::ApplyNotifier apply_notifier_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

class DecideEquivalenceTest : public EquivalenceHarness,
                              public ::testing::WithParamInterface<EquivalenceCase> {};

std::string Ms(uint64_t ms) { return std::to_string(ms); }

std::vector<std::string> With(std::vector<std::string> head, const std::vector<std::string>& tail) {
  head.insert(head.end(), tail.begin(), tail.end());
  return head;
}

// The resolver's own unit cases.
std::vector<EquivalenceCase> ResolverCases() {
  const std::string field = std::string("f\x1F") + "x";
  std::vector<EquivalenceCase> out = {
      {.name = "SetnxOnAbsentKey", .cmd = {"SETNX", "k", "v"}, .flags = PredicateFlags::kNx},
      {.name = "SetnxOnPresentKey",
       .history = {{.args = {"SET", "k", "old"}}},
       .cmd = {"SETNX", "k", "v"},
       .flags = PredicateFlags::kNx},
      {.name = "SetXxOnAbsentKey", .cmd = {"SET", "k", "v", "XX"}, .flags = PredicateFlags::kXx},
      {.name = "MsetnxSkipsAllIfAnyExist",
       .history = {{.args = {"SET", "b", "x"}}},
       .cmd = {"MSETNX", "a", "1", "b", "2"},
       .flags = PredicateFlags::kMsetNx},
      {.name = "SetWithRelativeExThenSetNx",
       .history = {{.args = {"SET", "k", "v", "EX", "100"}, .at_ms = 1'000'000}},
       .cmd = {"SET", "k", "v2", "NX"},
       .flags = PredicateFlags::kNx,
       .at_ms = 1'050'000},
      {.name = "SetWithRelativePxThenXxKeepTtl",
       .history = {{.args = {"SET", "k", "v", "PX", "60000"}, .at_ms = 2'000'000}},
       .cmd = {"SET", "k", "v2", "XX", "KEEPTTL"},
       .flags = PredicateFlags::kXx | PredicateFlags::kKeepTtl,
       .at_ms = 2'000'001},
      {.name = "SetexThenSetNx",
       .history = {{.args = {"SETEX", "k", "100", "v"}, .at_ms = 3'000'000}},
       .cmd = {"SET", "k", "v2", "NX"},
       .flags = PredicateFlags::kNx,
       .at_ms = 3'050'000},
      {.name = "SetexExpiredThenSetNx",
       .history = {{.args = {"SETEX", "k", "1", "v"}, .at_ms = 4'000'000}},
       .cmd = {"SET", "k", "v2", "NX"},
       .flags = PredicateFlags::kNx,
       .at_ms = 4'002'000},
      {.name = "HdelThenHsetnx",
       .history = {{.args = {"HSET", "h", field, "v"}, .at_ms = 7'000'000},
                   {.args = {"HDEL", "h", field}, .at_ms = 7'000'100}},
       .cmd = {"HSETNX", "h", field, "v2"},
       .flags = PredicateFlags::kNx,
       .at_ms = 7'000'200},
      {.name = "ZremThenZaddNx",
       .history = {{.args = {"ZADD", "z", "1", "m"}, .at_ms = 8'000'000},
                   {.args = {"ZREM", "z", "m"}, .at_ms = 8'000'100}},
       .cmd = {"ZADD", "z", "NX", "2", "m"},
       .flags = PredicateFlags::kNx,
       .at_ms = 8'000'200},
      {.name = "DelThenHsetnx",
       .history = {{.args = {"HSET", "h", "f", "v"}, .at_ms = 9'000'000},
                   {.args = {"DEL", "h"}, .at_ms = 9'000'100}},
       .cmd = {"HSETNX", "h", "f", "v2"},
       .flags = PredicateFlags::kNx,
       .at_ms = 9'000'200},
  };
  // Each EXPIRE form's absolute TTL: GT refuses it exactly, and takes
  // one past it.
  const std::vector<std::pair<std::vector<std::string>, uint64_t>> expires = {
      {{"EXPIRE", "k", "100"}, 10'100'000},
      {{"PEXPIRE", "k", "200000"}, 10'200'000},
      {{"EXPIREAT", "k", "10300"}, 10'300'000},
      {{"PEXPIREAT", "k", "10400000"}, 10'400'000}};
  for (const auto& [expire, ttl] : expires) {
    for (const uint64_t ask : {ttl, ttl + 1}) {
      out.push_back({.name = "ExpireFamily" + expire[0] + (ask == ttl ? "Equal" : "Past"),
                     .history = {{.args = {"SET", "k", "v"}, .at_ms = 10'000'000},
                                 {.args = expire, .at_ms = 10'000'000}},
                     .cmd = {"PEXPIREAT", "k", Ms(ask), "GT"},
                     .flags = PredicateFlags::kExpireGt,
                     .at_ms = 10'000'000});
    }
  }
  return out;
}

struct Predicate {
  std::string name;
  PredicateFlags flags;
  std::vector<std::string> tokens;
};

std::vector<Predicate> SetPredicates() {
  using F = PredicateFlags;
  return {{"Nx", F::kNx, {"NX"}},
          {"NxGet", F::kNx | F::kGet, {"NX", "GET"}},
          {"NxKeepTtl", F::kNx | F::kKeepTtl, {"NX", "KEEPTTL"}},
          {"NxGetKeepTtl", F::kNx | F::kGet | F::kKeepTtl, {"NX", "GET", "KEEPTTL"}},
          {"Xx", F::kXx, {"XX"}},
          {"XxGet", F::kXx | F::kGet, {"XX", "GET"}},
          {"XxKeepTtl", F::kXx | F::kKeepTtl, {"XX", "KEEPTTL"}},
          {"XxGetKeepTtl", F::kXx | F::kGet | F::kKeepTtl, {"XX", "GET", "KEEPTTL"}},
          {"Get", F::kGet, {"GET"}},
          {"KeepTtl", F::kKeepTtl, {"KEEPTTL"}},
          {"GetKeepTtl", F::kGet | F::kKeepTtl, {"GET", "KEEPTTL"}}};
}

std::vector<Predicate> ZaddPredicates() {
  using F = PredicateFlags;
  return {{"Ch", F::kZAddCh, {"CH"}},
          {"Nx", F::kNx, {"NX"}},
          {"NxCh", F::kNx | F::kZAddCh, {"NX", "CH"}},
          {"Xx", F::kXx, {"XX"}},
          {"XxCh", F::kXx | F::kZAddCh, {"XX", "CH"}},
          {"Gt", F::kZAddGt, {"GT"}},
          {"GtCh", F::kZAddGt | F::kZAddCh, {"GT", "CH"}},
          {"Lt", F::kZAddLt, {"LT"}},
          {"LtCh", F::kZAddLt | F::kZAddCh, {"LT", "CH"}},
          {"XxGt", F::kXx | F::kZAddGt, {"XX", "GT"}},
          {"XxGtCh", F::kXx | F::kZAddGt | F::kZAddCh, {"XX", "GT", "CH"}},
          {"XxLt", F::kXx | F::kZAddLt, {"XX", "LT"}},
          {"XxLtCh", F::kXx | F::kZAddLt | F::kZAddCh, {"XX", "LT", "CH"}}};
}

std::vector<Predicate> ExpirePredicates() {
  using F = PredicateFlags;
  return {{"Nx", F::kNx, {"NX"}},
          {"Xx", F::kXx, {"XX"}},
          {"Gt", F::kExpireGt, {"GT"}},
          {"Lt", F::kExpireLt, {"LT"}},
          {"XxGt", F::kXx | F::kExpireGt, {"XX", "GT"}},
          {"XxLt", F::kXx | F::kExpireLt, {"XX", "LT"}}};
}

std::vector<EquivalenceCase> PredicateCases() {
  std::vector<EquivalenceCase> out;
  const std::vector<std::pair<std::string, std::vector<Step>>> strings = {
      {"WithTtl", {{.args = {"SET", "k", "old", "PXAT", Ms(kAt + 100'000)}}}},
      {"NoTtl", {{.args = {"SET", "k", "old"}}}},
      {"Absent", {}}};
  for (const auto& p : SetPredicates()) {
    for (const auto& [state, history] : strings) {
      out.push_back({.name = "Set" + p.name + state,
                     .history = history,
                     .cmd = With({"SET", "k", "v"}, p.tokens),
                     .flags = p.flags});
    }
  }
  for (const auto& [state, history] : strings) {
    out.push_back({.name = "Setnx" + state,
                   .history = history,
                   .cmd = {"SETNX", "k", "v"},
                   .flags = PredicateFlags::kNx});
  }
  for (const auto& p : ZaddPredicates()) {
    out.push_back({.name = "Zadd" + p.name + "Present",
                   .history = {{.args = {"ZADD", "k", "1", "a", "2", "b"}}},
                   .cmd = With(With({"ZADD", "k"}, p.tokens), {"3", "a", "1", "b"}),
                   .flags = p.flags});
    // GT without XX adds new members now; see ZaddGtAddsNewMembers.
    if (p.name == "Gt" || p.name == "GtCh") continue;
    out.push_back({.name = "Zadd" + p.name + "Absent",
                   .cmd = With(With({"ZADD", "k"}, p.tokens), {"3", "a", "1", "b"}),
                   .flags = p.flags});
  }
  for (const auto& p : ExpirePredicates()) {
    for (const auto& [state, history] : strings) {
      for (const char* ask : {"50000", "200000"}) {
        out.push_back({.name = "Pexpire" + p.name + state + ask,
                       .history = history,
                       .cmd = With({"PEXPIRE", "k", ask}, p.tokens),
                       .flags = p.flags});
      }
    }
  }
  const std::vector<Step> hash = {{.args = {"HSET", "h", "f", "v"}}};
  out.push_back({.name = "HsetnxFieldPresent",
                 .history = hash,
                 .cmd = {"HSETNX", "h", "f", "w"},
                 .flags = PredicateFlags::kNx});
  out.push_back({.name = "HsetnxFieldAbsent",
                 .history = hash,
                 .cmd = {"HSETNX", "h", "g", "w"},
                 .flags = PredicateFlags::kNx});
  out.push_back(
      {.name = "HsetnxKeyAbsent", .cmd = {"HSETNX", "h", "f", "w"}, .flags = PredicateFlags::kNx});
  out.push_back({.name = "MsetnxAllAbsent",
                 .cmd = {"MSETNX", "a", "1", "b", "2"},
                 .flags = PredicateFlags::kMsetNx});
  out.push_back({.name = "MsetnxOneExpired",
                 .history = {{.args = {"SET", "b", "x", "PX", "10"}, .at_ms = kAt - 20}},
                 .cmd = {"MSETNX", "a", "1", "b", "2"},
                 .flags = PredicateFlags::kMsetNx});
  const std::vector<Step> src = {{.args = {"SET", "src", "v", "PXAT", Ms(kAt + 100'000)}}};
  const std::vector<Step> both = {{.args = {"SET", "src", "v"}}, {.args = {"SET", "dst", "w"}}};
  out.push_back({.name = "RenamenxString",
                 .history = src,
                 .cmd = {"RENAMENX", "src", "dst"},
                 .flags = PredicateFlags::kNx});
  out.push_back({.name = "RenamenxOntoPresent",
                 .history = both,
                 .cmd = {"RENAMENX", "src", "dst"},
                 .flags = PredicateFlags::kNx});
  out.push_back({.name = "RenamenxMissingSource",
                 .cmd = {"RENAMENX", "src", "dst"},
                 .flags = PredicateFlags::kNx});
  out.push_back({.name = "CopyString",
                 .history = src,
                 .cmd = {"COPY", "src", "dst"},
                 .flags = PredicateFlags::kNx});
  out.push_back({.name = "CopyOntoPresent",
                 .history = both,
                 .cmd = {"COPY", "src", "dst"},
                 .flags = PredicateFlags::kNx});
  out.push_back({.name = "CopyReplace", .history = both, .cmd = {"COPY", "src", "dst", "REPLACE"}});
  out.push_back(
      {.name = "CopyMissingSource", .cmd = {"COPY", "src", "dst"}, .flags = PredicateFlags::kNx});
  return out;
}

TEST_P(DecideEquivalenceTest, MatchesTheResolver) {
  const EquivalenceCase& c = GetParam();
  const Outcome resolver = ResolverOutcome(c);
  const Outcome decided = NewOutcome(c);
  EXPECT_EQ(decided.effects, resolver.effects);
  EXPECT_EQ(decided.reply, resolver.reply);
}

INSTANTIATE_TEST_SUITE_P(ResolverUnitCases, DecideEquivalenceTest,
                         ::testing::ValuesIn(ResolverCases()),
                         [](const auto& info) { return info.param.name; });
INSTANTIATE_TEST_SUITE_P(Predicates, DecideEquivalenceTest, ::testing::ValuesIn(PredicateCases()),
                         [](const auto& info) { return info.param.name; });

// Where the new path deliberately differs, both sides are pinned.
using DecideDifferenceTest = EquivalenceHarness;

TEST_F(DecideDifferenceTest, ZaddGtAddsNewMembers) {
  const EquivalenceCase c{.name = "",
                          .history = {{.args = {"ZADD", "k", "1", "a"}}},
                          .cmd = {"ZADD", "k", "GT", "2", "a", "5", "c"},
                          .flags = PredicateFlags::kZAddGt};
  EXPECT_EQ(ResolverOutcome(c).effects,
            (std::vector<std::vector<std::string>>{{"ZADD", "k", "2", "a"}}));
  const Outcome decided = NewOutcome(c);
  EXPECT_EQ(decided.effects,
            (std::vector<std::vector<std::string>>{{"ZADD", "k", "2", "a", "5", "c"}}));
  EXPECT_EQ(decided.reply, ":1");
}

TEST_F(DecideDifferenceTest, KeepTtlOnAnExpiredKeyKeepsNone) {
  const EquivalenceCase c{.name = "",
                          .history = {{.args = {"SET", "k", "old", "PX", "10"}, .at_ms = kAt - 20}},
                          .cmd = {"SET", "k", "v", "KEEPTTL"},
                          .flags = PredicateFlags::kKeepTtl};
  EXPECT_EQ(ResolverOutcome(c).effects,
            (std::vector<std::vector<std::string>>{{"SET", "k", "v", "PXAT", Ms(kAt - 10)}}));
  EXPECT_EQ(NewOutcome(c).effects, (std::vector<std::vector<std::string>>{{"SET", "k", "v"}}));
}

TEST_F(DecideDifferenceTest, RenamenxMovesEveryType) {
  const EquivalenceCase c{.name = "",
                          .history = {{.args = {"SADD", "src", "m"}}},
                          .cmd = {"RENAMENX", "src", "dst"},
                          .flags = PredicateFlags::kNx};
  const Outcome resolver = ResolverOutcome(c);
  EXPECT_TRUE(resolver.effects.empty());
  EXPECT_EQ(resolver.reply.substr(0, 1), "-");
  const Outcome decided = NewOutcome(c);
  EXPECT_EQ(decided.effects,
            (std::vector<std::vector<std::string>>{{"DEL", "src"}, {"SADD", "dst", "m"}}));
  EXPECT_EQ(decided.reply, ":1");
}

TEST_F(DecideDifferenceTest, WrongTypeIsRefusedBeforeLogging) {
  const EquivalenceCase set_get{.name = "",
                                .history = {{.args = {"SADD", "k", "m"}}},
                                .cmd = {"SET", "k", "v", "GET"},
                                .flags = PredicateFlags::kGet};
  EXPECT_EQ(ResolverOutcome(set_get).effects,
            (std::vector<std::vector<std::string>>{{"SET", "k", "v"}}));
  EXPECT_EQ(NewOutcome(set_get).reply.substr(0, 15), "error Operation");

  const EquivalenceCase hsetnx{.name = "",
                               .history = {{.args = {"SET", "k", "v"}}},
                               .cmd = {"HSETNX", "k", "f", "w"},
                               .flags = PredicateFlags::kNx};
  EXPECT_EQ(ResolverOutcome(hsetnx).effects,
            (std::vector<std::vector<std::string>>{{"HSET", "k", "f", "w"}}));
  const Outcome decided = NewOutcome(hsetnx);
  EXPECT_TRUE(decided.effects.empty());
  EXPECT_EQ(decided.reply.substr(0, 15), "error Operation");
}

}  // namespace
}  // namespace abyss::engine
