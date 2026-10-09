#include "abyss/resp/command_registry.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/ops.h"

namespace abyss::resp {
namespace {

using Status = CommandRegistry::ResolveStatus;

// Advertising a write command that no tier can materialise means accepting it,
// durably logging it, and only then failing at apply -- and until the registry
// was trimmed it also meant a WAL entry that no build could ever replay. The
// registry and the parser table have to agree, so assert it rather than trust
// two hand-maintained lists to stay in step.
//
// Fan-out writes are exempt: the sequencer decides them into per-key
// effects, so MSET never needs a parser of its own. The writes that are
// always conditional are exempt too: the sequencer decides them whole,
// and logs effects that do parse.
TEST(CommandRegistryTest, EveryWriteHasAParserButTheAlwaysConditionalOnes) {
  CommandRegistry reg;
  std::vector<std::string> missing;
  for (const auto& spec : reg.All()) {
    if (spec.dispatch != Dispatch::kWritePath) continue;
    if (spec.multi_key_kind != core::MultiKeyKind::kNone) continue;
    if (core::ops::HasWriteParser(spec.name)) continue;
    missing.emplace_back(spec.name);
    EXPECT_NE(spec.predicate, nullptr) << spec.name << " is unconditional with no parser";
  }
  EXPECT_THAT(missing,
              ::testing::UnorderedElementsAre("SETNX", "MSETNX", "RENAMENX", "COPY", "HSETNX"));
}

TEST(CommandRegistryTest, FindsKnownCommand) {
  CommandRegistry reg;
  const auto* spec = reg.Find("GET");
  ASSERT_NE(spec, nullptr);
  EXPECT_EQ(spec->name, "GET");
  EXPECT_EQ(spec->arity, 2);
  EXPECT_EQ(spec->cls, CommandClass::kRead);
  EXPECT_EQ(spec->dispatch, Dispatch::kTieredRead);
}

TEST(CommandRegistryTest, LookupIsCaseInsensitive) {
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("get"), reg.Find("GET"));
  EXPECT_EQ(reg.Find("Get"), reg.Find("GET"));
  EXPECT_EQ(reg.Find("gEt"), reg.Find("GET"));
}

TEST(CommandRegistryTest, UnknownCommandReturnsNull) {
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("LPUSH"), nullptr);
  EXPECT_EQ(reg.Find("RPUSH"), nullptr);
  EXPECT_EQ(reg.Find("KEYS"), nullptr);
  EXPECT_EQ(reg.Find("WAIT"), nullptr);
  EXPECT_EQ(reg.Find("MULTI"), nullptr);
  EXPECT_EQ(reg.Find("SUBSCRIBE"), nullptr);
  EXPECT_EQ(reg.Find("EVAL"), nullptr);
  EXPECT_EQ(reg.Find("XADD"), nullptr);
  EXPECT_EQ(reg.Find("GIBBERISH"), nullptr);
}

TEST(CommandRegistryTest, FlushallAndFlushdbAreRegisteredAsFlushDispatch) {
  CommandRegistry reg;
  const auto* flushdb = reg.Find("FLUSHDB");
  ASSERT_NE(flushdb, nullptr);
  EXPECT_EQ(flushdb->cls, CommandClass::kAdmin);
  EXPECT_EQ(flushdb->dispatch, Dispatch::kFlush);
  EXPECT_FALSE(flushdb->loading_safe);

  const auto* flushall = reg.Find("FLUSHALL");
  ASSERT_NE(flushall, nullptr);
  EXPECT_EQ(flushall->cls, CommandClass::kAdmin);
  EXPECT_EQ(flushall->dispatch, Dispatch::kFlush);
  EXPECT_FALSE(flushall->loading_safe);
}

// Conditional writes take the write path, and keep their predicate
// extraction.
TEST(CommandRegistryTest, ConditionalWritesTakeTheWritePathWithTheirPredicates) {
  CommandRegistry reg;
  for (const std::string_view name : {"SETNX", "MSETNX", "RENAMENX", "COPY", "HSETNX"}) {
    const auto* spec = reg.Find(name);
    ASSERT_NE(spec, nullptr) << name;
    EXPECT_EQ(spec->dispatch, Dispatch::kWritePath) << name;
    EXPECT_NE(spec->predicate, nullptr) << name;
  }
}

TEST(CommandRegistryTest, SetHasWritePathBaseDispatch) {
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("SET")->dispatch, Dispatch::kWritePath);
  EXPECT_EQ(reg.Find("ZADD")->dispatch, Dispatch::kWritePath);
}

TEST(CommandRegistryTest, AdminCommands) {
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("DBSIZE")->dispatch, Dispatch::kAdmin);
}

TEST(CommandRegistryTest, StatelessAdminCommands) {
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("INFO")->dispatch, Dispatch::kStateless);
  EXPECT_EQ(reg.Find("CLUSTER")->dispatch, Dispatch::kStateless);
  EXPECT_EQ(reg.Find("CONFIG")->dispatch, Dispatch::kStateless);
}

TEST(CommandRegistryTest, ParentLoadingSafeOnlyAppliesAbsentSubcommand) {
  CommandRegistry reg;
  // PING has no subcommands; loading_safe is authoritative on the parent.
  EXPECT_TRUE(reg.Find("PING")->loading_safe);
  EXPECT_TRUE(reg.Find("HELLO")->loading_safe);
  EXPECT_TRUE(reg.Find("QUIT")->loading_safe);
  EXPECT_TRUE(reg.Find("INFO")->loading_safe);
  // CLUSTER's per-subcommand allowlist is the source of truth during loading.
  EXPECT_FALSE(reg.Find("CLUSTER")->loading_safe);
  EXPECT_FALSE(reg.Find("CLIENT")->loading_safe);
  EXPECT_FALSE(reg.Find("CONFIG")->loading_safe);
  // COMMAND with no subcommand is permitted during loading.
  EXPECT_TRUE(reg.Find("COMMAND")->loading_safe);
  // Data-plane commands are never loading-safe.
  EXPECT_FALSE(reg.Find("GET")->loading_safe);
  EXPECT_FALSE(reg.Find("SET")->loading_safe);
  EXPECT_FALSE(reg.Find("DEL")->loading_safe);
}

TEST(CommandRegistryTest, HashCommandSurfaceRegistered) {
  CommandRegistry reg;
  struct Expected {
    std::string_view name;
    int arity;
    CommandClass cls;
    Dispatch dispatch;
  };
  constexpr std::array<Expected, 6> kExpected{{
      {"HMGET", -3, CommandClass::kRead, Dispatch::kTieredRead},
      {"HMSET", -4, CommandClass::kWrite, Dispatch::kWritePath},
      {"HEXISTS", 3, CommandClass::kRead, Dispatch::kTieredRead},
      {"HKEYS", 2, CommandClass::kRead, Dispatch::kTieredRead},
      {"HVALS", 2, CommandClass::kRead, Dispatch::kTieredRead},
      {"HLEN", 2, CommandClass::kRead, Dispatch::kTieredRead},
  }};
  for (const auto& e : kExpected) {
    const auto* spec = reg.Find(e.name);
    ASSERT_NE(spec, nullptr) << e.name;
    EXPECT_EQ(spec->arity, e.arity) << e.name;
    EXPECT_EQ(spec->cls, e.cls) << e.name;
    EXPECT_EQ(spec->dispatch, e.dispatch) << e.name;
    EXPECT_EQ(spec->first_key, 1) << e.name;
    EXPECT_EQ(spec->last_key, 1) << e.name;
    EXPECT_EQ(spec->key_step, 1) << e.name;
  }
}

TEST(CommandRegistryTest, HmsetArityMismatch) {
  CommandRegistry reg;
  core::RespCommand too_few{{"HMSET", "h"}};
  EXPECT_EQ(reg.Resolve(too_few).status, Status::kArityMismatch);
  core::RespCommand ok{{"HMSET", "h", "f", "v"}};
  EXPECT_EQ(reg.Resolve(ok).status, Status::kOk);
}

TEST(CommandRegistryTest, HmgetArityMismatch) {
  CommandRegistry reg;
  core::RespCommand too_few{{"HMGET", "h"}};
  EXPECT_EQ(reg.Resolve(too_few).status, Status::kArityMismatch);
  core::RespCommand ok{{"HMGET", "h", "f"}};
  EXPECT_EQ(reg.Resolve(ok).status, Status::kOk);
}

TEST(CommandRegistryTest, ClusterSubcommandsCarryLoadingAllowlist) {
  CommandRegistry reg;
  const auto* cluster = reg.Find("CLUSTER");
  ASSERT_NE(cluster, nullptr);
  ASSERT_FALSE(cluster->subcommands.empty());

  for (const auto* allow : {"SLOTS", "INFO", "MYID"}) {
    const auto* sub = reg.FindSubcommand(*cluster, allow);
    ASSERT_NE(sub, nullptr) << allow;
    EXPECT_TRUE(sub->loading_safe) << allow;
  }
  for (const auto* deny : {"SHARDS", "NODES", "KEYSLOT", "COUNTKEYSINSLOT"}) {
    const auto* sub = reg.FindSubcommand(*cluster, deny);
    ASSERT_NE(sub, nullptr) << deny;
    EXPECT_FALSE(sub->loading_safe) << deny;
  }
}

TEST(CommandRegistryTest, ObjectSubcommandsHaveDistinctDispatch) {
  CommandRegistry reg;
  const auto* object = reg.Find("OBJECT");
  ASSERT_NE(object, nullptr);
  const auto* enc = reg.FindSubcommand(*object, "encoding");
  const auto* idle = reg.FindSubcommand(*object, "IDLETIME");
  ASSERT_NE(enc, nullptr);
  ASSERT_NE(idle, nullptr);
  EXPECT_EQ(enc->dispatch, Dispatch::kTieredRead);
  EXPECT_EQ(idle->dispatch, Dispatch::kAdmin);
}

TEST(CommandRegistryTest, ConfigOnlyExposesGetSubcommand) {
  CommandRegistry reg;
  const auto* config = reg.Find("CONFIG");
  ASSERT_NE(config, nullptr);
  EXPECT_NE(reg.FindSubcommand(*config, "GET"), nullptr);
  EXPECT_EQ(reg.FindSubcommand(*config, "SET"), nullptr);
  EXPECT_EQ(reg.FindSubcommand(*config, "REWRITE"), nullptr);
  EXPECT_EQ(reg.FindSubcommand(*config, "RESETSTAT"), nullptr);
}

TEST(CommandRegistryTest, DocsPopulatedForEveryCommandAndSubcommand) {
  CommandRegistry reg;
  for (const auto& spec : reg.All()) {
    EXPECT_FALSE(spec.docs.summary.empty()) << spec.name;
    EXPECT_FALSE(spec.docs.since.empty()) << spec.name;
    EXPECT_FALSE(spec.docs.group.empty()) << spec.name;
    EXPECT_FALSE(spec.docs.complexity.empty()) << spec.name;
    for (const auto& sub : spec.subcommands) {
      EXPECT_FALSE(sub.docs.summary.empty()) << spec.name << "|" << sub.name;
      EXPECT_FALSE(sub.docs.since.empty()) << spec.name << "|" << sub.name;
    }
  }
}

TEST(CommandRegistryTest, ResolveUnknownCommand) {
  CommandRegistry reg;
  core::RespCommand cmd{{"LPUSH", "mylist", "value"}};
  const auto r = reg.Resolve(cmd);
  EXPECT_EQ(r.status, Status::kUnknownCommand);
}

TEST(CommandRegistryTest, ResolveExactArityMatch) {
  CommandRegistry reg;
  core::RespCommand cmd{{"GET", "key"}};
  const auto r = reg.Resolve(cmd);
  ASSERT_EQ(r.status, Status::kOk);
  EXPECT_EQ(r.resolved.parent->name, "GET");
  EXPECT_EQ(r.resolved.subcommand, nullptr);
}

TEST(CommandRegistryTest, ResolveExactArityMismatchOnParent) {
  CommandRegistry reg;
  core::RespCommand cmd{{"GET"}};
  EXPECT_EQ(reg.Resolve(cmd).status, Status::kArityMismatch);
  core::RespCommand too_many{{"GET", "key", "extra"}};
  EXPECT_EQ(reg.Resolve(too_many).status, Status::kArityMismatch);
}

TEST(CommandRegistryTest, ResolveSubcommandMatchAndArityCheck) {
  CommandRegistry reg;
  core::RespCommand setname{{"CLIENT", "SETNAME", "alice"}};
  const auto r = reg.Resolve(setname);
  ASSERT_EQ(r.status, Status::kOk);
  ASSERT_NE(r.resolved.subcommand, nullptr);
  EXPECT_EQ(r.resolved.subcommand->name, "SETNAME");

  core::RespCommand setname_bad{{"CLIENT", "SETNAME"}};
  EXPECT_EQ(reg.Resolve(setname_bad).status, Status::kArityMismatch);
}

TEST(CommandRegistryTest, ResolveUnknownSubcommandReportsError) {
  CommandRegistry reg;
  core::RespCommand cmd{{"CLUSTER", "NOSUCH"}};
  const auto r = reg.Resolve(cmd);
  EXPECT_EQ(r.status, Status::kUnknownSubcommand);
  ASSERT_NE(r.resolved.parent, nullptr);
  EXPECT_EQ(r.resolved.parent->name, "CLUSTER");
}

TEST(CommandRegistryTest, ResolveContainerWithoutSubArgUsesParent) {
  CommandRegistry reg;
  // COMMAND alone (arity -1) resolves with no subcommand and uses parent dispatch.
  core::RespCommand cmd{{"COMMAND"}};
  const auto r = reg.Resolve(cmd);
  ASSERT_EQ(r.status, Status::kOk);
  ASSERT_NE(r.resolved.parent, nullptr);
  EXPECT_EQ(r.resolved.parent->name, "COMMAND");
  EXPECT_EQ(r.resolved.subcommand, nullptr);
  EXPECT_TRUE(r.resolved.LoadingSafe());
}

TEST(CommandRegistryTest, ResolveVariadicArityAccepted) {
  CommandRegistry reg;
  core::RespCommand two{{"MGET", "k1"}};
  core::RespCommand many{{"MGET", "k1", "k2", "k3", "k4"}};
  EXPECT_EQ(reg.Resolve(two).status, Status::kOk);
  EXPECT_EQ(reg.Resolve(many).status, Status::kOk);
  core::RespCommand too_few{{"MGET"}};
  EXPECT_EQ(reg.Resolve(too_few).status, Status::kArityMismatch);
}

TEST(CommandRegistryTest, ResolveCaseInsensitiveName) {
  CommandRegistry reg;
  core::RespCommand cmd{{"set", "k", "v"}};
  const auto r = reg.Resolve(cmd);
  ASSERT_EQ(r.status, Status::kOk);
  EXPECT_EQ(r.resolved.parent->name, "SET");
}

TEST(CommandRegistryTest, ResolveEmptyCommandIsUnknown) {
  CommandRegistry reg;
  core::RespCommand cmd{{}};
  EXPECT_EQ(reg.Resolve(cmd).status, Status::kUnknownCommand);
}

TEST(CommandRegistryTest, GlobalRegistryIsSharedInstance) {
  const auto& r1 = GlobalRegistry();
  const auto& r2 = GlobalRegistry();
  EXPECT_EQ(&r1, &r2);
}

TEST(CommandRegistryTest, CoverageIncludesStringsSetsSortedSetsAndGeneric) {
  CommandRegistry reg;
  EXPECT_NE(reg.Find("SADD"), nullptr);
  EXPECT_NE(reg.Find("SMEMBERS"), nullptr);
  EXPECT_NE(reg.Find("ZADD"), nullptr);
  EXPECT_NE(reg.Find("ZRANGEBYSCORE"), nullptr);
  EXPECT_NE(reg.Find("EXPIRE"), nullptr);
  EXPECT_NE(reg.Find("TYPE"), nullptr);
  EXPECT_NE(reg.Find("DEL"), nullptr);
  EXPECT_NE(reg.Find("CLUSTER"), nullptr);
}

}  // namespace
}  // namespace abyss::resp
