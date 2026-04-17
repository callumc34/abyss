#include "abyss/resp/command_registry.h"

#include <gtest/gtest.h>

namespace abyss::resp {
namespace {

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
  EXPECT_EQ(reg.Find("HGET"), nullptr);   // hashes not yet supported
  EXPECT_EQ(reg.Find("LPUSH"), nullptr);  // lists not yet supported
  EXPECT_EQ(reg.Find("KEYS"), nullptr);   // enumeration not yet supported
  EXPECT_EQ(reg.Find("WAIT"), nullptr);
  EXPECT_EQ(reg.Find("MULTI"), nullptr);
  EXPECT_EQ(reg.Find("SUBSCRIBE"), nullptr);
  EXPECT_EQ(reg.Find("EVAL"), nullptr);
  EXPECT_EQ(reg.Find("XADD"), nullptr);
  EXPECT_EQ(reg.Find("GIBBERISH"), nullptr);
}

TEST(CommandRegistryTest, ConditionalWritesAreMarked) {
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("SETNX")->dispatch, Dispatch::kConditionalWrite);
  EXPECT_EQ(reg.Find("MSETNX")->dispatch, Dispatch::kConditionalWrite);
  EXPECT_EQ(reg.Find("RENAMENX")->dispatch, Dispatch::kConditionalWrite);
  EXPECT_EQ(reg.Find("COPY")->dispatch, Dispatch::kConditionalWrite);
}

TEST(CommandRegistryTest, SetHasWritePathBaseDispatch) {
  // SET itself is base kWritePath; the request pipeline upgrades to
  // kConditionalWrite after parsing NX/XX/KEEPTTL/GET options.
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("SET")->dispatch, Dispatch::kWritePath);
  EXPECT_EQ(reg.Find("ZADD")->dispatch, Dispatch::kWritePath);
}

TEST(CommandRegistryTest, ConsumerRpcCommands) {
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("DBSIZE")->dispatch, Dispatch::kConsumerRpc);
}

TEST(CommandRegistryTest, StatelessAdminCommands) {
  CommandRegistry reg;
  EXPECT_EQ(reg.Find("INFO")->dispatch, Dispatch::kStateless);
  EXPECT_EQ(reg.Find("FLUSHALL")->dispatch, Dispatch::kStateless);
  EXPECT_EQ(reg.Find("FLUSHDB")->dispatch, Dispatch::kStateless);
}

TEST(CommandRegistryTest, ClassifyUnknownCommandReturnsNotFound) {
  CommandRegistry reg;
  core::RespCommand cmd{{"HGET", "myhash", "field"}};
  auto result = reg.Classify(cmd);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
  EXPECT_EQ(result.error().message(), "HGET");
}

TEST(CommandRegistryTest, ClassifyExactArityMatch) {
  CommandRegistry reg;
  core::RespCommand cmd{{"GET", "key"}};
  auto result = reg.Classify(cmd);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ((*result)->name, "GET");
}

TEST(CommandRegistryTest, ClassifyExactArityMismatchIsInvalidArgument) {
  CommandRegistry reg;
  core::RespCommand cmd{{"GET"}};  // GET requires exactly 2 args (GET + key)
  auto result = reg.Classify(cmd);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_EQ(result.error().message(), "GET");

  core::RespCommand too_many{{"GET", "key", "extra"}};
  auto result2 = reg.Classify(too_many);
  ASSERT_FALSE(result2.has_value());
  EXPECT_EQ(result2.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(CommandRegistryTest, ClassifyVariadicArityAccepted) {
  CommandRegistry reg;
  // MGET: arity -2 (at least 2 args including command name).
  core::RespCommand two{{"MGET", "k1"}};
  core::RespCommand many{{"MGET", "k1", "k2", "k3", "k4"}};
  EXPECT_TRUE(reg.Classify(two).has_value());
  EXPECT_TRUE(reg.Classify(many).has_value());

  core::RespCommand too_few{{"MGET"}};
  auto result = reg.Classify(too_few);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(CommandRegistryTest, ClassifyCaseInsensitiveName) {
  CommandRegistry reg;
  core::RespCommand cmd{{"set", "k", "v"}};
  auto result = reg.Classify(cmd);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ((*result)->name, "SET");
}

TEST(CommandRegistryTest, ClassifyEmptyCommandIsInvalidArgument) {
  CommandRegistry reg;
  core::RespCommand cmd{{}};
  auto result = reg.Classify(cmd);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(CommandRegistryTest, GlobalRegistryIsSharedInstance) {
  const auto& r1 = GlobalRegistry();
  const auto& r2 = GlobalRegistry();
  EXPECT_EQ(&r1, &r2);
}

TEST(CommandRegistryTest, CoverageIncludesStringsSetsSortedSetsAndGeneric) {
  CommandRegistry reg;
  // Spot-check a representative from each family.
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
