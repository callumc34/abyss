#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <string>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "alloc_counter.h"
#include "test_clock.h"

namespace abyss {
namespace {

using namespace std::chrono_literals;
namespace ops = core::ops;

// Past any small-string buffer, so a std::string of one allocates.
std::string LongKey(char c) {
  std::string key(64, c);
  return key;
}

TEST(LookupAllocTest, HotReadsOfLongKeysAllocateNothing) {
  testing::TestClock clock;
  core::EvictionPolicy policy{core::EvictionTTL{1}};
  hot::ShardedHotStore hot{hot::ShardedHotStoreConfig{
      .max_memory_bytes = size_t{1} << 20,
      .shard_count = 1,
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const std::string stub = LongKey('x');
  const std::string str = LongKey('k');
  const std::string set = LongKey('s');
  const std::string hash = LongKey('h');
  const std::string zset = LongKey('z');
  const std::string member = LongKey('m');
  const std::string absent = LongKey('a');
  const auto apply = [&hot](const ops::WriteOp& op) {
    ASSERT_TRUE(hot.Apply(op, core::kFirstSeq).has_value());
  };
  // Evicted, so a stub answers it.
  apply(ops::StringSet{.key = stub, .value = "v"});
  clock.Advance(2s);
  ASSERT_EQ(hot.EvictExpired(clock.SteadyNow()).by_deadline, 1U);
  apply(ops::StringSet{.key = str, .value = "v"});
  apply(ops::SetAdd{.key = set, .members = {member}});
  apply(ops::HashSet{.key = hash, .fields = {{.field = member, .value = "v"}}});
  apply(ops::ZsetAdd{.key = zset, .entries = {{.score = 1.5, .member = member}}});
  hot.SetAccessTime(clock.SteadyNow());
  const ops::ReadOp get{ops::StringGet{.key = str}};
  const ops::ReadOp is_member{ops::SetIsMember{.key = set, .member = member}};
  const ops::ReadOp hget{ops::HashGet{.key = hash, .field = member}};
  const ops::ReadOp score{ops::ZsetScore{.key = zset, .member = member}};
  const ops::ReadOp stub_type{ops::Type{.key = stub}};
  const ops::ReadOp miss{ops::StringGet{.key = absent}};

  const size_t before = testing::Allocs();
  const auto got = hot.Read(get);
  const auto is = hot.Read(is_member);
  const auto field = hot.Read(hget);
  const auto scored = hot.Read(score);
  const auto typed = hot.Read(stub_type);
  const auto missed = hot.Read(miss);
  const size_t allocs = testing::Allocs() - before;

  EXPECT_EQ(allocs, 0U);
  ASSERT_TRUE(got.result.has_value());
  EXPECT_EQ(got.result->AsString(), "v");
  ASSERT_TRUE(is.result.has_value());
  EXPECT_EQ(is.result->AsInteger(), 1);
  ASSERT_TRUE(field.result.has_value());
  EXPECT_EQ(field.result->AsString(), "v");
  ASSERT_TRUE(scored.result.has_value());
  EXPECT_EQ(scored.result->AsString(), "1.5");
  ASSERT_TRUE(typed.result.has_value()) << "a stub answers TYPE, keeping no access";
  EXPECT_EQ(typed.result->AsString(), "string");
  EXPECT_FALSE(missed.result.has_value());
}

}  // namespace
}  // namespace abyss
