#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/core/shard_router.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "integration_harness.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;

class TieredReadBufferHitTest : public ::testing::Test {
 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::IntegrationHarness harness_;

  core::RespCommand MakeCmd(std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  }

  double BufferHits() const {
    return metrics::testing::GetCounterValue(metrics::names::kHitsTotal, metrics::Tier::kBuffer)
        .value_or(0.0);
  }

  double HotHits() const {
    return metrics::testing::GetCounterValue(metrics::names::kHitsTotal, metrics::Tier::kHot)
        .value_or(0.0);
  }
};

TEST_F(TieredReadBufferHitTest, ReadAfterHotEvictionFallsThroughToBuffer) {
  // Seed both hot and the compaction buffer for the same key. Hot serves the
  // first read; after EvictExpired() drops it from hot under a future steady
  // time, the next read falls through to the buffer and the buffer-tier hit
  // counter advances: the buffer's delta is the whole string, so cold
  // is never read. Not naturally observable in steady-state production
  // timing because cold's safety margin guarantees a flush before hot
  // eviction (ADP-004 §Flush Strategy).
  ASSERT_TRUE(harness_.SeedHot({"SET", "key_b", "v"}).has_value());

  // Drive cold to absorb the SET so the buffer holds it AND cold's
  // latest_drained_seq matches hot's settled seq.
  const auto shard = core::ComputeShard("key_b", testing::IntegrationHarness::kShardCount);
  harness_.ColdPool().ConsumerFor(shard).Drain();

  const double hot_before = HotHits();
  const double buffer_before = BufferHits();

  auto hot_read = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "key_b"}));
  ASSERT_TRUE(hot_read.has_value()) << hot_read.error().message();
  EXPECT_EQ(hot_read->AsString(), "v");
  EXPECT_EQ(HotHits(), hot_before + 1.0) << "first read should serve from hot";
  EXPECT_EQ(BufferHits(), buffer_before) << "first read should not touch the buffer";

  // Force the hot store past every key's eviction deadline. EvictExpired
  // drops the entry; SingleShardStore returns it to the LRU/eviction pool
  // and a subsequent read misses hot. Advance past kEviction (24h default
  // for the harness) by a wide margin.
  harness_.Clock().Advance(48h);
  const auto evicted = harness_.ShardedHot().EvictExpired(harness_.Clock().SteadyNow());
  ASSERT_GE(evicted.Total(), 1U) << "expected key_b to evict from hot";

  auto buffer_read = harness_.Engine().DispatchRead("GET", MakeCmd({"GET", "key_b"}));
  ASSERT_TRUE(buffer_read.has_value()) << buffer_read.error().message();
  EXPECT_EQ(buffer_read->AsString(), "v");
  EXPECT_EQ(HotHits(), hot_before + 1.0)
      << "second read missed hot, so hot counter should not advance";
  EXPECT_EQ(BufferHits(), buffer_before + 1.0)
      << "second read should serve from the compaction buffer";
}

}  // namespace
}  // namespace abyss::engine
