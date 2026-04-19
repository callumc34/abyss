#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "integration_harness.h"
#include "mock_cold_store.h"

namespace abyss::acceptance {
namespace {

using namespace std::chrono_literals;

class Adp004ColdStallTest : public ::testing::Test {
 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::IntegrationHarness harness_;
};

TEST_F(Adp004ColdStallTest, ColdCorruptionPoisonsButDoesNotSpin) {
  harness_.HotPool().Stop();

  // NOLINTNEXTLINE(misc-const-correctness)
  testing::MockColdStore mock_cold;
  ON_CALL(mock_cold, ApplyBatch(::testing::_))
      .WillByDefault(::testing::Return(
          std::unexpected(core::Error{core::ErrorCode::kCorruption, "bad data"})));

  consumer::ColdConsumer consumer(harness_.Queue(), mock_cold, /*shard=*/0,
                                  consumer::ColdConsumer::Config{.quiet_threshold = 30s,
                                                                 .jitter_fraction = 0.0,
                                                                 .retry_initial_backoff = 0ms,
                                                                 .rng_seed = 42},
                                  core::EvictionPolicy{core::EvictionTTL{3600}},
                                  harness_.Clock().SteadyFn(), harness_.Clock().WallFn());

  std::vector<core::QueueEntry> entries{core::QueueEntry{
      .seq = 1,
      .appended_at = harness_.Clock().WallNow(),
      .payload = core::entry::Write{.cmd = core::RespCommand{.args = {"SET", "k", "v"}}},
  }};

  EXPECT_CALL(harness_.Queue(), Read(core::kColdConsumer, ::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::Return(entries))
      .WillRepeatedly(::testing::Return(std::vector<core::QueueEntry>{}));

  EXPECT_CALL(mock_cold, ApplyBatch(::testing::_)).Times(::testing::AtMost(1));

  consumer.Drain();
  consumer.Flush();
  harness_.Clock().Advance(60s);
  consumer.Drain();
  consumer.Flush();

  EXPECT_EQ(consumer.Snapshot().apply_poisoned, 1U);
  EXPECT_EQ(consumer.Snapshot().retry_attempts, 0U);
}

}  // namespace
}  // namespace abyss::acceptance
