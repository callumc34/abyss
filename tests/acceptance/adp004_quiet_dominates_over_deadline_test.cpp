#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "integration_harness.h"

namespace abyss::acceptance {
namespace {

using namespace std::chrono_literals;

class Adp004QuietDominatesTest : public ::testing::Test {
 protected:
  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::IntegrationHarness harness_;
};

TEST_F(Adp004QuietDominatesTest, BurstWorkloadCollapsesToOneColdWrite) {
  harness_.HotPool().Stop();

  constexpr int kWrites = 100;
  std::vector<core::QueueEntry> entries;
  entries.reserve(kWrites);
  for (int i = 0; i < kWrites; ++i) {
    entries.push_back(core::QueueEntry{
        .seq = static_cast<core::SequenceId>(i + 1),
        .appended_at = harness_.Clock().WallNow(),
        .payload = core::entry::Write{.cmd =
                                          core::RespCommand{
                                              .args = {"SET", "k", "v" + std::to_string(i)},
                                          }},
    });
  }

  EXPECT_CALL(harness_.Queue(), Read(core::kColdConsumer, ::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::Return(entries))
      .WillRepeatedly(::testing::Return(std::vector<core::QueueEntry>{}));

  auto& consumer = harness_.ColdPool().ConsumerFor(0);
  consumer.Drain();
  consumer.Flush();
  harness_.Clock().Advance(60s);
  consumer.Drain();
  consumer.Flush();

  const auto snap = consumer.Snapshot();
  EXPECT_EQ(snap.ops_flushed, 1U);
  EXPECT_GT(snap.flushes_quiet, 0U);
  EXPECT_EQ(snap.flushes_deadline, 0U);
}

}  // namespace
}  // namespace abyss::acceptance
