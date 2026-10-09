#include "abyss/metrics/names.h"

#include <gtest/gtest.h>

#include <set>
#include <string_view>

namespace abyss::metrics {
namespace {

TEST(Names, LabelKeyStringSpellings) {
  EXPECT_EQ(ToStringView(LabelKey::kTier), "tier");
  EXPECT_EQ(ToStringView(LabelKey::kShard), "shard");
  EXPECT_EQ(ToStringView(LabelKey::kCmd), "cmd");
  EXPECT_EQ(ToStringView(LabelKey::kReason), "reason");
  EXPECT_EQ(ToStringView(LabelKey::kStatus), "status");
  EXPECT_EQ(ToStringView(LabelKey::kOp), "op");
  EXPECT_EQ(ToStringView(LabelKey::kOutcome), "outcome");
  EXPECT_EQ(ToStringView(LabelKey::kPass), "pass");
}

TEST(Names, TierEnumSpellings) {
  EXPECT_EQ(ToStringView(Tier::kHot), "hot");
  EXPECT_EQ(ToStringView(Tier::kBuffer), "buffer");
  EXPECT_EQ(ToStringView(Tier::kCold), "cold");
}

TEST(Names, FlushStatusSpellings) {
  EXPECT_EQ(ToStringView(FlushStatus::kSuccess), "success");
  EXPECT_EQ(ToStringView(FlushStatus::kFailure), "failure");
}

TEST(Names, FlushReasonSpellings) {
  EXPECT_EQ(ToStringView(FlushReason::kQuiet), "quiet");
  EXPECT_EQ(ToStringView(FlushReason::kDeadline), "deadline");
  EXPECT_EQ(ToStringView(FlushReason::kPressure), "pressure");
}

TEST(Names, RedecideReasonSpellings) {
  EXPECT_EQ(ToStringView(RedecideReason::kAdmission), "admission");
  EXPECT_EQ(ToStringView(RedecideReason::kSpare), "spare");
  EXPECT_EQ(ToStringView(RedecideReason::kLoad), "load");
  EXPECT_EQ(ToStringView(RedecideReason::kBackpressure), "backpressure");
  static_assert(LabelKeyOf<RedecideReason>::value == LabelKey::kReason);
}

TEST(Names, FillOutcomeSpellings) {
  EXPECT_EQ(ToStringView(FillOutcome::kInstalled), "installed");
  EXPECT_EQ(ToStringView(FillOutcome::kDiscarded), "discarded");
  EXPECT_EQ(ToStringView(FillOutcome::kSkippedBackpressure), "skipped_backpressure");
  EXPECT_EQ(ToStringView(FillOutcome::kSkippedSize), "skipped_size");
  EXPECT_EQ(ToStringView(FillOutcome::kSkippedEvictCap), "skipped_evict_cap");
  EXPECT_EQ(ToStringView(FillOutcome::kFailed), "failed");
  static_assert(LabelKeyOf<FillOutcome>::value == LabelKey::kOutcome);
}

TEST(Names, MaintenancePassSpellings) {
  EXPECT_EQ(ToStringView(MaintenancePass::kTombstones), "tombstones");
  EXPECT_EQ(ToStringView(MaintenancePass::kParked), "parked");
  EXPECT_EQ(ToStringView(MaintenancePass::kTtl), "ttl");
  EXPECT_EQ(ToStringView(MaintenancePass::kDeadline), "deadline");
  EXPECT_EQ(ToStringView(MaintenancePass::kMemory), "memory");
  static_assert(LabelKeyOf<MaintenancePass>::value == LabelKey::kPass);
}

TEST(Names, LabelKeyOfMapping) {
  static_assert(LabelKeyOf<Tier>::value == LabelKey::kTier);
  static_assert(LabelKeyOf<FlushStatus>::value == LabelKey::kStatus);
  static_assert(LabelKeyOf<FlushReason>::value == LabelKey::kReason);
  static_assert(LabelKeyOf<CmdLabel>::value == LabelKey::kCmd);
  static_assert(LabelKeyOf<ShardLabel>::value == LabelKey::kShard);
}

TEST(Names, CatalogueNamesAreUnique) {
  constexpr std::array kAllNames = {
      names::kHotOpDurationSeconds.name,
      names::kColdOpDurationSeconds.name,
      names::kBufferOpDurationSeconds.name,
      names::kRespRequestDurationSeconds.name,
      names::kWalFlushDurationSeconds.name,
      names::kWalFlushBatchEntries.name,
      names::kWalBackpressureWaitsTotal.name,
      names::kWalBackpressureRejectionsTotal.name,
      names::kWalUnflushedBytes.name,
      names::kWalDurabilityLagSeconds.name,
      names::kWalFillWaitSeconds.name,
      names::kWalPublishWaitSeconds.name,
      names::kWalSpareSegments.name,
      names::kWalFreeSegments.name,
      names::kWalSegmentsGrownTotal.name,
      names::kWalSpareWaitsTotal.name,
      names::kWalSegmentPrepareFailuresTotal.name,
      names::kWalRingBytes.name,
      names::kWalIndexBytes.name,
      names::kWalScanBytesTotal.name,
      names::kQueueOffsetPersistDurationSeconds.name,
      names::kColdFlushBatchSize.name,
      names::kColdConsumerLagEntries.name,
      names::kColdBufferOldestEntryAgeSeconds.name,
      names::kHotConsumerSeq.name,
      names::kColdConsumerSeq.name,
      names::kHotMemoryBytes.name,
      names::kHotKeys.name,
      names::kHotStubEntries.name,
      names::kHotNegativeEntries.name,
      names::kHotFillsTotal.name,
      names::kHotUnevictableBytes.name,
      names::kHotMaintenanceHoldSeconds.name,
      names::kHotExpirySweepSeconds.name,
      names::kColdDiskBytes.name,
      names::kColdKeys.name,
      names::kQueueDepth.name,
      names::kQueueDiskBytes.name,
      names::kColdBufferEntries.name,
      names::kColdBufferBytes.name,
      names::kHitsTotal.name,
      names::kMissesTotal.name,
      names::kQueueAppendedTotal.name,
      names::kColdFlushTotal.name,
      names::kColdFlushReasonTotal.name,
      names::kTtlExpiredTotal.name,
      names::kColdApplyTypeConflictsTotal.name,
      names::kEvictedTotal.name,
      names::kHotStubDropsTotal.name,
      names::kHotLoadDiscardsTotal.name,
      names::kHotBackpressureWaitsTotal.name,
      names::kHotBackpressureRejectionsTotal.name,
      names::kSequencerLockedCopyBytesTotal.name,
      names::kSequencerRedecidesTotal.name,
      names::kSequencerLockHoldSeconds.name,
      names::kQueueOffsetPersistFailuresTotal.name,
      names::kQueueReadOutOfRangeTotal.name,
  };
  std::set<std::string_view> seen;
  for (const auto name : kAllNames) {
    EXPECT_TRUE(seen.insert(name).second) << "duplicate metric name: " << name;
  }
}

TEST(Names, LabelFormatterValues) {
  EXPECT_EQ(ToLabelString(Tier::kBuffer), "buffer");
  EXPECT_EQ(ToLabelString(FlushReason::kPressure), "pressure");
  EXPECT_EQ(ToLabelString(CmdLabel{"GET"}), "GET");
  EXPECT_EQ(ToLabelString(ShardLabel{42}), "42");
}

}  // namespace
}  // namespace abyss::metrics
