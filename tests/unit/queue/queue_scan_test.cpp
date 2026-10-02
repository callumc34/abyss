#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <mutex>
#include <vector>

#include "abyss/core/queue.h"
#include "mock_queue.h"

namespace abyss::core {
namespace {

using ::testing::_;
using ::testing::Invoke;

constexpr ShardId kShards = 4;

QueueEntry Entry(SequenceId seq) {
  return QueueEntry{.seq = seq, .payload = entry::Write{.cmd = RespCommand{{"SET", "k", "v"}}}};
}

using Logs = std::vector<std::vector<QueueEntry>>;

Logs MakeLogs() {
  Logs logs(kShards);
  for (auto& log : logs) {
    for (SequenceId seq = 0; seq < 3000; ++seq) log.push_back(Entry(seq));
  }
  return logs;
}

void ServeReads(testing::MockQueue& queue, const Logs& logs) {
  ON_CALL(queue, Read(_, _, _, _, _))
      .WillByDefault(Invoke([&logs](ShardId shard, SequenceId from, size_t max, Duration,
                                    Durability) -> Result<std::vector<QueueEntry>> {
        return testing::ReadFromLog(logs.at(shard), from, max);
      }));
}

Result<void> Accept(ShardId /*shard*/, std::vector<QueueEntry>& /*batch*/) { return {}; }

TEST(QueueScanTest, DeliversEachShardsRangeInOrderNeverConcurrentlyPerShard) {
  const Logs logs = MakeLogs();
  ::testing::NiceMock<testing::MockQueue> queue;
  ServeReads(queue, logs);
  const std::vector<SequenceId> from{0, 10, 2999, 3000};
  const std::vector<SequenceId> end{3000, 2500, 3000, 3000};
  std::vector<std::atomic<bool>> busy(kShards);
  std::vector<std::vector<SequenceId>> seen(kShards);
  std::atomic<bool> overlapped{false};
  const std::atomic<bool> cancel{false};

  auto scanned = queue.Scan(
      from, end, 3,
      [&](ShardId shard, std::vector<QueueEntry>& batch) {
        if (busy.at(shard).exchange(true)) overlapped = true;
        for (const auto& entry : batch) seen.at(shard).push_back(entry.seq);
        busy.at(shard) = false;
        return Result<void>{};
      },
      cancel);

  ASSERT_TRUE(scanned.has_value()) << scanned.error().message();
  EXPECT_FALSE(overlapped);
  for (ShardId shard = 0; shard < kShards; ++shard) {
    const auto& got = seen.at(shard);
    ASSERT_EQ(got.size(), end.at(shard) - from.at(shard)) << "shard " << shard;
    for (size_t i = 0; i < got.size(); ++i) {
      ASSERT_EQ(got.at(i), from.at(shard) + i) << "shard " << shard;
    }
  }
}

TEST(QueueScanTest, ASinkErrorStopsTheScanAndIsReturned) {
  const Logs logs = MakeLogs();
  ::testing::NiceMock<testing::MockQueue> queue;
  ServeReads(queue, logs);
  const std::vector<SequenceId> from(kShards, 0);
  const std::vector<SequenceId> end(kShards, 3000);
  const std::atomic<bool> cancel{false};
  std::atomic<int> calls{0};
  auto scanned = queue.Scan(
      from, end, 1,
      [&](ShardId, std::vector<QueueEntry>&) -> Result<void> {
        ++calls;
        return std::unexpected(Error{ErrorCode::kInternal, "sink failed"});
      },
      cancel);
  ASSERT_FALSE(scanned.has_value());
  EXPECT_EQ(scanned.error().message(), "sink failed");
  EXPECT_EQ(calls.load(), 1);
}

TEST(QueueScanTest, CancelIsUnavailable) {
  const Logs logs = MakeLogs();
  ::testing::NiceMock<testing::MockQueue> queue;
  ServeReads(queue, logs);
  const std::vector<SequenceId> from(kShards, 0);
  const std::vector<SequenceId> end(kShards, 3000);
  const std::atomic<bool> cancelled{true};
  auto scanned = queue.Scan(from, end, 2, Accept, cancelled);
  ASSERT_FALSE(scanned.has_value());
  EXPECT_EQ(scanned.error().code(), ErrorCode::kUnavailable);
}

TEST(QueueScanTest, AMissingEntryBelowTheEndIsAnError) {
  Logs logs = MakeLogs();
  logs.at(1).erase(logs.at(1).begin() + 100);
  ::testing::NiceMock<testing::MockQueue> queue;
  ServeReads(queue, logs);
  const std::vector<SequenceId> from(kShards, 0);
  const std::vector<SequenceId> end{0, 3000, 0, 0};
  const std::atomic<bool> cancel{false};
  auto scanned = queue.Scan(from, end, 2, Accept, cancel);
  ASSERT_FALSE(scanned.has_value());
  EXPECT_EQ(scanned.error().code(), ErrorCode::kInternal);
}

TEST(QueueScanTest, MismatchedBoundsAreRejected) {
  ::testing::NiceMock<testing::MockQueue> queue;
  const std::vector<SequenceId> from(2, 0);
  const std::vector<SequenceId> end(3, 0);
  const std::atomic<bool> cancel{false};
  auto scanned = queue.Scan(from, end, 1, Accept, cancel);
  ASSERT_FALSE(scanned.has_value());
  EXPECT_EQ(scanned.error().code(), ErrorCode::kInvalidArgument);
}

}  // namespace
}  // namespace abyss::core
