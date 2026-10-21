// The cold consumer absorbs at the queue's ack class but persists only
// effects whose entries are power-durable (ADP-004 §Persisting at the
// power-durable log). These tests stall real WAL flushes and read the
// RocksDB cold store directly, the only view that can see a violation.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/consumer/hot_consumer_progress.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/queue/wal_queue.h"
#include "durability_printer.h"
#include "temp_dir.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;

constexpr auto kWait = 5s;

// Holds every WAL flush until released.
class FlushStall {
 public:
  queue::FlushHook Hook() const {
    return [state = state_](core::ShardId) -> core::Result<void> {
      std::unique_lock lock(state->mu);
      state->cv.wait(lock, [&state] { return state->released; });
      return {};
    };
  }

  void Release() const {
    {
      const std::scoped_lock lock(state_->mu);
      state_->released = true;
    }
    state_->cv.notify_all();
  }

 private:
  struct State {
    std::mutex mu;
    std::condition_variable cv;
    bool released = false;
  };
  std::shared_ptr<State> state_ = std::make_shared<State>();
};

// Runs `on_apply` around every ApplyBatch: before it lands and after.
class ApplyHookColdStore : public core::ColdStore {
 public:
  using PromotionCommand = core::Result<std::optional<core::RespCommand>>;

  explicit ApplyHookColdStore(core::ColdStore& inner) : inner_(inner) {}

  void SetOnApply(std::function<void()> on_apply) { on_apply_ = std::move(on_apply); }

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op,
                                     std::optional<core::Duration> deadline) override {
    return inner_.Exec(op, deadline);
  }
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::SequenceId highest_wal_seq) override {
    if (on_apply_) on_apply_();
    auto applied = inner_.ApplyBatch(ops, highest_wal_seq);
    if (on_apply_) on_apply_();
    return applied;
  }
  core::Result<void> Checkpoint(core::ShardId shard, core::SequenceId up_to_wal_seq) override {
    return inner_.Checkpoint(shard, up_to_wal_seq);
  }
  core::Result<void> Wipe(core::ShardId shard) override { return inner_.Wipe(shard); }
  core::Result<core::StorageStats> Stats() override { return inner_.Stats(); }
  core::Result<void> Compact() override { return inner_.Compact(); }
  PromotionCommand GetPromotionCommand(std::string_view key) override {
    return inner_.GetPromotionCommand(key);
  }

 private:
  core::ColdStore& inner_;
  std::function<void()> on_apply_;
};

class NullHotProgress : public HotConsumerProgress {
 public:
  core::SequenceId HighestSettledSeq(core::ShardId /*shard*/) const override { return 0; }
};

core::RespCommand Cmd(std::vector<std::string> args) {
  return core::RespCommand{.args = std::move(args)};
}

core::QueueEntry WriteEntry(std::vector<std::string> args) {
  return core::QueueEntry{.appended_at = core::WallClock::now(),
                          .payload = core::entry::Write{.cmd = Cmd(std::move(args))}};
}

core::QueueEntry ConditionalEntry(std::vector<std::string> args) {
  return core::QueueEntry{.appended_at = core::WallClock::now(),
                          .payload = core::entry::Conditional{.cmd = Cmd(std::move(args))}};
}

// What the resolver appends for an applied Conditional.
core::QueueEntry ResolvedEntry(core::SequenceId ref, std::vector<std::string> args) {
  core::entry::Resolved resolved{.ref = ref, .decision = core::Decision::kApply};
  resolved.materialised_ops.push_back(Cmd(std::move(args)));
  return core::QueueEntry{.appended_at = core::WallClock::now(), .payload = std::move(resolved)};
}

std::optional<std::string> ReadCold(core::ColdStore& cold, const std::string& key) {
  const core::ops::ReadOp op = core::ops::StringGet{.key = key};
  auto value = cold.Exec(op);
  EXPECT_TRUE(value.has_value()) << value.error().message();
  if (!value.has_value() || value->IsNull()) return std::nullopt;
  return value->AsString();
}

template <typename Pred>
bool Eventually(Pred done, std::chrono::milliseconds timeout = kWait) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!done()) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

class ColdPersistenceGateTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = std::make_unique<testing::TempDir>("cold_gate"); }

  void TearDown() override {
    // A parked flusher would block the queue's shutdown.
    stall_.Release();
    consumers_.clear();
    queue_.reset();
    cold_.reset();
  }

  queue::WalConfig WalConfigFor(core::Durability durability, size_t shards) const {
    return queue::WalConfig{
        .wal_path = dir_->Sub("wal").string(),
        .segment_size_bytes = size_t{1} << 20U,
        .shard_count = shards,
        .durability = durability,
        .min_retention = 3600s,
        .retention_consumers = {core::kColdConsumer},
    };
  }

  void OpenWal(core::Durability durability, size_t shards = 1) {
    auto opened = queue::WalQueue::Open(WalConfigFor(durability, shards));
    ASSERT_TRUE(opened.has_value()) << opened.error().message();
    queue_ = std::move(*opened);
  }

  void OpenCold(uint32_t shards = 1) {
    auto created = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
        .data_path = dir_->Sub("cold").string(), .shard_count = shards});
    ASSERT_TRUE(created.has_value()) << created.error().message();
    cold_ = std::move(*created);
  }

  // Never flushes on a timer: every pass flushes the whole buffer.
  static ColdConsumer::Config AggressiveConfig() {
    ColdConsumer::Config cfg;
    cfg.quiet_threshold = 3600s;
    cfg.jitter_fraction = 0.0;
    cfg.buffer_high_water_bytes = 1;
    cfg.queue_read_timeout = 20ms;
    cfg.checkpoint_max_flushes = 1;
    cfg.checkpoint_min_interval = 0ms;
    cfg.rng_seed = 1;
    return cfg;
  }

  ColdConsumer& AddConsumer(core::ShardId shard, const ColdConsumer::Config& cfg) {
    consumers_.push_back(
        std::make_unique<ColdConsumer>(*queue_, *cold_, shard, cfg, policy_, rpc_));
    return *consumers_.back();
  }

  // Publishes `entry`; under process_crash it is acknowledged on return.
  core::SequenceId Append(core::ShardId shard, core::QueueEntry entry) {
    auto pending = queue_->BeginAppend(shard, std::move(entry));
    if (!pending.has_value()) {
      ADD_FAILURE() << pending.error().message();
      return 0;
    }
    pending->Publish();
    if (queue_->AckDurability() == core::Durability::kProcessCrash) {
      EXPECT_EQ(pending->durable().wait_for(0ms), std::future_status::ready)
          << "process_crash must acknowledge at publish";
    }
    return pending->seq();
  }

  void AwaitPowerDurable(core::ShardId shard, core::SequenceId seq) {
    auto durable = queue_->AwaitDurable(shard, seq, core::Durability::kPowerLoss, kWait);
    ASSERT_TRUE(durable.has_value() && *durable) << "seq " << seq << " never became power-durable";
  }

  core::SequenceId PowerEnd(core::ShardId shard) {
    return queue_->DurableEnd(shard, core::Durability::kPowerLoss).value();
  }

  std::optional<core::SequenceId> ColdCommit(core::ShardId shard) {
    return queue_->CommittedOffset(core::kColdConsumer, shard).value();
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<testing::TempDir> dir_;
  std::unique_ptr<queue::WalQueue> queue_;
  std::unique_ptr<cold::backends::RocksdbStore> cold_;
  core::EvictionPolicy policy_{core::EvictionTTL{86400}};
  core::ConsumerRpc rpc_;
  FlushStall stall_;
  std::vector<std::unique_ptr<ColdConsumer>> consumers_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// #166: with the WAL flush stalled, acknowledged writes reach the buffer
// and an aggressive flush runs, yet none reaches cold until the flush lands.
TEST_F(ColdPersistenceGateTest, StalledFlushKeepsAbsorbedWritesOutOfCold) {
  ASSERT_NO_FATAL_FAILURE(OpenWal(core::Durability::kProcessCrash));
  ASSERT_NO_FATAL_FAILURE(OpenCold());
  queue_->SetFlushHookForTesting(stall_.Hook());
  auto& consumer = AddConsumer(0, AggressiveConfig());

  constexpr int kKeys = 8;
  core::SequenceId last = 0;
  for (int i = 0; i < kKeys; ++i) {
    last = Append(0, WriteEntry({"SET", "k" + std::to_string(i), "v" + std::to_string(i)}));
  }
  consumer.Start();
  ASSERT_TRUE(consumer.WaitForDrainedSeq(last, kWait)) << "cold never absorbed the writes";
  ASSERT_TRUE(Eventually([&consumer] {
    return consumer.Snapshot().durability_waits_timed_out >= 2;
  })) << "the aggressive flush never reached the persistence gate";

  EXPECT_EQ(PowerEnd(0), 0U);
  EXPECT_EQ(consumer.Buffer().Size(), static_cast<size_t>(kKeys));
  for (int i = 0; i < kKeys; ++i) {
    EXPECT_EQ(ReadCold(*cold_, "k" + std::to_string(i)), std::nullopt)
        << "k" << i << " persisted ahead of the power-durable log";
  }
  EXPECT_EQ(ColdCommit(0), std::nullopt);

  stall_.Release();
  ASSERT_TRUE(Eventually([this] { return ReadCold(*cold_, "k7").has_value(); }));
  for (int i = 0; i < kKeys; ++i) {
    EXPECT_EQ(ReadCold(*cold_, "k" + std::to_string(i)), "v" + std::to_string(i));
  }
  // Rescheduled keys flush on the next pass, so the commit follows promptly.
  EXPECT_TRUE(Eventually([this, last] { return ColdCommit(0) == last; }));
  const auto snap = consumer.Snapshot();
  EXPECT_EQ(snap.apply_failures, 0U);
  EXPECT_EQ(snap.retry_attempts, 0U);
}

// A key first seen at a power-durable seq and rewritten above it: the gate
// follows its last write, not its first, so cold never shows a newer effect
// than the log can recover. A writer outrunning a slow device keeps it so.
TEST_F(ColdPersistenceGateTest, RewrittenKeyNeverPersistsAboveThePowerDurableEnd) {
  ASSERT_NO_FATAL_FAILURE(OpenWal(core::Durability::kProcessCrash));
  ASSERT_NO_FATAL_FAILURE(OpenCold());
  auto& consumer = AddConsumer(0, AggressiveConfig());

  const core::SequenceId first = Append(0, WriteEntry({"SET", "hot", "0"}));
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(0, first));
  queue_->SetFlushHookForTesting(stall_.Hook());
  core::SequenceId last = first;
  for (int i = 0; i < 20; ++i)
    last = Append(0, WriteEntry({"SET", "hot", std::to_string(last + 1)}));

  consumer.Start();
  ASSERT_TRUE(consumer.WaitForDrainedSeq(last, kWait));
  ASSERT_TRUE(
      Eventually([&consumer] { return consumer.Snapshot().durability_waits_timed_out >= 2; }));
  EXPECT_EQ(PowerEnd(0), first + 1);
  EXPECT_EQ(ReadCold(*cold_, "hot"), std::nullopt) << "persisted effects up to seq " << last;

  stall_.Release();
  ASSERT_TRUE(Eventually([this, last] { return ReadCold(*cold_, "hot") == std::to_string(last); }));

  // Each value names its own seq; a slow device keeps the end lagging.
  queue_->SetFlushHookForTesting([](core::ShardId) -> core::Result<void> {
    std::this_thread::sleep_for(3ms);
    return {};
  });
  std::atomic<bool> writing{true};
  std::thread writer([this, &writing, next = last + 1]() mutable {
    while (writing.load()) {
      EXPECT_EQ(Append(0, WriteEntry({"SET", "hot", std::to_string(next)})), next);
      ++next;
      std::this_thread::sleep_for(100us);
    }
  });
  int checks = 0;
  const auto until = std::chrono::steady_clock::now() + 300ms;
  while (std::chrono::steady_clock::now() < until) {
    const auto value = ReadCold(*cold_, "hot");
    const core::SequenceId end = PowerEnd(0);
    if (!value.has_value()) {
      ADD_FAILURE() << "the key vanished from cold";
      break;
    }
    EXPECT_LT(std::stoull(*value), end) << "cold shows an effect above the power-durable end";
    ++checks;
  }
  writing.store(false);
  writer.join();
  EXPECT_GT(checks, 0);
}

// The Conditional X is durable, its Resolved Y is not. Y carries the
// effect, so the effect must not reach cold until Y is power-durable.
TEST_F(ColdPersistenceGateTest, ConditionalEffectWaitsForItsResolved) {
  ASSERT_NO_FATAL_FAILURE(OpenWal(core::Durability::kProcessCrash));
  ASSERT_NO_FATAL_FAILURE(OpenCold());
  auto& consumer = AddConsumer(0, AggressiveConfig());

  const core::SequenceId x = Append(0, ConditionalEntry({"SET", "k", "v"}));
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(0, x));
  queue_->SetFlushHookForTesting(stall_.Hook());
  const core::SequenceId y = Append(0, ResolvedEntry(x, {"SET", "k", "v"}));

  EXPECT_EQ(consumer.Drain(), 2U);
  EXPECT_EQ(consumer.LatestDrainedSeq(), y);
  EXPECT_EQ(consumer.Flush(), ColdConsumer::FlushOutcome::kDurabilityPending);
  EXPECT_EQ(PowerEnd(0), x + 1);
  EXPECT_EQ(ReadCold(*cold_, "k"), std::nullopt) << "persisted a Resolved a power loss could drop";
  EXPECT_EQ(ColdCommit(0), std::nullopt) << "committed past the Conditional";

  stall_.Release();
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(0, y));
  EXPECT_EQ(consumer.Flush(), ColdConsumer::FlushOutcome::kProgress);
  EXPECT_EQ(ReadCold(*cold_, "k"), "v");
}

// A buffered key absent from hot is served from the buffer, never from
// cold's older state, while its batch is selected, held and applied.
TEST_F(ColdPersistenceGateTest, BufferedKeyStaysReadableWhileItsBatchIsInFlight) {
  ASSERT_NO_FATAL_FAILURE(OpenWal(core::Durability::kProcessCrash));
  ASSERT_NO_FATAL_FAILURE(OpenCold());
  ApplyHookColdStore cold{*cold_};
  ColdConsumer::Config cfg = AggressiveConfig();
  cfg.queue_read_timeout = 100ms;
  ColdConsumerPool pool(*queue_, cold, ColdConsumerPool::Config{.shard_count = 1, .consumer = cfg},
                        policy_, rpc_);
  hot::ShardedHotStore hot(hot::ShardedHotStoreConfig{
      .max_memory_bytes = 16UL * 1024UL * 1024UL,
      .shard_count = 1,
  });
  NullHotProgress hot_progress;
  engine::TieringEngine engine(*queue_, hot, cold, pool, hot_progress, rpc_,
                               engine::TieringEngineConfig{.shard_count = 1});
  const auto get = [&engine] {
    auto read = engine.DispatchRead("GET", Cmd({"GET", "k"}));
    if (!read.has_value()) return std::string("<error>");
    return read->IsBulkString() ? read->AsString() : std::string("<nil>");
  };

  const core::ops::WriteOp old_value = core::ops::StringSet{.key = "k", .value = "old"};
  ASSERT_TRUE(cold_->ApplyBatch(std::span(&old_value, 1), 0).has_value());
  queue_->SetFlushHookForTesting(stall_.Hook());
  const core::SequenceId seq = Append(0, WriteEntry({"SET", "k", "new"}));
  auto& consumer = pool.ConsumerFor(0);
  ASSERT_EQ(consumer.Drain(), 1U);
  ASSERT_EQ(get(), "new");

  // Selected, then held at the gate for the whole read timeout.
  auto held = std::async(std::launch::async, [&consumer] { return consumer.Flush(); });
  int reads = 0;
  while (held.wait_for(0ms) != std::future_status::ready) {
    EXPECT_EQ(get(), "new") << "read fell through to cold while the batch was held";
    ++reads;
  }
  EXPECT_EQ(held.get(), ColdConsumer::FlushOutcome::kDurabilityPending);
  EXPECT_GT(reads, 0);

  stall_.Release();
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(0, seq));
  std::vector<std::string> during_apply;
  cold.SetOnApply([&during_apply, &get] { during_apply.push_back(get()); });
  EXPECT_EQ(consumer.Flush(), ColdConsumer::FlushOutcome::kProgress);
  EXPECT_EQ(during_apply, (std::vector<std::string>{"new", "new"}));
  EXPECT_EQ(consumer.Buffer().Size(), 0U);
  EXPECT_EQ(ReadCold(*cold_, "k"), "new");
}

// The graceful drain persists through the gate. A slow but healthy device
// makes it wait, and the wait never times out.
TEST_F(ColdPersistenceGateTest, GracefulDrainWaitsForPowerDurabilityWithoutTimingOut) {
  ASSERT_NO_FATAL_FAILURE(OpenWal(core::Durability::kProcessCrash));
  ASSERT_NO_FATAL_FAILURE(OpenCold());
  ColdConsumer::Config cfg = AggressiveConfig();
  cfg.buffer_high_water_bytes = ColdConsumer::Config{}.buffer_high_water_bytes;
  cfg.queue_read_timeout = 200ms;
  auto& consumer = AddConsumer(0, cfg);
  queue_->SetFlushHookForTesting([](core::ShardId) -> core::Result<void> {
    std::this_thread::sleep_for(10ms);
    return {};
  });

  consumer.Start();
  core::SequenceId last = 0;
  for (int i = 0; i < 16; ++i) last = Append(0, WriteEntry({"SET", "d" + std::to_string(i), "v"}));
  ASSERT_TRUE(consumer.WaitForDrainedSeq(last, kWait));
  consumer.RequestStopAndDrain(std::chrono::steady_clock::now() + kWait);
  consumer.Join();

  EXPECT_EQ(consumer.Buffer().Size(), 0U);
  for (int i = 0; i < 16; ++i) EXPECT_EQ(ReadCold(*cold_, "d" + std::to_string(i)), "v");
  EXPECT_EQ(consumer.Snapshot().durability_waits_timed_out, 0U);
  EXPECT_EQ(ColdCommit(0), last);
}

// A power loss: the active segments keep only what their last flush
// covered. Writes acknowledged at power_loss survive; the later ones, and
// any cold state or committed offset derived from them, do not.
class ColdPowerLossTest : public ColdPersistenceGateTest,
                          public ::testing::WithParamInterface<core::Durability> {};

TEST_P(ColdPowerLossTest, ColdHoldsNothingAboveTheRecoveredLog) {
  constexpr size_t kShards = 2;
  ASSERT_NO_FATAL_FAILURE(OpenWal(GetParam(), kShards));
  ASSERT_NO_FATAL_FAILURE(OpenCold(kShards));
  const auto key = [](std::string_view name, core::ShardId shard) {
    return std::string(name) + std::to_string(shard);
  };

  std::vector<core::SequenceId> durable_end(kShards);
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    auto& consumer = AddConsumer(shard, AggressiveConfig());
    Append(shard, WriteEntry({"SET", key("a", shard), "a"}));
    const core::SequenceId a = Append(shard, WriteEntry({"SET", key("a2", shard), "a2"}));
    ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(shard, a));
    durable_end[shard] = a + 1;
    ASSERT_EQ(consumer.Drain(), 2U);
    ASSERT_EQ(consumer.Flush(), ColdConsumer::FlushOutcome::kProgress);
    ASSERT_EQ(ColdCommit(shard), a);
  }

  queue_->SetFlushHookForTesting(stall_.Hook());
  std::vector<queue::FlushedExtent> extents;
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    Append(shard, WriteEntry({"SET", key("b", shard), "b"}));
    const core::SequenceId x = Append(shard, ConditionalEntry({"SET", key("c", shard), "c"}));
    Append(shard, ResolvedEntry(x, {"SET", key("c", shard), "c"}));
    auto& consumer = *consumers_[shard];
    consumer.Drain();
    const auto outcome = consumer.Flush();
    if (GetParam() == core::Durability::kProcessCrash) {
      EXPECT_EQ(consumer.LatestDrainedSeq(), x + 1) << "cold absorbs at the ack class";
      EXPECT_EQ(outcome, ColdConsumer::FlushOutcome::kDurabilityPending);
    } else {
      EXPECT_EQ(outcome, ColdConsumer::FlushOutcome::kIdle) << "absorbed an unflushed entry";
    }
    extents.push_back(queue_->FlushedExtentForTesting(shard));
  }
  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  consumers_.clear();
  queue_->SkipFinalFlushForTesting();
  stall_.Release();
  queue_.reset();
  cold_.reset();
  for (const auto& extent : extents) std::filesystem::resize_file(extent.path, extent.offset);

  ASSERT_NO_FATAL_FAILURE(OpenWal(GetParam(), kShards));
  ASSERT_NO_FATAL_FAILURE(OpenCold(kShards));
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    EXPECT_EQ(PowerEnd(shard), durable_end[shard]) << "shard " << shard;
    auto recovered = queue_->Read(shard, 0, 16, 0ms, core::Durability::kPowerLoss);
    ASSERT_TRUE(recovered.has_value()) << recovered.error().message();
    EXPECT_EQ(recovered->size(), 2U) << "shard " << shard;
    EXPECT_EQ(ReadCold(*cold_, key("a", shard)), "a");
    EXPECT_EQ(ReadCold(*cold_, key("a2", shard)), "a2");
    EXPECT_EQ(ReadCold(*cold_, key("b", shard)), std::nullopt) << "cold outran the log";
    EXPECT_EQ(ReadCold(*cold_, key("c", shard)), std::nullopt) << "cold outran the log";
    const auto committed = ColdCommit(shard);
    EXPECT_TRUE(committed.has_value() && *committed < durable_end[shard])
        << "persisted offset missing or past the recovered log";
  }
}

INSTANTIATE_TEST_SUITE_P(BothClasses, ColdPowerLossTest,
                         ::testing::Values(core::Durability::kProcessCrash,
                                           core::Durability::kPowerLoss),
                         [](const ::testing::TestParamInfo<core::Durability>& param) {
                           return param.param == core::Durability::kProcessCrash ? "ProcessCrash"
                                                                                 : "PowerLoss";
                         });

}  // namespace
}  // namespace abyss::consumer
