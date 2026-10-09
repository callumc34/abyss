// The cold consumer absorbs at the queue's ack class but persists only
// effects whose entries are power-durable (ADP-004 §Persisting at the
// power-durable log). These tests stall real WAL flushes and read the
// RocksDB cold store directly, the only view that can see a violation.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
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
#include <variant>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/sequencer.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/append_result.h"
#include "abyss/queue/frame.h"
#include "abyss/queue/wal_queue.h"
#include "durability_printer.h"
#include "latch.h"
#include "on_exit.h"
#include "temp_dir.h"
#include "wal_power_loss.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;

constexpr auto kWait = 5s;
constexpr core::SequenceId kFirst = core::kFirstSeq;

// Holds every WAL flush until released, but for those allowed through.
class FlushStall {
 public:
  queue::FlushHook Hook() const {
    return [state = state_](uint32_t) -> core::Result<void> {
      std::unique_lock lock(state->mu);
      ++state->entered;
      state->cv.notify_all();
      state->cv.wait(lock, [&state] { return state->released || state->allowed > 0; });
      if (!state->released) --state->allowed;
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

  // The next `flushes` to reach the hook pass it.
  void Allow(int flushes) const {
    {
      const std::scoped_lock lock(state_->mu);
      state_->allowed += flushes;
    }
    state_->cv.notify_all();
  }

  bool AwaitEntered(int flushes) const {
    std::unique_lock lock(state_->mu);
    return state_->cv.wait_for(lock, kWait, [this, flushes] { return state_->entered >= flushes; });
  }

 private:
  struct State {
    std::mutex mu;
    std::condition_variable cv;
    bool released = false;
    int allowed = 0;
    int entered = 0;
  };
  std::shared_ptr<State> state_ = std::make_shared<State>();
};

// Runs `on_apply` around every ApplyBatch: before it lands and after.
class ApplyHookColdStore : public core::ColdStore {
 public:
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
  core::Result<std::optional<core::ColdKeyState>> LoadKey(std::string_view key,
                                                          core::SteadyTime deadline) override {
    return inner_.LoadKey(key, deadline);
  }
  core::Result<std::optional<core::KeyMeta>> ProbeKey(std::string_view key,
                                                      core::SteadyTime deadline) override {
    return inner_.ProbeKey(key, deadline);
  }
  core::Result<std::optional<core::LoadedAs>> LoadKeyAs(std::string_view key, core::KeyType type,
                                                        core::SteadyTime deadline) override {
    return inner_.LoadKeyAs(key, type, deadline);
  }
  core::Result<std::vector<std::optional<core::MemberValue>>> LoadMembers(
      std::string_view key, core::KeyType type, std::span<const std::string_view> members,
      core::SteadyTime deadline) override {
    return inner_.LoadMembers(key, type, members, deadline);
  }

 private:
  core::ColdStore& inner_;
  std::function<void()> on_apply_;
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
    OpenWalWith(WalConfigFor(durability, shards));
  }

  void OpenWalWith(const queue::WalConfig& config) {
    auto opened = queue::WalQueue::Open(config);
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

  EXPECT_EQ(PowerEnd(0), kFirst);
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
  queue_->SetFlushHookForTesting([](uint32_t) -> core::Result<void> {
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
  engine::Loader loader(hot, pool, cold);
  engine::Sequencer sequencer(hot, *queue_, loader, pool, engine::SequencerConfig{});
  engine::ReadPath read_path(hot, loader, sequencer, engine::ReadPathConfig{});
  engine::TieringEngine engine(read_path, sequencer);
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
  queue_->SetFlushHookForTesting([](uint32_t) -> core::Result<void> {
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

// A power loss: the log keeps only what its last flush covered. Writes
// acknowledged at power_loss survive; the later ones, and any cold
// state or committed offset derived from them, do not.
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
  }
  const queue::DurableExtent extent = queue_->DurableExtentForTesting(0);
  ASSERT_TRUE(queue_->FlushOffsets().has_value());
  consumers_.clear();
  queue_->SkipFinalFlushForTesting();
  stall_.Release();
  queue_.reset();
  cold_.reset();
  testing::SimulatePowerLoss(extent);

  ASSERT_NO_FATAL_FAILURE(OpenWal(GetParam(), kShards));
  ASSERT_NO_FATAL_FAILURE(OpenCold(kShards));
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    EXPECT_EQ(PowerEnd(shard), durable_end[shard]) << "shard " << shard;
    auto recovered = queue_->Read(shard, kFirst, 16, 0ms, core::Durability::kPowerLoss);
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

double SegmentsGrown() {
  return metrics::testing::GetCounterValue(metrics::names::kWalSegmentsGrownTotal).value_or(0.0);
}

uint64_t OrdinalOf(const queue::DurableExtent& extent) {
  return std::stoull(std::filesystem::path(extent.path).stem().string());
}

uint64_t WordAt(const std::string& bytes, uint64_t offset) {
  uint64_t word = 0;
  std::memcpy(&word, bytes.data() + offset, sizeof(word));
  return word;
}

// A power loss on a device that still holds what the unflushed writes
// replaced, which in a recycled segment is frames of an earlier
// generation. D sits inside a batch. Recovery ends exactly at D's last
// whole batch with every entry as written, and cold, run to completion,
// holds nothing above that end.
TEST_P(ColdPowerLossTest, StaleFramesPastTheDurableEndAreNeverReplayed) {
  constexpr size_t kShards = 4;
  constexpr core::SequenceId kKeys = 8;
  constexpr size_t kFrameSpace = 8192;
  auto config = WalConfigFor(GetParam(), kShards);
  config.segment_size_bytes = 4096 + kFrameSpace;
  config.min_retention = 0s;
  // Flushes are held below for longer than the default age bound.
  config.durability_window = 60s;
  config.offset_fsync_interval = std::chrono::hours{1};
  ASSERT_NO_FATAL_FAILURE(OpenWalWith(config));
  ASSERT_NO_FATAL_FAILURE(OpenCold(kShards));
  for (core::ShardId shard = 0; shard < kShards; ++shard) AddConsumer(shard, AggressiveConfig());

  // Values are one width, so every frame is one size and a frame of an
  // earlier generation starts exactly where a new one does.
  std::vector<std::vector<std::string>> written(kShards);
  uint64_t values = 0;
  const auto key = [](core::ShardId shard, core::SequenceId seq) {
    return "k" + std::to_string(shard) + "_" + std::to_string(seq % kKeys);
  };
  const auto next_entry = [&](core::ShardId shard, char tag) {
    const core::SequenceId seq = kFirst + written[shard].size();
    const std::string digits = std::to_string(values++);
    std::string value = tag + std::string(8 - digits.size(), '0') + digits;
    written[shard].push_back(value);
    return WriteEntry({"SET", key(shard, seq), std::move(value)});
  };
  const auto append_rows = [&](size_t rows) {
    for (size_t r = 0; r < rows; ++r) {
      for (core::ShardId shard = 0; shard < kShards; ++shard) {
        const core::SequenceId seq = kFirst + written[shard].size();
        ASSERT_EQ(Append(shard, next_entry(shard, 'w')), seq);
      }
    }
    for (core::ShardId shard = 0; shard < kShards; ++shard) {
      ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(shard, kFirst + written[shard].size() - 1));
    }
  };
  // Cold takes in and commits everything durable; retention then
  // reclaims every sealed segment, the oldest two into the free pool.
  const auto absorb_and_reclaim = [&] {
    for (auto& consumer : consumers_) {
      consumer->Drain();
      EXPECT_EQ(consumer->Flush(), ColdConsumer::FlushOutcome::kProgress);
      ASSERT_EQ(ColdCommit(consumer->Shard()), kFirst + written[consumer->Shard()].size() - 1);
    }
    ASSERT_TRUE(queue_->FlushOffsets().has_value());
    ASSERT_TRUE(queue_->FlushOffsets().has_value());
  };
  const size_t frame_bytes = [&] {
    std::vector<std::byte> frame;
    return queue::frame::EncodeEntry(WriteEntry({"SET", key(0, 0), std::string(9, 'w')}), 0, frame);
  }();
  // Half a segment, so a step crosses one segment end at most.
  const size_t step = kFrameSpace / frame_bytes / kShards / 2;
  ASSERT_GT(step, 2U);

  for (int i = 0; i < 100 && OrdinalOf(queue_->DurableExtentForTesting(0)) < 3; ++i) {
    ASSERT_NO_FATAL_FAILURE(append_rows(step));
  }
  ASSERT_NO_FATAL_FAILURE(absorb_and_reclaim());
  ASSERT_GT(queue_->FirstSeq(0).value(), kFirst) << "retention reclaimed nothing";

  // Each segment end crossed from here is reclaimed at once, so the
  // pool never runs dry and every spare prepared is a recycled file.
  // The spares published now end two past the active segment, so the
  // third one on is prepared after this count.
  const double grown = SegmentsGrown();
  const uint64_t from = OrdinalOf(queue_->DurableExtentForTesting(0));
  uint64_t active = from;
  for (int i = 0; i < 100 && active < from + 3; ++i) {
    ASSERT_NO_FATAL_FAILURE(append_rows(step));
    if (const uint64_t now = OrdinalOf(queue_->DurableExtentForTesting(0)); now != active) {
      ASSERT_NO_FATAL_FAILURE(absorb_and_reclaim());
      active = now;
    }
  }
  ASSERT_EQ(active, from + 3);
  ASSERT_EQ(SegmentsGrown(), grown) << "a spare was grown rather than recycled";

  // D moves inside a batch: flush 1 holds a snapshot of X alone, Y
  // makes the committer flush again, and that flush finds the batch on
  // shard 0 filled up to its first frame.
  const queue::DurableExtent before = queue_->DurableExtentForTesting(0);
  queue_->SetFlushHookForTesting(stall_.Hook());
  const core::SequenceId x = Append(1, next_entry(1, 'w'));
  ASSERT_TRUE(stall_.AwaitEntered(1));
  const core::SequenceId y = Append(2, next_entry(2, 'w'));
  const core::SequenceId batch_first = kFirst + written[0].size();
  std::vector<core::QueueEntry> batch_entries;
  batch_entries.reserve(5);
  for (int i = 0; i < 5; ++i) batch_entries.push_back(next_entry(0, 'w'));
  auto in_batch = std::make_shared<testing::Latch>();
  auto finish_batch = std::make_shared<testing::Latch>();
  // Released before the batch's future is waited on.
  std::future<core::Result<queue::AppendBatchResult>> batch;
  const testing::OnExit release_batch([finish_batch] { finish_batch->Open(); });
  queue_->SetBatchCommitHookForTesting([in_batch, finish_batch](std::size_t committed) {
    if (committed != 1) return;
    in_batch->Open();
    finish_batch->Wait();
  });
  batch = std::async(std::launch::async,
                     [this, &batch_entries] { return queue_->AppendBatch(0, batch_entries); });
  ASSERT_TRUE(in_batch->Wait());
  stall_.Allow(2);
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(1, x));
  ASSERT_NO_FATAL_FAILURE(AwaitPowerDurable(2, y));
  EXPECT_EQ(PowerEnd(0), batch_first) << "part of a batch is visible at power_loss";
  const queue::DurableExtent extent = queue_->DurableExtentForTesting(0);
  ASSERT_EQ(extent.path, before.path);
  ASSERT_EQ(extent.offset, before.offset + (3 * frame_bytes)) << "D is not inside the batch";
  std::vector<core::SequenceId> durable_end(kShards);
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    durable_end[shard] = shard == 0 ? batch_first : kFirst + written[shard].size();
  }

  // Everything past D is a frame of an earlier generation, lined up
  // with the frames being written now.
  const auto snapshot = testing::CaptureLog(std::filesystem::path(extent.path).parent_path());
  {
    const auto it = snapshot.find(std::filesystem::path(extent.path).filename().string());
    ASSERT_NE(it, snapshot.end());
    const uint64_t stale = WordAt(it->second, extent.offset);
    const uint64_t last = WordAt(it->second, extent.offset - frame_bytes);
    ASSERT_NE(stale, 0U) << "nothing was written past D in an earlier life";
    EXPECT_NE(queue::frame::CommitGen(stale), static_cast<uint32_t>(active));
    EXPECT_EQ(queue::frame::CommitLen(stale), queue::frame::CommitLen(last));
  }

  // No flush passes from here, so D stays put while the batch finishes
  // and more appends land past it, singles and batches on every shard.
  finish_batch->Open();
  ASSERT_EQ(batch.wait_for(kWait), std::future_status::ready);
  auto batched = batch.get();
  ASSERT_TRUE(batched.has_value()) << batched.error().message();
  ASSERT_EQ(batched->first_seq, batch_first);
  std::vector<queue::DurabilityFuture> unflushed;
  unflushed.push_back(std::move(batched->durable));
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    for (int i = 0; i < 6; ++i) {
      auto appended = queue_->Append(shard, next_entry(shard, 'L'));
      ASSERT_TRUE(appended.has_value()) << appended.error().message();
      unflushed.push_back(std::move(appended->durable));
    }
    std::vector<core::QueueEntry> entries;
    entries.reserve(3);
    for (int i = 0; i < 3; ++i) entries.push_back(next_entry(shard, 'L'));
    auto appended = queue_->AppendBatch(shard, entries);
    ASSERT_TRUE(appended.has_value()) << appended.error().message();
    unflushed.push_back(std::move(appended->durable));
  }
  // At process_crash cold takes these in, and must not persist them.
  for (auto& consumer : consumers_) {
    consumer->Drain();
    consumer->Flush();
  }
  ASSERT_EQ(queue_->DurableExtentForTesting(0).offset, extent.offset);
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    EXPECT_EQ(PowerEnd(shard), durable_end[shard]) << "shard " << shard;
  }
  if (GetParam() == core::Durability::kPowerLoss) {
    for (auto& future : unflushed) {
      EXPECT_EQ(future.wait_for(0ms), std::future_status::timeout) << "acknowledged past D";
    }
  }

  consumers_.clear();
  queue_->SkipFinalFlushForTesting();
  stall_.Release();
  queue_.reset();
  cold_.reset();
  testing::SimulatePowerLossRestoring(extent, snapshot);

  ASSERT_NO_FATAL_FAILURE(OpenWalWith(config));
  ASSERT_NO_FATAL_FAILURE(OpenCold(kShards));
  const std::atomic<bool> cancel{false};
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    SCOPED_TRACE("shard " + std::to_string(shard));
    const core::SequenceId end = durable_end[shard];
    EXPECT_EQ(PowerEnd(shard), end) << "an acknowledged write was lost, or D was passed";
    EXPECT_EQ(queue_->TailSeq(shard).value() + 1, end);
    const core::SequenceId first = queue_->FirstSeq(shard).value();
    auto recovered = queue_->Read(shard, first, 100000, 0ms, core::Durability::kPowerLoss);
    ASSERT_TRUE(recovered.has_value()) << recovered.error().message();
    ASSERT_EQ(recovered->size(), end - first);
    for (const auto& entry : *recovered) {
      const auto* write = std::get_if<core::entry::Write>(&entry.payload);
      ASSERT_NE(write, nullptr) << "seq " << entry.seq;
      const std::vector<std::string> want{"SET", key(shard, entry.seq),
                                          written[shard][entry.seq - kFirst]};
      EXPECT_EQ(write->cmd.args, want) << "seq " << entry.seq;
    }

    auto& consumer = AddConsumer(shard, AggressiveConfig());
    auto replayed = consumer.ReplayUntil(end - 1, cancel);
    ASSERT_TRUE(replayed.has_value()) << replayed.error().message();
    std::vector<std::optional<std::string>> want(kKeys);
    for (core::SequenceId seq = kFirst; seq < end; ++seq) {
      want[seq % kKeys] = written[shard][seq - kFirst];
    }
    for (core::SequenceId k = 0; k < kKeys; ++k) {
      EXPECT_EQ(ReadCold(*cold_, key(shard, k)), want[k]) << key(shard, k);
    }
    const auto committed = ColdCommit(shard);
    EXPECT_TRUE(committed.has_value() && *committed < end);
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
