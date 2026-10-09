#include "abyss/engine/sequencer.h"

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/consumer/hot_consumer.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/predicate.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/queue/reservation.h"
#include "abyss/queue/wal_queue.h"
#include "temp_dir.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
using Flags = core::PredicateFlags;

struct Options {
  uint32_t shards = 4;
  uint32_t log_count = 1;
  core::Durability durability = core::Durability::kProcessCrash;
  size_t segment_size_bytes = size_t{1} << 20;
};

// A real log, hot store, RocksDB cold store and cold consumers, the
// consumers driven by hand.
class SequencedEngineTest : public ::testing::Test {
 protected:
  void Open(Options options = {}) {
    options_ = options;
    auto queue = queue::WalQueue::Open(queue::WalConfig{
        .wal_path = dir_.Sub("wal").string(),
        .segment_size_bytes = options.segment_size_bytes,
        .shard_count = options.shards,
        .log_count = options.log_count,
        .durability = options.durability,
        .min_retention = 0s,
        .retention_consumers = {core::kColdConsumer},
        .offset_fsync_interval = std::chrono::hours{1},
    });
    ASSERT_TRUE(queue.has_value()) << queue.error().message();
    queue_ = std::move(*queue);
    auto cold = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
        .data_path = dir_.Sub("cold").string(),
        .shard_count = options.shards,
        .log_clock = [this](core::ShardId shard) -> uint64_t {
          return pool_ ? pool_->LogClockMs(shard) : 0;
        },
    });
    ASSERT_TRUE(cold.has_value()) << cold.error().message();
    cold_ = std::move(*cold);
    hot_ = NewHot();
    consumer::ColdConsumer::Config cold_config;
    cold_config.quiet_threshold = 3600s;
    cold_config.jitter_fraction = 0.0;
    cold_config.queue_read_timeout = 20ms;
    cold_config.checkpoint_min_interval = 0ms;
    cold_config.rng_seed = 1;
    pool_ = std::make_unique<consumer::ColdConsumerPool>(
        *queue_, *cold_,
        consumer::ColdConsumerPool::Config{.shard_count = options.shards, .consumer = cold_config},
        policy_, rpc_);
    Rewire();
  }

  std::unique_ptr<hot::ShardedHotStore> NewHot() {
    return std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = size_t{64} << 20,
        .shard_count = options_.shards,
        .drained = [this](core::ShardId shard) -> core::SequenceId {
          return pool_ ? pool_->ConsumerFor(shard).LatestDrainedSeq() : 0;
        },
        .eviction_policy = &policy_,
    });
  }
  // The loader, sequencer and engine over hot_.
  void Rewire() {
    engine_.reset();
    sequencer_.reset();
    loader_ = std::make_unique<Loader>(*hot_, *pool_, *cold_);
    sequencer_ = std::make_unique<Sequencer>(
        *hot_, *queue_, *loader_, *pool_,
        SequencerConfig{.write_timeout = 5s,
                        .wall_clock = [this] { return wall_ ? wall_() : core::WallClock::now(); }});
    engine_ = std::make_unique<TieringEngine>(
        *hot_, *cold_, *pool_, *sequencer_,
        TieringEngineConfig{.shard_count = options_.shards, .write_timeout = 5s});
  }

  void TearDown() override {
    engine_.reset();
    sequencer_.reset();
    loader_.reset();
    if (pool_) pool_->Stop(0ms);
    pool_.reset();
    hot_.reset();
    cold_.reset();
    queue_.reset();
  }

  std::string Write(std::vector<std::string> args, Flags flags = Flags::kNone) {
    auto reply = sequencer_->Execute(core::RespCommand{.args = std::move(args)}, flags);
    if (!reply.has_value()) return "error: " + reply.error().message();
    return Describe(*reply);
  }
  std::string Read(std::vector<std::string> args) {
    const std::string name = args.front();
    auto reply = engine_->DispatchRead(name, core::RespCommand{.args = std::move(args)});
    if (!reply.has_value()) return "error: " + reply.error().message();
    return Describe(*reply);
  }
  std::string Exists(std::string key) {
    auto reply = engine_->DispatchFanOut(core::MultiKeyKind::kExists,
                                         core::RespCommand{.args = {"EXISTS", std::move(key)}});
    if (!reply.has_value()) return "error: " + reply.error().message();
    return Describe(*reply);
  }
  static std::string Describe(const core::RespValue& value) {
    if (value.IsNull()) return "nil";
    if (value.IsInteger()) return ":" + std::to_string(value.AsInteger());
    if (value.IsError()) return "-" + value.AsString();
    return value.AsString();
  }

  // Absorbs everything into the buffer, then persists it to cold.
  void DrainToCold(core::ShardId shard) {
    auto& consumer = pool_->ConsumerFor(shard);
    for (int round = 0; round < 200; ++round) {
      consumer.Drain();
      consumer.FlushUnscheduled();
      if (consumer.Buffer().Size() == 0 &&
          consumer.LatestDrainedSeq() + 1 >=
              queue_->DurableEnd(shard, core::Durability::kProcessCrash).value()) {
        return;
      }
      std::this_thread::sleep_for(5ms);
    }
    ADD_FAILURE() << "shard " << shard << " never drained to cold";
  }
  // Every drained key leaves hot, a stub behind.
  void EvictDrained() {
    hot_->EvictExpired(core::SteadyClock::now() + std::chrono::hours{1'000'000});
  }

  std::vector<std::string> Members(std::string key) {
    auto reply =
        engine_->DispatchRead("SMEMBERS", core::RespCommand{.args = {"SMEMBERS", std::move(key)}});
    EXPECT_TRUE(reply.has_value() && reply->IsArray());
    std::vector<std::string> members;
    if (!reply.has_value() || !reply->IsArray()) return members;
    for (const auto& member : reply->AsArray()) members.push_back(member.AsString());
    std::ranges::sort(members);
    return members;
  }

  std::vector<core::QueueEntry> Logged(core::ShardId shard) {
    auto read = queue_->Read(shard, core::kFirstSeq, 100000, 0ms, core::Durability::kProcessCrash);
    EXPECT_TRUE(read.has_value()) << read.error().message();
    return read.has_value() ? std::move(*read) : std::vector<core::QueueEntry>{};
  }

  core::ShardId ShardOf(std::string_view key) const {
    return core::ComputeShard(key, options_.shards);
  }
  std::string KeyOn(core::ShardId shard, int n = 0, std::string_view prefix = "key") const {
    for (int i = 0;; ++i) {
      std::string key = std::string(prefix) + std::to_string(i);
      if (ShardOf(key) == shard && n-- == 0) return key;
    }
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"sequencer"};
  // The sequencer's wall clock, when set.
  std::function<core::WallTime()> wall_;
  Options options_;
  core::EvictionPolicy policy_{core::EvictionTTL{3600}};
  core::ConsumerRpc rpc_;
  std::unique_ptr<queue::WalQueue> queue_;
  std::unique_ptr<cold::backends::RocksdbStore> cold_;
  std::unique_ptr<hot::ShardedHotStore> hot_;
  std::unique_ptr<consumer::ColdConsumerPool> pool_;
  std::unique_ptr<Loader> loader_;
  std::unique_ptr<Sequencer> sequencer_;
  std::unique_ptr<TieringEngine> engine_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// Holds every WAL flush until released.
class FlushStall {
 public:
  queue::FlushHook Hook() const {
    return [state = state_](uint32_t) -> core::Result<void> {
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

std::vector<std::string> ArgsOf(const core::QueueEntry& entry) {
  if (const auto* write = std::get_if<core::entry::Write>(&entry.payload)) return write->cmd.args;
  return {"<flush>"};
}

// #167: a conditional's effect is logged where it was decided, so cold
// ends where hot does.
TEST_F(SequencedEngineTest, AConditionalsEffectLandsAtItsPosition) {
  Open();
  EXPECT_EQ(Write({"SET", "k", "a"}), "OK");
  EXPECT_EQ(Write({"SET", "k", "b", "XX"}, Flags::kXx), "OK");
  EXPECT_EQ(Write({"SET", "k", "c"}), "OK");
  const auto logged = Logged(ShardOf("k"));
  ASSERT_EQ(logged.size(), 3U);
  EXPECT_EQ(ArgsOf(logged[1]), (std::vector<std::string>{"SET", "k", "b"}));
  EXPECT_EQ(ArgsOf(logged[2]), (std::vector<std::string>{"SET", "k", "c"}));

  DrainToCold(ShardOf("k"));
  EXPECT_EQ(Describe(cold_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k"}}).value()),
            "c");
}

// #163: a write to a collection hot evicted loads its whole state first.
TEST_F(SequencedEngineTest, AWriteToAnEvictedCollectionSeesItsFullState) {
  Open();
  EXPECT_EQ(Write({"SADD", "s", "a", "b", "c"}), ":3");
  DrainToCold(ShardOf("s"));
  EvictDrained();
  ASSERT_FALSE(hot_->Read(core::ops::ReadOp{core::ops::SetCard{.key = "s"}}).result.has_value())
      << "still resident";

  EXPECT_EQ(Write({"SADD", "s", "c", "d"}), ":1");
  EXPECT_EQ(Read({"SCARD", "s"}), ":4");
  EXPECT_EQ(Write({"SREM", "s", "a", "z"}), ":1");
  EXPECT_EQ(Read({"SCARD", "s"}), ":3");
}

// MSET of two shards applies both under both locks: a reader that sees
// one key's new value then reads the other sees it too (or a newer).
TEST_F(SequencedEngineTest, ACrossShardMsetIsAtomicToAReader) {
  Open();
  const std::string a = KeyOn(0);
  const std::string b = KeyOn(1);
  constexpr int kWrites = 2000;
  EXPECT_EQ(Write({"MSET", a, "0", b, "0"}), "OK");
  std::atomic<bool> done{false};
  std::atomic<int> torn{0};
  const auto reader = [&](const std::string& first, const std::string& second) {
    while (!done.load()) {
      const int seen = std::stoi(Read({"GET", first}));
      const int then = std::stoi(Read({"GET", second}));
      if (then < seen) torn.fetch_add(1);
    }
  };
  std::thread ab(reader, a, b);
  std::thread ba(reader, b, a);
  for (int i = 1; i <= kWrites; ++i) {
    ASSERT_EQ(Write({"MSET", a, std::to_string(i), b, std::to_string(i)}), "OK");
  }
  done = true;
  ab.join();
  ba.join();
  EXPECT_EQ(torn.load(), 0) << "a reader saw one key of an MSET without the other";
}

// Under power_loss a read of a key whose write is applied but not yet
// durable waits for it; its reply never shows a write a power loss
// could take back.
TEST_F(SequencedEngineTest, APowerLossReadWaitsUntilTheWriteIsDurable) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  const auto release = [&stall] { stall.Release(); };

  auto writer = std::async(std::launch::async, [this] { return Write({"SET", "k", "v"}); });
  // Applied to hot before its durable wait.
  bool applied = false;
  for (int i = 0; i < 500 && !applied; ++i) {
    applied = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = "k"}}).result.has_value();
    if (!applied) std::this_thread::sleep_for(1ms);
  }
  if (!applied) release();
  ASSERT_TRUE(applied);
  auto reader = std::async(std::launch::async, [this] { return Read({"GET", "k"}); });
  EXPECT_EQ(reader.wait_for(100ms), std::future_status::timeout) << "read before durable";
  EXPECT_EQ(writer.wait_for(0ms), std::future_status::timeout);
  release();
  EXPECT_EQ(reader.get(), "v");
  EXPECT_EQ(writer.get(), "OK");
}

// M4: after FLUSHDB no earlier key is visible, evicted ones with stubs
// included, though cold still holds them until it drains the Flush.
TEST_F(SequencedEngineTest, FlushDbRacingWritersHidesEveryEarlierKey) {
  Open();
  constexpr int kKeys = 40;
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_EQ(Write({"SET", "pre:" + std::to_string(i), "v"}), "OK");
  }
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
  EvictDrained();
  ASSERT_EQ(Exists("pre:0"), ":1") << "a stub or cold answers before the flush";

  std::atomic<bool> stop{false};
  std::vector<std::thread> writers;
  writers.reserve(2);
  for (int w = 0; w < 2; ++w) {
    writers.emplace_back([this, w, &stop] {
      for (int i = 0; !stop.load(); ++i) {
        EXPECT_EQ(Write({"SET", "post:" + std::to_string(w) + ":" + std::to_string(i), "v"}), "OK");
      }
    });
  }
  std::this_thread::sleep_for(5ms);
  auto flushed = engine_->DispatchFlush(core::FlushTarget::kThisDb);
  stop = true;
  for (auto& writer : writers) writer.join();
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
  EXPECT_EQ(flushed->AsString(), "OK");
  for (int i = 0; i < kKeys; ++i) {
    const std::string key = "pre:" + std::to_string(i);
    EXPECT_EQ(Read({"GET", key}), "nil") << key;
    EXPECT_EQ(Exists(key), ":0") << key;
  }
}

// FLUSHDB over two logs is one reservation and publishes every shard.
TEST_F(SequencedEngineTest, FlushDbOverTwoLogsCompletes) {
  Open(Options{.log_count = 2});
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
    ASSERT_EQ(Write({"SET", KeyOn(shard), "v"}), "OK");
  }
  auto flushed = engine_->DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
  EXPECT_EQ(queue::ReservationsHeld(), 0U);
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
    const auto logged = Logged(shard);
    ASSERT_EQ(logged.size(), 2U) << shard;
    EXPECT_TRUE(std::holds_alternative<core::entry::Flush>(logged[1].payload)) << shard;
    EXPECT_EQ(Read({"GET", KeyOn(shard)}), "nil");
  }
}

// An MSET whose frames cannot fit one segment fails whole, never
// partly applied and never retried.
TEST_F(SequencedEngineTest, AnMsetPastASegmentFailsCleanly) {
  Open(Options{.shards = 1, .segment_size_bytes = size_t{64} << 10});
  std::vector<std::string> mset{"MSET"};
  for (int i = 0; i < 200; ++i) {
    mset.push_back("k" + std::to_string(i));
    mset.emplace_back(512, 'v');
  }
  const std::string refused = Write(mset);
  EXPECT_NE(refused.find("exceeds the segment frame space"), std::string::npos) << refused;
  EXPECT_EQ(Read({"GET", "k0"}), "nil");
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kProcessCrash).value(), core::kFirstSeq);
  EXPECT_EQ(Write({"SET", "after", "v"}), "OK") << "the log is left usable";
}

// A WRONGTYPE reveals the key's state too, so it waits, as any reply,
// until the write it saw is durable.
TEST_F(SequencedEngineTest, AWrongTypeReplyWaitsUntilWhatItSawIsDurable) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  auto setter = std::async(std::launch::async, [this] { return Write({"SET", "k", "v"}); });
  bool applied = false;
  for (int i = 0; i < 500 && !applied; ++i) {
    applied = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = "k"}}).result.has_value();
    if (!applied) std::this_thread::sleep_for(1ms);
  }
  if (!applied) stall.Release();
  ASSERT_TRUE(applied);
  auto adder = std::async(std::launch::async, [this] { return Write({"SADD", "k", "m"}); });
  EXPECT_EQ(adder.wait_for(100ms), std::future_status::timeout) << "WRONGTYPE before durable";
  stall.Release();
  EXPECT_EQ(adder.get(), "error: Operation against a key holding the wrong kind of value");
  EXPECT_EQ(setter.get(), "OK");
}

// A resident entry is the key's latest state: past its TTL it is
// absent, though cold, behind, still holds an older value.
TEST_F(SequencedEngineTest, AnExpiredResidentKeyIsAbsentNotItsColdValue) {
  Open();
  EXPECT_EQ(Write({"SET", "k", "v1"}), "OK");
  DrainToCold(ShardOf("k"));
  EXPECT_EQ(Write({"SET", "k", "v2", "PX", "50"}), "OK");
  std::this_thread::sleep_for(80ms);
  EXPECT_EQ(Read({"GET", "k"}), "nil");
  EXPECT_EQ(Exists("k"), ":0");
  EXPECT_EQ(Describe(cold_->Exec(core::ops::ReadOp{core::ops::StringGet{.key = "k"}}).value()),
            "v1")
      << "the cold consumer was meant to be behind";
}

// A restart past a key's eviction window skips its creation, so replay
// leaves it non-resident rather than build it from its later writes;
// it is then served whole from cold.
TEST_F(SequencedEngineTest, ARestartPastTheEvictionWindowServesTheWholeKey) {
  Open();
  const int64_t t0 = std::chrono::duration_cast<std::chrono::milliseconds>(
                         core::WallClock::now().time_since_epoch())
                         .count();
  const int64_t window =
      std::chrono::duration_cast<std::chrono::milliseconds>(policy_.Resolve("k")).count();
  std::atomic<int64_t> now{t0};
  wall_ = [&now] { return core::WallTime{std::chrono::milliseconds{now.load()}}; };
  const std::string idle = KeyOn(ShardOf("k") == 0 ? 1 : 0, 0, "idle");
  EXPECT_EQ(Write({"SET", idle, "v"}), "OK");
  EXPECT_EQ(Write({"SADD", "k", "a"}), ":1");
  now = t0 + window - 60'000;
  EXPECT_EQ(Write({"SADD", "k", "b"}), ":1");
  for (const core::ShardId shard : {ShardOf("k"), ShardOf(idle)}) DrainToCold(shard);

  // Restarted a minute past the window: a fresh hot store replayed.
  engine_.reset();
  sequencer_.reset();
  loader_.reset();
  hot_ = NewHot();
  core::AppliedSeqNotifier notifier(core::AppliedSeqNotifierConfig{.shard_count = options_.shards});
  for (const core::ShardId shard : {ShardOf("k"), ShardOf(idle)}) {
    consumer::HotConsumer replayer(
        *queue_, *hot_, rpc_, notifier,
        consumer::HotConsumer::Config{.shard = shard,
                                      .replay_batch_size = 32,
                                      .read_timeout = core::Duration{10},
                                      .wall_clock =
                                          [t0, window] {
                                            return core::WallTime{
                                                std::chrono::milliseconds{t0 + window + 60'000}};
                                          }},
        policy_);
    std::atomic<bool> cancel{false};
    const auto tail = queue_->TailSeq(shard).value();
    ASSERT_TRUE(replayer.ReplayUntil(tail, cancel).has_value());
  }
  Rewire();
  {
    auto locks = hot_->LockExclusive(std::vector<core::ShardId>{ShardOf(idle)});
    EXPECT_EQ(std::chrono::floor<std::chrono::milliseconds>(locks.LastAppendedAt(ShardOf(idle))),
              core::WallTime{std::chrono::milliseconds{t0}})
        << "an idle shard's skipped frames still restore its stamp";
  }
  now = t0 + window + 60'000;
  EXPECT_EQ(Members("k"), (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(Write({"RENAMENX", "k", "k2"}, Flags::kNx), ":1");
  EXPECT_EQ(Members("k2"), (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(Members("k"), std::vector<std::string>{});
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
