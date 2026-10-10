#pragma once

#include <gtest/gtest.h>

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
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/predicate.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "abyss/engine/bounded_thread_shard_scheduler.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/read_path.h"
#include "abyss/engine/recovery_coordinator.h"
#include "abyss/engine/sequencer.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/queue/wal_queue.h"
#include "cold_read.h"
#include "temp_dir.h"

namespace abyss::engine {

using namespace std::chrono_literals;
using Flags = core::PredicateFlags;

struct Options {
  uint32_t shards = 4;
  uint32_t log_count = 1;
  core::Durability durability = core::Durability::kProcessCrash;
  size_t segment_size_bytes = size_t{1} << 20;
  size_t hot_memory_bytes = size_t{64} << 20;
  size_t buffer_high_water_bytes = size_t{512} << 20;
  bool fill_doorkeeper = true;
  double stub_memory_fraction = 0.02;
  double backpressure_ratio = 1.25;
  uint64_t fill_max_members = 1024;
  // Generous, so a slow runner never times a cold read out.
  std::chrono::milliseconds cold_read_deadline{std::chrono::seconds{5}};
  std::chrono::milliseconds cold_scan_deadline{std::chrono::seconds{10}};
  std::chrono::milliseconds write_timeout{5000};
  // How often committed offsets persist, and so segments are reclaimed.
  std::chrono::milliseconds offset_fsync_interval{std::chrono::hours{1}};
  // Prefixes the store directories, so one test can open several.
  std::string run;
};

// The clocks a restart's hot store and replayer read.
struct Clocks {
  core::SteadyClockFn steady = core::DefaultSteadyClock;
  core::WallClockFn wall = core::DefaultWallClock;
};

// A real log, hot store, RocksDB cold store and cold consumers, the
// consumers driven by hand, and the engine over them. Restart reopens
// the log and cold store and recovers a fresh hot store from them.
class SequencedEngineTest : public ::testing::Test {
 protected:
  void Open(Options options = {}) {
    options_ = std::move(options);
    OpenStores();
    hot_ = NewHot();
    Rewire();
  }

  // As a process restart: everything but the files goes, then recovery
  // rebuilds hot from the log and cold catches up.
  // With `sample_peak`, hot's memory is polled while recovery runs,
  // and its highest reading kept in peak_hot_bytes_.
  core::Result<void> Restart(Clocks clocks = {}, bool sample_peak = false) {
    CloseAll();
    OpenStores();
    if (!queue_ || !cold_ || !pool_) {
      return std::unexpected(core::Error{core::ErrorCode::kInternal, "the stores did not open"});
    }
    hot_clocks_ = std::move(clocks);
    hot_ = NewHot();
    BoundedThreadShardScheduler scheduler(4);
    RecoveryCoordinator coordinator(
        *queue_, *pool_, *hot_, scheduler,
        RecoveryConfig{.replay_parallelism = 2,
                       .replayer = HotReplayer::Config{.steady_clock = hot_clocks_.steady,
                                                       .wall_clock = hot_clocks_.wall}});
    const std::atomic<bool> cancel{false};
    std::atomic<bool> done{false};
    peak_hot_bytes_ = 0;
    std::thread sampler;
    if (sample_peak) {
      sampler = std::thread([this, &done] {
        while (!done.load()) {
          peak_hot_bytes_ = std::max(peak_hot_bytes_, hot_->Stats()->used_bytes);
          std::this_thread::sleep_for(100us);
        }
      });
    }
    auto recovered = coordinator.Run(cancel);
    done = true;
    if (sampler.joinable()) sampler.join();
    drain_requests_ = coordinator.Replayer().DrainRequests();
    skipped_ = coordinator.Replayer().Skipped();
    Rewire();
    return recovered;
  }

  void OpenStores() {
    auto queue = queue::WalQueue::Open(queue::WalConfig{
        .wal_path = dir_.Sub(options_.run + "wal").string(),
        .segment_size_bytes = options_.segment_size_bytes,
        .shard_count = options_.shards,
        .log_count = options_.log_count,
        .durability = options_.durability,
        .min_retention = 0s,
        .retention_consumers = {core::kColdConsumer},
        .offset_fsync_interval = options_.offset_fsync_interval,
    });
    ASSERT_TRUE(queue.has_value()) << queue.error().message();
    queue_ = std::move(*queue);
    auto cold = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
        .data_path = dir_.Sub(options_.run + "cold").string(),
        .shard_count = options_.shards,
        .log_clock = [this](core::ShardId shard) -> uint64_t {
          return pool_ ? pool_->LogClockMs(shard) : 0;
        },
    });
    ASSERT_TRUE(cold.has_value()) << cold.error().message();
    cold_ = std::move(*cold);
    consumer::ColdConsumer::Config cold_config;
    cold_config.quiet_threshold = 3600s;
    cold_config.jitter_fraction = 0.0;
    cold_config.queue_read_timeout = 20ms;
    cold_config.checkpoint_min_interval = 0ms;
    cold_config.buffer_high_water_bytes = options_.buffer_high_water_bytes;
    cold_config.rng_seed = 1;
    pool_ = std::make_unique<consumer::ColdConsumerPool>(
        *queue_, *cold_,
        consumer::ColdConsumerPool::Config{.shard_count = options_.shards, .consumer = cold_config},
        policy_);
  }

  std::unique_ptr<hot::ShardedHotStore> NewHot() {
    return std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = options_.hot_memory_bytes,
        .shard_count = options_.shards,
        .stub_memory_fraction = options_.stub_memory_fraction,
        .backpressure_ratio = options_.backpressure_ratio,
        .drained = [this](core::ShardId shard) -> core::SequenceId {
          return pool_ ? pool_->ConsumerFor(shard).LatestDrainedSeq() : 0;
        },
        .eviction_policy = &policy_,
        .steady_clock = hot_clocks_.steady,
        .wall_clock = hot_clocks_.wall,
    });
  }
  // The loader, sequencer and engine over hot_.
  void Rewire() {
    engine_.reset();
    reads_.reset();
    sequencer_.reset();
    const auto wall = [this] { return wall_ ? wall_() : core::WallClock::now(); };
    loader_ = std::make_unique<Loader>(*hot_, *pool_,
                                       loader_cold_ != nullptr ? *loader_cold_ : *cold_, wall);
    sequencer_ = std::make_unique<Sequencer>(
        *hot_, *queue_, *loader_, *pool_,
        SequencerConfig{.write_timeout = options_.write_timeout, .wall_clock = wall});
    reads_ =
        std::make_unique<ReadPath>(*hot_, *loader_, *sequencer_,
                                   ReadPathConfig{.write_timeout = options_.write_timeout,
                                                  .cold_read_deadline = options_.cold_read_deadline,
                                                  .cold_scan_deadline = options_.cold_scan_deadline,
                                                  .fill_doorkeeper = options_.fill_doorkeeper,
                                                  .fill_max_members = options_.fill_max_members,
                                                  .wall_clock = wall});
    engine_ = std::make_unique<TieringEngine>(*reads_, *sequencer_);
  }

  void CloseAll() {
    engine_.reset();
    reads_.reset();
    sequencer_.reset();
    loader_.reset();
    if (pool_) pool_->Stop(0ms);
    pool_.reset();
    hot_.reset();
    cold_.reset();
    queue_.reset();
  }

  void TearDown() override { CloseAll(); }

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
  // What cold alone holds for a GET of `key`.
  std::string ColdGet(const std::string& key) {
    auto read = abyss::testing::ColdRead(*cold_, core::ops::StringGet{.key = key});
    if (!read.has_value()) return "error: " + read.error().message();
    return Describe(*read);
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
  // Whether hot holds `key`: an entry, live, expired or deleted.
  bool Resident(std::string_view key) {
    const core::ShardId shard = ShardOf(key);
    auto locks = hot_->LockExclusive(std::vector<core::ShardId>{shard});
    const auto presence = locks.View(key, 0).presence;
    return presence != hot::KeyView::Presence::kNonResident &&
           presence != hot::KeyView::Presence::kStub;
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
  // The sequencer's, read path's and loader's wall clock, when set.
  std::function<core::WallTime()> wall_;
  // What the loader reads cold through, when set; else cold_.
  core::ColdStore* loader_cold_ = nullptr;
  Clocks hot_clocks_;
  Options options_;
  core::EvictionPolicy policy_{core::EvictionTTL{3600}};
  uint64_t drain_requests_ = 0;
  uint64_t skipped_ = 0;
  uint64_t peak_hot_bytes_ = 0;
  std::unique_ptr<queue::WalQueue> queue_;
  std::unique_ptr<cold::backends::RocksdbStore> cold_;
  std::unique_ptr<hot::ShardedHotStore> hot_;
  std::unique_ptr<consumer::ColdConsumerPool> pool_;
  std::unique_ptr<Loader> loader_;
  std::unique_ptr<Sequencer> sequencer_;
  std::unique_ptr<ReadPath> reads_;
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

inline std::vector<std::string> ArgsOf(const core::QueueEntry& entry) {
  if (const auto* write = std::get_if<core::entry::Write>(&entry.payload)) return write->cmd.args;
  return {"<flush>"};
}

}  // namespace abyss::engine
