#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/shard_router.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/sequencer.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/platform/fs.h"
#include "mock_queue.h"
#include "test_clock.h"

namespace abyss::testing {

class IntegrationHarness {
 public:
  static constexpr uint32_t kShardCount = 4;
  static constexpr size_t kHotMemory = 4UL * 1024UL * 1024UL;

  struct Config {
    std::optional<core::EvictionPolicy> eviction_policy;
  };

  IntegrationHarness() : IntegrationHarness(Config{}) {}
  explicit IntegrationHarness(Config cfg) {
    if (cfg.eviction_policy.has_value()) {
      eviction_policy_ = std::move(*cfg.eviction_policy);
    }
    // Unique per harness instance: PID alone collides across sequential tests
    // in the same binary, leaking cold RocksDB state between them. Add an
    // atomic counter + steady-clock stamp so every instance gets a private dir.
    static std::atomic<uint64_t> harness_counter{0};
    const auto unique = std::to_string(abyss::platform::fs::ProcessId()) + "_" +
                        std::to_string(harness_counter.fetch_add(1, std::memory_order_relaxed)) +
                        "_" +
                        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    tmp_dir_ = std::filesystem::temp_directory_path() / ("abyss_test_" + unique);
    std::filesystem::create_directories(tmp_dir_);

    // The sequencer stamps appended_at by this clock, and cold expires by
    // appended_at: start it at the real time so its TTLs look real.
    clock_.SetWall(core::WallClock::now());

    hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = kHotMemory,
        .shard_count = kShardCount,
        .eviction_policy = &eviction_policy_,
        .steady_clock = clock_.SteadyFn(),
        .wall_clock = clock_.WallFn(),
    });

    auto cold_result = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
        .data_path = (tmp_dir_ / "cold").string(),
        .shard_count = kShardCount,
        .write_buffer_size_bytes = 1024UL * 1024UL,
        .wall_clock = clock_.WallFn(),
        .log_clock = [this](core::ShardId shard) -> uint64_t {
          return cold_pool_ ? cold_pool_->LogClockMs(shard) : 0;
        },
    });
    cold_ = std::move(cold_result).value();

    rpc_ = std::make_unique<core::ConsumerRpc>();

    InstallQueueMocks();

    cold_pool_ = std::make_unique<consumer::ColdConsumerPool>(
        queue_, *cold_, consumer::ColdConsumerPool::Config{.shard_count = kShardCount},
        eviction_policy_, *rpc_, clock_.SteadyFn(), clock_.WallFn());

    loader_ = std::make_unique<engine::Loader>(*hot_, *cold_pool_, *cold_, clock_.WallFn());
    sequencer_ =
        std::make_unique<engine::Sequencer>(*hot_, queue_, *loader_, *cold_pool_,
                                            engine::SequencerConfig{.wall_clock = clock_.WallFn()});
    engine_ = std::make_unique<engine::TieringEngine>(
        *hot_, *cold_, *cold_pool_, *sequencer_,
        engine::TieringEngineConfig{.shard_count = kShardCount});
  }

  ~IntegrationHarness() {
    if (cold_pool_) cold_pool_->Stop();
    engine_.reset();
    sequencer_.reset();
    loader_.reset();
    cold_pool_.reset();
    cold_.reset();
    hot_.reset();
    rpc_.reset();
    std::filesystem::remove_all(tmp_dir_);
  }

  IntegrationHarness(const IntegrationHarness&) = delete;
  IntegrationHarness& operator=(const IntegrationHarness&) = delete;
  IntegrationHarness(IntegrationHarness&&) = delete;
  IntegrationHarness& operator=(IntegrationHarness&&) = delete;

  engine::TieringEngine& Engine() { return *engine_; }
  core::HotStore& Hot() { return *hot_; }
  hot::ShardedHotStore& ShardedHot() { return *hot_; }
  core::ColdStore& Cold() { return *cold_; }

  core::Result<core::RespValue> SeedHot(std::initializer_list<std::string> args) {
    core::RespCommand cmd{.args = std::vector<std::string>(args)};
    return engine_->DispatchWrite(cmd.args.empty() ? std::string_view{} : cmd.args.front(),
                                  std::move(cmd));
  }
  consumer::ColdConsumerPool& ColdPool() { return *cold_pool_; }
  engine::Sequencer& Sequencer() { return *sequencer_; }
  consumer::CompactionBuffer& BufferFor(std::string_view key) {
    auto shard = core::ComputeShard(key, kShardCount);
    return cold_pool_->ConsumerFor(shard).Buffer();
  }
  core::ConsumerRpc& Rpc() { return *rpc_; }
  TestClock& Clock() { return clock_; }
  ::testing::NiceMock<MockQueue>& Queue() { return queue_; }

 private:
  void InstallQueueMocks() {
    // NOLINTBEGIN(performance-unnecessary-value-param) — gmock forces by-value
    // lambda params to match the MOCK_METHOD signature.
    ON_CALL(queue_, Read(::testing::_, ::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .WillByDefault([this](core::ShardId shard, core::SequenceId from_seq, size_t max_count,
                              core::Duration,
                              core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
          return queue_.ReadPublished(shard, from_seq, max_count);
        });
    ON_CALL(queue_, CommitOffset(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault(::testing::Return(core::Result<void>{}));
    // NOLINTEND(performance-unnecessary-value-param)
  }

  std::filesystem::path tmp_dir_;
  TestClock clock_;
  core::EvictionPolicy eviction_policy_{std::chrono::seconds{86400}};

  ::testing::NiceMock<MockQueue> queue_;

  std::unique_ptr<hot::ShardedHotStore> hot_;
  std::unique_ptr<cold::backends::RocksdbStore> cold_;
  std::unique_ptr<consumer::ColdConsumerPool> cold_pool_;
  std::unique_ptr<core::ConsumerRpc> rpc_;
  std::unique_ptr<engine::Loader> loader_;
  std::unique_ptr<engine::Sequencer> sequencer_;
  std::unique_ptr<engine::TieringEngine> engine_;
};

}  // namespace abyss::testing
