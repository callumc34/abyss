#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/shard_router.h"
#include "abyss/hot/sharded_hot_store.h"
#include "mock_queue.h"
#include "test_clock.h"

namespace abyss::testing {

class IntegrationHarness {
 public:
  static constexpr uint32_t kShardCount = 4;
  static constexpr size_t kHotMemory = 4UL * 1024UL * 1024UL;

  IntegrationHarness() {
    tmp_dir_ = std::filesystem::temp_directory_path() / ("abyss_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(tmp_dir_);

    hot_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
        .max_memory_bytes = kHotMemory,
        .shard_count = kShardCount,
        .steady_clock = clock_.SteadyFn(),
        .wall_clock = clock_.WallFn(),
    });

    auto cold_result = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
        .data_path = (tmp_dir_ / "cold").string(),
        .write_buffer_size_bytes = 1024UL * 1024UL,
    });
    cold_ = std::move(cold_result).value();

    rpc_ = std::make_unique<core::ConsumerRpc>();

    ON_CALL(queue_, Append(::testing::_, ::testing::_))
        .WillByDefault([this](core::ShardId, const core::QueueEntry&) {
          std::promise<core::Result<void>> p;
          p.set_value(core::Result<void>{});
          return queue::AppendResult{.seq = next_seq_++, .durable = p.get_future()};
        });

    core::EvictionPolicy eviction_policy{std::chrono::seconds{86400}};
    cold_pool_ = std::make_unique<consumer::ColdConsumerPool>(
        queue_, *cold_, consumer::ColdConsumerPool::Config{.shard_count = kShardCount},
        eviction_policy, clock_.SteadyFn(), clock_.WallFn());

    engine_ = std::make_unique<engine::TieringEngine>(queue_, *hot_, *cold_, *cold_pool_, *rpc_,
                                                      kShardCount);
  }

  ~IntegrationHarness() {
    if (cold_pool_) cold_pool_->Stop();
    engine_.reset();
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
  core::ColdStore& Cold() { return *cold_; }
  consumer::ColdConsumerPool& ColdPool() { return *cold_pool_; }
  consumer::CompactionBuffer& BufferFor(std::string_view key) {
    auto shard = hot::ComputeShard(key, kShardCount);
    return cold_pool_->ConsumerFor(shard).Buffer();
  }
  TestClock& Clock() { return clock_; }
  ::testing::NiceMock<MockQueue>& Queue() { return queue_; }

 private:
  std::filesystem::path tmp_dir_;
  TestClock clock_;
  uint64_t next_seq_ = 1;

  ::testing::NiceMock<MockQueue> queue_;
  std::unique_ptr<hot::ShardedHotStore> hot_;
  std::unique_ptr<cold::backends::RocksdbStore> cold_;
  std::unique_ptr<consumer::ColdConsumerPool> cold_pool_;
  std::unique_ptr<core::ConsumerRpc> rpc_;
  std::unique_ptr<engine::TieringEngine> engine_;
};

}  // namespace abyss::testing
