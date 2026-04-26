#pragma once

#include <array>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/consumer/hot_consumer_pool.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/shard_router.h"
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

  IntegrationHarness() {
    tmp_dir_ = std::filesystem::temp_directory_path() /
               ("abyss_test_" + std::to_string(abyss::platform::fs::ProcessId()));
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
        .wall_clock = clock_.WallFn(),
    });
    cold_ = std::move(cold_result).value();

    rpc_ = std::make_unique<core::ConsumerRpc>();
    apply_notifier_ = std::make_unique<core::ApplyNotifier>();

    InstallQueueMocks();

    core::EvictionPolicy eviction_policy{std::chrono::seconds{86400}};
    cold_pool_ = std::make_unique<consumer::ColdConsumerPool>(
        queue_, *cold_, consumer::ColdConsumerPool::Config{.shard_count = kShardCount},
        eviction_policy, clock_.SteadyFn(), clock_.WallFn());

    engine_ = std::make_unique<engine::TieringEngine>(
        queue_, *hot_, *cold_, *cold_pool_, *rpc_,
        engine::TieringEngineConfig{.shard_count = kShardCount});

    hot_pool_ =
        std::make_unique<consumer::HotConsumerPool>(queue_, *hot_, *rpc_, *apply_notifier_,
                                                    consumer::HotConsumerPool::Config{
                                                        .shard_count = kShardCount,
                                                        .consumer =
                                                            consumer::HotConsumer::Config{
                                                                .read_batch_size = 32,
                                                                .read_timeout = core::Duration{10},
                                                            },
                                                    },
                                                    eviction_policy);
    hot_pool_->Start();
  }

  ~IntegrationHarness() {
    if (hot_pool_) hot_pool_->Stop();
    if (cold_pool_) cold_pool_->Stop();
    engine_.reset();
    hot_pool_.reset();
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
  consumer::HotConsumerPool& HotPool() { return *hot_pool_; }
  consumer::CompactionBuffer& BufferFor(std::string_view key) {
    auto shard = core::ComputeShard(key, kShardCount);
    return cold_pool_->ConsumerFor(shard).Buffer();
  }
  core::ConsumerRpc& Rpc() { return *rpc_; }
  TestClock& Clock() { return clock_; }
  ::testing::NiceMock<MockQueue>& Queue() { return queue_; }
  core::SequenceId PeekNextSeq() const { return next_seq_; }

 private:
  void InstallQueueMocks() {
    // NOLINTBEGIN(performance-unnecessary-value-param) — gmock forces by-value
    // lambda params to match the MOCK_METHOD signature.
    ON_CALL(queue_, BeginAppend(::testing::_, ::testing::_))
        .WillByDefault([this](core::ShardId shard, core::QueueEntry entry) {
          std::promise<core::Result<void>> p;
          p.set_value(core::Result<void>{});
          const auto seq = next_seq_++;
          entry.seq = seq;
          {
            const std::lock_guard lock(queue_mutex_);
            hot_pending_.at(shard).push_back(entry);
            cold_pending_.at(shard).push_back(std::move(entry));
          }
          return queue::PendingAppend{seq, p.get_future(), std::make_unique<NoopAppendPublisher>()};
        });
    ON_CALL(queue_, Append(::testing::_, ::testing::_))
        .WillByDefault([this](core::ShardId shard, core::QueueEntry entry) {
          std::promise<core::Result<void>> p;
          p.set_value(core::Result<void>{});
          const auto seq = next_seq_++;
          entry.seq = seq;
          {
            const std::lock_guard lock(queue_mutex_);
            hot_pending_.at(shard).push_back(entry);
            cold_pending_.at(shard).push_back(std::move(entry));
          }
          return queue::AppendResult{.seq = seq, .durable = p.get_future()};
        });
    ON_CALL(queue_, Read(::testing::_, ::testing::_, ::testing::_, ::testing::_))
        .WillByDefault([this](core::ConsumerId consumer, core::ShardId shard, size_t max_count,
                              core::Duration) -> core::Result<std::vector<core::QueueEntry>> {
          std::vector<core::QueueEntry> out;
          auto& dq =
              (consumer == core::kHotConsumer) ? hot_pending_.at(shard) : cold_pending_.at(shard);
          {
            const std::lock_guard lock(queue_mutex_);
            while (!dq.empty() && out.size() < max_count) {
              out.push_back(std::move(dq.front()));
              dq.pop_front();
            }
          }
          return out;
        });
    ON_CALL(queue_, Ack(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault(::testing::Return(core::Result<void>{}));
    // NOLINTEND(performance-unnecessary-value-param)
  }

  std::filesystem::path tmp_dir_;
  TestClock clock_;
  uint64_t next_seq_ = 1;

  ::testing::NiceMock<MockQueue> queue_;
  std::mutex queue_mutex_;
  std::array<std::deque<core::QueueEntry>, kShardCount> hot_pending_;
  std::array<std::deque<core::QueueEntry>, kShardCount> cold_pending_;

  std::unique_ptr<hot::ShardedHotStore> hot_;
  std::unique_ptr<cold::backends::RocksdbStore> cold_;
  std::unique_ptr<consumer::ColdConsumerPool> cold_pool_;
  std::unique_ptr<core::ConsumerRpc> rpc_;
  std::unique_ptr<core::ApplyNotifier> apply_notifier_;
  std::unique_ptr<engine::TieringEngine> engine_;
  std::unique_ptr<consumer::HotConsumerPool> hot_pool_;
};

}  // namespace abyss::testing
