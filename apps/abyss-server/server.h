#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "abyss/config/config.h"
#include "abyss/consumer/cold_consumer.h"
#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/hot_consumer.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/queue/wal_queue.h"

#ifdef ABYSS_HAVE_ROCKSDB
#include "abyss/cold/backends/rocksdb_store.h"
#endif

namespace abyss::server {

class Server {
 public:
  explicit Server(config::Config config);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  Server(Server&&) = delete;
  Server& operator=(Server&&) = delete;

  bool Initialize();
  void Run(const std::atomic<bool>& stop);
  void Shutdown();
  bool IsReady() const { return ready_.load(std::memory_order_acquire); }

 private:
  bool SetupListener();
  void HandleConnection(int client_fd, std::atomic<bool>& finished);
  void CleanFinishedConnections();

  config::Config config_;

  // Component graph. Declaration order = construction order.
  // Destruction is reverse — dependents destroyed before their dependencies.
  std::unique_ptr<queue::WalQueue> queue_;
  std::unique_ptr<hot::ShardedHotStore> hot_store_;
#ifdef ABYSS_HAVE_ROCKSDB
  std::unique_ptr<cold::backends::RocksdbStore> cold_store_;
#endif
  std::unique_ptr<consumer::CompactionBuffer> compaction_buffer_;
  std::unique_ptr<core::ConsumerRpc> consumer_rpc_;
  std::unique_ptr<engine::TieringEngine> engine_;
  std::unique_ptr<consumer::HotConsumer> hot_consumer_;
  std::unique_ptr<consumer::ColdConsumer> cold_consumer_;

  int listen_fd_ = -1;
  std::atomic<bool> ready_{false};
  std::atomic<bool> shutting_down_{false};
  std::atomic<uint64_t> next_client_id_{1};

  struct TrackedConnection {
    std::thread thread;
    int fd = -1;
    std::atomic<bool> finished{false};
  };
  std::mutex connections_mutex_;
  std::vector<std::unique_ptr<TrackedConnection>> connections_;
};

}  // namespace abyss::server
