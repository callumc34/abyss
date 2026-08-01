#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "abyss/admin/health_handler.h"
#include "abyss/admin/http_server.h"
#include "abyss/admin/metrics_handler.h"
#include "abyss/admin/metrics_snapshotter.h"
#include "abyss/admin/ready_handler.h"
#include "abyss/admin/status_handler.h"
#include "abyss/config/config.h"
#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/consumer/hot_consumer_pool.h"
#include "abyss/consumer/resolver_pool.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/result.h"
#include "abyss/core/topology_manifest.h"
#include "abyss/engine/bounded_thread_shard_scheduler.h"
#include "abyss/engine/recovery_coordinator.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/eviction_worker.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"
#include "abyss/net/tcp_server.h"
#include "abyss/queue/wal_queue.h"
#include "abyss/resp/metrics.h"
#include "abyss/resp/node_identity.h"
#include "providers.h"

namespace abyss::server {

class Server {
 public:
  // The single source of truth for the process lifecycle. The client-visible
  // serving predicate (the RESP LOADING gate and /ready) is derived ONLY from
  // this value reaching kServing, which happens strictly after recovery
  // completes AND every consumer pool has started — so there is no state in
  // which the data plane is open with no consumer to fulfil an RPC (ENGINE-1).
  // Monotonic forward order; never moves backward.
  enum class LifecycleState : uint8_t {
    kInitializing = 0,  // signal handling armed, topology validated, stores opening
    kRecovering = 1,    // pure queue replay; LOADING asserted, /ready 503
    kServing = 2,       // pools live + recovery done; LOADING lifted in one edge
    kDraining = 3,      // graceful SIGTERM drain in progress; LOADING re-asserted
    kStopped = 4,       // teardown complete
  };

  explicit Server(config::Config config);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;
  Server(Server&&) = delete;
  Server& operator=(Server&&) = delete;

  bool Initialize();
  core::Result<void> Run(const std::atomic<bool>& stop);
  void Shutdown();

  LifecycleState Lifecycle() const { return lifecycle_.load(std::memory_order_acquire); }
  bool IsServing() const { return Lifecycle() == LifecycleState::kServing; }
  bool IsReady() const { return IsServing(); }
  bool IsShuttingDown() const { return Lifecycle() >= LifecycleState::kDraining; }

  uint16_t AdminBoundPort() const noexcept {
    return admin_http_ != nullptr ? admin_http_->BoundPort() : 0;
  }
  uint16_t MetricsBoundPort() const noexcept {
    return metrics_http_ != nullptr ? metrics_http_->BoundPort() : 0;
  }

  // POSIX: file descriptor. Windows: HANDLE reinterpreted as intptr_t.
  void set_ready_fd(intptr_t fd) { ready_fd_ = fd; }

 private:
  void NotifyReady();
  // Stores the new state and reflects it on the lifecycle-state gauge. The
  // single place the state changes, so the metric never drifts from the atomic.
  void Transition(LifecycleState next);

  config::Config config_;

  // Component graph. Declaration order = construction order.
  // Destruction is reverse — dependents destroyed before their dependencies.
  //
  // Single source of truth for per-prefix eviction. Borrowed by ShardedHotStore,
  // HotConsumerPool, ColdConsumerPool — declared above them so it outlives
  // every borrower. Do not reorder.
  std::unique_ptr<core::EvictionPolicy> eviction_policy_;
  // Validated cluster topology. Written/checked before any data subsystem opens.
  std::unique_ptr<core::TopologyManifest> topology_;
  std::unique_ptr<queue::WalQueue> queue_;
  std::unique_ptr<hot::ShardedHotStore> hot_store_;
  std::unique_ptr<core::ColdStore> cold_store_;
  std::unique_ptr<consumer::ColdConsumerPool> cold_pool_;
  // Declared after cold_pool_ so it is destroyed before it.
  std::unique_ptr<hot::EvictionWorker> hot_eviction_worker_;
  std::unique_ptr<core::ConsumerRpc> consumer_rpc_;
  std::unique_ptr<core::ApplyNotifier> apply_notifier_;
  std::unique_ptr<engine::TieringEngine> engine_;
  std::unique_ptr<consumer::HotConsumerPool> hot_pool_;
  std::unique_ptr<consumer::ResolverPool> resolver_pool_;
  std::unique_ptr<engine::BoundedThreadShardScheduler> recovery_scheduler_;
  std::unique_ptr<engine::RecoveryCoordinator> recovery_coordinator_;

  // Frontend providers — outlive the TCP server.
  std::unique_ptr<resp::NodeIdentity> node_identity_;
  std::unique_ptr<ServerStatsImpl> stats_;
  std::unique_ptr<ConfigProviderImpl> config_provider_;
  std::unique_ptr<LoadingStateImpl> loading_;
  std::unique_ptr<resp::RespMetrics> resp_metrics_;

  std::unique_ptr<net::TcpServer> tcp_server_;

  // Admin / metrics HTTP servers. Both are torn down at the head of Shutdown
  // so /ready flips to 503 before consumers stop, giving K8s its drain window.
  std::unique_ptr<StatusProviderImpl> status_provider_;
  // Reads status_provider_; declared after it so it is destroyed first.
  std::unique_ptr<admin::MetricsSnapshotter> metrics_snapshotter_;
  std::unique_ptr<admin::HealthHandler> health_handler_;
  std::unique_ptr<admin::ReadyHandler> ready_handler_;
  std::unique_ptr<admin::StatusHandler> status_handler_;
  std::unique_ptr<admin::MetricsHandler> metrics_handler_;
  std::unique_ptr<admin::HttpServer> admin_http_;
  std::unique_ptr<admin::HttpServer> metrics_http_;

  intptr_t ready_fd_ = -1;
  std::atomic<LifecycleState> lifecycle_{LifecycleState::kInitializing};
  metrics::GaugeHandle lifecycle_gauge_;
};

}  // namespace abyss::server
