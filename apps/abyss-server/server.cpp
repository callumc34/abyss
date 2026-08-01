#include "server.h"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "abyss/core/topology_manifest.h"
#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/resp/command_registry.h"
#include "abyss/version.h"

#ifdef ABYSS_HAVE_ROCKSDB
#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/cold/format/key_codec.h"
#endif

ABYSS_LOG_COMPONENT("abyss.server")

namespace abyss::server {

namespace {
constexpr std::chrono::milliseconds kStopPollInterval{100};
}  // namespace

Server::Server(config::Config config) : config_(std::move(config)) {
  lifecycle_gauge_ = metrics::Registry::Instance().Gauge(metrics::names::kServerLifecycleState);
  Transition(LifecycleState::kInitializing);
}

Server::~Server() { Shutdown(); }

void Server::Transition(LifecycleState next) {
  lifecycle_.store(next, std::memory_order_release);
  lifecycle_gauge_.Set(static_cast<double>(std::to_underlying(next)));
}

bool Server::Initialize() {
  // Honour the operator's metrics flag directly rather than relying on the
  // scrape endpoint simply not being bound. Per ADP-012 a disabled registry
  // scrapes empty while observation keeps updating state, so enabling it later
  // does not require any component to consult a flag on a hot path.
  metrics::Registry::Instance().SetEnabled(config_.metrics.enabled);

  if (config_.profile != "embedded") {
    ABYSS_LOG_CRITICAL("unsupported profile", {"profile", std::string_view{config_.profile}},
                       {"supported", std::string_view{"embedded"}});
    return false;
  }

#ifndef ABYSS_HAVE_ROCKSDB
  ABYSS_LOG_CRITICAL(
      ServerLog(),
      "embedded profile requires the RocksDB cold store; rebuild with -DABYSS_WITH_ROCKSDB=ON");
  return false;
#else
  std::error_code ec;
  std::filesystem::create_directories(config_.queue.wal_path, ec);
  if (ec) {
    ABYSS_LOG_CRITICAL("create WAL directory failed",
                       {"path", std::string_view{config_.queue.wal_path}}, {"err", ec.message()});
    return false;
  }
  std::filesystem::create_directories(config_.cold.data_path, ec);
  if (ec) {
    ABYSS_LOG_CRITICAL("create cold store directory failed",
                       {"path", std::string_view{config_.cold.data_path}}, {"err", ec.message()});
    return false;
  }

  // The FIRST data-dir action: validate (or, on a fresh dir, durably persist)
  // the cluster topology before WAL/cold/consumers open. A changed shard_count
  // or cold-encoding epoch refuses to start rather than silently re-routing the
  // WAL shard dirs and re-encoding every cold key prefix (ADP-014, finding G1).
  {
    core::TopologyDescriptor descriptor{
        .shard_count = config_.hot.shard_count,
        .cold_format_epoch = cold::format::kFormatVersion,
        .wire_slot_hash = core::kWireSlotScheme,
        .data_shard_hash = core::kDataShardScheme,
    };
    auto manifest = core::TopologyManifest::OpenOrValidate(config_.queue.wal_path, descriptor);
    if (!manifest.has_value()) {
      ABYSS_LOG_CRITICAL("topology manifest validation failed; refusing to start",
                         {"path", std::string_view{config_.queue.wal_path}},
                         {"err", std::string_view{manifest.error().message()}});
      return false;
    }
    topology_ = std::make_unique<core::TopologyManifest>(std::move(*manifest));
  }

  std::vector<core::EvictionRule> overrides;
  overrides.reserve(config_.hot.eviction_overrides.size());
  for (const auto& o : config_.hot.eviction_overrides) {
    overrides.push_back({.prefix = o.prefix, .eviction = o.eviction});
  }
  eviction_policy_ =
      std::make_unique<core::EvictionPolicy>(config_.hot.default_eviction, std::move(overrides));

  hot_store_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      .max_memory_bytes = config_.hot.max_memory_bytes,
      .shard_count = config_.hot.shard_count,
      .eviction_policy = eviction_policy_.get(),
  });

  auto fsync_policy = queue::FsyncPolicyFromString(config_.queue.fsync_policy);
  if (!fsync_policy.has_value()) {
    ABYSS_LOG_CRITICAL("invalid fsync_policy",
                       {"value", std::string_view{config_.queue.fsync_policy}},
                       {"err", std::string_view{fsync_policy.error().message()}});
    return false;
  }

  auto queue_result = queue::WalQueue::Open(queue::WalConfig{
      .wal_path = config_.queue.wal_path,
      .segment_size_bytes = config_.queue.segment_size_bytes,
      .max_value_size_bytes = config_.queue.max_value_size_bytes,
      .shard_count = hot_store_->shard_count(),
      .commit =
          {
              .policy = *fsync_policy,
              .interval = std::chrono::microseconds{config_.queue.group_commit_interval_us},
              .max_bytes = config_.queue.group_commit_max_bytes,
          },
      .min_retention = config_.queue.min_retention,
      // Cold and resolver gate retention; hot is volatile (replays from queue
      // on restart) per ADP-002 §"Eviction refresh vs queue retention".
      .retention_consumers = {core::kColdConsumer, core::kResolverConsumer},
      .volatile_consumers = {core::kHotConsumer},
  });
  if (!queue_result.has_value()) {
    ABYSS_LOG_CRITICAL("WAL open failed", {"path", std::string_view{config_.queue.wal_path}},
                       {"err", std::string_view{queue_result.error().message()}});
    return false;
  }
  if ((*queue_result)->IsRecovering()) {
    ABYSS_LOG_CRITICAL("WAL opened but still recovering; refusing to start");
    return false;
  }
  queue_ = std::move(*queue_result);

  consumer_rpc_ = std::make_unique<core::ConsumerRpc>(config_.consumer_rpc);
  // Must be indexed by the REAL shard count, not the default, or AwaitApplied /
  // NotifyApplied misroute across the modulo and read-your-write silently
  // breaks (ENGINE-3 wiring; the notifier requires the true shard_count).
  apply_notifier_ = std::make_unique<core::AppliedSeqNotifier>(
      core::AppliedSeqNotifierConfig{.shard_count = hot_store_->shard_count()});

  auto cold_result = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
      .data_path = config_.cold.data_path,
      // Must match the hot/consumer shard count.
      .shard_count = hot_store_->shard_count(),
      .write_buffer_size_bytes = config_.cold.write_buffer_size_bytes,
      .ttl_scanner = config_.cold.ttl_scanner,
  });
  if (!cold_result.has_value()) {
    ABYSS_LOG_CRITICAL("cold store open failed", {"path", std::string_view{config_.cold.data_path}},
                       {"err", std::string_view{cold_result.error().message()}});
    return false;
  }
  cold_store_ = std::move(*cold_result);

  cold_pool_ = std::make_unique<consumer::ColdConsumerPool>(
      *queue_, *cold_store_,
      consumer::ColdConsumerPool::Config{
          .shard_count = hot_store_->shard_count(),
          .consumer =
              consumer::ColdConsumer::Config{
                  .quiet_threshold = config_.cold_consumer.quiet_threshold,
                  .safety_margin = config_.cold_consumer.safety_margin,
                  .jitter_fraction = config_.cold_consumer.jitter_fraction,
                  .buffer_high_water_bytes = config_.cold_consumer.buffer_high_water_bytes,
                  .buffer_low_water_bytes = config_.cold_consumer.buffer_low_water_bytes,
                  .max_flush_batch_size = config_.cold_consumer.max_flush_batch_size,
                  .queue_read_max_count = config_.cold_consumer.queue_read_max_count,
                  .replay_batch_size = config_.recovery.cold_replay_batch_size,
                  .queue_read_timeout = config_.cold_consumer.queue_read_timeout,
                  .retry_initial_backoff = config_.cold_consumer.retry_initial_backoff,
                  .retry_max_backoff = config_.cold_consumer.retry_max_backoff,
                  .checkpoint_max_flushes = config_.cold_consumer.checkpoint_max_flushes,
                  .checkpoint_min_interval = config_.cold_consumer.checkpoint_min_interval,
                  .loop_initial_backoff = config_.cold_consumer.loop_initial_backoff,
                  .loop_max_backoff = config_.cold_consumer.loop_max_backoff,
              },
      },
      *eviction_policy_, *consumer_rpc_);

  hot_pool_ = std::make_unique<consumer::HotConsumerPool>(
      *queue_, *hot_store_, *consumer_rpc_, *apply_notifier_,
      consumer::HotConsumerPool::Config{
          .shard_count = hot_store_->shard_count(),
          .consumer =
              consumer::HotConsumer::Config{
                  .read_batch_size = config_.hot_consumer.read_batch_size,
                  .replay_batch_size = config_.recovery.hot_replay_batch_size,
                  .read_timeout = config_.hot_consumer.read_timeout,
              },
      },
      *eviction_policy_);

  engine_ = std::make_unique<engine::TieringEngine>(
      *queue_, *hot_store_, *cold_store_, *cold_pool_, *hot_pool_, *consumer_rpc_,
      engine::TieringEngineConfig{
          .shard_count = hot_store_->shard_count(),
          .write_timeout = config_.engine.write_timeout,
          .min_rpc_wait_fraction = config_.engine.min_rpc_wait_fraction,
          .buffer_consistency_wait_timeout = config_.engine.buffer_consistency_wait_timeout,
      });

  hot_eviction_worker_ = std::make_unique<hot::EvictionWorker>(
      *hot_store_, hot::EvictionWorker::Config{
                       .tick = config_.hot.eviction_tick,
                       // GC tombstones once the shard's cold consumer drains past them.
                       .tombstone_horizon =
                           [pool = cold_pool_.get()](core::ShardId shard) {
                             return pool->ConsumerFor(shard).LatestDrainedSeq();
                           },
                   });
  resolver_pool_ = std::make_unique<consumer::ResolverPool>(
      *queue_, *cold_store_, *cold_pool_, *consumer_rpc_, *apply_notifier_,
      consumer::ResolverPool::Config{
          .shard_count = hot_store_->shard_count(),
          .consumer =
              consumer::Resolver::Config{
                  .replay_batch_size = config_.recovery.resolver_replay_batch_size,
              },
      });

  recovery_scheduler_ =
      std::make_unique<engine::BoundedThreadShardScheduler>(config_.recovery.replay_parallelism);
  recovery_coordinator_ = std::make_unique<engine::RecoveryCoordinator>(
      *queue_, *resolver_pool_, *cold_pool_, *hot_pool_, *recovery_scheduler_,
      engine::RecoveryConfig{
          .replay_parallelism = config_.recovery.replay_parallelism,
          .hot_replay_batch_size = config_.recovery.hot_replay_batch_size,
          .cold_replay_batch_size = config_.recovery.cold_replay_batch_size,
          .resolver_replay_batch_size = config_.recovery.resolver_replay_batch_size,
      });

  auto identity = resp::NodeIdentity::Open(config_.queue.wal_path);
  if (!identity.has_value()) {
    ABYSS_LOG_CRITICAL("node identity load failed",
                       {"err", std::string_view{identity.error().message()}});
    return false;
  }
  node_identity_ = std::make_unique<resp::NodeIdentity>(std::move(*identity));

  stats_ = std::make_unique<ServerStatsImpl>(
      *queue_, *hot_store_, cold_store_.get(), std::string{kVersion}, config_.net.bind,
      /*advertise_address=*/std::string{}, /*mode=*/"standalone", config_.net.port,
      hot_store_->shard_count());
  config_provider_ = std::make_unique<ConfigProviderImpl>(config_);
  // The LOADING gate derives from the single lifecycle atomic: data commands
  // are gated until the state machine reaches kServing, which Server::Run
  // asserts ONLY after recovery completes AND every consumer pool is live. This
  // closes the ENGINE-1 window where the gate lifted on coordinator completion
  // alone, before the pools could fulfil an RPC. The gate is also re-asserted in
  // kDraining (>kServing is false) so shutdown stops accepting data writes.
  loading_ = std::make_unique<LoadingStateImpl>([this] { return !IsServing(); });
  resp_metrics_ = std::make_unique<resp::RespMetrics>(resp::GlobalRegistry());

  resp::PipelineDependencies pipeline_deps{
      .dispatcher = engine_.get(),
      .loading = loading_.get(),
      .stats = stats_.get(),
      .config = config_provider_.get(),
      .identity = node_identity_.get(),
      .metrics = resp_metrics_.get(),
  };

  net::TcpServerConfig tcp_config{
      .bind = config_.net.bind,
      .port = config_.net.port,
      .max_connections = config_.net.max_connections,
      .accept_queue = config_.net.accept_queue,
      .io_threads = config_.net.io_threads,
      .connection =
          net::ConnectionConfig{
              .max_read_buffer_bytes = config_.net.max_read_buffer_bytes,
              .write_backpressure_bytes = config_.net.write_backpressure_bytes,
              .write_resume_bytes = config_.net.write_resume_bytes,
              .write_hard_limit_bytes = config_.net.write_hard_limit_bytes,
              .idle_timeout = config_.net.idle_timeout,
          },
      .shutdown_grace = config_.net.shutdown_grace,
      .reaper_tick = config_.net.reaper_tick,
  };
  tcp_server_ = std::make_unique<net::TcpServer>(tcp_config, resp::GlobalRegistry(), pipeline_deps);

  stats_->set_connection_count_provider(
      [server_ptr = tcp_server_.get()] { return server_ptr->ActiveConnections(); });

  status_provider_ = std::make_unique<StatusProviderImpl>(StatusProviderImpl::Deps{
      .config = &config_,
      .queue = queue_.get(),
      .hot_store = hot_store_.get(),
      .cold_store = cold_store_.get(),
      .hot_pool = hot_pool_.get(),
      .cold_pool = cold_pool_.get(),
      .resolver_pool = resolver_pool_.get(),
      .recovery_coordinator = recovery_coordinator_.get(),
      .node_identity = node_identity_.get(),
      .ready = [this] { return IsReady(); },
      .loading = [provider =
                      loading_.get()] { return provider != nullptr && provider->IsLoading(); },
      .shutting_down = [this] { return IsShuttingDown(); },
      .connection_count =
          [server_ptr = tcp_server_.get()] {
            return server_ptr != nullptr ? server_ptr->ActiveConnections() : size_t{0};
          },
      .reaper_failures =
          [queue_ptr = queue_.get()] {
            return queue_ptr != nullptr ? queue_ptr->ReaperFailures() : uint64_t{0};
          },
      .oldest_eligible_unreaped_age_ms = [queue_ptr = queue_.get()]() -> uint64_t {
        if (queue_ptr == nullptr) return 0;
        const auto age = queue_ptr->OldestEligibleUnreapedAge();
        return static_cast<uint64_t>(age.value_or(core::Duration::zero()).count());
      },
      .read_buffer_high_water_bytes = [server_ptr = tcp_server_.get()]() -> uint64_t {
        return server_ptr != nullptr ? server_ptr->MaxReadBufferHighWaterBytes() : uint64_t{0};
      },
      .resp_port =
          [server_ptr = tcp_server_.get()] {
            return server_ptr != nullptr ? server_ptr->BoundPort() : uint16_t{0};
          },
      .admin_port = [this] { return AdminBoundPort(); },
      .metrics_port = [this] { return MetricsBoundPort(); },
  });

  metrics_snapshotter_ = std::make_unique<admin::MetricsSnapshotter>(*status_provider_);

  health_handler_ = std::make_unique<admin::HealthHandler>();
  ready_handler_ = std::make_unique<admin::ReadyHandler>(admin::ReadyChecks{
      .tcp_bound =
          [server_ptr = tcp_server_.get()] {
            return server_ptr != nullptr && server_ptr->IsRunning();
          },
      .recovery_complete =
          [provider = loading_.get()] { return provider != nullptr && !provider->IsLoading(); },
      .not_shutting_down = [this] { return !IsShuttingDown(); },
  });
  status_handler_ = std::make_unique<admin::StatusHandler>(status_provider_.get());
  metrics_handler_ = std::make_unique<admin::MetricsHandler>(
      [] { return metrics::Registry::Instance().Scrape(); });

  if (config_.admin.enabled) {
    admin_http_ = std::make_unique<admin::HttpServer>(admin::HttpServerConfig{
        .bind = config_.admin.bind,
        .port = config_.admin.port,
        .label = "admin",
    });
    admin_http_->AddHandler("/healthz", health_handler_.get());
    admin_http_->AddHandler("/ready", ready_handler_.get());
    admin_http_->AddHandler("/status", status_handler_.get());
  }

  if (config_.metrics.enabled) {
    metrics_http_ = std::make_unique<admin::HttpServer>(admin::HttpServerConfig{
        .bind = config_.metrics.bind,
        .port = config_.metrics.port,
        .label = "metrics",
    });
    metrics_http_->AddHandler("/metrics", metrics_handler_.get());
  }

  ABYSS_LOG_INFO("server initialized",
                 {"shard_count", static_cast<int64_t>(hot_store_->shard_count())});
  return true;
#endif
}

core::Result<void> Server::Run(const std::atomic<bool>& stop) {
  if (!tcp_server_) {
    return std::unexpected(core::Error{core::ErrorCode::kInternal, "server not initialized"});
  }

  // Bind admin and metrics first so /healthz and /ready answer immediately.
  // /ready reports 503 (recovery_complete=false) until lifecycle_ reaches
  // kServing, i.e. until recovery completes AND the consumer pools are live.
  if (admin_http_) {
    if (auto r = admin_http_->Start(); !r.has_value()) {
      ABYSS_LOG_CRITICAL("admin http start failed", {"err", std::string_view{r.error().message()}});
      return std::unexpected(r.error());
    }
  }
  if (metrics_http_) {
    if (auto r = metrics_http_->Start(); !r.has_value()) {
      ABYSS_LOG_CRITICAL("metrics http start failed",
                         {"err", std::string_view{r.error().message()}});
      if (admin_http_) admin_http_->Stop();
      return std::unexpected(r.error());
    }
  }

  // Bind the data-plane listener. The LOADING gate (loading_->IsLoading(),
  // backed by lifecycle_ != kServing) makes data commands return -LOADING until
  // the kServing edge below; admin RESP commands stay available throughout.
  if (auto r = tcp_server_->Start(); !r.has_value()) {
    ABYSS_LOG_CRITICAL("tcp server start failed", {"err", std::string_view{r.error().message()}});
    if (admin_http_) admin_http_->Stop();
    if (metrics_http_) metrics_http_->Stop();
    return std::unexpected(r.error());
  }
  config_.net.port = tcp_server_->BoundPort();
  if (stats_) stats_->SetTcpPort(config_.net.port);

  ABYSS_LOG_INFO("listening", {"version", std::string_view{kVersion}},
                 {"bind", std::string_view{config_.net.bind}},
                 {"port", static_cast<int64_t>(config_.net.port)},
                 {"admin_port", static_cast<int64_t>(AdminBoundPort())},
                 {"metrics_port", static_cast<int64_t>(MetricsBoundPort())});

  // Enter recovery: pure queue replay. The LOADING gate stays asserted and
  // /ready returns 503 throughout (lifecycle_ != kServing). Blocks until all
  // phases complete or the shutdown signal arrives. On cancellation the
  // coordinator returns kUnavailable and we fall through to a clean shutdown
  // without ever reaching kServing — /ready stays 503 until process exit.
  Transition(LifecycleState::kRecovering);
  if (recovery_coordinator_) {
    if (auto r = recovery_coordinator_->Run(stop); !r.has_value()) {
      ABYSS_LOG_CRITICAL("recovery failed; shutting down",
                         {"err", std::string_view{r.error().message()}});
      Shutdown();
      return std::unexpected(r.error());
    }
  }

  // Recovery is complete; start the per-shard consumer threads for steady-
  // state tailing, plus the eviction maintenance worker. The LOADING gate is
  // STILL asserted here — it lifts only at the single kServing edge below,
  // strictly after a hot consumer thread exists to fulfil write/conditional
  // RPC promises (ENGINE-1: never serve with no consumer).
  if (hot_eviction_worker_) hot_eviction_worker_->Start();
  if (resolver_pool_) resolver_pool_->Start();
  if (cold_pool_) cold_pool_->Start();
  if (hot_pool_) hot_pool_->Start();

  // Cold-store background work (TTL scanner). Deliberately deferred until
  // after recovery so it doesn't contend with replay on disk I/O.
  if (!stop.load(std::memory_order_acquire) && cold_store_) {
    if (auto r = cold_store_->Start(); !r.has_value()) {
      ABYSS_LOG_WARN("cold store start failed", {"err", std::string_view{r.error().message()}});
    }
  }

  // The single gate-lifting edge: pools are live AND recovery is done, so the
  // LOADING gate (loading_ predicate) and /ready flip in one observable
  // transition. A late SIGTERM may have arrived during pool startup; if so,
  // skip kServing and head straight to shutdown so we never open the gate while
  // already tearing down.
  if (!stop.load(std::memory_order_acquire)) {
    Transition(LifecycleState::kServing);
  }
  // Emit the readiness pipe line only after all subsystems are up — operators
  // and supervisors that wait on it then know the data plane is serving.
  NotifyReady();
  ABYSS_LOG_INFO("server ready");

  // Snapshot gauges are pushed on their own cadence rather than the stop-poll:
  // each Observe reads store statistics (including RocksDB property lookups),
  // which are cheap but not free at the poll rate.
  auto next_snapshot = std::chrono::steady_clock::now();
  while (!stop.load(std::memory_order_acquire) && tcp_server_->IsRunning()) {
    if (metrics_snapshotter_) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= next_snapshot) {
        metrics_snapshotter_->Observe();
        next_snapshot = now + config_.metrics.snapshot_interval;
      }
    }
    std::this_thread::sleep_for(kStopPollInterval);
  }

  Shutdown();
  return {};
}

void Server::NotifyReady() {
  if (ready_fd_ < 0) return;

  std::string line = R"({"bind":")";
  line += config_.net.bind;
  line += R"(","port":)";
  line += std::to_string(config_.net.port);
  if (admin_http_) {
    line += R"(,"admin_bind":")";
    line += config_.admin.bind;
    line += R"(","admin_port":)";
    line += std::to_string(AdminBoundPort());
  }
  if (metrics_http_) {
    line += R"(,"metrics_bind":")";
    line += config_.metrics.bind;
    line += R"(","metrics_port":)";
    line += std::to_string(MetricsBoundPort());
  }
  line += "}\n";

#ifdef _WIN32
  auto handle = reinterpret_cast<HANDLE>(ready_fd_);
  DWORD written = 0;
  WriteFile(handle, line.data(), static_cast<DWORD>(line.size()), &written, nullptr);
  CloseHandle(handle);
#else
  const int fd = static_cast<int>(ready_fd_);
  const char* data = line.data();
  size_t remaining = line.size();
  while (remaining > 0) {
    const auto n = ::write(fd, data, remaining);
    if (n < 0) {
      if (errno == EINTR) continue;
      break;
    }
    data += n;
    remaining -= static_cast<size_t>(n);
  }
  ::close(fd);
#endif
  ready_fd_ = -1;
}

void Server::Shutdown() {
  // Single-shot guard: only the first caller past kDraining runs teardown.
  // CAS from any pre-draining state to kDraining; a concurrent/repeat call
  // observes >=kDraining and returns. (~Server and the Run loop both call
  // Shutdown.)
  for (;;) {
    auto current = lifecycle_.load(std::memory_order_acquire);
    if (current >= LifecycleState::kDraining) return;
    if (lifecycle_.compare_exchange_weak(current, LifecycleState::kDraining,
                                         std::memory_order_acq_rel)) {
      lifecycle_gauge_.Set(static_cast<double>(std::to_underlying(LifecycleState::kDraining)));
      break;
    }
  }

  ABYSS_LOG_INFO("shutdown starting");

  // kDraining re-asserts the LOADING gate and flips /ready to 503 (both derive
  // from lifecycle_ != kServing) so K8s pulls the pod from Service endpoints
  // before we tear down the data plane. Admin liveness keeps responding via the
  // http servers below until they're stopped.
  if (admin_http_) admin_http_->Stop();
  if (metrics_http_) metrics_http_->Stop();

  if (tcp_server_) {
    tcp_server_->Stop();
  }

  // Stop the eviction worker before the cold pool: its tombstone-GC tick reads
  // the cold consumers' drained seq, so it must not run once the pool stops.
  if (hot_eviction_worker_) hot_eviction_worker_->Stop();
  // Graceful cold drain (G6): flush the in-memory compaction buffer to durable
  // cold and advance the cold ack BEFORE the hard stop, bounded by the
  // shutdown grace budget. A rolling restart no longer discards the buffer and
  // forces a full cold replay. The drain Checkpoints (durable) before acking,
  // so the advanced ack is never past durable; on deadline expiry the remaining
  // slice replays from the WAL (correctness preserved). Cold store is stopped
  // AFTER the pool so the drain's ApplyBatch sees a live store.
  if (cold_pool_) {
    cold_pool_->Stop(
        std::chrono::duration_cast<std::chrono::milliseconds>(config_.cold_consumer.drain_grace));
  }
  if (hot_pool_) hot_pool_->Stop();
  if (resolver_pool_) resolver_pool_->Stop();
  if (cold_store_) {
    if (auto r = cold_store_->Stop(); !r.has_value()) {
      ABYSS_LOG_WARN("cold store stop failed", {"err", std::string_view{r.error().message()}});
    }
  }

  Transition(LifecycleState::kStopped);
  ABYSS_LOG_INFO("shutdown complete");
}

}  // namespace abyss::server
