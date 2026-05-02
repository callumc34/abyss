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

#include "abyss/log/log.h"
#include "abyss/resp/command_registry.h"
#include "abyss/version.h"

#ifdef ABYSS_HAVE_ROCKSDB
#include "abyss/cold/backends/rocksdb_store.h"
#endif

ABYSS_LOG_COMPONENT("abyss.server")

namespace abyss::server {

namespace {
constexpr std::chrono::milliseconds kStopPollInterval{100};
}  // namespace

Server::Server(config::Config config) : config_(std::move(config)) {}

Server::~Server() { Shutdown(); }

bool Server::Initialize() {
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

  hot_store_ = std::make_unique<hot::ShardedHotStore>(hot::ShardedHotStoreConfig{
      .max_memory_bytes = config_.hot.max_memory_bytes,
      .shard_count = config_.hot.shard_count,
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
  apply_notifier_ = std::make_unique<core::ApplyNotifier>();

  auto cold_result = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
      .data_path = config_.cold.data_path,
      .write_buffer_size_bytes = config_.cold.write_buffer_size_bytes,
  });
  if (!cold_result.has_value()) {
    ABYSS_LOG_CRITICAL("cold store open failed", {"path", std::string_view{config_.cold.data_path}},
                       {"err", std::string_view{cold_result.error().message()}});
    return false;
  }
  cold_store_ = std::move(*cold_result);

  std::vector<core::EvictionRule> overrides;
  overrides.reserve(config_.hot.eviction_overrides.size());
  for (const auto& o : config_.hot.eviction_overrides) {
    overrides.push_back({.prefix = o.prefix, .eviction = o.eviction});
  }
  core::EvictionPolicy eviction_policy{config_.hot.default_eviction, std::move(overrides)};

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
                  .queue_read_timeout = config_.cold_consumer.queue_read_timeout,
                  .retry_initial_backoff = config_.cold_consumer.retry_initial_backoff,
                  .retry_max_backoff = config_.cold_consumer.retry_max_backoff,
              },
      },
      eviction_policy);

  engine_ = std::make_unique<engine::TieringEngine>(
      *queue_, *hot_store_, *cold_store_, *cold_pool_, *consumer_rpc_,
      engine::TieringEngineConfig{
          .shard_count = hot_store_->shard_count(),
          .write_timeout = config_.engine.write_timeout,
          .min_rpc_wait_fraction = config_.engine.min_rpc_wait_fraction,
      });

  hot_pool_ = std::make_unique<consumer::HotConsumerPool>(
      *queue_, *hot_store_, *consumer_rpc_, *apply_notifier_,
      consumer::HotConsumerPool::Config{
          .shard_count = hot_store_->shard_count(),
          .consumer =
              consumer::HotConsumer::Config{
                  .read_batch_size = config_.hot_consumer.read_batch_size,
                  .read_timeout = config_.hot_consumer.read_timeout,
              },
      },
      eviction_policy);

  hot_eviction_worker_ = std::make_unique<hot::EvictionWorker>(
      *hot_store_,
      hot::EvictionWorker::Config{
          .tick = config_.hot.eviction_tick,
          .default_eviction =
              core::EvictionTTL{static_cast<uint64_t>(config_.hot.default_eviction.count())},
      });
  resolver_pool_ = std::make_unique<consumer::ResolverPool>(
      *queue_, *cold_store_, *cold_pool_, *consumer_rpc_, *apply_notifier_,
      consumer::ResolverPool::Config{
          .shard_count = hot_store_->shard_count(),
          .consumer = consumer::Resolver::Config{},
      });

  // Resolver replay must finish before any consumer starts. ADP-011 §Recovery.
  if (auto r = resolver_pool_->ReplayForRecovery(); !r.has_value()) {
    ABYSS_LOG_CRITICAL("resolver replay failed", {"err", std::string_view{r.error().message()}});
    return false;
  }

  // Captured before clients connect: LOADING flips once hot catches up to here.
  std::vector<core::SequenceId> ready_watermarks(hot_store_->shard_count(), 0);
  for (uint32_t s = 0; s < hot_store_->shard_count(); ++s) {
    if (auto t = queue_->TailSeq(s); t.has_value()) ready_watermarks[s] = *t;
  }

  hot_eviction_worker_->Start();

  resolver_pool_->Start();
  hot_pool_->Start();
  cold_pool_->Start();

  auto identity = resp::NodeIdentity::Open(config_.queue.wal_path);
  if (!identity.has_value()) {
    ABYSS_LOG_CRITICAL("node identity load failed",
                       {"err", std::string_view{identity.error().message()}});
    return false;
  }
  node_identity_ = std::make_unique<resp::NodeIdentity>(std::move(*identity));

  stats_ = std::make_unique<ServerStatsImpl>(
      *queue_, *hot_store_, cold_store_.get(), std::string{kVersion}, config_.net.bind,
      /*advertise_address=*/std::string{}, /*mode=*/"standalone", config_.net.port);
  config_provider_ = std::make_unique<ConfigProviderImpl>(config_);
  loading_ =
      std::make_unique<LoadingStateImpl>([queue_ptr = queue_.get(), pool_ptr = hot_pool_.get(),
                                          watermarks = std::move(ready_watermarks)] {
        if (queue_ptr != nullptr && queue_ptr->IsRecovering()) return true;
        if (pool_ptr == nullptr) return false;
        for (uint32_t s = 0; s < watermarks.size(); ++s) {
          if (pool_ptr->ConsumerFor(s).HighestSettledSeq() < watermarks[s]) return true;
        }
        return false;
      });
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

  ready_.store(true, std::memory_order_release);
  ABYSS_LOG_INFO("server ready", {"shard_count", static_cast<int64_t>(hot_store_->shard_count())});
  return true;
#endif
}

void Server::Run(const std::atomic<bool>& stop) {
  if (!tcp_server_) return;
  if (auto r = tcp_server_->Start(); !r.has_value()) {
    ABYSS_LOG_CRITICAL("tcp server start failed", {"err", std::string_view{r.error().message()}});
    return;
  }

  config_.net.port = tcp_server_->BoundPort();
  if (stats_) stats_->SetTcpPort(config_.net.port);

  NotifyReady();
  ABYSS_LOG_INFO("listening", {"version", std::string_view{kVersion}},
                 {"bind", std::string_view{config_.net.bind}},
                 {"port", static_cast<int64_t>(config_.net.port)});

  while (!stop.load(std::memory_order_acquire) && tcp_server_->IsRunning()) {
    std::this_thread::sleep_for(kStopPollInterval);
  }

  Shutdown();
}

void Server::NotifyReady() {
  if (ready_fd_ < 0) return;

  std::string line = R"({"bind":")";
  line += config_.net.bind;
  line += R"(","port":)";
  line += std::to_string(config_.net.port);
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
  if (shutting_down_.exchange(true, std::memory_order_acq_rel)) return;

  ABYSS_LOG_INFO("shutdown starting");

  if (tcp_server_) {
    tcp_server_->Stop();
  }

  if (cold_pool_) cold_pool_->Stop();
  if (hot_pool_) hot_pool_->Stop();
  if (resolver_pool_) resolver_pool_->Stop();
  if (hot_eviction_worker_) hot_eviction_worker_->Stop();

  ready_.store(false, std::memory_order_release);
  ABYSS_LOG_INFO("shutdown complete");
}

}  // namespace abyss::server
