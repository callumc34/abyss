#include <CLI/CLI.hpp>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <optional>
#include <string>

#include "abyss/config/config.h"
#include "abyss/log/log.h"
#include "abyss/version.h"
#include "server.h"

namespace {

std::atomic<bool> g_shutdown_requested{false};
std::atomic<int> g_shutdown_signal{0};

extern "C" void ShutdownHandler(int sig) {
  g_shutdown_signal.store(sig, std::memory_order_release);
  g_shutdown_requested.store(true, std::memory_order_release);
}

std::string ResolveConfigPath(const std::string& cli_path) {
  if (!cli_path.empty()) return cli_path;
  const char* env = nullptr;
  env = std::getenv("ABYSS_CONFIG_PATH");
  if (env != nullptr && *env != '\0') {
    return env;
  }
  return {};
}

}  // namespace

// NOLINTNEXTLINE(modernize-avoid-c-arrays,bugprone-exception-escape)
int main(int argc, char* argv[]) {
  // Local-dev sandbox: used only when neither --config nor --data-dir is given.
  constexpr auto kDefaultDataDir = "/tmp/abyss";

  CLI::App app{"abyss — Redis-compatible hot-cold tiered KV store"};
  app.set_version_flag("--version", abyss::kVersion);

  std::string config_path;
  std::string data_dir = kDefaultDataDir;
  std::optional<uint16_t> port_override;
  std::optional<uint32_t> shard_count_override;
  intptr_t ready_fd = -1;

  app.add_option("-c,--config", config_path, "Path to YAML config file (ABYSS_CONFIG_PATH)");
  app.add_option("-p,--port", port_override,
                 "RESP listen port (overrides config). Pass 0 to let the OS pick an ephemeral "
                 "port; the bound port is emitted via --ready-fd when set.");
  app.add_option("-d,--data-dir", data_dir,
                 "Data directory for WAL and cold store (used when no config file is given)")
      ->default_val(data_dir);
  app.add_option("--shard-count", shard_count_override,
                 "Hot-store shard count (overrides config). The queue and consumer pools are "
                 "opened with the same count.");
  app.add_option("--ready-fd", ready_fd,
                 "Readiness pipe. POSIX fd, or Windows HANDLE cast to intptr_t. Once the "
                 "listener is bound, one JSON line ({\"bind\":\"...\",\"port\":N}) is written "
                 "and the handle is closed.");

  CLI11_PARSE(app, argc, argv);

  const auto bootstrap_logger = abyss::log::Get("abyss.server.bootstrap");

  abyss::config::Config config;
  const std::string resolved = ResolveConfigPath(config_path);
  if (!resolved.empty()) {
    auto loaded = abyss::config::Config::LoadFromFile(resolved);
    if (!loaded.has_value()) {
      ABYSS_LOG_CRITICAL(bootstrap_logger, "config load failed",
                         {"path", std::string_view{resolved}},
                         {"err", std::string_view{loaded.error().message()}});
      return EXIT_FAILURE;
    }
    config = std::move(*loaded);
  } else {
    config = abyss::config::Config::Defaults();
    config.queue.wal_path = data_dir + "/wal";
    config.cold.data_path = data_dir + "/cold";
    config.ApplyEnvironmentOverrides();
    if (auto r = config.Validate(); !r.has_value()) {
      ABYSS_LOG_CRITICAL(bootstrap_logger, "config invalid",
                         {"err", std::string_view{r.error().message()}});
      return EXIT_FAILURE;
    }
  }

  if (port_override.has_value()) config.net.port = *port_override;
  if (shard_count_override.has_value()) config.hot.shard_count = *shard_count_override;

  if (auto r = config.Validate(); !r.has_value()) {
    ABYSS_LOG_CRITICAL(bootstrap_logger, "config invalid after overrides",
                       {"err", std::string_view{r.error().message()}});
    return EXIT_FAILURE;
  }

  abyss::log::Init(config.log);

  ABYSS_LOG_INFO(bootstrap_logger, "abyss starting", {"version", std::string_view{abyss::kVersion}},
                 {"profile", std::string_view{config.profile}},
                 {"bind", std::string_view{config.net.bind}},
                 {"port", static_cast<int64_t>(config.net.port)},
                 {"config_path",
                  resolved.empty() ? std::string_view{"<defaults>"} : std::string_view{resolved}});

#ifndef _WIN32
  struct sigaction sa{};
  sa.sa_handler = ShutdownHandler;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGTERM, &sa, nullptr);
  sigaction(SIGINT, &sa, nullptr);
  signal(SIGPIPE, SIG_IGN);  // NOLINT(cert-err33-c)
#else
  signal(SIGINT, ShutdownHandler);
  signal(SIGTERM, ShutdownHandler);
#endif

  abyss::server::Server server(config);
  server.set_ready_fd(ready_fd);
  if (!server.Initialize()) {
    ABYSS_LOG_CRITICAL(bootstrap_logger, "server initialize failed");
    return EXIT_FAILURE;
  }

  server.Run(g_shutdown_requested);

  ABYSS_LOG_INFO(bootstrap_logger, "abyss stopped",
                 {"signal", static_cast<int64_t>(g_shutdown_signal.load())});
  return EXIT_SUCCESS;
}
