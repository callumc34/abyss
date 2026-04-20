#include <CLI/CLI.hpp>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>

#include "abyss/config/config.h"
#include "abyss/version.h"
#include "server.h"

namespace {

std::atomic<bool> g_shutdown_requested{false};

extern "C" void ShutdownHandler(int /*sig*/) {
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
  // TODO(Callum): Set default in abyss::config
  constexpr auto kDefaultDataDir = "/tmp/abyss";

  CLI::App app{"abyss — Redis-compatible hot-cold tiered KV store"};
  app.set_version_flag("--version", abyss::kVersion);

  std::string config_path;
  std::string data_dir = kDefaultDataDir;
  std::optional<uint16_t> port_override;
  std::optional<uint32_t> shard_count_override;

  app.add_option("-c,--config", config_path, "Path to YAML config file (ABYSS_CONFIG_PATH)");
  app.add_option("-p,--port", port_override,
                 "RESP listen port (overrides config). Pass 0 to let the OS pick an ephemeral "
                 "port; the bound port is announced on the stdout ready line.");
  app.add_option("-d,--data-dir", data_dir,
                 "Data directory for WAL and cold store (used when no config file is given)")
      ->default_val(data_dir);
  app.add_option("--shard-count", shard_count_override,
                 "Hot-store shard count (overrides config). The queue and consumer pools are "
                 "opened with the same count.");

  CLI11_PARSE(app, argc, argv);

  abyss::config::Config config;
  const std::string resolved = ResolveConfigPath(config_path);
  if (!resolved.empty()) {
    auto loaded = abyss::config::Config::LoadFromFile(resolved);
    if (!loaded.has_value()) {
      std::cerr << "config error: " << loaded.error().message() << "\n";
      return EXIT_FAILURE;
    }
    config = std::move(*loaded);
  } else {
    config = abyss::config::Config::Defaults();
    config.queue.wal_path = data_dir + "/wal";
    config.cold.data_path = data_dir + "/cold";
    config.ApplyEnvironmentOverrides();
    if (auto r = config.Validate(); !r.has_value()) {
      std::cerr << "config error: " << r.error().message() << "\n";
      return EXIT_FAILURE;
    }
  }

  if (port_override.has_value()) config.resp.port = *port_override;
  if (shard_count_override.has_value()) config.hot.shard_count = *shard_count_override;

  if (auto r = config.Validate(); !r.has_value()) {
    std::cerr << "config error: " << r.error().message() << "\n";
    return EXIT_FAILURE;
  }

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
  if (!server.Initialize()) {
    return EXIT_FAILURE;
  }

  server.Run(g_shutdown_requested);
  return EXIT_SUCCESS;
}
