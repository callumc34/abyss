#include <csignal>
#include <cstdlib>
#include <string>

#include <CLI/CLI.hpp>

#include "abyss/config/config.h"
#include "abyss/version.h"
#include "server.h"

namespace {

std::atomic<bool> g_shutdown_requested{false};

extern "C" void ShutdownHandler(int /*sig*/) {
  g_shutdown_requested.store(true, std::memory_order_release);
}

}  // namespace

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
int main(int argc, char* argv[]) {
  constexpr auto kDefaultDataDir = "/tmp/abyss";

  CLI::App app{"abyss — Redis-compatible hot-cold tiered KV store"};
  app.set_version_flag("--version", abyss::kVersion);

  abyss::config::Config config = abyss::config::Config::Defaults();
  std::string data_dir = kDefaultDataDir;

  app.add_option("-p,--port", config.resp.port, "RESP listen port")
      ->default_val(config.resp.port);
  app.add_option("-d,--data-dir", data_dir, "Data directory for WAL and cold store")
      ->default_val(data_dir);

  CLI11_PARSE(app, argc, argv);

  config.queue.wal_path = data_dir + "/wal";
  config.cold.data_path = data_dir + "/cold";

  struct sigaction sa{};
  sa.sa_handler = ShutdownHandler;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGTERM, &sa, nullptr);
  sigaction(SIGINT, &sa, nullptr);
  signal(SIGPIPE, SIG_IGN);  // NOLINT(cert-err33-c)

  abyss::server::Server server(config);
  if (!server.Initialize()) {
    return EXIT_FAILURE;
  }

  server.Run(g_shutdown_requested);
  return EXIT_SUCCESS;
}
