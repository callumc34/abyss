#include <hiredis/hiredis.h>

#include <CLI/CLI.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/result.h"
#include "metrics_scraper.h"
#include "reporter.h"
#include "run_loop.h"
#include "workload.h"

namespace {

constexpr std::string_view kCmdGet = "GET";
constexpr std::string_view kCmdSet = "SET";

std::string KeyFor(uint64_t idx) {
  std::array<char, 32> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "k%020llu", static_cast<unsigned long long>(idx));
  return buf.data();
}

struct Endpoint {
  std::string host;
  int port = 0;
};

abyss::core::Result<Endpoint> ParseEndpoint(const std::string& s) {
  const auto colon = s.rfind(':');
  if (colon == std::string::npos) {
    return std::unexpected(
        abyss::core::Error(abyss::core::ErrorCode::kInvalidArgument, "endpoint must be host:port"));
  }
  Endpoint e;
  e.host = s.substr(0, colon);
  try {
    e.port = std::stoi(s.substr(colon + 1));
    // NOLINTNEXTLINE(bugprone-empty-catch): std::stoi communicates errors via exceptions.
  } catch (...) {
    return std::unexpected(
        abyss::core::Error(abyss::core::ErrorCode::kInvalidArgument, "bad port in endpoint"));
  }
  return e;
}

redisContext* ConnectOne(const Endpoint& ep, std::chrono::milliseconds timeout) {
  timeval tv{
      .tv_sec = timeout.count() / 1000,
      .tv_usec = static_cast<int>((timeout.count() % 1000) * 1000),
  };
  redisContext* ctx = redisConnectWithTimeout(ep.host.c_str(), ep.port, tv);
  if (ctx == nullptr) return nullptr;
  if (ctx->err != 0) {
    redisFree(ctx);
    return nullptr;
  }
  redisSetTimeout(ctx, tv);
  return ctx;
}

bool PreloadKeys(redisContext* ctx, uint64_t key_count, const std::string& value) {
  for (uint64_t i = 0; i < key_count; ++i) {
    const auto key = KeyFor(i);
    auto* reply = static_cast<redisReply*>(
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
        redisCommand(ctx, "SET %s %b", key.c_str(), value.data(), value.size()));
    if (reply == nullptr) {
      std::cerr << "preload SET " << i << ": null reply (" << ctx->errstr << ")\n";
      return false;
    }
    const bool ok = (reply->type == REDIS_REPLY_STATUS);
    freeReplyObject(reply);
    if (!ok) {
      std::cerr << "preload SET " << i << ": non-OK reply\n";
      return false;
    }
  }
  return true;
}

abyss::perf::MetricSnapshot Scrape(abyss::perf::MetricsScraper& scraper, const std::string& phase) {
  abyss::perf::MetricSnapshot snap;
  snap.phase = phase;
  auto rc = scraper.Snapshot();
  if (rc.has_value()) snap.metrics = std::move(*rc);
  return snap;
}

}  // namespace

int main(int argc, char** argv) {
  CLI::App app{"Abyss TCP load generator (ADP-013)"};

  std::string workload_path;
  std::string endpoint_str = "127.0.0.1:6379";
  std::string metrics_url = "http://127.0.0.1:9090";
  std::string output_path;
  std::string hgrm_dir;
  bool gate = false;
  std::string run_id;
  bool skip_preload = false;

  app.add_option("--workload", workload_path, "Workload YAML path")->required();
  app.add_option("--server", endpoint_str, "Redis endpoint host:port");
  app.add_option("--metrics-url", metrics_url,
                 "Server metrics endpoint (empty to disable scraping)");
  app.add_option("--output", output_path, "JSON report path (omit for stdout)");
  app.add_option("--hgrm-dir", hgrm_dir, "Per-op .hgrm output directory");
  app.add_flag("--gate", gate, "Exit non-zero if any target fails");
  app.add_option("--run-id", run_id, "Run identifier override");
  app.add_flag("--skip-preload", skip_preload, "Skip workload preload phase");

  CLI11_PARSE(app, argc, argv);

  auto workload_or = abyss::perf::LoadWorkloadFile(workload_path);
  if (!workload_or.has_value()) {
    std::cerr << "workload load failed: " << workload_or.error().message() << '\n';
    return 2;
  }
  auto workload = std::move(*workload_or);

  auto ep_or = ParseEndpoint(endpoint_str);
  if (!ep_or.has_value()) {
    std::cerr << "bad endpoint: " << ep_or.error().message() << '\n';
    return 2;
  }
  const auto& endpoint = *ep_or;

  const int total_connections = workload.workers * workload.connections_per_worker;
  if (total_connections <= 0) {
    std::cerr << "workload must specify positive workers and connections_per_worker\n";
    return 2;
  }

  std::vector<redisContext*> connections(static_cast<size_t>(total_connections), nullptr);
  for (int i = 0; i < total_connections; ++i) {
    auto* ctx = ConnectOne(endpoint, std::chrono::seconds{5});
    if (ctx == nullptr) {
      std::cerr << "connection " << i << " failed\n";
      for (auto* c : connections) {
        if (c != nullptr) redisFree(c);
      }
      return 1;
    }
    connections[i] = ctx;
  }

  const std::string value_str(workload.value_size_bytes, 'x');

  if (workload.preload.enabled && !skip_preload) {
    const auto preload_key_count =
        workload.preload.key_count > 0 ? workload.preload.key_count : workload.key_count;
    std::cerr << "loadgen: preloading " << preload_key_count << " keys\n";
    if (!PreloadKeys(connections[0], preload_key_count, value_str)) {
      std::cerr << "preload failed; aborting\n";
      for (auto* c : connections) redisFree(c);
      return 1;
    }
  }

  abyss::perf::MetricsScraper scraper{metrics_url};
  std::vector<abyss::perf::MetricSnapshot> server_snapshots;
  if (!metrics_url.empty()) {
    server_snapshots.push_back(Scrape(scraper, "start"));
  }

  abyss::perf::RunLoopConfig cfg;
  cfg.workers = total_connections;
  cfg.duration = workload.duration;
  cfg.warmup = workload.warmup;
  cfg.target_rate_ops_per_worker =
      workload.target_rate_ops / static_cast<uint64_t>(total_connections);
  cfg.key_count = workload.key_count;
  cfg.value_size_bytes = workload.value_size_bytes;
  cfg.key_distribution = workload.key_distribution;
  cfg.mix = workload.mix;

  std::atomic<uint64_t> error_count{0};

  abyss::perf::OpFn op = [&](int worker_id, std::string_view op_name, uint64_t key_index) {
    auto* ctx = connections[static_cast<size_t>(worker_id)];
    const auto key = KeyFor(key_index);
    redisReply* reply = nullptr;
    if (op_name == kCmdGet) {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
      reply = static_cast<redisReply*>(redisCommand(ctx, "GET %s", key.c_str()));
    } else if (op_name == kCmdSet) {
      reply = static_cast<redisReply*>(
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
          redisCommand(ctx, "SET %s %b", key.c_str(), value_str.data(), value_str.size()));
    } else {
      // Future op types land here; v1 supports GET/SET only.
      error_count.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    if (reply == nullptr) {
      error_count.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
      error_count.fetch_add(1, std::memory_order_relaxed);
    }
    freeReplyObject(reply);
  };

  std::atomic<bool> mid_scrape_done{false};
  std::thread mid_scrape;
  if (!metrics_url.empty()) {
    mid_scrape = std::thread([&]() {
      const auto wait_for = workload.warmup + (workload.duration / 2);
      std::this_thread::sleep_for(wait_for);
      auto snap = Scrape(scraper, "mid");
      server_snapshots.push_back(std::move(snap));
      mid_scrape_done.store(true, std::memory_order_release);
    });
  }

  const auto result = abyss::perf::RunLoop(cfg, op);

  if (mid_scrape.joinable()) mid_scrape.join();
  if (!metrics_url.empty()) {
    server_snapshots.push_back(Scrape(scraper, "end"));
  }

  for (auto* ctx : connections) {
    redisFree(ctx);
  }

  abyss::perf::RunReport report;
  report.run_id = run_id.empty()
                      ? std::to_string(std::chrono::system_clock::now().time_since_epoch().count())
                      : run_id;
  report.started_at = std::chrono::system_clock::now() - workload.duration;
  report.duration = workload.duration;
  report.build = abyss::perf::CurrentBuildInfo();
  report.host = abyss::perf::CurrentHostInfo();
  report.classification = abyss::perf::DetectClassification();
  report.workload = workload;
  for (const auto& [name, hist] : result.per_op_histograms) {
    const auto count = result.per_op_counts.at(name);
    report.operations[name] =
        abyss::perf::StatsFromHistogram(hist, count, result.measured_duration);
  }
  report.server_metrics = std::move(server_snapshots);
  abyss::perf::EvaluateTargets(report);

  if (output_path.empty()) {
    abyss::perf::WriteReportJson(report, std::cout);
    std::cout << '\n';
  } else {
    std::ofstream out{output_path};
    if (!out.is_open()) {
      std::cerr << "failed to open output: " << output_path << '\n';
      return 1;
    }
    abyss::perf::WriteReportJson(report, out);
  }

  if (!hgrm_dir.empty()) {
    std::error_code ec;
    std::filesystem::create_directories(hgrm_dir, ec);
    if (ec) {
      std::cerr << "failed to create hgrm dir: " << ec.message() << '\n';
      return 1;
    }
    for (const auto& [name, hist] : result.per_op_histograms) {
      const auto path = std::filesystem::path{hgrm_dir} / (name + ".hgrm");
      std::ofstream out{path};
      if (!out.is_open()) {
        std::cerr << "failed to open hgrm: " << path << '\n';
        return 1;
      }
      hist.WriteHgrmTo(out);
    }
  }

  if (error_count.load() > 0) {
    std::cerr << "loadgen: " << error_count.load() << " op errors during run\n";
  }
  if (gate && !report.pass) {
    std::cerr << "loadgen: one or more targets failed\n";
    return 3;
  }
  return 0;
}
