#include <hiredis/hiredis.h>
#include <sys/select.h>

#include <CLI/CLI.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/result.h"
#include "metrics_scraper.h"
#include "reporter.h"
#include "run_loop.h"
#include "server_identity.h"
#include "sweep.h"
#include "workload.h"

namespace {

constexpr std::string_view kCmdGet = "GET";
constexpr std::string_view kCmdSet = "SET";
// No key and no engine work: the front end's own ceiling.
constexpr std::string_view kCmdPing = "PING";

enum class Op : uint8_t { kGet, kSet, kPing };

Op OpOf(std::string_view name) {
  if (name == kCmdSet) return Op::kSet;
  if (name == kCmdGet) return Op::kGet;
  return Op::kPing;
}

// Every value loadgen writes is `value`, so a GET answers with it or
// with nil.
bool Answers(Op op, const redisReply& reply, std::string_view value) {
  const std::string_view str =
      reply.str != nullptr ? std::string_view{reply.str, reply.len} : std::string_view{};
  switch (op) {
    case Op::kSet:
      return reply.type == REDIS_REPLY_STATUS && str == "OK";
    case Op::kPing:
      return reply.type == REDIS_REPLY_STATUS && str == "PONG";
    case Op::kGet:
      return reply.type == REDIS_REPLY_NIL || (reply.type == REDIS_REPLY_STRING && str == value);
  }
  return false;
}

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
  // AwaitReply's select() indexes an fd_set by descriptor.
  if (ctx->err != 0 || ctx->fd >= FD_SETSIZE) {
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

// A failed scrape records no snapshot rather than an empty one.
std::optional<abyss::perf::MetricSnapshot> Scrape(abyss::perf::MetricsScraper& scraper,
                                                  const std::string& phase) {
  auto rc = scraper.Snapshot();
  if (!rc.has_value()) {
    std::cerr << "loadgen: " << phase << " metrics scrape failed: " << rc.error().message() << '\n';
    return std::nullopt;
  }
  return abyss::perf::MetricSnapshot{.phase = phase, .metrics = std::move(*rc)};
}

std::optional<abyss::perf::HelloFields> QueryHello(redisContext* ctx) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  auto* reply = static_cast<redisReply*>(redisCommand(ctx, "HELLO 2"));
  if (reply == nullptr) return std::nullopt;
  std::optional<abyss::perf::HelloFields> hello;
  if (reply->type == REDIS_REPLY_ARRAY || reply->type == REDIS_REPLY_MAP) {
    hello.emplace();
    const std::span<redisReply*> fields{reply->element, reply->elements};
    for (size_t i = 0; i + 1 < fields.size(); i += 2) {
      const redisReply* key = fields[i];
      const redisReply* value = fields[i + 1];
      if (key->type != REDIS_REPLY_STRING || value->type != REDIS_REPLY_STRING) continue;
      const std::string_view name{key->str, key->len};
      if (name == "server") hello->server.assign(value->str, value->len);
      if (name == "version") hello->version.assign(value->str, value->len);
    }
  }
  freeReplyObject(reply);
  return hello;
}

std::string QueryInfoServer(redisContext* ctx) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  auto* reply = static_cast<redisReply*>(redisCommand(ctx, "INFO server"));
  if (reply == nullptr) return {};
  std::string info;
  if (reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_VERB) {
    info.assign(reply->str, reply->len);
  }
  freeReplyObject(reply);
  return info;
}

// Best effort: keys a server does not expose are left out.
std::map<std::string, std::string> QueryServerConfig(redisContext* ctx, std::string_view kind) {
  std::vector<std::string> patterns{"appendonly", "appendfsync", "io-threads"};
  if (kind == "abyss") patterns = {"wal-durability", "shard-count", "wal-segment-size-bytes"};
  if (kind == "dragonfly") patterns = {"*"};
  std::map<std::string, std::string> config;
  for (const auto& pattern : patterns) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    auto* reply = static_cast<redisReply*>(redisCommand(ctx, "CONFIG GET %s", pattern.c_str()));
    if (reply == nullptr) break;
    if (reply->type == REDIS_REPLY_ARRAY || reply->type == REDIS_REPLY_MAP) {
      const std::span<redisReply*> fields{reply->element, reply->elements};
      for (size_t i = 0; i + 1 < fields.size(); i += 2) {
        const redisReply* key = fields[i];
        const redisReply* value = fields[i + 1];
        if (key->type != REDIS_REPLY_STRING || value->type != REDIS_REPLY_STRING) continue;
        config[std::string{key->str, key->len}] = std::string{value->str, value->len};
      }
    }
    freeReplyObject(reply);
  }
  return config;
}

abyss::perf::ServerIdentity IdentifyServer(redisContext* ctx) {
  const auto hello = QueryHello(ctx);
  const std::string info = abyss::perf::NeedsInfo(hello) ? QueryInfoServer(ctx) : std::string{};
  return abyss::perf::ResolveServerIdentity(hello, info);
}

// One hiredis connection driven through append/flush/read so several
// requests can be in flight. Each reply is checked against the request
// it answers, oldest first, so a reply out of order fails as an error.
class HiredisConnection final : public abyss::perf::PipelinedConnection {
 public:
  using Clock = std::chrono::steady_clock;

  HiredisConnection(redisContext* ctx, const std::string& value, std::atomic<uint64_t>& failures)
      : ctx_(ctx), value_(value), failures_(failures) {}

  // The mix holds only GET, SET and PING; the caller rejects the rest.
  bool Send(std::string_view op_name, uint64_t key_index) override {
    const Op op = OpOf(op_name);
    const auto key = KeyFor(key_index);
    std::array<const char*, 3> argv{op == Op::kSet ? "SET" : "GET", key.c_str(), value_.c_str()};
    std::array<size_t, 3> lens{3, key.size(), value_.size()};
    int args = op == Op::kSet ? 3 : 2;
    if (op == Op::kPing) {
      argv[0] = "PING";
      lens[0] = kCmdPing.size();
      args = 1;
    }
    if (redisAppendCommandArgv(ctx_, args, argv.data(), lens.data()) != REDIS_OK) return Fail();
    sent_.push_back(op);
    return true;
  }

  uint64_t answered() const { return answered_.load(std::memory_order_relaxed); }
  uint64_t refused() const { return refused_.load(std::memory_order_relaxed); }
  uint64_t mismatched() const { return mismatched_.load(std::memory_order_relaxed); }

  bool Flush() override {
    int done = 0;
    while (done == 0) {
      if (redisBufferWrite(ctx_, &done) != REDIS_OK) return Fail();
    }
    return true;
  }

  Await AwaitReply(Clock::time_point deadline) override {
    void* reply = nullptr;
    if (redisGetReplyFromReader(ctx_, &reply) != REDIS_OK) return Failed();
    while (reply == nullptr) {
      if (deadline != Clock::time_point::max() && !WaitReadable(deadline)) {
        return Await::kTimeout;
      }
      if (redisBufferRead(ctx_) != REDIS_OK || redisGetReplyFromReader(ctx_, &reply) != REDIS_OK) {
        return Failed();
      }
    }
    auto* parsed = static_cast<redisReply*>(reply);
    const Op op = sent_.front();
    sent_.pop_front();
    const bool error = parsed->type == REDIS_REPLY_ERROR;
    const bool answers = error || Answers(op, *parsed, value_);
    if (!answers && mismatched_.fetch_add(1, std::memory_order_relaxed) == 0) {
      std::cerr << "loadgen: a reply does not answer its request (reply type " << parsed->type
                << ")\n";
    }
    freeReplyObject(parsed);
    if (error || !answers) {
      refused_.fetch_add(1, std::memory_order_relaxed);
      return Await::kError;
    }
    answered_.fetch_add(1, std::memory_order_relaxed);
    return Await::kReply;
  }

 private:
  // Microsecond timeout: the deadline is the next scheduled send. A
  // passed deadline still polls once.
  bool WaitReadable(Clock::time_point deadline) const {
    const auto remaining = std::max(deadline - Clock::now(), Clock::duration::zero());
    const auto us = std::chrono::ceil<std::chrono::microseconds>(remaining).count();
    timeval tv{
        .tv_sec = static_cast<time_t>(us / 1'000'000),
        .tv_usec = static_cast<suseconds_t>(us % 1'000'000),
    };
    fd_set readable;
    FD_ZERO(&readable);
    FD_SET(ctx_->fd, &readable);
    return select(ctx_->fd + 1, &readable, nullptr, nullptr, &tv) > 0;
  }

  bool Fail() {
    std::cerr << "loadgen: connection failed: " << ctx_->errstr << '\n';
    failures_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  Await Failed() {
    Fail();
    return Await::kFailed;
  }

  redisContext* ctx_;
  const std::string& value_;
  std::atomic<uint64_t>& failures_;
  std::deque<Op> sent_;
  // Written by the connection's worker, read by the report.
  std::atomic<uint64_t> answered_{0};
  std::atomic<uint64_t> refused_{0};
  std::atomic<uint64_t> mismatched_{0};
};

constexpr int kExitOpErrors = 4;

}  // namespace

// NOLINTNEXTLINE(bugprone-exception-escape)
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
  bool sweep = false;
  double sweep_p99_bound_us = 0.0;
  int64_t sweep_step_seconds = 0;
  std::string per_connection_path;

  app.add_option("--workload", workload_path, "Workload YAML path")->required();
  app.add_option("--server", endpoint_str, "Redis endpoint host:port");
  app.add_option("--metrics-url", metrics_url,
                 "Server metrics endpoint (empty to disable scraping)");
  app.add_option("--output", output_path, "JSON report path (omit for stdout)");
  app.add_option("--hgrm-dir", hgrm_dir, "Per-op .hgrm output directory");
  app.add_option("--per-connection-out", per_connection_path,
                 "CSV of each connection's answered and refused requests in the measured window");
  app.add_flag("--gate", gate, "Exit non-zero if any target fails");
  app.add_option("--run-id", run_id, "Run identifier override");
  app.add_flag("--skip-preload", skip_preload, "Skip workload preload phase");
  app.add_flag("--sweep", sweep,
               "Find the highest open-loop rate whose SET p99 holds --sweep-p99-bound-us");
  app.add_option("--sweep-p99-bound-us", sweep_p99_bound_us,
                 "SET p99 bound for --sweep, e.g. the W2 bound from the write probe");
  app.add_option("--sweep-step-seconds", sweep_step_seconds,
                 "Measured seconds per sweep step (default: workload duration)");

  CLI11_PARSE(app, argc, argv);

  auto workload_or = abyss::perf::LoadWorkloadFile(workload_path);
  if (!workload_or.has_value()) {
    std::cerr << "workload load failed: " << workload_or.error().message() << '\n';
    return 2;
  }
  auto workload = std::move(*workload_or);
  for (const auto& [name, weight] : workload.mix.weights) {
    if (name != kCmdGet && name != kCmdSet && name != kCmdPing) {
      std::cerr << "unsupported op in mix: " << name << " (GET, SET and PING only)\n";
      return 2;
    }
  }

  if (sweep) {
    if (sweep_p99_bound_us <= 0.0 || workload.target_rate_ops == 0 ||
        !workload.mix.weights.contains(std::string{kCmdSet})) {
      std::cerr << "--sweep needs --sweep-p99-bound-us, an open-loop start rate "
                   "(target_rate_ops > 0) and SET in the mix\n";
      return 2;
    }
    if (sweep_step_seconds > 0) workload.duration = std::chrono::seconds{sweep_step_seconds};
  }

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

  const auto server = IdentifyServer(connections[0]);
  const auto server_config = QueryServerConfig(connections[0], server.kind);
  std::cerr << "loadgen: measuring " << server.kind << ' ' << server.version << '\n';

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
    if (auto snap = Scrape(scraper, "start")) server_snapshots.push_back(std::move(*snap));
  }

  abyss::perf::RunLoopConfig cfg;
  cfg.workers = total_connections;
  cfg.duration = workload.duration;
  cfg.warmup = workload.warmup;
  cfg.key_count = workload.key_count;
  cfg.value_size_bytes = workload.value_size_bytes;
  cfg.key_distribution = workload.key_distribution;
  cfg.mix = workload.mix;

  std::atomic<uint64_t> connection_failures{0};
  std::vector<std::unique_ptr<HiredisConnection>> transports;
  std::vector<abyss::perf::PipelinedConnection*> pipelined;
  transports.reserve(connections.size());
  pipelined.reserve(connections.size());
  for (auto* ctx : connections) {
    transports.push_back(std::make_unique<HiredisConnection>(ctx, value_str, connection_failures));
    pipelined.push_back(transports.back().get());
  }
  // Each connection's counts when the measured window opened.
  std::vector<std::pair<uint64_t, uint64_t>> at_start(transports.size());
  cfg.at_measure_start = [&] {
    for (size_t i = 0; i < transports.size(); ++i) {
      at_start[i] = {transports[i]->answered(), transports[i]->refused()};
    }
  };
  const auto run_at = [&](uint64_t rate_ops) {
    cfg.target_rate_ops = rate_ops;
    return abyss::perf::RunPipelinedLoop(cfg, workload.pipeline_depth, workload.arrival, pipelined);
  };

  abyss::perf::RunLoopResult result;
  std::optional<abyss::perf::RateSweep> rate_sweep;
  // Steps share the server: WAL length, hot set and buffers carry over.
  std::vector<std::optional<abyss::perf::MetricSnapshot>> step_positions;
  if (sweep) {
    // Below one request/s per connection the offered rate would round to
    // closed loop.
    rate_sweep.emplace(workload.target_rate_ops, static_cast<int64_t>(sweep_p99_bound_us * 1000.0),
                       static_cast<uint64_t>(total_connections));
    // The report carries the highest passing step, else the last one run.
    while (auto rate = rate_sweep->Next()) {
      auto step = run_at(*rate);
      const auto& judged =
          rate_sweep->Record(abyss::perf::AchievedOps(step),
                             step.per_op_histograms.at(std::string{kCmdSet}).PercentileNs(99.0),
                             abyss::perf::TotalErrors(step));
      std::cerr << "loadgen: sweep step " << judged.offered_ops << " ops/s "
                << (judged.pass ? "held" : "failed") << '\n';
      step_positions.push_back(metrics_url.empty() ? std::nullopt : Scrape(scraper, "step"));
      if (judged.pass || !rate_sweep->Result().has_value()) {
        result = std::move(step);
        workload.target_rate_ops = judged.offered_ops;
      }
      if (connection_failures.load() > 0) break;
    }
  } else {
    std::thread mid_scrape;
    if (!metrics_url.empty()) {
      mid_scrape = std::thread([&]() {
        const auto wait_for = workload.warmup + (workload.duration / 2);
        std::this_thread::sleep_for(wait_for);
        if (auto snap = Scrape(scraper, "mid")) server_snapshots.push_back(std::move(*snap));
      });
    }
    result = run_at(workload.target_rate_ops);
    if (mid_scrape.joinable()) mid_scrape.join();
  }

  if (!metrics_url.empty()) {
    if (auto snap = Scrape(scraper, "end")) server_snapshots.push_back(std::move(*snap));
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
  report.server = server;
  report.config = server_config;
  report.workload = workload;
  for (const auto& [name, hist] : result.per_op_histograms) {
    const auto count = result.per_op_counts.at(name);
    report.operations[name] =
        abyss::perf::StatsFromHistogram(hist, count, result.measured_duration);
    report.operations[name].errors = result.per_op_errors.at(name);
  }
  report.driver =
      abyss::perf::MakeDriverStats(result.send_lag, result.backlog, result.open_loop,
                                   result.driver_cpu, result.measured_duration, workload.targets);
  report.server_metrics = std::move(server_snapshots);
  if (rate_sweep.has_value()) {
    report.sweep = rate_sweep->Steps();
    report.sweep_result_ops = rate_sweep->Result();
    for (auto& step : report.sweep) {
      step.effective_offered_ops = 0;
      for (size_t w = 0; w < connections.size(); ++w) {
        step.effective_offered_ops +=
            abyss::perf::WorkerRate(step.offered_ops, connections.size(), w);
      }
    }
    for (size_t i = 0; i < report.sweep.size() && i < step_positions.size(); ++i) {
      const auto& position = step_positions[i];
      if (!position.has_value()) continue;
      const auto& metrics = position->metrics;
      if (const auto it = metrics.find("abyss_queue_depth"); it != metrics.end()) {
        report.sweep[i].queue_entries = it->second;
      }
      if (const auto it = metrics.find("abyss_queue_disk_bytes"); it != metrics.end()) {
        report.sweep[i].queue_bytes = it->second;
      }
    }
  }
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
  abyss::perf::WriteSummary(report, std::cerr);

  uint64_t answered = 0;
  uint64_t mismatched = 0;
  for (const auto& transport : transports) {
    answered += transport->answered();
    mismatched += transport->mismatched();
  }
  std::cerr << "loadgen: " << answered << " replies answered their requests in order, "
            << mismatched << " did not\n";
  if (!per_connection_path.empty()) {
    std::ofstream out{per_connection_path};
    if (!out.is_open()) {
      std::cerr << "failed to open per-connection output: " << per_connection_path << '\n';
      return 1;
    }
    out << "connection,answered,refused\n";
    for (size_t i = 0; i < transports.size(); ++i) {
      out << i << ',' << transports[i]->answered() - at_start[i].first << ','
          << transports[i]->refused() - at_start[i].second << '\n';
    }
  }

  // A sweep fails a step on error replies; a broken connection fails
  // the run.
  const uint64_t errors =
      connection_failures.load() + (sweep ? 0 : abyss::perf::TotalErrors(result));
  if (errors > 0) {
    std::cerr << "loadgen: " << errors << " errors during run\n";
    return kExitOpErrors;
  }
  if (sweep && !report.sweep_result_ops.has_value()) {
    std::cerr << "loadgen: no swept rate held the bound\n";
    return 3;
  }
  if (gate && !report.pass) {
    std::cerr << "loadgen: one or more targets failed\n";
    return 3;
  }
  return 0;
}
