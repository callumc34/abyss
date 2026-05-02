#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "abyss/admin/http_handler.h"
#include "abyss/core/result.h"

namespace abyss::admin {

struct HttpServerConfig {
  std::string bind = "0.0.0.0";
  // 0 requests an OS-assigned ephemeral port; the bound port is reported via
  // BoundPort() once Start() succeeds.
  uint16_t port = 0;
  std::string label = "admin";
  // Worker thread pool size. Admin/metrics traffic is light; one or two
  // threads is plenty. Default keeps the server responsive without spawning
  // an unbounded number of threads under burst.
  size_t worker_threads = 2;
  // Hard read/write deadlines guard against slow clients holding workers.
  std::chrono::milliseconds read_timeout{5000};
  std::chrono::milliseconds write_timeout{5000};
  // Conservative cap on request size; admin endpoints take no bodies.
  size_t max_request_bytes = 65536;
};

// Minimal HTTP/1.1 server scoped to a fixed route table. Backed by cpp-httplib
// internally; that dependency does not leak through the public interface.
//
// Lifecycle: AddHandler() before Start(); Start() / Stop() / Join() are
// main-thread, not concurrent with each other.
class HttpServer {
 public:
  explicit HttpServer(HttpServerConfig config);
  ~HttpServer();

  HttpServer(const HttpServer&) = delete;
  HttpServer& operator=(const HttpServer&) = delete;
  HttpServer(HttpServer&&) = delete;
  HttpServer& operator=(HttpServer&&) = delete;

  // The handler is borrowed; it must outlive the server.
  void AddHandler(std::string path, HttpHandler* handler);

  core::Result<void> Start();
  void Stop();

  bool IsRunning() const noexcept;
  uint16_t BoundPort() const noexcept;
  const std::string& Label() const noexcept { return config_.label; }

 private:
  HttpServerConfig config_;

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace abyss::admin
