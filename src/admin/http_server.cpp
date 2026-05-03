#include "abyss/admin/http_server.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

// httplib must come after the project headers above so its macros (e.g.
// CPPHTTPLIB_OPENSSL_SUPPORT) can be tuned via project options without
// surprising the rest of the translation unit.
#include <httplib.h>  // NOLINT(misc-include-cleaner)

#include "abyss/admin/http_handler.h"
#include "abyss/admin/http_request.h"
#include "abyss/admin/http_response.h"
#include "abyss/core/result.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.admin.http")

namespace abyss::admin {

namespace {

HttpMethod ParseMethod(const std::string& method) {
  if (method == "GET") return HttpMethod::kGet;
  if (method == "HEAD") return HttpMethod::kHead;
  return HttpMethod::kOther;
}

void TranslateRequest(const httplib::Request& src, HttpRequest& dst) {
  dst.method = ParseMethod(src.method);
  dst.method_text = src.method;
  dst.path = src.path;
  for (const auto& [key, value] : src.headers) {
    dst.headers.emplace(key, value);
  }
  dst.body = src.body;
}

void TranslateResponse(const HttpResponse& src, httplib::Response& dst) {
  dst.status = src.status;
  for (const auto& [key, value] : src.headers) {
    dst.set_header(key, value);
  }
  dst.set_content(src.body, src.content_type);
}

}  // namespace

struct HttpServer::Impl {
  httplib::Server server;
  std::mutex handlers_mu;
  std::unordered_map<std::string, HttpHandler*> handlers;
  std::thread loop;
  std::atomic<bool> running{false};
  std::atomic<uint16_t> bound_port{0};
};

HttpServer::HttpServer(HttpServerConfig config)
    : config_(std::move(config)), impl_(std::make_unique<Impl>()) {}

HttpServer::~HttpServer() { Stop(); }

void HttpServer::AddHandler(std::string path, HttpHandler* handler) {
  const std::scoped_lock lock(impl_->handlers_mu);
  impl_->handlers[std::move(path)] = handler;
}

core::Result<void> HttpServer::Start() {
  if (impl_->running.load(std::memory_order_acquire)) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "http server already running"});
  }

  impl_->server.set_read_timeout(
      std::chrono::duration_cast<std::chrono::seconds>(config_.read_timeout).count(),
      static_cast<long>((config_.read_timeout % std::chrono::seconds{1}).count()) *
          1000L);  // microseconds remainder
  impl_->server.set_write_timeout(
      std::chrono::duration_cast<std::chrono::seconds>(config_.write_timeout).count(),
      static_cast<long>((config_.write_timeout % std::chrono::seconds{1}).count()) * 1000L);
  impl_->server.set_payload_max_length(config_.max_request_bytes);
  impl_->server.new_task_queue = [n = config_.worker_threads] {
    return new httplib::ThreadPool(n);  // NOLINT(cppcoreguidelines-owning-memory)
  };

  // Catch-all dispatch — single entry point keeps method/path validation in
  // one place rather than scattering it across cpp-httplib's per-method APIs.
  auto dispatch = [this](const httplib::Request& req, httplib::Response& res) {
    HttpRequest abyss_req;
    TranslateRequest(req, abyss_req);

    HttpHandler* handler = nullptr;
    {
      const std::scoped_lock lock(impl_->handlers_mu);
      const auto it = impl_->handlers.find(abyss_req.path);
      if (it != impl_->handlers.end()) handler = it->second;
    }

    HttpResponse abyss_res;
    if (handler == nullptr) {
      abyss_res = HttpResponse::NotFound();
    } else if (abyss_req.method != HttpMethod::kGet && abyss_req.method != HttpMethod::kHead) {
      abyss_res = HttpResponse::MethodNotAllowed();
      abyss_res.headers["Allow"] = "GET, HEAD";
    } else {
      abyss_res = handler->Handle(abyss_req);
      // HEAD: same headers as GET, empty body.
      if (abyss_req.method == HttpMethod::kHead) abyss_res.body.clear();
    }

    TranslateResponse(abyss_res, res);
    ABYSS_LOG_DEBUG("admin request", {"label", std::string_view{config_.label}},
                    {"method", std::string_view{abyss_req.method_text}},
                    {"path", std::string_view{abyss_req.path}},
                    {"status", static_cast<int64_t>(abyss_res.status)});
  };

  impl_->server.Get(".*", dispatch);
  impl_->server.Post(".*", dispatch);
  impl_->server.Put(".*", dispatch);
  impl_->server.Delete(".*", dispatch);
  impl_->server.Patch(".*", dispatch);
  impl_->server.Options(".*", dispatch);

  const int bound = impl_->server.bind_to_any_port(config_.bind, config_.port);
  if (bound <= 0) {
    std::string msg = "http bind failed: ";
    msg += config_.bind;
    msg += ':';
    msg += std::to_string(config_.port);
    return std::unexpected(core::Error{core::ErrorCode::kInternal, std::move(msg)});
  }
  impl_->bound_port.store(static_cast<uint16_t>(bound), std::memory_order_release);

  impl_->loop = std::thread([this] {
    impl_->server.listen_after_bind();
    impl_->running.store(false, std::memory_order_release);
  });

  impl_->server.wait_until_ready();
  impl_->running.store(true, std::memory_order_release);

  ABYSS_LOG_INFO("admin http listening", {"label", std::string_view{config_.label}},
                 {"bind", std::string_view{config_.bind}},
                 {"port", static_cast<int64_t>(BoundPort())});
  return {};
}

void HttpServer::Stop() {
  if (!impl_->running.load(std::memory_order_acquire) && !impl_->loop.joinable()) return;
  impl_->server.stop();
  if (impl_->loop.joinable()) impl_->loop.join();
  impl_->running.store(false, std::memory_order_release);
  impl_->bound_port.store(0, std::memory_order_release);
}

bool HttpServer::IsRunning() const noexcept {
  return impl_->running.load(std::memory_order_acquire);
}

uint16_t HttpServer::BoundPort() const noexcept {
  return impl_->bound_port.load(std::memory_order_acquire);
}

}  // namespace abyss::admin
