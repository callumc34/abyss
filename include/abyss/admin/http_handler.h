#pragma once

#include "abyss/admin/http_request.h"
#include "abyss/admin/http_response.h"

namespace abyss::admin {

// Implementations must be thread-safe; the HTTP server may dispatch concurrent
// requests from its worker pool.
class HttpHandler {
 public:
  HttpHandler() = default;
  virtual ~HttpHandler() = default;
  HttpHandler(const HttpHandler&) = delete;
  HttpHandler& operator=(const HttpHandler&) = delete;
  HttpHandler(HttpHandler&&) = delete;
  HttpHandler& operator=(HttpHandler&&) = delete;

  virtual HttpResponse Handle(const HttpRequest& request) = 0;
};

}  // namespace abyss::admin
