#pragma once

#include <string>
#include <unordered_map>
#include <utility>

namespace abyss::admin {

struct HttpResponse {
  int status = 200;
  std::string content_type = "text/plain; charset=utf-8";
  std::string body;
  std::unordered_map<std::string, std::string> headers;

  static HttpResponse Ok(std::string body, std::string content_type = "text/plain; charset=utf-8") {
    return {.status = 200, .content_type = std::move(content_type), .body = std::move(body)};
  }

  static HttpResponse ServiceUnavailable(std::string body) {
    return {.status = 503, .body = std::move(body)};
  }

  static HttpResponse NotFound() { return {.status = 404, .body = "not found\n"}; }

  static HttpResponse MethodNotAllowed() { return {.status = 405, .body = "method not allowed\n"}; }

  static HttpResponse BadRequest(std::string body = "bad request\n") {
    return {.status = 400, .body = std::move(body)};
  }
};

}  // namespace abyss::admin
