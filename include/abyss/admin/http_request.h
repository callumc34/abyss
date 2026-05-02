#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace abyss::admin {

enum class HttpMethod : uint8_t {
  kGet,
  kHead,
  kOther,
};

struct HttpRequest {
  HttpMethod method = HttpMethod::kOther;
  std::string method_text;
  std::string path;
  std::string query;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};

}  // namespace abyss::admin
