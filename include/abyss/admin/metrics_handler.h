#pragma once

#include <functional>
#include <string>

#include "abyss/admin/http_handler.h"

namespace abyss::admin {

using ScrapeFn = std::function<std::string()>;

// Serves the Prometheus text-format scrape payload. Empty body when metrics
// are disabled or no metrics have been registered.
class MetricsHandler : public HttpHandler {
 public:
  explicit MetricsHandler(ScrapeFn scrape);
  HttpResponse Handle(const HttpRequest& request) override;

 private:
  ScrapeFn scrape_;
};

}  // namespace abyss::admin
