#include "abyss/admin/metrics_handler.h"

#include <string>
#include <utility>

namespace abyss::admin {

namespace {
constexpr const char* kPromContentType = "text/plain; version=0.0.4; charset=utf-8";
}  // namespace

MetricsHandler::MetricsHandler(ScrapeFn scrape) : scrape_(std::move(scrape)) {}

HttpResponse MetricsHandler::Handle(const HttpRequest& /*request*/) {
  std::string payload = scrape_ ? scrape_() : std::string{};
  return HttpResponse::Ok(std::move(payload), kPromContentType);
}

}  // namespace abyss::admin
