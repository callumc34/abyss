#pragma once

#include <functional>

#include "abyss/admin/http_handler.h"

namespace abyss::admin {

// Each callback returns true when the corresponding readiness condition is
// satisfied. All must be true for /ready to return 200; otherwise 503 with a
// diagnostic body indicating which failed.
struct ReadyChecks {
  std::function<bool()> tcp_bound;
  std::function<bool()> recovery_complete;
  std::function<bool()> not_shutting_down;
};

class ReadyHandler : public HttpHandler {
 public:
  explicit ReadyHandler(ReadyChecks checks);
  HttpResponse Handle(const HttpRequest& request) override;

 private:
  ReadyChecks checks_;
};

}  // namespace abyss::admin
