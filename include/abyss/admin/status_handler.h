#pragma once

#include "abyss/admin/http_handler.h"
#include "abyss/admin/status_provider.h"

namespace abyss::admin {

class StatusHandler : public HttpHandler {
 public:
  explicit StatusHandler(const StatusProvider* provider);
  HttpResponse Handle(const HttpRequest& request) override;

 private:
  const StatusProvider* provider_;
};

}  // namespace abyss::admin
