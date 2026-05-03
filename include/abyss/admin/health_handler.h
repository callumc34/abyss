#pragma once

#include "abyss/admin/http_handler.h"

namespace abyss::admin {

// Liveness probe. K8s SIG-Node convention: succeed unconditionally — the
// fact that the request is being answered is itself the liveness signal.
class HealthHandler : public HttpHandler {
 public:
  HealthHandler() = default;
  HttpResponse Handle(const HttpRequest& request) override;
};

}  // namespace abyss::admin
