#include "abyss/admin/health_handler.h"

namespace abyss::admin {

HttpResponse HealthHandler::Handle(const HttpRequest& /*request*/) {
  return HttpResponse::Ok("ok\n");
}

}  // namespace abyss::admin
