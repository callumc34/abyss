#include "abyss/admin/health_handler.h"

#include <gtest/gtest.h>

#include "abyss/admin/http_request.h"

namespace abyss::admin {
namespace {

TEST(HealthHandlerTest, AlwaysReturns200) {
  HealthHandler handler;
  HttpRequest request;
  request.method = HttpMethod::kGet;
  request.path = "/healthz";
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 200);
  EXPECT_EQ(response.body, "ok\n");
}

}  // namespace
}  // namespace abyss::admin
