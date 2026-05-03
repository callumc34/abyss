#include "abyss/admin/metrics_handler.h"

#include <gtest/gtest.h>

#include <string>

#include "abyss/admin/http_request.h"

namespace abyss::admin {
namespace {

TEST(MetricsHandlerTest, ReturnsScrapePayload) {
  MetricsHandler handler([] { return std::string{"# HELP abyss_test 1\n"}; });
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 200);
  EXPECT_EQ(response.body, "# HELP abyss_test 1\n");
  EXPECT_NE(response.content_type.find("text/plain"), std::string::npos);
  EXPECT_NE(response.content_type.find("version=0.0.4"), std::string::npos);
}

TEST(MetricsHandlerTest, EmptyScrapeIs200WithEmptyBody) {
  MetricsHandler handler([] { return std::string{}; });
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 200);
  EXPECT_TRUE(response.body.empty());
}

TEST(MetricsHandlerTest, NullScrapeIs200WithEmptyBody) {
  MetricsHandler handler({});
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 200);
  EXPECT_TRUE(response.body.empty());
}

}  // namespace
}  // namespace abyss::admin
