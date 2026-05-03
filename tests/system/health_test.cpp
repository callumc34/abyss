#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "http_client.h"
#include "server_fixture.h"

namespace abyss::system_test {
namespace {

using abyss::testing::HttpTestClient;

class HealthEndpointsTest : public IsolatedServerTest {};

TEST_F(HealthEndpointsTest, AdminAndMetricsBindToEphemeralPorts) {
  ASSERT_GT(Server().Port(), 0);
  ASSERT_GT(Server().AdminPort(), 0);
  ASSERT_GT(Server().MetricsPort(), 0);
  EXPECT_NE(Server().Port(), Server().AdminPort());
  EXPECT_NE(Server().Port(), Server().MetricsPort());
  EXPECT_NE(Server().AdminPort(), Server().MetricsPort());
}

TEST_F(HealthEndpointsTest, HealthzReturns200) {
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().AdminPort(), "GET", "/healthz");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 200);
  EXPECT_EQ(resp.body, "ok\n");
}

TEST_F(HealthEndpointsTest, ReadyReturns200OnceServerIsReady) {
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().AdminPort(), "GET", "/ready");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 200);
  EXPECT_NE(resp.body.find("tcp_bound: true"), std::string::npos);
  EXPECT_NE(resp.body.find("recovery_complete: true"), std::string::npos);
  EXPECT_NE(resp.body.find("not_shutting_down: true"), std::string::npos);
}

TEST_F(HealthEndpointsTest, MetricsReturnsPrometheusContentType) {
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().MetricsPort(), "GET", "/metrics");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 200);
  const auto it = resp.headers.find("Content-Type");
  ASSERT_NE(it, resp.headers.end());
  EXPECT_NE(it->second.find("text/plain"), std::string::npos);
  EXPECT_NE(it->second.find("version=0.0.4"), std::string::npos);
}

TEST_F(HealthEndpointsTest, MetricsBodyIsPrometheusTextFormat) {
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().MetricsPort(), "GET", "/metrics");
  ASSERT_TRUE(resp.ok) << resp.error;
  ASSERT_EQ(resp.status, 200);
  // We don't require any specific metric since instrumentation is per-subsystem,
  // but the response body should at minimum parse as either empty or as a
  // sequence of HELP/TYPE/sample lines starting with '#' or 'abyss_'.
  if (!resp.body.empty()) {
    const char first = resp.body.front();
    EXPECT_TRUE(first == '#' || first == 'a') << "unexpected first byte: " << int{first};
  }
}

TEST_F(HealthEndpointsTest, StatusReturnsJson) {
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().AdminPort(), "GET", "/status");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 200);
  const auto it = resp.headers.find("Content-Type");
  ASSERT_NE(it, resp.headers.end());
  EXPECT_NE(it->second.find("application/json"), std::string::npos);
  ASSERT_FALSE(resp.body.empty());
  EXPECT_EQ(resp.body.front(), '{');
  EXPECT_EQ(resp.body.back(), '}');
}

TEST_F(HealthEndpointsTest, StatusBodyIncludesFoundationalKeys) {
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().AdminPort(), "GET", "/status");
  ASSERT_TRUE(resp.ok) << resp.error;
  ASSERT_EQ(resp.status, 200);
  for (auto key : {std::string_view{"\"schema_version\""}, std::string_view{"\"abyss\""},
                   std::string_view{"\"server\""}, std::string_view{"\"config\""},
                   std::string_view{"\"endpoints\""}, std::string_view{"\"queue\""},
                   std::string_view{"\"hot\""}, std::string_view{"\"cold\""},
                   std::string_view{"\"consumers\""}, std::string_view{"\"lag\""},
                   std::string_view{"\"connections\""}, std::string_view{"\"cluster\":null"}}) {
    EXPECT_TRUE(resp.body.contains(key)) << "missing key: " << key;
  }
}

TEST_F(HealthEndpointsTest, UnknownPathReturns404) {
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().AdminPort(), "GET", "/nope");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 404);
}

TEST_F(HealthEndpointsTest, PostReturns405) {
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().AdminPort(), "POST", "/healthz");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 405);
}

TEST_F(HealthEndpointsTest, MetricsServerDoesNotServeStatus) {
  // /status is admin-only; the metrics port returns 404 for it.
  const auto resp = HttpTestClient::Send("127.0.0.1", Server().MetricsPort(), "GET", "/status");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 404);
}

}  // namespace
}  // namespace abyss::system_test
