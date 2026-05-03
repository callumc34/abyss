#include "abyss/admin/http_server.h"

#include <gtest/gtest.h>

#include <string>

#include "abyss/admin/http_handler.h"
#include "abyss/admin/http_request.h"
#include "abyss/admin/http_response.h"
#include "abyss/platform/net.h"
#include "http_client.h"

namespace abyss::admin {
namespace {

class FixedHandler : public HttpHandler {
 public:
  explicit FixedHandler(HttpResponse response) : response_(std::move(response)) {}
  HttpResponse Handle(const HttpRequest& request) override {
    last_request_ = request;
    return response_;
  }
  const HttpRequest& LastRequest() const { return last_request_; }

 private:
  HttpResponse response_;
  HttpRequest last_request_;
};

class HttpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto r = platform::net::Init();
    ASSERT_TRUE(r.has_value()) << r.error().message();
  }

  HttpServerConfig EphemeralConfig() {
    return HttpServerConfig{.bind = "127.0.0.1", .port = 0, .label = "test", .worker_threads = 1};
  }
};

TEST_F(HttpServerTest, BindsEphemeralPortAndStops) {
  HttpServer server(EphemeralConfig());
  auto r = server.Start();
  ASSERT_TRUE(r.has_value()) << r.error().message();
  EXPECT_GT(server.BoundPort(), 0);
  EXPECT_TRUE(server.IsRunning());
  server.Stop();
  EXPECT_FALSE(server.IsRunning());
  EXPECT_EQ(server.BoundPort(), 0);
}

TEST_F(HttpServerTest, GetDispatchesToRegisteredHandler) {
  FixedHandler handler(HttpResponse::Ok("hello\n"));
  HttpServer server(EphemeralConfig());
  server.AddHandler("/echo", &handler);
  ASSERT_TRUE(server.Start().has_value());

  auto resp = testing::HttpTestClient::Send("127.0.0.1", server.BoundPort(), "GET", "/echo");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 200);
  EXPECT_EQ(resp.body, "hello\n");
  EXPECT_EQ(handler.LastRequest().method, HttpMethod::kGet);
  EXPECT_EQ(handler.LastRequest().path, "/echo");
  server.Stop();
}

TEST_F(HttpServerTest, UnknownPathReturns404) {
  HttpServer server(EphemeralConfig());
  ASSERT_TRUE(server.Start().has_value());

  auto resp = testing::HttpTestClient::Send("127.0.0.1", server.BoundPort(), "GET", "/nope");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 404);
  server.Stop();
}

TEST_F(HttpServerTest, PostReturns405WithAllowHeader) {
  FixedHandler handler(HttpResponse::Ok("ok\n"));
  HttpServer server(EphemeralConfig());
  server.AddHandler("/echo", &handler);
  ASSERT_TRUE(server.Start().has_value());

  auto resp = testing::HttpTestClient::Send("127.0.0.1", server.BoundPort(), "POST", "/echo");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 405);
  EXPECT_TRUE(resp.headers.contains("Allow"));
  server.Stop();
}

TEST_F(HttpServerTest, HeadReturnsHeadersWithEmptyBody) {
  FixedHandler handler(HttpResponse::Ok("hello\n"));
  HttpServer server(EphemeralConfig());
  server.AddHandler("/echo", &handler);
  ASSERT_TRUE(server.Start().has_value());

  auto resp = testing::HttpTestClient::Send("127.0.0.1", server.BoundPort(), "HEAD", "/echo");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 200);
  EXPECT_TRUE(resp.body.empty());
  server.Stop();
}

TEST_F(HttpServerTest, ServiceUnavailableHandlerReturns503) {
  FixedHandler handler(HttpResponse::ServiceUnavailable("loading\n"));
  HttpServer server(EphemeralConfig());
  server.AddHandler("/ready", &handler);
  ASSERT_TRUE(server.Start().has_value());

  auto resp = testing::HttpTestClient::Send("127.0.0.1", server.BoundPort(), "GET", "/ready");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 503);
  EXPECT_EQ(resp.body, "loading\n");
  server.Stop();
}

TEST_F(HttpServerTest, DoubleStartFails) {
  HttpServer server(EphemeralConfig());
  ASSERT_TRUE(server.Start().has_value());
  auto second = server.Start();
  EXPECT_FALSE(second.has_value());
  server.Stop();
}

}  // namespace
}  // namespace abyss::admin
