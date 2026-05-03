#include "abyss/admin/ready_handler.h"

#include <gtest/gtest.h>

#include "abyss/admin/http_request.h"

namespace abyss::admin {
namespace {

ReadyChecks AllOk() {
  return ReadyChecks{
      .tcp_bound = [] { return true; },
      .recovery_complete = [] { return true; },
      .not_shutting_down = [] { return true; },
  };
}

TEST(ReadyHandlerTest, AllChecksPassReturns200) {
  ReadyHandler handler(AllOk());
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 200);
  EXPECT_NE(response.body.find("tcp_bound: true"), std::string::npos);
  EXPECT_NE(response.body.find("recovery_complete: true"), std::string::npos);
  EXPECT_NE(response.body.find("not_shutting_down: true"), std::string::npos);
}

TEST(ReadyHandlerTest, LoadingReturns503WithDiagnostic) {
  auto checks = AllOk();
  checks.recovery_complete = [] { return false; };
  ReadyHandler handler(std::move(checks));
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 503);
  EXPECT_NE(response.body.find("recovery_complete: false"), std::string::npos);
}

TEST(ReadyHandlerTest, ShutdownReturns503) {
  auto checks = AllOk();
  checks.not_shutting_down = [] { return false; };
  ReadyHandler handler(std::move(checks));
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 503);
  EXPECT_NE(response.body.find("not_shutting_down: false"), std::string::npos);
}

TEST(ReadyHandlerTest, TcpDownReturns503) {
  auto checks = AllOk();
  checks.tcp_bound = [] { return false; };
  ReadyHandler handler(std::move(checks));
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 503);
  EXPECT_NE(response.body.find("tcp_bound: false"), std::string::npos);
}

}  // namespace
}  // namespace abyss::admin
