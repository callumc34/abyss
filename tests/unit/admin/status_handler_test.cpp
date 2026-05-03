#include "abyss/admin/status_handler.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>

#include "abyss/admin/http_request.h"
#include "abyss/admin/status_provider.h"

namespace abyss::admin {
namespace {

class FakeProvider : public StatusProvider {
 public:
  explicit FakeProvider(StatusSnapshot snapshot) : snapshot_(std::move(snapshot)) {}
  StatusSnapshot Snapshot() const override { return snapshot_; }

 private:
  StatusSnapshot snapshot_;
};

StatusSnapshot MakeMinimalSnapshot() {
  StatusSnapshot s;
  s.build.version = "0.1.0";
  s.build.commit = "abc1234";
  s.build.date = "2026-05-03";
  s.server.node_id = "node-xyz";
  s.server.mode = "standalone";
  s.server.role = "master";
  s.server.ready = true;
  s.config.profile = "embedded";
  s.config.shard_count = 4;
  s.config.fsync_policy = "group_commit";
  s.config.default_eviction_seconds = 86400;
  s.endpoints.resp = StatusEndpoint{.bind = "0.0.0.0", .port = 6379};
  s.endpoints.admin =
      StatusEndpoint{.bind = "0.0.0.0", .port = 8080, .enabled = true, .emit_enabled = true};
  s.endpoints.metrics =
      StatusEndpoint{.bind = "0.0.0.0", .port = 9090, .enabled = true, .emit_enabled = true};
  s.queue.backend = "builtin_wal";
  s.hot.backend = "builtin_hashmap";
  s.cold.backend = "builtin_rocksdb";
  return s;
}

bool ContainsAll(const std::string& haystack, std::initializer_list<std::string_view> needles) {
  return std::ranges::all_of(needles, [&](std::string_view n) { return haystack.contains(n); });
}

TEST(StatusHandlerTest, ReturnsJsonContentType) {
  FakeProvider provider(MakeMinimalSnapshot());
  StatusHandler handler(&provider);
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 200);
  EXPECT_NE(response.content_type.find("application/json"), std::string::npos);
}

TEST(StatusHandlerTest, BodyHasAllTopLevelKeys) {
  FakeProvider provider(MakeMinimalSnapshot());
  StatusHandler handler(&provider);
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_TRUE(ContainsAll(response.body, {
                                             "\"schema_version\"",
                                             "\"abyss\"",
                                             "\"server\"",
                                             "\"config\"",
                                             "\"endpoints\"",
                                             "\"queue\"",
                                             "\"hot\"",
                                             "\"cold\"",
                                             "\"consumers\"",
                                             "\"lag\"",
                                             "\"connections\"",
                                             "\"cluster\"",
                                         }));
}

TEST(StatusHandlerTest, IncludesNodeIdAndVersion) {
  FakeProvider provider(MakeMinimalSnapshot());
  StatusHandler handler(&provider);
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_TRUE(ContainsAll(response.body, {"\"node_id\":\"node-xyz\"", "\"version\":\"0.1.0\""}));
}

TEST(StatusHandlerTest, ClusterIsExplicitNull) {
  FakeProvider provider(MakeMinimalSnapshot());
  StatusHandler handler(&provider);
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_NE(response.body.find("\"cluster\":null"), std::string::npos);
}

TEST(StatusHandlerTest, EndpointEmitsEnabledOnlyWhenRequested) {
  StatusSnapshot s = MakeMinimalSnapshot();
  s.endpoints.resp.emit_enabled = false;  // RESP doesn't carry an enabled flag.
  s.endpoints.admin.emit_enabled = true;
  s.endpoints.admin.enabled = false;
  FakeProvider provider(s);
  StatusHandler handler(&provider);
  HttpRequest request;
  const auto response = handler.Handle(request);
  // Admin endpoint should carry "enabled":false.
  EXPECT_NE(response.body.find("\"admin\":{\"bind\":\"0.0.0.0\",\"port\":8080,\"enabled\":false}"),
            std::string::npos);
}

TEST(StatusHandlerTest, NullProviderReturns503) {
  StatusHandler handler(nullptr);
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_EQ(response.status, 503);
}

TEST(StatusHandlerTest, EscapesQuotesAndBackslashes) {
  StatusSnapshot s = MakeMinimalSnapshot();
  s.server.node_id = R"(node "with" special \chars)";
  FakeProvider provider(s);
  StatusHandler handler(&provider);
  HttpRequest request;
  const auto response = handler.Handle(request);
  EXPECT_NE(response.body.find(R"("node_id":"node \"with\" special \\chars")"), std::string::npos);
}

}  // namespace
}  // namespace abyss::admin
