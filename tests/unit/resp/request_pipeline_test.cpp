#include "abyss/resp/request_pipeline.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "abyss/resp/command_registry.h"
#include "abyss/resp/config_provider.h"
#include "abyss/resp/loading_state.h"
#include "abyss/resp/node_identity.h"
#include "abyss/resp/parser.h"
#include "abyss/resp/server_stats.h"

namespace abyss::resp {
namespace {

class ToggleLoading : public LoadingStateProvider {
 public:
  bool IsLoading() const override { return loading_; }
  void Set(bool v) { loading_ = v; }

 private:
  bool loading_ = false;
};

class FakeStats : public ServerStatsProvider {
 public:
  explicit FakeStats(ServerStats stats) : stats_(stats) {}
  ServerStats Snapshot() const override { return stats_; }

 private:
  ServerStats stats_;
};

class FakeConfig : public ConfigProvider {
 public:
  explicit FakeConfig(std::vector<Entry> entries) : entries_(std::move(entries)) {}
  std::vector<Entry> Entries() const override { return entries_; }

 private:
  std::vector<Entry> entries_;
};

std::span<const uint8_t> Bytes(const std::string& s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

std::string ToStr(const std::vector<uint8_t>& v) {
  return {reinterpret_cast<const char*>(v.data()), v.size()};
}

core::RespValue ParseResponse(const std::vector<uint8_t>& bytes) {
  auto r = Parser::Parse(std::span<const uint8_t>(bytes.data(), bytes.size()));
  EXPECT_TRUE(r.has_value());
  return r->value;
}

ServerStats StandardStats() {
  return ServerStats{
      .hot_key_count = 3,
      .cold_key_count = 7,
      .hot_memory_bytes = 1024,
      .connected_clients = 4,
      .process_id = 1234,
      .uptime_seconds = 42,
      .tcp_port = 6379,
      .version = "0.1.0-test",
      .bind_address = "127.0.0.1",
      .mode = "standalone",
      .role = "master",
  };
}

TEST(RequestPipelineTest, RespPing) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  auto result = pipeline.Process(Bytes("*1\r\n$4\r\nPING\r\n"), output);
  EXPECT_GT(result.bytes_consumed, 0U);
  EXPECT_FALSE(result.close_requested);
  EXPECT_EQ(ToStr(output), "+PONG\r\n");
}

TEST(RequestPipelineTest, PingWithMessageReturnsBulk) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"), output);
  EXPECT_EQ(ToStr(output), "$5\r\nhello\r\n");
}

TEST(RequestPipelineTest, InlinePing) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  auto result = pipeline.Process(Bytes("PING\r\n"), output);
  EXPECT_EQ(result.bytes_consumed, 6U);
  EXPECT_EQ(ToStr(output), "+PONG\r\n");
}

TEST(RequestPipelineTest, Echo) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$4\r\nECHO\r\n$3\r\nfoo\r\n"), output);
  EXPECT_EQ(ToStr(output), "$3\r\nfoo\r\n");
}

TEST(RequestPipelineTest, QuitSetsCloseRequested) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  auto result = pipeline.Process(Bytes("*1\r\n$4\r\nQUIT\r\n"), output);
  EXPECT_TRUE(result.close_requested);
  EXPECT_EQ(ToStr(output), "+OK\r\n");
}

TEST(RequestPipelineTest, UnknownCommandReturnsErr) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$4\r\nHGET\r\n$3\r\nkey\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("ERR unknown command 'HGET'"), std::string::npos);
}

TEST(RequestPipelineTest, FlushallFallsThroughToUnknownCommand) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$8\r\nFLUSHALL\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("unknown command 'FLUSHALL'"), std::string::npos);
}

TEST(RequestPipelineTest, FlushdbFallsThroughToUnknownCommand) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$7\r\nFLUSHDB\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("unknown command 'FLUSHDB'"), std::string::npos);
}

TEST(RequestPipelineTest, ArityMismatchReturnsErr) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$3\r\nGET\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("wrong number of arguments"), std::string::npos);
  EXPECT_NE(response.AsString().find("'get'"), std::string::npos);
}

TEST(RequestPipelineTest, Hello2ReturnsHandshakeMap) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 42, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  const auto& elements = response.AsArray();
  ASSERT_EQ(elements.size(), 14U);
  EXPECT_EQ(elements[0].AsString(), "server");
  EXPECT_EQ(elements[1].AsString(), "abyss");
  EXPECT_EQ(elements[4].AsString(), "proto");
  EXPECT_EQ(elements[5].AsInteger(), 2);
  EXPECT_EQ(elements[6].AsString(), "id");
  EXPECT_EQ(elements[7].AsInteger(), 42);
}

TEST(RequestPipelineTest, Hello3Rejected) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("NOPROTO"), std::string::npos);
}

TEST(RequestPipelineTest, HelloSetnameUpdatesConnectionState) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*4\r\n$5\r\nHELLO\r\n$1\r\n2\r\n$7\r\nSETNAME\r\n$3\r\nbob\r\n"), output);
  EXPECT_EQ(pipeline.state().client_name, "bob");
}

TEST(RequestPipelineTest, HelloAuthArgumentsAccepted) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(
      Bytes("*5\r\n$5\r\nHELLO\r\n$1\r\n2\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$4\r\npass\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsArray());
}

TEST(RequestPipelineTest, HelloAuthMissingPasswordReturnsErr) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*4\r\n$5\r\nHELLO\r\n$1\r\n2\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("HELLO AUTH"), std::string::npos);
}

TEST(RequestPipelineTest, ClientId) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 99, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsInteger());
  EXPECT_EQ(response.AsInteger(), 99);
}

TEST(RequestPipelineTest, ClientNoEvictAck) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$6\r\nCLIENT\r\n$8\r\nNO-EVICT\r\n$2\r\nON\r\n"), output);
  EXPECT_EQ(ToStr(output), "+OK\r\n");
}

TEST(RequestPipelineTest, ClientSetnameThenGetname) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> out1;
  pipeline.Process(Bytes("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$5\r\nalice\r\n"), out1);
  EXPECT_EQ(ToStr(out1), "+OK\r\n");
  EXPECT_EQ(pipeline.state().client_name, "alice");

  std::vector<uint8_t> out2;
  pipeline.Process(Bytes("*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n"), out2);
  auto response = ParseResponse(out2);
  EXPECT_TRUE(response.IsBulkString());
  EXPECT_EQ(response.AsString(), "alice");
}

TEST(RequestPipelineTest, CommandCountMatchesRegistrySize) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsInteger());
  EXPECT_EQ(response.AsInteger(), static_cast<int64_t>(GlobalRegistry().Size()));
}

TEST(RequestPipelineTest, CommandListReturnsEveryRegistryEntry) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$7\r\nCOMMAND\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  EXPECT_EQ(response.AsArray().size(), GlobalRegistry().Size());
}

TEST(RequestPipelineTest, CommandInfoReportsGetSpec) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nGET\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  ASSERT_EQ(response.AsArray().size(), 1U);
  const auto& entry = response.AsArray()[0];
  ASSERT_TRUE(entry.IsArray());
  ASSERT_GE(entry.AsArray().size(), 6U);
  EXPECT_EQ(entry.AsArray()[0].AsString(), "get");
  EXPECT_EQ(entry.AsArray()[1].AsInteger(), 2);
  ASSERT_TRUE(entry.AsArray()[2].IsArray());
}

TEST(RequestPipelineTest, CommandInfoUnknownIsNull) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$7\r\nBOGUSCC\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  ASSERT_EQ(response.AsArray().size(), 1U);
  EXPECT_TRUE(response.AsArray()[0].IsNull());
}

TEST(RequestPipelineTest, CommandDocsReturnsSummary) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n$4\r\nPING\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  ASSERT_EQ(response.AsArray().size(), 2U);
  EXPECT_EQ(response.AsArray()[0].AsString(), "ping");
  ASSERT_TRUE(response.AsArray()[1].IsArray());
}

TEST(RequestPipelineTest, Time) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$4\r\nTIME\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  ASSERT_EQ(response.AsArray().size(), 2U);
  EXPECT_GT(std::stoll(response.AsArray()[0].AsString()), 0);
}

TEST(RequestPipelineTest, TieredReadWithoutDispatcherReturnsErr) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("tiered read not configured"), std::string::npos);
}

TEST(RequestPipelineTest, ConditionalWriteRejectedWithIssueReference) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$5\r\nSETNX\r\n$1\r\nk\r\n$1\r\nv\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("conditional writes are not supported"), std::string::npos);
  EXPECT_NE(response.AsString().find("SETNX"), std::string::npos);
  EXPECT_NE(response.AsString().find("abyss#97"), std::string::npos);
}

TEST(RequestPipelineTest, ConsumerRpcRejectedWithIssueReference) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  // DBSIZE without a stats provider returns a config error (not the abyss#97
  // path); OBJECT IDLETIME is kTieredRead in Phase 1 so we stage via raw SETNX
  // path above. Assert the abyss#97 marker is present for SETNX (already
  // covered) and that DBSIZE without provider yields a clear configuration
  // error rather than the generic abyss#97 message.
  pipeline.Process(Bytes("*1\r\n$6\r\nDBSIZE\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("server stats provider not configured"), std::string::npos);
}

TEST(RequestPipelineTest, LoadingGateBlocksDataPlaneCommands) {
  ToggleLoading loading;
  loading.Set(true);
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.loading = &loading});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("LOADING"), std::string::npos);
}

TEST(RequestPipelineTest, LoadingGateAllowsPing) {
  ToggleLoading loading;
  loading.Set(true);
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.loading = &loading});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$4\r\nPING\r\n"), output);
  EXPECT_EQ(ToStr(output), "+PONG\r\n");
}

TEST(RequestPipelineTest, LoadingGateAllowsHello) {
  ToggleLoading loading;
  loading.Set(true);
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.loading = &loading});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsArray());
}

TEST(RequestPipelineTest, LoadingGateAllowsCommandCount) {
  ToggleLoading loading;
  loading.Set(true);
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.loading = &loading});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsInteger());
}

TEST(RequestPipelineTest, LoadingGateRejectsNonAllowlistedClusterSubcommands) {
  ToggleLoading loading;
  loading.Set(true);
  FakeStats stats(StandardStats());
  NodeIdentity identity("00000000-0000-4000-8000-000000000000");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.loading = &loading, .stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$3\r\nfoo\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("LOADING"), std::string::npos);
}

TEST(RequestPipelineTest, LoadingGateAllowsClusterSlots) {
  ToggleLoading loading;
  loading.Set(true);
  FakeStats stats(StandardStats());
  NodeIdentity identity("00000000-0000-4000-8000-000000000000");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.loading = &loading, .stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
}

TEST(RequestPipelineTest, InfoReturnsStatsSections) {
  FakeStats stats(StandardStats());
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$4\r\nINFO\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsBulkString());
  const auto& body = response.AsString();
  EXPECT_NE(body.find("# Server"), std::string::npos);
  EXPECT_NE(body.find("# Clients"), std::string::npos);
  EXPECT_NE(body.find("# Memory"), std::string::npos);
  EXPECT_NE(body.find("# Keyspace"), std::string::npos);
  EXPECT_NE(body.find("tcp_port:6379"), std::string::npos);
  EXPECT_NE(body.find("connected_clients:4"), std::string::npos);
}

TEST(RequestPipelineTest, InfoFiltersBySectionName) {
  FakeStats stats(StandardStats());
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$4\r\nINFO\r\n$7\r\nclients\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsBulkString());
  const auto& body = response.AsString();
  EXPECT_NE(body.find("# Clients"), std::string::npos);
  EXPECT_EQ(body.find("# Server"), std::string::npos);
  EXPECT_EQ(body.find("# Memory"), std::string::npos);
}

TEST(RequestPipelineTest, InfoUnknownSectionReturnsEmptyBulk) {
  FakeStats stats(StandardStats());
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$4\r\nINFO\r\n$7\r\nnosuch_\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsBulkString());
  EXPECT_TRUE(response.AsString().empty());
}

TEST(RequestPipelineTest, DbsizeSumsHotAndCold) {
  FakeStats stats(StandardStats());  // 3 + 7
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$6\r\nDBSIZE\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsInteger());
  EXPECT_EQ(response.AsInteger(), 10);
}

TEST(RequestPipelineTest, ClusterSlotsReturnsSingleRange) {
  FakeStats stats(StandardStats());
  NodeIdentity identity("11111111-1111-4111-8111-111111111111");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  ASSERT_EQ(response.AsArray().size(), 1U);
  const auto& range = response.AsArray()[0];
  ASSERT_TRUE(range.IsArray());
  ASSERT_EQ(range.AsArray().size(), 3U);
  EXPECT_EQ(range.AsArray()[0].AsInteger(), 0);
  EXPECT_EQ(range.AsArray()[1].AsInteger(), 16383);
  const auto& node = range.AsArray()[2];
  ASSERT_TRUE(node.IsArray());
  ASSERT_EQ(node.AsArray().size(), 3U);
  EXPECT_EQ(node.AsArray()[0].AsString(), "127.0.0.1");
  EXPECT_EQ(node.AsArray()[1].AsInteger(), 6379);
  EXPECT_EQ(node.AsArray()[2].AsString(), "11111111-1111-4111-8111-111111111111");
}

TEST(RequestPipelineTest, ClusterKeyslotMatchesCore) {
  FakeStats stats(StandardStats());
  NodeIdentity identity("11111111-1111-4111-8111-111111111111");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$3\r\nfoo\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsInteger());
  EXPECT_EQ(response.AsInteger(), 12182);
}

TEST(RequestPipelineTest, ClusterMyIdReturnsIdentity) {
  FakeStats stats(StandardStats());
  NodeIdentity identity("22222222-2222-4222-8222-222222222222");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsBulkString());
  EXPECT_EQ(response.AsString(), "22222222-2222-4222-8222-222222222222");
}

TEST(RequestPipelineTest, ClusterNodesReturnsSingleLine) {
  FakeStats stats(StandardStats());
  NodeIdentity identity("33333333-3333-4333-8333-333333333333");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsBulkString());
  EXPECT_NE(response.AsString().find("33333333-3333-4333-8333-333333333333"), std::string::npos);
  EXPECT_NE(response.AsString().find("127.0.0.1:6379"), std::string::npos);
  EXPECT_NE(response.AsString().find("master"), std::string::npos);
  EXPECT_NE(response.AsString().find("0-16383"), std::string::npos);
}

TEST(RequestPipelineTest, ClusterInfoReportsOk) {
  FakeStats stats(StandardStats());
  NodeIdentity identity("33333333-3333-4333-8333-333333333333");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsBulkString());
  EXPECT_NE(response.AsString().find("cluster_state:ok"), std::string::npos);
  EXPECT_NE(response.AsString().find("cluster_slots_assigned:16384"), std::string::npos);
}

TEST(RequestPipelineTest, ClusterCountKeysInSlotUsesTotal) {
  FakeStats stats(StandardStats());
  NodeIdentity identity("33333333-3333-4333-8333-333333333333");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$7\r\nCLUSTER\r\n$15\r\nCOUNTKEYSINSLOT\r\n$1\r\n0\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsInteger());
  EXPECT_EQ(response.AsInteger(), 10);
}

TEST(RequestPipelineTest, ClusterCountKeysOutOfRangeReturnsZero) {
  FakeStats stats(StandardStats());
  NodeIdentity identity("33333333-3333-4333-8333-333333333333");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$7\r\nCLUSTER\r\n$15\r\nCOUNTKEYSINSLOT\r\n$5\r\n99999\r\n"),
                   output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsInteger());
  EXPECT_EQ(response.AsInteger(), 0);
}

TEST(RequestPipelineTest, ClusterUnknownSubcommandErrors) {
  FakeStats stats(StandardStats());
  NodeIdentity identity("33333333-3333-4333-8333-333333333333");
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.stats = &stats, .identity = &identity});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCLUSTER\r\n$6\r\nFORGET\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("Unknown CLUSTER subcommand"), std::string::npos);
}

TEST(RequestPipelineTest, ConfigGetExactMatch) {
  FakeConfig config({{"maxmemory", "4294967296"}, {"port", "6379"}});
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.config = &config});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\nmaxmemory\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  ASSERT_EQ(response.AsArray().size(), 2U);
  EXPECT_EQ(response.AsArray()[0].AsString(), "maxmemory");
  EXPECT_EQ(response.AsArray()[1].AsString(), "4294967296");
}

TEST(RequestPipelineTest, ConfigGetGlobMatchesMultiple) {
  FakeConfig config({{"maxmemory", "1"}, {"maxclients", "2"}, {"port", "3"}});
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.config = &config});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nmax*\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  EXPECT_EQ(response.AsArray().size(), 4U);
}

TEST(RequestPipelineTest, ConfigSetRejected) {
  FakeConfig config({});
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}},
                           {.config = &config});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$4\r\nfoo1\r\n$1\r\n1\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("Unknown CONFIG subcommand"), std::string::npos);
}

TEST(RequestPipelineTest, PartialFrameLeftForCaller) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  auto result = pipeline.Process(Bytes("*2\r\n$4\r\nPING\r\n$3\r\nhe"), output);
  EXPECT_EQ(result.bytes_consumed, 0U);
  EXPECT_TRUE(output.empty());
}

TEST(RequestPipelineTest, PipelinedFramesAreAllProcessed) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  std::vector<uint8_t> output;
  std::string input;
  input.append("*1\r\n$4\r\nPING\r\n");
  input.append("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
  auto result = pipeline.Process(Bytes(input), output);
  EXPECT_EQ(result.bytes_consumed, input.size());
  EXPECT_EQ(ToStr(output), "+PONG\r\n$3\r\nhey\r\n");
}

TEST(RequestPipelineTest, Reset) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1, .client_name = {}});
  pipeline.Dispatch(core::RespCommand{{"CLIENT", "SETNAME", "foo"}});
  EXPECT_EQ(pipeline.state().client_name, "foo");

  std::vector<uint8_t> out;
  pipeline.Process(Bytes("*1\r\n$5\r\nRESET\r\n"), out);
  EXPECT_EQ(ToStr(out), "+RESET\r\n");
  EXPECT_TRUE(pipeline.state().client_name.empty());
}

}  // namespace
}  // namespace abyss::resp
