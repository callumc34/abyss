#include "abyss/resp/request_pipeline.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "abyss/resp/command_registry.h"
#include "abyss/resp/parser.h"

namespace abyss::resp {
namespace {

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

TEST(RequestPipelineTest, RespPing) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  auto result = pipeline.Process(Bytes("*1\r\n$4\r\nPING\r\n"), output);
  EXPECT_GT(result.bytes_consumed, 0U);
  EXPECT_FALSE(result.close_requested);
  EXPECT_EQ(ToStr(output), "+PONG\r\n");
}

TEST(RequestPipelineTest, PingWithMessageReturnsBulk) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"), output);
  EXPECT_EQ(ToStr(output), "$5\r\nhello\r\n");
}

TEST(RequestPipelineTest, InlinePing) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  auto result = pipeline.Process(Bytes("PING\r\n"), output);
  EXPECT_EQ(result.bytes_consumed, 6U);
  EXPECT_EQ(ToStr(output), "+PONG\r\n");
}

TEST(RequestPipelineTest, Echo) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$4\r\nECHO\r\n$3\r\nfoo\r\n"), output);
  EXPECT_EQ(ToStr(output), "$3\r\nfoo\r\n");
}

TEST(RequestPipelineTest, QuitSetsCloseRequested) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  auto result = pipeline.Process(Bytes("*1\r\n$4\r\nQUIT\r\n"), output);
  EXPECT_TRUE(result.close_requested);
  EXPECT_EQ(ToStr(output), "+OK\r\n");
}

TEST(RequestPipelineTest, UnknownCommandReturnsErr) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$4\r\nHGET\r\n$3\r\nkey\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("ERR unknown command 'HGET'"), std::string::npos);
}

TEST(RequestPipelineTest, ArityMismatchReturnsErr) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$3\r\nGET\r\n"), output);  // GET needs a key
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("wrong number of arguments"), std::string::npos);
  EXPECT_NE(response.AsString().find("'get'"), std::string::npos);
}

TEST(RequestPipelineTest, Hello2ReturnsHandshakeMap) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 42});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  const auto& elements = response.AsArray();
  ASSERT_EQ(elements.size(), 14U);
  // Spot-check a few key/value pairs by position.
  EXPECT_EQ(elements[0].AsString(), "server");
  EXPECT_EQ(elements[1].AsString(), "abyss");
  EXPECT_EQ(elements[4].AsString(), "proto");
  EXPECT_EQ(elements[5].AsInteger(), 2);
  EXPECT_EQ(elements[6].AsString(), "id");
  EXPECT_EQ(elements[7].AsInteger(), 42);
}

TEST(RequestPipelineTest, Hello3Rejected) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("NOPROTO"), std::string::npos);
}

TEST(RequestPipelineTest, HelloSetnameUpdatesConnectionState) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*4\r\n$5\r\nHELLO\r\n$1\r\n2\r\n$7\r\nSETNAME\r\n$3\r\nbob\r\n"), output);
  EXPECT_EQ(pipeline.state().client_name, "bob");
}

TEST(RequestPipelineTest, ClientId) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 99});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsInteger());
  EXPECT_EQ(response.AsInteger(), 99);
}

TEST(RequestPipelineTest, ClientSetnameThenGetname) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
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
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsInteger());
  EXPECT_EQ(response.AsInteger(), static_cast<int64_t>(GlobalRegistry().Size()));
}

TEST(RequestPipelineTest, Time) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  pipeline.Process(Bytes("*1\r\n$4\r\nTIME\r\n"), output);
  auto response = ParseResponse(output);
  ASSERT_TRUE(response.IsArray());
  ASSERT_EQ(response.AsArray().size(), 2U);
  // Values are stringly-typed; just sanity-check they parse as numbers.
  EXPECT_GT(std::stoll(response.AsArray()[0].AsString()), 0);
}

TEST(RequestPipelineTest, DeferredDispatchReturnsNotImplemented) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  // GET is kTieredRead → should return the not-implemented error.
  pipeline.Process(Bytes("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"), output);
  auto response = ParseResponse(output);
  EXPECT_TRUE(response.IsError());
  EXPECT_NE(response.AsString().find("not implemented"), std::string::npos);
}

TEST(RequestPipelineTest, PartialFrameLeftForCaller) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  // Half a frame — only the length header for a bulk command.
  auto result = pipeline.Process(Bytes("*2\r\n$4\r\nPING\r\n$3\r\nhe"), output);
  // Should consume nothing because the frame is incomplete.
  EXPECT_EQ(result.bytes_consumed, 0U);
  EXPECT_TRUE(output.empty());
}

TEST(RequestPipelineTest, PipelinedFramesAreAllProcessed) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  std::vector<uint8_t> output;
  std::string input;
  input.append("*1\r\n$4\r\nPING\r\n");
  input.append("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
  auto result = pipeline.Process(Bytes(input), output);
  EXPECT_EQ(result.bytes_consumed, input.size());
  // Expect two framed responses concatenated.
  EXPECT_EQ(ToStr(output), "+PONG\r\n$3\r\nhey\r\n");
}

TEST(RequestPipelineTest, Reset) {
  RequestPipeline pipeline(GlobalRegistry(), {.client_id = 1});
  pipeline.Dispatch(core::RespCommand{{"CLIENT", "SETNAME", "foo"}});
  EXPECT_EQ(pipeline.state().client_name, "foo");

  std::vector<uint8_t> out;
  pipeline.Process(Bytes("*1\r\n$5\r\nRESET\r\n"), out);
  EXPECT_EQ(ToStr(out), "+RESET\r\n");
  EXPECT_TRUE(pipeline.state().client_name.empty());
}

}  // namespace
}  // namespace abyss::resp
