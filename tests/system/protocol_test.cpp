#include <string>
#include <thread>
#include <vector>

#include "server_fixture.h"

namespace abyss::system_test {
namespace {

class ProtocolTest : public SystemTest {};

TEST_F(ProtocolTest, UnknownCommandReturnsError) {
  auto r = Client().Command({"FOOBAR"});
  EXPECT_TRUE(r.IsError());
}

TEST_F(ProtocolTest, WrongArityReturnsError) {
  auto r = Client().Command({"GET"});
  EXPECT_TRUE(r.IsError());
}

TEST_F(ProtocolTest, WrongtypeReturnsError) {
  Client().Command({"SET", "k", "v"});
  auto r = Client().Command({"SADD", "k", "m"});
  EXPECT_TRUE(r.IsError());
}

// ENGINE-7: a SET carrying an unrecognised option is a syntax error at the
// frontend; nothing may be appended to the queue, so the key stays absent even
// after a restart replays the log. Isolated fixture: this writes and restarts.
using SetOptionValidationTest = IsolatedDataServerTest;

TEST_F(SetOptionValidationTest, SetWithUnknownOptionNotCommittedToWal) {
  auto rejected = Client().Command({"SET", "eng7", "v", "BOGUS"});
  ASSERT_TRUE(rejected.IsError()) << rejected;
  EXPECT_NE(rejected.String().find("syntax error"), std::string::npos) << rejected;
  EXPECT_TRUE(Client().Command({"GET", "eng7"}).IsNil());

  RestartServer();
  EXPECT_TRUE(Client().Command({"GET", "eng7"}).IsNil());

  // The legitimate option set still round-trips through the same path.
  EXPECT_TRUE(Client().Command({"SET", "eng7", "v", "EX", "600"}).IsOk());
  auto stored = Client().Command({"GET", "eng7"});
  ASSERT_TRUE(stored.IsBulk());
  EXPECT_EQ(stored.String(), "v");
}

class DataProtocolTest : public DataCommandTest {};

TEST_F(DataProtocolTest, PipelinedCommands) {
  std::vector<std::vector<std::string>> cmds = {
      {"SET", "a", "1"}, {"SET", "b", "2"}, {"GET", "a"}, {"GET", "b"}, {"DEL", "a", "b"},
  };
  auto replies = Client().Pipeline(cmds);
  ASSERT_EQ(replies.size(), 5);
  EXPECT_TRUE(replies[0].IsOk());
  EXPECT_TRUE(replies[1].IsOk());
  EXPECT_EQ(replies[2].String(), "1");
  EXPECT_EQ(replies[3].String(), "2");
  EXPECT_EQ(replies[4].Integer(), 2);
}

TEST_F(DataProtocolTest, BinaryPayload) {
  std::string val;
  for (int i = 0; i < 256; ++i) val += static_cast<char>(i);
  EXPECT_TRUE(Client().Command({"SET", "bin", val}).IsOk());
  auto r = Client().Command({"GET", "bin"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), val);
}

TEST_F(DataProtocolTest, LargePayload) {
  std::string big(4UL * 1024UL * 1024UL, 'A');
  EXPECT_TRUE(Client().Command({"SET", "big", big}).IsOk());
  auto r = Client().Command({"GET", "big"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String().size(), big.size());
}

TEST_F(DataProtocolTest, EmptyBulkString) {
  EXPECT_TRUE(Client().Command({"SET", "k", ""}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "");
}

TEST_F(DataProtocolTest, ConcurrentClients) {
  constexpr int kClients = 8;
  constexpr int kOpsPerClient = 50;

  std::vector<std::thread> threads;
  threads.reserve(kClients);
  std::atomic<int> errors{0};

  uint16_t port = ServerPort();
  for (int t = 0; t < kClients; ++t) {
    threads.emplace_back([&errors, t, port]() {
      RedisClient c;
      if (!c.Connect("127.0.0.1", port)) {
        ++errors;
        return;
      }
      for (int i = 0; i < kOpsPerClient; ++i) {
        std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
        auto set_reply = c.Command({"SET", key, "v"});
        if (!set_reply.IsOk()) ++errors;
        auto get_reply = c.Command({"GET", key});
        if (!get_reply.IsBulk() || get_reply.String() != "v") ++errors;
      }
    });
  }

  for (auto& th : threads) th.join();
  EXPECT_EQ(errors.load(), 0);
}

}  // namespace
}  // namespace abyss::system_test
