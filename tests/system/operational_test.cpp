#include "server_fixture.h"

namespace abyss::system_test {
namespace {

class OperationalTest : public SystemTest {};

TEST_F(OperationalTest, Ping) {
  auto r = Client().Command({"PING"});
  ASSERT_TRUE(r.IsStatus());
  EXPECT_EQ(r.String(), "PONG");
}

TEST_F(OperationalTest, PingWithMessage) {
  auto r = Client().Command({"PING", "hello"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "hello");
}

TEST_F(OperationalTest, Echo) {
  auto r = Client().Command({"ECHO", "test message"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "test message");
}

TEST_F(OperationalTest, InfoReturnsString) {
  auto r = Client().Command({"INFO"});
  EXPECT_TRUE(r.IsBulk() || r.IsStatus()) << "INFO should return a string, got: " << r;
}

TEST_F(OperationalTest, Quit) {
  auto r = Client().Command({"QUIT"});
  EXPECT_TRUE(r.IsOk());
}

TEST_F(OperationalTest, MultipleSequentialConnections) {
  uint16_t port = ServerPort();
  for (int i = 0; i < 10; ++i) {
    RedisClient c;
    ASSERT_TRUE(c.Connect("127.0.0.1", port));
    auto r = c.Command({"PING"});
    EXPECT_TRUE(r.IsStatus()) << "connection " << i << " failed";
  }
}

}  // namespace
}  // namespace abyss::system_test
