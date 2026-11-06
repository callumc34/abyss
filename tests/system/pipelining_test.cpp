#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "progress_watchdog.h"
#include "prom_scrape.h"
#include "server_fixture.h"

namespace abyss::system_test {
namespace {

using abyss::testing::ProgressWatchdog;

// redis-cli --pipe: one connection writes 10k fast-path SETs as one
// stream while it reads, ten times the in-flight cap. Each SET answers
// with its key's old value, so the replies show their order.
TEST_F(DataCommandTest, TenThousandPipelinedSetsAllReplyInOrder) {
  constexpr int kCommands = 10000;
  constexpr int kKeys = 16;
  const auto key = [](int i) { return "bulk" + std::to_string(i % kKeys); };
  const auto value = [](int i) { return "v" + std::to_string(i); };
  // Resident, so every SET takes the fast path.
  for (int i = -kKeys; i < 0; ++i) {
    ASSERT_TRUE(Client().Command({"SET", key(i + kKeys), value(i)}).IsOk());
  }
  std::vector<std::vector<std::string>> commands;
  commands.reserve(kCommands);
  for (int i = 0; i < kCommands; ++i) commands.push_back({"SET", key(i), value(i), "GET"});

  ProgressWatchdog watchdog("10k pipelined SETs: sending");
  bool sent = false;
  std::thread writer([&] { sent = Client().Send(commands); });
  int wrong = 0;
  std::string first_wrong;
  for (int i = 0; i < kCommands; ++i) {
    const Reply reply = Client().Read();
    watchdog.Tick();
    if (reply.IsBulk() && reply.String() == value(i - kKeys)) continue;
    if (wrong++ == 0) first_wrong = "reply " + std::to_string(i) + ": " + reply.String();
    if (reply.IsError() && reply.String() == "connection lost") break;
  }
  writer.join();
  ASSERT_TRUE(sent);
  EXPECT_EQ(wrong, 0) << "first: " << first_wrong;
}

constexpr size_t kBackpressureBytes = 262144;
constexpr size_t kBigBytes = 4096;

class SlowConsumerTest : public IsolatedDataServerTest {
 protected:
  TestServer::Config MakeServerConfig() const override {
    TestServer::Config config;
    config.config_yaml = R"yaml(
queue:
  wal_path: "${DATA_DIR}/wal"
cold:
  data_path: "${DATA_DIR}/cold"
net:
  write_backpressure_bytes: )yaml" +
                         std::to_string(kBackpressureBytes) +
                         R"yaml(
  write_resume_bytes: 65536
)yaml";
    return config;
  }
};

struct NetState {
  double paused = 0;
  double bytes_out = 0;
  double gets = 0;
  double echoes = 0;
  double closed_backpressure = 0;

  bool operator==(const NetState&) const = default;
};

std::optional<NetState> ReadNet(uint16_t port) {
  const std::string body = Scrape(port);
  if (body.empty()) return std::nullopt;
  return NetState{
      .paused = ParseCounter(body, "abyss_net_backpressure_active").value_or(0),
      .bytes_out = ParseCounter(body, "abyss_net_bytes_out_total").value_or(0),
      .gets = ParseCounter(body, "abyss_resp_requests_total", {{"cmd", "GET"}, {"status", "ok"}})
                  .value_or(0),
      .echoes = ParseCounter(body, "abyss_resp_requests_total", {{"cmd", "ECHO"}, {"status", "ok"}})
                    .value_or(0),
      .closed_backpressure =
          ParseCounter(body, "abyss_net_connections_closed_total", {{"reason", "backpressure"}})
              .value_or(0),
  };
}

// RESP's bulk string encoding: $<len>\r\n<bytes>\r\n.
size_t BulkBytes(size_t length) { return 1 + std::to_string(length).size() + 2 + length + 2; }

// A client that stops reading mid-pipeline is paused at the reply-byte
// cap, holding at most the cap plus one reply; another client meanwhile
// runs without error; on reading again the slow one gets every reply,
// in order. The slow client's writer runs until it has read every
// reply, so nothing in between returns early.
TEST_F(SlowConsumerTest, APausedClientHoldsTheCapAndStallsNoOneElse) {
  constexpr int kPairs = 5000;
  constexpr int kOtherCommands = 1000;
  const std::string big(kBigBytes, 'b');
  ASSERT_TRUE(Client().Command({"SET", "big", big}).IsOk());
  const uint16_t port = Server().MetricsPort();
  const std::optional<NetState> before = ReadNet(port);
  ASSERT_TRUE(before.has_value());

  // GET big, then ECHO <n>: big replies to reach the cap, and numbered
  // ones to show the order.
  std::vector<std::vector<std::string>> commands;
  std::vector<size_t> reply_bytes;
  for (int i = 0; i < kPairs; ++i) {
    commands.push_back({"GET", "big"});
    reply_bytes.push_back(BulkBytes(kBigBytes));
    commands.push_back({"ECHO", std::to_string(i)});
    reply_bytes.push_back(BulkBytes(std::to_string(i).size()));
  }
  RedisClient slow;
  ASSERT_TRUE(slow.Connect("127.0.0.1", Server().Port()));
  ProgressWatchdog watchdog("slow consumer: sending");
  bool sent = false;
  std::thread writer([&] { sent = slow.Send(commands); });

  // Paused, and still: the same counts on two scrapes in a row.
  std::optional<NetState> held;
  std::optional<NetState> last;
  const auto settle_by = std::chrono::steady_clock::now() + std::chrono::seconds{30};
  while (!held.has_value() && std::chrono::steady_clock::now() < settle_by) {
    const std::optional<NetState> now = ReadNet(port);
    if (now.has_value() && now->paused >= 1 && last.has_value() && *now == *last) held = now;
    last = now;
    watchdog.Tick();
    if (!held.has_value()) std::this_thread::sleep_for(std::chrono::milliseconds{50});
  }
  EXPECT_TRUE(held.has_value()) << "the slow client was never paused";
  if (held.has_value()) {
    const auto dispatched =
        static_cast<size_t>((held->gets - before->gets) + (held->echoes - before->echoes));
    EXPECT_GT(dispatched, 0U);
    EXPECT_LT(dispatched, commands.size()) << "the cap never held the slow client back";
    size_t generated = 0;
    for (size_t i = 0; i < std::min(dispatched, commands.size()); ++i) {
      generated += reply_bytes[i];
    }
    const auto written = static_cast<size_t>(held->bytes_out - before->bytes_out);
    EXPECT_GE(generated, written);
    const size_t buffered = generated > written ? generated - written : 0;
    EXPECT_LE(buffered, kBackpressureBytes + BulkBytes(kBigBytes))
        << dispatched << " dispatched, " << generated << " reply bytes made, " << written
        << " written";
    std::cout << "[slow consumer] paused after " << dispatched << " of " << commands.size()
              << " commands: " << generated << " reply bytes made, " << written << " written, "
              << buffered << " held (cap " << kBackpressureBytes << ")\n";
  }

  watchdog.Beat("slow consumer: another client");
  int other_wrong = 0;
  for (int i = 0; i < kOtherCommands && other_wrong == 0; i += 2) {
    const std::string k = "other" + std::to_string(i);
    const auto replies = Client().Pipeline({{"SET", k, std::to_string(i)}, {"GET", k}});
    const bool right = replies.size() == 2 && replies[0].IsOk() && replies[1].IsBulk() &&
                       replies[1].String() == std::to_string(i);
    EXPECT_TRUE(right) << "the other client's command " << i << " failed";
    if (!right) ++other_wrong;
    watchdog.Tick();
  }

  watchdog.Beat("slow consumer: reading");
  int wrong = 0;
  std::string first_wrong;
  for (size_t i = 0; i < commands.size(); ++i) {
    const Reply reply = slow.Read();
    watchdog.Tick();
    const bool echo = i % 2 == 1;
    const std::string want = echo ? std::to_string(i / 2) : big;
    if (reply.IsBulk() && reply.String() == want) continue;
    if (wrong++ == 0) first_wrong = "reply " + std::to_string(i) + ": " + reply.String();
    if (reply.IsError() && reply.String() == "connection lost") break;
  }
  writer.join();
  EXPECT_TRUE(sent);
  EXPECT_EQ(wrong, 0) << "first: " << first_wrong;
  const std::optional<NetState> after = ReadNet(port);
  ASSERT_TRUE(after.has_value());
  EXPECT_EQ(after->closed_backpressure, before->closed_backpressure);
}

}  // namespace
}  // namespace abyss::system_test
