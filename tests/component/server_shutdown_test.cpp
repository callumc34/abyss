#include <gtest/gtest.h>

#include "server.h"

#ifdef ABYSS_HAVE_ROCKSDB

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "abyss/config/config.h"
#include "abyss/core/durability.h"
#include "abyss/core/result.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/queue/wal_queue.h"
#include "abyss/resp/serializer.h"
#include "cold_gate.h"
#include "hooked_cold_store.h"
#include "on_exit.h"
#include "sync_redis_client.h"
#include "temp_dir.h"

namespace abyss::server {
namespace {

using namespace std::chrono_literals;

constexpr uint32_t kShards = 4;

std::string Command(std::initializer_list<std::string> args) {
  const auto bytes = resp::Serializer::SerializeCommand(core::RespCommand{.args = args});
  return {bytes.begin(), bytes.end()};
}

double OrphanGauge() {
  return metrics::testing::GetGaugeValue(metrics::names::kNetOrphans).value_or(0);
}
double AbandonedAtShutdown() {
  return metrics::testing::GetCounterValue(metrics::names::kNetCommandsAbandonedTotal,
                                           metrics::AbandonReason::kShutdown)
      .value_or(0);
}
double ClosedAtShutdown() {
  return metrics::testing::GetCounterValue(metrics::names::kNetConnectionsClosedTotal,
                                           metrics::CloseReason::kServerShutdown)
      .value_or(0);
}

template <typename Done>
bool PollUntil(const Done& done, std::chrono::milliseconds budget = 30s) {
  const auto until = std::chrono::steady_clock::now() + budget;
  while (!done()) {
    if (std::chrono::steady_clock::now() >= until) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

// Holds every WAL flush until released.
class FlushHold {
 public:
  queue::FlushHook Hook() {
    return [this](uint32_t) -> core::Result<void> {
      std::unique_lock lock(mu_);
      cv_.wait(lock, [this] { return released_; });
      return {};
    };
  }
  void Release() {
    {
      const std::scoped_lock lock(mu_);
      released_ = true;
    }
    cv_.notify_all();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool released_ = false;
};

config::Config ServerConfig(const abyss::testing::TempDir& dir) {
  config::Config config;
  config.queue.wal_path = dir.Sub("wal").string();
  config.queue.segment_size_bytes = size_t{1} << 20U;
  config.queue.durability = core::Durability::kPowerLoss;
  config.cold.data_path = dir.Sub("cold").string();
  config.hot.shard_count = kShards;
  config.hot.max_memory_bytes = size_t{64} << 20U;
  config.net.bind = "127.0.0.1";
  config.net.port = 0;
  config.net.io_threads = 1;
  // Straight to closing the connections: what is in flight stays so.
  config.net.shutdown_grace = 0s;
  config.engine.command_timeout = 2s;
  config.engine.cold_io_threads = 2;
  config.admin.enabled = false;
  config.metrics.bind = "127.0.0.1";
  config.metrics.port = 0;
  return config;
}

// The server's own stop, Server::Shutdown, with work of every kind in
// flight: a cold read pending, a power_loss write sequenced and waiting
// to be durable, and a read its deadline gave up whose cold read is
// still running. Each is held until the stop has closed the
// connections, then let go as the engine stops. The stop ends clean,
// with no orphan left.
TEST(ServerShutdownTest, AStopWithWorkOfEveryKindInFlightEndsClean) {
  const abyss::testing::TempDir dir("server-stop");
  engine::ColdKeyGate gate;
  std::unique_ptr<abyss::testing::HookedColdStore> hooked;
  ServerTestHooks hooks{
      .loader_cold = [&gate, &hooked](core::ColdStore& cold) -> core::ColdStore& {
        hooked = std::make_unique<abyss::testing::HookedColdStore>(
            [&cold]() -> core::ColdStore& { return cold; });
        hooked->SetHook([&gate](std::string_view key) { gate.Hook(key); });
        return *hooked;
      },
  };
  FlushHold flushes;

  Server server(ServerConfig(dir), std::move(hooks));
  ASSERT_TRUE(server.Initialize());
  std::atomic<bool> stop{false};
  core::Result<void> ran;
  std::thread runner([&server, &stop, &ran] { ran = server.Run(stop); });
  // On a failed assertion: let go of what is held, then stop.
  const abyss::testing::OnExit join([&stop, &runner] {
    stop.store(true);
    if (runner.joinable()) runner.join();
  });
  const abyss::testing::OnExit release([&gate, &flushes] {
    gate.ReleaseAll();
    flushes.Release();
  });
  ASSERT_TRUE(PollUntil([&server] { return server.IsServing(); }));
  const uint16_t port = server.RespBoundPort();
  const double orphans = OrphanGauge();
  const double closed = ClosedAtShutdown();
  const double abandoned = AbandonedAtShutdown();

  gate.Hold("gone");
  gate.Hold("pending");
  component_test::SyncRedisClient timed_out;
  component_test::SyncRedisClient pending;
  component_test::SyncRedisClient sequenced;
  ASSERT_TRUE(timed_out.Connect(port));
  ASSERT_TRUE(pending.Connect(port));
  ASSERT_TRUE(sequenced.Connect(port));

  ASSERT_TRUE(timed_out.SendRaw(Command({"GET", "gone"})));
  ASSERT_TRUE(gate.AwaitBlocked(1));
  EXPECT_EQ(timed_out.ReadReply(30s), "-ERR read timed out loading its key\r\n");
  ASSERT_TRUE(PollUntil([orphans] { return OrphanGauge() == orphans + 1; }))
      << "its task waits, orphaned, for its cold read";

  ASSERT_TRUE(pending.SendRaw(Command({"GET", "pending"})));
  ASSERT_TRUE(gate.AwaitBlocked(2));

  server.QueueForTesting().SetFlushHookForTesting(flushes.Hook());
  ASSERT_TRUE(sequenced.SendRaw(Command({"SET", "k", "v"})));
  const core::ShardId shard = core::ComputeShard("k", kShards);
  ASSERT_TRUE(PollUntil([&server, shard] {
    return server.QueueForTesting().DurableEnd(shard, core::Durability::kProcessCrash).value_or(0) >
           1;
  })) << "the SET never sequenced";

  stop.store(true);
  ASSERT_TRUE(PollUntil([closed] { return ClosedAtShutdown() == closed + 3; }))
      << "the stop never closed the connections";
  gate.ReleaseAll();
  flushes.Release();
  runner.join();

  ASSERT_TRUE(ran.has_value()) << ran.error().message();
  EXPECT_EQ(server.Lifecycle(), Server::LifecycleState::kStopped);
  EXPECT_EQ(OrphanGauge(), orphans);
  // The pending read is given up by the stop unless its own deadline
  // passed first.
  RecordProperty("shutdown_abandons", std::to_string(AbandonedAtShutdown() - abandoned));
}

}  // namespace
}  // namespace abyss::server

#endif  // ABYSS_HAVE_ROCKSDB
