#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

#include "abyss/net/tcp_server.h"
#include "abyss/resp/command_registry.h"
#include "fake_poller.h"
#include "reactor.h"
#include "stub_dispatcher.h"

namespace abyss::net {
namespace {

// The reactors built here are never registered with the server, so the server
// only supplies configuration and the stop flag; no socket is ever bound.
TcpServerConfig HostConfig() {
  TcpServerConfig c;
  c.bind = "127.0.0.1";
  c.port = 0;
  c.io_threads = 1;
  c.shutdown_grace = std::chrono::seconds{1};
  c.reaper_tick = std::chrono::milliseconds{5};
  return c;
}

template <class Pred>
bool SpinUntil(Pred pred, std::chrono::milliseconds budget) {
  const auto deadline = std::chrono::steady_clock::now() + budget;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  return pred();
}

// XCONC-7: destroying a Reactor in the window between Start() spawning the
// worker and the worker setting running_ must still join the joinable thread.
// Gating the join on running_ let ~thread run on a joinable thread, which is a
// std::terminate. Repeated so the race window is hit rather than hoped for.
TEST(ReactorLifecycleTest, ReactorDestroyedBeforeRunDoesNotTerminate) {
  for (int i = 0; i < 32; ++i) {
    testing::StubDispatcher dispatcher;
    TcpServer host(HostConfig(), resp::GlobalRegistry(),
                   resp::PipelineDependencies{.dispatcher = &dispatcher});
    Reactor reactor(/*id=*/0, host, std::make_unique<testing::FakePoller>(),
                    /*is_acceptor=*/false);
    reactor.Start();
    // No Join(): the destructor is the only thing that can reap the worker.
  }
  SUCCEED();
}

// The started_ flag added for the destructor must not change what IsRunning()
// reports: TcpServer and /ready observe the worker's own running_.
TEST(ReactorLifecycleTest, IsRunningKeepsWorkerSemantics) {
  testing::StubDispatcher dispatcher;
  TcpServer host(HostConfig(), resp::GlobalRegistry(),
                 resp::PipelineDependencies{.dispatcher = &dispatcher});
  Reactor reactor(/*id=*/0, host, std::make_unique<testing::FakePoller>(),
                  /*is_acceptor=*/false);

  EXPECT_FALSE(reactor.IsRunning());
  reactor.Start();
  EXPECT_TRUE(SpinUntil([&reactor] { return reactor.IsRunning(); }, std::chrono::seconds{5}));

  host.RequestStop();
  EXPECT_TRUE(SpinUntil([&reactor] { return !reactor.IsRunning(); }, std::chrono::seconds{5}));

  // Joining explicitly leaves the destructor with a non-joinable thread; it
  // must tolerate that too.
  reactor.Join();
}

}  // namespace
}  // namespace abyss::net
