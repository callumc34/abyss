#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/predicate.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/sequencer.h"
#include "abyss/hot/sharded_hot_store.h"
#include "mock_cold_store.h"
#include "mock_queue.h"

namespace abyss::engine::testing {

// No buffered deltas, and a drained seq the test moves.
class DrainRouter : public consumer::CompactionBufferRouter {
 public:
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& /*op*/,
                                     std::optional<core::Duration> /*deadline*/) override {
    return std::unexpected(core::Error{core::ErrorCode::kNotFound, ""});
  }
  core::Result<core::RespValue> Read(std::string_view /*key*/) const override {
    return std::unexpected(core::Error{core::ErrorCode::kNotFound, ""});
  }
  consumer::BufferKeyPresence Probe(std::string_view /*key*/) const override {
    return consumer::BufferKeyPresence::kAbsent;
  }
  consumer::HashOverlay HashOverlayFor(std::string_view /*key*/) const override { return {}; }
  std::optional<consumer::CompactedState> Snapshot(core::ShardId /*shard*/,
                                                   std::string_view /*key*/) const override {
    return std::nullopt;
  }
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId target_seq,
                         std::chrono::milliseconds timeout) override {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return drained_ >= target_seq; });
  }

  void Advance(core::SequenceId to) {
    {
      const std::scoped_lock lock(mu_);
      drained_ = to;
    }
    cv_.notify_all();
  }
  core::SequenceId Drained() const {
    const std::scoped_lock lock(mu_);
    return drained_;
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  core::SequenceId drained_ = 0;
};

// A real hot store and loader over an in-memory queue and a cold map,
// with a wall clock the test sets.
class SequencerFixture : public ::testing::Test {
 protected:
  static constexpr uint32_t kShards = 4;

  struct Options {
    size_t max_memory_bytes = size_t{64} << 20;
    double stub_memory_fraction = 0.02;
    uint32_t log_count = 1;
    std::chrono::milliseconds write_timeout{2000};
    // Eviction waits for the router's drained seq; otherwise all is.
    bool drain_gated = false;
  };

  SequencerFixture() {
    wall_ms_.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                       core::WallClock::now().time_since_epoch())
                       .count());
    using ::testing::_;
    ON_CALL(cold_, LoadKey(_, _))
        .WillByDefault([this](std::string_view key,
                              core::SteadyTime) -> core::Result<std::optional<core::ColdKeyState>> {
          std::function<void(std::string_view)> hook;
          {
            const std::scoped_lock lock(cold_mu_);
            ++loads_;
            hook = on_load_;
          }
          if (hook) hook(key);
          const std::scoped_lock lock(cold_mu_);
          const auto it = cold_keys_.find(std::string(key));
          if (it == cold_keys_.end()) return std::optional<core::ColdKeyState>{};
          return std::optional<core::ColdKeyState>{it->second};
        });
    ON_CALL(cold_, ProbeKey(_, _))
        .WillByDefault([this](std::string_view key,
                              core::SteadyTime) -> core::Result<std::optional<core::KeyMeta>> {
          const std::scoped_lock lock(cold_mu_);
          ++probes_;
          const auto it = cold_keys_.find(std::string(key));
          if (it == cold_keys_.end()) return std::optional<core::KeyMeta>{};
          return std::optional<core::KeyMeta>{core::KeyMeta{
              .type = it->second.type, .abs_ttl_ms = it->second.abs_ttl_ms, .cardinality = 1}};
        });
    Build(Options{});
  }

  void Build(Options options) {
    options_ = options;
    queue_.SetLogCount(options.log_count);
    sequencer_.reset();
    loader_.reset();
    hot_.reset();
    hot::ShardedHotStoreConfig config{
        .max_memory_bytes = options.max_memory_bytes,
        .shard_count = kShards,
        .stub_memory_fraction = options.stub_memory_fraction,
        .wall_clock = [this] { return Wall(); },
    };
    if (options.drain_gated) {
      config.drained = [this](core::ShardId) { return router_.Drained(); };
    }
    hot_ = std::make_unique<hot::ShardedHotStore>(std::move(config));
    loader_ = std::make_unique<Loader>(*hot_, router_, cold_, [this] { return Wall(); });
    sequencer_ =
        std::make_unique<Sequencer>(*hot_, queue_, *loader_, router_,
                                    SequencerConfig{
                                        .write_timeout = options.write_timeout,
                                        .backpressure_wait_slice = std::chrono::milliseconds{5},
                                        .wall_clock = [this] { return WallRead(); },
                                    });
  }

  core::WallTime Wall() const { return core::WallTime{std::chrono::milliseconds{wall_ms_.load()}}; }
  // The sequencer's own reads go through here, so a test can step them.
  core::WallTime WallRead() { return wall_read_ ? wall_read_() : Wall(); }
  int64_t NowMs() const { return wall_ms_.load(); }
  void SetNowMs(int64_t ms) { wall_ms_.store(ms); }

  core::Result<core::RespValue> Run(std::vector<std::string> args,
                                    core::PredicateFlags flags = core::PredicateFlags::kNone) {
    return sequencer_->Execute(core::RespCommand{.args = std::move(args)}, flags);
  }
  // A reply, failing the test on an error Result.
  std::string Reply(std::vector<std::string> args,
                    core::PredicateFlags flags = core::PredicateFlags::kNone) {
    auto result = Run(std::move(args), flags);
    if (!result.has_value()) return "error: " + result.error().message();
    return Describe(*result);
  }
  static std::string Describe(const core::RespValue& value) {
    if (value.IsNull()) return "nil";
    if (value.IsInteger()) return ":" + std::to_string(value.AsInteger());
    if (value.IsError()) return "-" + value.AsString();
    return value.AsString();
  }

  // Every entry published on `shard`, as argument vectors.
  std::vector<std::vector<std::string>> Logged(core::ShardId shard) const {
    std::vector<std::vector<std::string>> out;
    for (const auto& entry : queue_.Published(shard)) {
      if (const auto* write = std::get_if<core::entry::Write>(&entry.payload)) {
        out.push_back(write->cmd.args);
      } else {
        out.push_back({"<flush>"});
      }
    }
    return out;
  }
  size_t PublishedCount() const {
    size_t total = 0;
    for (core::ShardId shard = 0; shard < kShards; ++shard) {
      total += queue_.Published(shard).size();
    }
    return total;
  }

  // The `n`th key that hashes to `shard`.
  static std::string KeyOn(core::ShardId shard, int n = 0, std::string_view prefix = "key") {
    for (int i = 0;; ++i) {
      std::string key = std::string(prefix) + std::to_string(i);
      if (core::ComputeShard(key, kShards) == shard && n-- == 0) return key;
    }
  }
  static core::ShardId ShardOf(std::string_view key) { return core::ComputeShard(key, kShards); }

  void PutCold(const std::string& key, core::ColdKeyState state) {
    const std::scoped_lock lock(cold_mu_);
    cold_keys_[key] = std::move(state);
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  Options options_;
  std::atomic<int64_t> wall_ms_{0};
  std::function<core::WallTime()> wall_read_;
  ::testing::NiceMock<abyss::testing::MockQueue> queue_;
  ::testing::NiceMock<abyss::testing::MockColdStore> cold_;
  DrainRouter router_;
  std::mutex cold_mu_;
  std::map<std::string, core::ColdKeyState> cold_keys_;
  int loads_ = 0;
  int probes_ = 0;
  std::function<void(std::string_view)> on_load_;
  std::unique_ptr<hot::ShardedHotStore> hot_;
  std::unique_ptr<Loader> loader_;
  std::unique_ptr<Sequencer> sequencer_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

}  // namespace abyss::engine::testing
