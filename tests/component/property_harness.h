#pragma once

// What the property tests share: a fake wall clock every component
// reads, the server's request pipeline over the engine, a cold store
// whose loads a test can interleave with, and the three-way check of
// one key's state (hot, then buffer plus cold) against the model.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <variant>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/platform/fs.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/request_pipeline.h"
#include "hooked_cold_store.h"
#include "redis_model.h"
#include "sequenced_engine_fixture.h"

namespace abyss::engine {

using abyss::testing::RedisModel;

// ASan, TSan and Windows' per-ack FlushFileBuffers slow every step
// 5 to 15 times; there the bounded tiers shrink by this.
#ifdef ABYSS_PROPERTY_REDUCED
inline constexpr int kReducedDivisor = 8;
#else
inline constexpr int kReducedDivisor = 1;
#endif

// A deterministic generator: the seed alone fixes every choice.
class Rng {
 public:
  // The seed is mixed first: splitmix64 streams from seeds a step
  // apart are one stream shifted by a draw.
  explicit Rng(uint64_t seed) : state_(Mix(seed)) {}

  // splitmix64.
  uint64_t Next() { return Mix(state_ += kGolden); }
  uint64_t Below(uint64_t n) { return n == 0 ? 0 : Next() % n; }
  int64_t Between(int64_t lo, int64_t hi) {
    return lo + static_cast<int64_t>(Below(static_cast<uint64_t>(hi - lo + 1)));
  }
  bool Chance(double p) { return static_cast<double>(Next() >> 11U) * 0x1.0p-53 < p; }

 private:
  static constexpr uint64_t kGolden = 0x9E3779B97F4A7C15ULL;
  static uint64_t Mix(uint64_t z) {
    z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31U);
  }

  uint64_t state_;
};

inline std::optional<uint64_t> EnvNumber(const char* name) {
  // NOLINTNEXTLINE(concurrency-mt-unsafe): read before any thread starts.
  const char* text = std::getenv(name);
  if (text == nullptr || *text == '\0') return std::nullopt;
  return std::strtoull(text, nullptr, 10);
}

// What a reply says about its command's effect: an error by its exact
// text.
enum class ReplyKind : uint8_t {
  // A reply the model can give: compared.
  kDefinite,
  // Timed out in the durable wait, after it was logged and applied:
  // took effect inside its window, or not at all.
  kIndeterminate,
  // Timed out or refused before anything was reserved: no effect.
  kNoOp,
  // Any other error.
  kUnexpected,
};

// The text after an error's prefix.
inline std::string_view ErrorMessage(const core::RespValue& reply) {
  const std::string_view text = reply.AsString();
  const size_t space = text.find(' ');
  return space == std::string_view::npos ? std::string_view{} : text.substr(space + 1);
}

inline bool IsWrite(std::string_view name) {
  static constexpr auto kWrites = std::to_array<std::string_view>(
      {"SET",    "SETEX",   "PSETEX",   "SETNX",     "MSET",    "MSETNX",   "DEL",  "UNLINK",
       "SADD",   "SREM",    "ZADD",     "ZREM",      "HSET",    "HMSET",    "HDEL", "HSETNX",
       "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "RENAMENX", "COPY", "FLUSHDB"});
  return std::ranges::find(kWrites, name) != kWrites.end();
}

// A cold load that ran out of its deadline: the cold store's check, the
// RocksDB read's own, or a wait on another's load of the key.
inline bool LoadTimedOut(std::string_view message) {
  if (message == "cold load deadline passed" ||
      message == "timed out awaiting another load of the key") {
    return true;
  }
  return std::ranges::any_of(
      std::to_array<std::string_view>({"probe key", "load scan", "load members"}),
      [message](std::string_view read) {
        return message.starts_with(read) &&
               message.substr(read.size()).starts_with(": Operation timed out");
      });
}

// `write`: the command is a write, whose loads run on its deadline
// before anything is reserved; a read's run on cold's own, which a
// healthy store meets.
inline ReplyKind Classify(const core::RespValue& reply, bool write) {
  if (!reply.IsError()) return ReplyKind::kDefinite;
  const std::string_view text = reply.AsString();
  const std::string_view message = ErrorMessage(reply);
  if (text.starts_with("WRONGTYPE ") || text.starts_with("CROSSSLOT ") ||
      text == "ERR no such key" || text == "ERR source and destination objects are the same") {
    return ReplyKind::kDefinite;
  }
  if (message ==
          "write durable wait exceeded server timeout; the write is applied and may yet become "
          "durable" ||
      message == "flush durable wait exceeded server timeout; retry to complete the wipe") {
    return ReplyKind::kIndeterminate;
  }
  static constexpr auto kNoOps = std::to_array<std::string_view>({
      "durable wait exceeded server timeout; the reply would show a write not yet durable",
      "write timed out waiting for a spare WAL segment",
      "flush timed out waiting for a spare WAL segment",
      "write timed out loading its keys",
      "write timed out awaiting a load of its key",
      "command not allowed when hot memory is over its limit and cold is behind",
  });
  if (std::ranges::find(kNoOps, message) != kNoOps.end()) return ReplyKind::kNoOp;
  // Admission's, bare once its deadline has passed, else with the
  // window's numbers after this.
  if (message == "WAL durability window full" ||
      message.starts_with("WAL durability window full: the device is not keeping up")) {
    return ReplyKind::kNoOp;
  }
  if (write && LoadTimedOut(message)) return ReplyKind::kNoOp;
  return ReplyKind::kUnexpected;
}

// A healthy cold store answers well inside these, so a cold read that
// runs out of one is a failure, not a flake.
inline void PatientColdReads(Options& options) {
  options.cold_read_deadline = std::chrono::seconds(5);
  options.cold_scan_deadline = std::chrono::seconds(5);
}

// The configuration a seed fixes, swarm style.
struct Swarm {
  Options options;
  // Absorb a shard nearing its backpressure limit before each write;
  // without it writes meet -OOM, on a short write timeout.
  bool relieve = true;
  std::string text;
};

inline Swarm SwarmFor(uint64_t seed) {
  Rng rng(seed ^ 0xC0FFEEULL);
  Options o;
  constexpr std::array<uint32_t, 4> kShards = {1, 2, 4, 8};
  o.shards = kShards.at(rng.Below(kShards.size()));
  o.log_count = o.shards >= 2 && rng.Chance(0.35) ? 2 : 1;
  o.durability = rng.Chance(0.5) ? core::Durability::kPowerLoss : core::Durability::kProcessCrash;
  o.segment_size_bytes = rng.Chance(0.5) ? size_t{16} << 10 : size_t{64} << 10;
  constexpr std::array<size_t, 3> kMemory = {size_t{16} << 10, size_t{64} << 10, size_t{4} << 20};
  o.hot_memory_bytes = kMemory.at(rng.Below(kMemory.size()));
  o.fill_doorkeeper = rng.Chance(0.5);
  o.stub_memory_fraction = rng.Chance(0.3) ? 0.0 : 0.02;
  constexpr std::array<uint64_t, 3> kFillMembers = {2, 8, 1024};
  o.fill_max_members = kFillMembers.at(rng.Below(kFillMembers.size()));
  constexpr std::array<double, 3> kRatio = {1.1, 1.25, 2.0};
  o.backpressure_ratio = kRatio.at(rng.Below(kRatio.size()));
  PatientColdReads(o);
  o.run = "seed" + std::to_string(seed) + "-";
  // Persisted offsets let retention reclaim segments, so a restart
  // replays a log whose head is gone.
  if (rng.Chance(0.5)) o.offset_fsync_interval = std::chrono::milliseconds(10);
  const bool relieve = !rng.Chance(0.25);
  if (!relieve) {
    // Small shards, so writes outrun cold and meet -OOM.
    o.write_timeout = std::chrono::milliseconds(80);
    o.hot_memory_bytes = size_t{16} << 10;
    o.shards = std::max<uint32_t>(o.shards, 4);
    o.log_count = 1;
  }
  std::ostringstream text;
  text << "shards=" << o.shards << " logs=" << o.log_count
       << " durability=" << core::DurabilityName(o.durability)
       << " segment=" << o.segment_size_bytes << " hot=" << o.hot_memory_bytes
       << " doorkeeper=" << o.fill_doorkeeper << " stubs=" << o.stub_memory_fraction
       << " fill_max_members=" << o.fill_max_members << " ratio=" << o.backpressure_ratio
       << " relieve=" << relieve << " offsets_ms=" << o.offset_fsync_interval.count();
  return {.options = o, .relieve = relieve, .text = text.str()};
}

class PropertyHarness : public SequencedEngineTest {
 public:
  PropertyHarness() {
#ifndef _WIN32
    // Power loss is simulated on the files; the device barrier only
    // slows each power_loss ack by milliseconds on macOS.
    platform::fs::testing::SetFullFsyncForTesting([](int) { return 0; });
#endif
  }
  ~PropertyHarness() override {
#ifndef _WIN32
    platform::fs::testing::SetFullFsyncForTesting(nullptr);
#endif
  }
  PropertyHarness(const PropertyHarness&) = delete;
  PropertyHarness& operator=(const PropertyHarness&) = delete;
  PropertyHarness(PropertyHarness&&) = delete;
  PropertyHarness& operator=(PropertyHarness&&) = delete;

 protected:
  // A failing test's stores stay on disk, renamed, for a post-mortem.
  void TearDown() override {
    CloseAll();
    if (!HasFailure()) return;
    const auto kept = dir_.Path().string() + "-kept";
    std::error_code ec;
    std::filesystem::rename(dir_.Path(), kept, ec);
    if (!ec) std::cout << "[property] kept the failing test's stores at " << kept << '\n';
  }

  // Opens with every clock on now_ms_.
  void OpenAt(Options options, int64_t start_ms) {
    now_ms_ = start_ms;
    wall_ = [this] { return Wall(); };
    hot_clocks_ = Clocks{.wall = [this] { return Wall(); }};
    loader_cold_ = &hooked_;
    Open(std::move(options));
    Wire();
  }
  // Recovers stores left by another process, every clock on now_ms_.
  core::Result<void> RecoverAt(Options options, int64_t start_ms) {
    now_ms_ = start_ms;
    wall_ = [this] { return Wall(); };
    loader_cold_ = &hooked_;
    options_ = std::move(options);
    return RestartAt();
  }
  core::Result<void> RestartAt() {
    auto restarted = Restart(Clocks{.wall = [this] { return Wall(); }});
    Wire();
    return restarted;
  }
  void Wire() {
    pipeline_ = std::make_unique<resp::RequestPipeline>(
        resp::GlobalRegistry(), resp::ConnectionState{},
        resp::PipelineDependencies{.dispatcher = engine_.get()});
  }
  core::WallTime Wall() const {
    return core::WallTime(std::chrono::milliseconds(now_ms_.load(std::memory_order_relaxed)));
  }

  core::RespValue Run(const std::vector<std::string>& args) {
    return pipeline_->Dispatch(core::RespCommand{.args = args});
  }

  // Absorbs `shard`'s log into its buffer as far as it is durable at
  // the ack class, which the drain reads at.
  void Absorb(core::ShardId shard, size_t max_count = SIZE_MAX) {
    const auto end = queue_->DurableEnd(shard, core::Durability::kProcessCrash);
    if (!end.has_value() || *end <= core::kFirstSeq) return;
    const auto durable = queue_->AwaitDurable(shard, *end - 1, queue_->AckDurability(), 5s);
    EXPECT_TRUE(durable.has_value() && *durable) << "shard " << shard << " never became durable";
    auto& consumer = pool_->ConsumerFor(shard);
    size_t taken = 0;
    for (int round = 0; round < 1000 && taken < max_count; ++round) {
      if (consumer.LatestDrainedSeq() + 1 >= *end) return;
      const size_t got = consumer.DrainWithBatch(std::min<size_t>(max_count - taken, 4096));
      if (got == 0) return;
      taken += got;
    }
  }
  void FlushCold(core::ShardId shard) { (void)pool_->ConsumerFor(shard).FlushUnscheduled(); }

  // Absorbs each of `keys`' shards over its backpressure limit, so a
  // write never waits for a drain nobody else drives.
  void Relieve(const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
      const core::ShardId shard = ShardOf(key);
      const auto memory = hot_->Memory(shard);
      if (memory.used_bytes * 10 > memory.limit_bytes * 8) Absorb(shard);
    }
  }
  void RelieveAll() {
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
      const auto memory = hot_->Memory(shard);
      if (memory.used_bytes * 10 > memory.limit_bytes * 8) Absorb(shard);
    }
  }

  static RedisModel::Type ModelType(hot::Entry::Type type) {
    switch (type) {
      case hot::Entry::Type::kString:
        return RedisModel::Type::kString;
      case hot::Entry::Type::kSet:
        return RedisModel::Type::kSet;
      case hot::Entry::Type::kZset:
        return RedisModel::Type::kZset;
      case hot::Entry::Type::kHash:
        return RedisModel::Type::kHash;
    }
    return RedisModel::Type::kString;
  }

  // A hot value as the model holds it; a zset whose two indexes
  // disagree is reported as an error.
  static std::variant<RedisModel::Value, std::string> FromHot(const hot::Value& value,
                                                              int64_t abs_ttl_ms) {
    RedisModel::Value out{.abs_ttl_ms = abs_ttl_ms};
    std::string error;
    std::visit(
        [&out, &error](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::string>) {
            out.type = RedisModel::Type::kString;
            out.str = v;
          } else if constexpr (std::is_same_v<T, hot::SetValue>) {
            out.type = RedisModel::Type::kSet;
            out.set.insert(v.members.begin(), v.members.end());
          } else if constexpr (std::is_same_v<T, hot::HashValue>) {
            out.type = RedisModel::Type::kHash;
            out.hash.insert(v.fields.begin(), v.fields.end());
          } else {
            out.type = RedisModel::Type::kZset;
            out.zset.insert(v.member_scores.begin(), v.member_scores.end());
            size_t indexed = 0;
            for (const auto& [score, members] : v.score_members) {
              for (const auto& member : members) {
                ++indexed;
                const auto it = v.member_scores.find(member);
                if (it == v.member_scores.end() || it->second != score) {
                  error = "zset score index disagrees at " + member;
                }
              }
            }
            if (indexed != v.member_scores.size()) error = "zset score index size disagrees";
          }
        },
        value);
    if (!error.empty()) return error;
    return out;
  }

  static std::string Show(const RedisModel::Value* value) {
    return value == nullptr ? "absent" : RedisModel::Describe(*value);
  }

  enum class Where : uint8_t { kHot, kStub, kBufferCold };
  struct Seen {
    Where where = Where::kHot;
    std::optional<std::string> mismatch;
  };

  // Hot's state of `key` against the model at now; buffer plus cold's
  // when hot does not hold it (the residency invariant says they then
  // hold its whole state).
  Seen CheckKey(const std::string& key) {
    const int64_t now = now_ms_;
    const RedisModel::Value* want = model_.Find(key, now);
    const core::ShardId shard = ShardOf(key);
    const auto differs = [&key, want](std::string_view tier, const std::string& got) {
      return "key " + key + " in " + std::string(tier) + ": got " + got + ", model " + Show(want);
    };
    {
      auto locks = hot_->LockExclusive(std::vector<core::ShardId>{shard});
      const hot::KeyView view = locks.View(key, static_cast<uint64_t>(now));
      using Presence = hot::KeyView::Presence;
      switch (view.presence) {
        case Presence::kLive: {
          auto got = FromHot(*view.value, view.abs_ttl_ms);
          if (const auto* error = std::get_if<std::string>(&got)) {
            return {.mismatch = differs("hot", *error)};
          }
          const auto& value = std::get<RedisModel::Value>(got);
          if (ModelType(view.type) != value.type) {
            return {.mismatch = differs("hot", "an entry typed apart from its value")};
          }
          if (want == nullptr || !(*want == value)) {
            return {.mismatch = differs("hot", RedisModel::Describe(value))};
          }
          return {};
        }
        case Presence::kTombstoned:
        case Presence::kExpired:
          if (want != nullptr) {
            return {.mismatch =
                        differs("hot", view.flush_floor ? "flushed" : "deleted or expired")};
          }
          return {};
        case Presence::kStub:
          if (want == nullptr || want->type != ModelType(view.type) ||
              want->abs_ttl_ms != view.abs_ttl_ms) {
            return {.where = Where::kStub,
                    .mismatch = differs(
                        "a stub", "type " + std::to_string(static_cast<int>(ModelType(view.type))) +
                                      " ttl " + std::to_string(view.abs_ttl_ms))};
          }
          break;
        case Presence::kNonResident:
          break;
      }
    }
    auto loaded =
        loader_->Load(shard, key, Need::kState, core::SteadyClock::now() + std::chrono::seconds(5));
    if (!loaded.has_value()) {
      return {.where = Where::kBufferCold,
              .mismatch = differs("buffer+cold", "error " + loaded.error().message())};
    }
    Seen seen{.where = Where::kBufferCold};
    if (std::holds_alternative<hot::LoadedAbsent>(*loaded)) {
      if (want != nullptr)
        return {.where = seen.where, .mismatch = differs("buffer+cold", "absent")};
      return seen;
    }
    if (std::holds_alternative<hot::LoadedExists>(*loaded)) {
      return {.where = seen.where, .mismatch = differs("buffer+cold", "only its meta")};
    }
    const auto& full = std::get<hot::LoadedFull>(*loaded);
    if (full.abs_ttl_ms != 0 && now >= full.abs_ttl_ms) {
      if (want != nullptr) {
        return {.where = seen.where, .mismatch = differs("buffer+cold", "expired")};
      }
      return seen;
    }
    auto got = FromHot(full.value, full.abs_ttl_ms);
    if (const auto* error = std::get_if<std::string>(&got)) {
      return {.where = seen.where, .mismatch = differs("buffer+cold", *error)};
    }
    const auto& value = std::get<RedisModel::Value>(got);
    if (want == nullptr || !(*want == value)) {
      return {.where = seen.where, .mismatch = differs("buffer+cold", RedisModel::Describe(value))};
    }
    return seen;
  }

  // The read a key's whole state answers through the server.
  static std::vector<std::string> FullRead(const std::string& key, const RedisModel::Value* want) {
    if (want == nullptr) return {"GET", key};
    switch (want->type) {
      case RedisModel::Type::kString:
        return {"GET", key};
      case RedisModel::Type::kSet:
        return {"SMEMBERS", key};
      case RedisModel::Type::kZset:
        return {"ZRANGE", key, "0", "-1", "WITHSCORES"};
      case RedisModel::Type::kHash:
        return {"HGETALL", key};
    }
    return {"GET", key};
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::atomic<int64_t> now_ms_{0};
  RedisModel model_{[this](std::span<const std::string> keys) {
    return std::ranges::any_of(keys, [this, &keys](const std::string& key) {
      return queue_->LogOf(ShardOf(key)) != queue_->LogOf(ShardOf(keys.front()));
    });
  }};
  abyss::testing::HookedColdStore hooked_{[this]() -> core::ColdStore& { return *cold_; }};
  std::unique_ptr<resp::RequestPipeline> pipeline_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

}  // namespace abyss::engine
