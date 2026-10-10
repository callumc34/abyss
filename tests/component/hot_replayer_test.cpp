#include "abyss/engine/hot_replayer.h"

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "sequenced_engine_fixture.h"

namespace abyss::engine {
namespace {

core::QueueEntry Frame(std::vector<std::string> args, bool replaces_state,
                       core::WallTime at = core::WallClock::now()) {
  return core::QueueEntry{
      .appended_at = at,
      .payload = core::entry::Write{.cmd = core::RespCommand{.args = std::move(args)}},
      .replaces_state = replaces_state,
  };
}

core::QueueEntry FlushFrame() {
  return core::QueueEntry{.appended_at = core::WallClock::now(), .payload = core::entry::Flush{}};
}

core::WallTime AtMs(int64_t ms) { return core::WallTime{std::chrono::milliseconds{ms}}; }

// A value in a canonical order, so equal states print equal.
std::string Canonical(const hot::Value& value) {
  return std::visit(
      [](const auto& v) -> std::string {
        using T = std::decay_t<decltype(v)>;
        std::vector<std::string> parts;
        if constexpr (std::is_same_v<T, std::string>) {
          return v;
        } else if constexpr (std::is_same_v<T, hot::SetValue>) {
          for (const auto& member : v.members) parts.push_back(member);
        } else if constexpr (std::is_same_v<T, hot::HashValue>) {
          for (const auto& [field, val] : v.fields) {
            std::string part = field;
            part += "=";
            part += val;
            parts.push_back(std::move(part));
          }
        } else {
          for (const auto& [member, score] : v.member_scores) {
            std::string part = member;
            part += ":";
            part += std::to_string(score);
            parts.push_back(std::move(part));
          }
        }
        std::ranges::sort(parts);
        std::string out;
        for (const auto& part : parts) out += part + ",";
        return out;
      },
      value);
}

class HotReplayerRecoveryTest : public SequencedEngineTest {
 protected:
  core::SequenceId Append(core::ShardId shard, core::QueueEntry entry) {
    auto appended = queue_->Append(shard, std::move(entry));
    EXPECT_TRUE(appended.has_value()) << appended.error().message();
    return appended.has_value() ? appended->seq : 0;
  }

  // Each key's entry as hot holds it: presence, type, TTL, latest_seq
  // and value, judging no TTL.
  std::string HotState(const std::vector<std::string>& keys) {
    std::string out;
    for (const auto& key : keys) {
      auto locks = hot_->LockExclusive(std::vector<core::ShardId>{ShardOf(key)});
      const hot::KeyView view = locks.View(key, 0);
      out += key + " " + std::to_string(static_cast<int>(view.presence)) + " " +
             std::to_string(static_cast<int>(view.type)) + " " + std::to_string(view.abs_ttl_ms) +
             " " + std::to_string(view.latest_seq) + " " +
             (view.value != nullptr ? Canonical(*view.value) : "-") + "\n";
    }
    return out;
  }

  struct Replayed {
    std::string before_sweep;
    int hot_clock_reads = 0;
  };
  // Recovery as the coordinator runs it, from copies of `wal` and `cold`,
  // with the wall clock at `now_ms`, stopping to look before the sweep.
  Replayed ReplayAt(const std::filesystem::path& wal, const std::filesystem::path& cold,
                    int64_t now_ms, const std::vector<std::string>& keys) {
    CloseAll();
    std::filesystem::remove_all(dir_.Sub("wal"));
    std::filesystem::remove_all(dir_.Sub("cold"));
    std::filesystem::copy(wal, dir_.Sub("wal"), std::filesystem::copy_options::recursive);
    std::filesystem::copy(cold, dir_.Sub("cold"), std::filesystem::copy_options::recursive);
    OpenStores();
    auto reads = std::make_shared<std::atomic<int>>(0);
    hot_clocks_ = Clocks{
        .steady =
            [reads] {
              reads->fetch_add(1);
              return core::SteadyClock::now();
            },
        .wall =
            [reads, now_ms] {
              reads->fetch_add(1);
              return AtMs(now_ms);
            },
    };
    hot_ = NewHot();
    wall_ = [now_ms] { return AtMs(now_ms); };
    HotReplayer replayer(
        *hot_, [](core::ShardId, core::SequenceId) { return core::Result<void>{}; },
        HotReplayer::Config{.wall_clock = [now_ms] { return AtMs(now_ms); }});

    const uint32_t shards = options_.shards;
    std::vector<core::SequenceId> end(shards);
    std::vector<core::SequenceId> first(shards);
    std::vector<core::SequenceId> from_cold(shards);
    std::vector<core::SequenceId> from(shards);
    for (core::ShardId s = 0; s < shards; ++s) {
      end[s] = queue_->DurableEnd(s, core::Durability::kProcessCrash).value();
      first[s] = queue_->FirstSeq(s).value();
      from_cold[s] = pool_->ConsumerFor(s).BeginReplay().value();
      from[s] = std::min(first[s], from_cold[s]);
    }
    replayer.Begin(first, end);
    const std::atomic<bool> cancel{false};
    const auto by_seq = [](const core::QueueEntry& entry) { return entry.seq; };
    const core::Queue::ScanSink sink = [&](core::ShardId shard,
                                           std::vector<core::QueueEntry>& batch) {
      const std::span<const core::QueueEntry> entries(batch);
      const auto cold_at = std::ranges::lower_bound(entries, from_cold[shard], {}, by_seq);
      if (auto applied = pool_->ConsumerFor(shard).ApplyReplayBatch(
              entries.subspan(static_cast<size_t>(cold_at - entries.begin())), cancel);
          !applied.has_value()) {
        return applied;
      }
      batch.erase(batch.begin(), std::ranges::lower_bound(batch, first[shard], {}, by_seq));
      return replayer.Apply(shard, batch);
    };
    EXPECT_TRUE(queue_->Scan(from, end, 1, sink, cancel).has_value());

    Replayed out{.before_sweep = HotState(keys), .hot_clock_reads = reads->load()};
    for (core::ShardId s = 0; s < shards; ++s) {
      EXPECT_TRUE(pool_->ConsumerFor(s).FinishReplay(end[s], cancel).has_value());
    }
    EXPECT_TRUE(replayer.Finish().has_value());
    Rewire();
    return out;
  }
};

// A key whose first retained frame is an add that does not replace its
// state, its earlier history reclaimed into cold, is left out of hot:
// building it from that frame would make a partial collection. A read
// is served from buffer plus cold, which hold it whole.
TEST_F(HotReplayerRecoveryTest, AReclaimedTailKeyStaysNonResidentAndReadsWhole) {
  Open(Options{.shards = 2});
  const std::string k = KeyOn(0, 0, "set");
  const std::vector<std::string_view> earlier{"a", "b"};
  const core::ops::WriteOp reclaimed = core::ops::SetAdd{.key = k, .members = earlier};
  ASSERT_TRUE(cold_->ApplyBatch(std::span(&reclaimed, 1), 0).has_value());
  Append(0, Frame({"SADD", k, "c"}, false));
  Append(0, Frame({"SET", KeyOn(0, 1, "str"), "v"}, true));

  ASSERT_TRUE(Restart().has_value());

  EXPECT_FALSE(Resident(k)) << "replay built a partial collection";
  EXPECT_TRUE(Resident(KeyOn(0, 1, "str")));
  EXPECT_EQ(skipped_, 1U);
  EXPECT_EQ(Members(k), (std::vector<std::string>{"a", "b", "c"}));
  EXPECT_EQ(Read({"SCARD", k}), ":3");
}

// FLUSHDB in the middle of the log: no key from before it survives
// recovery, an evicted one's stub included.
TEST_F(HotReplayerRecoveryTest, AFlushMidLogLeavesNoPreFlushKey) {
  Open(Options{.shards = 2, .hot_memory_bytes = size_t{64} << 10});
  const std::string a = KeyOn(0, 0, "pre");
  const std::string b = KeyOn(1, 0, "pre");
  Append(0, Frame({"SET", a, "v"}, true));
  Append(1, Frame({"SET", b, "v"}, true));
  // Enough after them that replay evicts both, leaving stubs.
  for (int i = 0; i < 2000; ++i) {
    const std::string key = "fill" + std::to_string(i);
    Append(ShardOf(key), Frame({"SET", key, std::string(200, 'x')}, true));
  }
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) Append(shard, FlushFrame());
  const std::string c = KeyOn(0, 0, "post");
  Append(0, Frame({"SET", c, "v"}, true));

  ASSERT_TRUE(Restart().has_value());

  for (const auto& key : {a, b}) {
    EXPECT_FALSE(hot_->FindStub(key).has_value()) << key;
    EXPECT_FALSE(Resident(key)) << key;
    EXPECT_EQ(Exists(key), ":0") << key;
    EXPECT_EQ(Read({"GET", key}), "nil") << key;
  }
  EXPECT_EQ(Read({"GET", "fill0"}), "nil");
  EXPECT_EQ(Read({"GET", c}), "v");
}

// A log three times hot's budget, the buffer's high water above all of
// it, so cold flushes nothing on its own while replay runs. Cold is fed
// first, so every frame hot holds is drained, and evicting keeps replay
// within the backpressure ratio without a forced drain; the sweep then
// brings hot under its budget.
TEST_F(HotReplayerRecoveryTest, ALogThreeTimesTheBudgetReplaysWithinTheRatio) {
  constexpr size_t kBudget = size_t{1} << 20;
  Open(Options{.shards = 1,
               .segment_size_bytes = size_t{16} << 20,
               .hot_memory_bytes = kBudget,
               .buffer_high_water_bytes = size_t{256} << 20});
  size_t logged = 0;
  for (int i = 0; logged < 3 * kBudget; ++i) {
    const std::string key = "k" + std::to_string(i);
    std::string value(200, static_cast<char>('a' + (i % 26)));
    logged += key.size() + value.size();
    Append(0, Frame({"SET", key, std::move(value)}, true));
  }

  ASSERT_TRUE(Restart(Clocks{}, /*sample_peak=*/true).has_value());

  EXPECT_LE(static_cast<double>(peak_hot_bytes_), 1.25 * static_cast<double>(kBudget))
      << "replay passed the backpressure ratio";
  EXPECT_LE(hot_->Stats()->used_bytes, kBudget) << "the sweep left hot over its budget";
  EXPECT_EQ(metrics::testing::GetCounterValue(metrics::names::kRecoveryColdDrainRequestsTotal)
                .value_or(0.0),
            static_cast<double>(drain_requests_));
  EXPECT_EQ(drain_requests_, 0U) << "cold was fed first, so nothing hot held was undrained";
  EXPECT_EQ(Read({"GET", "k0"}), std::string(200, 'a'));
}

// A key whose last write is older than its eviction window when the
// process restarts is not resident after recovery: the sweep judges it
// by when it was written, not by when replay linked it. It is served
// whole from cold. A key written since stays resident, and an idle
// shard's frames still restore its stamp.
TEST_F(HotReplayerRecoveryTest, ARestartPastTheEvictionWindowLeavesAKeyToCold) {
  Open();
  const int64_t t0 = std::chrono::duration_cast<std::chrono::milliseconds>(
                         core::WallClock::now().time_since_epoch())
                         .count();
  const int64_t window =
      std::chrono::duration_cast<std::chrono::milliseconds>(policy_.Resolve("k")).count();
  std::atomic<int64_t> now{t0};
  wall_ = [&now] { return core::WallTime{std::chrono::milliseconds{now.load()}}; };
  const std::string idle = KeyOn(ShardOf("k") == 0 ? 1 : 0, 0, "idle");
  EXPECT_EQ(Write({"SET", idle, "v"}), "OK");
  EXPECT_EQ(Write({"SADD", "k", "a"}), ":1");
  now = t0 + 60'000;
  EXPECT_EQ(Write({"SADD", "k", "b"}), ":1");
  now = t0 + window;
  EXPECT_EQ(Write({"SET", "fresh", "v"}), "OK");

  // Restarted two minutes past k's window, a minute past its last write.
  now = t0 + window + 120'000;
  ASSERT_TRUE(Restart(Clocks{.wall = wall_}).has_value());

  EXPECT_FALSE(Resident("k")) << "k outlived its eviction window across the restart";
  EXPECT_FALSE(Resident(idle));
  EXPECT_TRUE(Resident("fresh"));
  {
    auto locks = hot_->LockExclusive(std::vector<core::ShardId>{ShardOf(idle)});
    EXPECT_GE(std::chrono::floor<std::chrono::milliseconds>(locks.LastAppendedAt(ShardOf(idle))),
              core::WallTime{std::chrono::milliseconds{t0}})
        << "an idle shard's replayed frames still restore its stamp";
  }
  EXPECT_EQ(Members("k"), (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(Write({"RENAMENX", "k", "k2"}, Flags::kNx), ":1");
  EXPECT_EQ(Members("k2"), (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(Members("k"), std::vector<std::string>{});
}

// The same log, replayed with the wall clock an hour behind, on time and
// an hour ahead, leaves hot the same up to the sweep, which alone reads
// the clock: replay judges TTLs only at each frame's appended_at and
// links keys there.
// Reads after recovery agree as well, for TTLs well clear of the shift.
TEST_F(HotReplayerRecoveryTest, ReplayNeverReadsTheClock) {
  Open(Options{.shards = 2});
  const int64_t t0 = std::chrono::duration_cast<std::chrono::milliseconds>(
                         core::WallClock::now().time_since_epoch())
                         .count();
  wall_ = [t0] { return AtMs(t0); };
  const std::string near_ms = std::to_string(t0 + (int64_t{30} * 60 * 1000));
  const std::string far_ms = std::to_string(t0 + (int64_t{10} * 3600 * 1000));
  const std::vector<std::vector<std::string>> writes{
      {"SET", "near", "v", "PXAT", near_ms},
      {"SET", "far", "v", "PXAT", far_ms},
      {"SET", "plain", "v"},
      {"SADD", "set", "a", "b", "c"},
      {"PEXPIREAT", "set", near_ms},
      {"SADD", "set2", "x", "y"},
      {"PEXPIREAT", "set2", far_ms},
      {"HSET", "h", "f1", "v1", "f2", "v2"},
      {"HDEL", "h", "f1"},
      {"ZADD", "z", "1", "m", "2", "n"},
      {"ZREM", "z", "m"},
      {"SET", "gone", "v"},
      {"DEL", "gone"},
      {"SET", "p", "v", "PXAT", far_ms},
      {"PERSIST", "p"},
  };
  for (const auto& args : writes) ASSERT_NE(Write(args).substr(0, 5), "error") << args[0];
  const std::vector<std::string> keys{"near", "far", "plain", "set", "set2", "h", "z", "gone", "p"};
  CloseAll();
  const auto wal = dir_.Sub("wal.orig");
  const auto cold = dir_.Sub("cold.orig");
  std::filesystem::copy(dir_.Sub("wal"), wal, std::filesystem::copy_options::recursive);
  std::filesystem::copy(dir_.Sub("cold"), cold, std::filesystem::copy_options::recursive);

  std::vector<std::string> states;
  std::vector<std::string> reads;
  for (const int64_t shift : {int64_t{-3600} * 1000, int64_t{0}, int64_t{3600} * 1000}) {
    SCOPED_TRACE("shift " + std::to_string(shift) + " ms");
    const int64_t now = t0 + shift;
    const Replayed replayed = ReplayAt(wal, cold, now, keys);
    EXPECT_EQ(replayed.hot_clock_reads, 0) << "replay read hot's clock";
    states.push_back(replayed.before_sweep);
    // A TTL read as an expiry time, so the shift cancels out.
    const auto expiry = [this, now](const std::string& key) {
      const std::string pttl = Read({"PTTL", key});
      return pttl.starts_with(":-") ? pttl : std::to_string(now + std::stoll(pttl.substr(1)));
    };
    std::string read;
    read += Read({"GET", "far"}) + " " + expiry("far") + "\n";
    read += Read({"GET", "plain"}) + " " + expiry("plain") + "\n";
    read += Read({"SCARD", "set2"}) + " " + expiry("set2") + "\n";
    read += Read({"HGET", "h", "f2"}) + " " + Read({"HLEN", "h"}) + "\n";
    read += Read({"ZSCORE", "z", "n"}) + " " + Read({"ZCARD", "z"}) + "\n";
    read += Read({"GET", "gone"}) + " " + Exists("gone") + "\n";
    read += Read({"GET", "p"}) + " " + expiry("p") + "\n";
    reads.push_back(read);
  }
  EXPECT_EQ(states[0], states[1]);
  EXPECT_EQ(states[1], states[2]);
  EXPECT_EQ(reads[0], reads[1]);
  EXPECT_EQ(reads[1], reads[2]);
  EXPECT_NE(states[1].find("near"), std::string::npos);
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
