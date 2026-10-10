// Crash and power-loss recovery against what clients were told.
//
// Concurrent clients write and read a small keyspace while flushes
// stall and resume at random. Then the process "loses power": past the
// durable extent an arbitrary subset of the log's dirty pages reaches
// the device, with single-bit flips, lost pages of a recycled segment
// revert to its old frames, and later segments that took no frame may
// be cut short. Recovery must then hold a prefix of the log as it stood
// (RocksDB's ExpectedState pattern):
// - every write acknowledged at the durability class is in it;
// - every reply, to a read or a write, is explained by some state in it
//   no older than what the client had already been acknowledged: no
//   reply reflects a write the power loss took;
// - the recovered engine holds exactly the prefix, replayed; cold,
//   which may read past the power-durable end, holds nothing beyond.
//
// ABYSS_CRASH_SEED=<n> replays one seed's configuration and crash.

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <latch>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#ifndef _WIN32
#include <sys/stat.h>
#endif

#include "abyss/core/ascii.h"
#include "abyss/core/queue_entry.h"
#include "abyss/platform/fs.h"
#include "abyss/platform/mapped_file.h"
#include "crash_harness.h"
#include "property_harness.h"
#include "wal_power_loss.h"

namespace abyss::engine {
namespace {

constexpr int kCrashKeys = 8;

enum class Op : uint8_t { kSet, kDel, kSAdd, kSetNx, kGet, kExists, kType };

// One client call: what it asked, what it was told, and when.
struct Call {
  int client = 0;
  uint64_t call = 0;
  uint64_t ret = 0;
  Op op = Op::kGet;
  int key = 0;
  // Makes each written value and member unique.
  uint64_t id = 0;
  std::string reply;
};

std::string KeyName(int key) { return "c" + std::to_string(key); }

constexpr std::string_view kTimedOut = "<timed out after logging>";
constexpr std::string_view kNoEffect = "<no effect>";
constexpr std::string_view kUnexpected = "<unexpected> ";

// A reply as the crash checks compare it: in full, but a timeout or a
// refusal, which no state explains, by its kind.
std::string Text(const core::RespValue& reply, bool write) {
  switch (Classify(reply, write)) {
    case ReplyKind::kIndeterminate:
      return std::string(kTimedOut);
    case ReplyKind::kNoOp:
      return std::string(kNoEffect);
    case ReplyKind::kUnexpected:
      return std::string(kUnexpected) + RedisModel::Render(reply);
    case ReplyKind::kDefinite:
      break;
  }
  return RedisModel::Render(reply);
}

bool Writes(Op op) {
  return op == Op::kSet || op == Op::kDel || op == Op::kSAdd || op == Op::kSetNx;
}

std::vector<std::string> ArgsOf(const Call& call) {
  const std::string key = KeyName(call.key);
  switch (call.op) {
    case Op::kSet:
      return {"SET", key, "v" + std::to_string(call.id)};
    case Op::kDel:
      return {"DEL", key};
    case Op::kSAdd:
      return {"SADD", key, "m" + std::to_string(call.id)};
    case Op::kSetNx:
      return {"SETNX", key, "v" + std::to_string(call.id)};
    case Op::kGet:
      return {"GET", key};
    case Op::kExists:
      return {"EXISTS", key};
    case Op::kType:
      return {"TYPE", key};
  }
  return {};
}

// A call's kind, by a roll in [0, 100).
Op PickOp(uint64_t roll) {
  constexpr std::array<std::pair<uint64_t, Op>, 6> kBelow = {{{25, Op::kSet},
                                                              {37, Op::kDel},
                                                              {57, Op::kSAdd},
                                                              {65, Op::kSetNx},
                                                              {80, Op::kGet},
                                                              {90, Op::kExists}}};
  for (const auto& [below, op] : kBelow) {
    if (roll < below) return op;
  }
  return Op::kType;
}

// Writes whose effect their unique id names, once acknowledged.
bool IdentifiesItsEffect(const Call& call) {
  return (call.op == Op::kSet && call.reply == "+OK") ||
         (call.op == Op::kSAdd && call.reply == ":1") ||
         (call.op == Op::kSetNx && call.reply == ":1");
}

// Each key's history in a log, version by version.
struct Versions {
  // Entry args touching each key, in log order.
  std::map<std::string, std::vector<std::vector<std::string>>> by_key;
  // A written value's or member's (key, version), from 1.
  std::map<std::string, std::pair<std::string, size_t>> of_id;
};

Versions VersionsOf(const std::vector<std::vector<core::QueueEntry>>& log) {
  Versions versions;
  for (const auto& shard : log) {
    for (const auto& entry : shard) {
      const auto* write = std::get_if<core::entry::Write>(&entry.payload);
      if (write == nullptr) continue;
      const auto& args = write->cmd.args;
      auto& history = versions.by_key[args.at(1)];
      history.push_back(args);
      if (args.size() > 2) versions.of_id[args[2]] = {args[1], history.size()};
    }
  }
  return versions;
}

class CrashTest : public PropertyHarness {
 protected:
  struct Tally {
    uint64_t calls = 0;
    uint64_t acknowledged = 0;
    uint64_t checked_replies = 0;
    uint64_t lost_frames = 0;
    // Segments past the durable extent that held an earlier generation.
    uint64_t recycled = 0;
    // Frames cold had taken before the power loss.
    uint64_t cold_absorbed = 0;
    // Calls whose replies were checked: those of a crash that keeps
    // what was acknowledged.
    uint64_t checked_calls = 0;
    // Replies refused or timed out, which no state explains.
    uint64_t refused = 0;
    bool ok = true;
  };

  // The flush gate: closed, a flush waits before its fdatasync.
  void Gate(bool closed) {
    {
      const std::scoped_lock lock(gate_mu_);
      gate_closed_ = closed;
    }
    gate_cv_.notify_all();
  }
  queue::FlushHook GateHook() {
    return [this](uint32_t) -> core::Result<void> {
      std::unique_lock lock(gate_mu_);
      gate_cv_.wait(lock, [this] { return !gate_closed_; });
      return {};
    };
  }

  // Each shard's retained log, as recovery left it.
  std::vector<std::vector<core::QueueEntry>> WholeLog() {
    std::vector<std::vector<core::QueueEntry>> log(options_.shards);
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
      auto read = queue_->Read(shard, queue_->FirstSeq(shard).value_or(core::kFirstSeq), 1'000'000,
                               std::chrono::milliseconds(0), core::Durability::kProcessCrash);
      EXPECT_TRUE(read.has_value()) << read.error().message();
      if (read.has_value()) log[shard] = std::move(*read);
    }
    return log;
  }

  // Every frame published since the last call, with its log position,
  // read before cold can absorb it, so retention never takes one first.
  void Tail() {
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
      auto& log = full_log_.at(shard);
      const core::SequenceId next = core::kFirstSeq + log.size();
      auto read = queue_->Read(shard, next, 4096, std::chrono::milliseconds(0),
                               core::Durability::kProcessCrash);
      if (!read.has_value()) continue;
      for (auto& entry : *read) {
        if (const auto pos = queue_->PositionForTesting(shard, entry.seq)) {
          positions_[{shard, entry.seq}] = *pos;
        }
        log.push_back(std::move(entry));
      }
    }
  }

  // Each free-pool file's bytes, by inode: what a recycled segment's
  // lost pages revert to.
  void SnapshotFreeFiles() {
    const auto dir = dir_.Sub(options_.run + "wal") / "log-0000";
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
      const std::string name = entry.path().filename().string();
      if (!name.starts_with("free-")) continue;
      const auto inode = InodeOf(entry.path());
      if (!inode.has_value() || free_files_.contains(*inode)) continue;
      std::ifstream in(entry.path(), std::ios::binary);
      std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
      if (!bytes.empty()) free_files_[*inode] = std::move(bytes);
    }
  }

  // A file's identity across renames; none on Windows, where recycled
  // segments' lost pages revert to zeroes instead.
  static std::optional<uint64_t> InodeOf(const std::filesystem::path& path) {
#ifndef _WIN32
    struct stat st{};
    if (::stat(path.c_str(), &st) == 0) return static_cast<uint64_t>(st.st_ino);
#else
    (void)path;
#endif
    return std::nullopt;
  }

  // Cold takes what it can as clients run: absorbs at the ack class,
  // flushes and checkpoints, and persists its commits so retention
  // reclaims and recycles segments.
  void Drain(const std::atomic<bool>& stop, uint64_t seed) {
    Rng rng(seed);
    while (!stop.load()) {
      Tail();
      for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
        auto& consumer = pool_->ConsumerFor(shard);
        if (consumer.LatestDrainedSeq() + 1 <
            queue_->DurableEnd(shard, queue_->AckDurability()).value_or(0)) {
          consumer.DrainWithBatch(1 + rng.Below(64));
        }
        if (rng.Chance(0.3)) (void)consumer.FlushUnscheduled();
      }
      if (rng.Chance(0.2)) {
        EXPECT_TRUE(queue_->FlushOffsets().has_value());
        SnapshotFreeFiles();
      }
      std::this_thread::sleep_for(std::chrono::microseconds(rng.Between(100, 800)));
    }
  }

  Tally PowerLoss(uint64_t seed) {
    Rng rng(seed);
    Options options;
    options.shards = 1 + static_cast<uint32_t>(rng.Below(4));
    // One in four at process_crash, which a power loss may take acks
    // from: those check the log and cold, not the replies.
    options.durability =
        seed % 4 == 0 ? core::Durability::kProcessCrash : core::Durability::kPowerLoss;
    options.segment_size_bytes = size_t{16} << 10;
    // Keys leave hot, so reads and writes go to buffer and cold; the
    // ratio leaves room for what the stalled flushes hold back.
    options.hot_memory_bytes = 4096;
    options.backpressure_ratio = 4.0;
    options.write_timeout = std::chrono::milliseconds(300);
    options.offset_fsync_interval = std::chrono::milliseconds(10);
    PatientColdReads(options);
    options.run = "power" + std::to_string(seed) + "-";
    std::cout << "[crash] power loss seed=" << seed << " shards=" << options.shards
              << " durability=" << core::DurabilityName(options.durability) << '\n'
              << std::flush;
    OpenAt(options, 1'700'000'000'000);
    full_log_.assign(options.shards, {});
    positions_.clear();
    free_files_.clear();
    queue_->SetFlushHookForTesting(GateHook());

    std::vector<Call> calls;
    std::mutex calls_mu;
    std::atomic<uint64_t> clock{1};
    std::atomic<bool> crashed{false};
    // Flushes have stopped for good: writes and conflicting decisions
    // crowd a few keys, where a reply can show an unflushed write.
    std::atomic<bool> edge{false};
    std::atomic<bool> stop_drain{false};
    std::atomic<uint64_t> ids{1};
    constexpr int kClients = 8;
    std::latch start(kClients + 1);
    std::thread drainer([this, &stop_drain, seed] { Drain(stop_drain, seed ^ 0xD7A1ULL); });
    std::vector<std::thread> clients;
    clients.reserve(kClients);
    for (int c = 0; c < kClients; ++c) {
      clients.emplace_back([&, c] {
        Rng mine((seed * 100) + static_cast<uint64_t>(c));
        resp::RequestPipeline pipeline(resp::GlobalRegistry(), resp::ConnectionState{},
                                       resp::PipelineDependencies{.dispatcher = engine_.get()});
        start.arrive_and_wait();
        std::vector<Call> local;
        while (!crashed.load()) {
          // Keys 0 and 1 are the edge's alone.
          Call call{.client = c, .key = 2 + static_cast<int>(mine.Below(kCrashKeys - 2))};
          call.op = PickOp(mine.Below(100));
          if (edge.load()) {
            constexpr std::array<Op, 6> kEdge = {Op::kSet,  Op::kSet, Op::kSet,
                                                 Op::kSAdd, Op::kGet, Op::kType};
            call.key = static_cast<int>(mine.Below(2));
            call.op = kEdge.at(mine.Below(kEdge.size()));
          }
          call.id = ids.fetch_add(1);
          call.call = clock.fetch_add(1);
          const core::RespValue reply = pipeline.Dispatch(core::RespCommand{.args = ArgsOf(call)});
          call.ret = clock.fetch_add(1);
          // Told after the power loss: not told at all.
          if (crashed.load()) break;
          call.reply = Text(reply, Writes(call.op));
          local.push_back(std::move(call));
        }
        const std::scoped_lock lock(calls_mu);
        for (auto& call : local) calls.push_back(std::move(call));
      });
    }
    start.arrive_and_wait();
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(rng.Between(50, 150));
    while (std::chrono::steady_clock::now() < until) {
      Gate(rng.Chance(0.6));
      std::this_thread::sleep_for(std::chrono::microseconds(rng.Between(200, 3000)));
    }
    // The edge keys hold sets, durably; then flushes stop, and writes
    // and conflicting decisions go on against what is unflushed.
    Gate(false);
    for (int k = 0; k < 2; ++k) {
      (void)Run({"DEL", KeyName(k)});
      (void)Run({"SADD", KeyName(k), "edge"});
    }
    Gate(true);
    edge = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(rng.Between(20, 60)));
    crashed = true;
    for (auto& client : clients) client.join();
    stop_drain = true;
    drainer.join();

    // The log as the clients left it, then what the device kept.
    Tail();
    // Cold catches up with all that was published, as a consumer that
    // kept up would: at process_crash that runs past the power-durable
    // end, which must not reach RocksDB.
    for (core::ShardId shard = 0; shard < options.shards; ++shard) {
      auto& consumer = pool_->ConsumerFor(shard);
      const core::SequenceId end =
          queue_->DurableEnd(shard, queue_->AckDurability()).value_or(core::kFirstSeq);
      while (consumer.LatestDrainedSeq() + 1 < end && consumer.DrainWithBatch(4096) > 0) {
      }
      (void)consumer.FlushUnscheduled();
    }
    const auto before = full_log_;
    uint64_t cold_absorbed = 0;
    for (core::ShardId shard = 0; shard < options.shards; ++shard) {
      cold_absorbed += pool_->ConsumerFor(shard).LatestDrainedSeq();
    }
    const queue::DurableExtent extent = queue_->DurableExtentForTesting(0);
    queue_->SkipFinalFlushForTesting();
    Gate(false);
    CloseAll();
    uint64_t recycled = 0;
    abyss::testing::SimulatePowerLossDropping(
        extent, seed, {.keep_page = 0.3},
        [this, &recycled](const std::filesystem::path& path, uint64_t from,
                          uint64_t to) -> std::optional<std::string> {
          const auto inode = InodeOf(path);
          if (!inode.has_value()) return std::nullopt;
          const auto it = free_files_.find(*inode);
          if (it == free_files_.end() || it->second.size() < to) return std::nullopt;
          ++recycled;
          return it->second.substr(from, to - from);
        });
    if (::testing::Test::HasFatalFailure()) return {.ok = false};

    auto restarted = RestartAt();
    if (!restarted.has_value()) {
      ADD_FAILURE() << "seed " << seed << ": recovery failed: " << restarted.error().message()
                    << "\nreplay: ABYSS_CRASH_SEED=" << seed;
      return {.ok = false};
    }
    Tally run = Check(seed, calls, before, options_.durability == core::Durability::kPowerLoss);
    run.recycled = recycled;
    run.cold_absorbed = cold_absorbed;
    CloseAll();
    return run;
  }

  // The crash edge, ordered: a write is applied but unflushed when a
  // read, a refused decision and a type probe of its key reply, then the
  // power fails and takes every unflushed page. Without the read path's
  // fence or the decision's, a reply shows the lost write.
  Tally EdgeCrash() {
    constexpr uint64_t kSeed = 99;
    Options options;
    options.shards = 2;
    options.durability = core::Durability::kPowerLoss;
    options.segment_size_bytes = size_t{16} << 10;
    options.write_timeout = std::chrono::milliseconds(300);
    PatientColdReads(options);
    options.run = "edge-";
    OpenAt(options, 1'700'000'000'000);
    full_log_.assign(options.shards, {});
    positions_.clear();
    queue_->SetFlushHookForTesting(GateHook());
    const std::string key = KeyName(0);
    const core::ShardId shard = ShardOf(key);
    EXPECT_EQ(RedisModel::Render(Run({"SADD", key, "m0"})), ":1");
    const core::SequenceId published =
        queue_->DurableEnd(shard, core::Durability::kProcessCrash).value_or(0);
    Gate(true);
    std::atomic<uint64_t> clock{1};
    std::thread writer([this, &key] {
      resp::RequestPipeline pipeline(resp::GlobalRegistry(), resp::ConnectionState{},
                                     resp::PipelineDependencies{.dispatcher = engine_.get()});
      (void)pipeline.Dispatch(core::RespCommand{.args = {"SET", key, "v1"}});
    });
    // Applied and published, never flushed.
    for (int i = 0;
         i < 2000 &&
         queue_->DurableEnd(shard, core::Durability::kProcessCrash).value_or(0) == published;
         ++i) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    std::vector<Call> calls;
    std::mutex calls_mu;
    std::vector<std::thread> readers;
    for (const Op op : {Op::kSAdd, Op::kGet, Op::kType, Op::kSetNx}) {
      readers.emplace_back([&, op] {
        resp::RequestPipeline pipeline(resp::GlobalRegistry(), resp::ConnectionState{},
                                       resp::PipelineDependencies{.dispatcher = engine_.get()});
        Call call{.client = static_cast<int>(op), .op = op, .key = 0, .id = 1000};
        call.call = clock.fetch_add(1);
        call.reply = Text(pipeline.Dispatch(core::RespCommand{.args = ArgsOf(call)}), Writes(op));
        call.ret = clock.fetch_add(1);
        const std::scoped_lock lock(calls_mu);
        calls.push_back(std::move(call));
      });
    }
    for (auto& reader : readers) reader.join();
    writer.join();
    Tail();
    const auto before = full_log_;
    const queue::DurableExtent extent = queue_->DurableExtentForTesting(0);
    queue_->SkipFinalFlushForTesting();
    Gate(false);
    CloseAll();
    abyss::testing::SimulatePowerLossDropping(extent, kSeed, {.keep_page = 0.0, .bit_flips = 0});
    auto restarted = RestartAt();
    if (!restarted.has_value()) {
      ADD_FAILURE() << "the edge crash's recovery failed: " << restarted.error().message();
      return {.ok = false};
    }
    Tally run = Check(kSeed, calls, before, /*acks_survive=*/true);
    CloseAll();
    return run;
  }

  // The recovered log against the log before, cold against it, and
  // every reply against the recovered prefix. `acks_survive`: the
  // crash keeps what was acknowledged, as a kill -9 does at either
  // class, and a power loss at power_loss.
  Tally Check(uint64_t seed, const std::vector<Call>& calls,
              const std::vector<std::vector<core::QueueEntry>>& before, bool acks_survive) {
    Tally run{.calls = calls.size()};
    const auto fail = [&run, seed](const std::string& what) {
      run.ok = false;
      ADD_FAILURE() << "seed " << seed << ": " << what << "\nreplay: ABYSS_CRASH_SEED=" << seed;
    };
    std::vector<std::vector<core::QueueEntry>> after(before.size());
    uint64_t max_kept = 0;
    uint64_t min_lost = UINT64_MAX;
    for (core::ShardId shard = 0; shard < before.size(); ++shard) {
      const auto end = queue_->DurableEnd(shard, core::Durability::kProcessCrash);
      const auto first = queue_->FirstSeq(shard);
      if (!end.has_value() || !first.has_value()) {
        fail("shard " + std::to_string(shard) + " has no recovered range");
        return run;
      }
      const size_t kept = *end - core::kFirstSeq;
      if (kept > before[shard].size()) {
        fail("shard " + std::to_string(shard) + " recovered frames never written");
        return run;
      }
      run.lost_frames += before[shard].size() - kept;
      // Retention may reclaim the head as this reads, a segment at a
      // time: read from where it now starts until it rests.
      core::SequenceId from = *first;
      auto read = queue_->Read(shard, from, 1'000'000, std::chrono::milliseconds(0),
                               core::Durability::kProcessCrash);
      const auto give_up = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (!read.has_value() && read.error().code() == core::ErrorCode::kOutOfRange &&
             std::chrono::steady_clock::now() < give_up) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        from = queue_->FirstSeq(shard).value_or(*first);
        read = queue_->Read(shard, from, 1'000'000, std::chrono::milliseconds(0),
                            core::Durability::kProcessCrash);
      }
      if (!read.has_value()) {
        fail("shard " + std::to_string(shard) + ": " + read.error().message());
        return run;
      }
      if (read->size() != *end - from) {
        fail("shard " + std::to_string(shard) + " recovered " + std::to_string(read->size()) +
             " entries from seq " + std::to_string(from) + " to its end " + std::to_string(*end));
        return run;
      }
      for (const auto& entry : *read) {
        const auto& written = before[shard].at(entry.seq - core::kFirstSeq);
        if (engine::ArgsOf(entry) != engine::ArgsOf(written)) {
          fail("shard " + std::to_string(shard) + " seq " + std::to_string(entry.seq) +
               " recovered other than written");
          return run;
        }
      }
      after[shard].assign(before[shard].begin(),
                          before[shard].begin() + static_cast<std::ptrdiff_t>(kept));
      for (size_t i = 0; i < before[shard].size(); ++i) {
        const auto known = positions_.find({shard, before[shard][i].seq});
        if (known == positions_.end()) continue;
        const uint64_t pos = known->second;
        if (i < kept) {
          max_kept = std::max(max_kept, pos);
        } else {
          min_lost = std::min(min_lost, pos);
        }
      }
      // Cold never ahead of the log: its committed offset is below the
      // recovered end, or new writes would reuse seqs cold skips.
      const auto committed = queue_->CommittedOffset(core::kColdConsumer, shard);
      if (committed.has_value() && committed->has_value() && **committed >= *end) {
        fail("shard " + std::to_string(shard) + ": cold committed " + std::to_string(**committed) +
             " past the recovered end " + std::to_string(*end));
        return run;
      }
    }
    // One log: what recovered is a prefix of it by position, whichever
    // shard each frame was on.
    if (max_kept > min_lost) {
      fail("recovery kept a frame at " + std::to_string(max_kept) + " past one lost at " +
           std::to_string(min_lost));
      return run;
    }
    const Versions all = VersionsOf(before);
    const Versions kept = VersionsOf(after);
    const auto recovered = [&kept](const std::string& key) {
      const auto it = kept.by_key.find(key);
      return it == kept.by_key.end() ? size_t{0} : it->second.size();
    };
    // What each kind of call replies at each version of each key.
    std::map<std::string, std::vector<std::map<Op, std::string>>> replies;
    for (int k = 0; k < kCrashKeys; ++k) {
      const std::string key = KeyName(k);
      auto& list = replies[key];
      RedisModel model;
      const auto answer = [&model, k] {
        std::map<Op, std::string> out;
        for (const Op op : {Op::kDel, Op::kSAdd, Op::kSetNx, Op::kGet, Op::kExists, Op::kType}) {
          RedisModel probe = model;
          // An id no write used, so a probe's own value never matches.
          const Call call{.op = op, .key = k, .id = 0};
          out[op] = Text(probe.Execute(ArgsOf(call), 0), Writes(op));
        }
        return out;
      };
      list.push_back(answer());
      const auto it = all.by_key.find(key);
      if (it == all.by_key.end()) continue;
      for (const auto& args : it->second) {
        (void)model.Execute(args, 0);
        list.push_back(answer());
      }
    }
    // Each key's acknowledged writes by when they returned, with the
    // highest version acknowledged by then.
    std::map<int, std::vector<std::pair<uint64_t, size_t>>> acked;
    for (const Call& call : calls) {
      if (!IdentifiesItsEffect(call)) continue;
      const auto it = all.of_id.find(ArgsOf(call)[2]);
      if (it != all.of_id.end()) acked[call.key].emplace_back(call.ret, it->second.second);
    }
    for (auto& [key, list] : acked) {
      std::ranges::sort(list);
      for (size_t i = 1; i < list.size(); ++i) {
        list[i].second = std::max(list[i].second, list[i - 1].second);
      }
    }
    const auto floor_of = [&acked](const Call& call) {
      size_t floor = 0;
      if (const auto it = acked.find(call.key); it != acked.end()) {
        const auto& list = it->second;
        const auto past = std::ranges::lower_bound(list, std::pair<uint64_t, size_t>{call.call, 0});
        if (past != list.begin()) floor = std::prev(past)->second;
      }
      return floor;
    };

    for (const Call& call : calls) {
      const std::string key = KeyName(call.key);
      const auto args = ArgsOf(call);
      if (call.reply.starts_with(kUnexpected)) {
        fail("client " + std::to_string(call.client) + " " + args[0] + " " + key + ": " +
             call.reply);
        return run;
      }
      if (call.reply == kTimedOut || call.reply == kNoEffect) ++run.refused;
      if (!acks_survive) continue;
      ++run.checked_calls;
      if (IdentifiesItsEffect(call)) {
        ++run.acknowledged;
        const auto it = all.of_id.find(args[2]);
        std::string what = "acknowledged ";
        what.append(args[0]).append(" ").append(key).append(" ").append(args[2]);
        if (it == all.of_id.end()) {
          fail(what + " was never logged");
          return run;
        }
        if (it->second.second > recovered(key)) {
          what.append(" was lost: version ").append(std::to_string(it->second.second));
          what.append(" of ").append(key).append(", recovered ");
          fail(what + std::to_string(recovered(key)));
          return run;
        }
        continue;
      }
      if (call.op == Op::kDel && call.reply == ":1") {
        // Its DEL is one after every write acknowledged before it.
        ++run.acknowledged;
        const auto it = all.by_key.find(key);
        bool kept_del = false;
        for (size_t v = floor_of(call) + 1;
             it != all.by_key.end() && v <= recovered(key) && !kept_del; ++v) {
          kept_del = it->second.at(v - 1).at(0) == "DEL";
        }
        if (!kept_del) {
          fail("acknowledged DEL " + key + " was lost");
          return run;
        }
        continue;
      }
      if (call.reply == kTimedOut || call.reply == kNoEffect) continue;
      const size_t floor = floor_of(call);
      ++run.checked_replies;
      bool explained = false;
      const auto& versions = replies.at(key);
      for (size_t v = floor; v <= recovered(key) && v < versions.size() && !explained; ++v) {
        explained = versions[v].at(call.op) == call.reply;
      }
      if (!explained) {
        std::string history;
        const auto it = all.by_key.find(key);
        if (it != all.by_key.end()) {
          for (size_t v = 0; v < it->second.size(); ++v) {
            history.append("\n  v").append(std::to_string(v + 1)).append(":");
            for (const auto& arg : it->second[v]) history.append(" ").append(arg);
          }
        }
        std::string what = "reply to client " + std::to_string(call.client);
        what.append(" ").append(args[0]).append(" ").append(key).append(" -> ").append(call.reply);
        what.append(" is explained by no state from version ").append(std::to_string(floor));
        what.append(" to the recovered ").append(std::to_string(recovered(key)));
        what.append("; it saw a lost write").append(history);
        fail(what);
        return run;
      }
    }

    // The engine holds the recovered prefix, replayed: in hot, then in
    // buffer plus cold once everything is evicted.
    model_.Clear();
    for (const auto& shard : after) {
      for (const auto& entry : shard) {
        if (const auto* write = std::get_if<core::entry::Write>(&entry.payload)) {
          (void)model_.Execute(write->cmd.args, 0);
        }
      }
    }
    const auto check_all = [&](const std::string& when) {
      for (int k = 0; k < kCrashKeys; ++k) {
        if (const auto seen = CheckKey(KeyName(k)); seen.mismatch.has_value()) {
          fail(when + ": " + *seen.mismatch);
          return false;
        }
      }
      return true;
    };
    if (!check_all("after recovery")) return run;
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
    EvictDrained();
    if (!check_all("after recovery, from buffer and cold")) return run;
    // New writes take the seqs the power loss freed; cold must apply
    // them, not skip them as already absorbed.
    for (int k = 0; k < kCrashKeys; ++k) {
      const std::vector<std::string> set = {"SET", KeyName(k), "after" + std::to_string(k)};
      const std::string got = RedisModel::Render(Run(set));
      if (got != "+OK") {
        fail("a write after recovery: " + got);
        return run;
      }
      (void)model_.Execute(set, 0);
    }
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
    EvictDrained();
    (void)check_all("new writes after recovery, from buffer and cold");
    return run;
  }

  static void Add(Tally& total, const Tally& run) {
    total.calls += run.calls;
    total.acknowledged += run.acknowledged;
    total.checked_replies += run.checked_replies;
    total.lost_frames += run.lost_frames;
    total.recycled += run.recycled;
    total.cold_absorbed += run.cold_absorbed;
    total.checked_calls += run.checked_calls;
    total.refused += run.refused;
  }

  // A server that refused or timed out most calls would pass every
  // check above with little to check.
  static void ExpectServed(const Tally& total) {
    EXPECT_GE(total.acknowledged * 20, total.checked_calls) << "too few writes acknowledged";
    EXPECT_GE(total.checked_replies * 20, total.checked_calls) << "too few replies checked";
    EXPECT_LE(total.refused * 10, total.calls) << "too many calls refused or timed out";
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::mutex gate_mu_;
  std::condition_variable gate_cv_;
  bool gate_closed_ = false;
  std::vector<std::vector<core::QueueEntry>> full_log_;
  std::map<std::pair<core::ShardId, core::SequenceId>, uint64_t> positions_;
  std::map<uint64_t, std::string> free_files_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(CrashTest, PowerLossRecoversAPrefixThatExplainsEveryReply) {
  std::vector<uint64_t> seeds;
  if (const auto only = EnvNumber("ABYSS_CRASH_SEED")) {
    seeds = {*only};
  } else if (const auto count = EnvNumber("ABYSS_CRASH_POWER_LOSSES")) {
    // The long tier, opt-in.
    for (uint64_t s = 100; s < 100 + *count; ++s) seeds.push_back(s);
  } else if (kSanitizerDivisor > 1) {
    // One of each class: seed 4 runs at process_crash.
    seeds = {1, 4};
  } else {
    for (uint64_t s = 1; s <= 12; ++s) seeds.push_back(s);
  }
  Tally total;
  const auto started = std::chrono::steady_clock::now();
  if (!EnvNumber("ABYSS_CRASH_SEED").has_value()) {
    const Tally edge = EdgeCrash();
    Add(total, edge);
    if (!edge.ok) return;
  }
  for (const uint64_t seed : seeds) {
    const Tally run = PowerLoss(seed);
    Add(total, run);
    if (!run.ok) break;
  }
  std::cout << "[crash] power loss: " << seeds.size() << " crashes, " << total.calls << " calls ("
            << total.refused << " refused or timed out), " << total.acknowledged
            << " acknowledged writes kept, " << total.checked_replies << " replies explained, "
            << total.lost_frames << " unacknowledged frames lost, " << total.cold_absorbed
            << " frames cold took, " << total.recycled << " lost pages of recycled segments, "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - started)
                   .count()
            << " ms" << '\n'
            << std::flush;
  if (HasFailure()) return;
  EXPECT_GT(total.lost_frames, 0U) << "no crash lost anything: the test tested nothing";
  EXPECT_GT(total.cold_absorbed, 0U) << "cold took nothing before a crash";
  ExpectServed(total);
#ifndef _WIN32
  // Windows has no inodes to find a recycled file's old bytes by.
  if (kSanitizerDivisor == 1 && seeds.size() > 1) {
    EXPECT_GT(total.recycled, 0U) << "no lost page of a recycled segment reverted";
  }
#endif
}

#ifndef _WIN32

constexpr const char* kVictimEnv = "ABYSS_CRASH_VICTIM_DIR";
constexpr int kKillClients = 8;
constexpr size_t kSlotsPerClient = 20000;
constexpr int64_t kKillStartMs = 1'700'000'000'000;

// One journalled call, in a MAP_SHARED file whose pages outlive the
// kill. `state` is written last: 1 invoked, 2 replied.
struct Slot {
  uint64_t call;
  uint64_t ret;
  uint64_t id;
  uint8_t state;
  uint8_t op;
  uint8_t key;
  uint8_t reply_len;
  // Room for every reply in full; an unexpected one may be cut.
  std::array<char, 100> reply;
};
static_assert(sizeof(Slot) == 128);

class Journal {
 public:
  Journal(const std::filesystem::path& path, bool create) {
    const size_t bytes = sizeof(Slot) * kSlotsPerClient * kKillClients;
    auto file = platform::fs::Open(
        path, {.mode = platform::fs::OpenMode::kReadWrite, .create = create, .truncate = create});
    if (!file.has_value()) return;
    if (create && !platform::fs::Ftruncate(*file, bytes).has_value()) return;
    auto map = platform::fs::MappedFile::Map(*file, bytes);
    if (!map.has_value()) return;
    map_ = std::move(*map);
    slots_ = {reinterpret_cast<Slot*>(map_.data()), kSlotsPerClient * kKillClients};
  }
  bool ok() const { return !slots_.empty(); }
  Slot& At(int client, size_t i) {
    return slots_[(static_cast<size_t>(client) * kSlotsPerClient) + i];
  }
  static void Publish(Slot& slot, uint8_t state) {
    std::atomic_ref<uint8_t>(slot.state).store(state, std::memory_order_release);
  }

 private:
  platform::fs::MappedFile map_;
  std::span<Slot> slots_;
};

Options KillOptions(uint64_t seed, const std::filesystem::path& dir) {
  Rng rng(seed);
  Options options;
  options.shards = 1 + static_cast<uint32_t>(rng.Below(4));
  options.durability = core::Durability::kProcessCrash;
  options.segment_size_bytes = size_t{16} << 10;
  options.write_timeout = std::chrono::seconds(1);
  PatientColdReads(options);
  // Absolute: the stores live in the victim's directory.
  options.run = dir.string() + "/";
  return options;
}

// Driven out-of-process by KillNineAtProcessCrash...: clients journal
// every call, the victim signals ready, and goes on until killed.
TEST_F(CrashTest, KillNineVictim) {
  bool is_victim = false;
  const auto dir = abyss::testing::VictimDirFromEnv(kVictimEnv, &is_victim);
  if (!is_victim) GTEST_SKIP() << "crash victim; driven out-of-process by CrashTest";
  abyss::testing::ExitWithParent(std::chrono::seconds(30));
  const uint64_t seed = std::stoull(abyss::testing::crash_internal::ReadFileOrEmpty(dir / "seed"));
  OpenAt(KillOptions(seed, dir), kKillStartMs);
  Journal journal(dir / "journal", /*create=*/true);
  ASSERT_TRUE(journal.ok());
  std::atomic<uint64_t> clock{1};
  std::atomic<uint64_t> ids{1};
  std::atomic<uint64_t> done{0};
  std::vector<std::thread> clients;
  clients.reserve(kKillClients);
  for (int c = 0; c < kKillClients; ++c) {
    clients.emplace_back([&, c] {
      Rng mine((seed * 100) + static_cast<uint64_t>(c));
      resp::RequestPipeline pipeline(resp::GlobalRegistry(), resp::ConnectionState{},
                                     resp::PipelineDependencies{.dispatcher = engine_.get()});
      for (size_t i = 0; i < kSlotsPerClient; ++i) {
        Call call{.client = c, .key = static_cast<int>(mine.Below(kCrashKeys))};
        call.op = PickOp(mine.Below(100));
        call.id = ids.fetch_add(1);
        Slot& slot = journal.At(c, i);
        slot.op = static_cast<uint8_t>(call.op);
        slot.key = static_cast<uint8_t>(call.key);
        slot.id = call.id;
        slot.call = clock.fetch_add(1);
        Journal::Publish(slot, 1);
        const std::string reply =
            Text(pipeline.Dispatch(core::RespCommand{.args = ArgsOf(call)}), Writes(call.op));
        slot.ret = clock.fetch_add(1);
        slot.reply_len = static_cast<uint8_t>(std::min(reply.size(), slot.reply.size()));
        std::copy_n(reply.begin(), slot.reply_len, slot.reply.begin());
        Journal::Publish(slot, 2);
        done.fetch_add(1);
      }
    });
  }
  while (done.load() < 2000) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  abyss::testing::SignalReady(dir, "victim.ready", "");
  for (auto& client : clients) client.join();
  abyss::testing::SignalReadyAndPark(dir, "victim.done", "");
}

// A kill -9 at process_crash: every write the victim acknowledged is
// recovered, and every reply it gave is explained by the recovered log.
TEST_F(CrashTest, KillNineAtProcessCrashRecoversEveryAckAndExplainsEveryReply) {
  bool is_victim = false;
  (void)abyss::testing::VictimDirFromEnv(kVictimEnv, &is_victim);
  if (is_victim) GTEST_SKIP();
  std::vector<uint64_t> seeds = {1, 2};
  if (kSanitizerDivisor > 1) seeds = {1};
  if (const auto count = EnvNumber("ABYSS_CRASH_KILLS")) {
    seeds.clear();
    for (uint64_t seed = 100; seed < 100 + *count; ++seed) seeds.push_back(seed);
  }
  Tally total;
  for (const uint64_t seed : seeds) {
    const auto dir = dir_.Sub("kill" + std::to_string(seed));
    std::filesystem::create_directories(dir);
    {
      std::ofstream out(dir / "seed");
      out << seed;
    }
    const auto outcome =
        abyss::testing::SpawnAndKillVictim({.gtest_filter = "CrashTest.KillNineVictim",
                                            .dir_env_var = kVictimEnv,
                                            .dir = dir,
                                            .ready_deadline = std::chrono::seconds(10)});
    ASSERT_TRUE(outcome.error.empty()) << outcome.error;
    ASSERT_TRUE(outcome.reached_ready);
    ASSERT_TRUE(outcome.died_by_signal);

    std::vector<Call> calls;
    uint64_t in_flight = 0;
    {
      Journal journal(dir / "journal", /*create=*/false);
      ASSERT_TRUE(journal.ok());
      for (int c = 0; c < kKillClients; ++c) {
        for (size_t i = 0; i < kSlotsPerClient; ++i) {
          const Slot& slot = journal.At(c, i);
          if (slot.state == 1) ++in_flight;
          if (slot.state != 2) continue;
          calls.push_back(Call{.client = c,
                               .call = slot.call,
                               .ret = slot.ret,
                               .op = static_cast<Op>(slot.op),
                               .key = slot.key,
                               .id = slot.id,
                               .reply = std::string(slot.reply.data(), slot.reply_len)});
        }
      }
    }
    std::cout << "[crash] kill -9 seed=" << seed << ": " << calls.size() << " replied, "
              << in_flight << " in flight" << '\n'
              << std::flush;
    auto recovered = RecoverAt(KillOptions(seed, dir), kKillStartMs);
    ASSERT_TRUE(recovered.has_value()) << recovered.error().message();
    const Tally run = Check(seed, calls, WholeLog(), /*acks_survive=*/true);
    Add(total, run);
    CloseAll();
    if (!run.ok) break;
  }
  std::cout << "[crash] kill -9: " << seeds.size() << " kills, " << total.calls << " calls ("
            << total.refused << " refused or timed out), " << total.acknowledged
            << " acknowledged writes recovered, " << total.checked_replies << " replies explained"
            << '\n'
            << std::flush;
  if (!HasFailure()) ExpectServed(total);
}

#endif  // !_WIN32

// A crash at every frame of a cross-shard batch, and inside each frame:
// MSET, multi-key DEL and FLUSHDB each recover whole or not at all, on
// every shard, through the log and the engine's recovery.
TEST_F(CrashTest, ACrossShardBatchRecoversWholeOrNotAtAllAtEveryFrame) {
  namespace fs = std::filesystem;
  constexpr uint32_t kShards = 4;
  struct Batch {
    std::string name;
    std::vector<std::string> args;
    // A key's value with the batch applied.
    std::function<std::string(uint32_t)> applied;
  };
  Options options;
  options.shards = kShards;
  options.durability = core::Durability::kPowerLoss;
  options.segment_size_bytes = size_t{64} << 10;
  PatientColdReads(options);
  options.run = "batch-";
  const auto key = [this](uint32_t shard) { return KeyOn(shard, 0, "b"); };
  std::vector<Batch> batches = {
      {.name = "MSET", .args = {"MSET"}, .applied = [](uint32_t) { return std::string("new"); }},
      {.name = "DEL", .args = {"DEL"}, .applied = [](uint32_t) { return std::string("nil"); }},
      {.name = "FLUSHDB",
       .args = {"FLUSHDB"},
       .applied = [](uint32_t) { return std::string("nil"); }},
  };
  // Under a sanitizer, one batch kind keeps the test inside its timeout.
  if (kSanitizerDivisor > 1) batches.resize(1);
  uint64_t cuts = 0;
  for (auto& batch : batches) {
    options.run = "batch-" + batch.name + "-";
    OpenAt(options, 1'700'000'000'000);
    for (uint32_t shard = 0; shard < kShards; ++shard) {
      if (batch.name == "MSET") batch.args.insert(batch.args.end(), {key(shard), "new"});
      if (batch.name == "DEL") batch.args.push_back(key(shard));
      ASSERT_EQ(RedisModel::Render(Run({"SET", key(shard), "old"})), "+OK");
    }
    ASSERT_EQ(RedisModel::Render(Run(batch.args)).front(), batch.name == "DEL" ? ':' : '+');
    // Each shard's frame of the batch, and the batch's end.
    std::vector<uint64_t> starts;
    for (core::ShardId shard = 0; shard < kShards; ++shard) {
      const auto end = queue_->DurableEnd(shard, core::Durability::kPowerLoss);
      ASSERT_TRUE(end.has_value());
      const auto pos = queue_->PositionForTesting(shard, *end - 1);
      ASSERT_TRUE(pos.has_value());
      starts.push_back(pos.value_or(0));
    }
    std::ranges::sort(starts);
    const queue::DurableExtent durable = queue_->DurableExtentForTesting(0);
    const uint64_t frame_space = options.segment_size_bytes - 4096;
    const uint64_t end =
        (std::stoull(fs::path(durable.path).stem().string()) * frame_space) + durable.offset - 4096;
    const fs::path wal = dir_.Sub(options.run + "wal");
    const fs::path backup = dir_.Sub(options.run + "backup");
    CloseAll();
    fs::create_directories(backup);
    fs::copy(wal, backup / "wal", fs::copy_options::recursive);
    fs::copy(dir_.Sub(options.run + "cold"), backup / "cold", fs::copy_options::recursive);

    std::vector<uint64_t> at;
    for (const uint64_t start : starts) at.insert(at.end(), {start, start + 8, start + 40});
    at.push_back(end);
    for (const uint64_t cut : at) {
      SCOPED_TRACE(batch.name + " cut at " + std::to_string(cut) + " of [" +
                   std::to_string(starts.front()) + ", " + std::to_string(end) + ")");
      ++cuts;
      CloseAll();
      fs::remove_all(wal);
      fs::remove_all(dir_.Sub(options.run + "cold"));
      fs::copy(backup / "wal", wal, fs::copy_options::recursive);
      fs::copy(backup / "cold", dir_.Sub(options.run + "cold"), fs::copy_options::recursive);
      abyss::testing::SimulatePowerLoss(
          abyss::testing::ExtentAt(wal, 0, options.segment_size_bytes, cut));
      auto restarted = RestartAt();
      ASSERT_TRUE(restarted.has_value()) << restarted.error().message();
      std::set<std::string> seen;
      for (uint32_t shard = 0; shard < kShards; ++shard) {
        std::string got = RedisModel::Render(Run({"GET", key(shard)}));
        if (got.starts_with("$")) got.erase(0, 1);
        if (got == batch.applied(shard)) {
          seen.insert("applied");
        } else if (got == "old") {
          seen.insert("not");
        } else {
          seen.insert(got);
        }
      }
      EXPECT_EQ(seen.size(), 1U) << "a torn batch: shards disagree";
      EXPECT_EQ(*seen.begin(), cut >= end ? "applied" : "not");
    }
    CloseAll();
  }
  std::cout << "[crash] cross-shard batches: " << cuts << " cuts recovered whole or not at all"
            << '\n'
            << std::flush;
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
