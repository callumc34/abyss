// Oracle for the A6 cold-durable-checkpoint contract: "the cold consumer committed
// WAL seq N" must imply "every write through N is on cold's stable storage".
//
// No system or integration test can observe a violation of that contract. Every
// post-restart read-back in the system suite is served by the hot tier, which is
// a volatile materialised view rebuilt by pure queue replay, so a cold-tier
// durability loss is repaired by replay before any assertion can see it. This
// test therefore reads the cold store directly and deliberately never replays
// the committed WAL prefix — replaying it would reconstruct exactly the data whose
// loss is under test.
//
// The crash victim is a separate process launched through posix_spawn (exec,
// not a bare fork): the victim runs WAL group-commit and RocksDB background
// threads, and forking such a process leaves their mutexes locked forever in
// the child.

#include <gtest/gtest.h>

#if defined(_WIN32) || !defined(ABYSS_HAVE_ROCKSDB)

namespace abyss::cold {
namespace {

TEST(ColdDurabilityTest, ColdCommittedPrefixSurvivesKillWithoutWalReplay) {
  GTEST_SKIP() << "requires POSIX process spawn and the RocksDB cold backend";
}

}  // namespace
}  // namespace abyss::cold

#else

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/queue/wal_queue.h"
#include "cold_replay.h"
#include "crash_harness.h"

namespace abyss::cold {
namespace {

// A committing consumer besides cold; the queue treats ids alike.
constexpr core::ConsumerId kTestConsumer = 0;

using namespace std::chrono_literals;

constexpr const char* kVictimDirEnv = "ABYSS_COLD_DURABILITY_VICTIM_DIR";
constexpr const char* kVictimFilter = "ColdDurabilityVictim.Run";
constexpr const char* kReadyFileName = "victim.ready";
constexpr core::ShardId kShard = 0;
constexpr uint32_t kShardCount = 1;
// Arm A (the contract): flushed, checkpointed and committed through a real store.
constexpr int kDurableKeys = 24;
// Arm B (the negative control): flushed and committed through a store whose
// Checkpoint is stubbed out, i.e. the exact mutant this file exists to catch.
constexpr int kMutantKeys = 8;
constexpr auto kReadyDeadline = 90s;

// WAL seq range an arm appended, and the offset its cold consumer persisted.
struct ArmReport {
  core::SequenceId first = 0;
  core::SequenceId last = 0;
  core::SequenceId committed = 0;
};

struct VictimReport {
  ArmReport durable;
  ArmReport mutant;
};

queue::WalConfig MakeWalConfig(const std::filesystem::path& dir) {
  return queue::WalConfig{
      .wal_path = dir.string(),
      .segment_size_bytes = size_t{1} << 20U,
      .shard_count = kShardCount,
      // The append future then means power-durable, so cold may commit it.
      .durability = core::Durability::kPowerLoss,
      // Long enough that the reaper cannot remove a segment mid-test.
      .min_retention = std::chrono::seconds{3600},
      .retention_consumers = {kTestConsumer, core::kColdConsumer},
  };
}

backends::RocksdbConfig MakeColdConfig(const std::filesystem::path& dir) {
  return backends::RocksdbConfig{.data_path = dir.string(), .shard_count = kShardCount};
}

std::string KeyFor(std::string_view prefix, uint64_t index) {
  return std::string(prefix) + ":" + std::to_string(index);
}

std::string ValueFor(uint64_t index) { return "v" + std::to_string(index); }

// Reads a key straight out of cold's stable storage: no hot tier, no compaction
// buffer, no queue replay. This is the only view that can see the loss.
std::optional<std::string> ReadCold(core::ColdStore& cold, const std::string& key) {
  auto loaded = cold.LoadKey(key, core::SteadyClock::now() + 10s);
  if (!loaded.has_value() || !loaded->has_value()) return std::nullopt;
  const auto* value = std::get_if<std::string>(&(*loaded)->value);
  if (value == nullptr) return std::nullopt;
  return *value;
}

// ---------------------------------------------------------------------------
// Crash victim (child process)
// ---------------------------------------------------------------------------

// The mutant: ApplyBatch still lands in the memtable and RocksDB's in-process
// WAL buffer, but nothing is ever fsynced. Reverting ColdStore::Checkpoint to a
// no-op produces precisely this store, so arm B shows what the durable arm's
// assertions look like when the contract is broken.
class NoCheckpointColdStore : public core::ColdStore {
 public:
  explicit NoCheckpointColdStore(core::ColdStore& inner) : inner_(inner) {}

  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::SequenceId highest_wal_seq) override {
    return inner_.ApplyBatch(ops, highest_wal_seq);
  }
  core::Result<void> Checkpoint(core::ShardId /*shard*/,
                                core::SequenceId /*up_to_wal_seq*/) override {
    return {};
  }
  core::Result<void> Wipe(core::ShardId shard) override { return inner_.Wipe(shard); }
  core::Result<core::StorageStats> Stats() override { return inner_.Stats(); }
  core::Result<void> Compact() override { return inner_.Compact(); }
  core::Result<std::optional<core::ColdKeyState> > LoadKey(std::string_view key,
                                                           core::SteadyTime deadline) override {
    return inner_.LoadKey(key, deadline);
  }
  core::Result<std::optional<core::KeyMeta> > ProbeKey(std::string_view key,
                                                       core::SteadyTime deadline) override {
    return inner_.ProbeKey(key, deadline);
  }
  core::Result<std::optional<core::LoadedAs> > LoadKeyAs(std::string_view key, core::KeyType type,
                                                         core::SteadyTime deadline) override {
    return inner_.LoadKeyAs(key, type, deadline);
  }
  core::Result<std::vector<std::optional<core::MemberValue> > > LoadMembers(
      std::string_view key, core::KeyType type, std::span<const std::string_view> members,
      core::SteadyTime deadline) override {
    return inner_.LoadMembers(key, type, members, deadline);
  }

 private:
  core::ColdStore& inner_;
};

// Appends `count` SETs, then drives a real ColdConsumer through
// drain -> flush -> checkpoint -> commit against `cold`.
void RunArm(core::Queue& queue, core::ColdStore& cold, std::string_view prefix, int count,
            const core::EvictionPolicy& eviction, ArmReport* out) {
  std::vector<core::QueueEntry> entries;
  entries.reserve(static_cast<size_t>(count));
  for (int i = 0; i < count; ++i) {
    core::QueueEntry entry;
    entry.appended_at = core::WallClock::now();
    entry.payload = core::entry::Write{
        .cmd = core::RespCommand{.args = {"SET", KeyFor(prefix, static_cast<uint64_t>(i)),
                                          ValueFor(static_cast<uint64_t>(i))}}};
    entries.push_back(std::move(entry));
  }

  auto appended = queue.AppendBatch(kShard, entries, core::SteadyClock::now() + 10s);
  ASSERT_TRUE(appended.has_value()) << appended.error().message();
  auto wal_durable = appended->durable.get();
  ASSERT_TRUE(wal_durable.has_value()) << wal_durable.error().message();

  consumer::ColdConsumer::Config config;
  // Checkpoint on every flush: the cadence must not depend on wall-clock timing
  // for this test to be deterministic.
  config.checkpoint_max_flushes = 1;
  config.checkpoint_min_interval = 0ms;
  config.queue_read_timeout = 50ms;
  config.rng_seed = 1;
  consumer::ColdConsumer cold_consumer(queue, cold, kShard, config, eviction);

  const std::atomic<bool> cancel{false};
  auto replay = abyss::testing::ReplayCold(queue, cold_consumer, appended->last_seq + 1, cancel);
  ASSERT_TRUE(replay.has_value()) << replay.error().message();

  auto committed = queue.CommittedOffset(core::kColdConsumer, kShard);
  ASSERT_TRUE(committed.has_value()) << committed.error().message();
  ASSERT_TRUE(committed->has_value()) << "the cold consumer committed nothing";

  out->first = appended->first_seq;
  out->last = appended->last_seq;
  out->committed = committed->value_or(0);
}

// Fixed field order: durable {first,last,committed}, then mutant {first,last,committed}.
std::string SerializeReport(const VictimReport& report) {
  std::ostringstream out;
  out << report.durable.first << ' ' << report.durable.last << ' ' << report.durable.committed
      << ' ' << report.mutant.first << ' ' << report.mutant.last << ' ' << report.mutant.committed
      << '\n';
  return out.str();
}

TEST(ColdDurabilityVictim, Run) {
  bool is_victim = false;
  const auto dir = abyss::testing::VictimDirFromEnv(kVictimDirEnv, &is_victim);
  if (!is_victim) {
    GTEST_SKIP() << "crash victim; driven out-of-process by ColdDurabilityTest";
  }

  const core::EvictionPolicy eviction{core::EvictionTTL{86400}};

  auto durable_queue = queue::WalQueue::Open(MakeWalConfig(dir / "wal_durable"));
  ASSERT_TRUE(durable_queue.has_value()) << durable_queue.error().message();
  auto durable_cold = backends::RocksdbStore::Create(MakeColdConfig(dir / "cold_durable"));
  ASSERT_TRUE(durable_cold.has_value()) << durable_cold.error().message();

  auto mutant_queue = queue::WalQueue::Open(MakeWalConfig(dir / "wal_mutant"));
  ASSERT_TRUE(mutant_queue.has_value()) << mutant_queue.error().message();
  auto mutant_cold = backends::RocksdbStore::Create(MakeColdConfig(dir / "cold_mutant"));
  ASSERT_TRUE(mutant_cold.has_value()) << mutant_cold.error().message();
  NoCheckpointColdStore stubbed_cold{**mutant_cold};

  VictimReport report;
  ASSERT_NO_FATAL_FAILURE(
      RunArm(**durable_queue, **durable_cold, "a", kDurableKeys, eviction, &report.durable));
  ASSERT_NO_FATAL_FAILURE(
      RunArm(**mutant_queue, stubbed_cold, "b", kMutantKeys, eviction, &report.mutant));
  // Committed offsets persist lazily; checkpoint them so the kill tests the
  // cold store's durability, not the offset cadence.
  ASSERT_TRUE((*durable_queue)->FlushOffsets().has_value());
  ASSERT_TRUE((*mutant_queue)->FlushOffsets().has_value());

  // Parks with every queue and cold store still open and undestroyed. A clean
  // shutdown would flush RocksDB's WAL buffer and erase the distinction under
  // test; the parent SIGKILLs us here instead.
  abyss::testing::SignalReadyAndPark(dir, kReadyFileName, SerializeReport(report));
}

// ---------------------------------------------------------------------------
// Parent process
// ---------------------------------------------------------------------------

std::optional<VictimReport> ParseReport(const std::string& payload) {
  std::istringstream in(payload);
  VictimReport report;
  in >> report.durable.first >> report.durable.last >> report.durable.committed >>
      report.mutant.first >> report.mutant.last >> report.mutant.committed;
  if (in.fail()) return std::nullopt;
  return report;
}

class ColdDurabilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_cold_durability_XXXXXX";
    std::string path = tmpl.string();
    ASSERT_NE(::mkdtemp(path.data()), nullptr);
    tmp_dir_ = path;
  }

  void TearDown() override {
    if (!tmp_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_dir_, ec);
    }
  }

  // Runs the victim to its ready point and kills it there.
  void CrashVictim() {
    const auto outcome = abyss::testing::SpawnAndKillVictim(abyss::testing::VictimSpec{
        .gtest_filter = kVictimFilter,
        .dir_env_var = kVictimDirEnv,
        .dir = tmp_dir_,
        .ready_file_name = kReadyFileName,
        .ready_deadline = kReadyDeadline,
    });

    ASSERT_TRUE(outcome.error.empty()) << outcome.error;
    ASSERT_TRUE(outcome.reached_ready) << "crash victim never reached its ready point";
    ASSERT_TRUE(outcome.died_by_signal)
        << "victim must die by signal: a clean exit flushes RocksDB's WAL buffer and the test "
           "would prove nothing";
    EXPECT_EQ(outcome.term_signal, SIGKILL);

    auto report = ParseReport(outcome.ready_payload);
    ASSERT_TRUE(report.has_value())
        << "victim report unreadable: '" << outcome.ready_payload << "'";
    // ASSERT_TRUE above returns from this void function when the optional is
    // empty; the analyser does not model gtest's macro expansion.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    report_ = *report;
  }

  core::SequenceId PersistedColdCommit(const std::filesystem::path& wal_dir) {
    auto reopened = queue::WalQueue::Open(MakeWalConfig(wal_dir));
    EXPECT_TRUE(reopened.has_value());
    if (!reopened.has_value()) return 0;
    auto committed = (*reopened)->CommittedOffset(core::kColdConsumer, kShard);
    EXPECT_TRUE(committed.has_value() && committed->has_value());
    return committed.has_value() ? committed->value_or(0) : 0;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::filesystem::path tmp_dir_;
  VictimReport report_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(ColdDurabilityTest, ColdCommittedPrefixSurvivesKillWithoutWalReplay) {
  ASSERT_NO_FATAL_FAILURE(CrashVictim());

  const core::SequenceId committed = PersistedColdCommit(tmp_dir_ / "wal_durable");
  EXPECT_EQ(committed, report_.durable.committed)
      << "the cold commit itself was not durable across the kill";
  ASSERT_EQ(committed, report_.durable.last)
      << "cold did not commit the whole appended prefix, so this arm asserts nothing";

  // Reopen cold from disk only. No ColdConsumer, no hot store, and above all no
  // replay of the committed WAL prefix: replay would rebuild the very writes whose
  // durability is under test.
  auto cold = backends::RocksdbStore::Create(MakeColdConfig(tmp_dir_ / "cold_durable"));
  ASSERT_TRUE(cold.has_value()) << cold.error().message();

  for (core::SequenceId seq = report_.durable.first; seq <= committed; ++seq) {
    const uint64_t index = seq - report_.durable.first;
    const std::string key = KeyFor("a", index);
    const auto value = ReadCold(**cold, key);
    ASSERT_TRUE(value.has_value()) << "A6 violation: the cold consumer committed WAL seq "
                                   << committed << ", which covers seq " << seq << ", but '" << key
                                   << "' is absent from cold's stable storage after SIGKILL. A "
                                      "committed WAL prefix is reapable, "
                                      "so this write is recoverable from neither tier.";
    // Guarded by the ASSERT_TRUE above; see the note at report_ assignment.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    EXPECT_EQ(*value, ValueFor(index));
  }
}

// Without this, the arm above could pass on a machine where the kill loses
// nothing, and a no-op Checkpoint would go undetected.
TEST_F(ColdDurabilityTest, UncheckpointedCommittedWritesAreDetectablyLost) {
  ASSERT_NO_FATAL_FAILURE(CrashVictim());

  const core::SequenceId committed = PersistedColdCommit(tmp_dir_ / "wal_mutant");
  ASSERT_EQ(committed, report_.mutant.last)
      << "the stubbed-checkpoint arm did not commit, so it demonstrates nothing";

  auto cold = backends::RocksdbStore::Create(MakeColdConfig(tmp_dir_ / "cold_mutant"));
  ASSERT_TRUE(cold.has_value()) << cold.error().message();

  size_t survivors = 0;
  for (core::SequenceId seq = report_.mutant.first; seq <= committed; ++seq) {
    if (ReadCold(**cold, KeyFor("b", seq - report_.mutant.first)).has_value()) ++survivors;
  }
  EXPECT_EQ(survivors, 0U)
      << "with Checkpoint stubbed to a no-op the committed writes must be gone after SIGKILL. They "
         "survived, so this kill does not actually destroy uncheckpointed cold state and "
         "ColdCommittedPrefixSurvivesKillWithoutWalReplay cannot fail.";
}

}  // namespace
}  // namespace abyss::cold

#endif  // _WIN32 || !ABYSS_HAVE_ROCKSDB
