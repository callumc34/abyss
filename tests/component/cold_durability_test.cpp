// Oracle for the A6 cold-durable-checkpoint contract: "the cold consumer acked
// WAL seq N" must imply "every write through N is on cold's stable storage".
//
// No system or integration test can observe a violation of that contract. Every
// post-restart read-back in the system suite is served by the hot tier, which is
// a volatile materialised view rebuilt by pure queue replay, so a cold-tier
// durability loss is repaired by replay before any assertion can see it. This
// test therefore reads the cold store directly and deliberately never replays
// the acked WAL prefix — replaying it would reconstruct exactly the data whose
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

TEST(ColdDurabilityTest, ColdAckedPrefixSurvivesKillWithoutWalReplay) {
  GTEST_SKIP() << "requires POSIX process spawn and the RocksDB cold backend";
}

}  // namespace
}  // namespace abyss::cold

#else

#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <crt_externs.h>
#include <mach-o/dyld.h>
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/cold_consumer.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/queue/wal_queue.h"

namespace abyss::cold {
namespace {

using namespace std::chrono_literals;

constexpr const char* kVictimDirEnv = "ABYSS_COLD_DURABILITY_VICTIM_DIR";
constexpr const char* kVictimFilter = "--gtest_filter=ColdDurabilityVictim.Run";
constexpr const char* kReadyFileName = "victim.ready";
constexpr core::ShardId kShard = 0;
constexpr uint32_t kShardCount = 1;
// Arm A (the contract): flushed, checkpointed and acked through a real store.
constexpr int kDurableKeys = 24;
// Arm B (the negative control): flushed and acked through a store whose
// Checkpoint is stubbed out, i.e. the exact mutant this file exists to catch.
constexpr int kMutantKeys = 8;
constexpr auto kReadyDeadline = 90s;
constexpr auto kReadyPollInterval = 2ms;

// WAL seq range an arm appended, and the offset its cold consumer persisted.
struct ArmReport {
  core::SequenceId first = 0;
  core::SequenceId last = 0;
  core::SequenceId ack = 0;
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
      // Long enough that the reaper cannot remove a segment mid-test.
      .min_retention = std::chrono::seconds{3600},
      .retention_consumers = {core::kHotConsumer, core::kColdConsumer},
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
  const core::ops::ReadOp op = core::ops::StringGet{.key = key};
  auto value = cold.Exec(op);
  if (!value.has_value() || value->IsNull()) return std::nullopt;
  return value->AsString();
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
  using PromotionCommand = core::Result<std::optional<core::RespCommand> >;

  explicit NoCheckpointColdStore(core::ColdStore& inner) : inner_(inner) {}

  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op,
                                     std::optional<core::Duration> deadline) override {
    return inner_.Exec(op, deadline);
  }
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
  PromotionCommand GetPromotionCommand(std::string_view key) override {
    return inner_.GetPromotionCommand(key);
  }

 private:
  core::ColdStore& inner_;
};

// Appends `count` SETs, then drives a real ColdConsumer through
// drain -> flush -> checkpoint -> ack against `cold`.
void RunArm(core::Queue& queue, core::ColdStore& cold, std::string_view prefix, int count,
            const core::EvictionPolicy& eviction, core::ConsumerRpc& rpc, ArmReport* out) {
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

  auto appended = queue.AppendBatch(kShard, entries);
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
  consumer::ColdConsumer cold_consumer(queue, cold, kShard, config, eviction, rpc);

  const std::atomic<bool> cancel{false};
  auto replay = cold_consumer.ReplayUntil(appended->last_seq, cancel);
  ASSERT_TRUE(replay.has_value()) << replay.error().message();

  auto ack = queue.AckOffset(core::kColdConsumer, kShard);
  ASSERT_TRUE(ack.has_value()) << ack.error().message();

  out->first = appended->first_seq;
  out->last = appended->last_seq;
  out->ack = *ack;
}

// Published under a rename so the parent can never read a partial record.
void PublishReport(const std::filesystem::path& dir, const VictimReport& report) {
  const auto tmp = dir / "victim.ready.tmp";
  {
    std::ofstream out(tmp, std::ios::trunc);
    ASSERT_TRUE(out.good());
    // Fixed field order: durable {first,last,ack}, then mutant {first,last,ack}.
    out << report.durable.first << ' ' << report.durable.last << ' ' << report.durable.ack << ' '
        << report.mutant.first << ' ' << report.mutant.last << ' ' << report.mutant.ack << '\n';
    ASSERT_TRUE(out.good());
  }
  std::error_code ec;
  std::filesystem::rename(tmp, dir / kReadyFileName, ec);
  ASSERT_FALSE(ec) << ec.message();
}

TEST(ColdDurabilityVictim, Run) {
  const char* dir_env = std::getenv(kVictimDirEnv);
  if (dir_env == nullptr) {
    GTEST_SKIP() << "crash victim; driven out-of-process by ColdDurabilityTest";
  }
  const std::filesystem::path dir{dir_env};

  const core::EvictionPolicy eviction{core::EvictionTTL{86400}};
  core::ConsumerRpc rpc;

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
      RunArm(**durable_queue, **durable_cold, "a", kDurableKeys, eviction, rpc, &report.durable));
  ASSERT_NO_FATAL_FAILURE(
      RunArm(**mutant_queue, stubbed_cold, "b", kMutantKeys, eviction, rpc, &report.mutant));

  ASSERT_NO_FATAL_FAILURE(PublishReport(dir, report));

  // Park with every queue and cold store still open and undestroyed. A clean
  // shutdown would flush RocksDB's WAL buffer and erase the distinction under
  // test; the parent SIGKILLs us here instead.
  for (;;) {
    ::pause();
  }
}

// ---------------------------------------------------------------------------
// Parent process
// ---------------------------------------------------------------------------

char** CurrentEnviron() {
#if defined(__APPLE__)
  return *_NSGetEnviron();
#else
  return environ;
#endif
}

std::optional<std::filesystem::path> SelfExecutablePath() {
  std::filesystem::path raw;
#if defined(__APPLE__)
  constexpr size_t kMaxPathBytes = 4096;
  std::string buf(kMaxPathBytes, '\0');
  auto size = static_cast<uint32_t>(buf.size());
  if (_NSGetExecutablePath(buf.data(), &size) != 0) return std::nullopt;
  buf.resize(std::strlen(buf.c_str()));
  raw = buf;
#else
  std::error_code link_ec;
  raw = std::filesystem::read_symlink("/proc/self/exe", link_ec);
  if (link_ec) return std::nullopt;
#endif
  std::error_code ec;
  auto resolved = std::filesystem::weakly_canonical(raw, ec);
  return ec ? raw : resolved;
}

// Re-execs this test binary filtered to the victim test. GTEST_* variables are
// dropped from the child environment so an inherited filter, shard index or
// output path cannot silently turn the victim into a no-op.
pid_t SpawnVictim(const std::filesystem::path& exe, const std::filesystem::path& dir) {
  std::string exe_arg = exe.string();
  std::string filter_arg = kVictimFilter;
  std::vector<char*> argv{exe_arg.data(), filter_arg.data(), nullptr};

  std::vector<std::string> env_storage;
  for (char** e = CurrentEnviron(); e != nullptr && *e != nullptr; ++e) {
    const std::string_view entry{*e};
    if (entry.starts_with("GTEST_")) continue;
    env_storage.emplace_back(entry);
  }
  env_storage.emplace_back(std::string(kVictimDirEnv) + "=" + dir.string());

  std::vector<char*> envp;
  envp.reserve(env_storage.size() + 1);
  for (auto& entry : env_storage) envp.push_back(entry.data());
  envp.push_back(nullptr);

  pid_t pid = -1;
  if (::posix_spawn(&pid, exe_arg.c_str(), nullptr, nullptr, argv.data(), envp.data()) != 0) {
    return -1;
  }
  return pid;
}

std::optional<VictimReport> ReadReport(const std::filesystem::path& path) {
  std::ifstream in(path);
  if (!in) return std::nullopt;
  VictimReport report;
  in >> report.durable.first >> report.durable.last >> report.durable.ack >> report.mutant.first >>
      report.mutant.last >> report.mutant.ack;
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
    const auto exe = SelfExecutablePath();
    ASSERT_TRUE(exe.has_value()) << "could not resolve this test binary's path";
    const pid_t pid = SpawnVictim(*exe, tmp_dir_);
    ASSERT_GT(pid, 0) << "posix_spawn of the crash victim failed";

    const auto ready_path = tmp_dir_ / kReadyFileName;
    const auto deadline = std::chrono::steady_clock::now() + kReadyDeadline;
    bool ready = false;
    while (std::chrono::steady_clock::now() < deadline) {
      if (std::filesystem::exists(ready_path)) {
        ready = true;
        break;
      }
      int early_status = 0;
      if (::waitpid(pid, &early_status, WNOHANG) == pid) {
        FAIL() << "crash victim exited before signalling ready (raw status " << early_status << ")";
      }
      std::this_thread::sleep_for(kReadyPollInterval);
    }

    ASSERT_EQ(::kill(pid, SIGKILL), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(ready) << "crash victim never reached its ready point";
    ASSERT_NE(WIFSIGNALED(status), 0)
        << "victim must die by signal: a clean exit flushes RocksDB's WAL buffer and the test "
           "would prove nothing";
    EXPECT_EQ(WTERMSIG(status), SIGKILL);

    auto report = ReadReport(ready_path);
    ASSERT_TRUE(report.has_value()) << "victim report unreadable";
    report_ = *report;
  }

  core::SequenceId PersistedColdAck(const std::filesystem::path& wal_dir) {
    auto reopened = queue::WalQueue::Open(MakeWalConfig(wal_dir));
    EXPECT_TRUE(reopened.has_value());
    if (!reopened.has_value()) return 0;
    auto ack = (*reopened)->AckOffset(core::kColdConsumer, kShard);
    EXPECT_TRUE(ack.has_value());
    return ack.has_value() ? *ack : 0;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::filesystem::path tmp_dir_;
  VictimReport report_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(ColdDurabilityTest, ColdAckedPrefixSurvivesKillWithoutWalReplay) {
  ASSERT_NO_FATAL_FAILURE(CrashVictim());

  const core::SequenceId ack = PersistedColdAck(tmp_dir_ / "wal_durable");
  EXPECT_EQ(ack, report_.durable.ack) << "the cold ack itself was not durable across the kill";
  ASSERT_EQ(ack, report_.durable.last)
      << "cold did not ack the whole appended prefix, so this arm asserts nothing";

  // Reopen cold from disk only. No ColdConsumer, no hot store, and above all no
  // replay of the acked WAL prefix: replay would rebuild the very writes whose
  // durability is under test.
  auto cold = backends::RocksdbStore::Create(MakeColdConfig(tmp_dir_ / "cold_durable"));
  ASSERT_TRUE(cold.has_value()) << cold.error().message();

  for (core::SequenceId seq = report_.durable.first; seq <= ack; ++seq) {
    const uint64_t index = seq - report_.durable.first;
    const std::string key = KeyFor("a", index);
    const auto value = ReadCold(**cold, key);
    ASSERT_TRUE(value.has_value())
        << "A6 violation: the cold consumer acked WAL seq " << ack << ", which covers seq " << seq
        << ", but '" << key
        << "' is absent from cold's stable storage after SIGKILL. An acked WAL prefix is reapable, "
           "so this write is recoverable from neither tier.";
    EXPECT_EQ(*value, ValueFor(index));
  }
}

// Without this, the arm above could pass on a machine where the kill loses
// nothing, and a no-op Checkpoint would go undetected.
TEST_F(ColdDurabilityTest, UncheckpointedAckedWritesAreDetectablyLost) {
  ASSERT_NO_FATAL_FAILURE(CrashVictim());

  const core::SequenceId ack = PersistedColdAck(tmp_dir_ / "wal_mutant");
  ASSERT_EQ(ack, report_.mutant.last)
      << "the stubbed-checkpoint arm did not ack, so it demonstrates nothing";

  auto cold = backends::RocksdbStore::Create(MakeColdConfig(tmp_dir_ / "cold_mutant"));
  ASSERT_TRUE(cold.has_value()) << cold.error().message();

  size_t survivors = 0;
  for (core::SequenceId seq = report_.mutant.first; seq <= ack; ++seq) {
    if (ReadCold(**cold, KeyFor("b", seq - report_.mutant.first)).has_value()) ++survivors;
  }
  EXPECT_EQ(survivors, 0U)
      << "with Checkpoint stubbed to a no-op the acked writes must be gone after SIGKILL. They "
         "survived, so this kill does not actually destroy uncheckpointed cold state and "
         "ColdAckedPrefixSurvivesKillWithoutWalReplay cannot fail.";
}

}  // namespace
}  // namespace abyss::cold

#endif  // _WIN32 || !ABYSS_HAVE_ROCKSDB
