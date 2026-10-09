#include "abyss/cold/backends/rocksdb_store.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <array>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/cold/format/key_codec.h"
#include "abyss/cold/ttl_scanner.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_format.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/shard_router.h"
#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.cold.store")

namespace abyss::cold::backends {

namespace fmt = ::abyss::cold::format;

namespace {

using core::Error;
using core::ErrorCode;
using core::RespValue;

constexpr std::string_view kZsetScoreIndexCfName = "zset_score_idx";

enum class ExpireOutcome : uint8_t {
  kDeleted,
  kNotExpired,
  kNotFound,
  kConflict,
};

ErrorCode MapStatusCode(const rocksdb::Status& status) {
  if (status.IsNotFound()) return ErrorCode::kNotFound;
  if (status.IsCorruption()) return ErrorCode::kCorruption;
  if (status.IsIOError()) return ErrorCode::kUnavailable;
  if (status.IsInvalidArgument()) return ErrorCode::kInvalidArgument;
  // A read whose ReadOptions::deadline elapsed surfaces as TimedOut. Map it to
  // kTimeout so the engine fails the scan closed and bumps the cold-scan
  // deadline metric rather than treating it as an internal fault (COLD-2).
  if (status.IsTimedOut()) return ErrorCode::kTimeout;
  return ErrorCode::kInternal;
}

Error FromStatus(const rocksdb::Status& status, std::string_view context) {
  Error err{MapStatusCode(status), std::string(context) + ": " + status.ToString()};
  // NotFound is a normal GET outcome; everything else is operator-actionable.
  if (err.code() != ErrorCode::kNotFound) {
    ABYSS_LOG_ERROR("rocksdb operation failed", {"op", std::string_view{context}},
                    {"status", status.ToString()});
  }
  return err;
}

rocksdb::ColumnFamilyOptions MakeCfOptions(const RocksdbConfig& config) {
  rocksdb::ColumnFamilyOptions cf_opts;
  cf_opts.write_buffer_size = config.write_buffer_size_bytes;
  cf_opts.max_write_buffer_number = static_cast<int>(config.max_write_buffer_number);
  switch (config.compaction_style) {
    case CompactionStyle::kLevel:
      cf_opts.compaction_style = rocksdb::kCompactionStyleLevel;
      break;
    case CompactionStyle::kUniversal:
      cf_opts.compaction_style = rocksdb::kCompactionStyleUniversal;
      break;
  }

  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.filter_policy.reset(
      rocksdb::NewBloomFilterPolicy(static_cast<double>(config.bloom_filter_bits_per_key)));
  cf_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

  return cf_opts;
}

rocksdb::Slice ToSlice(std::string_view sv) { return {sv.data(), sv.size()}; }

std::string_view ToSv(const rocksdb::Slice& s) { return {s.data(), s.size()}; }

// Computes the exclusive upper bound for a byte-prefix scan.
std::string LexicographicSuccessor(std::string_view prefix) {
  std::string out(prefix);
  while (!out.empty() && static_cast<uint8_t>(out.back()) == 0xFF) {
    out.pop_back();
  }
  if (out.empty()) return {};
  out.back() = static_cast<char>(static_cast<uint8_t>(out.back()) + 1);
  return out;
}

uint64_t WallMs(const core::WallClockFn& clock) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(clock().time_since_epoch()).count());
}

// Double parsing for ZRANGEBYSCORE bounds. Handles Redis "-inf"/"+inf" and the
// exclusive "(" prefix. Returns (value, is_exclusive) or an error.
struct ScoreBound {
  double value = 0.0;
  bool exclusive = false;
};

core::Result<ScoreBound> ParseScoreBound(std::string_view s, bool is_min) {
  if (s.empty()) {
    return std::unexpected(Error(ErrorCode::kInvalidArgument, "empty score bound"));
  }
  ScoreBound out;
  if (s.front() == '(') {
    out.exclusive = true;
    s.remove_prefix(1);
  }
  if (s == "+inf" || s == "inf") {
    out.value = std::numeric_limits<double>::infinity();
    return out;
  }
  if (s == "-inf") {
    out.value = -std::numeric_limits<double>::infinity();
    return out;
  }
  // std::from_chars does not support leading '+', strip it to match Redis.
  if (!s.empty() && s.front() == '+') s.remove_prefix(1);
  double v = 0.0;
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc{} || ptr != s.data() + s.size()) {
    return std::unexpected(Error(ErrorCode::kInvalidArgument,
                                 std::string(is_min ? "min" : "max") + " is not a valid double"));
  }
  out.value = v;
  return out;
}

// Parsed ZRANGEBYLEX bound. Redis lex syntax: `[value` (inclusive), `(value`
// (exclusive), `-` (negative infinity), `+` (positive infinity). A bare value
// is a syntax error. Total and non-throwing, mirroring ParseScoreBound and the
// hot-side ParseLexBound so cold and hot agree on validity bit-for-bit.
struct LexBound {
  std::string value;
  bool exclusive = false;
  bool neg_inf = false;
  bool pos_inf = false;
};

core::Result<LexBound> ParseLexBound(std::string_view s) {
  if (s == "-") return LexBound{.neg_inf = true};
  if (s == "+") return LexBound{.pos_inf = true};
  if (!s.empty() && s.front() == '[') {
    return LexBound{.value = std::string(s.substr(1)), .exclusive = false};
  }
  if (!s.empty() && s.front() == '(') {
    return LexBound{.value = std::string(s.substr(1)), .exclusive = true};
  }
  return std::unexpected(
      Error(ErrorCode::kInvalidArgument, "not a valid lex range bound: '" + std::string(s) + "'"));
}

bool LexAtOrAboveMin(std::string_view member, const LexBound& min) {
  if (min.neg_inf) return true;
  if (min.pos_inf) return false;
  return min.exclusive ? member > min.value : member >= min.value;
}

bool LexAtOrBelowMax(std::string_view member, const LexBound& max) {
  if (max.pos_inf) return true;
  if (max.neg_inf) return false;
  return max.exclusive ? member < max.value : member <= max.value;
}

// Decodes a zset member-indexed value: 8 bytes of native-endian IEEE 754.
core::Result<double> DecodeZsetMemberScore(std::string_view value) {
  if (value.size() != 8) {
    return std::unexpected(
        Error(ErrorCode::kCorruption, "zset member value must be exactly 8 bytes"));
  }
  uint64_t bits = 0;
  std::memcpy(&bits, value.data(), 8);
  return std::bit_cast<double>(bits);
}

std::string EncodeZsetMemberScoreValue(double score) {
  std::string out(8, '\0');
  auto bits = std::bit_cast<uint64_t>(score);
  std::memcpy(out.data(), &bits, 8);
  return out;
}

// 0 means no TTL, so a flagged 0, long expired, becomes 1.
int64_t LoadedTtl(uint8_t flags, uint64_t abs_ttl_ms) {
  if ((flags & fmt::kFlagHasTtl) == 0) return 0;
  return std::max<int64_t>(static_cast<int64_t>(abs_ttl_ms), 1);
}

Error LoadTimeout() { return {ErrorCode::kTimeout, "cold load deadline passed"}; }

// How many records a load visits between deadline checks.
constexpr uint64_t kDeadlineCheckMask = 1023;

}  // namespace

struct CfHandleDeleter {
  rocksdb::DB* db = nullptr;
  void operator()(rocksdb::ColumnFamilyHandle* h) const {
    if (db != nullptr && h != nullptr) db->DestroyColumnFamilyHandle(h);
  }
};
using CfHandle = std::unique_ptr<rocksdb::ColumnFamilyHandle, CfHandleDeleter>;

struct RocksdbStore::Impl : public TtlScannerBackend {
  RocksdbConfig config;
  std::unique_ptr<rocksdb::OptimisticTransactionDB> db;
  CfHandle default_cf;
  CfHandle zset_score_idx_cf;
  std::unique_ptr<TtlScanner> ttl_scanner;
  std::mt19937_64 rng{0};  // NOLINT(bugprone-random-generator-seed): re-seeded at Create.
  // Bumped on every TtlScanner delete.
  metrics::CounterHandle ttl_expired_total;
  mutable metrics::CounterHandle type_conflicts_total;

  // Per-shard durable frontier recorded by Checkpoint. Guards the recorded
  // value against concurrent ApplyBatch/Checkpoint from a single consumer; the
  // cold commit is computed from the value Checkpoint returns, not read here.
  std::mutex checkpoint_mu;
  std::unordered_map<core::ShardId, core::SequenceId> checkpointed_seq;

  // Reads judge expiry by this and never write; deletes follow LogClockMs.
  uint64_t NowMs() const { return WallMs(config.wall_clock); }
  uint64_t LogClockMs(core::ShardId shard) const {
    return config.log_clock ? config.log_clock(shard) : 0;
  }

  // TtlScannerBackend.
  core::Result<SweepReport> SampleAndExpire(SweepRequest req) override;

  // --- Dispatch ------------------------------------------------------------

  core::Result<RespValue> Exec(const core::ops::ReadOp& op,
                               std::optional<core::Duration> deadline) const;
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::SequenceId highest_wal_seq);
  core::Result<void> Checkpoint(core::ShardId shard, core::SequenceId up_to_wal_seq);
  core::Result<RespValue> ExecDel(const core::ops::Del& op) const;

  // Writes a fully-built batch and fsyncs the WAL before returning, so the
  // standalone-command write (ExecDel) is durable on return without relying on
  // a later cold checkpoint. ApplyBatch deliberately does NOT route through
  // this — its durability comes from the amortised checkpoint.
  core::Result<void> WriteDurable(rocksdb::WriteBatch* batch, std::string_view context) const;

  // --- Read handlers -------------------------------------------------------

  // Read-only: an expired key reads as absent and stays on disk for the
  // TTL scanner. `deadline` propagates into rocksdb::ReadOptions::deadline.
  core::Result<RespValue> Handle(const core::ops::StringGet& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::SetIsMember& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::SetMembers& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::SetCard& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::ZsetScore& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::ZsetCard& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::ZsetRange& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::HashGet& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::HashGetAll& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::HashMultiGet& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::HashFieldExists& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::HashKeys& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::HashVals& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::HashLen& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::Exists& op,
                                 std::optional<core::Duration> deadline) const;

  // Builds a rocksdb::ReadOptions with `.deadline` set when `deadline` is
  // present. `deadline` is interpreted as a relative duration from now.
  rocksdb::ReadOptions MakeReadOptions(std::optional<core::Duration> deadline) const;

  // --- Loads (ColdStore::LoadKey and friends) -----------------------------

  core::Result<std::optional<core::ColdKeyState>> LoadKey(std::string_view key,
                                                          core::SteadyTime deadline) const;
  core::Result<std::optional<core::KeyMeta>> ProbeKey(std::string_view key,
                                                      core::SteadyTime deadline) const;
  core::Result<std::optional<core::MemberValue>> LoadMember(std::string_view key,
                                                            core::KeyType type,
                                                            std::string_view member,
                                                            core::SteadyTime deadline) const;
  // kTimeout once `deadline` has passed.
  core::Result<rocksdb::ReadOptions> LoadOptions(core::SteadyTime deadline) const;
  // Fills `payload`, when given, with a string's value.
  core::Result<std::optional<core::KeyMeta>> ReadKeyMeta(const rocksdb::ReadOptions& ro,
                                                         std::string_view key,
                                                         std::string* payload) const;
  // Visits every record under `prefix`, checking `deadline` as it goes.
  template <typename Fn>
  core::Result<void> LoadPrefix(rocksdb::ReadOptions ro, std::string_view prefix,
                                core::SteadyTime deadline, const Fn& fn) const;

  // --- Write handlers (all take the shared WBWI) --------------------------

  // All Apply overloads are const: they mutate the passed-in `wb` and read
  // `db`/CF handles through const accessors, never `*this`.
  core::Result<void> Apply(const core::ops::StringSet& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::Del& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::SetAdd& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::SetRem& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::ZsetAdd& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::ZsetRem& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::HashSet& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::HashMSet& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::HashDel& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::Expire& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::Persist& op, rocksdb::WriteBatchWithIndex& wb) const;

  // --- Shared helpers ------------------------------------------------------

  // The meta, or nullopt when absent or expired by the wall clock.
  core::Result<std::optional<fmt::MetaValue>> ReadMetaIfLive(uint8_t inner_type,
                                                             std::string_view key) const;

  // Reads the meta without TTL filtering: the apply path judges no TTL.
  core::Result<std::optional<fmt::MetaValue>> ReadMetaForWrite(rocksdb::WriteBatchWithIndex& wb,
                                                               uint8_t inner_type,
                                                               std::string_view key) const;

  // Applies `delta` to the cardinality of (inner_type, key)'s meta record.
  core::Result<void> ApplyMetaDelta(rocksdb::WriteBatchWithIndex& wb, uint8_t inner_type,
                                    std::string_view key, int64_t delta) const;

  // Enumerates every sub-record for (inner_type, key)..
  core::Result<void> IterateAndDeleteCollection(rocksdb::WriteBatchWithIndex& wb,
                                                uint8_t inner_type, std::string_view key) const;

  // Enforces one-logical-type-per-key on the cold apply path. When a write
  // establishes `key` as `kept_type`, this drops every slice the key holds
  // under any OTHER type (the string record, and the meta/members/score-index
  // of the other collection types). Without this, an implicit cross-window type
  // change — e.g. a key flushed as a hash, then SET to a string in a later
  // compaction window whose CompactedState emits no leading Del — would leave
  // the stale prior-type slices behind and resurrect them on read (COLDC-6).
  // Idempotent and cheap: deletes of absent slices are harmless, and the kept
  // type's own slices are never touched, so this composes with the in-window
  // delete-before-write and the meta-delta bookkeeping.
  // True if it dropped any slice.
  core::Result<bool> ClearOtherTypeSlices(rocksdb::WriteBatchWithIndex& wb, uint8_t kept_type,
                                          std::string_view key) const;

  // ClearOtherTypeSlices for SADD, HSET and ZADD. The write path logs a
  // DEL before an add that changes a key's type, so a foreign type here,
  // live or expired, means it and cold disagree. That is reported, and
  // the add still applies as logged: failing the batch would stall the
  // shard behind an effect the log says happened.
  core::Result<void> ClearForeignTypeForAdd(rocksdb::WriteBatchWithIndex& wb, uint8_t kept_type,
                                            std::string_view key) const;

  // The TTL scanner's delete, judged by `log_now_ms` (LogClockMs). An
  // optimistic transaction re-reads the record under its snapshot and
  // deletes it only if still expired. A concurrent apply that rewrote it
  // aborts the commit (kConflict), leaving the fresh data alone. The
  // meta's GetForUpdate covers a collection: an apply that adds or
  // removes a member, or changes the TTL, also writes the meta.
  core::Result<ExpireOutcome> SweepString(std::string_view key, uint64_t log_now_ms);
  core::Result<ExpireOutcome> SweepCollection(uint8_t inner_type, std::string_view key,
                                              uint64_t log_now_ms);

  // One-sample helpers used by SampleAndExpire. Each draws a single random
  // sample from its type's range and sweeps it if expired by its shard's
  // log clock.
  core::Result<void> SampleAndExpireString(SweepReport& report);
  core::Result<void> SampleAndExpireMeta(SweepReport& report);

  core::Result<bool> AnyLiveRecord(std::string_view key) const;

  // Read-side counterpart to ClearOtherTypeSlices. A collection read for
  // `expected_type` whose own meta is absent must distinguish "key absent" from
  // "key is live under a different type": the latter is a WRONGTYPE, not an
  // empty result, matching the buffer/hot tiers and Redis (COLDC-6 read facet).
  // Returns kWrongType if `key` is live under any type other than
  // `expected_type`, otherwise success (so the caller serves its empty result).
  core::Result<void> CheckNoForeignType(uint8_t expected_type, std::string_view key) const;

  // Generic prefix scan. `deadline` (relative duration from now) is threaded
  // into rocksdb::ReadOptions::deadline via MakeReadOptions so a scan over a
  // large collection fails closed with a timeout rather than running unbounded
  // (COLD-2 / invariant 5). nullopt leaves the scan unbounded (write path).
  template <typename Fn>
  core::Result<void> ScanPrefix(rocksdb::WriteBatchWithIndex* wb, rocksdb::ColumnFamilyHandle* cf,
                                std::string_view prefix, std::optional<core::Duration> deadline,
                                const Fn& fn) const;
};

// --- Factory & lifecycle ----------------------------------------------------

core::Result<std::unique_ptr<RocksdbStore>> RocksdbStore::Create(RocksdbConfig config) {
  if (config.shard_count < 1 || config.shard_count > fmt::kMaxShardCount) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "cold store shard_count must be in [1, " +
                                               std::to_string(fmt::kMaxShardCount) + "], got " +
                                               std::to_string(config.shard_count)));
  }

  std::error_code ec;
  std::filesystem::create_directories(config.data_path, ec);
  if (ec) {
    return std::unexpected(
        Error(ErrorCode::kUnavailable,
              "failed to create data_path '" + config.data_path + "': " + ec.message()));
  }

  rocksdb::DBOptions db_opts;
  db_opts.create_if_missing = true;
  db_opts.create_missing_column_families = true;
  // ApplyBatch writes the WAL with sync=false (memtable + RocksDB WAL buffer
  // only); durability is established by an explicit FlushWAL(sync=true) in
  // Checkpoint. manual_wal_flush keeps those sync=false writes out of the OS
  // until the checkpoint, so a checkpoint is the single group-amortised fsync
  // of the WAL tail rather than one fsync per micro-flush (A6 / XDUR-1).
  db_opts.manual_wal_flush = true;

  const auto cf_opts = MakeCfOptions(config);
  const std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs{
      {rocksdb::kDefaultColumnFamilyName, cf_opts},
      {std::string(kZsetScoreIndexCfName), cf_opts},
  };

  auto impl = std::make_unique<Impl>();
  impl->ttl_expired_total =
      metrics::Registry::Instance().Counter(metrics::names::kTtlExpiredTotal, metrics::Tier::kCold);
  impl->type_conflicts_total =
      metrics::Registry::Instance().Counter(metrics::names::kColdApplyTypeConflictsTotal);
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  rocksdb::OptimisticTransactionDB* raw_db = nullptr;
  auto status = rocksdb::OptimisticTransactionDB::Open(db_opts, config.data_path, cf_descs,
                                                       &cf_handles, &raw_db);
  if (!status.ok()) {
    return std::unexpected(
        FromStatus(status, "RocksdbStore::Create: OptimisticTransactionDB::Open"));
  }
  impl->db.reset(raw_db);

  impl->config = std::move(config);
  impl->default_cf = CfHandle(cf_handles[0], CfHandleDeleter{impl->db.get()});
  impl->zset_score_idx_cf = CfHandle(cf_handles[1], CfHandleDeleter{impl->db.get()});

  const auto format_key = fmt::EncodeFormatVersionKey();
  std::string existing;
  status = impl->db->Get(rocksdb::ReadOptions(), impl->default_cf.get(), format_key, &existing);
  if (status.ok()) {
    auto decoded = fmt::DecodeFormatVersionValue(existing);
    if (!decoded.has_value()) {
      return std::unexpected(decoded.error());
    }
    if (*decoded != fmt::kFormatVersion) {
      return std::unexpected(
          Error(ErrorCode::kCorruption, "cold store format version " + std::to_string(*decoded) +
                                            " does not match binary version " +
                                            std::to_string(fmt::kFormatVersion)));
    }
  } else if (status.IsNotFound()) {
    const auto format_value = fmt::EncodeFormatVersionValue(fmt::kFormatVersion);
    status =
        impl->db->Put(rocksdb::WriteOptions(), impl->default_cf.get(), format_key, format_value);
    if (!status.ok()) {
      return std::unexpected(FromStatus(status, "RocksdbStore::Create: Put format version"));
    }
    // manual_wal_flush keeps the Put in the WAL buffer; flush+fsync it now so a
    // crash before the first cold checkpoint can't lose the format marker.
    status = impl->db->FlushWAL(/*sync=*/true);
    if (!status.ok()) {
      return std::unexpected(FromStatus(status, "RocksdbStore::Create: FlushWAL format version"));
    }
  } else {
    return std::unexpected(FromStatus(status, "RocksdbStore::Create: Get format version"));
  }

  ABYSS_LOG_INFO("cold store opened", {"path", std::string_view{impl->config.data_path}},
                 {"format_version", static_cast<int64_t>(fmt::kFormatVersion)});

  // Seed the sampling RNG. Production seeds from std::random_device; tests
  // can supply a deterministic seed by injecting their own backend.
  std::random_device rd;
  impl->rng.seed((static_cast<uint64_t>(rd()) << 32) | static_cast<uint64_t>(rd()));

  // Construct the TTL scanner. The scanner is created in a stopped state;
  // RocksdbStore::Start() is what flips it on, called by the server after
  // recovery completes.
  TtlScanner::Hooks hooks;
  if (impl->config.ttl_scanner_hooks.has_value()) {
    hooks = *impl->config.ttl_scanner_hooks;
  } else {
    const std::string data_path_copy = impl->config.data_path;
    // std::function tolerates exceptions; alloc failures from Result/string are
    // operator-actionable, not silently swallowed here.
    // NOLINTNEXTLINE(bugprone-exception-escape)
    hooks.disk_usage = [data_path_copy] { return DefaultDiskUsage(data_path_copy); };
    hooks.cpu_clock = DefaultThreadCpuClock;
    hooks.steady_clock = core::DefaultSteadyClock;
  }
  const auto mode = impl->config.ttl_scanner_mode.value_or(TtlScanner::ExecutionMode::kOwnedThread);
  auto scanner = TtlScanner::Create(*impl, impl->config.ttl_scanner, mode, std::move(hooks));
  if (!scanner.has_value()) {
    return std::unexpected(scanner.error());
  }
  impl->ttl_scanner = std::move(*scanner);

  return std::unique_ptr<RocksdbStore>(new RocksdbStore(std::move(impl)));
}

RocksdbStore::RocksdbStore(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

RocksdbStore::~RocksdbStore() = default;

// --- Public ColdStore forwards ---------------------------------------------

core::Result<RespValue> RocksdbStore::Exec(const core::ops::ReadOp& op,
                                           std::optional<core::Duration> deadline) {
  return impl_->Exec(op, deadline);
}

core::Result<void> RocksdbStore::ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                            core::SequenceId highest_wal_seq) {
  return impl_->ApplyBatch(ops, highest_wal_seq);
}

core::Result<void> RocksdbStore::Checkpoint(core::ShardId shard, core::SequenceId up_to_wal_seq) {
  return impl_->Checkpoint(shard, up_to_wal_seq);
}

core::Result<void> RocksdbStore::Wipe(core::ShardId shard) {
  // DeleteRange per <type><shard> slice, batched into one synced write. The
  // batch is atomic and fsynced (wo.sync) so a post-wipe crash can't resurrect
  // pre-Flush data once the cold consumer's commit is durable (#136). DeleteRange
  // is the sanctioned exception for this bounded admin wipe (not the per-key
  // DEL path); see ADP-010 §Per-shard wipe. 0xFF (format version) is outside
  // every range, so it survives.
  rocksdb::WriteBatch batch;
  for (const uint8_t type : {fmt::kTypeString, fmt::kTypeMeta, fmt::kTypeHashField,
                             fmt::kTypeSetMember, fmt::kTypeZsetMember}) {
    const auto begin = fmt::ShardTypePrefix(type, shard);
    auto s = batch.DeleteRange(impl_->default_cf.get(), begin, LexicographicSuccessor(begin));
    if (!s.ok()) return std::unexpected(FromStatus(s, "Wipe: DeleteRange default_cf"));
  }
  const auto score_begin = fmt::ShardTypePrefix(fmt::kTypeZsetScoreIndex, shard);
  if (auto s = batch.DeleteRange(impl_->zset_score_idx_cf.get(), score_begin,
                                 LexicographicSuccessor(score_begin));
      !s.ok()) {
    return std::unexpected(FromStatus(s, "Wipe: DeleteRange zset_score_idx_cf"));
  }

  // DeleteRange + synced write: atomic and fsynced so a post-wipe crash can't
  // resurrect pre-Flush data once the cold consumer's commit is durable (#136).
  return impl_->WriteDurable(&batch, "Wipe: Write");
}

core::Result<RespValue> RocksdbStore::ExecDel(const core::ops::Del& op) {
  return impl_->ExecDel(op);
}

core::Result<core::StorageStats> RocksdbStore::Stats() {
  core::StorageStats out;

  // Default CF only: every score-index record (0x06) duplicates a member record
  // (0x05) in the default CF, so summing both CFs double-counts zset members.
  uint64_t num_keys = 0;
  if (impl_->db->GetIntProperty(impl_->default_cf.get(), "rocksdb.estimate-num-keys", &num_keys)) {
    out.key_count += num_keys;
  }

  // disk_bytes is physical footprint, so it still spans both column families.
  uint64_t live_bytes = 0;
  if (impl_->db->GetIntProperty(impl_->default_cf.get(), "rocksdb.total-sst-files-size",
                                &live_bytes)) {
    out.disk_bytes += live_bytes;
  }
  if (impl_->db->GetIntProperty(impl_->zset_score_idx_cf.get(), "rocksdb.total-sst-files-size",
                                &live_bytes)) {
    out.disk_bytes += live_bytes;
  }

  return out;
}

core::Result<void> RocksdbStore::Compact() {
  const rocksdb::CompactRangeOptions opts;
  auto status = impl_->db->CompactRange(opts, impl_->default_cf.get(), nullptr, nullptr);
  if (!status.ok()) {
    return std::unexpected(FromStatus(status, "Compact (default)"));
  }
  status = impl_->db->CompactRange(opts, impl_->zset_score_idx_cf.get(), nullptr, nullptr);
  if (!status.ok()) {
    return std::unexpected(FromStatus(status, "Compact (zset_score_idx)"));
  }
  return {};
}

core::Result<std::optional<core::RespCommand>> RocksdbStore::GetPromotionCommand(
    std::string_view key) {
  // Strings only for now; collection promotion is additive here.
  const auto encoded_key = fmt::EncodeStringKey(key, impl_->config.shard_count);
  std::string raw;
  auto status = impl_->db->Get(rocksdb::ReadOptions(), impl_->default_cf.get(), encoded_key, &raw);
  if (status.IsNotFound()) return std::optional<core::RespCommand>{};
  if (!status.ok()) return std::unexpected(FromStatus(status, "GetPromotionCommand"));

  auto decoded = fmt::DecodeStringValue(raw);
  if (!decoded.has_value()) return std::unexpected(decoded.error());
  if (fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, impl_->NowMs())) {
    return std::optional<core::RespCommand>{};
  }

  core::RespCommand cmd;
  cmd.args.reserve(5);
  cmd.args.emplace_back("SET");
  cmd.args.emplace_back(key);
  cmd.args.emplace_back(decoded->payload);
  if ((decoded->flags & fmt::kFlagHasTtl) != 0) {
    cmd.args.emplace_back("PXAT");
    cmd.args.emplace_back(std::to_string(decoded->abs_ttl_ms));
  }
  return std::optional<core::RespCommand>{std::move(cmd)};
}

core::Result<std::optional<core::ColdKeyState>> RocksdbStore::LoadKey(std::string_view key,
                                                                      core::SteadyTime deadline) {
  return impl_->LoadKey(key, deadline);
}

core::Result<std::optional<core::KeyMeta>> RocksdbStore::ProbeKey(std::string_view key,
                                                                  core::SteadyTime deadline) {
  return impl_->ProbeKey(key, deadline);
}

core::Result<std::optional<core::MemberValue>> RocksdbStore::LoadMember(std::string_view key,
                                                                        core::KeyType type,
                                                                        std::string_view member,
                                                                        core::SteadyTime deadline) {
  return impl_->LoadMember(key, type, member, deadline);
}

core::Result<void> RocksdbStore::Start() {
  if (impl_->ttl_scanner != nullptr) impl_->ttl_scanner->Start();
  return {};
}

core::Result<void> RocksdbStore::Stop() {
  if (impl_->ttl_scanner != nullptr) impl_->ttl_scanner->Stop();
  return {};
}

core::Result<SweepReport> RocksdbStore::RunScannerTickForTesting() {
  if (impl_->ttl_scanner == nullptr) {
    return std::unexpected(Error(ErrorCode::kUnavailable, "ttl scanner is not constructed"));
  }
  return impl_->ttl_scanner->Tick();
}

TtlScanner::Snapshot RocksdbStore::ScannerSnapshot() const {
  if (impl_->ttl_scanner == nullptr) return TtlScanner::Snapshot{};
  return impl_->ttl_scanner->CurrentSnapshot();
}

std::vector<RocksdbStore::RawRecord> RocksdbStore::RecordsForTesting() const {
  std::vector<RawRecord> out;
  rocksdb::ManagedSnapshot snapshot(impl_->db.get());
  rocksdb::ReadOptions ro;
  ro.snapshot = snapshot.snapshot();
  for (auto* cf : {impl_->default_cf.get(), impl_->zset_score_idx_cf.get()}) {
    const std::unique_ptr<rocksdb::Iterator> it(impl_->db->NewIterator(ro, cf));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      out.push_back({.column_family = cf->GetName(),
                     .key = it->key().ToString(),
                     .value = it->value().ToString()});
    }
  }
  return out;
}

// --- Dispatch ---------------------------------------------------------------

core::Result<RespValue> RocksdbStore::Impl::Exec(const core::ops::ReadOp& op,
                                                 std::optional<core::Duration> deadline) const {
  if (deadline.has_value() && *deadline <= core::Duration::zero()) {
    return std::unexpected(
        core::Error(core::ErrorCode::kTimeout, "cold read deadline already elapsed"));
  }
  return std::visit([this, &deadline](const auto& o) { return this->Handle(o, deadline); }, op);
}

rocksdb::ReadOptions RocksdbStore::Impl::MakeReadOptions(
    std::optional<core::Duration> deadline) const {
  rocksdb::ReadOptions opts;
  if (deadline.has_value()) {
    // RocksDB compares `deadline` against SystemClock::NowMicros(); set it to
    // the absolute wall-clock microsecond at which this read should abort.
    const auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    const auto delta_us = std::chrono::duration_cast<std::chrono::microseconds>(*deadline);
    opts.deadline = now_us + delta_us;
  }
  return opts;
}

// --- Loads --------------------------------------------------------------

core::Result<rocksdb::ReadOptions> RocksdbStore::Impl::LoadOptions(
    core::SteadyTime deadline) const {
  const auto now = config.steady_clock();
  if (now >= deadline) return std::unexpected(LoadTimeout());
  rocksdb::ReadOptions ro;
  const auto wall_us = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  ro.deadline = wall_us + std::chrono::duration_cast<std::chrono::microseconds>(deadline - now);
  return ro;
}

core::Result<std::optional<core::KeyMeta>> RocksdbStore::Impl::ReadKeyMeta(
    const rocksdb::ReadOptions& ro, std::string_view key, std::string* payload) const {
  // ADP-010's existence probe: the string record, then each collection's
  // meta record, in KeyType order. All point reads, one batch.
  constexpr size_t kRecords = 4;
  const std::array<std::string, kRecords> encoded{
      fmt::EncodeStringKey(key, config.shard_count),
      fmt::EncodeMetaKey(fmt::kTypeSetMember, key, config.shard_count),
      fmt::EncodeMetaKey(fmt::kTypeHashField, key, config.shard_count),
      fmt::EncodeMetaKey(fmt::kTypeZsetMember, key, config.shard_count),
  };
  std::array<rocksdb::Slice, kRecords> keys;
  for (size_t i = 0; i < kRecords; ++i) keys.at(i) = ToSlice(encoded.at(i));
  std::array<rocksdb::PinnableSlice, kRecords> values;
  std::array<rocksdb::Status, kRecords> statuses;
  rocksdb::DB& base = *db;
  base.MultiGet(ro, default_cf.get(), kRecords, keys.data(), values.data(), statuses.data());

  for (size_t i = 0; i < kRecords; ++i) {
    const auto& status = statuses.at(i);
    if (status.IsNotFound()) continue;
    if (!status.ok()) return std::unexpected(FromStatus(status, "probe key"));
    const auto type = static_cast<core::KeyType>(i);
    const std::string_view raw = ToSv(values.at(i));
    if (type == core::KeyType::kString) {
      auto decoded = fmt::DecodeStringValue(raw);
      if (!decoded.has_value()) return std::unexpected(decoded.error());
      if (payload != nullptr) payload->assign(decoded->payload);
      return core::KeyMeta{.type = type,
                           .abs_ttl_ms = LoadedTtl(decoded->flags, decoded->abs_ttl_ms),
                           .cardinality = 1};
    }
    auto decoded = fmt::DecodeMetaValue(raw);
    if (!decoded.has_value()) return std::unexpected(decoded.error());
    return core::KeyMeta{.type = type,
                         .abs_ttl_ms = LoadedTtl(decoded->flags, decoded->abs_ttl_ms),
                         .cardinality = decoded->cardinality};
  }
  return std::optional<core::KeyMeta>{};
}

core::Result<std::optional<core::KeyMeta>> RocksdbStore::Impl::ProbeKey(
    std::string_view key, core::SteadyTime deadline) const {
  auto ro = LoadOptions(deadline);
  if (!ro.has_value()) return std::unexpected(ro.error());
  return ReadKeyMeta(*ro, key, nullptr);
}

template <typename Fn>
core::Result<void> RocksdbStore::Impl::LoadPrefix(rocksdb::ReadOptions ro, std::string_view prefix,
                                                  core::SteadyTime deadline, const Fn& fn) const {
  const std::string upper = LexicographicSuccessor(prefix);
  const rocksdb::Slice upper_slice(upper);
  if (!upper.empty()) ro.iterate_upper_bound = &upper_slice;
  const std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro, default_cf.get()));
  uint64_t visited = 0;
  for (it->Seek(ToSlice(prefix)); it->Valid(); it->Next()) {
    const auto k = ToSv(it->key());
    if (!k.starts_with(prefix)) break;
    if ((++visited & kDeadlineCheckMask) == 0 && config.steady_clock() >= deadline) {
      return std::unexpected(LoadTimeout());
    }
    if (auto r = fn(k.substr(prefix.size()), ToSv(it->value())); !r.has_value()) return r;
  }
  if (!it->status().ok()) return std::unexpected(FromStatus(it->status(), "load scan"));
  return {};
}

core::Result<std::optional<core::ColdKeyState>> RocksdbStore::Impl::LoadKey(
    std::string_view key, core::SteadyTime deadline) const {
  auto ro = LoadOptions(deadline);
  if (!ro.has_value()) return std::unexpected(ro.error());
  // One view across the meta read and the member scan.
  rocksdb::ManagedSnapshot snapshot(db.get());
  ro->snapshot = snapshot.snapshot();

  std::string payload;
  auto meta = ReadKeyMeta(*ro, key, &payload);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) return std::optional<core::ColdKeyState>{};
  core::ColdKeyState state{.type = (*meta)->type, .abs_ttl_ms = (*meta)->abs_ttl_ms};
  const auto cardinality = static_cast<size_t>((*meta)->cardinality);

  core::Result<void> scanned;
  switch (state.type) {
    case core::KeyType::kString:
      state.value = std::move(payload);
      break;
    case core::KeyType::kSet: {
      auto& members = state.value.emplace<std::unordered_set<std::string>>();
      members.reserve(cardinality);
      scanned = LoadPrefix(*ro, fmt::SetMemberPrefix(key, config.shard_count), deadline,
                           [&](std::string_view member, std::string_view) -> core::Result<void> {
                             members.emplace(member);
                             return {};
                           });
      break;
    }
    case core::KeyType::kHash: {
      auto& fields = state.value.emplace<std::unordered_map<std::string, std::string>>();
      fields.reserve(cardinality);
      scanned =
          LoadPrefix(*ro, fmt::HashFieldPrefix(key, config.shard_count), deadline,
                     [&](std::string_view field, std::string_view value) -> core::Result<void> {
                       fields.emplace(field, value);
                       return {};
                     });
      break;
    }
    case core::KeyType::kZset: {
      auto& scores = state.value.emplace<std::unordered_map<std::string, double>>();
      scores.reserve(cardinality);
      scanned =
          LoadPrefix(*ro, fmt::ZsetMemberPrefix(key, config.shard_count), deadline,
                     [&](std::string_view member, std::string_view value) -> core::Result<void> {
                       auto score = DecodeZsetMemberScore(value);
                       if (!score.has_value()) return std::unexpected(score.error());
                       scores.emplace(member, *score);
                       return {};
                     });
      break;
    }
  }
  if (!scanned.has_value()) return std::unexpected(scanned.error());
  return std::optional<core::ColdKeyState>{std::move(state)};
}

core::Result<std::optional<core::MemberValue>> RocksdbStore::Impl::LoadMember(
    std::string_view key, core::KeyType type, std::string_view member,
    core::SteadyTime deadline) const {
  if (type == core::KeyType::kString) {
    return std::unexpected(Error(ErrorCode::kInvalidArgument, "a string has no members"));
  }
  auto ro = LoadOptions(deadline);
  if (!ro.has_value()) return std::unexpected(ro.error());
  std::string encoded;
  switch (type) {
    case core::KeyType::kSet:
      encoded = fmt::EncodeSetMemberKey(key, member, config.shard_count);
      break;
    case core::KeyType::kHash:
      encoded = fmt::EncodeHashFieldKey(key, member, config.shard_count);
      break;
    case core::KeyType::kZset:
      encoded = fmt::EncodeZsetMemberKey(key, member, config.shard_count);
      break;
    case core::KeyType::kString:
      break;
  }
  std::string raw;
  const auto status = db->Get(*ro, default_cf.get(), encoded, &raw);
  if (status.IsNotFound()) return std::optional<core::MemberValue>{};
  if (!status.ok()) return std::unexpected(FromStatus(status, "load member"));
  if (type == core::KeyType::kHash) return std::optional<core::MemberValue>{std::move(raw)};
  if (type == core::KeyType::kZset) {
    auto score = DecodeZsetMemberScore(raw);
    if (!score.has_value()) return std::unexpected(score.error());
    return std::optional<core::MemberValue>{*score};
  }
  return std::optional<core::MemberValue>{std::monostate{}};
}

core::Result<void> RocksdbStore::Impl::ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                                  core::SequenceId /*highest_wal_seq*/) {
  rocksdb::WriteBatchWithIndex wb(rocksdb::BytewiseComparator(), 0, /*overwrite_key=*/true);

  for (const auto& op : ops) {
    auto r = std::visit([this, &wb](const auto& o) { return this->Apply(o, wb); }, op);
    if (!r.has_value()) return r;
  }

  // Default WriteOptions: sync=false. With manual_wal_flush the WAL stays in
  // RocksDB's buffer; this is a memtable-only write. Durability is established
  // by a later Checkpoint(FlushWAL sync=true), never per batch (A6). The cold
  // consumer tracks highest_wal_seq and gates its commit on the checkpointed seq,
  // so it is not recorded here.
  auto status = db->Write(rocksdb::WriteOptions(), wb.GetWriteBatch());
  if (!status.ok()) return std::unexpected(FromStatus(status, "ApplyBatch"));
  return {};
}

core::Result<void> RocksdbStore::Impl::Checkpoint(core::ShardId shard,
                                                  core::SequenceId up_to_wal_seq) {
  // FlushWAL flushes RocksDB's WAL buffer to the OS and, with sync=true,
  // fsyncs it — one group-amortised fsync of the WAL tail for every ApplyBatch
  // since the last checkpoint. Idempotent: a checkpoint with nothing dirty is
  // a cheap flush of an empty buffer.
  auto status = db->FlushWAL(/*sync=*/true);
  if (!status.ok()) return std::unexpected(FromStatus(status, "Checkpoint: FlushWAL"));

  const std::scoped_lock lock(checkpoint_mu);
  auto& recorded = checkpointed_seq[shard];
  recorded = std::max(recorded, up_to_wal_seq);
  return {};
}

core::Result<void> RocksdbStore::Impl::WriteDurable(rocksdb::WriteBatch* batch,
                                                    std::string_view context) const {
  rocksdb::WriteOptions wo;
  wo.sync = true;
  // OptimisticTransactionDB hides plain WriteBatch writes; go through the base
  // DB. sync=true forces FlushWAL+fsync of this write regardless of
  // manual_wal_flush, so the standalone command is durable on return.
  auto status = db->GetBaseDB()->Write(wo, batch);
  if (!status.ok()) return std::unexpected(FromStatus(status, context));
  return {};
}

// --- Read handlers ----------------------------------------------------------

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::StringGet& op,
                                                   std::optional<core::Duration> deadline) const {
  const auto encoded_key = fmt::EncodeStringKey(op.key, config.shard_count);
  std::string raw;
  auto status = db->Get(MakeReadOptions(deadline), default_cf.get(), encoded_key, &raw);
  if (status.IsNotFound()) return RespValue::Null();
  if (!status.ok()) return std::unexpected(FromStatus(status, "GET"));

  auto decoded = fmt::DecodeStringValue(raw);
  if (!decoded.has_value()) return std::unexpected(decoded.error());

  if (fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, NowMs())) return RespValue::Null();
  return RespValue::BulkString(std::string(decoded->payload));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::SetIsMember& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeSetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeSetMember, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Integer(0);
  }

  const auto encoded = fmt::EncodeSetMemberKey(op.key, op.member, config.shard_count);
  std::string raw;
  auto status = db->Get(MakeReadOptions(deadline), default_cf.get(), encoded, &raw);
  if (status.IsNotFound()) return RespValue::Integer(0);
  if (!status.ok()) return std::unexpected(FromStatus(status, "SISMEMBER"));
  return RespValue::Integer(1);
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::SetMembers& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeSetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeSetMember, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Array({});
  }

  std::vector<RespValue> members;
  members.reserve((*meta)->cardinality);
  const auto prefix = fmt::SetMemberPrefix(op.key, config.shard_count);
  auto r = ScanPrefix(
      nullptr, default_cf.get(), prefix, deadline,
      [&](std::string_view full_key, std::string_view) -> core::Result<bool> {
        members.push_back(RespValue::BulkString(std::string(full_key.substr(prefix.size()))));
        return true;
      });
  if (!r.has_value()) return std::unexpected(r.error());
  return RespValue::Array(std::move(members));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(
    const core::ops::SetCard& op, std::optional<core::Duration> /*deadline*/) const {
  auto meta = ReadMetaIfLive(fmt::kTypeSetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeSetMember, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Integer(0);
  }
  return RespValue::Integer(static_cast<int64_t>((*meta)->cardinality));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::ZsetScore& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeZsetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeZsetMember, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Null();
  }

  const auto encoded = fmt::EncodeZsetMemberKey(op.key, op.member, config.shard_count);
  std::string raw;
  auto status = db->Get(MakeReadOptions(deadline), default_cf.get(), encoded, &raw);
  if (status.IsNotFound()) return RespValue::Null();
  if (!status.ok()) return std::unexpected(FromStatus(status, "ZSCORE"));

  auto score = DecodeZsetMemberScore(raw);
  if (!score.has_value()) return std::unexpected(score.error());
  return RespValue::BulkString(core::FormatRespDouble(*score));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(
    const core::ops::ZsetCard& op, std::optional<core::Duration> /*deadline*/) const {
  auto meta = ReadMetaIfLive(fmt::kTypeZsetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeZsetMember, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Integer(0);
  }
  return RespValue::Integer(static_cast<int64_t>((*meta)->cardinality));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::ZsetRange& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeZsetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeZsetMember, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Array({});
  }

  // Collect (member, score) in the requested order, then apply offset/count.
  std::vector<std::pair<std::string, double>> ordered;
  ordered.reserve((*meta)->cardinality);

  if (op.by_lex) {
    // Lex range on member names, using the member-indexed CF (which iterates in
    // pure member byte-lex order). The min/max [/(/-/+ bounds are applied per
    // member so the slice matches the hot tier's ExecZsetRange by_lex branch.
    auto min_bound = ParseLexBound(op.min);
    if (!min_bound.has_value()) return std::unexpected(min_bound.error());
    auto max_bound = ParseLexBound(op.max);
    if (!max_bound.has_value()) return std::unexpected(max_bound.error());

    const auto prefix = fmt::ZsetMemberPrefix(op.key, config.shard_count);
    auto r =
        ScanPrefix(nullptr, default_cf.get(), prefix, deadline,
                   [&](std::string_view full_key, std::string_view value) -> core::Result<bool> {
                     auto member = full_key.substr(prefix.size());
                     if (!LexAtOrAboveMin(member, *min_bound)) return true;
                     if (!LexAtOrBelowMax(member, *max_bound)) return false;  // past upper, stop
                     auto s = DecodeZsetMemberScore(value);
                     if (!s.has_value()) return std::unexpected(s.error());
                     ordered.emplace_back(std::string(member), *s);
                     return true;
                   });
    if (!r.has_value()) return std::unexpected(r.error());
  } else {
    // Score-ordered (index-based or by-score). Both use the score-index CF
    // because Redis's default ZRANGE semantics are score-then-lex order.
    auto min = ParseScoreBound(op.min.empty() ? "-inf" : op.min, /*is_min=*/true);
    if (!min.has_value()) return std::unexpected(min.error());
    auto max = ParseScoreBound(op.max.empty() ? "+inf" : op.max, /*is_min=*/false);
    if (!max.has_value()) return std::unexpected(max.error());

    const auto prefix = fmt::ZsetScoreIndexPrefix(op.key, config.shard_count);
    // Each score-index key is: prefix || 8-byte sortable score || member.
    const auto score_offset = prefix.size();
    auto r = ScanPrefix(
        nullptr, zset_score_idx_cf.get(), prefix, deadline,
        [&](std::string_view full_key, std::string_view) -> core::Result<bool> {
          if (full_key.size() < score_offset + 8) {
            return std::unexpected(Error(ErrorCode::kCorruption, "zset score-index key too short"));
          }
          uint64_t sortable = 0;
          for (int i = 0; i < 8; ++i) {
            sortable = (sortable << 8) | static_cast<uint8_t>(full_key[score_offset + i]);
          }
          // Invert the SortableDouble transform.
          const uint64_t bits =
              (sortable & (1ULL << 63)) != 0 ? sortable & ~(1ULL << 63) : ~sortable;
          auto score = std::bit_cast<double>(bits);
          if (op.by_score) {
            if (min->exclusive ? score <= min->value : score < min->value) {
              return true;
            }
            if (max->exclusive ? score >= max->value : score > max->value) {
              return false;  // past upper bound, stop
            }
          }
          auto member = std::string(full_key.substr(score_offset + 8));
          ordered.emplace_back(std::move(member), score);
          return true;
        });
    if (!r.has_value()) return std::unexpected(r.error());

    // Index-based ZRANGE (no by_score, no by_lex) further narrows by start/stop indices.
    if (!op.by_score && !op.by_lex) {
      auto size = static_cast<int64_t>(ordered.size());
      int64_t start = 0;
      int64_t stop = size - 1;
      if (!op.min.empty()) {
        auto [ptr, ec] = std::from_chars(op.min.data(), op.min.data() + op.min.size(), start);
        if (ec != std::errc{} || ptr != op.min.data() + op.min.size()) {
          return std::unexpected(
              Error(ErrorCode::kInvalidArgument, "ZRANGE start index is not an integer"));
        }
      }
      if (!op.max.empty()) {
        auto [ptr, ec] = std::from_chars(op.max.data(), op.max.data() + op.max.size(), stop);
        if (ec != std::errc{} || ptr != op.max.data() + op.max.size()) {
          return std::unexpected(
              Error(ErrorCode::kInvalidArgument, "ZRANGE stop index is not an integer"));
        }
      }
      if (start < 0) start = std::max<int64_t>(size + start, 0);
      if (stop < 0) stop = size + stop;
      stop = std::min(stop, size - 1);
      if (start > stop || start >= size) {
        ordered.clear();
      } else {
        ordered.erase(ordered.begin() + stop + 1, ordered.end());
        ordered.erase(ordered.begin(), ordered.begin() + start);
      }
    }
  }

  if (op.rev) {
    std::ranges::reverse(ordered);
  }

  std::vector<RespValue> out;
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  int64_t offset = std::max<int64_t>(op.offset, 0);
  int64_t count = op.count;
  auto end = static_cast<int64_t>(ordered.size());
  if (count >= 0) end = std::min(end, offset + count);
  out.reserve(static_cast<size_t>(std::max<int64_t>(end - offset, 0)) * (op.with_scores ? 2 : 1));
  for (int64_t i = offset; i < end; ++i) {
    out.push_back(RespValue::BulkString(ordered[static_cast<size_t>(i)].first));
    if (op.with_scores) {
      out.push_back(
          RespValue::BulkString(core::FormatRespDouble(ordered[static_cast<size_t>(i)].second)));
    }
  }
  return RespValue::Array(std::move(out));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::HashGet& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeHashField, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeHashField, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Null();
  }

  const auto encoded = fmt::EncodeHashFieldKey(op.key, op.field, config.shard_count);
  std::string raw;
  auto status = db->Get(MakeReadOptions(deadline), default_cf.get(), encoded, &raw);
  if (status.IsNotFound()) return RespValue::Null();
  if (!status.ok()) return std::unexpected(FromStatus(status, "HGET"));
  return RespValue::BulkString(std::move(raw));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::HashGetAll& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeHashField, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeHashField, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Array({});
  }

  std::vector<RespValue> pairs;
  pairs.reserve((*meta)->cardinality * 2);
  const auto prefix = fmt::HashFieldPrefix(op.key, config.shard_count);
  auto r = ScanPrefix(
      nullptr, default_cf.get(), prefix, deadline,
      [&](std::string_view full_key, std::string_view value) -> core::Result<bool> {
        pairs.push_back(RespValue::BulkString(std::string(full_key.substr(prefix.size()))));
        pairs.push_back(RespValue::BulkString(std::string(value)));
        return true;
      });
  if (!r.has_value()) return std::unexpected(r.error());
  return RespValue::Array(std::move(pairs));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::HashMultiGet& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeHashField, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());

  std::vector<RespValue> out;
  out.reserve(op.fields.size());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeHashField, op.key); !c.has_value())
      return std::unexpected(c.error());
    for (size_t i = 0; i < op.fields.size(); ++i) out.push_back(RespValue::Null());
    return RespValue::Array(std::move(out));
  }

  const auto read_opts = MakeReadOptions(deadline);
  for (auto field : op.fields) {
    const auto encoded = fmt::EncodeHashFieldKey(op.key, field, config.shard_count);
    std::string raw;
    auto status = db->Get(read_opts, default_cf.get(), encoded, &raw);
    if (status.IsNotFound()) {
      out.push_back(RespValue::Null());
    } else if (!status.ok()) {
      return std::unexpected(FromStatus(status, "HMGET"));
    } else {
      out.push_back(RespValue::BulkString(std::move(raw)));
    }
  }
  return RespValue::Array(std::move(out));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::HashFieldExists& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeHashField, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeHashField, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Integer(0);
  }

  const auto encoded = fmt::EncodeHashFieldKey(op.key, op.field, config.shard_count);
  std::string raw;
  auto status = db->Get(MakeReadOptions(deadline), default_cf.get(), encoded, &raw);
  if (status.IsNotFound()) return RespValue::Integer(0);
  if (!status.ok()) return std::unexpected(FromStatus(status, "HEXISTS"));
  return RespValue::Integer(1);
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::HashKeys& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeHashField, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeHashField, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Array({});
  }

  std::vector<RespValue> keys;
  keys.reserve((*meta)->cardinality);
  const auto prefix = fmt::HashFieldPrefix(op.key, config.shard_count);
  auto r = ScanPrefix(
      nullptr, default_cf.get(), prefix, deadline,
      [&](std::string_view full_key, std::string_view /*value*/) -> core::Result<bool> {
        keys.push_back(RespValue::BulkString(std::string(full_key.substr(prefix.size()))));
        return true;
      });
  if (!r.has_value()) return std::unexpected(r.error());
  return RespValue::Array(std::move(keys));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::HashVals& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeHashField, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeHashField, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Array({});
  }

  std::vector<RespValue> vals;
  vals.reserve((*meta)->cardinality);
  const auto prefix = fmt::HashFieldPrefix(op.key, config.shard_count);
  auto r =
      ScanPrefix(nullptr, default_cf.get(), prefix, deadline,
                 [&](std::string_view /*full_key*/, std::string_view value) -> core::Result<bool> {
                   vals.push_back(RespValue::BulkString(std::string(value)));
                   return true;
                 });
  if (!r.has_value()) return std::unexpected(r.error());
  return RespValue::Array(std::move(vals));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(
    const core::ops::HashLen& op, std::optional<core::Duration> /*deadline*/) const {
  auto meta = ReadMetaIfLive(fmt::kTypeHashField, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) {
    if (auto c = CheckNoForeignType(fmt::kTypeHashField, op.key); !c.has_value())
      return std::unexpected(c.error());
    return RespValue::Integer(0);
  }
  return RespValue::Integer(static_cast<int64_t>((*meta)->cardinality));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(
    const core::ops::Exists& op, std::optional<core::Duration> /*deadline*/) const {
  int64_t count = 0;
  for (auto key : op.keys) {
    auto r = AnyLiveRecord(key);
    if (!r.has_value()) return std::unexpected(r.error());
    if (*r) ++count;
  }
  return RespValue::Integer(count);
}

// --- Write handlers ---------------------------------------------------------

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::StringSet& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  // SET replaces whatever type the key held.
  if (auto r = ClearOtherTypeSlices(wb, fmt::kTypeString, op.key); !r.has_value()) {
    return std::unexpected(r.error());
  }

  const uint8_t flags = (op.abs_ttl_ms == 0) ? 0 : fmt::kFlagHasTtl;
  const auto encoded_key = fmt::EncodeStringKey(op.key, config.shard_count);
  const auto encoded_value = fmt::EncodeStringValue({
      .flags = flags,
      .abs_ttl_ms = op.abs_ttl_ms,
      .payload = op.value,
  });
  auto s = wb.Put(default_cf.get(), encoded_key, encoded_value);
  if (!s.ok()) return std::unexpected(FromStatus(s, "SET"));
  return {};
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::Del& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  for (auto key : op.keys) {
    const auto string_key = fmt::EncodeStringKey(key, config.shard_count);
    auto s = wb.Delete(default_cf.get(), string_key);
    if (!s.ok()) return std::unexpected(FromStatus(s, "DEL string"));

    for (auto inner_type : {fmt::kTypeHashField, fmt::kTypeSetMember, fmt::kTypeZsetMember}) {
      auto meta = ReadMetaForWrite(wb, inner_type, key);
      if (!meta.has_value()) return std::unexpected(meta.error());
      if (!meta->has_value()) continue;
      auto r = IterateAndDeleteCollection(wb, inner_type, key);
      if (!r.has_value()) return r;
    }
  }
  return {};
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::SetAdd& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  if (auto r = ClearForeignTypeForAdd(wb, fmt::kTypeSetMember, op.key); !r.has_value()) return r;

  int64_t added = 0;
  for (auto member : op.members) {
    const auto encoded = fmt::EncodeSetMemberKey(op.key, member, config.shard_count);
    std::string existing;
    auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), encoded,
                                  &existing);
    if (s.IsNotFound()) {
      auto put = wb.Put(default_cf.get(), encoded, "");
      if (!put.ok()) return std::unexpected(FromStatus(put, "SADD"));
      ++added;
    } else if (!s.ok()) {
      return std::unexpected(FromStatus(s, "SADD existence"));
    }
  }
  if (added == 0) return {};
  return ApplyMetaDelta(wb, fmt::kTypeSetMember, op.key, added);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::SetRem& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  auto live = ReadMetaForWrite(wb, fmt::kTypeSetMember, op.key);
  if (!live.has_value()) return std::unexpected(live.error());
  if (!live->has_value()) return {};  // key doesn't exist — no-op

  int64_t removed = 0;
  for (auto member : op.members) {
    const auto encoded = fmt::EncodeSetMemberKey(op.key, member, config.shard_count);
    std::string existing;
    auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), encoded,
                                  &existing);
    if (s.IsNotFound()) continue;
    if (!s.ok()) return std::unexpected(FromStatus(s, "SREM existence"));
    auto del = wb.Delete(default_cf.get(), encoded);
    if (!del.ok()) return std::unexpected(FromStatus(del, "SREM"));
    ++removed;
  }
  if (removed == 0) return {};
  return ApplyMetaDelta(wb, fmt::kTypeSetMember, op.key, -removed);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::ZsetAdd& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  if (auto r = ClearForeignTypeForAdd(wb, fmt::kTypeZsetMember, op.key); !r.has_value()) return r;

  int64_t added = 0;
  for (const auto& entry : op.entries) {
    const auto member_key = fmt::EncodeZsetMemberKey(op.key, entry.member, config.shard_count);
    std::string existing;
    auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), member_key,
                                  &existing);

    double old_score = 0.0;
    bool had_old = false;
    if (s.ok()) {
      auto decoded = DecodeZsetMemberScore(existing);
      if (!decoded.has_value()) return std::unexpected(decoded.error());
      old_score = *decoded;
      had_old = true;
    } else if (!s.IsNotFound()) {
      return std::unexpected(FromStatus(s, "ZADD existence"));
    }

    if (had_old && old_score == entry.score) {
      continue;  // already at this score — nothing to change
    }

    if (had_old) {
      const auto old_score_key =
          fmt::EncodeZsetScoreIndexKey(op.key, old_score, entry.member, config.shard_count);
      auto del = wb.Delete(zset_score_idx_cf.get(), old_score_key);
      if (!del.ok()) return std::unexpected(FromStatus(del, "ZADD old score index delete"));
    }

    auto member_put = wb.Put(default_cf.get(), member_key, EncodeZsetMemberScoreValue(entry.score));
    if (!member_put.ok()) return std::unexpected(FromStatus(member_put, "ZADD member"));

    const auto score_key =
        fmt::EncodeZsetScoreIndexKey(op.key, entry.score, entry.member, config.shard_count);
    auto score_put = wb.Put(zset_score_idx_cf.get(), score_key, "");
    if (!score_put.ok()) return std::unexpected(FromStatus(score_put, "ZADD score index"));

    if (!had_old) ++added;
  }
  if (added == 0) return {};
  return ApplyMetaDelta(wb, fmt::kTypeZsetMember, op.key, added);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::ZsetRem& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  auto live = ReadMetaForWrite(wb, fmt::kTypeZsetMember, op.key);
  if (!live.has_value()) return std::unexpected(live.error());
  if (!live->has_value()) return {};

  int64_t removed = 0;
  for (auto member : op.members) {
    const auto member_key = fmt::EncodeZsetMemberKey(op.key, member, config.shard_count);
    std::string existing;
    auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), member_key,
                                  &existing);
    if (s.IsNotFound()) continue;
    if (!s.ok()) return std::unexpected(FromStatus(s, "ZREM existence"));
    auto decoded = DecodeZsetMemberScore(existing);
    if (!decoded.has_value()) return std::unexpected(decoded.error());

    auto del_member = wb.Delete(default_cf.get(), member_key);
    if (!del_member.ok()) return std::unexpected(FromStatus(del_member, "ZREM member"));

    const auto score_key =
        fmt::EncodeZsetScoreIndexKey(op.key, *decoded, member, config.shard_count);
    auto del_score = wb.Delete(zset_score_idx_cf.get(), score_key);
    if (!del_score.ok()) return std::unexpected(FromStatus(del_score, "ZREM score index"));
    ++removed;
  }
  if (removed == 0) return {};
  return ApplyMetaDelta(wb, fmt::kTypeZsetMember, op.key, -removed);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::HashSet& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  if (auto r = ClearForeignTypeForAdd(wb, fmt::kTypeHashField, op.key); !r.has_value()) return r;

  int64_t added = 0;
  for (const auto& fv : op.fields) {
    const auto encoded = fmt::EncodeHashFieldKey(op.key, fv.field, config.shard_count);
    std::string existing;
    auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), encoded,
                                  &existing);
    if (s.IsNotFound()) {
      ++added;
    } else if (!s.ok()) {
      return std::unexpected(FromStatus(s, "HSET existence"));
    }
    auto put = wb.Put(default_cf.get(), encoded, ToSlice(fv.value));
    if (!put.ok()) return std::unexpected(FromStatus(put, "HSET"));
  }
  if (added == 0) return {};
  return ApplyMetaDelta(wb, fmt::kTypeHashField, op.key, added);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::HashMSet& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  return Apply(core::ops::HashSet{.key = op.key, .fields = op.fields}, wb);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::HashDel& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  auto live = ReadMetaForWrite(wb, fmt::kTypeHashField, op.key);
  if (!live.has_value()) return std::unexpected(live.error());
  if (!live->has_value()) return {};

  int64_t removed = 0;
  for (auto field : op.fields) {
    const auto encoded = fmt::EncodeHashFieldKey(op.key, field, config.shard_count);
    std::string existing;
    auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), encoded,
                                  &existing);
    if (s.IsNotFound()) continue;
    if (!s.ok()) return std::unexpected(FromStatus(s, "HDEL existence"));
    auto del = wb.Delete(default_cf.get(), encoded);
    if (!del.ok()) return std::unexpected(FromStatus(del, "HDEL"));
    ++removed;
  }
  if (removed == 0) return {};
  return ApplyMetaDelta(wb, fmt::kTypeHashField, op.key, -removed);
}

namespace {

// Predicate (NX/XX/GT/LT) was resolved upstream; this just rewrites flags+TTL.
core::Result<void> ApplyTtlChange(rocksdb::WriteBatchWithIndex& wb, rocksdb::DB& db,
                                  rocksdb::ColumnFamilyHandle& cf, std::string_view key,
                                  uint8_t new_flags, uint64_t new_abs_ttl_ms,
                                  uint32_t shard_count) {
  const auto string_key = fmt::EncodeStringKey(key, shard_count);
  std::string raw;
  auto s = wb.GetFromBatchAndDB(&db, rocksdb::ReadOptions(), &cf, string_key, &raw);
  if (s.ok()) {
    auto decoded = fmt::DecodeStringValue(raw);
    if (!decoded.has_value()) return std::unexpected(decoded.error());
    const auto encoded = fmt::EncodeStringValue({
        .flags = new_flags,
        .abs_ttl_ms = new_abs_ttl_ms,
        .payload = decoded->payload,
    });
    auto put = wb.Put(&cf, string_key, encoded);
    if (!put.ok()) return std::unexpected(FromStatus(put, "EXPIRE/PERSIST string"));
    return {};
  }
  if (!s.IsNotFound()) {
    return std::unexpected(FromStatus(s, "EXPIRE/PERSIST string existence"));
  }

  for (auto inner_type : {fmt::kTypeHashField, fmt::kTypeSetMember, fmt::kTypeZsetMember}) {
    const auto meta_key = fmt::EncodeMetaKey(inner_type, key, shard_count);
    std::string meta_raw;
    auto ms = wb.GetFromBatchAndDB(&db, rocksdb::ReadOptions(), &cf, meta_key, &meta_raw);
    if (ms.IsNotFound()) continue;
    if (!ms.ok()) return std::unexpected(FromStatus(ms, "EXPIRE/PERSIST meta existence"));
    auto decoded = fmt::DecodeMetaValue(meta_raw);
    if (!decoded.has_value()) return std::unexpected(decoded.error());
    const auto encoded = fmt::EncodeMetaValue({
        .flags = new_flags,
        .abs_ttl_ms = new_abs_ttl_ms,
        .cardinality = decoded->cardinality,
    });
    auto put = wb.Put(&cf, meta_key, encoded);
    if (!put.ok()) return std::unexpected(FromStatus(put, "EXPIRE/PERSIST meta"));
    return {};
  }

  // Key may have been concurrently evicted; no-op is safe.
  return {};
}

}  // namespace

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::Expire& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  return ApplyTtlChange(wb, *db, *default_cf, op.key, fmt::kFlagHasTtl, op.abs_ttl_ms,
                        config.shard_count);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::Persist& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  return ApplyTtlChange(wb, *db, *default_cf, op.key, /*new_flags=*/0, /*new_abs_ttl_ms=*/0,
                        config.shard_count);
}

// --- DEL --------------------------------------------------------------------

core::Result<RespValue> RocksdbStore::Impl::ExecDel(const core::ops::Del& op) const {
  rocksdb::WriteBatchWithIndex wb(rocksdb::BytewiseComparator(), 0, /*overwrite_key=*/true);
  int64_t deleted = 0;
  const uint64_t now = NowMs();

  for (auto key : op.keys) {
    bool counted = false;

    // String record.
    const auto string_key = fmt::EncodeStringKey(key, config.shard_count);
    std::string raw;
    auto s = db->Get(rocksdb::ReadOptions(), default_cf.get(), string_key, &raw);
    if (s.ok()) {
      auto decoded = fmt::DecodeStringValue(raw);
      if (!decoded.has_value()) return std::unexpected(decoded.error());
      if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, now)) {
        counted = true;
      }
      auto del = wb.Delete(default_cf.get(), string_key);
      if (!del.ok()) return std::unexpected(FromStatus(del, "DEL string"));
    } else if (!s.IsNotFound()) {
      return std::unexpected(FromStatus(s, "DEL string existence"));
    }

    // Collection records — a well-formed key has at most one collection type.
    for (auto inner_type : {fmt::kTypeHashField, fmt::kTypeSetMember, fmt::kTypeZsetMember}) {
      const auto meta_key = fmt::EncodeMetaKey(inner_type, key, config.shard_count);
      std::string meta_raw;
      auto ms = db->Get(rocksdb::ReadOptions(), default_cf.get(), meta_key, &meta_raw);
      if (ms.IsNotFound()) continue;
      if (!ms.ok()) return std::unexpected(FromStatus(ms, "DEL meta existence"));
      auto decoded = fmt::DecodeMetaValue(meta_raw);
      if (!decoded.has_value()) return std::unexpected(decoded.error());
      if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, now) && !counted) {
        counted = true;
      }
      auto r = IterateAndDeleteCollection(wb, inner_type, key);
      if (!r.has_value()) return std::unexpected(r.error());
    }

    if (counted) ++deleted;
  }

  // Standalone DEL command path (not the cold-flush path): make it durable on
  // return rather than waiting for a later cold checkpoint (XERR-1 / COLD-1).
  if (auto r = WriteDurable(wb.GetWriteBatch(), "DEL"); !r.has_value()) {
    return std::unexpected(r.error());
  }
  return RespValue::Integer(deleted);
}

// --- Shared helpers ---------------------------------------------------------

core::Result<std::optional<fmt::MetaValue>> RocksdbStore::Impl::ReadMetaIfLive(
    uint8_t inner_type, std::string_view key) const {
  const auto encoded = fmt::EncodeMetaKey(inner_type, key, config.shard_count);
  std::string raw;
  auto s = db->Get(rocksdb::ReadOptions(), default_cf.get(), encoded, &raw);
  if (s.IsNotFound()) return std::optional<fmt::MetaValue>{};
  if (!s.ok()) return std::unexpected(FromStatus(s, "read meta"));
  auto decoded = fmt::DecodeMetaValue(raw);
  if (!decoded.has_value()) return std::unexpected(decoded.error());
  if (fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, NowMs())) {
    return std::optional<fmt::MetaValue>{};
  }
  return std::optional<fmt::MetaValue>{*decoded};
}

core::Result<std::optional<fmt::MetaValue>> RocksdbStore::Impl::ReadMetaForWrite(
    rocksdb::WriteBatchWithIndex& wb, uint8_t inner_type, std::string_view key) const {
  const auto encoded = fmt::EncodeMetaKey(inner_type, key, config.shard_count);
  std::string raw;
  auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), encoded, &raw);
  if (s.IsNotFound()) return std::optional<fmt::MetaValue>{};
  if (!s.ok()) return std::unexpected(FromStatus(s, "read meta (batch)"));
  auto decoded = fmt::DecodeMetaValue(raw);
  if (!decoded.has_value()) return std::unexpected(decoded.error());
  return std::optional<fmt::MetaValue>{*decoded};
}

core::Result<void> RocksdbStore::Impl::ApplyMetaDelta(rocksdb::WriteBatchWithIndex& wb,
                                                      uint8_t inner_type, std::string_view key,
                                                      int64_t delta) const {
  auto current = ReadMetaForWrite(wb, inner_type, key);
  if (!current.has_value()) return std::unexpected(current.error());

  fmt::MetaValue next;
  if (current->has_value()) {
    next = **current;
  }

  int64_t updated = static_cast<int64_t>(next.cardinality) + delta;
  if (updated < 0) {
    return std::unexpected(
        Error(ErrorCode::kCorruption, "meta cardinality would go negative — upstream bug"));
  }
  next.cardinality = static_cast<uint64_t>(updated);

  const auto meta_key = fmt::EncodeMetaKey(inner_type, key, config.shard_count);
  if (next.cardinality == 0) {
    auto s = wb.Delete(default_cf.get(), meta_key);
    if (!s.ok()) return std::unexpected(FromStatus(s, "ApplyMetaDelta delete"));
    return {};
  }
  auto s = wb.Put(default_cf.get(), meta_key, fmt::EncodeMetaValue(next));
  if (!s.ok()) return std::unexpected(FromStatus(s, "ApplyMetaDelta put"));
  return {};
}

core::Result<void> RocksdbStore::Impl::IterateAndDeleteCollection(rocksdb::WriteBatchWithIndex& wb,
                                                                  uint8_t inner_type,
                                                                  std::string_view key) const {
  std::string member_prefix;
  switch (inner_type) {
    case fmt::kTypeHashField:
      member_prefix = fmt::HashFieldPrefix(key, config.shard_count);
      break;
    case fmt::kTypeSetMember:
      member_prefix = fmt::SetMemberPrefix(key, config.shard_count);
      break;
    case fmt::kTypeZsetMember:
      member_prefix = fmt::ZsetMemberPrefix(key, config.shard_count);
      break;
    default:
      return std::unexpected(
          Error(ErrorCode::kInternal, "IterateAndDeleteCollection: unknown inner type"));
  }

  auto r = ScanPrefix(&wb, default_cf.get(), member_prefix, std::nullopt,
                      [&](std::string_view full_key, std::string_view) -> core::Result<bool> {
                        auto s = wb.Delete(default_cf.get(), ToSlice(full_key));
                        if (!s.ok())
                          return std::unexpected(FromStatus(s, "collection member delete"));
                        return true;
                      });
  if (!r.has_value()) return std::unexpected(r.error());

  if (inner_type == fmt::kTypeZsetMember) {
    const auto score_prefix = fmt::ZsetScoreIndexPrefix(key, config.shard_count);
    auto zr = ScanPrefix(&wb, zset_score_idx_cf.get(), score_prefix, std::nullopt,
                         [&](std::string_view full_key, std::string_view) -> core::Result<bool> {
                           auto s = wb.Delete(zset_score_idx_cf.get(), ToSlice(full_key));
                           if (!s.ok())
                             return std::unexpected(FromStatus(s, "zset score index delete"));
                           return true;
                         });
    if (!zr.has_value()) return std::unexpected(zr.error());
  }

  const auto meta_key = fmt::EncodeMetaKey(inner_type, key, config.shard_count);
  auto s = wb.Delete(default_cf.get(), meta_key);
  if (!s.ok()) return std::unexpected(FromStatus(s, "meta delete"));
  return {};
}

core::Result<bool> RocksdbStore::Impl::ClearOtherTypeSlices(rocksdb::WriteBatchWithIndex& wb,
                                                            uint8_t kept_type,
                                                            std::string_view key) const {
  // Presence-checked: a key almost never holds a foreign type, so probe first
  // and only emit deletes when there is something to clear. This keeps the
  // common type-establish path write-free (no tombstone pollution of the LSM)
  // and only runs the full collection sweep on the rare cross-window change.
  bool cleared = false;
  if (kept_type != fmt::kTypeString) {
    const auto string_key = fmt::EncodeStringKey(key, config.shard_count);
    std::string raw;
    auto s =
        wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), string_key, &raw);
    if (s.ok()) {
      auto del = wb.Delete(default_cf.get(), string_key);
      if (!del.ok()) return std::unexpected(FromStatus(del, "clear other-type string slice"));
      cleared = true;
    } else if (!s.IsNotFound()) {
      return std::unexpected(FromStatus(s, "clear other-type string probe"));
    }
  }

  for (auto inner_type : {fmt::kTypeHashField, fmt::kTypeSetMember, fmt::kTypeZsetMember}) {
    if (inner_type == kept_type) continue;
    auto meta = ReadMetaForWrite(wb, inner_type, key);
    if (!meta.has_value()) return std::unexpected(meta.error());
    if (!meta->has_value()) continue;
    auto r = IterateAndDeleteCollection(wb, inner_type, key);
    if (!r.has_value()) return std::unexpected(r.error());
    cleared = true;
  }
  return cleared;
}

core::Result<void> RocksdbStore::Impl::ClearForeignTypeForAdd(rocksdb::WriteBatchWithIndex& wb,
                                                              uint8_t kept_type,
                                                              std::string_view key) const {
  auto cleared = ClearOtherTypeSlices(wb, kept_type, key);
  if (!cleared.has_value()) return std::unexpected(cleared.error());
  if (*cleared) {
    type_conflicts_total.Increment();
    ABYSS_LOG_ERROR("cold apply: an add found its key holding another type; applying it as logged",
                    {"shard", static_cast<int64_t>(core::ComputeShard(key, config.shard_count))},
                    {"add_type", static_cast<int64_t>(kept_type)});
  }
  return {};
}

core::Result<ExpireOutcome> RocksdbStore::Impl::SweepString(std::string_view key,
                                                            uint64_t log_now_ms) {
  rocksdb::WriteOptions write_opts;
  rocksdb::OptimisticTransactionOptions txn_opts;
  txn_opts.set_snapshot = true;
  std::unique_ptr<rocksdb::Transaction> txn(db->BeginTransaction(write_opts, txn_opts));

  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = txn->GetSnapshot();

  const auto encoded_key = fmt::EncodeStringKey(key, config.shard_count);
  std::string raw;
  auto s = txn->GetForUpdate(read_opts, default_cf.get(), encoded_key, &raw);
  if (s.IsNotFound()) {
    txn->Rollback();
    return ExpireOutcome::kNotFound;
  }
  if (!s.ok()) {
    return std::unexpected(FromStatus(s, "expire string GetForUpdate"));
  }

  auto decoded = fmt::DecodeStringValue(raw);
  if (!decoded.has_value()) {
    txn->Rollback();
    return std::unexpected(decoded.error());
  }
  if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, log_now_ms)) {
    txn->Rollback();
    return ExpireOutcome::kNotExpired;
  }

  if (auto del = txn->Delete(default_cf.get(), encoded_key); !del.ok()) {
    txn->Rollback();
    return std::unexpected(FromStatus(del, "expire string delete"));
  }

  auto commit = txn->Commit();
  if (commit.IsBusy() || commit.IsTryAgain()) {
    return ExpireOutcome::kConflict;
  }
  if (!commit.ok()) {
    return std::unexpected(FromStatus(commit, "expire string commit"));
  }
  ttl_expired_total.Increment();
  return ExpireOutcome::kDeleted;
}

core::Result<ExpireOutcome> RocksdbStore::Impl::SweepCollection(uint8_t inner_type,
                                                                std::string_view key,
                                                                uint64_t log_now_ms) {
  std::string member_prefix;
  switch (inner_type) {
    case fmt::kTypeHashField:
      member_prefix = fmt::HashFieldPrefix(key, config.shard_count);
      break;
    case fmt::kTypeSetMember:
      member_prefix = fmt::SetMemberPrefix(key, config.shard_count);
      break;
    case fmt::kTypeZsetMember:
      member_prefix = fmt::ZsetMemberPrefix(key, config.shard_count);
      break;
    default:
      return std::unexpected(Error(ErrorCode::kInternal, "SweepCollection: unknown inner type"));
  }

  rocksdb::WriteOptions write_opts;
  rocksdb::OptimisticTransactionOptions txn_opts;
  txn_opts.set_snapshot = true;
  std::unique_ptr<rocksdb::Transaction> txn(db->BeginTransaction(write_opts, txn_opts));

  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = txn->GetSnapshot();

  const auto meta_key = fmt::EncodeMetaKey(inner_type, key, config.shard_count);
  std::string raw_meta;
  auto s = txn->GetForUpdate(read_opts, default_cf.get(), meta_key, &raw_meta);
  if (s.IsNotFound()) {
    txn->Rollback();
    return ExpireOutcome::kNotFound;
  }
  if (!s.ok()) {
    return std::unexpected(FromStatus(s, "expire collection GetForUpdate meta"));
  }

  auto decoded = fmt::DecodeMetaValue(raw_meta);
  if (!decoded.has_value()) {
    txn->Rollback();
    return std::unexpected(decoded.error());
  }
  if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, log_now_ms)) {
    txn->Rollback();
    return ExpireOutcome::kNotExpired;
  }

  // Member iteration uses the txn's snapshot, so we delete only the member
  // set as it existed at snapshot time. A concurrent write to the meta
  // record (any added or removed member) makes Commit abort.
  std::string upper = LexicographicSuccessor(member_prefix);
  rocksdb::Slice upper_slice(upper);
  rocksdb::ReadOptions iter_opts = read_opts;
  if (!upper.empty()) iter_opts.iterate_upper_bound = &upper_slice;
  std::unique_ptr<rocksdb::Iterator> it(txn->GetIterator(iter_opts, default_cf.get()));
  for (it->Seek(ToSlice(member_prefix)); it->Valid(); it->Next()) {
    auto k = ToSv(it->key());
    if (!k.starts_with(member_prefix)) break;
    if (auto del = txn->Delete(default_cf.get(), ToSlice(k)); !del.ok()) {
      txn->Rollback();
      return std::unexpected(FromStatus(del, "expire collection member delete"));
    }
  }
  if (!it->status().ok()) {
    txn->Rollback();
    return std::unexpected(FromStatus(it->status(), "expire collection member scan"));
  }

  if (inner_type == fmt::kTypeZsetMember) {
    const auto score_prefix = fmt::ZsetScoreIndexPrefix(key, config.shard_count);
    std::string score_upper = LexicographicSuccessor(score_prefix);
    rocksdb::Slice score_upper_slice(score_upper);
    rocksdb::ReadOptions score_iter_opts = read_opts;
    if (!score_upper.empty()) score_iter_opts.iterate_upper_bound = &score_upper_slice;
    std::unique_ptr<rocksdb::Iterator> score_it(
        txn->GetIterator(score_iter_opts, zset_score_idx_cf.get()));
    for (score_it->Seek(ToSlice(score_prefix)); score_it->Valid(); score_it->Next()) {
      auto k = ToSv(score_it->key());
      if (!k.starts_with(score_prefix)) break;
      if (auto del = txn->Delete(zset_score_idx_cf.get(), ToSlice(k)); !del.ok()) {
        txn->Rollback();
        return std::unexpected(FromStatus(del, "expire collection score index delete"));
      }
    }
    if (!score_it->status().ok()) {
      txn->Rollback();
      return std::unexpected(FromStatus(score_it->status(), "expire collection score index scan"));
    }
  }

  if (auto del = txn->Delete(default_cf.get(), meta_key); !del.ok()) {
    txn->Rollback();
    return std::unexpected(FromStatus(del, "expire collection meta delete"));
  }

  auto commit = txn->Commit();
  if (commit.IsBusy() || commit.IsTryAgain()) {
    return ExpireOutcome::kConflict;
  }
  if (!commit.ok()) {
    return std::unexpected(FromStatus(commit, "expire collection commit"));
  }
  ttl_expired_total.Increment();
  return ExpireOutcome::kDeleted;
}

// --- Active TTL sampling ----------------------------------------------------

namespace {

// <type><shard><random>: a random shard in range, then a random point
// in it. Drawing the shard bytes at random too would seek past the
// last shard and wrap to the first key, so one live key starved all.
std::array<char, 1 + fmt::kShardBytes + sizeof(uint64_t)> SampleTarget(std::mt19937_64& rng,
                                                                       uint8_t type,
                                                                       uint32_t shard_count) {
  std::array<char, 1 + fmt::kShardBytes + sizeof(uint64_t)> target{};
  target[0] = static_cast<char>(type);
  const auto shard = static_cast<uint16_t>(rng() % std::max<uint32_t>(shard_count, 1));
  target[1] = static_cast<char>(shard >> 8);
  target[2] = static_cast<char>(shard & 0xFF);
  const uint64_t suffix = rng();
  std::memcpy(target.data() + 1 + fmt::kShardBytes, &suffix, sizeof(suffix));
  return target;
}

}  // namespace

core::Result<SweepReport> RocksdbStore::Impl::SampleAndExpire(SweepRequest req) {
  SweepReport report{};
  if (req.sample_size == 0) return report;
  for (size_t i = 0; i < req.sample_size; ++i) {
    const bool sample_string = (i % 2 == 0);
    auto r = sample_string ? SampleAndExpireString(report) : SampleAndExpireMeta(report);
    if (!r.has_value()) return std::unexpected(r.error());
  }
  return report;
}

core::Result<void> RocksdbStore::Impl::SampleAndExpireString(SweepReport& report) {
  const auto seek_target = SampleTarget(rng, fmt::kTypeString, config.shard_count);

  const std::array<char, 1> upper{static_cast<char>(fmt::kTypeString + 1)};
  const rocksdb::Slice upper_slice(upper.data(), upper.size());
  rocksdb::ReadOptions opts;
  opts.iterate_upper_bound = &upper_slice;

  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(opts, default_cf.get()));
  it->Seek(rocksdb::Slice(seek_target.data(), seek_target.size()));
  if (!it->Valid()) {
    const std::array<char, 1> start{static_cast<char>(fmt::kTypeString)};
    it->Seek(rocksdb::Slice(start.data(), start.size()));
    if (!it->Valid()) {
      if (!it->status().ok()) {
        return std::unexpected(FromStatus(it->status(), "sample string iterator"));
      }
      return {};
    }
  }

  ++report.sampled_strings;

  const auto encoded_key = ToSv(it->key());
  if (encoded_key.size() < 1 + fmt::kShardBytes ||
      static_cast<uint8_t>(encoded_key[0]) != fmt::kTypeString) {
    return {};
  }
  // Layout: <kTypeString><shard:kShardBytes><user key> (ADP-010).
  const auto user_key = std::string(encoded_key.substr(1 + fmt::kShardBytes));
  const auto value = ToSv(it->value());

  auto decoded = fmt::DecodeStringValue(value);
  if (!decoded.has_value()) {
    ABYSS_LOG_WARN("ttl scanner: corrupt string value, skipping");
    return {};
  }

  if ((decoded->flags & fmt::kFlagHasTtl) == 0) return {};
  ++report.with_ttl_strings;

  const uint64_t log_now_ms = LogClockMs(core::ComputeShard(user_key, config.shard_count));
  if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, log_now_ms)) return {};
  ++report.expired_strings;

  it.reset();

  auto outcome = SweepString(user_key, log_now_ms);
  if (!outcome.has_value()) return std::unexpected(outcome.error());

  switch (*outcome) {
    case ExpireOutcome::kDeleted:
      ++report.deleted_strings;
      break;
    case ExpireOutcome::kConflict:
    case ExpireOutcome::kNotExpired:
    case ExpireOutcome::kNotFound:
      ++report.conflicts_strings;
      break;
  }

  return {};
}

core::Result<void> RocksdbStore::Impl::SampleAndExpireMeta(SweepReport& report) {
  const auto seek_target = SampleTarget(rng, fmt::kTypeMeta, config.shard_count);

  const std::array<char, 1> upper{static_cast<char>(fmt::kTypeMeta + 1)};
  const rocksdb::Slice upper_slice(upper.data(), upper.size());
  rocksdb::ReadOptions opts;
  opts.iterate_upper_bound = &upper_slice;

  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(opts, default_cf.get()));
  it->Seek(rocksdb::Slice(seek_target.data(), seek_target.size()));
  if (!it->Valid()) {
    const std::array<char, 1> start{static_cast<char>(fmt::kTypeMeta)};
    it->Seek(rocksdb::Slice(start.data(), start.size()));
    if (!it->Valid()) {
      if (!it->status().ok()) {
        return std::unexpected(FromStatus(it->status(), "sample meta iterator"));
      }
      return {};
    }
  }

  ++report.sampled_collections;

  const auto encoded_key = ToSv(it->key());
  // Layout: <kTypeMeta><shard:kShardBytes><inner_type><user key> (ADP-010).
  if (encoded_key.size() < 2 + fmt::kShardBytes ||
      static_cast<uint8_t>(encoded_key[0]) != fmt::kTypeMeta) {
    return {};
  }
  const auto inner_type = static_cast<uint8_t>(encoded_key[1 + fmt::kShardBytes]);
  if (inner_type != fmt::kTypeHashField && inner_type != fmt::kTypeSetMember &&
      inner_type != fmt::kTypeZsetMember) {
    return {};
  }
  const auto user_key = std::string(encoded_key.substr(2 + fmt::kShardBytes));
  const auto value = ToSv(it->value());

  auto decoded = fmt::DecodeMetaValue(value);
  if (!decoded.has_value()) {
    ABYSS_LOG_WARN("ttl scanner: corrupt meta value, skipping");
    return {};
  }

  if ((decoded->flags & fmt::kFlagHasTtl) == 0) return {};
  ++report.with_ttl_collections;

  const uint64_t log_now_ms = LogClockMs(core::ComputeShard(user_key, config.shard_count));
  if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, log_now_ms)) return {};
  ++report.expired_collections;

  it.reset();

  auto outcome = SweepCollection(inner_type, user_key, log_now_ms);
  if (!outcome.has_value()) return std::unexpected(outcome.error());

  switch (*outcome) {
    case ExpireOutcome::kDeleted:
      ++report.deleted_collections;
      break;
    case ExpireOutcome::kConflict:
    case ExpireOutcome::kNotExpired:
    case ExpireOutcome::kNotFound:
      ++report.conflicts_collections;
      break;
  }

  return {};
}

core::Result<bool> RocksdbStore::Impl::AnyLiveRecord(std::string_view key) const {
  const auto string_key = fmt::EncodeStringKey(key, config.shard_count);
  std::string raw;
  auto s = db->Get(rocksdb::ReadOptions(), default_cf.get(), string_key, &raw);
  if (s.ok()) {
    auto decoded = fmt::DecodeStringValue(raw);
    if (!decoded.has_value()) return std::unexpected(decoded.error());
    if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, NowMs())) return true;
  } else if (!s.IsNotFound()) {
    return std::unexpected(FromStatus(s, "EXISTS string"));
  }

  // A well-formed key has at most one collection type.
  // a mixed-type key is an upstream bug.
  for (auto inner_type : {fmt::kTypeHashField, fmt::kTypeSetMember, fmt::kTypeZsetMember}) {
    auto meta = ReadMetaIfLive(inner_type, key);
    if (!meta.has_value()) return std::unexpected(meta.error());
    if (meta->has_value()) return true;
  }
  return false;
}

core::Result<void> RocksdbStore::Impl::CheckNoForeignType(uint8_t expected_type,
                                                          std::string_view key) const {
  static const Error kWrongType(ErrorCode::kWrongType,
                                "Operation against a key holding the wrong kind of value");

  if (expected_type != fmt::kTypeString) {
    const auto string_key = fmt::EncodeStringKey(key, config.shard_count);
    std::string raw;
    auto s = db->Get(rocksdb::ReadOptions(), default_cf.get(), string_key, &raw);
    if (s.ok()) {
      auto decoded = fmt::DecodeStringValue(raw);
      if (!decoded.has_value()) return std::unexpected(decoded.error());
      if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, NowMs())) {
        return std::unexpected(kWrongType);
      }
    } else if (!s.IsNotFound()) {
      return std::unexpected(FromStatus(s, "type check string"));
    }
  }

  for (auto inner_type : {fmt::kTypeHashField, fmt::kTypeSetMember, fmt::kTypeZsetMember}) {
    if (inner_type == expected_type) continue;
    auto meta = ReadMetaIfLive(inner_type, key);
    if (!meta.has_value()) return std::unexpected(meta.error());
    if (meta->has_value()) return std::unexpected(kWrongType);
  }
  return {};
}

template <typename Fn>
core::Result<void> RocksdbStore::Impl::ScanPrefix(rocksdb::WriteBatchWithIndex* wb,
                                                  rocksdb::ColumnFamilyHandle* cf,
                                                  std::string_view prefix,
                                                  std::optional<core::Duration> deadline,
                                                  const Fn& fn) const {
  std::string upper_storage = LexicographicSuccessor(prefix);
  rocksdb::Slice upper_slice(upper_storage);
  rocksdb::ReadOptions ro = MakeReadOptions(deadline);
  if (!upper_storage.empty()) {
    ro.iterate_upper_bound = &upper_slice;
  }

  std::unique_ptr<rocksdb::Iterator> base(db->NewIterator(ro, cf));
  std::unique_ptr<rocksdb::Iterator> it;
  if (wb != nullptr) {
    it.reset(wb->NewIteratorWithBase(cf, base.release()));
  } else {
    it = std::move(base);
  }

  for (it->Seek(ToSlice(prefix)); it->Valid(); it->Next()) {
    auto k = ToSv(it->key());
    if (!k.starts_with(prefix)) break;
    auto cont = fn(k, ToSv(it->value()));
    if (!cont.has_value()) return std::unexpected(cont.error());
    if (!*cont) break;
  }
  if (!it->status().ok()) return std::unexpected(FromStatus(it->status(), "prefix scan"));
  return {};
}

}  // namespace abyss::cold::backends
