#include "abyss/cold/backends/rocksdb_store.h"

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <bit>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/cold/format/key_codec.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_format.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.cold.store")

namespace abyss::cold::backends {

namespace fmt = ::abyss::cold::format;

namespace {

using core::Error;
using core::ErrorCode;
using core::RespValue;

constexpr std::string_view kZsetScoreIndexCfName = "zset_score_idx";

ErrorCode MapStatusCode(const rocksdb::Status& status) {
  if (status.IsNotFound()) return ErrorCode::kNotFound;
  if (status.IsCorruption()) return ErrorCode::kCorruption;
  if (status.IsIOError()) return ErrorCode::kUnavailable;
  if (status.IsInvalidArgument()) return ErrorCode::kInvalidArgument;
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

}  // namespace

struct CfHandleDeleter {
  rocksdb::DB* db = nullptr;
  void operator()(rocksdb::ColumnFamilyHandle* h) const {
    if (db != nullptr && h != nullptr) db->DestroyColumnFamilyHandle(h);
  }
};
using CfHandle = std::unique_ptr<rocksdb::ColumnFamilyHandle, CfHandleDeleter>;

struct RocksdbStore::Impl {
  RocksdbConfig config;
  std::unique_ptr<rocksdb::DB> db;
  CfHandle default_cf;
  CfHandle zset_score_idx_cf;

  uint64_t NowMs() const { return WallMs(config.wall_clock); }

  // --- Dispatch ------------------------------------------------------------

  core::Result<RespValue> Exec(const core::ops::ReadOp& op,
                               std::optional<core::Duration> deadline) const;
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops) const;
  core::Result<RespValue> ExecDel(const core::ops::Del& op) const;

  // --- Read handlers -------------------------------------------------------

  // const handlers may emit inline lazy-deletes through `db`. `deadline`
  // propagates into rocksdb::ReadOptions::deadline.
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
  core::Result<RespValue> Handle(const core::ops::Exists& op,
                                 std::optional<core::Duration> deadline) const;
  core::Result<RespValue> Handle(const core::ops::MultiStringGet& op,
                                 std::optional<core::Duration> deadline) const;

  // Builds a rocksdb::ReadOptions with `.deadline` set when `deadline` is
  // present. `deadline` is interpreted as a relative duration from now.
  rocksdb::ReadOptions MakeReadOptions(std::optional<core::Duration> deadline) const;

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
  core::Result<void> Apply(const core::ops::HashDel& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::MultiStringSet& op,
                           rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::Expire& op, rocksdb::WriteBatchWithIndex& wb) const;
  core::Result<void> Apply(const core::ops::Persist& op, rocksdb::WriteBatchWithIndex& wb) const;

  // --- Shared helpers ------------------------------------------------------

  // Returns the live meta or nullopt. If the meta exists but is expired,
  // inline-deletes every sub-record for the key before returning nullopt.
  core::Result<std::optional<fmt::MetaValue>> ReadMetaIfLive(uint8_t inner_type,
                                                             std::string_view key) const;

  // Reads the meta without TTL filtering.
  core::Result<std::optional<fmt::MetaValue>> ReadMetaForWrite(rocksdb::WriteBatchWithIndex& wb,
                                                               uint8_t inner_type,
                                                               std::string_view key) const;

  // Write-path counterpart to `ReadMetaIfLive`.
  core::Result<std::optional<fmt::MetaValue>> ReadMetaOrPurgeIfExpired(
      rocksdb::WriteBatchWithIndex& wb, uint8_t inner_type, std::string_view key) const;

  // Applies `delta` to the cardinality of (inner_type, key)'s meta record.
  core::Result<void> ApplyMetaDelta(rocksdb::WriteBatchWithIndex& wb, uint8_t inner_type,
                                    std::string_view key, int64_t delta) const;

  // Enumerates every sub-record for (inner_type, key)..
  core::Result<void> IterateAndDeleteCollection(rocksdb::WriteBatchWithIndex& wb,
                                                uint8_t inner_type, std::string_view key) const;

  // Inline-expire a string: a single point-delete written directly to the DB.
  core::Result<void> InlineExpireString(std::string_view key) const;

  // Inline-expire a collection: opens a fresh WBWI and then commits.
  core::Result<void> InlineExpireCollection(uint8_t inner_type, std::string_view key) const;

  core::Result<bool> AnyLiveRecord(std::string_view key) const;

  // Generic prefix scan.
  template <typename Fn>
  core::Result<void> ScanPrefix(rocksdb::WriteBatchWithIndex* wb, rocksdb::ColumnFamilyHandle* cf,
                                std::string_view prefix, const Fn& fn) const;
};

// --- Factory & lifecycle ----------------------------------------------------

core::Result<std::unique_ptr<RocksdbStore>> RocksdbStore::Create(RocksdbConfig config) {
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

  const auto cf_opts = MakeCfOptions(config);
  const std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs{
      {rocksdb::kDefaultColumnFamilyName, cf_opts},
      {std::string(kZsetScoreIndexCfName), cf_opts},
  };

  auto impl = std::make_unique<Impl>();
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  auto status = rocksdb::DB::Open(db_opts, config.data_path, cf_descs, &cf_handles, &impl->db);
  if (!status.ok()) {
    return std::unexpected(FromStatus(status, "RocksdbStore::Create: DB::Open"));
  }

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
  } else {
    return std::unexpected(FromStatus(status, "RocksdbStore::Create: Get format version"));
  }

  ABYSS_LOG_INFO("cold store opened", {"path", std::string_view{impl->config.data_path}},
                 {"format_version", static_cast<int64_t>(fmt::kFormatVersion)});
  return std::unique_ptr<RocksdbStore>(new RocksdbStore(std::move(impl)));
}

RocksdbStore::RocksdbStore(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

RocksdbStore::~RocksdbStore() = default;

// --- Public ColdStore forwards ---------------------------------------------

core::Result<RespValue> RocksdbStore::Exec(const core::ops::ReadOp& op,
                                           std::optional<core::Duration> deadline) {
  return impl_->Exec(op, deadline);
}

core::Result<void> RocksdbStore::ApplyBatch(std::span<const core::ops::WriteOp> ops) {
  return impl_->ApplyBatch(ops);
}

core::Result<RespValue> RocksdbStore::ExecDel(const core::ops::Del& op) {
  return impl_->ExecDel(op);
}

core::Result<core::StorageStats> RocksdbStore::Stats() {
  core::StorageStats out;

  uint64_t num_keys = 0;
  if (impl_->db->GetIntProperty(impl_->default_cf.get(), "rocksdb.estimate-num-keys", &num_keys)) {
    out.key_count += num_keys;
  }
  if (impl_->db->GetIntProperty(impl_->zset_score_idx_cf.get(), "rocksdb.estimate-num-keys",
                                &num_keys)) {
    out.key_count += num_keys;
  }

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
  const auto encoded_key = fmt::EncodeStringKey(key);
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

core::Result<void> RocksdbStore::Impl::ApplyBatch(std::span<const core::ops::WriteOp> ops) const {
  rocksdb::WriteBatchWithIndex wb(rocksdb::BytewiseComparator(), 0, /*overwrite_key=*/true);

  for (const auto& op : ops) {
    auto r = std::visit([this, &wb](const auto& o) { return this->Apply(o, wb); }, op);
    if (!r.has_value()) return r;
  }

  auto status = db->Write(rocksdb::WriteOptions(), wb.GetWriteBatch());
  if (!status.ok()) return std::unexpected(FromStatus(status, "ApplyBatch"));
  return {};
}

// --- Read handlers ----------------------------------------------------------

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::StringGet& op,
                                                   std::optional<core::Duration> deadline) const {
  const auto encoded_key = fmt::EncodeStringKey(op.key);
  std::string raw;
  auto status = db->Get(MakeReadOptions(deadline), default_cf.get(), encoded_key, &raw);
  if (status.IsNotFound()) return RespValue::Null();
  if (!status.ok()) return std::unexpected(FromStatus(status, "GET"));

  auto decoded = fmt::DecodeStringValue(raw);
  if (!decoded.has_value()) return std::unexpected(decoded.error());

  if (fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, NowMs())) {
    auto r = InlineExpireString(op.key);
    if (!r.has_value()) return std::unexpected(r.error());
    return RespValue::Null();
  }
  return RespValue::BulkString(std::string(decoded->payload));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::SetIsMember& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeSetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) return RespValue::Integer(0);

  const auto encoded = fmt::EncodeSetMemberKey(op.key, op.member);
  std::string raw;
  auto status = db->Get(MakeReadOptions(deadline), default_cf.get(), encoded, &raw);
  if (status.IsNotFound()) return RespValue::Integer(0);
  if (!status.ok()) return std::unexpected(FromStatus(status, "SISMEMBER"));
  return RespValue::Integer(1);
}

core::Result<RespValue> RocksdbStore::Impl::Handle(
    const core::ops::SetMembers& op, std::optional<core::Duration> /*deadline*/) const {
  auto meta = ReadMetaIfLive(fmt::kTypeSetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) return RespValue::Array({});

  std::vector<RespValue> members;
  members.reserve((*meta)->cardinality);
  const auto prefix = fmt::SetMemberPrefix(op.key);
  auto r = ScanPrefix(
      nullptr, default_cf.get(), prefix,
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
  if (!meta->has_value()) return RespValue::Integer(0);
  return RespValue::Integer(static_cast<int64_t>((*meta)->cardinality));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::ZsetScore& op,
                                                   std::optional<core::Duration> deadline) const {
  auto meta = ReadMetaIfLive(fmt::kTypeZsetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) return RespValue::Null();

  const auto encoded = fmt::EncodeZsetMemberKey(op.key, op.member);
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
  if (!meta->has_value()) return RespValue::Integer(0);
  return RespValue::Integer(static_cast<int64_t>((*meta)->cardinality));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(
    const core::ops::ZsetRange& op, std::optional<core::Duration> /*deadline*/) const {
  auto meta = ReadMetaIfLive(fmt::kTypeZsetMember, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) return RespValue::Array({});

  // Collect (member, score) in the requested order, then apply offset/count.
  std::vector<std::pair<std::string, double>> ordered;
  ordered.reserve((*meta)->cardinality);

  if (op.by_lex) {
    // Lex range on member names, using the member-indexed CF. Bounds: if min
    // is empty, start at prefix; otherwise begin at the given member. Same
    // for max.
    const auto prefix = fmt::ZsetMemberPrefix(op.key);
    auto r =
        ScanPrefix(nullptr, default_cf.get(), prefix,
                   [&](std::string_view full_key, std::string_view value) -> core::Result<bool> {
                     auto m = std::string(full_key.substr(prefix.size()));
                     auto s = DecodeZsetMemberScore(value);
                     if (!s.has_value()) return std::unexpected(s.error());
                     ordered.emplace_back(std::move(m), *s);
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

    const auto prefix = fmt::ZsetScoreIndexPrefix(op.key);
    // Each score-index key is: prefix || 8-byte sortable score || member.
    const auto score_offset = prefix.size();
    auto r = ScanPrefix(
        nullptr, zset_score_idx_cf.get(), prefix,
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
  if (!meta->has_value()) return RespValue::Null();

  const auto encoded = fmt::EncodeHashFieldKey(op.key, op.field);
  std::string raw;
  auto status = db->Get(MakeReadOptions(deadline), default_cf.get(), encoded, &raw);
  if (status.IsNotFound()) return RespValue::Null();
  if (!status.ok()) return std::unexpected(FromStatus(status, "HGET"));
  return RespValue::BulkString(std::move(raw));
}

core::Result<RespValue> RocksdbStore::Impl::Handle(
    const core::ops::HashGetAll& op, std::optional<core::Duration> /*deadline*/) const {
  auto meta = ReadMetaIfLive(fmt::kTypeHashField, op.key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) return RespValue::Array({});

  std::vector<RespValue> pairs;
  pairs.reserve((*meta)->cardinality * 2);
  const auto prefix = fmt::HashFieldPrefix(op.key);
  auto r = ScanPrefix(
      nullptr, default_cf.get(), prefix,
      [&](std::string_view full_key, std::string_view value) -> core::Result<bool> {
        pairs.push_back(RespValue::BulkString(std::string(full_key.substr(prefix.size()))));
        pairs.push_back(RespValue::BulkString(std::string(value)));
        return true;
      });
  if (!r.has_value()) return std::unexpected(r.error());
  return RespValue::Array(std::move(pairs));
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

core::Result<RespValue> RocksdbStore::Impl::Handle(const core::ops::MultiStringGet& op,
                                                   std::optional<core::Duration> deadline) const {
  std::vector<RespValue> out;
  out.reserve(op.keys.size());
  for (auto key : op.keys) {
    auto r = Handle(core::ops::StringGet{.key = key}, deadline);
    if (!r.has_value()) return std::unexpected(r.error());
    out.push_back(std::move(*r));
  }
  return RespValue::Array(std::move(out));
}

// --- Write handlers ---------------------------------------------------------

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::StringSet& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  const uint8_t flags = (op.abs_ttl_ms == 0) ? 0 : fmt::kFlagHasTtl;
  const auto encoded_key = fmt::EncodeStringKey(op.key);
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
    const auto string_key = fmt::EncodeStringKey(key);
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
  auto live = ReadMetaOrPurgeIfExpired(wb, fmt::kTypeSetMember, op.key);
  if (!live.has_value()) return std::unexpected(live.error());

  int64_t added = 0;
  for (auto member : op.members) {
    const auto encoded = fmt::EncodeSetMemberKey(op.key, member);
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
  auto live = ReadMetaOrPurgeIfExpired(wb, fmt::kTypeSetMember, op.key);
  if (!live.has_value()) return std::unexpected(live.error());
  if (!live->has_value()) return {};  // key doesn't exist — no-op

  int64_t removed = 0;
  for (auto member : op.members) {
    const auto encoded = fmt::EncodeSetMemberKey(op.key, member);
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
  auto live = ReadMetaOrPurgeIfExpired(wb, fmt::kTypeZsetMember, op.key);
  if (!live.has_value()) return std::unexpected(live.error());

  int64_t added = 0;
  for (const auto& entry : op.entries) {
    const auto member_key = fmt::EncodeZsetMemberKey(op.key, entry.member);
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
      const auto old_score_key = fmt::EncodeZsetScoreIndexKey(op.key, old_score, entry.member);
      auto del = wb.Delete(zset_score_idx_cf.get(), old_score_key);
      if (!del.ok()) return std::unexpected(FromStatus(del, "ZADD old score index delete"));
    }

    auto member_put = wb.Put(default_cf.get(), member_key, EncodeZsetMemberScoreValue(entry.score));
    if (!member_put.ok()) return std::unexpected(FromStatus(member_put, "ZADD member"));

    const auto score_key = fmt::EncodeZsetScoreIndexKey(op.key, entry.score, entry.member);
    auto score_put = wb.Put(zset_score_idx_cf.get(), score_key, "");
    if (!score_put.ok()) return std::unexpected(FromStatus(score_put, "ZADD score index"));

    if (!had_old) ++added;
  }
  if (added == 0) return {};
  return ApplyMetaDelta(wb, fmt::kTypeZsetMember, op.key, added);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::ZsetRem& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  auto live = ReadMetaOrPurgeIfExpired(wb, fmt::kTypeZsetMember, op.key);
  if (!live.has_value()) return std::unexpected(live.error());
  if (!live->has_value()) return {};

  int64_t removed = 0;
  for (auto member : op.members) {
    const auto member_key = fmt::EncodeZsetMemberKey(op.key, member);
    std::string existing;
    auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), member_key,
                                  &existing);
    if (s.IsNotFound()) continue;
    if (!s.ok()) return std::unexpected(FromStatus(s, "ZREM existence"));
    auto decoded = DecodeZsetMemberScore(existing);
    if (!decoded.has_value()) return std::unexpected(decoded.error());

    auto del_member = wb.Delete(default_cf.get(), member_key);
    if (!del_member.ok()) return std::unexpected(FromStatus(del_member, "ZREM member"));

    const auto score_key = fmt::EncodeZsetScoreIndexKey(op.key, *decoded, member);
    auto del_score = wb.Delete(zset_score_idx_cf.get(), score_key);
    if (!del_score.ok()) return std::unexpected(FromStatus(del_score, "ZREM score index"));
    ++removed;
  }
  if (removed == 0) return {};
  return ApplyMetaDelta(wb, fmt::kTypeZsetMember, op.key, -removed);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::HashSet& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  auto live = ReadMetaOrPurgeIfExpired(wb, fmt::kTypeHashField, op.key);
  if (!live.has_value()) return std::unexpected(live.error());

  int64_t added = 0;
  for (const auto& fv : op.fields) {
    const auto encoded = fmt::EncodeHashFieldKey(op.key, fv.field);
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

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::HashDel& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  auto live = ReadMetaOrPurgeIfExpired(wb, fmt::kTypeHashField, op.key);
  if (!live.has_value()) return std::unexpected(live.error());
  if (!live->has_value()) return {};

  int64_t removed = 0;
  for (auto field : op.fields) {
    const auto encoded = fmt::EncodeHashFieldKey(op.key, field);
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

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::MultiStringSet& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  for (const auto& entry : op.entries) {
    auto r = Apply(core::ops::StringSet{.key = entry.key, .value = entry.value}, wb);
    if (!r.has_value()) return r;
  }
  return {};
}

namespace {

// Predicate (NX/XX/GT/LT) was resolved upstream; this just rewrites flags+TTL.
core::Result<void> ApplyTtlChange(rocksdb::WriteBatchWithIndex& wb, rocksdb::DB& db,
                                  rocksdb::ColumnFamilyHandle& cf, std::string_view key,
                                  uint8_t new_flags, uint64_t new_abs_ttl_ms) {
  const auto string_key = fmt::EncodeStringKey(key);
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
    const auto meta_key = fmt::EncodeMetaKey(inner_type, key);
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
  return ApplyTtlChange(wb, *db, *default_cf, op.key, fmt::kFlagHasTtl, op.abs_ttl_ms);
}

core::Result<void> RocksdbStore::Impl::Apply(const core::ops::Persist& op,
                                             rocksdb::WriteBatchWithIndex& wb) const {
  return ApplyTtlChange(wb, *db, *default_cf, op.key, /*new_flags=*/0, /*new_abs_ttl_ms=*/0);
}

// --- DEL --------------------------------------------------------------------

core::Result<RespValue> RocksdbStore::Impl::ExecDel(const core::ops::Del& op) const {
  rocksdb::WriteBatchWithIndex wb(rocksdb::BytewiseComparator(), 0, /*overwrite_key=*/true);
  int64_t deleted = 0;
  const uint64_t now = NowMs();

  for (auto key : op.keys) {
    bool counted = false;

    // String record.
    const auto string_key = fmt::EncodeStringKey(key);
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
      const auto meta_key = fmt::EncodeMetaKey(inner_type, key);
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

  auto status = db->Write(rocksdb::WriteOptions(), wb.GetWriteBatch());
  if (!status.ok()) return std::unexpected(FromStatus(status, "DEL"));
  return RespValue::Integer(deleted);
}

// --- Shared helpers ---------------------------------------------------------

core::Result<std::optional<fmt::MetaValue>> RocksdbStore::Impl::ReadMetaIfLive(
    uint8_t inner_type, std::string_view key) const {
  const auto encoded = fmt::EncodeMetaKey(inner_type, key);
  std::string raw;
  auto s = db->Get(rocksdb::ReadOptions(), default_cf.get(), encoded, &raw);
  if (s.IsNotFound()) return std::optional<fmt::MetaValue>{};
  if (!s.ok()) return std::unexpected(FromStatus(s, "read meta"));
  auto decoded = fmt::DecodeMetaValue(raw);
  if (!decoded.has_value()) return std::unexpected(decoded.error());
  if (fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, NowMs())) {
    auto r = InlineExpireCollection(inner_type, key);
    if (!r.has_value()) return std::unexpected(r.error());
    return std::optional<fmt::MetaValue>{};
  }
  return std::optional<fmt::MetaValue>{*decoded};
}

core::Result<std::optional<fmt::MetaValue>> RocksdbStore::Impl::ReadMetaForWrite(
    rocksdb::WriteBatchWithIndex& wb, uint8_t inner_type, std::string_view key) const {
  const auto encoded = fmt::EncodeMetaKey(inner_type, key);
  std::string raw;
  auto s = wb.GetFromBatchAndDB(db.get(), rocksdb::ReadOptions(), default_cf.get(), encoded, &raw);
  if (s.IsNotFound()) return std::optional<fmt::MetaValue>{};
  if (!s.ok()) return std::unexpected(FromStatus(s, "read meta (batch)"));
  auto decoded = fmt::DecodeMetaValue(raw);
  if (!decoded.has_value()) return std::unexpected(decoded.error());
  return std::optional<fmt::MetaValue>{*decoded};
}

core::Result<std::optional<fmt::MetaValue>> RocksdbStore::Impl::ReadMetaOrPurgeIfExpired(
    rocksdb::WriteBatchWithIndex& wb, uint8_t inner_type, std::string_view key) const {
  auto meta = ReadMetaForWrite(wb, inner_type, key);
  if (!meta.has_value()) return std::unexpected(meta.error());
  if (!meta->has_value()) return std::optional<fmt::MetaValue>{};
  if (!fmt::IsExpired((*meta)->flags, (*meta)->abs_ttl_ms, NowMs())) return *meta;

  auto r = IterateAndDeleteCollection(wb, inner_type, key);
  if (!r.has_value()) return std::unexpected(r.error());
  return std::optional<fmt::MetaValue>{};
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

  const auto meta_key = fmt::EncodeMetaKey(inner_type, key);
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
      member_prefix = fmt::HashFieldPrefix(key);
      break;
    case fmt::kTypeSetMember:
      member_prefix = fmt::SetMemberPrefix(key);
      break;
    case fmt::kTypeZsetMember:
      member_prefix = fmt::ZsetMemberPrefix(key);
      break;
    default:
      return std::unexpected(
          Error(ErrorCode::kInternal, "IterateAndDeleteCollection: unknown inner type"));
  }

  auto r = ScanPrefix(&wb, default_cf.get(), member_prefix,
                      [&](std::string_view full_key, std::string_view) -> core::Result<bool> {
                        auto s = wb.Delete(default_cf.get(), ToSlice(full_key));
                        if (!s.ok())
                          return std::unexpected(FromStatus(s, "collection member delete"));
                        return true;
                      });
  if (!r.has_value()) return std::unexpected(r.error());

  if (inner_type == fmt::kTypeZsetMember) {
    const auto score_prefix = fmt::ZsetScoreIndexPrefix(key);
    auto zr = ScanPrefix(&wb, zset_score_idx_cf.get(), score_prefix,
                         [&](std::string_view full_key, std::string_view) -> core::Result<bool> {
                           auto s = wb.Delete(zset_score_idx_cf.get(), ToSlice(full_key));
                           if (!s.ok())
                             return std::unexpected(FromStatus(s, "zset score index delete"));
                           return true;
                         });
    if (!zr.has_value()) return std::unexpected(zr.error());
  }

  const auto meta_key = fmt::EncodeMetaKey(inner_type, key);
  auto s = wb.Delete(default_cf.get(), meta_key);
  if (!s.ok()) return std::unexpected(FromStatus(s, "meta delete"));
  return {};
}

core::Result<void> RocksdbStore::Impl::InlineExpireString(std::string_view key) const {
  const auto encoded = fmt::EncodeStringKey(key);
  auto s = db->Delete(rocksdb::WriteOptions(), default_cf.get(), encoded);
  if (!s.ok()) return std::unexpected(FromStatus(s, "lazy expire string"));
  return {};
}

core::Result<void> RocksdbStore::Impl::InlineExpireCollection(uint8_t inner_type,
                                                              std::string_view key) const {
  rocksdb::WriteBatchWithIndex wb(rocksdb::BytewiseComparator(), 0, /*overwrite_key=*/true);
  auto r = IterateAndDeleteCollection(wb, inner_type, key);
  if (!r.has_value()) return r;
  auto s = db->Write(rocksdb::WriteOptions(), wb.GetWriteBatch());
  if (!s.ok()) return std::unexpected(FromStatus(s, "lazy expire collection"));
  return {};
}

core::Result<bool> RocksdbStore::Impl::AnyLiveRecord(std::string_view key) const {
  const auto string_key = fmt::EncodeStringKey(key);
  std::string raw;
  auto s = db->Get(rocksdb::ReadOptions(), default_cf.get(), string_key, &raw);
  if (s.ok()) {
    auto decoded = fmt::DecodeStringValue(raw);
    if (!decoded.has_value()) return std::unexpected(decoded.error());
    if (!fmt::IsExpired(decoded->flags, decoded->abs_ttl_ms, NowMs())) return true;
    auto r = InlineExpireString(key);
    if (!r.has_value()) return std::unexpected(r.error());
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

template <typename Fn>
core::Result<void> RocksdbStore::Impl::ScanPrefix(rocksdb::WriteBatchWithIndex* wb,
                                                  rocksdb::ColumnFamilyHandle* cf,
                                                  std::string_view prefix, const Fn& fn) const {
  std::string upper_storage = LexicographicSuccessor(prefix);
  rocksdb::Slice upper_slice(upper_storage);
  rocksdb::ReadOptions ro;
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
