#include "abyss/cold/backends/rocksdb_store.h"

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/cold/format/key_codec.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::cold::backends {

namespace fmt = ::abyss::cold::format;

namespace {

using core::Error;
using core::ErrorCode;
using core::RespCommand;
using core::RespValue;

constexpr std::string_view kZsetScoreIndexCfName = "zset_score_idx";

std::string AsciiUpper(std::string_view s) {
  std::string out(s);
  for (char& c : out) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return out;
}

uint64_t NowWallMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   core::WallClock::now().time_since_epoch())
                                   .count());
}

core::Result<uint64_t> ParseUint64(std::string_view s) {
  uint64_t value = 0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(Error(ErrorCode::kInvalidArgument,
                                 "not a valid unsigned integer: '" + std::string(s) + "'"));
  }
  return value;
}

ErrorCode MapStatusCode(const rocksdb::Status& status) {
  if (status.IsNotFound()) return ErrorCode::kNotFound;
  if (status.IsCorruption()) return ErrorCode::kCorruption;
  if (status.IsIOError()) return ErrorCode::kUnavailable;
  if (status.IsInvalidArgument()) return ErrorCode::kInvalidArgument;
  return ErrorCode::kInternal;
}

Error FromStatus(const rocksdb::Status& status, std::string_view context) {
  return Error(MapStatusCode(status), std::string(context) + ": " + status.ToString());
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
  // db declared first — destroyed last (reverse member order), ensuring CF
  // handles are cleaned up before the DB is closed.
  std::unique_ptr<rocksdb::DB> db;
  CfHandle default_cf;
  CfHandle zset_score_idx_cf;
};

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

  // Format version: verify on existing DB, write on fresh.
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

  return std::unique_ptr<RocksdbStore>(new RocksdbStore(std::move(impl)));
}

RocksdbStore::RocksdbStore(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

RocksdbStore::~RocksdbStore() = default;

namespace {

// Parses the trailing options of SET: `EX seconds` or `PX milliseconds`.
core::Result<void> ParseSetOptions(const RespCommand& cmd, size_t options_start,
                                   uint64_t& abs_ttl_ms) {
  abs_ttl_ms = 0;
  for (size_t i = options_start; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt != "EX" && opt != "PX") {
      return std::unexpected(
          Error(ErrorCode::kInvalidArgument, "unsupported SET option '" + cmd.args[i] + "'"));
    }
    if (i + 1 >= cmd.args.size()) {
      return std::unexpected(
          Error(ErrorCode::kInvalidArgument, "syntax error — expected value after '" + opt + "'"));
    }
    auto ttl_arg = ParseUint64(cmd.args[++i]);
    if (!ttl_arg.has_value()) return std::unexpected(ttl_arg.error());
    const uint64_t ttl_ms = (opt == "EX") ? (*ttl_arg * 1000) : *ttl_arg;
    abs_ttl_ms = NowWallMs() + ttl_ms;
  }
  return {};
}

}  // namespace

core::Result<RespValue> RocksdbStore::Exec(const RespCommand& cmd) {
  if (cmd.args.empty()) {
    return std::unexpected(Error(ErrorCode::kInvalidArgument, "empty command"));
  }
  const auto name = AsciiUpper(cmd.Name());
  if (name == "GET") return ExecGet(cmd);
  if (name == "SET") return ExecSet(cmd);
  if (name == "DEL") return ExecDel(cmd);
  return std::unexpected(
      Error(ErrorCode::kInvalidArgument, "unknown or unsupported command '" + cmd.Name() + "'"));
}

core::Result<RespValue> RocksdbStore::ExecGet(const RespCommand& cmd) {
  if (cmd.args.size() != 2) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "wrong number of arguments for 'GET'"));
  }
  const auto encoded_key = fmt::EncodeStringKey(cmd.args[1]);
  std::string raw;
  auto status = impl_->db->Get(rocksdb::ReadOptions(), impl_->default_cf.get(), encoded_key, &raw);
  if (status.IsNotFound()) {
    return RespValue::Null();
  }
  if (!status.ok()) {
    return std::unexpected(FromStatus(status, "GET"));
  }
  auto decoded = fmt::DecodeStringValue(raw);
  if (!decoded.has_value()) {
    return std::unexpected(decoded.error());
  }
  // TTL is stored but not enforced here — requires lazy expiry integration.
  return RespValue::String(std::string(decoded->payload));
}

core::Result<RespValue> RocksdbStore::ExecSet(const RespCommand& cmd) {
  if (cmd.args.size() < 3) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "wrong number of arguments for 'SET'"));
  }
  uint64_t abs_ttl_ms = 0;
  if (auto r = ParseSetOptions(cmd, 3, abs_ttl_ms); !r.has_value()) {
    return std::unexpected(r.error());
  }

  const uint8_t flags = (abs_ttl_ms == 0) ? 0 : fmt::kFlagHasTtl;
  const auto encoded_key = fmt::EncodeStringKey(cmd.args[1]);
  const auto encoded_value = fmt::EncodeStringValue({
      .flags = flags,
      .abs_ttl_ms = abs_ttl_ms,
      .payload = cmd.args[2],
  });

  const auto status =
      impl_->db->Put(rocksdb::WriteOptions(), impl_->default_cf.get(), encoded_key, encoded_value);
  if (!status.ok()) {
    return std::unexpected(FromStatus(status, "SET"));
  }
  return RespValue::String("OK");
}

core::Result<RespValue> RocksdbStore::ExecDel(const RespCommand& cmd) {
  if (cmd.args.size() < 2) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "wrong number of arguments for 'DEL'"));
  }

  rocksdb::WriteBatch batch;
  int64_t deleted = 0;
  for (size_t i = 1; i < cmd.args.size(); ++i) {
    const auto encoded_key = fmt::EncodeStringKey(cmd.args[i]);
    // Count pre-existing entries so DEL returns a meaningful count.
    std::string sink;
    auto status =
        impl_->db->Get(rocksdb::ReadOptions(), impl_->default_cf.get(), encoded_key, &sink);
    if (status.ok()) {
      ++deleted;
    } else if (!status.IsNotFound()) {
      return std::unexpected(FromStatus(status, "DEL"));
    }
    auto del_status = batch.Delete(impl_->default_cf.get(), encoded_key);
    if (!del_status.ok()) {
      return std::unexpected(FromStatus(del_status, "DEL"));
    }
  }

  const auto status = impl_->db->Write(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) {
    return std::unexpected(FromStatus(status, "DEL"));
  }
  return RespValue::Integer(deleted);
}

core::Result<void> RocksdbStore::ApplyBatch(std::span<const RespCommand> /*cmds*/) {
  // Requires compacted batch write support for all types.
  return std::unexpected(Error(ErrorCode::kInternal, "RocksdbStore::ApplyBatch not implemented"));
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
  rocksdb::CompactRangeOptions opts;
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

}  // namespace abyss::cold::backends
