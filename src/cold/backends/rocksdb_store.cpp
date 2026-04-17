#include "abyss/cold/backends/rocksdb_store.h"

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/cold/format/key_codec.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

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
  return {MapStatusCode(status), std::string(context) + ": " + status.ToString()};
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

core::Result<RespValue> RocksdbStore::Exec(const core::ops::ReadOp& op) {
  return std::visit(
      [this](const auto& o) -> core::Result<RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringGet>) {
          return ExecStringGet(o);
        } else {
          return std::unexpected(
              Error(ErrorCode::kInvalidArgument, "read op not yet supported in cold store"));
        }
      },
      op);
}

core::Result<RespValue> RocksdbStore::ExecStringGet(const core::ops::StringGet& op) {
  const auto encoded_key = fmt::EncodeStringKey(op.key);
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
  return RespValue::BulkString(std::string(decoded->payload));
}

core::Result<RespValue> RocksdbStore::ExecDel(const core::ops::Del& op) {
  rocksdb::WriteBatch batch;
  int64_t deleted = 0;

  for (auto key : op.keys) {
    auto encoded = fmt::EncodeStringKey(key);
    std::string value;
    bool value_found = false;

    if (impl_->db->KeyMayExist(rocksdb::ReadOptions(), impl_->default_cf.get(), encoded, &value,
                               &value_found)) {
      if (value_found) {
        ++deleted;
      } else {
        auto s = impl_->db->Get(rocksdb::ReadOptions(), impl_->default_cf.get(), encoded, &value);
        if (s.ok()) ++deleted;
      }
    }

    auto del_status = batch.Delete(impl_->default_cf.get(), encoded);
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

core::Result<void> RocksdbStore::ApplyBatch(std::span<const core::ops::WriteOp> ops) {
  rocksdb::WriteBatch batch;

  for (const auto& op : ops) {
    auto result = std::visit(
        [this, &batch](const auto& o) -> core::Result<void> {
          using T = std::decay_t<decltype(o)>;
          if constexpr (std::is_same_v<T, core::ops::StringSet>) {
            const uint8_t flags = (o.abs_ttl_ms == 0) ? 0 : fmt::kFlagHasTtl;
            const auto encoded_key = fmt::EncodeStringKey(o.key);
            const auto encoded_value = fmt::EncodeStringValue({
                .flags = flags,
                .abs_ttl_ms = o.abs_ttl_ms,
                .payload = o.value,
            });
            auto s = batch.Put(impl_->default_cf.get(), encoded_key, encoded_value);
            if (!s.ok()) return std::unexpected(FromStatus(s, "ApplyBatch SET"));
            return {};
          } else if constexpr (std::is_same_v<T, core::ops::Del>) {
            for (auto key : o.keys) {
              auto encoded = fmt::EncodeStringKey(key);
              auto s = batch.Delete(impl_->default_cf.get(), encoded);
              if (!s.ok()) return std::unexpected(FromStatus(s, "ApplyBatch DEL"));
            }
            return {};
          } else {
            return std::unexpected(
                Error(ErrorCode::kInvalidArgument, "write op not yet supported in cold store"));
          }
        },
        op);
    if (!result.has_value()) return result;
  }

  const auto status = impl_->db->Write(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) {
    return std::unexpected(FromStatus(status, "ApplyBatch"));
  }
  return {};
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

}  // namespace abyss::cold::backends
