#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/string_hash.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct StorageStats {
  uint64_t disk_bytes = 0;
  uint64_t key_count = 0;
};

// In the order of ColdValue's alternatives.
enum class KeyType : uint8_t { kString, kSet, kHash, kZset };

// A key's type and TTL (0: none), without its value.
struct KeyMeta {
  KeyType type = KeyType::kString;
  int64_t abs_ttl_ms = 0;
  // Members or fields; 1 for a string.
  uint64_t cardinality = 0;

  bool operator==(const KeyMeta&) const = default;
};

// A string, a set's members, a hash's fields, or a zset's scores.
using ColdValue = std::variant<std::string, StringSet, StringMap<std::string>, StringMap<double>>;

struct ColdKeyState {
  KeyType type = KeyType::kString;
  ColdValue value;
  int64_t abs_ttl_ms = 0;

  bool operator==(const ColdKeyState&) const = default;
};

// A set member's presence, a hash field's value or a zset score.
using MemberValue = std::variant<std::monostate, std::string, double>;

// A key loaded as one type: its whole state when it holds that type,
// else only its meta.
using LoadedAs = std::variant<KeyMeta, ColdKeyState>;

// Reads are loads: the engine answers from what they return.
class ColdStore {
 public:
  ColdStore() = default;
  virtual ~ColdStore() = default;
  ColdStore(const ColdStore&) = delete;
  ColdStore& operator=(const ColdStore&) = delete;
  ColdStore(ColdStore&&) = delete;
  ColdStore& operator=(ColdStore&&) = delete;

  // Applies a compacted batch. `highest_wal_seq` is the highest WAL sequence
  // this batch materialises. The write is a non-durable (memtable-only) write;
  // durability is established only by a later Checkpoint, never per batch. The
  // cold consumer must not commit past an applied-but-uncheckpointed seq.
  virtual Result<void> ApplyBatch(std::span<const ops::WriteOp> ops,
                                  SequenceId highest_wal_seq) = 0;

  // Makes every write issued so far durable on cold's stable storage and
  // records `up_to_wal_seq` as the highest WAL seq now durable for `shard`.
  // Idempotent and cheap when nothing is dirty. After this returns ok every
  // write with WAL seq <= up_to_wal_seq applied via ApplyBatch is on stable
  // storage. This is the A6 cold-durable frontier the cold commit is gated on.
  virtual Result<void> Checkpoint(ShardId shard, SequenceId up_to_wal_seq) = 0;

  // Drops every key owned by `shard` and no other shard's. Per-shard so a
  // lagging shard's FLUSHDB replay can't clobber a peer's data (ADP-006, 010).
  virtual Result<void> Wipe(ShardId shard) = 0;

  virtual Result<StorageStats> Stats() = 0;
  virtual Result<void> Compact() = 0;

  // The loads below judge no TTL and delete nothing: an expired key is
  // returned with its TTL. Each reads one consistent view, and fails
  // kTimeout once `deadline` passes.

  // `key`'s whole state, or nullopt when cold does not hold it.
  virtual Result<std::optional<ColdKeyState>> LoadKey(std::string_view key,
                                                      SteadyTime deadline) = 0;
  // LoadKey, reading members only when `key` holds `type`. A string's
  // value comes with its meta, in one read.
  virtual Result<std::optional<LoadedAs>> LoadKeyAs(std::string_view key, KeyType type,
                                                    SteadyTime deadline) = 0;
  // `key`'s type and TTL, reading none of its members.
  virtual Result<std::optional<KeyMeta>> ProbeKey(std::string_view key, SteadyTime deadline) = 0;
  // Members or fields of `key` as `type`, in one batch, each nullopt
  // when absent.
  virtual Result<std::vector<std::optional<MemberValue>>> LoadMembers(
      std::string_view key, KeyType type, std::span<const std::string_view> members,
      SteadyTime deadline) = 0;

  // Server calls Start() once after recovery completes to enable any
  // backend-internal background work (TTL sweeping, prefetch, etc.). Stop()
  // is called during shutdown before the store is destroyed. Both are
  // idempotent. Backends with no background work leave the defaults.
  virtual Result<void> Start() { return {}; }
  virtual Result<void> Stop() { return {}; }
};

}  // namespace abyss::core
