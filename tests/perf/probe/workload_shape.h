#pragma once

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "abyss/core/resp_types.h"

namespace abyss::perf::probe {

// How a probe's keys, values, TTLs and key types are drawn. `trace`
// follows the Twitter (OSDI '20) and Meta (FAST '20) cache traces:
//   - keys of 10-50 bytes;
//   - values: 30% of 16-99 bytes, 64% of 100-400 (median about 200),
//     1% up to 16 KiB and 5% of 16-64 KiB;
//   - 85% of keys, of every type, are created with a TTL of 60 s,
//     240 s, 1 h or 1 day, with 10% jitter, 47% of those under 300 s;
//   - `collection_fraction` of keys are sets and sorted sets, half each.
// Otherwise every key is a string of `value_bytes`, a share
// `large_fraction` of 16-64 KiB, with no TTL.
struct ShapeConfig {
  bool trace = false;
  uint64_t value_bytes = 64;
  double large_fraction = 0.0;
  double collection_fraction = 0.2;
};

enum class KeyType : uint8_t { kString, kSet, kZset };

class WorkloadShape {
 public:
  WorkloadShape(ShapeConfig config, uint64_t key_count);

  // Formatted up front: snprintf does not scale across threads on macOS.
  const std::string& Key(uint64_t index) const { return keys_[index]; }
  KeyType TypeOf(uint64_t index) const;
  // Zero for none.
  uint64_t TtlMs(uint64_t index, std::mt19937_64& rng) const;
  uint64_t ValueBytes(std::mt19937_64& rng) const;

  // A write of key `index` as its type takes one: SET (with its TTL),
  // SADD or ZADD, and for a collection with a TTL, a PEXPIRE one write
  // in four.
  core::RespCommand Write(uint64_t index, std::mt19937_64& rng) const;
  // A point read: GET, SISMEMBER or ZSCORE.
  core::RespCommand Read(uint64_t index, std::mt19937_64& rng) const;
  // A key's first write, with a value of at most 400 bytes, then a
  // PEXPIRE for a collection with a TTL.
  std::vector<core::RespCommand> Create(uint64_t index, std::mt19937_64& rng) const;

 private:
  core::RespCommand SetOf(uint64_t index, uint64_t value_bytes, std::mt19937_64& rng) const;

  ShapeConfig config_;
  std::vector<std::string> keys_;
  std::string filler_;
};

}  // namespace abyss::perf::probe
