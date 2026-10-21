#pragma once

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::testing {

// What an empty store sized for `keys` holds: its map's buckets. A
// sized store's buckets never grow, so its bytes move only with
// entries, as a production shard's do.
inline uint64_t EmptyStoreBytes(size_t keys) {
  return hot::SingleShardStore{hot::SingleShardConfig{.reserve_keys = keys}}.Stats().used_bytes;
}

// A string entry's own bytes, without its map's buckets.
inline uint64_t StringEntryBytes(std::string_view key, std::string_view value) {
  hot::SingleShardStore probe{hot::SingleShardConfig{.reserve_keys = 1}};
  const uint64_t empty = probe.Stats().used_bytes;
  EXPECT_TRUE(probe
                  .Apply(core::ops::WriteOp{core::ops::StringSet{.key = key, .value = value}},
                         core::EvictionTTL{86400})
                  .has_value());
  return probe.Stats().used_bytes - empty;
}

}  // namespace abyss::testing
