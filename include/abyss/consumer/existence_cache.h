#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::consumer {

// Per-resolver-shard demand-driven cache. Hint-only; on miss the resolver
// falls through to the buffer and cold tiers. See ADP-011 §Existence cache.
class ExistenceCache {
 public:
  struct Config {
    std::chrono::seconds entry_ttl{86400};
    size_t max_entries = 10'000'000;
    size_t max_bytes = 1ULL << 30;
    size_t max_string_value_bytes = 4096;
  };

  enum class KeyType : uint8_t { kUnknown, kString, kHash, kSet, kZset };

  struct KeyMeta {
    bool exists = false;
    KeyType type = KeyType::kUnknown;
    uint64_t abs_ttl_ms = 0;
    core::SequenceId latest_seq = 0;
    std::optional<std::string> string_value;
  };

  struct MemberMeta {
    double score = 0.0;
    core::SequenceId latest_seq = 0;
  };

  struct FieldMeta {
    std::string value;
    bool value_known = false;
    core::SequenceId latest_seq = 0;
  };

  enum class EvictionReason : uint8_t { kTtl, kCapacity };

  ExistenceCache(Config config, core::SteadyClockFn clock);

  std::optional<KeyMeta> GetKey(std::string_view key) const ABYSS_EXCLUDES(mu_);
  void UpsertKey(std::string_view key, KeyMeta meta) ABYSS_EXCLUDES(mu_);
  void TombstoneKey(std::string_view key, core::SequenceId seq) ABYSS_EXCLUDES(mu_);

  std::optional<MemberMeta> GetMember(std::string_view key, std::string_view member) const
      ABYSS_EXCLUDES(mu_);
  void UpsertMember(std::string_view key, std::string_view member, MemberMeta meta)
      ABYSS_EXCLUDES(mu_);
  void RemoveMember(std::string_view key, std::string_view member) ABYSS_EXCLUDES(mu_);

  std::optional<FieldMeta> GetField(std::string_view key, std::string_view field) const
      ABYSS_EXCLUDES(mu_);
  void UpsertField(std::string_view key, std::string_view field, FieldMeta meta)
      ABYSS_EXCLUDES(mu_);
  void RemoveField(std::string_view key, std::string_view field) ABYSS_EXCLUDES(mu_);

  size_t SweepExpired() ABYSS_EXCLUDES(mu_);

  // Drops every cache entry. Used by the FLUSHDB path. Cumulative eviction
  // counters are preserved.
  void Clear() ABYSS_EXCLUDES(mu_);

  size_t Size() const ABYSS_EXCLUDES(mu_);
  size_t BytesEstimate() const ABYSS_EXCLUDES(mu_);
  uint64_t EvictionsTtl() const noexcept;
  uint64_t EvictionsCapacity() const noexcept;

 private:
  enum class EntryKind : uint8_t { kKey, kMember, kField };

  struct Entry {
    EntryKind kind;
    std::string primary;
    std::string secondary;
    KeyMeta key_meta;
    MemberMeta member_meta;
    FieldMeta field_meta;
    core::SteadyTime last_access;
    size_t bytes = 0;
  };

  using EntryList = std::list<Entry>;
  using EntryIter = EntryList::iterator;

  static std::string KeyKey(std::string_view k) { return std::string(k); }
  static std::string MemberKey(std::string_view k, std::string_view m) {
    std::string out;
    out.reserve(k.size() + 1 + m.size());
    out.append(k);
    out.push_back('\x1F');
    out.append(m);
    return out;
  }
  static std::string FieldKey(std::string_view k, std::string_view f) { return MemberKey(k, f); }

  size_t EstimateEntryBytes(const Entry& e) const;

  void Touch(EntryIter it) ABYSS_REQUIRES(mu_);
  void EnforceCapacity() ABYSS_REQUIRES(mu_);
  void EvictEntry(EntryIter it, EvictionReason reason) ABYSS_REQUIRES(mu_);

  Config config_;
  core::SteadyClockFn clock_;

  mutable std::mutex mu_;
  EntryList entries_ ABYSS_GUARDED_BY(mu_);
  std::unordered_map<std::string, EntryIter> key_index_ ABYSS_GUARDED_BY(mu_);
  std::unordered_map<std::string, EntryIter> member_index_ ABYSS_GUARDED_BY(mu_);
  std::unordered_map<std::string, EntryIter> field_index_ ABYSS_GUARDED_BY(mu_);
  size_t bytes_ ABYSS_GUARDED_BY(mu_) = 0;

  std::atomic<uint64_t> evictions_ttl_{0};
  std::atomic<uint64_t> evictions_capacity_{0};
};

}  // namespace abyss::consumer
