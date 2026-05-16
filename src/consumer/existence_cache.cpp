#include "abyss/consumer/existence_cache.h"

#include <utility>

namespace abyss::consumer {

namespace {

constexpr size_t kEntryStructOverhead = 256;

}  // namespace

ExistenceCache::ExistenceCache(Config config, core::SteadyClockFn clock)
    : config_(config), clock_(std::move(clock)) {}

size_t ExistenceCache::EstimateEntryBytes(const Entry& e) const {
  size_t bytes = kEntryStructOverhead + e.primary.size() + e.secondary.size();
  if (e.kind == EntryKind::kKey && e.key_meta.string_value.has_value()) {
    bytes += e.key_meta.string_value->size();
  }
  if (e.kind == EntryKind::kField && e.field_meta.value_known) {
    bytes += e.field_meta.value.size();
  }
  return bytes;
}

void ExistenceCache::Touch(EntryIter it) {
  it->last_access = clock_();
  entries_.splice(entries_.end(), entries_, it);
}

void ExistenceCache::EvictEntry(EntryIter it, EvictionReason reason) {
  switch (it->kind) {
    case EntryKind::kKey:
      key_index_.erase(it->primary);
      break;
    case EntryKind::kMember:
      member_index_.erase(MemberKey(it->primary, it->secondary));
      break;
    case EntryKind::kField:
      field_index_.erase(FieldKey(it->primary, it->secondary));
      break;
  }
  bytes_ -= it->bytes;
  if (reason == EvictionReason::kTtl) {
    evictions_ttl_.fetch_add(1, std::memory_order_relaxed);
  } else {
    evictions_capacity_.fetch_add(1, std::memory_order_relaxed);
  }
  entries_.erase(it);
}

void ExistenceCache::EnforceCapacity() {
  while (!entries_.empty() &&
         (entries_.size() > config_.max_entries || bytes_ > config_.max_bytes)) {
    EvictEntry(entries_.begin(), EvictionReason::kCapacity);
  }
}

std::optional<ExistenceCache::KeyMeta> ExistenceCache::GetKey(std::string_view key) const {
  const std::scoped_lock lock(mu_);
  auto it = key_index_.find(KeyKey(key));
  if (it == key_index_.end()) return std::nullopt;
  // LRU touch needs a mutable list iterator; data state is unchanged.
  auto* self = const_cast<ExistenceCache*>(this);  // NOLINT(cppcoreguidelines-pro-type-const-cast)
  self->Touch(it->second);
  return it->second->key_meta;
}

void ExistenceCache::UpsertKey(std::string_view key, KeyMeta meta) {
  const std::scoped_lock lock(mu_);
  auto k = KeyKey(key);
  auto idx = key_index_.find(k);
  if (idx != key_index_.end()) {
    auto& entry = *idx->second;
    bytes_ -= entry.bytes;
    if (meta.string_value.has_value() &&
        meta.string_value->size() > config_.max_string_value_bytes) {
      meta.string_value.reset();
    }
    entry.key_meta = std::move(meta);
    entry.last_access = clock_();
    entry.bytes = EstimateEntryBytes(entry);
    bytes_ += entry.bytes;
    entries_.splice(entries_.end(), entries_, idx->second);
    EnforceCapacity();
    return;
  }
  if (meta.string_value.has_value() && meta.string_value->size() > config_.max_string_value_bytes) {
    meta.string_value.reset();
  }
  Entry entry{.kind = EntryKind::kKey,
              .primary = std::move(k),
              .secondary = {},
              .key_meta = std::move(meta),
              .member_meta = {},
              .field_meta = {},
              .last_access = clock_(),
              .bytes = 0};
  entry.bytes = EstimateEntryBytes(entry);
  entries_.push_back(std::move(entry));
  auto last = std::prev(entries_.end());
  key_index_.emplace(last->primary, last);
  bytes_ += last->bytes;
  EnforceCapacity();
}

void ExistenceCache::TombstoneKey(std::string_view key, core::SequenceId seq) {
  UpsertKey(key, KeyMeta{.exists = false,
                         .type = KeyType::kUnknown,
                         .abs_ttl_ms = 0,
                         .latest_seq = seq,
                         .string_value = std::nullopt});
}

std::optional<ExistenceCache::MemberMeta> ExistenceCache::GetMember(std::string_view key,
                                                                    std::string_view member) const {
  const std::scoped_lock lock(mu_);
  auto it = member_index_.find(MemberKey(key, member));
  if (it == member_index_.end()) return std::nullopt;
  auto* self = const_cast<ExistenceCache*>(this);  // NOLINT(cppcoreguidelines-pro-type-const-cast)
  self->Touch(it->second);
  return it->second->member_meta;
}

void ExistenceCache::UpsertMember(std::string_view key, std::string_view member, MemberMeta meta) {
  const std::scoped_lock lock(mu_);
  auto k = MemberKey(key, member);
  auto idx = member_index_.find(k);
  if (idx != member_index_.end()) {
    auto& entry = *idx->second;
    bytes_ -= entry.bytes;
    entry.member_meta = meta;
    entry.last_access = clock_();
    entry.bytes = EstimateEntryBytes(entry);
    bytes_ += entry.bytes;
    entries_.splice(entries_.end(), entries_, idx->second);
    EnforceCapacity();
    return;
  }
  Entry entry{.kind = EntryKind::kMember,
              .primary = std::string(key),
              .secondary = std::string(member),
              .key_meta = {},
              .member_meta = meta,
              .field_meta = {},
              .last_access = clock_(),
              .bytes = 0};
  entry.bytes = EstimateEntryBytes(entry);
  entries_.push_back(std::move(entry));
  auto last = std::prev(entries_.end());
  member_index_.emplace(std::move(k), last);
  bytes_ += last->bytes;
  EnforceCapacity();
}

void ExistenceCache::RemoveMember(std::string_view key, std::string_view member) {
  const std::scoped_lock lock(mu_);
  auto it = member_index_.find(MemberKey(key, member));
  if (it == member_index_.end()) return;
  EvictEntry(it->second, EvictionReason::kCapacity);
}

std::optional<ExistenceCache::FieldMeta> ExistenceCache::GetField(std::string_view key,
                                                                  std::string_view field) const {
  const std::scoped_lock lock(mu_);
  auto it = field_index_.find(FieldKey(key, field));
  if (it == field_index_.end()) return std::nullopt;
  auto* self = const_cast<ExistenceCache*>(this);  // NOLINT(cppcoreguidelines-pro-type-const-cast)
  self->Touch(it->second);
  return it->second->field_meta;
}

void ExistenceCache::UpsertField(std::string_view key, std::string_view field, FieldMeta meta) {
  const std::scoped_lock lock(mu_);
  auto k = FieldKey(key, field);
  auto idx = field_index_.find(k);
  if (idx != field_index_.end()) {
    auto& entry = *idx->second;
    bytes_ -= entry.bytes;
    entry.field_meta = std::move(meta);
    entry.last_access = clock_();
    entry.bytes = EstimateEntryBytes(entry);
    bytes_ += entry.bytes;
    entries_.splice(entries_.end(), entries_, idx->second);
    EnforceCapacity();
    return;
  }
  Entry entry{.kind = EntryKind::kField,
              .primary = std::string(key),
              .secondary = std::string(field),
              .key_meta = {},
              .member_meta = {},
              .field_meta = std::move(meta),
              .last_access = clock_(),
              .bytes = 0};
  entry.bytes = EstimateEntryBytes(entry);
  entries_.push_back(std::move(entry));
  auto last = std::prev(entries_.end());
  field_index_.emplace(std::move(k), last);
  bytes_ += last->bytes;
  EnforceCapacity();
}

void ExistenceCache::RemoveField(std::string_view key, std::string_view field) {
  const std::scoped_lock lock(mu_);
  auto it = field_index_.find(FieldKey(key, field));
  if (it == field_index_.end()) return;
  EvictEntry(it->second, EvictionReason::kCapacity);
}

void ExistenceCache::Clear() {
  const std::scoped_lock lock(mu_);
  entries_.clear();
  key_index_.clear();
  member_index_.clear();
  field_index_.clear();
  bytes_ = 0;
}

size_t ExistenceCache::SweepExpired() {
  const std::scoped_lock lock(mu_);
  const auto now = clock_();
  size_t swept = 0;
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (now - it->last_access < config_.entry_ttl) break;  // LRU-ordered
    auto next = std::next(it);
    EvictEntry(it, EvictionReason::kTtl);
    ++swept;
    it = next;
  }
  return swept;
}

size_t ExistenceCache::Size() const {
  const std::scoped_lock lock(mu_);
  return entries_.size();
}

size_t ExistenceCache::BytesEstimate() const {
  const std::scoped_lock lock(mu_);
  return bytes_;
}

uint64_t ExistenceCache::EvictionsTtl() const noexcept {
  return evictions_ttl_.load(std::memory_order_relaxed);
}

uint64_t ExistenceCache::EvictionsCapacity() const noexcept {
  return evictions_capacity_.load(std::memory_order_relaxed);
}

}  // namespace abyss::consumer
