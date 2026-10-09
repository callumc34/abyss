#include "abyss/hot/single_shard_store.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <functional>
#include <iterator>
#include <limits>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/fatal.h"
#include "abyss/core/resp_format.h"

namespace abyss::hot {

namespace {

int64_t WallMs(core::WallTime t) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

int64_t WallMs(const core::WallClockFn& clock) { return WallMs(clock()); }

bool IsExpiredByTtl(const Entry& entry, const core::WallClockFn& clock) {
  if (entry.abs_ttl_ms == 0) return false;
  return WallMs(clock) >= entry.abs_ttl_ms;
}

// What an entry and its key count toward used bytes.
size_t Footprint(const Entry& entry, std::string_view key) {
  return entry.bytes + sizeof(std::string) + key.size();
}

// Only create/replace ops grow memory; delete/expire ops only shrink it, so
// the budget post-check runs solely on growth ops.
bool Grows(const core::ops::WriteOp& op) {
  return std::holds_alternative<core::ops::StringSet>(op) ||
         std::holds_alternative<core::ops::SetAdd>(op) ||
         std::holds_alternative<core::ops::ZsetAdd>(op) ||
         std::holds_alternative<core::ops::HashSet>(op) ||
         std::holds_alternative<core::ops::HashMSet>(op);
}

// Sets a flag for one scope, clearing it however the scope ends.
class FlagScope {
 public:
  explicit FlagScope(bool& flag) : flag_(flag) { flag_ = true; }
  FlagScope(const FlagScope&) = delete;
  FlagScope& operator=(const FlagScope&) = delete;
  FlagScope(FlagScope&&) = delete;
  FlagScope& operator=(FlagScope&&) = delete;
  ~FlagScope() { flag_ = false; }

 private:
  bool& flag_;
};

// One stored collection string, as ApproximateBytes counts it.
size_t StringBytes(const std::string& s) { return sizeof(std::string) + s.capacity(); }

// A zset score bucket's own cost, as ApproximateBytes counts it.
constexpr size_t kScoreBucketBytes = sizeof(double) + sizeof(std::set<std::string, std::less<>>);

core::SteadyTime AccessedAt(const Entry& entry) {
  const std::atomic_ref<core::SteadyTime::rep> stamp(entry.accessed);
  return core::SteadyTime{core::SteadyTime::duration{stamp.load(std::memory_order_relaxed)}};
}

bool TtlPassed(int64_t abs_ttl_ms, int64_t now_ms) {
  return abs_ttl_ms != 0 && now_ms >= abs_ttl_ms;
}

// Drops `member` from its score bucket, and the bucket once empty. Both
// finds are checked: a NaN score breaks the map's ordering.
void UnindexScore(ZsetValue& zset, size_t& bytes, double score, const std::string& member) {
  const auto bucket = zset.score_members.find(score);
  if (bucket == zset.score_members.end()) return;
  const auto pos = bucket->second.find(member);
  if (pos == bucket->second.end()) return;
  bytes -= StringBytes(*pos);
  bucket->second.erase(pos);
  if (bucket->second.empty()) {
    bytes -= kScoreBucketBytes;
    zset.score_members.erase(bucket);
  }
}

// Total, non-throwing parse of a ZRANGEBYSCORE bound into a double. A score
// bound is a remote-supplied string read under a shared lock, so a throwing
// conversion (std::stod) could unwind through the reactor; std::from_chars
// never throws and surfaces malformed input as a clean error Result instead.
// `empty_default`/the -inf/+inf sentinels stand in for an unbounded edge, since
// from_chars(double) does not itself accept "inf". C8's ParseLexBound should
// follow this same from_chars/Result discipline for lex bounds.
core::Result<double> ParseScoreBound(std::string_view s, double empty_default) {
  if (s.empty()) return empty_default;
  if (s == "-inf") return -std::numeric_limits<double>::infinity();
  if (s == "+inf" || s == "inf") return std::numeric_limits<double>::infinity();
  double value = 0.0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument,
                                       "not a valid score: '" + std::string(s) + "'"));
  }
  return value;
}

// Total, non-throwing parse of a ZRANGE index bound (a signed integer) into an
// int64. Empty stands in for the supplied default edge index. Replaces a
// throwing std::stoll on remote input read under the shared lock.
core::Result<int64_t> ParseIndexBound(std::string_view s, int64_t empty_default) {
  if (s.empty()) return empty_default;
  int64_t value = 0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument,
                                       "not a valid index: '" + std::string(s) + "'"));
  }
  return value;
}

// A parsed ZRANGEBYLEX bound. Redis lex syntax: `[value` (inclusive), `(value`
// (exclusive), `-` (negative infinity), `+` (positive infinity). A bare value
// with no prefix is a syntax error. Total and non-throwing — it never inspects
// a numeric conversion, so it follows C11's from_chars/Result discipline by
// surfacing malformed input as a clean error rather than unwinding.
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
  return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument,
                                     "not a valid lex range bound: '" + std::string(s) + "'"));
}

// True iff `member` is at or above the lex lower bound `min`.
bool LexAtOrAboveMin(std::string_view member, const LexBound& min) {
  if (min.neg_inf) return true;
  if (min.pos_inf) return false;
  return min.exclusive ? member > min.value : member >= min.value;
}

// True iff `member` is at or below the lex upper bound `max`.
bool LexAtOrBelowMax(std::string_view member, const LexBound& max) {
  if (max.pos_inf) return true;
  if (max.neg_inf) return false;
  return max.exclusive ? member < max.value : member <= max.value;
}

}  // namespace

size_t Entry::ApproximateBytes() const { return hot::ApproximateBytes(value); }

core::SteadyTime Entry::Deadline() const {
  return std::max(linked_at, AccessedAt(*this)) + eviction;
}

HoldBudget::HoldBudget(size_t max_entries) : max_entries_(max_entries) {}

HoldBudget HoldBudget::Unbounded() {
  HoldBudget budget(std::numeric_limits<size_t>::max());
  budget.timed_ = false;
  return budget;
}

bool HoldBudget::Take() {
  if (examined_ >= max_entries_) return false;
  if (timed_) {
    // From the first entry, so a hold with nothing to do reads no clock;
    // then every eighth, as one entry's work is far below it.
    if (examined_ == 0) {
      until_ = core::SteadyClock::now() + kHoldTime;
    } else if ((examined_ % 8) == 0 && core::SteadyClock::now() >= until_) {
      max_entries_ = examined_;
      return false;
    }
  }
  ++examined_;
  return true;
}

void SeqHeap::Push(Entry& entry) {
  heap_.push_back(&entry);
  entry.heap_slot = static_cast<uint32_t>(heap_.size() - 1);
  SiftUp(heap_.size() - 1);
}

void SeqHeap::Erase(Entry& entry) {
  const size_t slot = entry.heap_slot;
  entry.heap_slot = Entry::kNoSlot;
  Entry* last = heap_.back();
  heap_.pop_back();
  if (slot == heap_.size()) return;
  Place(slot, last);
  SiftUp(slot);
  SiftDown(last->heap_slot);
}

void SeqHeap::Update(Entry& entry) {
  SiftUp(entry.heap_slot);
  SiftDown(entry.heap_slot);
}

void SeqHeap::Place(size_t slot, Entry* entry) {
  heap_[slot] = entry;
  entry->heap_slot = static_cast<uint32_t>(slot);
}

void SeqHeap::SiftUp(size_t slot) {
  Entry* entry = heap_[slot];
  while (slot > 0) {
    const size_t parent = (slot - 1) / 2;
    if (heap_[parent]->latest_seq <= entry->latest_seq) break;
    Place(slot, heap_[parent]);
    slot = parent;
  }
  Place(slot, entry);
}

void SeqHeap::SiftDown(size_t slot) {
  Entry* entry = heap_[slot];
  for (;;) {
    size_t child = (2 * slot) + 1;
    if (child >= heap_.size()) break;
    if (child + 1 < heap_.size() && heap_[child + 1]->latest_seq < heap_[child]->latest_seq) {
      ++child;
    }
    if (entry->latest_seq <= heap_[child]->latest_seq) break;
    Place(slot, heap_[child]);
    slot = child;
  }
  Place(slot, entry);
}

size_t ApproximateBytes(const Value& value) {
  size_t bytes = sizeof(Entry);
  std::visit(
      [&bytes](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string>) {
          bytes += v.capacity();
        } else if constexpr (std::is_same_v<T, SetValue>) {
          for (const auto& m : v.members) {
            bytes += sizeof(m) + m.capacity();
          }
        } else if constexpr (std::is_same_v<T, HashValue>) {
          for (const auto& [k, val] : v.fields) {
            bytes += sizeof(k) + k.capacity() + sizeof(val) + val.capacity();
          }
        } else if constexpr (std::is_same_v<T, ZsetValue>) {
          for (const auto& [m, s] : v.member_scores) {
            bytes += sizeof(m) + m.capacity() + sizeof(s);
          }
          // The score-ordered index duplicates every member string and adds
          // map/set node overhead. Counting only member_scores understated a
          // zset by ~half its real heap; include score_members so used bytes
          // is the single, correct hot-memory oracle.
          for (const auto& [score, members] : v.score_members) {
            bytes += sizeof(score) + sizeof(members);
            for (const auto& m : members) {
              bytes += sizeof(m) + m.capacity();
            }
          }
        }
      },
      value);
  return bytes;
}

LoadedFull MakeLoadedFull(Value value, int64_t abs_ttl_ms) {
  const size_t bytes = ApproximateBytes(value);
  return LoadedFull{.value = std::move(value), .abs_ttl_ms = abs_ttl_ms, .bytes = bytes};
}

std::string_view KeyView::string_value() const {
  const auto* str = value != nullptr ? std::get_if<std::string>(value) : nullptr;
  return str != nullptr ? std::string_view(*str) : std::string_view{};
}

bool KeyView::set_has(std::string_view member) const {
  const auto* set = value != nullptr ? std::get_if<SetValue>(value) : nullptr;
  return set != nullptr && set->members.contains(member);
}

std::optional<double> KeyView::zset_score(std::string_view member) const {
  const auto* zset = value != nullptr ? std::get_if<ZsetValue>(value) : nullptr;
  if (zset == nullptr) return std::nullopt;
  const auto it = zset->member_scores.find(member);
  if (it == zset->member_scores.end()) return std::nullopt;
  return it->second;
}

bool KeyView::hash_has(std::string_view field) const { return hash_get(field).has_value(); }

std::optional<std::string_view> KeyView::hash_get(std::string_view field) const {
  const auto* hash = value != nullptr ? std::get_if<HashValue>(value) : nullptr;
  if (hash == nullptr) return std::nullopt;
  const auto it = hash->fields.find(field);
  if (it == hash->fields.end()) return std::nullopt;
  return std::string_view(it->second);
}

size_t KeyView::collection_size() const {
  if (value == nullptr) return 0;
  return std::visit(
      [](const auto& v) -> size_t {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, SetValue>) {
          return v.members.size();
        } else if constexpr (std::is_same_v<T, HashValue>) {
          return v.fields.size();
        } else if constexpr (std::is_same_v<T, ZsetValue>) {
          return v.member_scores.size();
        } else {
          return 0;
        }
      },
      *value);
}

const Stub* StubCache::Find(std::string_view key) const {
  if (index_.empty()) return nullptr;
  const auto it = index_.find(key);
  return it == index_.end() ? nullptr : &it->second->stub;
}

void StubCache::Put(std::string_view key, const Stub& stub, bool trim) {
  if (max_entries_ == 0) return;
  if (const auto it = index_.find(key); it != index_.end()) {
    it->second->stub = stub;
    order_.splice(order_.begin(), order_, it->second);
  } else {
    order_.push_front(Node{.key = std::string(key), .stub = stub});
    index_.emplace(order_.front().key, order_.begin());
    bytes_ += kStubBytes + key.size();
  }
  while (trim && index_.size() > max_entries_) {
    EraseNode(std::prev(order_.end()));
    ++drops_;
  }
}

StubCache StubCache::Release() {
  StubCache released(max_entries_);
  released.order_.swap(order_);
  released.index_.swap(index_);
  released.bytes_ = bytes_;
  bytes_ = 0;
  return released;
}

bool StubCache::Erase(std::string_view key) {
  if (index_.empty()) return false;
  const auto it = index_.find(key);
  if (it == index_.end()) return false;
  EraseNode(it->second);
  return true;
}

void StubCache::Clear() {
  index_.clear();
  order_.clear();
  bytes_ = 0;
}

void StubCache::EraseNode(Order::iterator node) {
  bytes_ -= kStubBytes + node->key.size();
  index_.erase(node->key);
  order_.erase(node);
}

SingleShardStore::SingleShardStore(SingleShardConfig config)
    : config_(std::move(config)),
      governor_(config_.max_memory_bytes),
      stubs_(config_.stub_max_entries) {
  if (config_.reserve_keys > 0) entries_.reserve(config_.reserve_keys);
}

// --- Read operations (const) ---

namespace {

core::Error NotFound() { return {core::ErrorCode::kNotFound, ""}; }

core::Error WrongType() {
  return {core::ErrorCode::kWrongType, "Operation against a key holding the wrong kind of value"};
}

std::string_view TypeName(Entry::Type type) {
  switch (type) {
    case Entry::Type::kSet:
      return "set";
    case Entry::Type::kHash:
      return "hash";
    case Entry::Type::kZset:
      return "zset";
    case Entry::Type::kString:
      break;
  }
  return "string";
}

core::RespValue ReadOf(const core::ops::StringGet& /*op*/, const std::string& value) {
  return core::RespValue::BulkString(value);
}

core::RespValue ReadOf(const core::ops::SetIsMember& op, const SetValue& set) {
  return core::RespValue::Integer(set.members.contains(op.member) ? 1 : 0);
}

core::RespValue ReadOf(const core::ops::SetMembers& /*op*/, const SetValue& set) {
  std::vector<core::RespValue> elements;
  elements.reserve(set.members.size());
  for (const auto& m : set.members) {
    elements.push_back(core::RespValue::BulkString(m));
  }
  return core::RespValue::Array(std::move(elements));
}

core::RespValue ReadOf(const core::ops::SetCard& /*op*/, const SetValue& set) {
  return core::RespValue::Integer(static_cast<int64_t>(set.members.size()));
}

core::RespValue ReadOf(const core::ops::ZsetScore& op, const ZsetValue& zset) {
  auto it = zset.member_scores.find(op.member);
  if (it == zset.member_scores.end()) {
    return core::RespValue::Null();
  }
  return core::RespValue::BulkString(core::FormatRespDouble(it->second));
}

core::RespValue ReadOf(const core::ops::ZsetCard& /*op*/, const ZsetValue& zset) {
  return core::RespValue::Integer(static_cast<int64_t>(zset.member_scores.size()));
}

core::Result<core::RespValue> ReadOf(const core::ops::ZsetRange& op, const ZsetValue& zset) {
  std::vector<core::RespValue> elements;

  if (op.by_lex) {
    auto min_parsed = ParseLexBound(op.min);
    if (!min_parsed.has_value()) return std::unexpected(min_parsed.error());
    auto max_parsed = ParseLexBound(op.max);
    if (!max_parsed.has_value()) return std::unexpected(max_parsed.error());

    // Lex range is over member names in pure lexicographic order (Redis assumes
    // equal scores). member_scores keys are the members; collect and sort so
    // the order matches cold's member-indexed CF scan byte-for-byte.
    std::vector<std::string_view> members;
    members.reserve(zset.member_scores.size());
    for (const auto& [member, score] : zset.member_scores) {
      if (LexAtOrAboveMin(member, *min_parsed) && LexAtOrBelowMax(member, *max_parsed)) {
        members.emplace_back(member);
      }
    }
    std::ranges::sort(members);
    if (op.rev) std::ranges::reverse(members);

    int64_t start = std::max<int64_t>(op.offset, 0);
    int64_t count = op.count < 0 ? static_cast<int64_t>(members.size()) : op.count;
    if (std::cmp_less(start, members.size())) {
      auto end = std::min(start + count, static_cast<int64_t>(members.size()));
      for (int64_t i = start; i < end; ++i) {
        elements.push_back(
            core::RespValue::BulkString(std::string(members[static_cast<size_t>(i)])));
      }
    }
  } else if (op.by_score) {
    auto min_parsed = ParseScoreBound(op.min, -std::numeric_limits<double>::infinity());
    if (!min_parsed.has_value()) return std::unexpected(min_parsed.error());
    auto max_parsed = ParseScoreBound(op.max, std::numeric_limits<double>::infinity());
    if (!max_parsed.has_value()) return std::unexpected(max_parsed.error());
    double min_score = *min_parsed;
    double max_score = *max_parsed;

    if (op.rev) std::swap(min_score, max_score);

    auto lo = zset.score_members.lower_bound(std::min(min_score, max_score));
    auto hi = zset.score_members.upper_bound(std::max(min_score, max_score));

    std::vector<std::pair<std::string, double>> collected;
    for (auto it = lo; it != hi; ++it) {
      for (const auto& member : it->second) {
        collected.emplace_back(member, it->first);
      }
    }

    if (op.rev) {
      std::ranges::reverse(collected);
    }

    int64_t start = op.offset;
    int64_t count = op.count < 0 ? static_cast<int64_t>(collected.size()) : op.count;
    if (std::cmp_less(start, collected.size())) {
      auto end = std::min(start + count, static_cast<int64_t>(collected.size()));
      for (int64_t i = start; i < end; ++i) {
        elements.push_back(core::RespValue::BulkString(collected[static_cast<size_t>(i)].first));
        if (op.with_scores) {
          elements.push_back(core::RespValue::BulkString(
              core::FormatRespDouble(collected[static_cast<size_t>(i)].second)));
        }
      }
    }
  } else {
    // Index-based range — convert to score iteration
    const auto& sm = zset.score_members;
    std::vector<std::pair<std::string, double>> all;
    for (const auto& [score, members] : sm) {
      for (const auto& member : members) {
        all.emplace_back(member, score);
      }
    }
    if (op.rev) {
      std::ranges::reverse(all);
    }

    auto min_parsed = ParseIndexBound(op.min, 0);
    if (!min_parsed.has_value()) return std::unexpected(min_parsed.error());
    auto max_parsed = ParseIndexBound(op.max, static_cast<int64_t>(all.size()) - 1);
    if (!max_parsed.has_value()) return std::unexpected(max_parsed.error());
    int64_t min_idx = *min_parsed;
    int64_t max_idx = *max_parsed;

    if (min_idx < 0) min_idx += static_cast<int64_t>(all.size());
    if (max_idx < 0) max_idx += static_cast<int64_t>(all.size());
    min_idx = std::max(min_idx, int64_t{0});
    max_idx = std::min(max_idx, static_cast<int64_t>(all.size()) - 1);

    // NOLINTNEXTLINE(bugprone-infinite-loop)
    for (int64_t i = min_idx; i <= max_idx; ++i) {
      elements.push_back(core::RespValue::BulkString(all[static_cast<size_t>(i)].first));
      if (op.with_scores) {
        elements.push_back(core::RespValue::BulkString(
            core::FormatRespDouble(all[static_cast<size_t>(i)].second)));
      }
    }
  }

  return core::RespValue::Array(std::move(elements));
}

core::RespValue ReadOf(const core::ops::HashGet& op, const HashValue& hash) {
  auto it = hash.fields.find(op.field);
  if (it == hash.fields.end()) {
    return core::RespValue::Null();
  }
  return core::RespValue::BulkString(it->second);
}

core::RespValue ReadOf(const core::ops::HashGetAll& /*op*/, const HashValue& hash) {
  std::vector<core::RespValue> elements;
  elements.reserve(hash.fields.size() * 2);
  for (const auto& [k, v] : hash.fields) {
    elements.push_back(core::RespValue::BulkString(k));
    elements.push_back(core::RespValue::BulkString(v));
  }
  return core::RespValue::Array(std::move(elements));
}

core::RespValue ReadOf(const core::ops::HashMultiGet& op, const HashValue& hash) {
  std::vector<core::RespValue> elements;
  elements.reserve(op.fields.size());
  for (auto field : op.fields) {
    auto it = hash.fields.find(field);
    if (it == hash.fields.end()) {
      elements.push_back(core::RespValue::Null());
    } else {
      elements.push_back(core::RespValue::BulkString(it->second));
    }
  }
  return core::RespValue::Array(std::move(elements));
}

core::RespValue ReadOf(const core::ops::HashFieldExists& op, const HashValue& hash) {
  return core::RespValue::Integer(hash.fields.contains(op.field) ? 1 : 0);
}

core::RespValue ReadOf(const core::ops::HashKeys& /*op*/, const HashValue& hash) {
  std::vector<core::RespValue> elements;
  elements.reserve(hash.fields.size());
  for (const auto& [k, _] : hash.fields) {
    elements.push_back(core::RespValue::BulkString(k));
  }
  return core::RespValue::Array(std::move(elements));
}

core::RespValue ReadOf(const core::ops::HashVals& /*op*/, const HashValue& hash) {
  std::vector<core::RespValue> elements;
  elements.reserve(hash.fields.size());
  for (const auto& [_, v] : hash.fields) {
    elements.push_back(core::RespValue::BulkString(v));
  }
  return core::RespValue::Array(std::move(elements));
}

core::RespValue ReadOf(const core::ops::HashLen& /*op*/, const HashValue& hash) {
  return core::RespValue::Integer(static_cast<int64_t>(hash.fields.size()));
}

// The type a read of `Op` needs its key to hold.
template <typename Op>
constexpr Entry::Type ReadType() {
  if constexpr (std::is_same_v<Op, core::ops::StringGet>) {
    return Entry::Type::kString;
  } else if constexpr (std::is_same_v<Op, core::ops::SetIsMember> ||
                       std::is_same_v<Op, core::ops::SetMembers> ||
                       std::is_same_v<Op, core::ops::SetCard>) {
    return Entry::Type::kSet;
  } else if constexpr (std::is_same_v<Op, core::ops::ZsetScore> ||
                       std::is_same_v<Op, core::ops::ZsetCard> ||
                       std::is_same_v<Op, core::ops::ZsetRange>) {
    return Entry::Type::kZset;
  } else {
    return Entry::Type::kHash;
  }
}

// `op` over `value`, which must hold a V.
template <typename V, typename Op>
core::Result<core::RespValue> ReadAs(const Op& op, const Value& value) {
  const auto* typed = std::get_if<V>(&value);
  if (typed == nullptr) return std::unexpected(WrongType());
  return ReadOf(op, *typed);
}

}  // namespace

bool IsMetaRead(const core::ops::ReadOp& op) {
  return std::holds_alternative<core::ops::Exists>(op) ||
         std::holds_alternative<core::ops::Ttl>(op) || std::holds_alternative<core::ops::Type>(op);
}

core::RespValue EmptyReadResponse(const core::ops::ReadOp& op) {
  return std::visit(
      [](const auto& o) -> core::RespValue {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringGet> ||
                      std::is_same_v<T, core::ops::ZsetScore> ||
                      std::is_same_v<T, core::ops::HashGet>) {
          return core::RespValue::Null();
        } else if constexpr (std::is_same_v<T, core::ops::SetIsMember> ||
                             std::is_same_v<T, core::ops::SetCard> ||
                             std::is_same_v<T, core::ops::ZsetCard> ||
                             std::is_same_v<T, core::ops::HashFieldExists> ||
                             std::is_same_v<T, core::ops::HashLen> ||
                             std::is_same_v<T, core::ops::Exists>) {
          return core::RespValue::Integer(0);
        } else if constexpr (std::is_same_v<T, core::ops::Ttl>) {
          return core::RespValue::Integer(-2);
        } else if constexpr (std::is_same_v<T, core::ops::Type>) {
          return core::RespValue::SimpleString("none");
        } else if constexpr (std::is_same_v<T, core::ops::HashMultiGet>) {
          return core::RespValue::Array(
              std::vector<core::RespValue>(o.fields.size(), core::RespValue::Null()));
        } else {
          // SetMembers, ZsetRange, HashGetAll, HashKeys, HashVals.
          return core::RespValue::Array({});
        }
      },
      op);
}

core::Result<core::RespValue> AnswerRead(const core::ops::ReadOp& op, Entry::Type type,
                                         const Value* value, int64_t abs_ttl_ms, int64_t now_ms) {
  return std::visit(
      [&](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return core::RespValue::Integer(1);
        } else if constexpr (std::is_same_v<T, core::ops::Type>) {
          return core::RespValue::SimpleString(std::string(TypeName(type)));
        } else if constexpr (std::is_same_v<T, core::ops::Ttl>) {
          if (abs_ttl_ms == 0) return core::RespValue::Integer(-1);
          const int64_t left = std::max<int64_t>(abs_ttl_ms - now_ms, 0);
          // Redis rounds TTL to the nearest second.
          return core::RespValue::Integer(o.millis ? left : (left + 500) / 1000);
        } else {
          constexpr Entry::Type kWant = ReadType<T>();
          if (type != kWant) return std::unexpected(WrongType());
          if (value == nullptr) return std::unexpected(NotFound());
          if constexpr (kWant == Entry::Type::kString) {
            return ReadAs<std::string>(o, *value);
          } else if constexpr (kWant == Entry::Type::kSet) {
            return ReadAs<SetValue>(o, *value);
          } else if constexpr (kWant == Entry::Type::kZset) {
            return ReadAs<ZsetValue>(o, *value);
          } else {
            return ReadAs<HashValue>(o, *value);
          }
        }
      },
      op);
}

core::Result<core::RespValue> SingleShardStore::Exec(const core::ops::ReadOp& op) const {
  if (const auto* exists = std::get_if<core::ops::Exists>(&op)) {
    int64_t count = 0;
    for (auto key : exists->keys) {
      if (FindLiveEntry(key) != nullptr) ++count;
    }
    return core::RespValue::Integer(count);
  }
  const Entry* entry = FindEntry(core::ops::PrimaryKey(op));
  // A tombstone is authoritative: the key was deleted.
  if (entry != nullptr && entry->tombstoned) return EmptyReadResponse(op);
  const int64_t now_ms = WallMs(config_.wall_clock);
  if (entry == nullptr || TtlPassed(entry->abs_ttl_ms, now_ms)) return std::unexpected(NotFound());
  if (!IsMetaRead(op)) NoteAccess(*entry);
  return AnswerRead(op, entry->type, &entry->value, entry->abs_ttl_ms, now_ms);
}
// --- Write operations ---

core::Result<core::RespValue> SingleShardStore::Apply(const core::ops::WriteOp& op,
                                                      core::EvictionTTL eviction,
                                                      core::SequenceId seq,
                                                      core::SequenceId horizon) {
  auto result = Mutate(op, eviction, seq, SetMove{});
  HoldBudget budget = HoldBudget::Capped();
  // The write has already applied (and is durable in the queue). Make room by
  // evicting OTHER LRU victims down to the budget, protecting the just-written
  // key. If even then the entry cannot fit (a single value larger than the
  // whole budget, no other victims), surface kResourceExhausted as an admission
  // signal — the entry is NOT lost, it stays durable in the queue/cold
  // (invariant 2). Suppressed during replay.
  if (Grows(op) && result.has_value() &&
      !EnsureCapacityFor(core::ops::PrimaryKey(op), horizon, budget)) {
    return std::unexpected(
        core::Error(core::ErrorCode::kResourceExhausted, "hot store memory budget exhausted"));
  }
  return result;
}

core::Result<core::RespValue> SingleShardStore::Mutate(const core::ops::WriteOp& op,
                                                       core::EvictionTTL eviction,
                                                       core::SequenceId seq, SetMove move) {
  auto result = std::visit(
      [this, eviction, seq, move](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringSet>) {
          return ApplyStringSet(o, eviction, move);
        } else if constexpr (std::is_same_v<T, core::ops::Del>) {
          return ApplyDel(o, seq);
        } else if constexpr (std::is_same_v<T, core::ops::SetAdd>) {
          return ApplySetAdd(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::SetRem>) {
          return ApplySetRem(o, seq);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetAdd>) {
          return ApplyZsetAdd(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetRem>) {
          return ApplyZsetRem(o, seq);
        } else if constexpr (std::is_same_v<T, core::ops::HashSet>) {
          return ApplyHashSet(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::HashMSet>) {
          return ApplyHashMSet(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::HashDel>) {
          return ApplyHashDel(o, seq);
        } else if constexpr (std::is_same_v<T, core::ops::Expire>) {
          return ApplyExpire(o);
        } else if constexpr (std::is_same_v<T, core::ops::Persist>) {
          return ApplyPersist(o);
        } else {
          return std::unexpected(
              core::Error(core::ErrorCode::kInternal, "unsupported write operation"));
        }
      },
      op);

  if (const auto* del = std::get_if<core::ops::Del>(&op)) {
    for (const auto key : del->keys) MarkWritten(key, seq);
  } else {
    MarkWritten(core::ops::PrimaryKey(op), seq);
  }
  return result;
}

std::vector<core::RespValue> SingleShardStore::ApplyEffects(std::span<core::Effect> effects,
                                                            core::SequenceId first_seq,
                                                            core::WallTime appended_at,
                                                            const core::EvictionPolicy& policy,
                                                            core::SequenceId horizon) {
  ABYSS_DCHECK(first_seq >= core::kFirstSeq, "effects applied from seq 0, which names no entry");
  const int64_t at_ms = WallMs(appended_at);
  const FlagScope applying(applying_effects_);
  std::vector<core::RespValue> replies;
  replies.reserve(effects.size());
  // One hold's eviction for the whole call: an overshoot is the
  // eviction worker's and backpressure's to bound.
  HoldBudget budget = HoldBudget::Capped();
  for (size_t i = 0; i < effects.size(); ++i) {
    core::Effect& effect = effects[i];
    auto op = core::ops::ParseWriteOp(effect.cmd.Name(), effect.cmd, static_cast<uint64_t>(at_ms));
    if (!op.has_value()) {
      core::Fatal("decided effect does not parse: " + op.error().message());
    }
    const auto key = core::ops::PrimaryKey(*op);
    ABYSS_DCHECK(ExpiryIsLogged(effect, key, at_ms),
                 "an effect that reads state met a key expired at appended_at: " + effect.key);
    const SetMove move{
        .value = std::holds_alternative<core::ops::StringSet>(*op) ? &effect.cmd.args[2] : nullptr,
        .reply_old_value = effect.reply_old_value,
    };
    auto result = Mutate(*op, policy.Resolve(key), first_seq + i, move);
    if (effect.observed_expiry) ++expired_count_;
    // Decided effects always apply: an overshoot shows in Stats.
    if (Grows(*op)) EnsureCapacityFor(key, horizon, budget);
    // Decide ruled out every apply error and the effect is logged, so
    // an error here means hot and the log disagree.
    if (!result.has_value()) {
      core::Fatal("a decided effect failed to apply: " + result.error().message());
    }
    replies.push_back(std::move(*result));
  }
  return replies;
}

core::Result<void> SingleShardStore::ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                                core::EvictionTTL eviction, core::SequenceId seq,
                                                core::SequenceId horizon) {
  for (const auto& op : ops) {
    auto result = Apply(op, eviction, seq, horizon);
    if (!result.has_value()) return std::unexpected(result.error());
  }
  return {};
}

core::Result<core::RespValue> SingleShardStore::ApplyStringSet(const core::ops::StringSet& op,
                                                               core::EvictionTTL eviction,
                                                               SetMove move) {
  // op.value views *move.value, so it is read before the move.
  const auto take_value = [&op, move] {
    return move.value != nullptr ? std::move(*move.value) : std::string(op.value);
  };
  core::RespValue reply =
      move.reply_old_value ? core::RespValue::Null() : core::RespValue::SimpleString("OK");
  auto it = entries_.find(op.key);
  // A tombstone is treated as absent: fall to the create path so the key is
  // resurrected as a live string (with the key_count increment that implies).
  if (it != entries_.end() && !it->second.tombstoned) {
    if (it->second.type != Entry::Type::kString) {
      TrackRemove(it->second, op.key);
      it->second.type = Entry::Type::kString;
      Bury(it->second.value);
      it->second.value = take_value();
      it->second.bytes = it->second.ApproximateBytes();
      it->second.eviction = eviction;
      it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
      TrackInsert(it->second, op.key);
      return reply;
    }
    TrackRemove(it->second, op.key);
    auto& value = std::get<std::string>(it->second.value);
    if (move.reply_old_value && !ExpiredForApply(it->second)) {
      reply = core::RespValue::BulkString(std::move(value));
    }
    Bury(it->second.value);
    it->second.value = take_value();
    it->second.bytes = it->second.ApproximateBytes();
    it->second.eviction = eviction;
    it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
    TrackInsert(it->second, op.key);
    return reply;
  }

  // GetOrCreateEntry now tracks an empty-entry baseline; remove it before
  // re-adding the populated footprint so the create path stays balanced.
  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kString, eviction);
  TrackRemove(entry, op.key);
  entry.value = take_value();
  entry.bytes = entry.ApproximateBytes();
  entry.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
  TrackInsert(entry, op.key);
  return reply;
}

core::Result<core::RespValue> SingleShardStore::ApplyDel(const core::ops::Del& op,
                                                         core::SequenceId seq) {
  int64_t removed = 0;
  for (auto key : op.keys) {
    auto it = entries_.find(key);
    if (it == entries_.end()) {
      InsertTombstone(key, seq);
      continue;
    }
    if (it->second.tombstoned) continue;
    // Already TTL-dead, so not counted (Redis parity), but tombstoned
    // all the same: an older value may still be in buffer or cold.
    const bool expired = ExpiredForApply(it->second);
    TombstoneEntry(it->second, key, seq);
    if (expired) {
      ++expired_count_;
    } else {
      ++removed;
    }
  }
  return core::RespValue::Integer(removed);
}

core::Result<core::RespValue> SingleShardStore::ApplySetAdd(const core::ops::SetAdd& op,
                                                            core::EvictionTTL eviction) {
  auto it = entries_.find(op.key);
  if (it != entries_.end() && !it->second.tombstoned && !ExpiredForApply(it->second) &&
      it->second.type != Entry::Type::kSet) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned && ExpiredForApply(it->second)) {
    RemoveEntry(op.key);
  }

  // GetOrCreateEntry establishes a tracked empty-entry baseline on create or
  // tombstone-resurrect, so this leading TrackRemove always has a matching
  // prior TrackInsert (HOT-2). The pattern is then balanced for every case:
  // remove the old (empty or populated) footprint, mutate, re-add the new one.
  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kSet, eviction);
  TrackRemove(entry, op.key);
  auto& members = std::get<SetValue>(entry.value).members;
  int64_t added = 0;
  for (auto member : op.members) {
    const auto [pos, inserted] = members.insert(std::string(member));
    if (!inserted) continue;
    entry.bytes += StringBytes(*pos);
    ++added;
  }
  entry.eviction = eviction;
  TrackInsert(entry, op.key);
  return core::RespValue::Integer(added);
}

core::Result<core::RespValue> SingleShardStore::ApplySetRem(const core::ops::SetRem& op,
                                                            core::SequenceId seq) {
  auto it = entries_.find(op.key);
  if (it == entries_.end() || it->second.tombstoned || ExpiredForApply(it->second)) {
    return core::RespValue::Integer(0);
  }
  if (it->second.type != Entry::Type::kSet) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& members = std::get<SetValue>(it->second.value).members;
  int64_t removed = 0;
  for (auto member : op.members) {
    const auto pos = members.find(member);
    if (pos == members.end()) continue;
    it->second.bytes -= StringBytes(*pos);
    members.erase(pos);
    ++removed;
  }
  TrackInsert(it->second, op.key);
  // Emptying a collection deletes the key (Redis semantics); leave a tombstone
  // so the delete is authoritative against a lagging overlay.
  if (members.empty()) {
    TombstoneEntry(it->second, op.key, seq);
  }
  return core::RespValue::Integer(removed);
}

core::Result<core::RespValue> SingleShardStore::ApplyZsetAdd(const core::ops::ZsetAdd& op,
                                                             core::EvictionTTL eviction) {
  auto it = entries_.find(op.key);
  if (it != entries_.end() && !it->second.tombstoned && !ExpiredForApply(it->second) &&
      it->second.type != Entry::Type::kZset) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned && ExpiredForApply(it->second)) {
    RemoveEntry(op.key);
  }

  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kZset, eviction);
  TrackRemove(entry, op.key);
  auto& zset = std::get<ZsetValue>(entry.value);

  int64_t added = 0;
  for (const auto& e : op.entries) {
    std::string member(e.member);
    const auto [scored, inserted] = zset.member_scores.try_emplace(member, e.score);
    if (inserted) {
      entry.bytes += StringBytes(scored->first) + sizeof(double);
      ++added;
    } else {
      UnindexScore(zset, entry.bytes, scored->second, member);
      scored->second = e.score;
    }
    const auto [bucket, new_bucket] = zset.score_members.try_emplace(e.score);
    if (new_bucket) entry.bytes += kScoreBucketBytes;
    entry.bytes += StringBytes(*bucket->second.insert(std::move(member)).first);
  }

  entry.eviction = eviction;
  TrackInsert(entry, op.key);
  return core::RespValue::Integer(added);
}

core::Result<core::RespValue> SingleShardStore::ApplyZsetRem(const core::ops::ZsetRem& op,
                                                             core::SequenceId seq) {
  auto it = entries_.find(op.key);
  if (it == entries_.end() || it->second.tombstoned || ExpiredForApply(it->second)) {
    return core::RespValue::Integer(0);
  }
  if (it->second.type != Entry::Type::kZset) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& zset = std::get<ZsetValue>(it->second.value);

  int64_t removed = 0;
  for (auto member : op.members) {
    const auto scored = zset.member_scores.find(member);
    if (scored == zset.member_scores.end()) continue;
    UnindexScore(zset, it->second.bytes, scored->second, scored->first);
    it->second.bytes -= StringBytes(scored->first) + sizeof(double);
    zset.member_scores.erase(scored);
    ++removed;
  }

  TrackInsert(it->second, op.key);
  if (zset.member_scores.empty()) {
    TombstoneEntry(it->second, op.key, seq);
  }
  return core::RespValue::Integer(removed);
}

core::Result<core::RespValue> SingleShardStore::ApplyHashSet(const core::ops::HashSet& op,
                                                             core::EvictionTTL eviction) {
  auto it = entries_.find(op.key);
  if (it != entries_.end() && !it->second.tombstoned && !ExpiredForApply(it->second) &&
      it->second.type != Entry::Type::kHash) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned && ExpiredForApply(it->second)) {
    RemoveEntry(op.key);
  }

  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kHash, eviction);
  TrackRemove(entry, op.key);
  auto& fields = std::get<HashValue>(entry.value).fields;
  int64_t new_fields = 0;
  for (const auto& fv : op.fields) {
    auto [field_it, inserted] = fields.try_emplace(std::string(fv.field), std::string(fv.value));
    if (inserted) {
      entry.bytes += StringBytes(field_it->first) + StringBytes(field_it->second);
      ++new_fields;
    } else {
      entry.bytes -= StringBytes(field_it->second);
      field_it->second = std::string(fv.value);
      entry.bytes += StringBytes(field_it->second);
    }
  }
  entry.eviction = eviction;
  TrackInsert(entry, op.key);
  return core::RespValue::Integer(new_fields);
}

core::Result<core::RespValue> SingleShardStore::ApplyHashMSet(const core::ops::HashMSet& op,
                                                              core::EvictionTTL eviction) {
  // HMSET shares HSET's apply path; the only difference is the reply (+OK
  // vs new-field count). Constructing a transient HashSet preserves type
  // safety for any future hot-store apply changes.
  const core::ops::HashSet hset{.key = op.key, .fields = op.fields};
  auto r = ApplyHashSet(hset, eviction);
  if (!r.has_value()) return r;
  return core::RespValue::SimpleString("OK");
}

core::Result<core::RespValue> SingleShardStore::ApplyHashDel(const core::ops::HashDel& op,
                                                             core::SequenceId seq) {
  auto it = entries_.find(op.key);
  if (it == entries_.end() || it->second.tombstoned || ExpiredForApply(it->second)) {
    return core::RespValue::Integer(0);
  }
  if (it->second.type != Entry::Type::kHash) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& fields = std::get<HashValue>(it->second.value).fields;
  int64_t removed = 0;
  for (auto field : op.fields) {
    const auto pos = fields.find(field);
    if (pos == fields.end()) continue;
    it->second.bytes -= StringBytes(pos->first) + StringBytes(pos->second);
    fields.erase(pos);
    ++removed;
  }
  TrackInsert(it->second, op.key);
  if (fields.empty()) {
    TombstoneEntry(it->second, op.key, seq);
  }
  return core::RespValue::Integer(removed);
}

core::Result<core::RespValue> SingleShardStore::ApplyExpire(const core::ops::Expire& op) {
  auto it = entries_.find(op.key);
  if (it == entries_.end()) return core::RespValue::Integer(0);
  // A tombstone is logically absent (mirrors FindLiveEntry/Probe). EXPIRE on a
  // deleted-but-not-yet-GC'd key returns 0 and must not resurrect or mutate the
  // tombstone — the tombstone is reclaimed by GcTombstones, not by a TTL op.
  if (it->second.tombstoned) return core::RespValue::Integer(0);
  // An expired entry stays, read as absent, until cold drains it.
  if (ExpiredForApply(it->second)) return core::RespValue::Integer(0);
  TrackRemove(it->second, op.key);
  it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
  TrackInsert(it->second, op.key);
  return core::RespValue::Integer(1);
}

core::Result<core::RespValue> SingleShardStore::ApplyPersist(const core::ops::Persist& op) {
  auto it = entries_.find(op.key);
  if (it == entries_.end()) return core::RespValue::Integer(0);
  // A tombstone is logically absent: PERSIST returns 0 and leaves it untouched.
  if (it->second.tombstoned) return core::RespValue::Integer(0);
  if (ExpiredForApply(it->second)) return core::RespValue::Integer(0);
  if (it->second.abs_ttl_ms == 0) {
    return core::RespValue::Integer(0);
  }
  TrackRemove(it->second, op.key);
  it->second.abs_ttl_ms = 0;
  TrackInsert(it->second, op.key);
  return core::RespValue::Integer(1);
}

// --- Maintenance ---

void SingleShardStore::SetAccessTime(core::SteadyTime now) {
  access_now_.store(now.time_since_epoch().count(), std::memory_order_relaxed);
}

void SingleShardStore::NoteAccess(const Entry& entry) const {
  const auto now = access_now_.load(std::memory_order_relaxed);
  const std::atomic_ref<core::SteadyTime::rep> stamp(entry.accessed);
  // Once a tick, so a hot key's line is not written on every read.
  if (stamp.load(std::memory_order_relaxed) < now) stamp.store(now, std::memory_order_relaxed);
}

void SingleShardStore::EvictDue(Entry& entry, core::SequenceId horizon, int64_t now_ms,
                                EvictExpiredReport& report) {
  // Undrained, even if expired: a miss would read an older value from
  // buffer or cold.
  if (entry.latest_seq > horizon) {
    Park(entry);
    ++report.parked;
    return;
  }
  const bool ttl_expired = TtlPassed(entry.abs_ttl_ms, now_ms);
  // TTL is a deletion, deadline a tier transition; TTL wins when both
  // fire, since the key is gone either way.
  Evict(entries_.find(*entry.key), /*leave_stub=*/!ttl_expired);
  if (ttl_expired) {
    ++expired_count_;
    ++report.by_ttl;
  } else {
    ++eviction_count_;
    ++report.by_deadline;
  }
}

bool SingleShardStore::ReleaseParked(core::SteadyTime now, core::SequenceId horizon,
                                     HoldBudget& budget, EvictExpiredReport& report) {
  const int64_t now_ms = WallMs(config_.wall_clock);
  while (Entry* entry = parked_.Top()) {
    if (entry->latest_seq > horizon) break;
    if (!budget.Take()) return false;
    Unpark(*entry);
    if (!TtlPassed(entry->abs_ttl_ms, now_ms) && entry->Deadline() > now) {
      // Read since it parked.
      LruLink(*entry, std::max(entry->linked_at, AccessedAt(*entry)));
      IndexTtl(*entry);
      continue;
    }
    EvictDue(*entry, horizon, now_ms, report);
  }
  UpdateBackpressure();
  return true;
}

bool SingleShardStore::ExpireTtl(core::SteadyTime /*now*/, core::SequenceId horizon,
                                 HoldBudget& budget, EvictExpiredReport& report) {
  const int64_t now_ms = WallMs(config_.wall_clock);
  while (!ttl_index_.empty()) {
    const auto [bucket, keys] = *ttl_index_.begin();
    if (bucket * kTtlBucketMs > now_ms) break;
    Entry* entry = keys.head;
    while (entry != nullptr) {
      if (!budget.Take()) return false;
      // Taken before entry leaves the bucket.
      Entry* const next = entry->ttl_next;
      if (TtlPassed(entry->abs_ttl_ms, now_ms)) EvictDue(*entry, horizon, now_ms, report);
      entry = next;
    }
    // What the bucket still holds is not yet due: it is now's.
    if (!ttl_index_.empty() && ttl_index_.begin()->first == bucket) break;
  }
  UpdateBackpressure();
  return true;
}

bool SingleShardStore::EvictPastDeadline(core::SteadyTime now, core::SequenceId horizon,
                                         HoldBudget& budget, EvictExpiredReport& report) {
  const int64_t now_ms = WallMs(config_.wall_clock);
  for (LruList& list : lru_lists_) {
    // In linked_at order, so the first not due by its link ends the
    // list: a read only moves a deadline later.
    while (list.oldest != nullptr && list.oldest->linked_at + list.eviction <= now) {
      if (!budget.Take()) return false;
      Entry& entry = *list.oldest;
      const core::SteadyTime accessed = AccessedAt(entry);
      if (accessed > entry.linked_at && accessed + list.eviction > now) {
        LruUnlink(entry);
        LruLink(entry, accessed);
        continue;
      }
      EvictDue(entry, horizon, now_ms, report);
    }
  }
  UpdateBackpressure();
  return true;
}

SingleShardStore::EvictExpiredReport SingleShardStore::EvictExpired(core::SteadyTime now,
                                                                    core::SequenceId horizon) {
  EvictExpiredReport report;
  HoldBudget budget = HoldBudget::Unbounded();
  ReleaseParked(now, horizon, budget, report);
  ExpireTtl(now, horizon, budget, report);
  EvictPastDeadline(now, horizon, budget, report);
  return report;
}

size_t SingleShardStore::EvictLru(size_t target_bytes, core::SequenceId horizon) {
  HoldBudget budget = HoldBudget::Unbounded();
  size_t evicted = 0;
  EvictLru(target_bytes, std::string_view{}, horizon, budget, evicted);
  return evicted;
}

bool SingleShardStore::EvictLru(size_t target_bytes, core::SequenceId horizon, HoldBudget& budget,
                                size_t& evicted) {
  return EvictLru(target_bytes, std::string_view{}, horizon, budget, evicted);
}

bool SingleShardStore::EvictLru(size_t target_bytes, std::string_view protect_key,
                                core::SequenceId horizon, HoldBudget& budget, size_t& evicted) {
  if (UsedBytes() <= target_bytes || (lru_dry_ && horizon <= lru_dry_horizon_)) {
    UpdateBackpressure();
    return true;
  }
  lru_dry_ = false;
  // Off its list for the walk, and back at the warm end after.
  Entry* guarded = protect_key.empty() ? nullptr : FindEntry(protect_key);
  if (guarded != nullptr && !Linked(*guarded)) guarded = nullptr;
  if (guarded != nullptr) LruUnlink(*guarded);
  const bool skipped_evictable = guarded != nullptr && guarded->latest_seq <= horizon;
  for (LruList& list : lru_lists_) {
    if (list.cursor_set && list.cursor_horizon != horizon) list.cursor_set = false;
  }
  bool done = false;
  for (;;) {
    if (UsedBytes() <= target_bytes) {
      done = true;
      break;
    }
    // The least recently linked of the lists' next entries.
    LruList* from = nullptr;
    for (LruList& list : lru_lists_) {
      Entry* next = list.cursor_set ? list.cursor : list.oldest;
      if (next != nullptr && (from == nullptr || next->linked_at < Front(*from)->linked_at)) {
        from = &list;
      }
    }
    if (from == nullptr) {
      // Nothing left to evict: every live entry is undrained until
      // cold passes this horizon.
      if (!skipped_evictable) {
        lru_dry_ = true;
        lru_dry_horizon_ = horizon;
      }
      done = true;
      break;
    }
    if (!budget.Take()) break;
    Entry& entry = *Front(*from);
    ++lru_visits_;
    const core::SteadyTime accessed = AccessedAt(entry);
    if (accessed > entry.linked_at) {
      LruUnlink(entry);
      LruLink(entry, accessed);
    } else if (entry.latest_seq <= horizon) {
      const bool expired = IsExpiredByTtl(entry, config_.wall_clock);
      Evict(entries_.find(*entry.key), /*leave_stub=*/!expired);
      eviction_count_++;
      ++evicted;
    } else {
      // Written last, so undrained entries sit near the warm end and
      // the walk passes each once a horizon.
      from->cursor_set = true;
      from->cursor = entry.lru_newer;
      from->cursor_horizon = horizon;
    }
  }
  if (guarded != nullptr) LruLink(*guarded, guarded->linked_at);
  UpdateBackpressure();
  return done;
}

bool SingleShardStore::EnsureCapacityFor(std::string_view protect_key, core::SequenceId horizon,
                                         HoldBudget& budget) {
  // Called post-write: the just-written entry (protect_key) is already counted
  // in used bytes and must survive, so make room by evicting OTHER LRU keys
  // toward the budget. Suppressed during replay: evicting mid-replay would
  // make the rebuilt hot view depend on memory timing, breaking deterministic
  // queue replay (invariant 4). The eviction worker reconverges after replay.
  if (replay_mode_ || !governor_.Enabled()) return true;
  if (!governor_.WouldExceed(UsedBytes(), 0)) {
    UpdateBackpressure();
    return true;
  }
  // Down to 95% of the budget, so the next writes do not trigger again.
  const size_t target = governor_.Target(0);
  size_t evicted = 0;
  EvictLru(target - (target / 20), protect_key, horizon, budget, evicted);
  // The protected entry may never fit (a single value larger than the whole
  // budget). It is already durable in the queue, so the caller surfaces
  // kResourceExhausted as an admission signal — not a lost write
  // (invariant 2). Other live keys leave once cold drains them, so only
  // tombstones, stubs, the TTL index and this entry count.
  uint64_t floor = UsedBytes() - live_bytes_;
  if (const Entry* entry = FindEntry(protect_key); entry != nullptr && !entry->tombstoned) {
    floor += Footprint(*entry, protect_key);
  }
  return !governor_.WouldExceed(floor, 0);
}

bool SingleShardStore::MakeRoom(std::string_view key, size_t entry_bytes,
                                core::SequenceId horizon) {
  const size_t bytes = entry_bytes + sizeof(std::string) + key.size();
  if (!governor_.WouldExceed(UsedBytes(), bytes)) return true;
  const size_t target = governor_.Target(bytes);
  const size_t slack = governor_.max_bytes() / 20;
  HoldBudget budget = HoldBudget::Capped();
  size_t evicted = 0;
  EvictLru(target > slack ? target - slack : 0, std::string_view{}, horizon, budget, evicted);
  return !governor_.WouldExceed(UsedBytes(), bytes);
}

core::MemoryStats SingleShardStore::Stats() const {
  return {.used_bytes = UsedBytes(),
          .key_count = key_count_,
          .eviction_count = eviction_count_,
          .expired_count = expired_count_,
          .max_bytes = governor_.max_bytes(),
          .stub_entries = stubs_.size(),
          .stub_bytes = stubs_.bytes(),
          .stub_drops = stubs_.drops(),
          .negative_entries = negative_entries_,
          .load_discards = load_discards_,
          .unevictable_bytes = lru_dry_ ? live_bytes_ : parked_bytes_,
          .ttl_index_bytes = ttl_index_.size() * kTtlBucketBytes,
          .backpressured = backpressured_};
}

void SingleShardStore::Wipe(core::SequenceId seq) {
  ABYSS_DCHECK(seq >= core::kFirstSeq, "a Flush at seq 0, which names no entry");
  if (graveyard_ != nullptr) {
    graveyard_->entries.push_back(std::move(entries_));
    graveyard_->stubs.push_back(stubs_.Release());
    graveyard_->ttl_indexes.push_back(std::move(ttl_index_));
  }
  entries_.clear();
  ttl_index_.clear();
  lru_lists_.clear();
  tombstones_.Clear();
  parked_.Clear();
  parked_bytes_ = 0;
  lru_dry_ = false;
  stubs_.Clear();
  loading_.clear();
  negatives_.clear();
  negative_entries_ = 0;
  entry_bytes_ = 0;
  live_bytes_ = 0;
  key_count_ = 0;
  backpressured_ = false;
  flush_seq_ = std::max(flush_seq_, seq);
}

bool SingleShardStore::DropStub(std::string_view key) { return stubs_.Erase(key); }

std::optional<LoadToken> SingleShardStore::BeginLoad(std::string_view key) {
  if (entries_.contains(key) || loading_.contains(key)) return std::nullopt;
  const LoadToken token{.id = ++next_load_id_};
  loading_.emplace(std::string(key), token);
  return token;
}

LoadStart SingleShardStore::StartLoad(std::string_view key, core::SequenceId horizon) {
  using Status = LoadStart::Status;
  // Residency first: a key written after a Flush is resident, not
  // absent by the floor.
  if (HasEntry(key)) return {.status = Status::kResident};
  if (KnownAbsentAfterFlush(horizon)) return {.status = Status::kFlushed};
  if (const auto token = BeginLoad(key); token.has_value()) {
    return {.status = Status::kStarted, .token = *token};
  }
  return {.status = Status::kPending};
}

bool SingleShardStore::CompleteLoad(std::string_view key, LoadToken token, LoadResult&& result,
                                    core::EvictionTTL eviction, core::SequenceId horizon) {
  return InstallLoad(key, token, std::move(result), eviction, horizon, /*in_batch=*/false);
}

bool SingleShardStore::InstallLoad(std::string_view key, LoadToken token, LoadResult&& result,
                                   core::EvictionTTL eviction, core::SequenceId horizon,
                                   bool in_batch) {
  const auto pending = loading_.find(key);
  if (pending == loading_.end() || pending->second != token) {
    ++load_discards_;
    return false;
  }
  loading_.erase(pending);
  if (entries_.contains(key)) {
    ++load_discards_;
    return false;
  }
  LoadResult installed = std::move(result);
  if (const auto* exists = std::get_if<LoadedExists>(&installed)) {
    stubs_.Put(key, Stub{.type = exists->type, .abs_ttl_ms = exists->abs_ttl_ms},
               /*trim=*/!in_batch);
    return stubs_.Find(key) != nullptr;
  }
  stubs_.Erase(key);
  auto* full = std::get_if<LoadedFull>(&installed);
  if (full == nullptr) {
    InsertTombstone(key, 0);
    ++negative_entries_;
    negatives_.emplace_back(key);
    if (!in_batch) {
      HoldBudget budget = HoldBudget::Unbounded();
      TrimNegatives(budget);
    }
    return true;
  }

  const auto it = EmplaceEntry(key);
  Entry& entry = it->second;
  entry.key = &it->first;
  entry.type = full->type();
  entry.value = std::move(full->value);
  entry.bytes = full->bytes;
  entry.abs_ttl_ms = full->abs_ttl_ms;
  entry.eviction = eviction;
  // Loaded state is already drained.
  entry.latest_seq = 0;
  LruLink(entry, config_.steady_clock());
  IndexTtl(entry);
  NoteEvictable(entry.latest_seq);
  key_count_++;
  TrackInsert(entry, key);
  if (!in_batch) {
    HoldBudget budget = HoldBudget::Capped();
    EnsureCapacityFor(key, horizon, budget);
  }
  return true;
}

size_t SingleShardStore::CompleteLoads(std::span<LoadCompletion> loads,
                                       const core::EvictionPolicy& policy) {
  size_t installed = 0;
  for (auto& load : loads) {
    if (InstallLoad(load.key, load.token, std::move(load.result), policy.Resolve(load.key),
                    kAllDrained, /*in_batch=*/true)) {
      ++installed;
    }
  }
  return installed;
}

void SingleShardStore::AbortLoad(std::string_view key, LoadToken token) {
  const auto pending = loading_.find(key);
  if (pending != loading_.end() && pending->second == token) loading_.erase(pending);
}

bool SingleShardStore::LoadPending(std::string_view key) const {
  return !loading_.empty() && loading_.contains(key);
}

KeyView SingleShardStore::View(std::string_view key, core::SequenceId horizon,
                               uint64_t now_ms) const {
  const auto expired = [now_ms](int64_t abs_ttl_ms) {
    return abs_ttl_ms != 0 && std::cmp_greater_equal(now_ms, abs_ttl_ms);
  };
  using Presence = KeyView::Presence;
  if (const Entry* entry = FindEntry(key); entry != nullptr) {
    KeyView view{
        .type = entry->type, .abs_ttl_ms = entry->abs_ttl_ms, .latest_seq = entry->latest_seq};
    if (entry->tombstoned) {
      view.presence = Presence::kTombstoned;
      return view;
    }
    view.presence = expired(entry->abs_ttl_ms) ? Presence::kExpired : Presence::kLive;
    view.value = &entry->value;
    // A decision that changes nothing still read the key.
    if (view.presence == Presence::kLive) NoteAccess(*entry);
    return view;
  }
  // The floor first: a stub older than the Flush is dead.
  if (KnownAbsentAfterFlush(horizon)) {
    return KeyView{
        .presence = Presence::kTombstoned, .flush_floor = true, .latest_seq = flush_seq_};
  }
  if (LoadPending(key)) return KeyView{};
  if (const Stub* stub = stubs_.Find(key); stub != nullptr) {
    return KeyView{.presence = expired(stub->abs_ttl_ms) ? Presence::kExpired : Presence::kStub,
                   .type = stub->type,
                   .abs_ttl_ms = stub->abs_ttl_ms,
                   .latest_seq = stub->latest_seq};
  }
  return KeyView{};
}

SingleShardStore::ReadAnswer SingleShardStore::Read(const core::ops::ReadOp& op,
                                                    core::SequenceId horizon) const {
  if (const auto* exists = std::get_if<core::ops::Exists>(&op)) {
    ABYSS_DCHECK(exists->keys.size() == 1, "an existence read of more than one key");
  }
  const std::string_view key = core::ops::PrimaryKey(op);
  const int64_t now_ms = WallMs(config_.wall_clock);
  const Entry* entry = FindEntry(key);
  if (entry == nullptr) {
    if (KnownAbsentAfterFlush(horizon)) {
      return {.result = EmptyReadResponse(op), .fence = flush_seq_};
    }
    // A stub is current until a write or a Flush drops it. It keeps no
    // access: stubs are dropped oldest written first.
    if (const Stub* stub = stubs_.Find(key); stub != nullptr && IsMetaRead(op)) {
      if (TtlPassed(stub->abs_ttl_ms, now_ms)) {
        return {.result = EmptyReadResponse(op), .fence = stub->latest_seq};
      }
      return {.result = AnswerRead(op, stub->type, nullptr, stub->abs_ttl_ms, now_ms),
              .fence = stub->latest_seq};
    }
    return {.result = std::unexpected(NotFound())};
  }
  // A resident entry is the key's latest state, so one deleted or past
  // its TTL is absent, whatever older value buffer or cold still hold.
  if (entry->tombstoned || TtlPassed(entry->abs_ttl_ms, now_ms)) {
    return {.result = EmptyReadResponse(op), .fence = entry->latest_seq};
  }
  // Meta reads do not count as use, as in Redis.
  if (!IsMetaRead(op)) NoteAccess(*entry);
  return {.result = AnswerRead(op, entry->type, &entry->value, entry->abs_ttl_ms, now_ms),
          .fence = entry->latest_seq};
}

void SingleShardStore::RaiseAppendedAt(core::WallTime at) {
  last_appended_at_ = std::max(last_appended_at_, at);
}

bool SingleShardStore::OverBackpressure() const {
  return governor_.Enabled() &&
         static_cast<double>(UsedBytes()) >
             static_cast<double>(governor_.max_bytes()) * config_.backpressure_ratio;
}

// --- Internal helpers ---

bool SingleShardStore::ExpiredForApply(const Entry& entry) const {
  if (applying_effects_ || entry.abs_ttl_ms == 0) return false;
  return WallMs(config_.wall_clock) >= entry.abs_ttl_ms;
}

bool SingleShardStore::ExpiryIsLogged(const core::Effect& effect, std::string_view key,
                                      int64_t at_ms) const {
  if (effect.replaces_state && !effect.reply_old_value) return true;
  const Entry* entry = FindEntry(key);
  return entry == nullptr || entry->tombstoned || entry->abs_ttl_ms == 0 ||
         at_ms < entry->abs_ttl_ms;
}

Entry* SingleShardStore::FindEntry(std::string_view key) {
  const auto it = entries_.find(key);
  return it == entries_.end() ? nullptr : &it->second;
}

const Entry* SingleShardStore::FindEntry(std::string_view key) const {
  const auto it = entries_.find(key);
  return it == entries_.end() ? nullptr : &it->second;
}

const Entry* SingleShardStore::FindLiveEntry(std::string_view key) const {
  const auto* entry = FindEntry(key);
  if (entry == nullptr) return nullptr;
  if (entry->tombstoned) return nullptr;
  if (IsExpiredByTtl(*entry, config_.wall_clock)) return nullptr;
  return entry;
}

EntryMap::iterator SingleShardStore::EmplaceEntry(std::string_view key) {
  const auto buckets = static_cast<double>(entries_.bucket_count());
  const bool rehashes =
      static_cast<double>(entries_.size() + 1) > entries_.max_load_factor() * buckets;
  if (!rehashes || !config_.on_rehash) return entries_.try_emplace(std::string(key)).first;
  const auto start = std::chrono::steady_clock::now();
  const auto it = entries_.try_emplace(std::string(key)).first;
  config_.on_rehash(
      std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
  return it;
}

Entry& SingleShardStore::GetOrCreateEntry(std::string_view key, Entry::Type type,
                                          core::EvictionTTL eviction) {
  auto it = entries_.find(key);
  const bool inserted = it == entries_.end();
  if (inserted) it = EmplaceEntry(key);
  Entry& entry = it->second;
  entry.key = &it->first;
  // A tombstone is reborn as a fresh live key. Untrack its footprint first so
  // the slot is in the same state as a freshly inserted entry (the caller's
  // TrackInsert then accounts for the new value).
  if (!inserted && entry.tombstoned) {
    ForgetNegative(entry);
    Detach(entry);
    TrackRemove(entry, key);
  }
  if (inserted || entry.tombstoned) {
    entry.type = type;
    entry.tombstoned = false;
    entry.latest_seq = 0;
    entry.abs_ttl_ms = 0;
    entry.eviction = eviction;
    switch (type) {
      case Entry::Type::kString:
        entry.value = std::string{};
        break;
      case Entry::Type::kSet:
        entry.value = SetValue{};
        break;
      case Entry::Type::kHash:
        entry.value = HashValue{};
        break;
      case Entry::Type::kZset:
        entry.value = ZsetValue{};
        break;
    }
    entry.bytes = entry.ApproximateBytes();
    key_count_++;
    // Track the empty-entry baseline so the collection apply paths' balanced
    // TrackRemove/mutate/TrackInsert pattern has a matching prior insert. The
    // string-create path (ApplyStringSet) does its own TrackInsert after
    // setting the value, so it does not go through this baseline. The
    // write's MarkWritten links it.
    TrackInsert(entry, key);
  }
  return entry;
}

void SingleShardStore::RemoveEntry(std::string_view key) {
  auto it = entries_.find(key);
  if (it == entries_.end()) return;
  ForgetNegative(it->second);
  Detach(it->second);
  TrackRemove(it->second, key);
  key_count_--;
  Bury(it->second.value);
  Erase(it);
}

void SingleShardStore::TombstoneEntry(Entry& entry, std::string_view key, core::SequenceId seq) {
  if (entry.tombstoned) {
    ForgetNegative(entry);
    entry.latest_seq = seq;
    if (entry.heap_slot != Entry::kNoSlot) {
      tombstones_.Update(entry);
    } else if (seq != 0) {
      tombstones_.Push(entry);
    }
    return;
  }
  Detach(entry);
  TrackRemove(entry, key);
  Bury(entry.value);
  entry.value = std::string{};
  entry.bytes = entry.ApproximateBytes();
  entry.abs_ttl_ms = 0;
  entry.tombstoned = true;
  entry.latest_seq = seq;
  key_count_--;
  if (seq != 0) tombstones_.Push(entry);
  TrackInsert(entry, key);
}

void SingleShardStore::InsertTombstone(std::string_view key, core::SequenceId seq) {
  const auto it = EmplaceEntry(key);
  Entry& entry = it->second;
  entry.key = &it->first;
  entry.type = Entry::Type::kString;
  entry.value = std::string{};
  entry.bytes = entry.ApproximateBytes();
  entry.tombstoned = true;
  entry.latest_seq = seq;
  // Seq 0 is a load's absent answer: the negative cache reclaims it.
  if (seq != 0) tombstones_.Push(entry);
  TrackInsert(entry, key);
}

core::HotKeyPresence SingleShardStore::Probe(std::string_view key) const {
  const auto* entry = FindEntry(key);
  if (entry == nullptr) return core::HotKeyPresence::kAbsent;
  if (entry->tombstoned) return core::HotKeyPresence::kTombstoned;
  if (IsExpiredByTtl(*entry, config_.wall_clock)) return core::HotKeyPresence::kAbsent;
  return core::HotKeyPresence::kPresent;
}

bool SingleShardStore::GcTombstones(core::SequenceId horizon, HoldBudget& budget,
                                    size_t& reclaimed) {
  if (!TrimNegatives(budget)) return false;
  // Rewritten tombstones left the heap or moved in it when rewritten.
  while (Entry* entry = tombstones_.Top()) {
    if (entry->latest_seq > horizon) break;
    if (!budget.Take()) return false;
    tombstones_.Erase(*entry);
    const auto it = entries_.find(*entry->key);
    TrackRemove(it->second, it->first);
    Erase(it);
    ++reclaimed;
  }
  return true;
}

size_t SingleShardStore::GcTombstones(core::SequenceId horizon) {
  HoldBudget budget = HoldBudget::Unbounded();
  size_t reclaimed = 0;
  GcTombstones(horizon, budget, reclaimed);
  return reclaimed;
}

void SingleShardStore::TrackInsert(const Entry& entry, std::string_view key) {
  const auto bytes = Footprint(entry, key);
  entry_bytes_ += bytes;
  if (!entry.tombstoned) live_bytes_ += bytes;
  if (Parked(entry)) parked_bytes_ += bytes;
}

void SingleShardStore::TrackRemove(const Entry& entry, std::string_view key) {
  const auto bytes = Footprint(entry, key);
  entry_bytes_ = (entry_bytes_ >= bytes) ? entry_bytes_ - bytes : 0;
  if (!entry.tombstoned) live_bytes_ = (live_bytes_ >= bytes) ? live_bytes_ - bytes : 0;
  if (Parked(entry)) parked_bytes_ = (parked_bytes_ >= bytes) ? parked_bytes_ - bytes : 0;
}

void SingleShardStore::MarkWritten(std::string_view key, core::SequenceId seq) {
  ABYSS_DCHECK(seq >= core::kFirstSeq, "a write applied at seq 0, which names no entry");
  if (Entry* entry = FindEntry(key); entry != nullptr) {
    ForgetNegative(*entry);
    entry->latest_seq = seq;
    if (entry->tombstoned) {
      if (entry->heap_slot != Entry::kNoSlot) {
        tombstones_.Update(*entry);
      } else {
        tombstones_.Push(*entry);
      }
    } else {
      if (Parked(*entry)) Unpark(*entry);
      if (Linked(*entry)) LruUnlink(*entry);
      LruLink(*entry, config_.steady_clock());
      IndexTtl(*entry);
      NoteEvictable(seq);
    }
  }
  if (!loading_.empty()) {
    if (const auto pending = loading_.find(key); pending != loading_.end()) loading_.erase(pending);
  }
  stubs_.Erase(key);
}

void SingleShardStore::Evict(EntryMap::iterator it, bool leave_stub) {
  if (leave_stub) {
    stubs_.Put(it->first, Stub{.type = it->second.type,
                               .abs_ttl_ms = it->second.abs_ttl_ms,
                               .latest_seq = it->second.latest_seq});
  }
  Detach(it->second);
  TrackRemove(it->second, it->first);
  key_count_--;
  Bury(it->second.value);
  Erase(it);
}

void SingleShardStore::Erase(EntryMap::iterator it) {
  if (graveyard_ == nullptr) {
    entries_.erase(it);
  } else {
    graveyard_->nodes.push_back(entries_.extract(it));
  }
}

void SingleShardStore::Detach(Entry& entry) {
  if (entry.heap_slot != Entry::kNoSlot) {
    if (entry.tombstoned) {
      tombstones_.Erase(entry);
    } else {
      Unpark(entry);
    }
  }
  if (Linked(entry)) LruUnlink(entry);
  UnindexTtl(entry);
}

void SingleShardStore::Bury(Value& value) {
  if (graveyard_ != nullptr) graveyard_->values.push_back(std::move(value));
}

SingleShardStore::LruList& SingleShardStore::ListFor(core::EvictionTTL eviction) {
  for (LruList& list : lru_lists_) {
    if (list.eviction == eviction) return list;
  }
  if (lru_lists_.size() > std::numeric_limits<uint16_t>::max()) {
    core::Fatal("more eviction classes than a hot shard can list");
  }
  return lru_lists_.emplace_back(LruList{.eviction = eviction});
}

void SingleShardStore::LruLink(Entry& entry, core::SteadyTime at) {
  LruList& list = ListFor(entry.eviction);
  entry.lru_list = static_cast<uint16_t>(&list - &lru_lists_.front());
  entry.linked_at = list.newest != nullptr ? std::max(at, list.newest->linked_at) : at;
  entry.lru_older = list.newest;
  entry.lru_newer = nullptr;
  if (list.newest != nullptr) {
    list.newest->lru_newer = &entry;
  } else {
    list.oldest = &entry;
  }
  list.newest = &entry;
  if (list.cursor_set && list.cursor == nullptr) list.cursor = &entry;
}

void SingleShardStore::LruUnlink(Entry& entry) {
  LruList& list = lru_lists_[entry.lru_list];
  if (list.cursor_set && list.cursor == &entry) list.cursor = entry.lru_newer;
  if (entry.lru_newer != nullptr) {
    entry.lru_newer->lru_older = entry.lru_older;
  } else {
    list.newest = entry.lru_older;
  }
  if (entry.lru_older != nullptr) {
    entry.lru_older->lru_newer = entry.lru_newer;
  } else {
    list.oldest = entry.lru_newer;
  }
  entry.lru_newer = nullptr;
  entry.lru_older = nullptr;
}

bool SingleShardStore::Linked(const Entry& entry) const {
  if (entry.lru_newer != nullptr || entry.lru_older != nullptr) return true;
  return entry.lru_list < lru_lists_.size() && lru_lists_[entry.lru_list].oldest == &entry;
}

void SingleShardStore::IndexTtl(Entry& entry) {
  const bool indexed = !entry.tombstoned && !Parked(entry) && entry.abs_ttl_ms != 0;
  const int64_t bucket = indexed ? entry.abs_ttl_ms / kTtlBucketMs : Entry::kNoBucket;
  if (entry.ttl_bucket == bucket) return;
  UnindexTtl(entry);
  if (!indexed) return;
  TtlBucket& keys = ttl_index_[bucket];
  entry.ttl_bucket = bucket;
  entry.ttl_prev = keys.tail;
  entry.ttl_next = nullptr;
  if (keys.tail != nullptr) {
    keys.tail->ttl_next = &entry;
  } else {
    keys.head = &entry;
  }
  keys.tail = &entry;
  ++keys.size;
}

void SingleShardStore::UnindexTtl(Entry& entry) {
  if (entry.ttl_bucket == Entry::kNoBucket) return;
  const auto it = ttl_index_.find(entry.ttl_bucket);
  TtlBucket& keys = it->second;
  if (entry.ttl_prev != nullptr) {
    entry.ttl_prev->ttl_next = entry.ttl_next;
  } else {
    keys.head = entry.ttl_next;
  }
  if (entry.ttl_next != nullptr) {
    entry.ttl_next->ttl_prev = entry.ttl_prev;
  } else {
    keys.tail = entry.ttl_prev;
  }
  if (--keys.size == 0) ttl_index_.erase(it);
  entry.ttl_bucket = Entry::kNoBucket;
  entry.ttl_prev = nullptr;
  entry.ttl_next = nullptr;
}

void SingleShardStore::Park(Entry& entry) {
  LruUnlink(entry);
  UnindexTtl(entry);
  parked_.Push(entry);
  parked_bytes_ += Footprint(entry, *entry.key);
}

void SingleShardStore::Unpark(Entry& entry) {
  const auto bytes = Footprint(entry, *entry.key);
  parked_bytes_ = (parked_bytes_ >= bytes) ? parked_bytes_ - bytes : 0;
  parked_.Erase(entry);
}

void SingleShardStore::ForgetNegative(const Entry& entry) {
  if (entry.tombstoned && entry.latest_seq == 0) --negative_entries_;
}

bool SingleShardStore::TrimNegatives(HoldBudget& budget) {
  while (negatives_.size() > config_.negative_max_entries) {
    if (!budget.Take()) return false;
    const auto it = entries_.find(negatives_.front());
    negatives_.pop_front();
    // Keys rewritten since are skipped.
    if (it == entries_.end() || !it->second.tombstoned || it->second.latest_seq != 0) continue;
    --negative_entries_;
    TrackRemove(it->second, it->first);
    Erase(it);
  }
  return true;
}

void SingleShardStore::NoteEvictable(core::SequenceId latest_seq) {
  if (lru_dry_ && latest_seq <= lru_dry_horizon_) lru_dry_ = false;
}

void SingleShardStore::UpdateBackpressure() {
  backpressured_ = governor_.Enabled() &&
                   static_cast<double>(UsedBytes()) >
                       static_cast<double>(governor_.max_bytes()) * config_.backpressure_ratio;
}

}  // namespace abyss::hot
