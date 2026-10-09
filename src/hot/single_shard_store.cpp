#include "abyss/hot/single_shard_store.h"

#include <algorithm>
#include <charconv>
#include <chrono>
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
constexpr size_t kScoreBucketBytes = sizeof(double) + sizeof(std::set<std::string>);

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

// The single key a read op targets, or nullopt for the multi-key Exists probe
// (which resolves presence per key rather than producing one shaped reply).
std::optional<std::string_view> SingleKeyOf(const core::ops::ReadOp& op) {
  return std::visit(
      [](const auto& o) -> std::optional<std::string_view> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return std::nullopt;
        } else {
          return o.key;
        }
      },
      op);
}

// The empty/nil reply a read op yields for a missing key, shaped per op — as a
// success, so a tombstone hit answers authoritatively rather than falling through.
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
                             std::is_same_v<T, core::ops::HashLen>) {
          return core::RespValue::Integer(0);
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

}  // namespace

size_t Entry::ApproximateBytes() const { return hot::ApproximateBytes(value); }

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
  return set != nullptr && set->members.contains(std::string(member));
}

std::optional<double> KeyView::zset_score(std::string_view member) const {
  const auto* zset = value != nullptr ? std::get_if<ZsetValue>(value) : nullptr;
  if (zset == nullptr) return std::nullopt;
  const auto it = zset->member_scores.find(std::string(member));
  if (it == zset->member_scores.end()) return std::nullopt;
  return it->second;
}

bool KeyView::hash_has(std::string_view field) const { return hash_get(field).has_value(); }

std::optional<std::string_view> KeyView::hash_get(std::string_view field) const {
  const auto* hash = value != nullptr ? std::get_if<HashValue>(value) : nullptr;
  if (hash == nullptr) return std::nullopt;
  const auto it = hash->fields.find(std::string(field));
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

void StubCache::Put(std::string_view key, const Stub& stub) {
  if (max_entries_ == 0) return;
  if (const auto it = index_.find(key); it != index_.end()) {
    it->second->stub = stub;
    order_.splice(order_.begin(), order_, it->second);
    return;
  }
  order_.push_front(Node{.key = std::string(key), .stub = stub});
  index_.emplace(order_.front().key, order_.begin());
  bytes_ += kStubBytes + key.size();
  if (index_.size() > max_entries_) {
    EraseNode(std::prev(order_.end()));
    ++drops_;
  }
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
      stubs_(config_.stub_max_entries) {}

// --- Read operations (const) ---

core::Result<core::RespValue> SingleShardStore::Exec(const core::ops::ReadOp& op) const {
  // A tombstone is authoritative: the key was deleted.
  if (const auto key = SingleKeyOf(op);
      key.has_value() && Probe(*key) == core::HotKeyPresence::kTombstoned) {
    return EmptyReadResponse(op);
  }
  return std::visit(
      [this](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringGet>) {
          return ExecStringGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetIsMember>) {
          return ExecSetIsMember(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetMembers>) {
          return ExecSetMembers(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetCard>) {
          return ExecSetCard(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetScore>) {
          return ExecZsetScore(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetCard>) {
          return ExecZsetCard(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetRange>) {
          return ExecZsetRange(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashGet>) {
          return ExecHashGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashGetAll>) {
          return ExecHashGetAll(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashMultiGet>) {
          return ExecHashMultiGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashFieldExists>) {
          return ExecHashFieldExists(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashKeys>) {
          return ExecHashKeys(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashVals>) {
          return ExecHashVals(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashLen>) {
          return ExecHashLen(o);
        } else if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return ExecExists(o);
        } else {
          return std::unexpected(
              core::Error(core::ErrorCode::kInternal, "unsupported read operation"));
        }
      },
      op);
}

core::Result<core::RespValue> SingleShardStore::ExecStringGet(
    const core::ops::StringGet& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kString);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  return core::RespValue::BulkString(std::get<std::string>((*result)->value));
}

core::Result<core::RespValue> SingleShardStore::ExecSetIsMember(
    const core::ops::SetIsMember& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  return core::RespValue::Integer(members.contains(std::string(op.member)) ? 1 : 0);
}

core::Result<core::RespValue> SingleShardStore::ExecSetMembers(
    const core::ops::SetMembers& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  std::vector<core::RespValue> elements;
  elements.reserve(members.size());
  for (const auto& m : members) {
    elements.push_back(core::RespValue::BulkString(m));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecSetCard(const core::ops::SetCard& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  return core::RespValue::Integer(static_cast<int64_t>(members.size()));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetScore(
    const core::ops::ZsetScore& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
  auto it = zset.member_scores.find(std::string(op.member));
  if (it == zset.member_scores.end()) {
    return core::RespValue::Null();
  }
  return core::RespValue::BulkString(core::FormatRespDouble(it->second));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetCard(const core::ops::ZsetCard& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
  return core::RespValue::Integer(static_cast<int64_t>(zset.member_scores.size()));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetRange(
    const core::ops::ZsetRange& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
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

core::Result<core::RespValue> SingleShardStore::ExecHashGet(const core::ops::HashGet& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  auto it = fields.find(std::string(op.field));
  if (it == fields.end()) {
    return core::RespValue::Null();
  }
  return core::RespValue::BulkString(it->second);
}

core::Result<core::RespValue> SingleShardStore::ExecHashGetAll(
    const core::ops::HashGetAll& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(fields.size() * 2);
  for (const auto& [k, v] : fields) {
    elements.push_back(core::RespValue::BulkString(k));
    elements.push_back(core::RespValue::BulkString(v));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashMultiGet(
    const core::ops::HashMultiGet& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(op.fields.size());
  for (auto field : op.fields) {
    auto it = fields.find(std::string(field));
    if (it == fields.end()) {
      elements.push_back(core::RespValue::Null());
    } else {
      elements.push_back(core::RespValue::BulkString(it->second));
    }
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashFieldExists(
    const core::ops::HashFieldExists& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  return core::RespValue::Integer(fields.contains(std::string(op.field)) ? 1 : 0);
}

core::Result<core::RespValue> SingleShardStore::ExecHashKeys(const core::ops::HashKeys& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(fields.size());
  for (const auto& [k, _] : fields) {
    elements.push_back(core::RespValue::BulkString(k));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashVals(const core::ops::HashVals& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(fields.size());
  for (const auto& [_, v] : fields) {
    elements.push_back(core::RespValue::BulkString(v));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashLen(const core::ops::HashLen& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  return core::RespValue::Integer(static_cast<int64_t>(fields.size()));
}

core::Result<core::RespValue> SingleShardStore::ExecExists(const core::ops::Exists& op) const {
  int64_t count = 0;
  for (auto key : op.keys) {
    if (FindLiveEntry(key) != nullptr) ++count;
  }
  return core::RespValue::Integer(count);
}

// --- Write operations ---

core::Result<core::RespValue> SingleShardStore::Apply(const core::ops::WriteOp& op,
                                                      core::EvictionTTL eviction,
                                                      core::SequenceId seq,
                                                      core::SequenceId horizon) {
  auto result = Mutate(op, eviction, seq, SetMove{});
  // The write has already applied (and is durable in the queue). Make room by
  // evicting OTHER LRU victims down to the budget, protecting the just-written
  // key. If even then the entry cannot fit (a single value larger than the
  // whole budget, no other victims), surface kResourceExhausted as an admission
  // signal — the entry is NOT lost, it stays durable in the queue/cold
  // (invariant 2). Suppressed during replay.
  if (Grows(op) && result.has_value() && !EnsureCapacityFor(core::ops::PrimaryKey(op), horizon)) {
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
  const int64_t at_ms = WallMs(appended_at);
  const FlagScope applying(applying_effects_);
  std::vector<core::RespValue> replies;
  replies.reserve(effects.size());
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
    if (Grows(*op)) EnsureCapacityFor(key, horizon);
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
  auto it = entries_.find(std::string(op.key));
  // A tombstone is treated as absent: fall to the create path so the key is
  // resurrected as a live string (with the key_count increment that implies).
  if (it != entries_.end() && !it->second.tombstoned) {
    if (it->second.type != Entry::Type::kString) {
      TrackRemove(it->second, op.key);
      it->second.type = Entry::Type::kString;
      it->second.value = take_value();
      it->second.bytes = it->second.ApproximateBytes();
      it->second.eviction = eviction;
      it->second.eviction_deadline = config_.steady_clock() + eviction;
      it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
      TrackInsert(it->second, op.key);
      return reply;
    }
    TrackRemove(it->second, op.key);
    auto& value = std::get<std::string>(it->second.value);
    if (move.reply_old_value && !ExpiredForApply(it->second)) {
      reply = core::RespValue::BulkString(std::move(value));
    }
    value = take_value();
    it->second.bytes = it->second.ApproximateBytes();
    it->second.eviction = eviction;
    it->second.eviction_deadline = config_.steady_clock() + eviction;
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
    auto it = entries_.find(std::string(key));
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
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && !it->second.tombstoned && !ExpiredForApply(it->second) &&
      it->second.type != Entry::Type::kSet) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned && ExpiredForApply(it->second)) {
    RemoveEntry(std::string(op.key));
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
  entry.eviction_deadline = config_.steady_clock() + eviction;
  TrackInsert(entry, op.key);
  return core::RespValue::Integer(added);
}

core::Result<core::RespValue> SingleShardStore::ApplySetRem(const core::ops::SetRem& op,
                                                            core::SequenceId seq) {
  auto it = entries_.find(std::string(op.key));
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
    const auto pos = members.find(std::string(member));
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
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && !it->second.tombstoned && !ExpiredForApply(it->second) &&
      it->second.type != Entry::Type::kZset) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned && ExpiredForApply(it->second)) {
    RemoveEntry(std::string(op.key));
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
  entry.eviction_deadline = config_.steady_clock() + eviction;
  TrackInsert(entry, op.key);
  return core::RespValue::Integer(added);
}

core::Result<core::RespValue> SingleShardStore::ApplyZsetRem(const core::ops::ZsetRem& op,
                                                             core::SequenceId seq) {
  auto it = entries_.find(std::string(op.key));
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
    const auto scored = zset.member_scores.find(std::string(member));
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
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && !it->second.tombstoned && !ExpiredForApply(it->second) &&
      it->second.type != Entry::Type::kHash) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned && ExpiredForApply(it->second)) {
    RemoveEntry(std::string(op.key));
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
  entry.eviction_deadline = config_.steady_clock() + eviction;
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
  auto it = entries_.find(std::string(op.key));
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
    const auto pos = fields.find(std::string(field));
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
  auto it = entries_.find(std::string(op.key));
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
  auto it = entries_.find(std::string(op.key));
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

void SingleShardStore::RefreshAccess(std::string_view key, core::SteadyTime now) {
  auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return;
  if (!it->second.tombstoned) LruTouch(it->second);
  it->second.eviction_deadline = now + it->second.eviction;
}

SingleShardStore::EvictExpiredReport SingleShardStore::EvictExpired(core::SteadyTime now,
                                                                    core::SequenceId horizon) {
  EvictExpiredReport report;
  uint64_t unevictable = 0;
  for (auto it = entries_.begin(); it != entries_.end();) {
    // Tombstones are reclaimed by GcTombstones once cold catches up, never by
    // the eviction deadline — removing one early could expose a stale overlay.
    if (it->second.tombstoned) {
      ++it;
      continue;
    }
    // Undrained, even if expired: a miss would read an older value from
    // buffer or cold.
    if (it->second.latest_seq > horizon) {
      unevictable += Footprint(it->second, it->first);
      ++it;
      continue;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    const bool ttl_expired = IsExpiredByTtl(it->second, config_.wall_clock);
    const bool deadline_elapsed = it->second.eviction_deadline <= now;
    if (ttl_expired || deadline_elapsed) {
      it = Evict(it, /*leave_stub=*/!ttl_expired);
      // Bucket by the entry's actual disposition, not raw iteration: TTL is a
      // deletion (expired_count_), deadline is a tier transition
      // (eviction_count_). TTL wins when both fire (the semantic outcome is
      // "deleted entirely"). Tombstones are already skipped above, so C8's
      // HOT-4 tombstone-handling change cannot mis-bucket a tombstoned entry
      // here — only live entries reach this branch.
      if (ttl_expired) {
        expired_count_++;
        ++report.by_ttl;
      } else {
        eviction_count_++;
        ++report.by_deadline;
      }
    } else {
      ++it;
    }
  }
  unevictable_bytes_ = unevictable;
  UpdateBackpressure();
  return report;
}

size_t SingleShardStore::EvictLru(size_t target_bytes, core::SequenceId horizon) {
  return EvictLru(target_bytes, std::string_view{}, horizon);
}

size_t SingleShardStore::EvictLru(size_t target_bytes, std::string_view protect_key,
                                  core::SequenceId horizon) {
  if (UsedBytes() <= target_bytes || (lru_dry_ && horizon <= lru_dry_horizon_)) {
    UpdateBackpressure();
    return 0;
  }

  // Walk from the cold end. Undrained and protected entries were written
  // last, so they sit near the hot end and the skip over them is short.
  lru_dry_ = false;
  size_t evicted = 0;
  bool skipped_evictable = false;
  Entry* entry = lru_oldest_;
  while (entry != nullptr && UsedBytes() > target_bytes) {
    Entry* const newer = entry->lru_newer;
    ++lru_visits_;
    const bool drained = entry->latest_seq <= horizon;
    if (drained && !protect_key.empty() && *entry->lru_key == protect_key) {
      skipped_evictable = true;
    } else if (drained) {
      const bool expired = IsExpiredByTtl(*entry, config_.wall_clock);
      Evict(entries_.find(*entry->lru_key), /*leave_stub=*/!expired);
      eviction_count_++;
      ++evicted;
    }
    entry = newer;
  }
  // Nothing left to evict: every live entry is undrained until cold
  // passes this horizon.
  if (UsedBytes() > target_bytes && !skipped_evictable) {
    lru_dry_ = true;
    lru_dry_horizon_ = horizon;
    unevictable_bytes_ = live_bytes_;
  }
  UpdateBackpressure();
  return evicted;
}

bool SingleShardStore::EnsureCapacityFor(std::string_view protect_key, core::SequenceId horizon) {
  // Called post-write: the just-written entry (protect_key) is already counted
  // in used bytes and must survive, so make room by evicting OTHER LRU keys
  // down to the budget. Suppressed during replay: evicting mid-replay would
  // make the rebuilt hot view depend on memory timing, breaking deterministic
  // queue replay (invariant 4). The eviction worker reconverges after replay.
  if (replay_mode_ || !governor_.Enabled()) return true;
  if (!governor_.WouldExceed(UsedBytes(), 0)) {
    UpdateBackpressure();
    return true;
  }
  // Down to 95% of the budget, so the next writes do not trigger again.
  const size_t budget = governor_.Target(0);
  EvictLru(budget - (budget / 20), protect_key, horizon);
  // After evicting every other eligible key, the protected entry may still not
  // fit (a single value larger than the whole budget). It is already durable in
  // the queue, so the caller surfaces kResourceExhausted as an admission signal
  // — not a lost write (invariant 2).
  // Other live keys leave once cold drains them, so only tombstones,
  // stubs and this entry count.
  uint64_t floor = UsedBytes() - live_bytes_;
  if (const auto it = entries_.find(std::string(protect_key));
      it != entries_.end() && !it->second.tombstoned) {
    floor += Footprint(it->second, protect_key);
  }
  return !governor_.WouldExceed(floor, 0);
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
          .load_discards = load_discards_,
          .unevictable_bytes = lru_dry_ ? live_bytes_ : unevictable_bytes_,
          .backpressured = backpressured_};
}

void SingleShardStore::Wipe(core::SequenceId seq) {
  entries_.clear();
  lru_newest_ = nullptr;
  lru_oldest_ = nullptr;
  lru_dry_ = false;
  stubs_.Clear();
  loading_.clear();
  entry_bytes_ = 0;
  live_bytes_ = 0;
  key_count_ = 0;
  unevictable_bytes_ = 0;
  backpressured_ = false;
  flush_seq_ = std::max(flush_seq_, seq);
}

bool SingleShardStore::DropStub(std::string_view key) { return stubs_.Erase(key); }

std::optional<LoadToken> SingleShardStore::BeginLoad(std::string_view key) {
  std::string owned(key);
  if (entries_.contains(owned) || loading_.contains(owned)) return std::nullopt;
  const LoadToken token{.id = ++next_load_id_};
  loading_.emplace(std::move(owned), token);
  return token;
}

bool SingleShardStore::CompleteLoad(std::string_view key, LoadToken token, LoadResult&& result,
                                    core::EvictionTTL eviction, core::SequenceId horizon) {
  const std::string owned(key);
  const auto pending = loading_.find(owned);
  if (pending == loading_.end() || pending->second != token) {
    ++load_discards_;
    return false;
  }
  loading_.erase(pending);
  if (entries_.contains(owned)) {
    ++load_discards_;
    return false;
  }
  LoadResult installed = std::move(result);
  if (const auto* exists = std::get_if<LoadedExists>(&installed)) {
    stubs_.Put(key, Stub{.type = exists->type, .abs_ttl_ms = exists->abs_ttl_ms});
    return stubs_.Find(key) != nullptr;
  }
  stubs_.Erase(key);
  auto* full = std::get_if<LoadedFull>(&installed);
  if (full == nullptr) {
    // Drained by definition, so tombstone GC may take it.
    InsertTombstone(key, 0);
    return true;
  }

  const auto it = entries_.try_emplace(owned).first;
  Entry& entry = it->second;
  entry.type = full->type();
  entry.value = std::move(full->value);
  entry.bytes = full->bytes;
  entry.abs_ttl_ms = full->abs_ttl_ms;
  entry.eviction = eviction;
  entry.eviction_deadline = config_.steady_clock() + eviction;
  // Loaded state is already drained.
  entry.latest_seq = 0;
  LruLink(it);
  NoteEvictable(entry.latest_seq);
  key_count_++;
  TrackInsert(entry, key);
  EnsureCapacityFor(key, horizon);
  return true;
}

size_t SingleShardStore::CompleteLoads(std::span<LoadCompletion> loads,
                                       const core::EvictionPolicy& policy,
                                       core::SequenceId horizon) {
  size_t installed = 0;
  for (auto& load : loads) {
    if (CompleteLoad(load.key, load.token, std::move(load.result), policy.Resolve(load.key),
                     horizon)) {
      ++installed;
    }
  }
  return installed;
}

void SingleShardStore::AbortLoad(std::string_view key, LoadToken token) {
  const auto pending = loading_.find(std::string(key));
  if (pending != loading_.end() && pending->second == token) loading_.erase(pending);
}

bool SingleShardStore::LoadPending(std::string_view key) const {
  return !loading_.empty() && loading_.contains(std::string(key));
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

const Entry* SingleShardStore::FindEntry(std::string_view key) const {
  auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return nullptr;
  return &it->second;
}

const Entry* SingleShardStore::FindLiveEntry(std::string_view key) const {
  const auto* entry = FindEntry(key);
  if (entry == nullptr) return nullptr;
  if (entry->tombstoned) return nullptr;
  if (IsExpiredByTtl(*entry, config_.wall_clock)) return nullptr;
  return entry;
}

Entry& SingleShardStore::GetOrCreateEntry(std::string_view key, Entry::Type type,
                                          core::EvictionTTL eviction) {
  auto [it, inserted] = entries_.try_emplace(std::string(key));
  // A tombstone is reborn as a fresh live key. Untrack its footprint first so
  // the slot is in the same state as a freshly inserted entry (the caller's
  // TrackInsert then accounts for the new value).
  if (!inserted && it->second.tombstoned) {
    TrackRemove(it->second, key);
  }
  if (inserted || it->second.tombstoned) {
    it->second.type = type;
    it->second.tombstoned = false;
    it->second.latest_seq = 0;
    it->second.abs_ttl_ms = 0;
    it->second.eviction = eviction;
    it->second.eviction_deadline = config_.steady_clock() + eviction;
    switch (type) {
      case Entry::Type::kString:
        it->second.value = std::string{};
        break;
      case Entry::Type::kSet:
        it->second.value = SetValue{};
        break;
      case Entry::Type::kHash:
        it->second.value = HashValue{};
        break;
      case Entry::Type::kZset:
        it->second.value = ZsetValue{};
        break;
    }
    it->second.bytes = it->second.ApproximateBytes();
    LruLink(it);
    key_count_++;
    // Track the empty-entry baseline so the collection apply paths' balanced
    // TrackRemove/mutate/TrackInsert pattern has a matching prior insert. The
    // string-create path (ApplyStringSet) does its own TrackInsert after
    // setting the value, so it does not go through this baseline.
    TrackInsert(it->second, key);
  }
  return it->second;
}

core::Result<const Entry*> SingleShardStore::FindTypedEntry(std::string_view key,
                                                            Entry::Type expected) const {
  const auto* entry = FindLiveEntry(key);
  if (entry == nullptr) return nullptr;
  if (entry->type != expected) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  return entry;
}

void SingleShardStore::RemoveEntry(const std::string& key) {
  auto it = entries_.find(key);
  if (it == entries_.end()) return;
  if (!it->second.tombstoned) LruUnlink(it->second);
  TrackRemove(it->second, key);
  key_count_--;
  entries_.erase(it);
}

void SingleShardStore::TombstoneEntry(Entry& entry, std::string_view key, core::SequenceId seq) {
  if (entry.tombstoned) {
    entry.latest_seq = seq;
    return;
  }
  TrackRemove(entry, key);
  LruUnlink(entry);
  entry.value = std::string{};
  entry.bytes = entry.ApproximateBytes();
  entry.abs_ttl_ms = 0;
  entry.tombstoned = true;
  entry.latest_seq = seq;
  key_count_--;
  TrackInsert(entry, key);
}

void SingleShardStore::InsertTombstone(std::string_view key, core::SequenceId seq) {
  Entry& entry = entries_[std::string(key)];
  entry.type = Entry::Type::kString;
  entry.value = std::string{};
  entry.bytes = entry.ApproximateBytes();
  entry.tombstoned = true;
  entry.latest_seq = seq;
  TrackInsert(entry, key);
}

core::HotKeyPresence SingleShardStore::Probe(std::string_view key) const {
  const auto* entry = FindEntry(key);
  if (entry == nullptr) return core::HotKeyPresence::kAbsent;
  if (entry->tombstoned) return core::HotKeyPresence::kTombstoned;
  if (IsExpiredByTtl(*entry, config_.wall_clock)) return core::HotKeyPresence::kAbsent;
  return core::HotKeyPresence::kPresent;
}

size_t SingleShardStore::GcTombstones(core::SequenceId horizon) {
  size_t reclaimed = 0;
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (it->second.tombstoned && it->second.latest_seq <= horizon) {
      TrackRemove(it->second, it->first);
      it = entries_.erase(it);
      ++reclaimed;
    } else {
      ++it;
    }
  }
  return reclaimed;
}

void SingleShardStore::TrackInsert(const Entry& entry, std::string_view key) {
  const auto bytes = Footprint(entry, key);
  entry_bytes_ += bytes;
  if (!entry.tombstoned) live_bytes_ += bytes;
}

void SingleShardStore::TrackRemove(const Entry& entry, std::string_view key) {
  const auto bytes = Footprint(entry, key);
  entry_bytes_ = (entry_bytes_ >= bytes) ? entry_bytes_ - bytes : 0;
  if (!entry.tombstoned) live_bytes_ = (live_bytes_ >= bytes) ? live_bytes_ - bytes : 0;
}

void SingleShardStore::MarkWritten(std::string_view key, core::SequenceId seq) {
  const std::string owned(key);
  if (const auto it = entries_.find(owned); it != entries_.end()) {
    it->second.latest_seq = seq;
    if (!it->second.tombstoned) {
      LruTouch(it->second);
      NoteEvictable(seq);
    }
  }
  if (!loading_.empty()) loading_.erase(owned);
  stubs_.Erase(key);
}

SingleShardStore::EntryMap::iterator SingleShardStore::Evict(EntryMap::iterator it,
                                                             bool leave_stub) {
  if (leave_stub) {
    stubs_.Put(it->first, Stub{.type = it->second.type,
                               .abs_ttl_ms = it->second.abs_ttl_ms,
                               .latest_seq = it->second.latest_seq});
  }
  LruUnlink(it->second);
  TrackRemove(it->second, it->first);
  key_count_--;
  return entries_.erase(it);
}

void SingleShardStore::LruLink(EntryMap::iterator it) {
  it->second.lru_key = &it->first;
  it->second.lru_older = lru_newest_;
  it->second.lru_newer = nullptr;
  if (lru_newest_ != nullptr) {
    lru_newest_->lru_newer = &it->second;
  } else {
    lru_oldest_ = &it->second;
  }
  lru_newest_ = &it->second;
}

void SingleShardStore::LruUnlink(Entry& entry) {
  if (entry.lru_newer != nullptr) {
    entry.lru_newer->lru_older = entry.lru_older;
  } else {
    lru_newest_ = entry.lru_older;
  }
  if (entry.lru_older != nullptr) {
    entry.lru_older->lru_newer = entry.lru_newer;
  } else {
    lru_oldest_ = entry.lru_newer;
  }
  entry.lru_newer = nullptr;
  entry.lru_older = nullptr;
}

void SingleShardStore::LruTouch(Entry& entry) {
  if (lru_newest_ == &entry) return;
  LruUnlink(entry);
  entry.lru_older = lru_newest_;
  lru_newest_->lru_newer = &entry;
  lru_newest_ = &entry;
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
