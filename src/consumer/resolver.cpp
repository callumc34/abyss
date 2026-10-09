#include "abyss/consumer/resolver.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <future>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/fatal.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_format.h"
#include "abyss/core/shard_router.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.resolver")

namespace abyss::consumer {

namespace {

// Same capped doubling as the cold consumer's no-progress loop backoff.
constexpr std::chrono::milliseconds kAppendRetryInitialBackoff{1};
constexpr std::chrono::milliseconds kAppendRetryMaxBackoff{1000};

std::string AsciiUpper(std::string_view s) {
  std::string out(s);
  for (auto& c : out) {
    if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 'a' + 'A');
  }
  return out;
}

// Monotonic CAS-max advance: raises `target` to `value` iff `value` is larger.
void AdvanceMaxSeq(std::atomic<core::SequenceId>& target, core::SequenceId value) {
  auto cur = target.load(std::memory_order_relaxed);
  while (value > cur && !target.compare_exchange_weak(cur, value, std::memory_order_release,
                                                      std::memory_order_relaxed)) {
  }
}

uint64_t WallMs(core::WallTime t) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count());
}

bool ParseDouble(std::string_view s, double& out) {
  if (s.empty()) return false;
  const char* begin = s.data();
  const char* end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(begin, end, out);
  return ec == std::errc{} && ptr == end;
}

bool ParseUint64(std::string_view s, uint64_t& out) {
  if (s.empty()) return false;
  const char* begin = s.data();
  const char* end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(begin, end, out);
  return ec == std::errc{} && ptr == end;
}

core::entry::Resolved MakeSkip(core::SequenceId ref, core::RespValue return_value) {
  return core::entry::Resolved{.ref = ref,
                               .decision = core::Decision::kSkip,
                               .materialised_ops = {},
                               .return_value = std::move(return_value)};
}

core::entry::Resolved MakeApply(core::SequenceId ref, std::vector<core::RespCommand> ops,
                                core::RespValue return_value) {
  return core::entry::Resolved{.ref = ref,
                               .decision = core::Decision::kApply,
                               .materialised_ops = std::move(ops),
                               .return_value = std::move(return_value)};
}

core::RespCommand MakeSet(std::string_view key, std::string_view value, uint64_t abs_ttl_ms) {
  core::RespCommand cmd;
  if (abs_ttl_ms > 0) {
    cmd.args = {"SET", std::string(key), std::string(value), "PXAT", std::to_string(abs_ttl_ms)};
  } else {
    cmd.args = {"SET", std::string(key), std::string(value)};
  }
  return cmd;
}

core::RespCommand MakeDel(std::string_view key) {
  return core::RespCommand{.args = {"DEL", std::string(key)}};
}

core::RespCommand MakeExpirePxat(std::string_view key, uint64_t abs_ttl_ms) {
  return core::RespCommand{.args = {"PEXPIREAT", std::string(key), std::to_string(abs_ttl_ms)}};
}

core::RespCommand MakeHashSet(std::string_view key, std::string_view field, std::string_view val) {
  return core::RespCommand{
      .args = {"HSET", std::string(key), std::string(field), std::string(val)}};
}

core::RespCommand MakeZsetAdd(std::string_view key,
                              const std::vector<core::ops::ZsetAdd::Entry>& entries) {
  core::RespCommand cmd;
  cmd.args.reserve(2 + (entries.size() * 2));
  cmd.args.emplace_back("ZADD");
  cmd.args.emplace_back(key);
  for (const auto& e : entries) {
    cmd.args.emplace_back(core::FormatRespDouble(e.score));
    cmd.args.emplace_back(e.member);
  }
  return cmd;
}

}  // namespace

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

Resolver::Resolver(core::Queue& queue, core::ColdStore& cold, CompactionBufferRouter& buffer_router,
                   core::ConsumerRpc& rpc, core::ApplyNotifier& apply_notifier, Config config)
    : queue_(queue),
      cold_(cold),
      buffer_router_(buffer_router),
      rpc_(rpc),
      apply_notifier_(apply_notifier),
      config_(config),
      stripes_(std::max<uint32_t>(1, config.stripe_count)),
      cache_(config.cache, core::DefaultSteadyClock) {}

Resolver::~Resolver() { Stop(); }

void Resolver::Start() {
  if (running_.exchange(true, std::memory_order_acq_rel)) return;
  stop_requested_.store(false, std::memory_order_release);
  thread_ = std::thread(&Resolver::Run, this);
}

void Resolver::RequestStop() {
  {
    const std::scoped_lock lock(stop_mu_);
    stop_requested_.store(true, std::memory_order_release);
  }
  stop_cv_.notify_all();
}

void Resolver::Join() {
  if (!running_.load(std::memory_order_acquire)) return;
  if (thread_.joinable()) thread_.join();
  running_.store(false, std::memory_order_release);
}

void Resolver::Stop() {
  RequestStop();
  Join();
}

Resolver::Snapshot Resolver::GetSnapshot() const {
  Snapshot s;
  s.conditionals_resolved = conditionals_resolved_.load(std::memory_order_relaxed);
  s.decisions_apply = decisions_apply_.load(std::memory_order_relaxed);
  s.decisions_skip = decisions_skip_.load(std::memory_order_relaxed);
  s.cache_hits = cache_hits_.load(std::memory_order_relaxed);
  s.buffer_hits = buffer_hits_.load(std::memory_order_relaxed);
  s.cold_hits = cold_hits_.load(std::memory_order_relaxed);
  s.cold_timeouts = cold_timeouts_.load(std::memory_order_relaxed);
  s.cold_errors = cold_errors_.load(std::memory_order_relaxed);
  s.apply_wait_timeouts = apply_wait_timeouts_.load(std::memory_order_relaxed);
  s.durable_wait_timeouts = durable_wait_timeouts_.load(std::memory_order_relaxed);
  s.append_failures = append_failures_.load(std::memory_order_relaxed);
  s.commit_failures = commit_failures_.load(std::memory_order_relaxed);
  s.parse_failures = parse_failures_.load(std::memory_order_relaxed);
  s.replayed_resolveds_emitted = replayed_resolveds_emitted_.load(std::memory_order_relaxed);
  s.flushes_observed = flushes_observed_.load(std::memory_order_relaxed);
  s.flush_skip_resolveds_emitted = flush_skip_resolveds_emitted_.load(std::memory_order_relaxed);
  s.latest_drained_seq = latest_drained_seq_.load(std::memory_order_relaxed);
  s.last_commit_seq = last_commit_seq_.load(std::memory_order_relaxed);
  s.resolver_durable_floor = resolver_durable_floor_.load(std::memory_order_relaxed);
  s.latest_flush_seq = latest_flush_seq_.load(std::memory_order_relaxed);
  s.cache_entries = cache_.Size();
  s.cache_bytes = cache_.BytesEstimate();
  return s;
}

// ---------------------------------------------------------------------------
// Tiered lookup helpers
// ---------------------------------------------------------------------------

namespace {

struct KeyView {
  bool exists = false;
  bool definitive = false;  // false = error/timeout
  bool ttl_known = false;
  uint64_t abs_ttl_ms = 0;
  std::optional<std::string> string_value;  // populated only when known
  enum class Source { kMiss, kCache, kBuffer, kCold, kColdTimeout, kColdError };
  Source source = Source::kMiss;
};

}  // namespace

std::vector<uint32_t> Resolver::StripeIndicesFor(const std::vector<std::string_view>& keys) const {
  std::set<uint32_t> uniq;
  const auto count = static_cast<uint32_t>(stripes_.size());
  for (auto k : keys) uniq.insert(core::ComputeShard(k, count));
  return {uniq.begin(), uniq.end()};
}

// ---------------------------------------------------------------------------
// Decision algorithm
// ---------------------------------------------------------------------------

namespace {

KeyView LookupKey(ExistenceCache& cache, CompactionBufferRouter& buffer, core::ColdStore& cold,
                  std::optional<core::Duration> deadline, std::string_view key,
                  std::atomic<uint64_t>& cache_hits, std::atomic<uint64_t>& buffer_hits,
                  std::atomic<uint64_t>& cold_hits, std::atomic<uint64_t>& cold_timeouts,
                  std::atomic<uint64_t>& cold_errors, uint64_t now_ms,
                  bool cache_only_after_miss = false) {
  KeyView out;

  // Cache.
  if (auto cached = cache.GetKey(key); cached.has_value()) {
    out.source = KeyView::Source::kCache;
    out.definitive = true;
    out.exists = cached->exists;
    out.abs_ttl_ms = cached->abs_ttl_ms;
    out.ttl_known = cached->exists;
    out.string_value = cached->string_value;
    if (out.exists && out.abs_ttl_ms > 0 && out.abs_ttl_ms <= now_ms) {
      out.exists = false;
    }
    cache_hits.fetch_add(1, std::memory_order_relaxed);
    return out;
  }

  // Post-Flush replay: cold still holds pre-Flush garbage, so a cache miss is
  // definitively absent rather than a fall-through to stale tiers.
  if (cache_only_after_miss) {
    out.source = KeyView::Source::kMiss;
    out.definitive = true;
    out.exists = false;
    return out;
  }

  // Buffer (existence + string value).
  auto buf_existence = buffer.Exec(core::ops::ReadOp{core::ops::Exists{.keys = {key}}});
  if (buf_existence.has_value()) {
    const auto count = buf_existence->AsInteger();
    out.exists = (count > 0);
    out.definitive = true;
    out.source = KeyView::Source::kBuffer;
    if (out.exists) {
      auto buf_str = buffer.Exec(core::ops::ReadOp{core::ops::StringGet{.key = key}});
      if (buf_str.has_value() && buf_str->IsBulkString()) {
        out.string_value = buf_str->AsString();
      }
    }
    buffer_hits.fetch_add(1, std::memory_order_relaxed);
    return out;
  }

  // Cold.
  auto cold_existence = cold.Exec(core::ops::ReadOp{core::ops::Exists{.keys = {key}}}, deadline);
  if (!cold_existence.has_value()) {
    if (cold_existence.error().code() == core::ErrorCode::kTimeout) {
      cold_timeouts.fetch_add(1, std::memory_order_relaxed);
      out.source = KeyView::Source::kColdTimeout;
    } else {
      cold_errors.fetch_add(1, std::memory_order_relaxed);
      out.source = KeyView::Source::kColdError;
    }
    return out;
  }
  out.exists = (cold_existence->AsInteger() > 0);
  out.definitive = true;
  out.source = KeyView::Source::kCold;
  if (out.exists) {
    auto cold_str = cold.Exec(core::ops::ReadOp{core::ops::StringGet{.key = key}}, deadline);
    if (cold_str.has_value() && cold_str->IsBulkString()) {
      out.string_value = cold_str->AsString();
    }
  }
  cold_hits.fetch_add(1, std::memory_order_relaxed);
  return out;
}

// definitive=false on cold timeout/error.
std::optional<double> LookupZsetMemberScore(
    ExistenceCache& cache, CompactionBufferRouter& buffer, core::ColdStore& cold,
    std::optional<core::Duration> deadline, std::string_view key, std::string_view member,
    std::atomic<uint64_t>& cache_hits, std::atomic<uint64_t>& buffer_hits,
    std::atomic<uint64_t>& cold_hits, std::atomic<uint64_t>& cold_timeouts,
    std::atomic<uint64_t>& cold_errors, bool& definitive, bool cache_only_after_miss = false) {
  definitive = true;
  if (auto cached = cache.GetMember(key, member); cached.has_value()) {
    cache_hits.fetch_add(1, std::memory_order_relaxed);
    return cached->score;
  }
  // Post-Flush replay shortcut — see LookupKey.
  if (cache_only_after_miss) return std::nullopt;
  auto buf = buffer.Exec(core::ops::ReadOp{core::ops::ZsetScore{.key = key, .member = member}});
  if (buf.has_value()) {
    buffer_hits.fetch_add(1, std::memory_order_relaxed);
    if (buf->IsNull()) return std::nullopt;
    if (buf->IsBulkString()) {
      double score = 0.0;
      if (ParseDouble(buf->AsString(), score)) return score;
    }
    return std::nullopt;
  }
  auto col =
      cold.Exec(core::ops::ReadOp{core::ops::ZsetScore{.key = key, .member = member}}, deadline);
  if (!col.has_value()) {
    if (col.error().code() == core::ErrorCode::kTimeout) {
      cold_timeouts.fetch_add(1, std::memory_order_relaxed);
    } else {
      cold_errors.fetch_add(1, std::memory_order_relaxed);
    }
    definitive = false;
    return std::nullopt;
  }
  cold_hits.fetch_add(1, std::memory_order_relaxed);
  if (col->IsNull()) return std::nullopt;
  if (col->IsBulkString()) {
    double score = 0.0;
    if (ParseDouble(col->AsString(), score)) return score;
  }
  return std::nullopt;
}

std::optional<std::string> LookupHashFieldValue(
    ExistenceCache& cache, CompactionBufferRouter& buffer, core::ColdStore& cold,
    std::optional<core::Duration> deadline, std::string_view key, std::string_view field,
    std::atomic<uint64_t>& cache_hits, std::atomic<uint64_t>& buffer_hits,
    std::atomic<uint64_t>& cold_hits, std::atomic<uint64_t>& cold_timeouts,
    std::atomic<uint64_t>& cold_errors, bool& definitive, bool& exists,
    bool cache_only_after_miss = false) {
  definitive = true;
  exists = false;
  if (auto cached = cache.GetField(key, field); cached.has_value()) {
    cache_hits.fetch_add(1, std::memory_order_relaxed);
    exists = cached->value_known || !cached->value.empty();
    if (cached->value_known) return cached->value;
    return std::nullopt;
  }
  // Post-Flush replay shortcut — see LookupKey.
  if (cache_only_after_miss) return std::nullopt;
  auto buf = buffer.Exec(core::ops::ReadOp{core::ops::HashGet{.key = key, .field = field}});
  if (buf.has_value()) {
    buffer_hits.fetch_add(1, std::memory_order_relaxed);
    if (buf->IsNull()) {
      exists = false;
      return std::nullopt;
    }
    if (buf->IsBulkString()) {
      exists = true;
      return buf->AsString();
    }
    return std::nullopt;
  }
  auto col = cold.Exec(core::ops::ReadOp{core::ops::HashGet{.key = key, .field = field}}, deadline);
  if (!col.has_value()) {
    if (col.error().code() == core::ErrorCode::kTimeout) {
      cold_timeouts.fetch_add(1, std::memory_order_relaxed);
    } else {
      cold_errors.fetch_add(1, std::memory_order_relaxed);
    }
    definitive = false;
    return std::nullopt;
  }
  cold_hits.fetch_add(1, std::memory_order_relaxed);
  if (col->IsNull()) {
    exists = false;
    return std::nullopt;
  }
  if (col->IsBulkString()) {
    exists = true;
    return col->AsString();
  }
  return std::nullopt;
}

}  // namespace

// ---------------------------------------------------------------------------
// Per-command decision implementations
// ---------------------------------------------------------------------------

namespace {

struct SetParse {
  std::string_view key;
  std::string_view value;
  uint64_t abs_ttl_ms = 0;
  bool keep_ttl = false;
  bool nx = false;
  bool xx = false;
  bool get = false;
  bool valid = false;
  std::string error;
};

// The predicate comes from the entry's flags, never from re-reading the command
// text. Those are two representations of one decision, and re-deriving here is
// how they drift: the frontend already extracted the flags, validated their
// combinations, and recorded them in the entry (ADP-011 -- a Conditional is
// "op + predicate"). Only the operand data -- key, value, absolute TTL -- is
// read from the command, and that goes through the canonical parser rather than
// a second hand-rolled one.
SetParse ParseSetArgs(const core::RespCommand& cmd, core::PredicateFlags flags, uint64_t now_ms) {
  SetParse out;
  out.nx = core::HasFlag(flags, core::PredicateFlags::kNx);
  out.xx = core::HasFlag(flags, core::PredicateFlags::kXx);
  out.get = core::HasFlag(flags, core::PredicateFlags::kGet);
  out.keep_ttl = core::HasFlag(flags, core::PredicateFlags::kKeepTtl);

  if (out.nx && out.xx) {
    out.error = "syntax error";
    return out;
  }
  auto op = core::ops::ParseWriteOp(cmd.Name(), cmd, now_ms);
  if (!op.has_value()) {
    out.error = std::string{op.error().message()};
    return out;
  }
  const auto* set = std::get_if<core::ops::StringSet>(&*op);
  if (set == nullptr) {
    out.error = "internal: SET did not parse to a string-set op";
    return out;
  }
  out.key = set->key;
  out.value = set->value;
  out.abs_ttl_ms = set->abs_ttl_ms;
  out.valid = true;
  return out;
}

}  // namespace

// ---------------------------------------------------------------------------
// Decide
// ---------------------------------------------------------------------------

core::entry::Resolved Resolver::Decide(const core::QueueEntry& entry,
                                       const core::entry::Conditional& cond) {
  const auto& cmd = cond.cmd;
  const auto seq = entry.seq;
  const auto now_ms = WallMs(entry.appended_at);
  const std::optional<core::Duration> deadline{config_.cold_lookup_timeout};
  // During replay, cold still reflects pre-Flush state (cold replay runs after
  // resolver replay). Post-Flush danglings must trust the cache alone.
  const bool cache_only = replay_mode_.load(std::memory_order_acquire) &&
                          latest_flush_seq_.load(std::memory_order_acquire) > 0 &&
                          entry.seq > latest_flush_seq_.load(std::memory_order_acquire);

  if (cmd.args.empty()) {
    parse_failures_.fetch_add(1, std::memory_order_relaxed);
    return MakeSkip(
        seq, core::RespValue::Error(core::ErrorPrefix::kErr, "empty command in conditional entry"));
  }
  const auto name = AsciiUpper(cmd.args[0]);

  // -- SET / SETNX -----------------------------------------------------------
  if (name == "SET" || name == "SETNX") {
    SetParse parsed =
        (name == "SETNX")
            ? SetParse{.key =
                           cmd.args.size() > 1 ? std::string_view(cmd.args[1]) : std::string_view{},
                       .value =
                           cmd.args.size() > 2 ? std::string_view(cmd.args[2]) : std::string_view{},
                       .abs_ttl_ms = 0,
                       .keep_ttl = false,
                       .nx = true,
                       .xx = false,
                       .get = false,
                       .valid = cmd.args.size() == 3,
                       .error = cmd.args.size() != 3 ? "wrong number of arguments" : std::string{}}
            : ParseSetArgs(cmd, cond.flags, now_ms);
    if (!parsed.valid) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr, parsed.error));
    }
    KeyView view =
        LookupKey(cache_, buffer_router_, cold_, deadline, parsed.key, cache_hits_, buffer_hits_,
                  cold_hits_, cold_timeouts_, cold_errors_, now_ms, cache_only);
    if (!view.definitive) {
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "cold tier unavailable for conditional lookup"));
    }
    const bool exists = view.exists;
    if (parsed.nx && exists) {
      core::RespValue ret;
      if (parsed.get) {
        ret = view.string_value.has_value() ? core::RespValue::BulkString(*view.string_value)
                                            : core::RespValue::Null();
      } else if (name == "SETNX") {
        ret = core::RespValue::Integer(0);
      } else {
        ret = core::RespValue::Null();
      }
      return MakeSkip(seq, std::move(ret));
    }
    if (parsed.xx && !exists) {
      return MakeSkip(seq, core::RespValue::Null());
    }
    uint64_t abs_ttl = parsed.abs_ttl_ms;
    if (parsed.keep_ttl && view.ttl_known && view.abs_ttl_ms > 0) abs_ttl = view.abs_ttl_ms;
    auto materialised = std::vector<core::RespCommand>{MakeSet(parsed.key, parsed.value, abs_ttl)};
    core::RespValue ret;
    if (parsed.get) {
      ret = view.string_value.has_value() ? core::RespValue::BulkString(*view.string_value)
                                          : core::RespValue::Null();
    } else if (name == "SETNX") {
      ret = core::RespValue::Integer(1);
    } else {
      ret = core::RespValue::SimpleString("OK");
    }
    return MakeApply(seq, std::move(materialised), std::move(ret));
  }

  // -- MSETNX ---------------------------------------------------------------
  if (name == "MSETNX") {
    if (cmd.args.size() < 3 || (cmd.args.size() % 2) == 0) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "wrong number of arguments for 'MSETNX'"));
    }
    std::vector<std::pair<std::string_view, std::string_view>> kvs;
    kvs.reserve((cmd.args.size() - 1) / 2);
    for (size_t i = 1; i + 1 < cmd.args.size(); i += 2) {
      kvs.emplace_back(cmd.args[i], cmd.args[i + 1]);
    }
    for (const auto& [k, _] : kvs) {
      KeyView v = LookupKey(cache_, buffer_router_, cold_, deadline, k, cache_hits_, buffer_hits_,
                            cold_hits_, cold_timeouts_, cold_errors_, now_ms, cache_only);
      if (!v.definitive) {
        return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                    "cold tier unavailable for MSETNX"));
      }
      if (v.exists) {
        return MakeSkip(seq, core::RespValue::Integer(0));
      }
    }
    std::vector<core::RespCommand> materialised;
    materialised.reserve(kvs.size());
    for (const auto& [k, v] : kvs) materialised.push_back(MakeSet(k, v, 0));
    return MakeApply(seq, std::move(materialised), core::RespValue::Integer(1));
  }

  // -- ZADD (NX|XX|GT|LT|CH) -----------------------------------------------
  if (name == "ZADD") {
    if (cmd.args.size() < 4) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "wrong number of arguments for 'ZADD'"));
    }
    // Predicate from the entry, not from re-reading the tokens; see ParseSetArgs.
    const bool nx = core::HasFlag(cond.flags, core::PredicateFlags::kNx);
    const bool xx = core::HasFlag(cond.flags, core::PredicateFlags::kXx);
    const bool gt = core::HasFlag(cond.flags, core::PredicateFlags::kZAddGt);
    const bool lt = core::HasFlag(cond.flags, core::PredicateFlags::kZAddLt);
    const bool ch = core::HasFlag(cond.flags, core::PredicateFlags::kZAddCh);
    // The tokens are still skipped to find where the score-member pairs start.
    size_t i = 2;
    for (; i < cmd.args.size(); ++i) {
      const auto opt = AsciiUpper(cmd.args[i]);
      if (opt != "NX" && opt != "XX" && opt != "GT" && opt != "LT" && opt != "CH") break;
    }
    if (nx && xx) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(
          seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                      "XX and NX options at the same time are not compatible"));
    }
    if ((gt && lt) || (nx && (gt || lt))) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(
                               core::ErrorPrefix::kErr,
                               "GT, LT, and/or NX options at the same time are not compatible"));
    }
    if ((cmd.args.size() - i) < 2 || (cmd.args.size() - i) % 2 != 0) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "ZADD requires score-member pairs after flags"));
    }
    const std::string_view key = cmd.args[1];
    std::vector<core::ops::ZsetAdd::Entry> proposed;
    for (; i < cmd.args.size(); i += 2) {
      double score = 0.0;
      if (!ParseDouble(cmd.args[i], score)) {
        parse_failures_.fetch_add(1, std::memory_order_relaxed);
        return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                    "ZADD score is not a valid double"));
      }
      proposed.push_back({.score = score, .member = cmd.args[i + 1]});
    }

    // Per-member decision: filter `proposed` based on NX/XX/GT/LT.
    std::vector<core::ops::ZsetAdd::Entry> filtered;
    int64_t added = 0;
    int64_t changed = 0;
    for (const auto& p : proposed) {
      bool definitive = false;
      auto existing = LookupZsetMemberScore(cache_, buffer_router_, cold_, deadline, key, p.member,
                                            cache_hits_, buffer_hits_, cold_hits_, cold_timeouts_,
                                            cold_errors_, definitive, cache_only);
      if (!definitive) {
        return MakeSkip(seq,
                        core::RespValue::Error(core::ErrorPrefix::kErr,
                                               "cold tier unavailable for ZADD member lookup"));
      }
      const bool member_exists = existing.has_value();
      if (nx && member_exists) continue;
      if (xx && !member_exists) continue;
      if (gt && member_exists && !(p.score > *existing)) continue;
      if (lt && member_exists && !(p.score < *existing)) continue;
      // GT against absent: GT requires existing per Redis semantics; skip.
      if (gt && !member_exists) continue;
      filtered.push_back(p);
      if (!member_exists) ++added;
      if (member_exists && p.score != *existing) ++changed;
    }
    if (filtered.empty()) {
      return MakeSkip(seq, core::RespValue::Integer(0));
    }
    return MakeApply(seq, {MakeZsetAdd(key, filtered)},
                     core::RespValue::Integer(ch ? added + changed : added));
  }

  // -- EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT (NX|XX|GT|LT) ----------------
  if (name == "EXPIRE" || name == "PEXPIRE" || name == "EXPIREAT" || name == "PEXPIREAT") {
    if (cmd.args.size() < 3) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "wrong number of arguments for EXPIRE"));
    }
    uint64_t ttl_arg = 0;
    if (!ParseUint64(cmd.args[2], ttl_arg)) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(
          seq, core::RespValue::Error(core::ErrorPrefix::kErr, "TTL is not a valid integer"));
    }
    uint64_t requested_abs_ttl = 0;
    if (name == "EXPIRE") {
      requested_abs_ttl = now_ms + (ttl_arg * 1000);
    } else if (name == "PEXPIRE") {
      requested_abs_ttl = now_ms + ttl_arg;
    } else if (name == "EXPIREAT") {
      requested_abs_ttl = ttl_arg * 1000;
    } else {
      requested_abs_ttl = ttl_arg;
    }

    // Predicate from the entry, not from re-reading the tokens; see ParseSetArgs.
    const bool nx = core::HasFlag(cond.flags, core::PredicateFlags::kNx);
    const bool xx = core::HasFlag(cond.flags, core::PredicateFlags::kXx);
    const bool gt = core::HasFlag(cond.flags, core::PredicateFlags::kExpireGt);
    const bool lt = core::HasFlag(cond.flags, core::PredicateFlags::kExpireLt);
    if (nx && (xx || gt || lt)) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(
                               core::ErrorPrefix::kErr,
                               "NX and XX, GT or LT options at the same time are not compatible"));
    }
    if (gt && lt) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(
          seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                      "GT and LT options at the same time are not compatible"));
    }

    const std::string_view key = cmd.args[1];
    KeyView view =
        LookupKey(cache_, buffer_router_, cold_, deadline, key, cache_hits_, buffer_hits_,
                  cold_hits_, cold_timeouts_, cold_errors_, now_ms, cache_only);
    if (!view.definitive) {
      return MakeSkip(
          seq, core::RespValue::Error(core::ErrorPrefix::kErr, "cold tier unavailable for EXPIRE"));
    }
    if (!view.exists) return MakeSkip(seq, core::RespValue::Integer(0));

    const bool has_existing_ttl = view.abs_ttl_ms > 0;
    if (nx && has_existing_ttl) return MakeSkip(seq, core::RespValue::Integer(0));
    if (xx && !has_existing_ttl) return MakeSkip(seq, core::RespValue::Integer(0));
    if (gt) {
      if (!has_existing_ttl) return MakeSkip(seq, core::RespValue::Integer(0));
      if (!(requested_abs_ttl > view.abs_ttl_ms)) {
        return MakeSkip(seq, core::RespValue::Integer(0));
      }
    }
    if (lt) {
      // LT against no-existing-TTL always succeeds (no TTL = +inf).
      if (has_existing_ttl && !(requested_abs_ttl < view.abs_ttl_ms)) {
        return MakeSkip(seq, core::RespValue::Integer(0));
      }
    }
    return MakeApply(seq, {MakeExpirePxat(key, requested_abs_ttl)}, core::RespValue::Integer(1));
  }

  // -- RENAMENX -------------------------------------------------------------
  if (name == "RENAMENX") {
    if (cmd.args.size() != 3) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "wrong number of arguments for 'RENAMENX'"));
    }
    const std::string_view src = cmd.args[1];
    const std::string_view dst = cmd.args[2];
    KeyView src_view =
        LookupKey(cache_, buffer_router_, cold_, deadline, src, cache_hits_, buffer_hits_,
                  cold_hits_, cold_timeouts_, cold_errors_, now_ms, cache_only);
    KeyView dst_view =
        LookupKey(cache_, buffer_router_, cold_, deadline, dst, cache_hits_, buffer_hits_,
                  cold_hits_, cold_timeouts_, cold_errors_, now_ms, cache_only);
    if (!src_view.definitive || !dst_view.definitive) {
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "cold tier unavailable for RENAMENX"));
    }
    if (!src_view.exists) {
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr, "no such key"));
    }
    if (dst_view.exists) return MakeSkip(seq, core::RespValue::Integer(0));

    std::vector<core::RespCommand> materialised;
    materialised.push_back(MakeDel(src));
    if (src_view.string_value.has_value()) {
      materialised.push_back(MakeSet(dst, *src_view.string_value, src_view.abs_ttl_ms));
    } else {
      return MakeSkip(
          seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                      "RENAMENX of non-string keys requires a typed snapshot path "
                                      "(not yet wired)"));
    }
    return MakeApply(seq, std::move(materialised), core::RespValue::Integer(1));
  }

  // -- COPY (NX-or-REPLACE) ------------------------------------------------
  if (name == "COPY") {
    if (cmd.args.size() < 3) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "wrong number of arguments for 'COPY'"));
    }
    bool replace = false;
    for (size_t j = 3; j < cmd.args.size(); ++j) {
      const auto opt = AsciiUpper(cmd.args[j]);
      if (opt == "REPLACE") {
        replace = true;
      } else if (opt == "DB") {
        if (j + 1 >= cmd.args.size()) {
          return MakeSkip(seq,
                          core::RespValue::Error(core::ErrorPrefix::kErr, "syntax error after DB"));
        }
        ++j;
      } else {
        return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                    "syntax error: unknown COPY option"));
      }
    }
    const std::string_view src = cmd.args[1];
    const std::string_view dst = cmd.args[2];
    KeyView src_view =
        LookupKey(cache_, buffer_router_, cold_, deadline, src, cache_hits_, buffer_hits_,
                  cold_hits_, cold_timeouts_, cold_errors_, now_ms, cache_only);
    KeyView dst_view =
        LookupKey(cache_, buffer_router_, cold_, deadline, dst, cache_hits_, buffer_hits_,
                  cold_hits_, cold_timeouts_, cold_errors_, now_ms, cache_only);
    if (!src_view.definitive || !dst_view.definitive) {
      return MakeSkip(
          seq, core::RespValue::Error(core::ErrorPrefix::kErr, "cold tier unavailable for COPY"));
    }
    if (!src_view.exists) return MakeSkip(seq, core::RespValue::Integer(0));
    if (dst_view.exists && !replace) return MakeSkip(seq, core::RespValue::Integer(0));

    std::vector<core::RespCommand> materialised;
    if (dst_view.exists) materialised.push_back(MakeDel(dst));
    if (src_view.string_value.has_value()) {
      materialised.push_back(MakeSet(dst, *src_view.string_value, src_view.abs_ttl_ms));
    } else {
      return MakeSkip(
          seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                      "COPY of non-string keys requires a typed snapshot path "
                                      "(not yet wired)"));
    }
    return MakeApply(seq, std::move(materialised), core::RespValue::Integer(1));
  }

  // -- HSETNX --------------------------------------------------------------
  if (name == "HSETNX") {
    if (cmd.args.size() != 4) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "wrong number of arguments for 'HSETNX'"));
    }
    const std::string_view key = cmd.args[1];
    const std::string_view field = cmd.args[2];
    const std::string_view value = cmd.args[3];
    bool definitive = false;
    bool exists = false;
    (void)LookupHashFieldValue(cache_, buffer_router_, cold_, deadline, key, field, cache_hits_,
                               buffer_hits_, cold_hits_, cold_timeouts_, cold_errors_, definitive,
                               exists, cache_only);
    if (!definitive) {
      return MakeSkip(
          seq, core::RespValue::Error(core::ErrorPrefix::kErr, "cold tier unavailable for HSETNX"));
    }
    if (exists) return MakeSkip(seq, core::RespValue::Integer(0));
    return MakeApply(seq, {MakeHashSet(key, field, value)}, core::RespValue::Integer(1));
  }

  // Unknown conditional command — refuse rather than silently apply.
  parse_failures_.fetch_add(1, std::memory_order_relaxed);
  return MakeSkip(
      seq, core::RespValue::Error(core::ErrorPrefix::kErr, "unsupported conditional command"));
}

// ---------------------------------------------------------------------------
// Loop, append, fulfil
// ---------------------------------------------------------------------------

void Resolver::HandleFlush(const core::QueueEntry& entry) {
  ABYSS_LOG_DEBUG("resolver HandleFlush", {"shard", static_cast<int64_t>(config_.shard)},
                  {"seq", static_cast<uint64_t>(entry.seq)});
  cache_.Clear();
  latest_flush_seq_.store(entry.seq, std::memory_order_release);
  flushes_observed_.fetch_add(1, std::memory_order_relaxed);

  (void)rpc_.Fulfill(core::MakeFlushRpcId(core::kResolverConsumer, config_.shard, entry.seq),
                     core::RespValue::SimpleString("OK"));
  apply_notifier_.NotifyApplied(config_.shard, entry.seq);
}

bool Resolver::ProcessEntry(const core::QueueEntry& entry) {
  return std::visit(
      [this, &entry](const auto& payload) -> bool {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          ApplyToCache(entry.seq, entry.appended_at, payload.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Flush>) {
          HandleFlush(entry);
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          // Sorted+deduped stripe acquisition for deadlock-freedom.
          std::vector<std::string_view> keys;
          if (!payload.cmd.args.empty()) {
            const auto upper = AsciiUpper(payload.cmd.args[0]);
            if (upper == "MSETNX") {
              for (size_t i = 1; i + 1 < payload.cmd.args.size(); i += 2) {
                keys.emplace_back(payload.cmd.args[i]);
              }
            } else if (upper == "RENAMENX" || upper == "COPY") {
              if (payload.cmd.args.size() >= 3) {
                keys.emplace_back(payload.cmd.args[1]);
                keys.emplace_back(payload.cmd.args[2]);
              }
            } else if (payload.cmd.args.size() >= 2) {
              keys.emplace_back(payload.cmd.args[1]);
            }
          }
          const core::RpcId client_rpc_id = core::MakeRpcId(config_.shard, entry.seq);
          core::entry::Resolved resolved;
          core::SequenceId resolved_seq = 0;

          // The stripe locks serialise per-key Decide+Append+cache-update in
          // queue order (ADP-011 inv 4). They are released here, before the
          // latency-bound hot-apply wait and the RPC fulfilment, so a sibling
          // key sharing a stripe is never blocked behind another op's wait.
          {
            auto stripe_idxs = StripeIndicesFor(keys);
            std::vector<std::unique_lock<std::mutex>> locks;
            locks.reserve(stripe_idxs.size());
            for (auto idx : stripe_idxs) locks.emplace_back(stripes_[idx]);

            resolved = Decide(entry, payload);

            core::QueueEntry out{
                .seq = 0,
                .appended_at = core::WallClock::now(),
                .payload = resolved,
            };
            auto append = queue_.Append(config_.shard, std::move(out), AdmitBy());
            if (!append.has_value()) {
              append_failures_.fetch_add(1, std::memory_order_relaxed);
              ABYSS_LOG_ERROR("resolver append failed",
                              {"shard", static_cast<int64_t>(config_.shard)},
                              {"seq", static_cast<uint64_t>(entry.seq)},
                              {"err", std::string_view{append.error().message()}});
              // No reply: the retry still decides X, so a failure reply
              // could be false. The engine's timeout reports it pending.
              return false;
            }
            resolved_seq = append->seq;
            conditionals_resolved_.fetch_add(1, std::memory_order_relaxed);

            if (resolved.decision == core::Decision::kApply) {
              decisions_apply_.fetch_add(1, std::memory_order_relaxed);
            } else {
              decisions_skip_.fetch_add(1, std::memory_order_relaxed);
            }

            UpdateCacheFromResolved(entry.seq, entry.appended_at, resolved);
          }

          // Track the highest Resolved seq this resolver has emitted; the
          // steady-state commit clamp (Run) holds the persisted offset behind any
          // Conditional whose Resolved is not yet durable (XDUR-2). Resolveds
          // are appended monotonically after their Conditionals, so this is the
          // durability target the durable-floor advances behind.
          AdvanceMaxSeq(highest_emitted_resolved_seq_, resolved_seq);

          // Two independent waits, durability FIRST (invariant 3). A
          // durable-layer failure takes precedence over an apply timeout
          // (mirrors the write path, tiering_engine.cpp). The client conditional
          // ack must land only after the Resolved is durable AND hot-applied; on
          // EITHER wait failing the client gets an error, never the success
          // value — the write stays durable in the WAL and applies on catch-up.
          if (!AwaitResolvedDurable(resolved_seq, config_.durable_wait_timeout)) {
            (void)rpc_.Fulfill(
                client_rpc_id,
                core::RespValue::Error(core::ErrorPrefix::kErr,
                                       "conditional write durable wait exceeded timeout; will "
                                       "apply on consumer catch-up"));
            return true;
          }

          // Block until hot applies — required for read-your-write. On timeout
          // the write stays durable and applies on catch-up, but the client
          // must NOT see success (mirrors the unconditional write path).
          if (WaitForHotApply(resolved_seq, config_.hot_apply_wait)) {
            (void)rpc_.Fulfill(client_rpc_id, std::move(resolved.return_value));
          } else {
            (void)rpc_.Fulfill(
                client_rpc_id,
                core::RespValue::Error(core::ErrorPrefix::kErr,
                                       "write durable in queue but consumer did not apply within "
                                       "timeout"));
          }
        } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
          UpdateCacheFromResolved(entry.seq, entry.appended_at, payload);
        }
        return true;
      },
      entry.payload);
}

core::Result<void> Resolver::SeedCursor() {
  auto committed = queue_.CommittedOffset(core::kResolverConsumer, config_.shard);
  if (!committed.has_value()) return std::unexpected(committed.error());
  committed_ = *committed;
  next_read_seq_ = committed_.has_value() ? *committed_ + 1 : 0;
  if (committed_.has_value()) {
    AdvanceMaxSeq(latest_drained_seq_, *committed_);
    last_commit_seq_.store(*committed_, std::memory_order_release);
  }
  cursor_seeded_ = true;
  return {};
}

void Resolver::Commit(core::SequenceId seq) {
  if (auto commit = queue_.CommitOffset(core::kResolverConsumer, config_.shard, seq);
      !commit.has_value()) {
    commit_failures_.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  committed_ = seq;
  last_commit_seq_.store(seq, std::memory_order_release);
}

void Resolver::QueueFloor(core::SequenceId drained, core::SequenceId durable_target) {
  constexpr size_t kMaxPendingFloors = 64;
  if (!pending_floors_.empty() &&
      (pending_floors_.back().drained >= drained || pending_floors_.size() == kMaxPendingFloors)) {
    // Coalescing only delays the floor: the later target covers both.
    auto& back = pending_floors_.back();
    back.drained = std::max(back.drained, drained);
    back.durable_target = std::max(back.durable_target, durable_target);
    return;
  }
  pending_floors_.push_back({.drained = drained, .durable_target = durable_target});
}

void Resolver::AdvanceFloor() {
  if (pending_floors_.empty()) return;
  const auto end = queue_.DurableEnd(config_.shard, core::Durability::kPowerLoss);
  if (!end.has_value()) return;
  bool advanced = false;
  while (!pending_floors_.empty() && pending_floors_.front().durable_target < *end) {
    AdvanceMaxSeq(resolver_durable_floor_, pending_floors_.front().drained);
    pending_floors_.pop_front();
    advanced = true;
  }
  // The commit never passes the durable floor. The floor's 0 is ambiguous
  // until one advance is confirmed, so the first commit waits for that.
  const auto target = resolver_durable_floor_.load(std::memory_order_acquire);
  if (committed_.has_value() ? target > *committed_ : advanced) Commit(target);
}

void Resolver::FailOutOfRange(core::SequenceId requested) {
  const auto first = queue_.FirstSeq(config_.shard);
  const std::string first_text = first.has_value() ? std::to_string(*first) : "unknown";
  ABYSS_LOG_CRITICAL("resolver read below the first retained WAL seq",
                     {"consumer", std::string_view{"resolver"}},
                     {"shard", static_cast<int64_t>(config_.shard)},
                     {"requested_seq", static_cast<uint64_t>(requested)},
                     {"first_seq", std::string_view{first_text}});
  core::Fatal("resolver on shard " + std::to_string(config_.shard) + " read seq " +
              std::to_string(requested) + " below first retained seq " + first_text +
              ": WAL entries above its persisted offset were reclaimed");
}

void Resolver::Run() {
  ABYSS_LOG_DEBUG("resolver started", {"shard", static_cast<int64_t>(config_.shard)});

  while (!cursor_seeded_ && !stop_requested_.load(std::memory_order_acquire)) {
    if (auto seeded = SeedCursor(); !seeded.has_value()) {
      ABYSS_LOG_WARN("resolver could not read its committed offset; retrying",
                     {"shard", static_cast<int64_t>(config_.shard)},
                     {"err", std::string_view{seeded.error().message()}});
      std::this_thread::sleep_for(config_.read_timeout);
    }
  }

  auto append_backoff = kAppendRetryInitialBackoff;
  while (!stop_requested_.load(std::memory_order_acquire)) {
    auto read = queue_.Read(config_.shard, next_read_seq_, config_.read_batch_size,
                            config_.read_timeout, queue_.AckDurability());
    if (!read.has_value()) {
      if (read.error().code() == core::ErrorCode::kUnavailable) {
        ABYSS_LOG_WARN("resolver stopping: queue unavailable",
                       {"shard", static_cast<int64_t>(config_.shard)});
        return;
      }
      if (read.error().code() == core::ErrorCode::kOutOfRange) FailOutOfRange(next_read_seq_);
      continue;
    }
    // The cursor passes an entry only once it is processed, so an entry
    // is decided at most once whether or not the commit below advances.
    // A failed Resolved append holds the cursor at X and ends the batch.
    const core::SequenceId batch_start = next_read_seq_;
    bool retry = false;
    for (const auto& entry : *read) {
      if (!ProcessEntry(entry)) {
        retry = true;
        break;
      }
      next_read_seq_ = entry.seq + 1;
      latest_drained_seq_.store(entry.seq, std::memory_order_release);
    }
    if (next_read_seq_ > 0 && (!committed_.has_value() || *committed_ + 1 < next_read_seq_)) {
      // Kafka HW vs LEO: the cursor is read progress (log-end); the committed
      // offset is the high-watermark and must never outrun the durable tail. A
      // Conditional at X may have emitted a Resolved at Y > X that is
      // published+hot-applied+client-OK'd but not yet fsynced; committing past
      // X then would let a crash lose Y while recovery resumes past X and never
      // re-decides it (XDUR-2). So we commit a Conditional X only once every
      // Resolved emitted for Conditionals <= X is durable. The durability
      // target is max(drained, highest emitted Resolved seq): Resolveds sit at
      // seqs > their Conditionals, so confirming the highest emitted Resolved
      // is durable also satisfies the fail-closed CommitOffset gate.
      // The check is pipelined: decisions never wait on a device flush,
      // and the floor trails the drained position by about one flush.
      const auto drained = next_read_seq_ - 1;
      QueueFloor(drained,
                 std::max(drained, highest_emitted_resolved_seq_.load(std::memory_order_acquire)));
    }
    AdvanceFloor();
    cache_.SweepExpired();

    if (!retry || next_read_seq_ != batch_start) append_backoff = kAppendRetryInitialBackoff;
    if (retry) {
      std::unique_lock lock(stop_mu_);
      stop_cv_.wait_for(lock, append_backoff,
                        [this] { return stop_requested_.load(std::memory_order_acquire); });
      append_backoff = std::min(append_backoff * 2, kAppendRetryMaxBackoff);
    }
  }

  ABYSS_LOG_DEBUG("resolver stopped", {"shard", static_cast<int64_t>(config_.shard)});
}

bool Resolver::WaitForHotApply(core::SequenceId seq, std::chrono::milliseconds timeout) {
  auto fut = apply_notifier_.AwaitApplied(config_.shard, seq);
  if (fut.wait_for(timeout) == std::future_status::ready) {
    try {
      fut.get();
      return true;
    } catch (const std::future_error&) {
      return false;
    }
  }
  apply_wait_timeouts_.fetch_add(1, std::memory_order_relaxed);
  apply_notifier_.Cancel(config_.shard, seq);
  ABYSS_LOG_WARN("resolver hot-apply wait timeout", {"shard", static_cast<int64_t>(config_.shard)},
                 {"seq", static_cast<uint64_t>(seq)});
  return false;
}

bool Resolver::AwaitResolvedDurable(core::SequenceId resolved_seq,
                                    std::chrono::milliseconds timeout) {
  auto durable = queue_.AwaitDurable(config_.shard, resolved_seq, queue_.AckDurability(), timeout);
  if (durable.has_value() && *durable) return true;
  durable_wait_timeouts_.fetch_add(1, std::memory_order_relaxed);
  ABYSS_LOG_WARN("resolver resolved-durable wait timeout",
                 {"shard", static_cast<int64_t>(config_.shard)},
                 {"seq", static_cast<uint64_t>(resolved_seq)});
  return false;
}

// ---------------------------------------------------------------------------
// Cache updates from log entries
// ---------------------------------------------------------------------------

namespace {

// Marks `key` present in the cache as a collection of `type`, preserving any
// existing absolute TTL, and purges stale members/fields if the prior cached
// type differed (a type change invalidates the old member/field index).
void UpsertCollectionKey(ExistenceCache& cache, std::string_view key, ExistenceCache::KeyType type,
                         core::SequenceId seq) {
  auto cur = cache.GetKey(key);
  if (cur.has_value() && cur->exists && cur->type != type) {
    cache.RemoveMembersAndFields(key);
  }
  cache.UpsertKey(key, ExistenceCache::KeyMeta{
                           .exists = true,
                           .type = type,
                           .abs_ttl_ms = cur.has_value() ? cur->abs_ttl_ms : 0,
                           .latest_seq = seq,
                           .string_value = std::nullopt,
                       });
}

}  // namespace

void Resolver::ApplyToCache(core::SequenceId seq, core::WallTime appended_at,
                            const core::RespCommand& cmd) {
  if (cmd.args.empty()) return;
  const auto name = AsciiUpper(cmd.args[0]);
  auto op = core::ops::ParseWriteOp(name, cmd, WallMs(appended_at));
  // The cache is a hint-only layer; an unparseable write is the hot store's
  // authoritative parse error to report. Skip cache maintenance silently.
  if (!op.has_value()) return;

  std::visit(
      [&](const auto& w) {
        using T = std::decay_t<decltype(w)>;
        if constexpr (std::is_same_v<T, core::ops::StringSet>) {
          if (auto cur = cache_.GetKey(w.key);
              cur.has_value() && cur->exists && cur->type != ExistenceCache::KeyType::kString) {
            cache_.RemoveMembersAndFields(w.key);
          }
          cache_.UpsertKey(w.key, ExistenceCache::KeyMeta{
                                      .exists = true,
                                      .type = ExistenceCache::KeyType::kString,
                                      .abs_ttl_ms = w.abs_ttl_ms,
                                      .latest_seq = seq,
                                      .string_value = std::string(w.value),
                                  });
        } else if constexpr (std::is_same_v<T, core::ops::Del>) {
          for (const auto& key : w.keys) {
            cache_.RemoveMembersAndFields(key);
            cache_.TombstoneKey(key, seq);
          }
        } else if constexpr (std::is_same_v<T, core::ops::Expire>) {
          // EXPIRE/PERSIST never materialise a key from nothing.
          auto cur = cache_.GetKey(w.key);
          if (!cur.has_value() || !cur->exists) return;
          cur->abs_ttl_ms = w.abs_ttl_ms;
          cur->latest_seq = seq;
          cache_.UpsertKey(w.key, std::move(*cur));
        } else if constexpr (std::is_same_v<T, core::ops::Persist>) {
          auto cur = cache_.GetKey(w.key);
          if (!cur.has_value() || !cur->exists) return;
          cur->abs_ttl_ms = 0;
          cur->latest_seq = seq;
          cache_.UpsertKey(w.key, std::move(*cur));
        } else if constexpr (std::is_same_v<T, core::ops::SetAdd>) {
          UpsertCollectionKey(cache_, w.key, ExistenceCache::KeyType::kSet, seq);
          for (const auto& member : w.members) {
            cache_.UpsertMember(w.key, member,
                                ExistenceCache::MemberMeta{.score = 0.0, .latest_seq = seq});
          }
        } else if constexpr (std::is_same_v<T, core::ops::SetRem>) {
          for (const auto& member : w.members) cache_.RemoveMember(w.key, member);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetAdd>) {
          UpsertCollectionKey(cache_, w.key, ExistenceCache::KeyType::kZset, seq);
          for (const auto& e : w.entries) {
            cache_.UpsertMember(w.key, e.member,
                                ExistenceCache::MemberMeta{.score = e.score, .latest_seq = seq});
          }
        } else if constexpr (std::is_same_v<T, core::ops::ZsetRem>) {
          for (const auto& member : w.members) cache_.RemoveMember(w.key, member);
        } else if constexpr (std::is_same_v<T, core::ops::HashSet> ||
                             std::is_same_v<T, core::ops::HashMSet>) {
          UpsertCollectionKey(cache_, w.key, ExistenceCache::KeyType::kHash, seq);
          for (const auto& fv : w.fields) {
            cache_.UpsertField(
                w.key, fv.field,
                ExistenceCache::FieldMeta{
                    .value = std::string(fv.value), .value_known = true, .latest_seq = seq});
          }
        } else if constexpr (std::is_same_v<T, core::ops::HashDel>) {
          for (const auto& field : w.fields) cache_.RemoveField(w.key, field);
        }
      },
      *op);
}

void Resolver::UpdateCacheFromResolved(core::SequenceId seq, core::WallTime appended_at,
                                       const core::entry::Resolved& resolved) {
  if (resolved.decision != core::Decision::kApply) return;
  for (const auto& cmd : resolved.materialised_ops) {
    ApplyToCache(seq, appended_at, cmd);
  }
}

// ---------------------------------------------------------------------------
// Recovery replay
// ---------------------------------------------------------------------------

core::Result<void> Resolver::ReplayForRecovery(const std::atomic<bool>& cancel) {
  ABYSS_LOG_INFO("resolver replay starting", {"shard", static_cast<int64_t>(config_.shard)});

  // Gates `cache_only_after_miss` in Decide; cold replay runs after this.
  replay_mode_.store(true, std::memory_order_release);
  latest_flush_seq_.store(0, std::memory_order_release);
  struct ReplayGuard {
    std::atomic<bool>& flag;
    explicit ReplayGuard(std::atomic<bool>& f) : flag(f) {}
    ReplayGuard(const ReplayGuard&) = delete;
    ReplayGuard& operator=(const ReplayGuard&) = delete;
    ReplayGuard(ReplayGuard&&) = delete;
    ReplayGuard& operator=(ReplayGuard&&) = delete;
    ~ReplayGuard() { flag.store(false, std::memory_order_release); }
  };
  ReplayGuard guard(replay_mode_);

  // Every scan starts at the committed offset, which never passes a dangling
  // Conditional, so a retried replay sees the same danglings again.
  if (auto seeded = SeedCursor(); !seeded.has_value()) return std::unexpected(seeded.error());

  std::unordered_map<core::SequenceId, core::QueueEntry> dangling;
  // Highest seq of any Resolved this replay re-emits (pre-flush Skips and
  // terminal dangling re-decisions). Each commit barrier (HOTC-5) awaits
  // this seq's WAL fsync before advancing the recovery offset past the
  // danglings, so cold/hot never replay a non-durable re-emitted Resolved.
  core::SequenceId highest_reemitted_seq = 0;
  // Highest re-emitted seq a per-scan barrier already confirmed durable.
  core::SequenceId awaited_reemitted_seq = 0;
  // On timeout the offset stays put and kUnavailable retries the shard.
  const auto await_durable = [this](core::SequenceId barrier) -> core::Result<void> {
    auto durable = queue_.AwaitDurable(config_.shard, barrier, core::Durability::kPowerLoss,
                                       config_.durable_wait_timeout);
    if (durable.has_value() && *durable) return {};
    durable_wait_timeouts_.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_WARN("resolver recovery durability barrier timed out; offset left clamped",
                   {"shard", static_cast<int64_t>(config_.shard)},
                   {"barrier_seq", static_cast<uint64_t>(barrier)});
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable,
                                       "resolver recovery: scanned or re-emitted log not durable"});
  };
  while (true) {
    if (cancel.load(std::memory_order_acquire)) {
      ABYSS_LOG_WARN("resolver replay cancelled during scan",
                     {"shard", static_cast<int64_t>(config_.shard)});
      return std::unexpected(
          core::Error{core::ErrorCode::kUnavailable, "resolver replay cancelled"});
    }
    auto batch = queue_.Read(config_.shard, next_read_seq_, config_.replay_batch_size,
                             core::Duration{50}, queue_.AckDurability());
    if (!batch.has_value()) {
      if (batch.error().code() == core::ErrorCode::kOutOfRange) FailOutOfRange(next_read_seq_);
      break;
    }
    if (batch->empty()) break;

    for (const auto& entry : *batch) {
      next_read_seq_ = entry.seq + 1;
      std::visit(
          [&](const auto& payload) {
            using T = std::decay_t<decltype(payload)>;
            if constexpr (std::is_same_v<T, core::entry::Write>) {
              ApplyToCache(entry.seq, entry.appended_at, payload.cmd);
            } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
              dangling.emplace(entry.seq, entry);
            } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
              dangling.erase(payload.ref);
              UpdateCacheFromResolved(entry.seq, entry.appended_at, payload);
            } else if constexpr (std::is_same_v<T, core::entry::Flush>) {
              HandleFlush(entry);
              // Emit Skip Resolveds for pre-Flush danglings so hot/cold's
              // block-and-scan can advance past them.
              std::vector<core::SequenceId> to_emit;
              to_emit.reserve(dangling.size());
              for (const auto& [d_seq, _] : dangling) {
                if (d_seq < entry.seq) to_emit.push_back(d_seq);
              }
              for (auto d_seq : to_emit) {
                const auto& d_entry = dangling.at(d_seq);
                core::QueueEntry out{
                    .seq = 0,
                    .appended_at = d_entry.appended_at,
                    .payload = MakeSkip(d_seq, core::RespValue::Null()),
                };
                auto append = queue_.Append(config_.shard, std::move(out), AdmitBy());
                if (!append.has_value()) {
                  append_failures_.fetch_add(1, std::memory_order_relaxed);
                  ABYSS_LOG_ERROR("resolver pre-flush skip append failed",
                                  {"shard", static_cast<int64_t>(config_.shard)},
                                  {"ref", static_cast<uint64_t>(d_seq)},
                                  {"err", std::string_view{append.error().message()}});
                  continue;
                }
                highest_reemitted_seq = std::max(highest_reemitted_seq, append->seq);
                flush_skip_resolveds_emitted_.fetch_add(1, std::memory_order_relaxed);
                dangling.erase(d_seq);
              }
            }
          },
          entry.payload);
      latest_drained_seq_.store(entry.seq, std::memory_order_release);
    }

    // Low-water-mark commit past what this scan absorbed, but never past a
    // dangling Conditional. Holding the offset behind any unresolved
    // Conditional preserves replay correctness across a crash mid-recovery: a
    // dangling Conditional whose Resolved we have not yet emitted is rescanned
    // by the next ReplayForRecovery.
    core::SequenceId commit_to = next_read_seq_ - 1;
    bool committable = true;
    if (!dangling.empty()) {
      core::SequenceId oldest_dangling = std::numeric_limits<core::SequenceId>::max();
      for (const auto& [seq, _] : dangling) {
        oldest_dangling = std::min(oldest_dangling, seq);
      }
      committable = oldest_dangling > 0;
      if (committable) commit_to = std::min(commit_to, oldest_dangling - 1);
    }
    if (committable && (!committed_.has_value() || commit_to > *committed_)) {
      // A pre-flush Skip erased its dangling: it must be durable before the
      // commit passes that Conditional.
      if (highest_reemitted_seq > awaited_reemitted_seq) {
        if (auto barrier = await_durable(highest_reemitted_seq); !barrier.has_value()) {
          return barrier;
        }
        awaited_reemitted_seq = highest_reemitted_seq;
      }
      Commit(commit_to);
    }
  }

  std::vector<core::SequenceId> sorted;
  sorted.reserve(dangling.size());
  for (const auto& [seq, _] : dangling) sorted.push_back(seq);
  std::ranges::sort(sorted);
  for (auto seq : sorted) {
    if (cancel.load(std::memory_order_acquire)) {
      ABYSS_LOG_WARN("resolver replay cancelled during dangling emit",
                     {"shard", static_cast<int64_t>(config_.shard)});
      return std::unexpected(
          core::Error{core::ErrorCode::kUnavailable, "resolver replay cancelled"});
    }
    const auto& entry = dangling.at(seq);
    const auto& cond = std::get<core::entry::Conditional>(entry.payload);
    auto resolved = Decide(entry, cond);
    core::QueueEntry out{
        .seq = 0,
        .appended_at = entry.appended_at,
        .payload = resolved,
    };
    auto append = queue_.Append(config_.shard, std::move(out), AdmitBy());
    if (!append.has_value()) {
      ABYSS_LOG_ERROR(
          "resolver replay append failed", {"shard", static_cast<int64_t>(config_.shard)},
          {"seq", static_cast<uint64_t>(seq)}, {"err", std::string_view{append.error().message()}});
      return std::unexpected(append.error());
    }
    highest_reemitted_seq = std::max(highest_reemitted_seq, append->seq);
    UpdateCacheFromResolved(seq, entry.appended_at, resolved);
    replayed_resolveds_emitted_.fetch_add(1, std::memory_order_relaxed);
  }

  // HOTC-5 recovery barrier: the terminal commit jumps past the danglings
  // (which sit below the cursor) to the end of the scan. Before the offset
  // passes a Conditional, its Resolved MUST be durable: otherwise a crash
  // after the offset persists but before the Resolved fsync loses both the
  // Conditional (committed past, never re-read) and its Resolved, leaving
  // it permanently unresolved. So await everything scanned (a retried
  // replay scans Resolveds an earlier attempt appended) and everything
  // re-emitted. On timeout the offset stays at the per-scan low-water clamp
  // and kUnavailable makes the RecoveryCoordinator retry the shard.
  if (next_read_seq_ > 0) {
    const core::SequenceId scanned = next_read_seq_ - 1;
    if (auto barrier = await_durable(std::max(scanned, highest_reemitted_seq));
        !barrier.has_value()) {
      return barrier;
    }
    if (!committed_.has_value() || scanned > *committed_) Commit(scanned);
  }

  ABYSS_LOG_INFO("resolver replay complete", {"shard", static_cast<int64_t>(config_.shard)},
                 {"dangling_emitted", static_cast<uint64_t>(sorted.size())},
                 {"cache_entries", static_cast<uint64_t>(cache_.Size())});
  return {};
}

}  // namespace abyss::consumer
