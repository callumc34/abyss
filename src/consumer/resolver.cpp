#include "abyss/consumer/resolver.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <future>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/fire_and_forget.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_format.h"
#include "abyss/core/shard_router.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.resolver")

namespace abyss::consumer {

namespace {

std::string AsciiUpper(std::string_view s) {
  std::string out(s);
  for (auto& c : out) {
    if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 'a' + 'A');
  }
  return out;
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

void Resolver::RequestStop() { stop_requested_.store(true, std::memory_order_release); }

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
  s.append_failures = append_failures_.load(std::memory_order_relaxed);
  s.parse_failures = parse_failures_.load(std::memory_order_relaxed);
  s.replayed_resolveds_emitted = replayed_resolveds_emitted_.load(std::memory_order_relaxed);
  s.latest_drained_seq = latest_drained_seq_.load(std::memory_order_relaxed);
  s.last_ack_seq = last_ack_seq_.load(std::memory_order_relaxed);
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
                  std::atomic<uint64_t>& cold_errors, uint64_t now_ms) {
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
    std::atomic<uint64_t>& cold_errors, bool& definitive) {
  definitive = true;
  if (auto cached = cache.GetMember(key, member); cached.has_value()) {
    cache_hits.fetch_add(1, std::memory_order_relaxed);
    return cached->score;
  }
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
    std::atomic<uint64_t>& cold_errors, bool& definitive, bool& exists) {
  definitive = true;
  exists = false;
  if (auto cached = cache.GetField(key, field); cached.has_value()) {
    cache_hits.fetch_add(1, std::memory_order_relaxed);
    exists = cached->value_known || !cached->value.empty();
    if (cached->value_known) return cached->value;
    return std::nullopt;
  }
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

SetParse ParseSetArgs(const core::RespCommand& cmd, uint64_t now_ms) {
  SetParse out;
  if (cmd.args.size() < 3) {
    out.error = "wrong number of arguments for 'SET'";
    return out;
  }
  out.key = cmd.args[1];
  out.value = cmd.args[2];
  for (size_t i = 3; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "NX") {
      out.nx = true;
    } else if (opt == "XX") {
      out.xx = true;
    } else if (opt == "GET") {
      out.get = true;
    } else if (opt == "KEEPTTL") {
      out.keep_ttl = true;
    } else if (opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT") {
      if (i + 1 >= cmd.args.size()) {
        out.error = "syntax error after '" + opt + "'";
        return out;
      }
      uint64_t v = 0;
      if (!ParseUint64(cmd.args[++i], v)) {
        out.error = "value is not an integer";
        return out;
      }
      if (opt == "EX")
        out.abs_ttl_ms = now_ms + (v * 1000);
      else if (opt == "PX")
        out.abs_ttl_ms = now_ms + v;
      else if (opt == "EXAT")
        out.abs_ttl_ms = v * 1000;
      else
        out.abs_ttl_ms = v;
    } else {
      out.error = "syntax error: unknown SET option '" + opt + "'";
      return out;
    }
  }
  if (out.nx && out.xx) {
    out.error = "syntax error — NX and XX are mutually exclusive";
    return out;
  }
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
            : ParseSetArgs(cmd, now_ms);
    if (!parsed.valid) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr, parsed.error));
    }
    KeyView view = LookupKey(cache_, buffer_router_, cold_, deadline, parsed.key, cache_hits_,
                             buffer_hits_, cold_hits_, cold_timeouts_, cold_errors_, now_ms);
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
                            cold_hits_, cold_timeouts_, cold_errors_, now_ms);
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
    bool nx = false;
    bool xx = false;
    bool gt = false;
    bool lt = false;
    bool ch = false;
    size_t i = 2;
    for (; i < cmd.args.size(); ++i) {
      const auto opt = AsciiUpper(cmd.args[i]);
      if (opt == "NX")
        nx = true;
      else if (opt == "XX")
        xx = true;
      else if (opt == "GT")
        gt = true;
      else if (opt == "LT")
        lt = true;
      else if (opt == "CH")
        ch = true;
      else
        break;
    }
    if ((nx && xx) || (gt && lt) || (nx && (gt || lt))) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "syntax error — incompatible ZADD flags"));
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
      auto existing =
          LookupZsetMemberScore(cache_, buffer_router_, cold_, deadline, key, p.member, cache_hits_,
                                buffer_hits_, cold_hits_, cold_timeouts_, cold_errors_, definitive);
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
    if (name == "EXPIRE")
      requested_abs_ttl = now_ms + (ttl_arg * 1000);
    else if (name == "PEXPIRE")
      requested_abs_ttl = now_ms + ttl_arg;
    else if (name == "EXPIREAT")
      requested_abs_ttl = ttl_arg * 1000;
    else
      requested_abs_ttl = ttl_arg;

    bool nx = false;
    bool xx = false;
    bool gt = false;
    bool lt = false;
    for (size_t j = 3; j < cmd.args.size(); ++j) {
      const auto opt = AsciiUpper(cmd.args[j]);
      if (opt == "NX") {
        nx = true;
      } else if (opt == "XX") {
        xx = true;
      } else if (opt == "GT") {
        gt = true;
      } else if (opt == "LT") {
        lt = true;
      } else {
        parse_failures_.fetch_add(1, std::memory_order_relaxed);
        return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                    "syntax error: unknown EXPIRE option"));
      }
    }
    if ((nx && xx) || (gt && lt) || (nx && (gt || lt))) {
      parse_failures_.fetch_add(1, std::memory_order_relaxed);
      return MakeSkip(seq, core::RespValue::Error(core::ErrorPrefix::kErr,
                                                  "syntax error — incompatible EXPIRE flags"));
    }

    const std::string_view key = cmd.args[1];
    KeyView view = LookupKey(cache_, buffer_router_, cold_, deadline, key, cache_hits_,
                             buffer_hits_, cold_hits_, cold_timeouts_, cold_errors_, now_ms);
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
    KeyView src_view = LookupKey(cache_, buffer_router_, cold_, deadline, src, cache_hits_,
                                 buffer_hits_, cold_hits_, cold_timeouts_, cold_errors_, now_ms);
    KeyView dst_view = LookupKey(cache_, buffer_router_, cold_, deadline, dst, cache_hits_,
                                 buffer_hits_, cold_hits_, cold_timeouts_, cold_errors_, now_ms);
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
    KeyView src_view = LookupKey(cache_, buffer_router_, cold_, deadline, src, cache_hits_,
                                 buffer_hits_, cold_hits_, cold_timeouts_, cold_errors_, now_ms);
    KeyView dst_view = LookupKey(cache_, buffer_router_, cold_, deadline, dst, cache_hits_,
                                 buffer_hits_, cold_hits_, cold_timeouts_, cold_errors_, now_ms);
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
                               exists);
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

void Resolver::ProcessEntry(const core::QueueEntry& entry) {
  std::visit(
      [this, &entry](const auto& payload) {
        using T = std::decay_t<decltype(payload)>;
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          UpdateCacheFromWrite(entry.seq, payload.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          conditionals_resolved_.fetch_add(1, std::memory_order_relaxed);

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
          auto stripe_idxs = StripeIndicesFor(keys);
          std::vector<std::unique_lock<std::mutex>> locks;
          locks.reserve(stripe_idxs.size());
          for (auto idx : stripe_idxs) locks.emplace_back(stripes_[idx]);

          auto resolved = Decide(entry, payload);

          core::QueueEntry out{
              .seq = 0,
              .appended_at = core::WallClock::now(),
              .payload = resolved,
          };
          auto append = queue_.Append(config_.shard, std::move(out));
          const core::RpcId client_rpc_id = core::MakeRpcId(config_.shard, entry.seq);
          if (!append.has_value()) {
            append_failures_.fetch_add(1, std::memory_order_relaxed);
            ABYSS_LOG_ERROR("resolver append failed",
                            {"shard", static_cast<int64_t>(config_.shard)},
                            {"seq", static_cast<uint64_t>(entry.seq)},
                            {"err", std::string_view{append.error().message()}});
            (void)rpc_.Fulfill(client_rpc_id,
                               core::RespValue::Error(core::ErrorPrefix::kErr,
                                                      "resolver could not append decision"));
            return;
          }

          if (resolved.decision == core::Decision::kApply) {
            decisions_apply_.fetch_add(1, std::memory_order_relaxed);
          } else {
            decisions_skip_.fetch_add(1, std::memory_order_relaxed);
          }

          UpdateCacheFromResolved(entry.seq, resolved);

          // Block until hot applies — required for read-your-write.
          (void)WaitForHotApply(append->seq);

          (void)rpc_.Fulfill(client_rpc_id, std::move(resolved.return_value));
        } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
          UpdateCacheFromResolved(entry.seq, payload);
        }
      },
      entry.payload);
}

void Resolver::Run() {
  ABYSS_LOG_DEBUG("resolver started", {"shard", static_cast<int64_t>(config_.shard)});

  while (!stop_requested_.load(std::memory_order_acquire)) {
    auto read = queue_.Read(core::kResolverConsumer, config_.shard, config_.read_batch_size,
                            config_.read_timeout);
    if (!read.has_value()) {
      if (read.error().code() == core::ErrorCode::kUnavailable) {
        ABYSS_LOG_WARN("resolver stopping: queue unavailable",
                       {"shard", static_cast<int64_t>(config_.shard)});
        return;
      }
      continue;
    }
    for (const auto& entry : *read) {
      ProcessEntry(entry);
      latest_drained_seq_.store(entry.seq, std::memory_order_release);
    }
    const auto drained = latest_drained_seq_.load(std::memory_order_acquire);
    const auto last = last_ack_seq_.load(std::memory_order_acquire);
    if (drained > last) {
      auto ack = queue_.Ack(core::kResolverConsumer, config_.shard, drained);
      if (ack.has_value()) {
        last_ack_seq_.store(drained, std::memory_order_release);
      }
    }
    cache_.SweepExpired();
  }

  ABYSS_LOG_DEBUG("resolver stopped", {"shard", static_cast<int64_t>(config_.shard)});
}

bool Resolver::WaitForHotApply(core::SequenceId seq) {
  const core::RpcId id = core::MakeRpcId(config_.shard, seq);
  auto fut = apply_notifier_.AwaitApplied(id);
  if (fut.wait_for(config_.hot_apply_wait) == std::future_status::ready) {
    try {
      fut.get();
      return true;
    } catch (const std::future_error&) {
      return false;
    }
  }
  apply_wait_timeouts_.fetch_add(1, std::memory_order_relaxed);
  apply_notifier_.Cancel(id);
  ABYSS_LOG_WARN("resolver hot-apply wait timeout", {"shard", static_cast<int64_t>(config_.shard)},
                 {"seq", static_cast<uint64_t>(seq)});
  return false;
}

// ---------------------------------------------------------------------------
// Cache updates from log entries
// ---------------------------------------------------------------------------

void Resolver::UpdateCacheFromWrite(core::SequenceId seq, const core::RespCommand& cmd) {
  if (cmd.args.empty()) return;
  const auto upper = AsciiUpper(cmd.args[0]);
  if (upper == "SET") {
    if (cmd.args.size() < 3) return;
    auto parsed = ParseSetArgs(cmd, /*now_ms=*/0);  // ttl extracted as-is
    if (!parsed.valid) return;
    cache_.UpsertKey(parsed.key, ExistenceCache::KeyMeta{
                                     .exists = true,
                                     .type = ExistenceCache::KeyType::kString,
                                     .abs_ttl_ms = parsed.abs_ttl_ms,
                                     .latest_seq = seq,
                                     .string_value = std::string(parsed.value),
                                 });
  } else if (upper == "SETEX" || upper == "PSETEX") {
    if (cmd.args.size() < 4) return;
    cache_.UpsertKey(cmd.args[1], ExistenceCache::KeyMeta{
                                      .exists = true,
                                      .type = ExistenceCache::KeyType::kString,
                                      .abs_ttl_ms = 0,
                                      .latest_seq = seq,
                                      .string_value = std::string(cmd.args[3]),
                                  });
  } else if (upper == "DEL" || upper == "UNLINK") {
    for (size_t i = 1; i < cmd.args.size(); ++i) {
      cache_.TombstoneKey(cmd.args[i], seq);
    }
  } else if (upper == "PERSIST") {
    if (cmd.args.size() < 2) return;
    auto cur = cache_.GetKey(cmd.args[1]);
    ExistenceCache::KeyMeta m = cur.has_value() ? *cur : ExistenceCache::KeyMeta{.exists = true};
    m.abs_ttl_ms = 0;
    m.latest_seq = seq;
    cache_.UpsertKey(cmd.args[1], std::move(m));
  } else if (upper == "EXPIRE" || upper == "PEXPIRE" || upper == "EXPIREAT" ||
             upper == "PEXPIREAT" || upper == "PEXPIREAT") {
    if (cmd.args.size() < 3) return;
    uint64_t v = 0;
    if (!ParseUint64(cmd.args[2], v)) return;
    auto cur = cache_.GetKey(cmd.args[1]);
    if (!cur.has_value()) return;  // don't materialize a key from EXPIRE alone
    cur->latest_seq = seq;
    if (upper == "EXPIRE")
      cur->abs_ttl_ms = WallMs(core::WallClock::now()) + (v * 1000);
    else if (upper == "PEXPIRE")
      cur->abs_ttl_ms = WallMs(core::WallClock::now()) + v;
    else if (upper == "EXPIREAT")
      cur->abs_ttl_ms = v * 1000;
    else
      cur->abs_ttl_ms = v;
    cache_.UpsertKey(cmd.args[1], *cur);
  }
  // Member-level cache is demand-driven; UpdateCacheFromResolved fills in
  // affected entries.
}

void Resolver::UpdateCacheFromResolved(core::SequenceId seq,
                                       const core::entry::Resolved& resolved) {
  if (resolved.decision != core::Decision::kApply) return;
  for (const auto& cmd : resolved.materialised_ops) {
    UpdateCacheFromWrite(seq, cmd);
    if (cmd.args.size() < 2) continue;
    const auto upper = AsciiUpper(cmd.args[0]);
    if (upper == "ZADD") {
      // Update tracked members.
      for (size_t i = 2; i + 1 < cmd.args.size(); i += 2) {
        double score = 0.0;
        if (!ParseDouble(cmd.args[i], score)) continue;
        cache_.UpsertMember(cmd.args[1], cmd.args[i + 1],
                            ExistenceCache::MemberMeta{.score = score, .latest_seq = seq});
      }
    } else if (upper == "HSET") {
      for (size_t i = 2; i + 1 < cmd.args.size(); i += 2) {
        cache_.UpsertField(
            cmd.args[1], cmd.args[i],
            ExistenceCache::FieldMeta{
                .value = std::string(cmd.args[i + 1]), .value_known = true, .latest_seq = seq});
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Recovery replay
// ---------------------------------------------------------------------------

core::Result<void> Resolver::ReplayForRecovery(const std::atomic<bool>& cancel) {
  ABYSS_LOG_INFO("resolver replay starting", {"shard", static_cast<int64_t>(config_.shard)});

  std::unordered_map<core::SequenceId, core::QueueEntry> dangling;
  core::SequenceId highest_seen = 0;
  while (true) {
    if (cancel.load(std::memory_order_acquire)) {
      ABYSS_LOG_WARN("resolver replay cancelled during scan",
                     {"shard", static_cast<int64_t>(config_.shard)});
      return std::unexpected(
          core::Error{core::ErrorCode::kUnavailable, "resolver replay cancelled"});
    }
    auto batch = queue_.Read(core::kResolverConsumer, config_.shard, config_.replay_batch_size,
                             core::Duration{50});
    if (!batch.has_value()) break;
    if (batch->empty()) break;

    bool any_new = false;
    for (const auto& entry : *batch) {
      // Read returns from the persisted ack offset; once the consumer has
      // dangling Conditionals we cannot ack past, subsequent Reads will
      // re-emit entries we've already absorbed. Skip them.
      if (entry.seq <= highest_seen) continue;
      any_new = true;
      std::visit(
          [&](const auto& payload) {
            using T = std::decay_t<decltype(payload)>;
            if constexpr (std::is_same_v<T, core::entry::Write>) {
              UpdateCacheFromWrite(entry.seq, payload.cmd);
            } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
              dangling.emplace(entry.seq, entry);
            } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
              dangling.erase(payload.ref);
              UpdateCacheFromResolved(entry.seq, payload);
            }
          },
          entry.payload);
      highest_seen = entry.seq;
      latest_drained_seq_.store(entry.seq, std::memory_order_release);
    }
    if (!any_new) break;

    // Low-water-mark ack so the next Read advances past entries we've absorbed,
    // but never past a dangling Conditional. Holding the offset behind any
    // unresolved Conditional preserves replay correctness across a crash mid-
    // recovery: a dangling Conditional whose Resolved we have not yet emitted
    // stays in the queue's unread range and gets re-processed on the next
    // ReplayForRecovery.
    core::SequenceId ack_to = highest_seen;
    if (!dangling.empty()) {
      core::SequenceId oldest_dangling = std::numeric_limits<core::SequenceId>::max();
      for (const auto& [seq, _] : dangling) {
        oldest_dangling = std::min(oldest_dangling, seq);
      }
      if (oldest_dangling > 0 && oldest_dangling - 1 < ack_to) {
        ack_to = oldest_dangling - 1;
      } else if (oldest_dangling == 0) {
        ack_to = 0;
      }
    }
    if (ack_to > last_ack_seq_.load(std::memory_order_acquire)) {
      core::FireAndForget(queue_.Ack(core::kResolverConsumer, config_.shard, ack_to),
                          append_failures_);
      last_ack_seq_.store(ack_to, std::memory_order_release);
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
    auto append = queue_.Append(config_.shard, std::move(out));
    if (!append.has_value()) {
      ABYSS_LOG_ERROR(
          "resolver replay append failed", {"shard", static_cast<int64_t>(config_.shard)},
          {"seq", static_cast<uint64_t>(seq)}, {"err", std::string_view{append.error().message()}});
      return std::unexpected(append.error());
    }
    UpdateCacheFromResolved(seq, resolved);
    replayed_resolveds_emitted_.fetch_add(1, std::memory_order_relaxed);
  }

  const auto drained = latest_drained_seq_.load(std::memory_order_acquire);
  if (drained > 0) {
    core::FireAndForget(queue_.Ack(core::kResolverConsumer, config_.shard, drained),
                        append_failures_);
    last_ack_seq_.store(drained, std::memory_order_release);
  }

  ABYSS_LOG_INFO("resolver replay complete", {"shard", static_cast<int64_t>(config_.shard)},
                 {"dangling_emitted", static_cast<uint64_t>(sorted.size())},
                 {"cache_entries", static_cast<uint64_t>(cache_.Size())});
  return {};
}

}  // namespace abyss::consumer
