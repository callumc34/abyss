// Concurrent clients on one overlapping keyspace, checked for
// linearizability against the reference model (Wing-Gong search with
// Lowe's cache, as Porcupine runs it). Keys come in fixed groups of
// three that span shards on one log; every multi-key write stays in its
// group, so groups are independent objects and each group's history is
// checked alone against the model restricted to it. MGET is per key
// and not atomic (#170): each of its keys is a single-key read within
// the MGET's window.
//
// Each client is a connection: it pipelines its commands at random
// depth through the request pipeline and the engine's async dispatch,
// driving its own events, deadlines and orphans as a reactor does. A
// command's window runs from the feed that dispatched it to its reply.
// The check: within a connection, each op takes effect after the one
// before it; across connections, each group is linearizable, with
// real-time order and nothing more. ECHOs in the stream check that
// replies leave in command order. Across groups, message passing: a
// reader that sees a connection's write to one key, then reads a key
// that connection wrote before, sees that write or a later one. Every
// value is written once, so a read names the write it saw. Some
// batches are message-passing shapes on two keys of their own.
//
// The fake wall clock is frozen for each burst of operations and moved
// only with every client joined, past TTLs, so a burst's first touches
// race on observed expiries. A drainer absorbs and flushes cold, a
// maintainer evicts, and loads are slowed, raced by evictions, or held
// until a command waiting on them is given up at its deadline, which
// each seed sets short.
//
// Replies are classified by exact text. A timeout after the write was
// logged (the durable wait) is indeterminate: it took effect inside its
// window or not at all. A timeout before anything was reserved, or
// -OOM, is a no-op. Any other error fails the test.
//
// ABYSS_LINEARIZABILITY_SEED=<n> replays a seed's configuration and
// stream (the interleaving is the scheduler's); the replay line names
// the threads and ops to pass with it. Long tier:
// ABYSS_LINEARIZABILITY_SEEDS=<count>
// [ABYSS_LINEARIZABILITY_THREADS=<n>]
// [ABYSS_LINEARIZABILITY_OPS=<per thread>].

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <latch>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "abyss/core/ascii.h"
#include "abyss/core/command_dispatcher.h"
#include "abyss/core/completion.h"
#include "abyss/core/resp_types.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/orphans.h"
#include "abyss/resp/parser.h"
#include "abyss/resp/request_pipeline.h"
#include "abyss/resp/serializer.h"
#include "event_inbox.h"
#include "linearizability.h"
#include "on_exit.h"
#include "property_harness.h"

namespace abyss::engine {
namespace {

using abyss::testing::HistoryOp;

constexpr int kGroups = 6;
constexpr int kGroupKeys = 3;
constexpr int kBursts = 5;
// Per command slot of a pipelined batch: a message-passing shape.
constexpr double kMessagePassing = 0.2;

struct Record {
  int client = 0;
  uint64_t call = 0;
  uint64_t ret = 0;
  std::vector<std::string> args;
  core::RespValue reply;
  int64_t at_ms = 0;
  // Its place in its client's commands.
  uint64_t order = 0;
};

struct Outcome {
  uint64_t ops = 0;
  // Ops whose window overlaps another op's on the same group.
  uint64_t concurrent = 0;
  std::map<std::string, uint64_t> indeterminate;
  uint64_t no_ops = 0;
  // Of them, refused outright (-OOM, the window), not timed out.
  uint64_t refused = 0;
  std::map<std::string, uint64_t> no_op_texts;
  uint64_t explored = 0;
  // Pending commands given up at their deadline, and of them those
  // whose task still had work out, kept as orphans until it landed.
  uint64_t abandoned = 0;
  uint64_t orphaned = 0;
  // Loads held, and holds that ran to their bound.
  uint64_t held = 0;
  uint64_t hold_bounds = 0;
  // Events for no live or orphaned command.
  uint64_t late = 0;
  bool ok = true;
  std::string failure;
  // Message passing across groups: the pairs judged, and what broke.
  uint64_t cross_pairs = 0;
  std::string cross_failure;
};

// Loads held on the I/O pool once read, so commands really pass their
// deadlines with work still out: the first few, then at random. A hold
// on a key lets go once a client's deadline passes on a command naming
// it; at any client's deadline once older than a command's timeout, as
// a background fill's is; when the burst ends; or at a bound no run
// should reach. A key whose command was just given up is not held:
// the load may be that command's, and nothing would let it go.
class Holds {
 public:
  void Open(size_t first, double chance, std::chrono::milliseconds timeout) {
    const std::scoped_lock lock(mu_);
    first_ = first;
    chance_ = chance;
    timeout_ = timeout;
    open_ = true;
  }
  void Close() {
    const std::scoped_lock lock(mu_);
    open_ = false;
    cv_.notify_all();
  }

  // On a pool thread, once a load of `key` has read.
  void MaybeHold(std::string_view key, Rng& rng) {
    std::unique_lock lock(mu_);
    if (!open_) return;
    const auto since = core::SteadyClock::now();
    // Map nodes stay put as others are added.
    const Key& state = keys_[std::string(key)];
    if (state.released_at + timeout_ > since) return;
    if (first_ > 0) {
      --first_;
    } else if (!rng.Chance(chance_)) {
      return;
    }
    ++held_;
    const uint64_t at = state.released;
    if (!cv_.wait_for(lock, kBound,
                      [&] { return !open_ || state.released != at || since <= stale_before_; })) {
      ++bounds_;
    }
  }

  // A deadline passed on `commands`; holds from `stale_before` on hold
  // no live command.
  void Release(std::span<const std::vector<std::string>> commands, core::SteadyTime stale_before) {
    const std::scoped_lock lock(mu_);
    const auto now = core::SteadyClock::now();
    stale_before_ = std::max(stale_before_, stale_before);
    for (const auto& args : commands) {
      for (const auto& arg : args) {
        Key& state = keys_[arg];
        ++state.released;
        state.released_at = now;
      }
    }
    cv_.notify_all();
  }

  // The holds and bound hits since the last call.
  std::pair<uint64_t, uint64_t> Take() {
    const std::scoped_lock lock(mu_);
    return {std::exchange(held_, 0), std::exchange(bounds_, 0)};
  }

 private:
  struct Key {
    uint64_t released = 0;
    core::SteadyTime released_at{};
  };
  static constexpr std::chrono::seconds kBound{5};

  std::mutex mu_;
  std::condition_variable cv_;
  bool open_ = false;
  size_t first_ = 0;
  double chance_ = 0;
  std::chrono::milliseconds timeout_{0};
  core::SteadyTime stale_before_{};
  std::map<std::string, Key, std::less<>> keys_;
  uint64_t held_ = 0;
  uint64_t bounds_ = 0;
};

// One client's connection, driven on its thread as its reactor drives
// it: commands go to the pipeline as fast as it takes them, and its
// events, deadlines and orphans are handled here.
class PipelinedClient {
 public:
  struct Done {
    // Per command: the clock before the feed that dispatched it, and at
    // its reply.
    std::vector<uint64_t> call;
    std::vector<uint64_t> ret;
    std::vector<core::RespValue> replies;
  };
  // Commands dispatched and not yet replied when a deadline passed.
  using Expired = std::function<void(std::span<const std::vector<std::string>>)>;

  PipelinedClient(core::CommandDispatcher& dispatcher, uint64_t client_id)
      : pipeline_(std::make_unique<resp::RequestPipeline>(
            resp::GlobalRegistry(), resp::ConnectionState{.client_id = client_id},
            resp::PipelineDependencies{.dispatcher = &dispatcher},
            resp::PipelineHost{
                .sink = &inbox_,
                .abandoned_deadline = metrics::Registry::Instance().Counter(
                    metrics::names::kNetCommandsAbandonedTotal, metrics::AbandonReason::kDeadline),
            })) {}

  // Runs `batch`, sent as one write, until every command has replied
  // and nothing of it is owed a post. False on a reply it cannot parse.
  bool Run(const std::vector<std::vector<std::string>>& batch, std::atomic<uint64_t>& clock,
           Done& done, const Expired& expired, const std::function<void()>& progress) {
    std::vector<uint8_t> bytes;
    std::vector<size_t> ends;
    for (const auto& args : batch) {
      const std::vector<uint8_t> one =
          resp::Serializer::SerializeCommand(core::RespCommand{.args = args});
      bytes.insert(bytes.end(), one.begin(), one.end());
      ends.push_back(bytes.size());
    }
    done.call.assign(batch.size(), 0);
    done.ret.assign(batch.size(), 0);
    done.replies.clear();
    std::vector<uint8_t> out;
    size_t fed = 0;
    size_t dispatched = 0;
    size_t parsed = 0;
    for (;;) {
      if (fed < bytes.size() && pipeline_->CanDispatch(0)) {
        const uint64_t at = clock.fetch_add(1);
        fed += pipeline_->Feed(std::span(bytes).subspan(fed), 0).consumed;
        for (; dispatched < ends.size() && ends[dispatched] <= fed; ++dispatched) {
          done.call[dispatched] = at;
        }
      }
      pipeline_->Drain(out);
      while (done.replies.size() < batch.size()) {
        auto reply = resp::Parser::Parse(std::span(out).subspan(parsed));
        if (!reply.has_value()) {
          if (reply.error().code() == core::ErrorCode::kIncomplete) break;
          ADD_FAILURE() << "unparsable reply: " << reply.error().message();
          return false;
        }
        parsed += reply->bytes_consumed;
        done.ret[done.replies.size()] = clock.fetch_add(1);
        done.replies.push_back(std::move(reply->value));
      }
      progress();
      if (done.replies.size() == batch.size() && pipeline_->Quiescent() && orphans_.empty()) {
        return true;
      }
      if (fed < bytes.size() && pipeline_->CanDispatch(0)) continue;
      std::optional<core::CompletionEvent> event = inbox_.Take(pipeline_->NextDeadline());
      if (event.has_value()) {
        if (!orphans_.Route(*event, pipeline_.get())) ++late_;
        continue;
      }
      const size_t first = done.replies.size();
      if (auto orphan = pipeline_->Expire(core::SteadyClock::now())) {
        ++orphaned_;
        orphans_.Adopt(std::move(orphan));
      }
      expired(std::span(batch).subspan(first, dispatched - first));
    }
  }

  uint64_t orphaned() const { return orphaned_; }
  uint64_t late() const { return late_; }

 private:
  // Declared in the order they must outlive one another.
  abyss::testing::EventInbox inbox_;
  resp::Orphans orphans_;
  std::unique_ptr<resp::RequestPipeline> pipeline_;
  uint64_t orphaned_ = 0;
  uint64_t late_ = 0;
};

// A pipeline's depth: mostly shallow, sometimes deep.
size_t PickDepth(Rng& rng) {
  const uint64_t roll = rng.Below(10);
  if (roll < 4) return 1;
  if (roll < 7) return static_cast<size_t>(rng.Between(2, 4));
  if (roll < 9) return static_cast<size_t>(rng.Between(5, 12));
  return static_cast<size_t>(rng.Between(13, 32));
}

struct State {
  RedisModel model;
  // Never goes back: each op runs at its burst's frozen clock.
  int64_t now = 0;
};

constexpr std::string_view kNoOpReply = "<no-op>";

// ---- message passing across groups

// The keys a write other than FLUSHDB may change.
std::vector<std::string_view> ChangedKeys(std::string_view name,
                                          const std::vector<std::string>& args) {
  std::vector<std::string_view> keys;
  if (name == "MSET" || name == "MSETNX") {
    for (size_t i = 1; i < args.size(); i += 2) keys.emplace_back(args[i]);
  } else if (name == "DEL" || name == "UNLINK") {
    for (size_t i = 1; i < args.size(); ++i) keys.emplace_back(args[i]);
  } else if (name == "RENAMENX" || name == "COPY") {
    keys = {args.at(1), args.at(2)};
  } else {
    keys.emplace_back(args.at(1));
  }
  return keys;
}

// The string values a write names, each with its key.
std::vector<std::pair<std::string_view, std::string_view>> NamedValues(
    std::string_view name, const std::vector<std::string>& args) {
  std::vector<std::pair<std::string_view, std::string_view>> values;
  if (name == "SET" || name == "SETNX") {
    values.emplace_back(args.at(1), args.at(2));
  } else if (name == "MSET" || name == "MSETNX") {
    for (size_t i = 1; i + 1 < args.size(); i += 2) values.emplace_back(args[i], args[i + 1]);
  }
  return values;
}

struct SureWrite {
  std::string_view value;
  // Its key may expire once the clock moves.
  bool ttl = false;
};

// The value `record` left at `key` for certain, by its reply.
std::optional<SureWrite> SureValue(const Record& record, std::string_view name,
                                   std::string_view key) {
  const core::RespValue& reply = record.reply;
  if (Classify(reply, true) != ReplyKind::kDefinite || reply.IsError()) return std::nullopt;
  const bool ok = reply.IsSimpleString() && reply.AsString() == "OK";
  const bool one = reply.IsInteger() && reply.AsInteger() == 1;
  SureWrite sure;
  if (name == "SET") {
    bool conditional = false;
    bool get = false;
    for (size_t i = 3; i < record.args.size(); ++i) {
      const std::string flag = core::AsciiUpper(record.args[i]);
      if (flag == "NX" || flag == "XX") conditional = true;
      if (flag == "GET") get = true;
      if (flag == "KEEPTTL") sure.ttl = true;
      if (flag == "EX" || flag == "PX" || flag == "EXAT" || flag == "PXAT") {
        sure.ttl = true;
        ++i;
      }
    }
    // A conditional SET with GET replies the old value either way.
    if (conditional && (get || !ok)) return std::nullopt;
  } else if (name == "SETNX" || name == "MSETNX") {
    if (!one) return std::nullopt;
  } else if (name != "MSET" || !ok) {
    return std::nullopt;
  }
  bool named = false;
  for (const auto& [k, v] : NamedValues(name, record.args)) {
    if (k != key) continue;
    sure.value = v;
    named = true;
  }
  if (!named) return std::nullopt;
  return sure;
}

std::string Describe(const Record& record) {
  std::string text = "t" + std::to_string(record.client) + "." + std::to_string(record.order);
  for (const auto& arg : record.args) text += " " + arg;
  return text + " [" + std::to_string(record.call) + ", " + std::to_string(record.ret) + "]";
}

struct CrossKey {
  uint64_t pairs = 0;
  uint64_t broken = 0;
  std::string first;
};

// A client W writes w1 to key a, then w2 to key b in another group; a
// reader R reads b and sees w2, then reads a. w1 took effect before
// w2, which did before R's first read, which did before its second:
// that read must see w1 or what came after it. Judged only where
// nothing else explains the read: an older value back at a needs a
// COPY or RENAMENX onto it between w1 and the read, an absence any
// other write to a or an expiry, and a value that reached a key from
// another names no write to it.
CrossKey CheckCrossKeyOrder(const std::vector<Record>& records,
                            const std::vector<std::vector<std::string>>& groups) {
  CrossKey result;
  std::map<std::string_view, size_t> group_of;
  for (size_t g = 0; g < groups.size(); ++g) {
    for (const auto& key : groups[g]) group_of[key] = g;
  }
  std::vector<std::string> names(records.size());
  // Each value to the write that named it and the key it named it for;
  // a value named twice names neither.
  std::map<std::string_view, std::pair<size_t, std::string_view>> origin;
  std::set<std::string_view> twice;
  // Per key, the writes that may change it; FLUSHDB changes every key.
  std::map<std::string_view, std::vector<size_t>> changes;
  std::vector<size_t> flushes;
  struct Read {
    uint64_t order;
    size_t record;
    const core::RespValue* value;
  };
  // Per client and key: its writes and its reads.
  using ClientKey = std::pair<int, std::string_view>;
  std::map<ClientKey, std::vector<size_t>> writes;
  std::map<ClientKey, std::vector<Read>> reads;
  for (size_t i = 0; i < records.size(); ++i) {
    const Record& record = records[i];
    names[i] = core::AsciiUpper(record.args.at(0));
    const std::string& name = names[i];
    if (name == "FLUSHDB") {
      flushes.push_back(i);
      for (const auto& [key, group] : group_of) writes[{record.client, key}].push_back(i);
      continue;
    }
    if (IsWrite(name)) {
      for (const std::string_view key : ChangedKeys(name, record.args)) {
        changes[key].push_back(i);
        writes[{record.client, key}].push_back(i);
      }
      for (const auto& [key, value] : NamedValues(name, record.args)) {
        if (!origin.emplace(value, std::pair{i, key}).second) twice.insert(value);
      }
      continue;
    }
    if (Classify(record.reply, false) != ReplyKind::kDefinite) continue;
    if (name == "GET") {
      reads[{record.client, record.args.at(1)}].push_back(
          {.order = record.order, .record = i, .value = &record.reply});
    } else if (name == "MGET" && record.reply.IsArray() &&
               record.reply.AsArray().size() + 1 == record.args.size()) {
      for (size_t k = 1; k < record.args.size(); ++k) {
        reads[{record.client, record.args[k]}].push_back(
            {.order = record.order, .record = i, .value = &record.reply.AsArray()[k - 1]});
      }
    }
  }
  for (const std::string_view value : twice) origin.erase(value);
  const auto order_of = [&records](size_t i) { return records[i].order; };
  for (auto& [key, list] : writes) std::ranges::stable_sort(list, {}, order_of);
  for (auto& [key, list] : reads) std::ranges::stable_sort(list, {}, &Read::order);
  // Where `value` was written straight to `key`: the write.
  const auto written = [&origin](const core::RespValue& value,
                                 std::string_view key) -> std::optional<size_t> {
    if (!value.IsBulkString()) return std::nullopt;
    const auto it = origin.find(value.AsString());
    if (it == origin.end() || it->second.second != key) return std::nullopt;
    return it->second.first;
  };

  const std::vector<size_t> none;
  for (const auto& [reader_key, list] : reads) {
    const auto& [reader, b] = reader_key;
    const auto b_group = group_of.find(b);
    if (b_group == group_of.end()) continue;
    for (const Read& r1 : list) {
      const auto w2 = written(*r1.value, b);
      if (!w2.has_value()) continue;
      const Record& second = records[*w2];
      for (const auto& [a, a_group] : group_of) {
        if (a_group == b_group->second) continue;
        const auto own = writes.find({second.client, a});
        if (own == writes.end()) continue;
        const auto after = std::ranges::lower_bound(own->second, second.order, {}, order_of);
        if (after == own->second.begin()) continue;
        const size_t w1 = *std::prev(after);
        const auto sure = SureValue(records[w1], names[w1], a);
        if (!sure.has_value()) continue;
        const auto theirs = reads.find({reader, a});
        if (theirs == reads.end()) continue;
        const auto r2 = std::ranges::upper_bound(theirs->second, r1.order, {}, &Read::order);
        if (r2 == theirs->second.end()) continue;
        const Record& first = records[w1];
        const Record& later = records[r2->record];
        // Whether `y` can take effect between w1 and r2.
        const auto between = [&](size_t y) {
          const Record& op = records[y];
          if (y == w1 || (op.client == first.client && op.order < first.order) ||
              (op.client == later.client && op.order > later.order)) {
            return false;
          }
          return op.ret > first.call && op.call < later.ret;
        };
        const auto changed = changes.find(a);
        const std::vector<size_t>& on_a = changed == changes.end() ? none : changed->second;
        bool explained = false;
        if (r2->value->IsBulkString()) {
          if (r2->value->AsString() == sure->value) {
            ++result.pairs;
            continue;
          }
          const auto x = written(*r2->value, a);
          if (!x.has_value()) continue;
          const Record& older = records[*x];
          const bool before =
              (older.client == first.client && older.order < first.order) || older.ret < first.call;
          if (!before) {
            ++result.pairs;
            continue;
          }
          explained = std::ranges::any_of(on_a, [&](size_t y) {
            return (names[y] == "COPY" || names[y] == "RENAMENX") && records[y].args.at(2) == a &&
                   between(y);
          });
        } else if (r2->value->IsNull()) {
          explained = (sure->ttl && later.at_ms != first.at_ms) ||
                      std::ranges::any_of(on_a, between) || std::ranges::any_of(flushes, between);
        } else {
          continue;
        }
        if (explained) continue;
        ++result.pairs;
        ++result.broken;
        if (result.first.empty()) {
          result.first = Describe(records[r1.record]) + " saw " + std::string(b) + " from " +
                         Describe(second) + ", then " + Describe(later) + " saw " + std::string(a) +
                         " = " + RedisModel::Render(*r2->value) + ", before " + Describe(first);
        }
      }
    }
  }
  return result;
}

// Splits the history by key group and checks each group; `groups`
// names every key. MGET is split per key, FLUSHDB goes to every group.
// `tick` marks progress at each state the search explores.
Outcome CheckRecords(const std::vector<Record>& records,
                     const std::vector<std::vector<std::string>>& groups, uint64_t max_explored,
                     const std::function<void()>& tick = {}) {
  Outcome outcome;
  std::map<std::string, size_t> group_of;
  for (size_t g = 0; g < groups.size(); ++g) {
    for (const auto& key : groups[g]) group_of[key] = g;
  }
  const auto fail = [&outcome](std::string why) {
    if (!outcome.ok) return;
    outcome.ok = false;
    outcome.failure = std::move(why);
  };
  std::vector<std::vector<HistoryOp>> histories(groups.size());
  // The original records' windows per group, for the overlap count.
  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> windows(groups.size());
  for (const auto& record : records) {
    ++outcome.ops;
    const std::string name = core::AsciiUpper(record.args.at(0));
    const ReplyKind kind = Classify(record.reply, IsWrite(name));
    if (kind == ReplyKind::kUnexpected) {
      std::string args;
      for (const auto& arg : record.args) args += " " + arg;
      fail("an unexpected error:" + args + " -> " + record.reply.AsString());
      continue;
    }
    HistoryOp op{
        .client = record.client,
        .call = record.call,
        .ret = record.ret,
        .args = record.args,
        .reply = RedisModel::Canonical(name, record.reply),
        .at_ms = record.at_ms,
        .indeterminate = kind == ReplyKind::kIndeterminate,
        .order = record.order,
    };
    if (kind == ReplyKind::kIndeterminate) {
      ++outcome.indeterminate[std::string(ErrorMessage(record.reply))];
    }
    if (kind == ReplyKind::kNoOp) {
      ++outcome.no_ops;
      ++outcome.no_op_texts[std::string(ErrorMessage(record.reply))];
      if (record.reply.AsString().starts_with("OOM ") ||
          ErrorMessage(record.reply).starts_with("WAL durability window full")) {
        ++outcome.refused;
      }
      op.reply = std::string(kNoOpReply);
    }
    if (name == "FLUSHDB") {
      for (auto& history : histories) history.push_back(op);
      continue;
    }
    if (name == "MGET") {
      if (kind == ReplyKind::kDefinite &&
          (!record.reply.IsArray() || record.reply.AsArray().size() + 1 != record.args.size())) {
        fail("an MGET reply of the wrong shape: " + RedisModel::Render(record.reply));
        continue;
      }
      // Per key, not atomic: each a read within the MGET's window.
      windows.at(group_of.at(record.args.at(1))).emplace_back(record.call, record.ret);
      for (size_t i = 1; i < record.args.size(); ++i) {
        HistoryOp one = op;
        one.args = {"MGET", record.args[i]};
        if (kind == ReplyKind::kDefinite) {
          one.reply = "[" + RedisModel::Render(record.reply.AsArray().at(i - 1)) + "]";
        }
        histories.at(group_of.at(record.args[i])).push_back(std::move(one));
      }
      continue;
    }
    const size_t g = group_of.at(record.args.at(1));
    windows.at(g).emplace_back(record.call, record.ret);
    histories.at(g).push_back(std::move(op));
  }
  for (auto& list : windows) {
    std::ranges::sort(list);
    uint64_t reach = 0;
    for (size_t i = 0; i < list.size(); ++i) {
      const bool before = i > 0 && list[i].first < reach;
      const bool after = i + 1 < list.size() && list[i + 1].first < list[i].second;
      if (before || after) ++outcome.concurrent;
      reach = std::max(reach, list[i].second);
    }
  }

  const auto step = [&tick](State& state, const HistoryOp& op) {
    if (tick) tick();
    if (op.at_ms < state.now) return false;
    state.now = op.at_ms;
    if (op.reply == kNoOpReply) return true;
    const std::string reply =
        RedisModel::Canonical(op.args.at(0), state.model.Execute(op.args, op.at_ms));
    return op.indeterminate || reply == op.reply;
  };
  for (size_t g = 0; g < groups.size() && outcome.ok; ++g) {
    const std::vector<std::string>& keys = groups[g];
    const auto digest = [&keys](const State& state) {
      return std::to_string(state.now) + "|" + state.model.Digest(keys, state.now);
    };
    const auto result =
        abyss::testing::CheckLinearizable<State>(histories[g], State{}, step, digest, max_explored);
    outcome.explored += result.explored;
    if (result.ok) continue;
    std::ostringstream dump;
    dump << "group " << g << " {";
    for (const auto& key : keys) dump << " " << key;
    dump << " }: " << result.detail << "\nhistory (" << histories[g].size()
         << " ops; thread.place, call, return, at, command -> reply):";
    for (const auto& op : histories[g]) {
      dump << "\n  t" << op.client << "." << op.order << " " << op.call << " " << op.ret
           << (op.indeterminate ? " timed-out" : "") << " @" << op.at_ms << " ";
      for (const auto& arg : op.args) dump << arg << " ";
      dump << "-> " << op.reply;
    }
    fail(dump.str());
  }
  const CrossKey cross = CheckCrossKeyOrder(records, groups);
  outcome.cross_pairs = cross.pairs;
  if (cross.broken > 0) {
    outcome.cross_failure = std::to_string(cross.broken) + " of " + std::to_string(cross.pairs) +
                            " pairs; the first: " + cross.first;
  }
  return outcome;
}

class LinearizabilityTest : public PropertyHarness {
 protected:
  Outcome RunSeed(uint64_t seed, int threads, int ops_per_thread, uint64_t max_explored,
                  bool two_logs = false) {
    Swarm swarm = SwarmFor(seed);
    swarm.options.run = "lin" + std::to_string(seed) + "-";
    // Groups must span shards.
    if (swarm.options.shards < 4) {
      swarm.options.shards = 4;
      swarm.text += " (shards forced to 4)";
    }
    if (two_logs) {
      swarm.options.shards = 8;
      swarm.options.log_count = 2;
      swarm.text += " (pinned: 8 shards, 2 logs)";
    }
    // Short deadlines, and loads held past them, so pending commands
    // are given up with their work still out: five times longer where
    // every step is slower, or power_loss acks would time out waiting
    // to be durable.
    Rng knobs(seed ^ 0xDEAD11E5ULL);
    constexpr int64_t kSlowed = kReducedDivisor > 1 ? 5 : 1;
    constexpr std::array<int64_t, 3> kTimeoutMs = {80 * kSlowed, 120 * kSlowed, 200 * kSlowed};
    if (swarm.relieve) {
      swarm.options.command_timeout = std::chrono::milliseconds(kTimeoutMs.at(knobs.Below(3)));
    }
    constexpr std::array<double, 3> kHoldChance = {0.03, 0.05, 0.08};
    const double hold_chance = kHoldChance.at(knobs.Below(3));
    swarm.text += " command_timeout_ms=" + std::to_string(swarm.options.command_timeout.count()) +
                  " hold=" + std::to_string(hold_chance);
    const std::string replay = "ABYSS_LINEARIZABILITY_SEED=" + std::to_string(seed) +
                               " ABYSS_LINEARIZABILITY_THREADS=" + std::to_string(threads) +
                               " ABYSS_LINEARIZABILITY_OPS=" + std::to_string(ops_per_thread);
    std::cout << "[linearizability] seed=" << seed << " threads=" << threads
              << " ops/thread=" << ops_per_thread << " " << swarm.text << '\n'
              << std::flush;
    const auto opened_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               core::WallClock::now().time_since_epoch())
                               .count();
    OpenAt(swarm.options, opened_ms - (opened_ms % 1000));
    PickGroups();
    for (int g = 0; g < kGroups; ++g) {
      std::set<core::ShardId> shards;
      for (const auto& key : groups_.at(g)) shards.insert(ShardOf(key));
      EXPECT_GE(shards.size(), 2U) << "a group on one shard tests no cross-shard write";
    }
    Rng rng(seed);
    hooked_.SetHook([this](std::string_view) { OnLoad(); });
    // After the read, so a held load still succeeds, and lands late.
    hooked_.SetAfterHook([this](std::string_view key) {
      thread_local Rng rng(std::hash<std::thread::id>{}(std::this_thread::get_id()) ^ 0x401DULL);
      holds_.MaybeHold(key, rng);
    });
    const auto abandoned = [] {
      return metrics::testing::GetCounterValue(metrics::names::kNetCommandsAbandonedTotal,
                                               metrics::AbandonReason::kDeadline)
          .value_or(0);
    };
    const double abandoned_before = abandoned();

    std::vector<Record> records;
    std::mutex records_mu;
    std::vector<std::string> misordered;
    uint64_t orphaned = 0;
    uint64_t late = 0;
    std::atomic<uint64_t> clock{1};
    const int per_burst = std::max(1, ops_per_thread / kBursts);
    // A client's place in its commands, across bursts.
    std::vector<uint64_t> places(static_cast<size_t>(threads), 0);
    for (int burst = 0; burst < kBursts; ++burst) {
      Progress("seed " + std::to_string(seed) + " burst " + std::to_string(burst));
      holds_.Open(4, hold_chance, options_.command_timeout);
      std::atomic<bool> done{false};
      std::thread drainer([this, &done, seed, burst] { Drain(done, (seed * 31) + burst); });
      std::thread maintainer([this, &done, seed, burst] { Maintain(done, (seed * 37) + burst); });
      std::vector<std::thread> clients;
      clients.reserve(static_cast<size_t>(threads));
      std::latch start(threads);
      for (int t = 0; t < threads; ++t) {
        clients.emplace_back([&, t] {
          start.arrive_and_wait();
          Rng mine((seed * 1000) + (static_cast<uint64_t>(burst) * 100) + static_cast<uint64_t>(t));
          PipelinedClient client(*engine_, static_cast<uint64_t>(t) + 1);
          uint64_t& place = places.at(static_cast<size_t>(t));
          std::vector<Record> local;
          std::vector<std::string> wrong;
          PipelinedClient::Done replied;
          for (int issued = 0; issued < per_burst;) {
            const size_t depth = std::min(PickDepth(mine), static_cast<size_t>(per_burst - issued));
            std::vector<std::vector<std::string>> batch;
            batch.reserve(depth);
            for (size_t d = 0; d < depth; ++d) {
              if (mine.Chance(0.05)) {
                batch.push_back(
                    {"ECHO", "e" + std::to_string(t) + "." + std::to_string(place + d)});
              } else if (d + 1 < depth && mine.Chance(kMessagePassing)) {
                for (auto& command : MessagePassing(mine, t, place + d)) {
                  batch.push_back(std::move(command));
                }
                ++d;
              } else {
                batch.push_back(Generate(mine, t, place + d));
              }
            }
            issued += static_cast<int>(depth);
            const bool ran = client.Run(
                batch, clock, replied,
                [this](std::span<const std::vector<std::string>> commands) {
                  holds_.Release(commands, core::SteadyClock::now() - options_.command_timeout);
                },
                [this] { Heartbeat(); });
            if (!ran) break;
            for (size_t i = 0; i < batch.size(); ++i) {
              if (batch[i][0] == "ECHO") {
                const core::RespValue& echoed = replied.replies[i];
                if (!echoed.IsBulkString() || echoed.AsString() != batch[i][1]) {
                  wrong.push_back("t" + std::to_string(t) + " expected " + batch[i][1] + ", got " +
                                  RedisModel::Render(echoed));
                }
                continue;
              }
              local.push_back(Record{
                  .client = t,
                  .call = replied.call[i],
                  .ret = replied.ret[i],
                  .args = std::move(batch[i]),
                  .reply = std::move(replied.replies[i]),
                  .at_ms = now_ms_,
                  .order = place + i,
              });
            }
            place += batch.size();
          }
          const std::scoped_lock lock(records_mu);
          for (auto& record : local) records.push_back(std::move(record));
          for (auto& line : wrong) misordered.push_back(std::move(line));
          orphaned += client.orphaned();
          late += client.late();
        });
      }
      for (auto& client : clients) client.join();
      holds_.Close();
      done = true;
      drainer.join();
      maintainer.join();
      // Quiescent: the clock moves only now, often past TTLs set in
      // the burst.
      now_ms_ += rng.Chance(0.5) ? rng.Between(0, 50) : rng.Between(500, 2500);
    }
    hooked_.SetHook(nullptr);
    hooked_.SetAfterHook(nullptr);
    Progress("seed " + std::to_string(seed) + " check");
    Outcome outcome = CheckRecords(records, groups_, max_explored, [this] { Heartbeat(); });
    outcome.abandoned = static_cast<uint64_t>(abandoned() - abandoned_before);
    outcome.orphaned = orphaned;
    outcome.late = late;
    std::tie(outcome.held, outcome.hold_bounds) = holds_.Take();
    std::cout << "[linearizability] seed=" << seed << ": " << outcome.abandoned
              << " abandoned at their deadline (" << outcome.orphaned << " with work still out), "
              << outcome.held << " loads held (" << outcome.hold_bounds << " to the bound), "
              << outcome.late << " late events, " << outcome.cross_pairs
              << " cross-group pairs judged\n"
              << std::flush;
    if (!misordered.empty()) {
      outcome.ok = false;
      outcome.failure = "a reply left out of command order: " + misordered.front() + " (" +
                        std::to_string(misordered.size()) + " in all)";
    }
    // A few timeouts and refusals are expected on a loaded machine;
    // many are not, or a server refusing most ops would pass. The
    // holds time commands out on purpose: each the one it holds and a
    // few queued or waiting behind it.
    uint64_t indeterminate = 0;
    for (const auto& [text, count] : outcome.indeterminate) indeterminate += count;
    const uint64_t few = std::max<uint64_t>(10, outcome.ops / 10);
    const uint64_t timed_out = outcome.no_ops - outcome.refused;
    if (outcome.ok && indeterminate > std::max<uint64_t>(5, outcome.ops / 50)) {
      outcome.ok = false;
      outcome.failure = std::to_string(indeterminate) + " of " + std::to_string(outcome.ops) +
                        " ops timed out after being logged";
    }
    if (outcome.ok &&
        (outcome.refused > few || timed_out > few + outcome.abandoned + (2 * outcome.held) ||
         outcome.abandoned > few + (2 * outcome.held))) {
      outcome.ok = false;
      outcome.failure = std::to_string(outcome.no_ops) + " of " + std::to_string(outcome.ops) +
                        " ops were refused or timed out before being logged, for " +
                        std::to_string(outcome.held) +
                        " loads held: " + std::to_string(outcome.refused) + " refused, " +
                        std::to_string(outcome.abandoned) + " given up at their deadline";
      for (const auto& [text, count] : outcome.no_op_texts) {
        outcome.failure += "\n  " + std::to_string(count) + " x " + text;
      }
    }
    if (!outcome.ok) {
      ADD_FAILURE() << "seed " << seed << " (" << swarm.text << "): " << outcome.failure
                    << "\nreplay: " << replay;
    }
    if (!outcome.cross_failure.empty()) {
      outcome.ok = false;
      ADD_FAILURE() << "seed " << seed << " (" << swarm.text
                    << "): a read missed a write its writer made before one it saw: "
                    << outcome.cross_failure << "\nreplay: " << replay;
    }
    CloseAll();
    return outcome;
  }

  void PickGroups() {
    groups_.assign(kGroups, {});
    for (int g = 0; g < kGroups; ++g) {
      std::set<core::ShardId> used;
      std::optional<uint32_t> log;
      for (int n = 0; std::ssize(groups_[g]) < kGroupKeys; ++n) {
        std::string key = "g" + std::to_string(g) + "k" + std::to_string(n);
        const core::ShardId shard = ShardOf(key);
        if (log.has_value() && queue_->LogOf(shard) != *log) continue;
        // Spread over shards while there are fresh ones to take.
        if (used.contains(shard) && n < 200) continue;
        log = queue_->LogOf(shard);
        used.insert(shard);
        groups_[g].push_back(std::move(key));
      }
    }
    // Message passing's two keys, each a group of its own, on two
    // shards.
    const std::string a = "mpa";
    std::string b = "mpb0";
    for (int n = 1; ShardOf(b) == ShardOf(a); ++n) b = "mpb" + std::to_string(n);
    groups_.push_back({a});
    groups_.push_back({b});
  }

  // ---- the generator: one group per command

  // Unique: a client's place in its commands, and a count within one.
  static std::string Value(int thread, uint64_t place, int n) {
    return "t" + std::to_string(thread) + "." + std::to_string(place) + "." + std::to_string(n);
  }

  // A writer's SET of a, its old value asked for so it may load, then
  // SET of b; or a reader's GET of b, then of a.
  std::vector<std::vector<std::string>> MessagePassing(Rng& rng, int thread, uint64_t place) {
    const std::string& a = groups_.at(kGroups).front();
    const std::string& b = groups_.at(kGroups + 1).front();
    if (rng.Chance(0.5)) {
      return {
          {"SET", a, Value(thread, place, 0), "GET"},
          {"SET", b, Value(thread, place + 1, 0)},
      };
    }
    return {{"GET", b}, {"GET", a}};
  }

  std::vector<std::string> Generate(Rng& rng, int thread, uint64_t place) {
    const auto& group = groups_.at(rng.Below(kGroups));
    const auto key = [&] { return group.at(rng.Below(group.size())); };
    int n = 0;
    const auto value = [&] { return Value(thread, place, n++); };
    const auto member = [&] { return "m" + std::to_string(rng.Below(5)); };
    const auto num = [](int64_t n) { return std::to_string(n); };
    const uint64_t roll = rng.Below(1000);
    if (roll < 3) return {"FLUSHDB"};
    if (roll < 120) {
      std::vector<std::string> args = {"SET", key(), value()};
      const uint64_t cond = rng.Below(5);
      if (cond == 1) args.emplace_back("NX");
      if (cond == 2) args.emplace_back("XX");
      if (rng.Chance(0.2)) args.emplace_back("GET");
      if (rng.Chance(0.3)) args.insert(args.end(), {"PX", num(rng.Between(1, 2000))});
      return args;
    }
    if (roll < 150) return {"SETNX", key(), value()};
    if (roll < 200) {
      std::vector<std::string> args = {rng.Chance(0.6) ? "MSET" : "MSETNX"};
      for (const auto& k : group) {
        if (rng.Chance(0.7)) args.insert(args.end(), {k, value()});
      }
      if (args.size() == 1) args.insert(args.end(), {key(), value()});
      return args;
    }
    if (roll < 240) {
      std::vector<std::string> args = {"DEL", key()};
      if (rng.Chance(0.5)) args.push_back(key());
      return args;
    }
    if (roll < 300) return {rng.Chance(0.7) ? "SADD" : "SREM", key(), member(), member()};
    if (roll < 350) {
      std::vector<std::string> args = {"ZADD", key()};
      const uint64_t flag = rng.Below(6);
      if (flag == 1) args.emplace_back("NX");
      if (flag == 2) args.emplace_back("XX");
      if (flag == 3) args.emplace_back("GT");
      if (rng.Chance(0.3)) args.emplace_back("CH");
      args.insert(args.end(), {num(rng.Between(-3, 3)), member()});
      return args;
    }
    if (roll < 370) return {"ZREM", key(), member()};
    if (roll < 420) return {"HSET", key(), member(), value()};
    if (roll < 440) return {"HSETNX", key(), member(), value()};
    if (roll < 455) return {"HDEL", key(), member()};
    if (roll < 495) {
      std::vector<std::string> args = {"PEXPIRE", key(), num(rng.Between(-10, 2000))};
      constexpr std::array<std::string_view, 4> kFlags = {"NX", "XX", "GT", "LT"};
      if (rng.Chance(0.4)) args.emplace_back(kFlags.at(rng.Below(kFlags.size())));
      return args;
    }
    if (roll < 505) return {"PERSIST", key()};
    if (roll < 535) {
      const std::string a = key();
      std::string b = key();
      if (rng.Chance(0.5)) return {"RENAMENX", a, b};
      if (a == b) return {"COPY", a, group.at((group_index(a, group) + 1) % group.size())};
      if (rng.Chance(0.5)) return {"COPY", a, b, "REPLACE"};
      return {"COPY", a, b};
    }
    switch (rng.Below(12)) {
      case 0:
      case 1:
        return {"GET", key()};
      case 2: {
        std::vector<std::string> args = {"MGET"};
        for (const auto& k : group) args.push_back(k);
        return args;
      }
      case 3:
        return {"EXISTS", key()};
      case 4:
        return {"TYPE", key()};
      case 5:
        return {"PTTL", key()};
      case 6:
        return {"SMEMBERS", key()};
      case 7:
        return {"SCARD", key()};
      case 8:
        return {"ZRANGE", key(), "0", "-1", "WITHSCORES"};
      case 9:
        return {"HGETALL", key()};
      case 10:
        return {"HGET", key(), member()};
      default:
        return {"SISMEMBER", key(), member()};
    }
  }
  static size_t group_index(const std::string& key, const std::vector<std::string>& group) {
    for (size_t i = 0; i < group.size(); ++i) {
      if (group[i] == key) return i;
    }
    return 0;
  }

  // ---- background: cold absorbs and flushes, hot evicts

  void Drain(const std::atomic<bool>& done, uint64_t seed) {
    Rng rng(seed);
    while (!done.load()) {
      for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
        Absorb(shard, rng.Chance(0.5) ? 1 + rng.Below(4) : SIZE_MAX);
        if (rng.Chance(0.2)) FlushCold(shard);
      }
      std::this_thread::sleep_for(std::chrono::microseconds(rng.Between(50, 400)));
    }
  }

  void Maintain(const std::atomic<bool>& done, uint64_t seed) {
    Rng rng(seed);
    while (!done.load()) {
      switch (rng.Below(4)) {
        case 0:
          hot_->EvictExpired(core::SteadyClock::now() + std::chrono::hours{1'000'000});
          break;
        case 1:
          hot_->EvictToMemoryTarget();
          break;
        case 2:
          hot_->GcTombstones();
          break;
        default:
          hot_->EvictExpired(core::SteadyClock::now());
          break;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(rng.Between(100, 1000)));
    }
  }

  // A load is slowed, or raced by an eviction of everything drained.
  void OnLoad() {
    thread_local Rng rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    const uint64_t roll = rng.Below(10);
    if (roll < 3) {
      std::this_thread::sleep_for(std::chrono::microseconds(rng.Between(10, 300)));
    } else if (roll < 5) {
      hot_->EvictExpired(core::SteadyClock::now() + std::chrono::hours{1'000'000});
    }
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::vector<std::vector<std::string>> groups_;
  Holds holds_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

void Add(Outcome& total, const Outcome& outcome) {
  total.ops += outcome.ops;
  total.concurrent += outcome.concurrent;
  total.no_ops += outcome.no_ops;
  total.explored += outcome.explored;
  total.abandoned += outcome.abandoned;
  total.orphaned += outcome.orphaned;
  total.held += outcome.held;
  total.hold_bounds += outcome.hold_bounds;
  total.late += outcome.late;
  total.cross_pairs += outcome.cross_pairs;
  for (const auto& [text, count] : outcome.indeterminate) total.indeterminate[text] += count;
}

void Report(const std::string& tier, uint64_t seeds, const Outcome& total,
            std::chrono::steady_clock::duration took) {
  std::cout << "[linearizability] " << tier << ": " << seeds << " seeds, " << total.ops << " ops ("
            << total.concurrent << " overlapping another on their group), " << total.no_ops
            << " no-ops, " << total.abandoned << " abandoned (" << total.orphaned
            << " with work still out), " << total.held << " loads held, " << total.explored
            << " states searched, " << total.cross_pairs << " cross-group pairs judged, "
            << std::chrono::duration_cast<std::chrono::milliseconds>(took).count() << " ms\n";
  for (const auto& [text, count] : total.indeterminate) {
    std::cout << "[linearizability]   timed out after logging: " << count << " x " << text << '\n';
  }
  std::cout << std::flush;
}

TEST_F(LinearizabilityTest, ConcurrentClientsAreLinearizablePerKeyGroup) {
  const int threads = static_cast<int>(EnvNumber("ABYSS_LINEARIZABILITY_THREADS").value_or(4));
  const int ops =
      static_cast<int>(EnvNumber("ABYSS_LINEARIZABILITY_OPS").value_or(200 / kReducedDivisor));
  const auto started = std::chrono::steady_clock::now();
  Outcome total;
  std::vector<uint64_t> seeds;
  if (const auto seed = EnvNumber("ABYSS_LINEARIZABILITY_SEED")) {
    seeds = {*seed};
  } else {
    seeds = {1, 2, 3};
    if (kReducedDivisor > 1) seeds = {3};
  }
  for (const uint64_t seed : seeds) {
    // Seed 3 runs on two logs, so groups exercise CROSSSLOT-free
    // multi-key writes on the second.
    const Outcome outcome = RunSeed(seed, threads, ops, 2'000'000, seed == 3);
    Add(total, outcome);
    if (!outcome.ok) break;
  }
  Report("bounded tier", seeds.size(), total, std::chrono::steady_clock::now() - started);
  // Clients that ran one at a time overlap almost nothing; on a loaded
  // machine about half still overlap. A reduced build's few ops vary
  // too much to judge, and judge only a few cross-group pairs.
  if (!HasFailure() && kReducedDivisor == 1) {
    EXPECT_GE(total.concurrent * 4, total.ops) << "too few ops overlapped to test concurrency";
    EXPECT_GT(total.cross_pairs, 0U) << "no read judged a connection's order across groups";
  }
  // Else the check that an abandoned command never applies went
  // untested.
  if (!HasFailure()) {
    EXPECT_GT(total.orphaned, 0U) << "no command was given up with its work still out";
  }
}

TEST_F(LinearizabilityTest, LongTier) {
  const auto seeds = EnvNumber("ABYSS_LINEARIZABILITY_SEEDS");
  if (!seeds.has_value()) GTEST_SKIP() << "long tier: set ABYSS_LINEARIZABILITY_SEEDS";
  const auto threads = static_cast<int>(EnvNumber("ABYSS_LINEARIZABILITY_THREADS").value_or(8));
  const auto ops = static_cast<int>(EnvNumber("ABYSS_LINEARIZABILITY_OPS").value_or(1000));
  const uint64_t first = EnvNumber("ABYSS_LINEARIZABILITY_FIRST_SEED").value_or(1000);
  const auto started = std::chrono::steady_clock::now();
  Outcome total;
  uint64_t run = 0;
  for (uint64_t seed = first; seed < first + *seeds; ++seed, ++run) {
    const Outcome outcome = RunSeed(seed, threads, ops, 20'000'000, seed % 4 == 0);
    Add(total, outcome);
    if (!outcome.ok) break;
  }
  Report("long tier", run, total, std::chrono::steady_clock::now() - started);
}

// ---- the litmus: message passing through one connection's pipeline

// W pipelines SET a then SET b, a's write held at its load; meanwhile
// R reads b and must never see it. Released, both show, a no later
// than b. a and b sit on two logs, which order nothing between them.
class MessagePassingTest : public PropertyHarness {};

TEST_F(MessagePassingTest, ALaterWriteNeverShowsBeforeItsConnectionsEarlierOne) {
  Options options;
  options.shards = 4;
  options.log_count = 2;
  // One I/O thread holds a's load, the other serves R's.
  options.cold_io_threads = 2;
  // Past any run: a deadline would end the hold early.
  options.command_timeout = std::chrono::seconds(60);
  OpenAt(options, 1'700'000'000'000);
  const std::string a = KeyOn(0, 0, "a");
  const std::string b = KeyOn(1, 0, "b");
  ASSERT_NE(queue_->LogOf(ShardOf(a)), queue_->LogOf(ShardOf(b)));

  std::mutex mu;
  std::condition_variable cv;
  bool held = false;
  bool fed = false;
  bool released = false;
  hooked_.SetAfterHook([&](std::string_view key) {
    if (key != a) return;
    std::unique_lock lock(mu);
    held = true;
    cv.notify_all();
    cv.wait(lock, [&released] { return released; });
  });
  const auto release = [&] {
    const std::scoped_lock lock(mu);
    released = true;
    cv.notify_all();
  };
  const PipelinedClient::Expired never = [](std::span<const std::vector<std::string>>) {};
  std::atomic<uint64_t> clock{1};
  std::atomic<bool> written{false};
  PipelinedClient::Done w_replies;
  std::thread writer([&] {
    PipelinedClient w(*engine_, 1);
    // GET makes SET read a, so it loads a from cold, where it is held.
    (void)w.Run({{"SET", a, "1", "GET"}, {"SET", b, "1"}}, clock, w_replies, never, [&] {
      const std::scoped_lock lock(mu);
      fed = true;
      cv.notify_all();
    });
    written = true;
  });
  const abyss::testing::OnExit finish([&] {
    release();
    writer.join();
    hooked_.SetAfterHook(nullptr);
  });
  {
    std::unique_lock lock(mu);
    // A backstop: W's first feed and a's load take microseconds.
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] { return held && fed; }))
        << "a's load held " << held << ", W fed " << fed;
  }

  PipelinedClient r(*engine_, 2);
  const auto get = [&](const std::string& key) {
    PipelinedClient::Done done;
    if (!r.Run({{"GET", key}}, clock, done, never, [this] { Heartbeat(); })) {
      return core::RespValue::Error(core::ErrorPrefix::kErr, "unparsable reply");
    }
    return std::move(done.replies.at(0));
  };
  const auto shows = [](const core::RespValue& value) {
    return value.IsBulkString() && value.AsString() == "1";
  };
  constexpr int kHeldReads = 16;
  for (int i = 0; i < kHeldReads; ++i) {
    const core::RespValue seen = get(b);
    if (!seen.IsNull()) {
      ADD_FAILURE() << "read " << i << " saw " << b << " = " << RedisModel::Render(seen)
                    << " while the write to " << a << " before it was held";
      break;
    }
  }

  release();
  // Until W has both replies, and once more: b then a, so a shows no
  // later than b.
  const auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  for (bool last = false; !last;) {
    last = written.load();
    const core::RespValue seen_b = get(b);
    const core::RespValue seen_a = get(a);
    if (shows(seen_b) && !shows(seen_a)) {
      ADD_FAILURE() << "saw " << b << " = 1, then " << a << " = " << RedisModel::Render(seen_a);
      break;
    }
    if (last) {
      EXPECT_TRUE(shows(seen_a) && shows(seen_b))
          << "after both writes replied: " << a << " = " << RedisModel::Render(seen_a) << ", " << b
          << " = " << RedisModel::Render(seen_b);
    }
    if (!last && std::chrono::steady_clock::now() > bound) {
      ADD_FAILURE() << "W never had both replies";
      break;
    }
  }
  if (written.load()) {
    ASSERT_EQ(w_replies.replies.size(), 2U);
    EXPECT_TRUE(w_replies.replies[0].IsNull()) << RedisModel::Render(w_replies.replies[0]);
    EXPECT_TRUE(w_replies.replies[1].IsSimpleString() && w_replies.replies[1].AsString() == "OK")
        << RedisModel::Render(w_replies.replies[1]);
  }
}

// ---- the checker, end to end over records, with no engine

Record At(int client, uint64_t call, uint64_t ret, std::vector<std::string> args,
          core::RespValue reply, int64_t at_ms = 0, uint64_t order = 0) {
  return {
      .client = client,
      .call = call,
      .ret = ret,
      .args = std::move(args),
      .reply = std::move(reply),
      .at_ms = at_ms,
      .order = order,
  };
}

core::RespValue Ok() { return core::RespValue::SimpleString("OK"); }
core::RespValue Bulk(std::string text) { return core::RespValue::BulkString(std::move(text)); }
core::RespValue Error(core::ErrorPrefix prefix, std::string text) {
  return core::RespValue::Error(prefix, std::move(text));
}

// One group of two keys.
std::vector<std::vector<std::string>> AB() { return {{"a", "b"}}; }

TEST(LinearizabilityCheckerTest, AcceptsAnOverlappingReadAndRejectsAStaleOne) {
  std::vector<Record> history = {At(0, 1, 2, {"SET", "a", "1"}, Ok()),
                                 At(1, 3, 4, {"GET", "a"}, core::RespValue::Null())};
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
  history[1].call = 0;
  EXPECT_TRUE(CheckRecords(history, AB(), 1000).ok);
}

// A pipelined GET after its own client's SET, both in flight at once,
// sees the SET; another client's overlapping GET need not.
TEST(LinearizabilityCheckerTest, AClientsPipelinedOpsTakeEffectInItsOrder) {
  const std::vector<Record> stale = {
      At(0, 1, 4, {"SET", "a", "1"}, Ok(), 0, 0),
      At(0, 1, 5, {"GET", "a"}, core::RespValue::Null(), 0, 1),
  };
  EXPECT_FALSE(CheckRecords(stale, AB(), 1000).ok);
  const std::vector<Record> fresh = {
      At(0, 1, 4, {"SET", "a", "1"}, Ok(), 0, 0),
      At(0, 1, 5, {"GET", "a"}, Bulk("1"), 0, 1),
  };
  EXPECT_TRUE(CheckRecords(fresh, AB(), 1000).ok);
  const std::vector<Record> other = {
      At(0, 1, 4, {"SET", "a", "1"}, Ok(), 0, 0),
      At(1, 1, 5, {"GET", "a"}, core::RespValue::Null(), 0, 0),
  };
  EXPECT_TRUE(CheckRecords(other, AB(), 1000).ok);
}

// The positive control: a reader that sees an MSET's second key, then
// misses its first, saw it torn.
TEST(LinearizabilityCheckerTest, ATornMsetFails) {
  const std::vector<Record> history = {
      At(0, 1, 10, {"MSET", "a", "2", "b", "2"}, Ok()),
      At(1, 2, 3, {"MGET", "b", "a"}, core::RespValue::Array({Bulk("2"), core::RespValue::Null()})),
  };
  EXPECT_TRUE(CheckRecords(history, AB(), 1000).ok) << "MGET is not atomic: this is legal";
  const std::vector<Record> torn = {
      At(0, 1, 10, {"MSET", "a", "2", "b", "2"}, Ok()),
      At(1, 2, 3, {"GET", "b"}, Bulk("2")),
      At(1, 4, 5, {"GET", "a"}, core::RespValue::Null()),
  };
  EXPECT_FALSE(CheckRecords(torn, AB(), 1000).ok);
}

// A write that timed out after it was logged landed inside its window
// or not at all: never before its call.
TEST(LinearizabilityCheckerTest, ATimedOutWriteSeenBeforeItsCallFails) {
  const auto timed_out = Error(core::ErrorPrefix::kErr,
                               "write durable wait exceeded server timeout; the write is applied "
                               "and may yet become durable");
  const std::vector<Record> history = {At(0, 10, 11, {"SET", "a", "x"}, timed_out),
                                       At(1, 1, 2, {"GET", "a"}, Bulk("x"))};
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
  const std::vector<Record> after = {At(0, 1, 2, {"SET", "a", "x"}, timed_out),
                                     At(1, 3, 4, {"GET", "a"}, Bulk("x"))};
  EXPECT_TRUE(CheckRecords(after, AB(), 1000).ok);
}

// A timed-out PEXPIRE from an earlier burst cannot land after a later
// burst's SET, at the earlier burst's clock, to explain a lost write.
TEST(LinearizabilityCheckerTest, ATimedOutOpCannotLandAfterItsWindow) {
  const auto timed_out = Error(core::ErrorPrefix::kErr,
                               "write durable wait exceeded server timeout; the write is applied "
                               "and may yet become durable");
  const std::vector<Record> history = {
      At(0, 1, 2, {"SET", "a", "v0"}, Ok(), 1000),
      At(0, 3, 4, {"PEXPIRE", "a", "100"}, timed_out, 1000),
      At(1, 5, 6, {"SET", "a", "v"}, Ok(), 3000),
      At(1, 7, 8, {"GET", "a"}, core::RespValue::Null(), 3000),
  };
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
  // Within one burst, where the clock alone would not stop it.
  const std::vector<Record> one_burst = {
      At(0, 1, 2, {"SET", "a", "v0"}, Ok(), 1000),
      At(0, 3, 4, {"PEXPIRE", "a", "0"}, timed_out, 1000),
      At(1, 5, 6, {"SET", "a", "v"}, Ok(), 1000),
      At(1, 7, 8, {"GET", "a"}, core::RespValue::Null(), 1000),
  };
  EXPECT_FALSE(CheckRecords(one_burst, AB(), 1000).ok);
}

// -OOM refused the write before anything was reserved: a read that
// shows its value saw a write that never happened.
TEST(LinearizabilityCheckerTest, AReadObservingAnOomWriteFails) {
  const std::vector<Record> history = {
      At(0, 1, 2, {"SET", "a", "y"},
         Error(core::ErrorPrefix::kOom,
               "command not allowed when hot memory is over its limit and cold is behind")),
      At(1, 3, 4, {"GET", "a"}, Bulk("y"))};
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
}

TEST(LinearizabilityCheckerTest, AnUnexpectedErrorFails) {
  const std::vector<Record> history = {
      At(0, 1, 2, {"SET", "a", "y"}, Error(core::ErrorPrefix::kErr, "internal server error"))};
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
}

// Running out of search is an unknown, and an unknown fails.
TEST(LinearizabilityCheckerTest, AnExhaustedSearchFails) {
  const std::vector<Record> history = {At(0, 1, 2, {"SET", "a", "1"}, Ok()),
                                       At(1, 3, 4, {"GET", "a"}, Bulk("1"))};
  EXPECT_TRUE(CheckRecords(history, AB(), 1000).ok);
  const Outcome exhausted = CheckRecords(history, AB(), 1);
  EXPECT_FALSE(exhausted.ok);
  EXPECT_TRUE(exhausted.failure.contains("budget")) << exhausted.failure;
}

// Message passing across groups, each alone linearizable: W sets a,
// then b; R sees b, then a read of a must see W's write or a later one.
TEST(LinearizabilityCheckerTest, AReadMissingAnEarlierWriteOfAWriteItsReaderSawFails) {
  const std::vector<std::vector<std::string>> two = {{"a"}, {"b"}};
  std::vector<Record> history = {
      At(0, 3, 20, {"SET", "a", "x"}, Ok(), 0, 0),
      At(0, 3, 21, {"SET", "b", "y"}, Ok(), 0, 1),
      At(1, 4, 5, {"GET", "b"}, Bulk("y"), 0, 0),
      At(1, 6, 7, {"GET", "a"}, core::RespValue::Null(), 0, 1),
  };
  const Outcome missed = CheckRecords(history, two, 1000);
  EXPECT_TRUE(missed.ok) << missed.failure;
  EXPECT_EQ(missed.cross_pairs, 1U);
  EXPECT_TRUE(missed.cross_failure.contains("GET a")) << missed.cross_failure;
  history[3].reply = Bulk("x");
  const Outcome seen = CheckRecords(history, two, 1000);
  EXPECT_EQ(seen.cross_pairs, 1U);
  EXPECT_TRUE(seen.cross_failure.empty()) << seen.cross_failure;
  // A value written to a before W's write began.
  history.push_back(At(2, 1, 2, {"SET", "a", "old"}, Ok(), 0, 0));
  history[3].reply = Bulk("old");
  const Outcome stale = CheckRecords(history, two, 1000);
  EXPECT_TRUE(stale.ok) << stale.failure;
  EXPECT_FALSE(stale.cross_failure.empty());
  // A DEL of a that may fall between W's write and R's read explains a
  // miss: no pair is judged.
  history[3].reply = core::RespValue::Null();
  history.push_back(At(2, 6, 8, {"DEL", "a"}, core::RespValue::Integer(1), 0, 1));
  const Outcome deleted = CheckRecords(history, two, 1000);
  EXPECT_EQ(deleted.cross_pairs, 0U);
  EXPECT_TRUE(deleted.cross_failure.empty()) << deleted.cross_failure;
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
