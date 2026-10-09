#pragma once

// A Wing-Gong linearizability check with Lowe's memoisation, as
// Porcupine runs it, over one partition of a history. P-compositional
// use: partition by independent object (here, a group of keys no
// command crosses) and check each partition alone.

#include <algorithm>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace abyss::testing {

struct HistoryOp {
  int client = 0;
  uint64_t call = 0;
  uint64_t ret = 0;
  std::vector<std::string> args;
  std::string reply;
  // The wall clock the op ran under, for a model that judges TTLs.
  int64_t at_ms = 0;
  // Its outcome is unknown (a timeout after it was logged): it took
  // effect at one point inside [call, ret], or not at all.
  bool indeterminate = false;
};

struct LinearizabilityResult {
  bool ok = true;
  // States the search visited.
  uint64_t explored = 0;
  // When not ok: why, with the longest linearisable prefix found.
  std::string detail;
};

namespace linearizability_internal {

// One linearised set and state, as a cache key: the set's bits and a
// 128-bit hash of the state's digest. A hash collision only prunes the
// search, which can fail a good history but never pass a bad one.
struct Key {
  std::vector<uint64_t> bits;
  uint64_t h1 = 0;
  uint64_t h2 = 0;

  bool operator==(const Key&) const = default;
};

struct KeyHash {
  size_t operator()(const Key& key) const {
    uint64_t h = key.h1 ^ (key.h2 * 0x9E3779B97F4A7C15ULL);
    for (const uint64_t word : key.bits) h = (h ^ word) * 0x100000001B3ULL;
    return static_cast<size_t>(h);
  }
};

inline std::pair<uint64_t, uint64_t> Hash128(std::string_view text) {
  uint64_t a = 0xCBF29CE484222325ULL;
  uint64_t b = 0x84222325CBF29CE4ULL;
  for (const char c : text) {
    a = (a ^ static_cast<unsigned char>(c)) * 0x100000001B3ULL;
    b = (b + static_cast<unsigned char>(c)) * 0xFF51AFD7ED558CCDULL;
    b ^= b >> 33U;
  }
  return {a, b};
}

}  // namespace linearizability_internal

// `step(state, op)` returns whether `op`, with its reply, can take
// effect on `state`, updating it when so; an indeterminate op's reply
// is not checked. `digest(state)` names a state. An indeterminate op
// is tried applied, then as a no-op, always within its window.
template <typename State>
LinearizabilityResult CheckLinearizable(const std::vector<HistoryOp>& ops, State initial,
                                        const std::function<bool(State&, const HistoryOp&)>& step,
                                        const std::function<std::string(const State&)>& digest,
                                        uint64_t max_explored = 2'000'000) {
  using linearizability_internal::Key;
  struct Event {
    uint64_t time;
    bool call;
    size_t op;
    // Doubly linked, with a sentinel at index 0.
    size_t prev = 0;
    size_t next = 0;
    size_t match = 0;
  };
  LinearizabilityResult result;
  const size_t n = ops.size();
  if (n == 0) return result;
  std::vector<Event> events;
  events.reserve((2 * n) + 1);
  events.push_back({.time = 0, .call = false, .op = 0});
  for (size_t i = 0; i < n; ++i) {
    events.push_back({.time = ops[i].call, .call = true, .op = i});
    events.push_back({.time = ops[i].ret, .call = false, .op = i});
  }
  std::sort(events.begin() + 1, events.end(), [](const Event& a, const Event& b) {
    if (a.time != b.time) return a.time < b.time;
    return a.call && !b.call;
  });
  std::vector<size_t> call_of(n);
  std::vector<size_t> ret_of(n);
  for (size_t i = 1; i < events.size(); ++i) {
    events[i].prev = i - 1;
    events[i].next = i + 1 < events.size() ? i + 1 : 0;
    (events[i].call ? call_of : ret_of)[events[i].op] = i;
  }
  events[0].next = 1;
  events[0].prev = events.size() - 1;
  for (size_t i = 0; i < n; ++i) {
    events[call_of[i]].match = ret_of[i];
    events[ret_of[i]].match = call_of[i];
  }
  const auto unlink = [&events](size_t e) {
    events[events[e].prev].next = events[e].next;
    events[events[e].next].prev = events[e].prev;
  };
  const auto relink = [&events](size_t e) {
    events[events[e].prev].next = e;
    events[events[e].next].prev = e;
  };

  std::vector<uint64_t> linearized((n + 63) / 64, 0);
  const auto flip = [&linearized](size_t op) { linearized[op / 64] ^= uint64_t{1} << (op % 64); };
  std::unordered_set<Key, linearizability_internal::KeyHash> cache;
  struct Placed {
    size_t entry;
    State before;
    // 0 applied; 1 a no-op, for an indeterminate op.
    int option;
  };
  std::vector<Placed> calls;
  State state = std::move(initial);
  size_t longest = 0;
  size_t entry = events[0].next;
  int first_option = 0;
  while (events[0].next != 0) {
    if (++result.explored > max_explored) {
      result.ok = false;
      result.detail = "search budget of " + std::to_string(max_explored) + " states exhausted";
      return result;
    }
    if (entry == 0) {
      // Unreachable: every unplaced call's return follows it.
      result.ok = false;
      result.detail = "walked past the last return";
      return result;
    }
    if (events[entry].call) {
      const HistoryOp& op = ops[events[entry].op];
      const int options = op.indeterminate ? 2 : 1;
      bool placed = false;
      for (int option = first_option; option < options && !placed; ++option) {
        State next = state;
        if (option == 0 && !step(next, op)) continue;
        flip(events[entry].op);
        const auto [h1, h2] = linearizability_internal::Hash128(digest(next));
        if (cache.insert(Key{.bits = linearized, .h1 = h1, .h2 = h2}).second) {
          calls.push_back({.entry = entry, .before = std::move(state), .option = option});
          state = std::move(next);
          unlink(entry);
          unlink(events[entry].match);
          longest = std::max(longest, calls.size());
          placed = true;
        } else {
          flip(events[entry].op);
        }
      }
      first_option = 0;
      entry = placed ? events[0].next : events[entry].next;
      continue;
    }
    // A return whose call was not placed: undo the last placement.
    if (calls.empty()) {
      result.ok = false;
      const HistoryOp& op = ops[events[entry].op];
      std::string detail = "no linearisation; longest prefix " + std::to_string(longest) + " of " +
                           std::to_string(n) + "; first unplaceable return:";
      for (const auto& arg : op.args) detail += " " + arg;
      detail += " -> " + op.reply;
      result.detail = detail;
      return result;
    }
    Placed last = std::move(calls.back());
    calls.pop_back();
    state = std::move(last.before);
    flip(events[last.entry].op);
    relink(events[last.entry].match);
    relink(last.entry);
    // An indeterminate op is tried again as a no-op before moving on.
    if (last.option == 0 && ops[events[last.entry].op].indeterminate) {
      entry = last.entry;
      first_option = 1;
    } else {
      entry = events[last.entry].next;
    }
  }
  return result;
}

}  // namespace abyss::testing
