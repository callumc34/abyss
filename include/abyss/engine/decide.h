#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/effect.h"
#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::engine {

// A key's view, taken under its shard's lock.
using KeyLookup = std::function<hot::KeyView(std::string_view key)>;

// What a command must know of a key it reads.
enum class Need : uint8_t {
  // Whether it exists: a stub answers.
  kExistence,
  kState,
};

struct KeyLoad {
  std::string key;
  Need need;

  bool operator==(const KeyLoad&) const = default;
};

// The highest latest_seq a decision read on one shard.
struct ShardSeq {
  core::ShardId shard = 0;
  core::SequenceId seq = 0;

  bool operator==(const ShardSeq&) const = default;
};

// An effect argument moved out of the request.
struct Moved {
  uint32_t effect;
  uint32_t arg;
  uint32_t request_arg;

  bool operator==(const Moved&) const = default;
};

struct Decision {
  // Empty when a single-key command's reply is what applying its own
  // effect, the last one, returns. Filled otherwise.
  std::optional<core::RespValue> reply;
  // A DEL for each expired key the command read, then its own.
  std::vector<core::Effect> effects;
  // Keys whose state hot does not hold. When set, nothing else is:
  // load them and decide again.
  std::vector<KeyLoad> needs_load;
  // For the fence: per shard read, the highest latest_seq, at most one
  // entry each.
  std::vector<ShardSeq> observed;
  // A syntax error or WRONGTYPE: replied with nothing logged. A
  // WRONGTYPE keeps `observed`: the reply shows the key's state.
  std::optional<core::Error> error;
  // Every effect argument taken from the request, for Restore.
  std::vector<Moved> moved;
};

// Decides a write against hot's state. `flags` is the command's
// predicate; `now_ms` is the wall time relative TTLs start from, and
// the views' expiry time. `cmd` is unchanged when the decision has
// needs_load or error; it is moved from once effects are emitted,
// which is safe because every value-carrying emit follows all of the
// command's reads.
Decision Decide(core::RespCommand& cmd, core::PredicateFlags flags, uint64_t now_ms,
                const KeyLookup& lookup);

// Gives the decision's moved arguments back to `cmd`, the request it
// was decided from, so it can be decided again.
void Restore(Decision&& decision, core::RespCommand& cmd);

// The keys Decide would read or write for `cmd`, with the error it
// would reply for an unknown command or a wrong argument count.
core::Result<std::vector<std::string_view>> WriteKeys(const core::RespCommand& cmd);

// Whether `cmd` can add to hot memory: what waits on backpressure.
bool GrowsMemory(const core::RespCommand& cmd);

}  // namespace abyss::engine
