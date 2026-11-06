#pragma once

#include <functional>
#include <string_view>
#include <tuple>
#include <utility>

#include "abyss/core/completion.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/loader.h"
#include "blocking_wait.h"

namespace abyss::testing {

// Loader::LoadAsOrJoin, waiting for the landing of another's load it
// joined, as a command task waits for its kResume. `joined` runs
// between the join and the wait. kTimeout at `deadline`, unless the
// leader took the join first: its landing is then on its way, and is
// taken.
inline core::Result<engine::Loader::Shared> LoadAs(const engine::Loader& loader,
                                                   core::ShardId shard, std::string_view key,
                                                   core::KeyType type, core::SteadyTime deadline,
                                                   engine::Loader::Source* source = nullptr,
                                                   const std::function<void()>& joined = {}) {
  BlockingSink sink;
  engine::Loader::FlightJoin join;
  const core::Completion completion{.sink = &sink, .client_id = 0, .slot_seq = 0};
  if (auto loaded = loader.LoadAsOrJoin(shard, key, type, deadline, source, join, completion)) {
    return *std::move(loaded);
  }
  if (joined) joined();
  if (join.Arm()) {
    // A lost Cancel means the landing posts next, and the sink must
    // outlive that post.
    if (!sink.mailbox().WaitUntil(deadline) && loader.CancelJoin(join)) {
      return std::unexpected(
          core::Error{core::ErrorCode::kTimeout, "timed out awaiting another load of the key"});
    }
    std::ignore = sink.mailbox().Take();
  }
  if (source != nullptr) *source = join.source();
  return join.result();
}

}  // namespace abyss::testing
