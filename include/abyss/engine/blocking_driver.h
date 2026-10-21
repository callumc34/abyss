#pragma once

#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/engine/sequencer.h"

namespace abyss::engine {

// A write's attempts run to its reply on the calling thread, which
// blocks on a sink of its own between them; at the deadline it expires
// the write's wait. Its own loads run here too.
core::Result<core::RespValue> RunToCompletion(Sequencer& sequencer, core::RespCommand cmd,
                                              core::PredicateFlags flags);
// FLUSHDB, the same way.
core::Result<core::RespValue> RunFlushToCompletion(Sequencer& sequencer);

}  // namespace abyss::engine
