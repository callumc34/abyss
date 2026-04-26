#pragma once

#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::resp {

// kNone means unconditional (engine routes to DispatchWrite); any other
// flags route through the resolver. Errors surface as -ERR syntax error
// before any queue append.
using PredicateExtractor = core::Result<core::PredicateFlags> (*)(const core::RespCommand&);

core::Result<core::PredicateFlags> ExtractSetFlags(const core::RespCommand& cmd);
core::Result<core::PredicateFlags> ExtractSetNxFlags(const core::RespCommand& cmd);
core::Result<core::PredicateFlags> ExtractMsetNxFlags(const core::RespCommand& cmd);
core::Result<core::PredicateFlags> ExtractZAddFlags(const core::RespCommand& cmd);
core::Result<core::PredicateFlags> ExtractExpireFlags(const core::RespCommand& cmd);
core::Result<core::PredicateFlags> ExtractRenameNxFlags(const core::RespCommand& cmd);
core::Result<core::PredicateFlags> ExtractCopyFlags(const core::RespCommand& cmd);
core::Result<core::PredicateFlags> ExtractHsetNxFlags(const core::RespCommand& cmd);

}  // namespace abyss::resp
