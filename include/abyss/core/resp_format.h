#pragma once

#include <string>

namespace abyss::core {

// Redis-compatible double formatter for RESP2 bulk-string replies.
// Matches Redis `addReplyDouble` byte-for-byte so clients that do string
// equality on scores (ZSCORE, ZRANGE WITHSCORES, ZINCRBY, ...) see identical
// output across tiers and against upstream Redis.
//
//   NaN    -> "nan"
//   +inf   -> "inf"
//   -inf   -> "-inf"
//   other  -> %.17g in the C locale (general format, 17 significant digits)
std::string FormatRespDouble(double value);

}  // namespace abyss::core
