#pragma once

#include <string_view>

namespace abyss::core {

// Fail-stop for an unrecoverable invariant breach: logs `reason` at
// CRITICAL, flushes the log sink, then aborts the process.
[[noreturn]] void Fatal(std::string_view reason);

using FatalHandler = void (*)(std::string_view reason);

// Test seam: `handler` runs in place of std::abort, after the log. It
// must throw to unwind; if it returns, the process still aborts.
// nullptr restores the default.
void SetFatalHandlerForTesting(FatalHandler handler) noexcept;

}  // namespace abyss::core
