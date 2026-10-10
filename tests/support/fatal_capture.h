#pragma once

#include <string>
#include <string_view>

#include "abyss/core/fatal.h"

namespace abyss::testing {

// Thrown in place of std::abort while a ScopedFatalCapture is alive.
struct FatalCalled {
  std::string reason;
};

class ScopedFatalCapture {
 public:
  ScopedFatalCapture() { core::SetFatalHandlerForTesting(&Throw); }
  ~ScopedFatalCapture() { core::SetFatalHandlerForTesting(nullptr); }

  ScopedFatalCapture(const ScopedFatalCapture&) = delete;
  ScopedFatalCapture& operator=(const ScopedFatalCapture&) = delete;
  ScopedFatalCapture(ScopedFatalCapture&&) = delete;
  ScopedFatalCapture& operator=(ScopedFatalCapture&&) = delete;

 private:
  [[noreturn]] static void Throw(std::string_view reason) {
    throw FatalCalled{std::string(reason)};
  }
};

}  // namespace abyss::testing
