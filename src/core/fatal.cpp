#include "abyss/core/fatal.h"

#include <atomic>
#include <cstdlib>
#include <string_view>

#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.core.fatal")

namespace abyss::core {

namespace {

std::atomic<FatalHandler> g_fatal_handler{nullptr};

}  // namespace

void Fatal(std::string_view reason) {
  ABYSS_LOG_CRITICAL("fatal invariant breach; terminating", {"reason", reason});
  log::Flush();
  if (const FatalHandler handler = g_fatal_handler.load(std::memory_order_acquire);
      handler != nullptr) {
    handler(reason);
  }
  std::abort();
}

void SetFatalHandlerForTesting(FatalHandler handler) noexcept {
  g_fatal_handler.store(handler, std::memory_order_release);
}

}  // namespace abyss::core
