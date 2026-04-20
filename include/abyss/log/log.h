#pragma once

#include <atomic>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <variant>

namespace abyss::config {
struct LogConfig;
}

namespace abyss::log {

enum class Level : uint8_t {
  kTrace = 0,
  kDebug = 1,
  kInfo = 2,
  kWarn = 3,
  kError = 4,
  kCritical = 5,
  kOff = 6,
};

bool ParseLevel(std::string_view text, Level& out) noexcept;
std::string_view ToStringView(Level level) noexcept;

using LogValue = std::variant<std::string_view, std::string, int64_t, uint64_t, double, bool>;

struct LogField {
  std::string_view key;
  LogValue value;
};

// Truncated xxHash of a raw key. Log records never carry raw keys; callers
// emit KeyHash(key) as a "key_hash" field instead.
uint64_t KeyHash(std::string_view key) noexcept;

// First call in main(). Pre-Init calls route to a stderr fallback so
// third-party static init cannot crash; application code must not rely on it.
void Init(const config::LogConfig& config);

bool Initialized() noexcept;

class Logger {
 public:
  Logger() noexcept = default;

  // Lock-free atomic load; called by the macros before field evaluation.
  bool ShouldLog(Level level) const noexcept;

  void Log(Level level, std::string_view msg, std::span<const LogField> fields = {}) const;

  // Per-level convenience. No internal level gating; use the macros for that.
  void Trace(std::string_view msg, std::span<const LogField> fields = {}) const;
  void Debug(std::string_view msg, std::span<const LogField> fields = {}) const;
  void Info(std::string_view msg, std::span<const LogField> fields = {}) const;
  void Warn(std::string_view msg, std::span<const LogField> fields = {}) const;
  void Error(std::string_view msg, std::span<const LogField> fields = {}) const;
  void Critical(std::string_view msg, std::span<const LogField> fields = {}) const;

  std::string_view Name() const noexcept;

 private:
  friend Logger Get(std::string_view);

  struct Impl;
  const Impl* impl_ = nullptr;

  explicit Logger(const Impl* impl) noexcept : impl_(impl) {}
};

// Component-scoped logger; results are cached process-wide.
Logger Get(std::string_view component);

// Emission macros: expand to a level check that gates field-argument
// evaluation. `msg` should be static; dynamic values go in fields.
// Usage: ABYSS_LOG_INFO(logger, "flush complete", {"reason", "quiet"});
//
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define ABYSS_LOG_EMIT_(LOGGER, LEVEL, MSG, ...)                                                  \
  do {                                                                                            \
    const ::abyss::log::Logger& abyss_log_logger_ = (LOGGER);                                     \
    if (abyss_log_logger_.ShouldLog(LEVEL)) {                                                     \
      const ::std::initializer_list<::abyss::log::LogField> abyss_log_fields_{__VA_ARGS__};       \
      abyss_log_logger_.Log((LEVEL), (MSG),                                                       \
                            ::std::span<const ::abyss::log::LogField>(abyss_log_fields_.begin(),  \
                                                                      abyss_log_fields_.size())); \
    }                                                                                             \
  } while (false)

#define ABYSS_LOG_TRACE(LOGGER, MSG, ...) \
  ABYSS_LOG_EMIT_(LOGGER, ::abyss::log::Level::kTrace, MSG __VA_OPT__(, ) __VA_ARGS__)
#define ABYSS_LOG_DEBUG(LOGGER, MSG, ...) \
  ABYSS_LOG_EMIT_(LOGGER, ::abyss::log::Level::kDebug, MSG __VA_OPT__(, ) __VA_ARGS__)
#define ABYSS_LOG_INFO(LOGGER, MSG, ...) \
  ABYSS_LOG_EMIT_(LOGGER, ::abyss::log::Level::kInfo, MSG __VA_OPT__(, ) __VA_ARGS__)
#define ABYSS_LOG_WARN(LOGGER, MSG, ...) \
  ABYSS_LOG_EMIT_(LOGGER, ::abyss::log::Level::kWarn, MSG __VA_OPT__(, ) __VA_ARGS__)
#define ABYSS_LOG_ERROR(LOGGER, MSG, ...) \
  ABYSS_LOG_EMIT_(LOGGER, ::abyss::log::Level::kError, MSG __VA_OPT__(, ) __VA_ARGS__)
#define ABYSS_LOG_CRITICAL(LOGGER, MSG, ...) \
  ABYSS_LOG_EMIT_(LOGGER, ::abyss::log::Level::kCritical, MSG __VA_OPT__(, ) __VA_ARGS__)
// NOLINTEND(cppcoreguidelines-macro-usage)

}  // namespace abyss::log
