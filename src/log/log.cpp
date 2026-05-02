#include "abyss/log/log.h"

#include <xxhash.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/config/config.h"
#include "abyss/core/thread_annotations.h"

#if ABYSS_WITH_LOGGING
#include <spdlog/formatter.h>
#include <spdlog/logger.h>
#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/sink.h>
#include <spdlog/sinks/stdout_sinks.h>

#include "log_internal.h"
#endif

namespace abyss::log {

namespace {

constexpr std::string_view LevelSpelling(Level level) noexcept {
  switch (level) {
    case Level::kTrace:
      return "trace";
    case Level::kDebug:
      return "debug";
    case Level::kInfo:
      return "info";
    case Level::kWarn:
      return "warn";
    case Level::kError:
      return "error";
    case Level::kCritical:
      return "critical";
    case Level::kOff:
      return "off";
  }
  return "off";
}

std::string LowerCase(std::string_view s) {
  std::string out(s);
  std::ranges::transform(out, out.begin(), [](unsigned char c) { return std::tolower(c); });
  return out;
}

}  // namespace

bool ParseLevel(std::string_view text, Level& out) noexcept {
  const std::string lc = LowerCase(text);
  if (lc == "trace") {
    out = Level::kTrace;
    return true;
  }
  if (lc == "debug") {
    out = Level::kDebug;
    return true;
  }
  if (lc == "info") {
    out = Level::kInfo;
    return true;
  }
  if (lc == "warn" || lc == "warning") {
    out = Level::kWarn;
    return true;
  }
  if (lc == "error" || lc == "err") {
    out = Level::kError;
    return true;
  }
  if (lc == "critical" || lc == "fatal") {
    out = Level::kCritical;
    return true;
  }
  if (lc == "off" || lc == "none") {
    out = Level::kOff;
    return true;
  }
  return false;
}

std::string_view ToStringView(Level level) noexcept { return LevelSpelling(level); }

uint64_t KeyHash(std::string_view key) noexcept { return XXH3_64bits(key.data(), key.size()); }

#if ABYSS_WITH_LOGGING

namespace {

spdlog::level::level_enum ToSpdlog(Level level) noexcept {
  switch (level) {
    case Level::kTrace:
      return spdlog::level::trace;
    case Level::kDebug:
      return spdlog::level::debug;
    case Level::kInfo:
      return spdlog::level::info;
    case Level::kWarn:
      return spdlog::level::warn;
    case Level::kError:
      return spdlog::level::err;
    case Level::kCritical:
      return spdlog::level::critical;
    case Level::kOff:
      return spdlog::level::off;
  }
  return spdlog::level::off;
}

Level FromSpdlog(spdlog::level::level_enum level) noexcept {
  switch (level) {
    case spdlog::level::trace:
      return Level::kTrace;
    case spdlog::level::debug:
      return Level::kDebug;
    case spdlog::level::info:
      return Level::kInfo;
    case spdlog::level::warn:
      return Level::kWarn;
    case spdlog::level::err:
      return Level::kError;
    case spdlog::level::critical:
      return Level::kCritical;
    case spdlog::level::off:
      return Level::kOff;
    case spdlog::level::n_levels:
      return Level::kOff;
  }
  return Level::kOff;
}

void AppendEscapedJsonString(std::string& out, std::string_view s) {
  out.push_back('"');
  for (char ch : s) {
    const auto uch = static_cast<unsigned char>(ch);
    switch (ch) {
      case '"':
        out.append("\\\"");
        break;
      case '\\':
        out.append("\\\\");
        break;
      case '\b':
        out.append("\\b");
        break;
      case '\f':
        out.append("\\f");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      default:
        if (uch < 0x20) {
          std::array<char, 8> buf{};
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
          std::snprintf(buf.data(), buf.size(), "\\u%04x", uch);
          out.append(buf.data());
        } else {
          out.push_back(ch);
        }
    }
  }
  out.push_back('"');
}

void AppendDouble(std::string& out, double v) {
  std::array<char, 32> buf{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(buf.data(), buf.size(), "%.17g", v);
  out.append(buf.data());
}

void AppendIso8601(std::string& out, std::chrono::system_clock::time_point tp) {
  using namespace std::chrono;
  const auto secs = time_point_cast<seconds>(tp);
  const auto micros = duration_cast<microseconds>(tp - secs).count();
  const std::time_t tt = system_clock::to_time_t(secs);
  std::tm tm_utc{};
#ifdef _WIN32
  gmtime_s(&tm_utc, &tt);
#else
  gmtime_r(&tt, &tm_utc);
#endif
  std::array<char, 40> buf{};
  int written = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  written = std::snprintf(buf.data(), buf.size(), "%04d-%02d-%02dT%02d:%02d:%02d.%06lldZ",
                          tm_utc.tm_year + 1900, tm_utc.tm_mon + 1, tm_utc.tm_mday, tm_utc.tm_hour,
                          tm_utc.tm_min, tm_utc.tm_sec, static_cast<long long>(micros));
  if (written > 0) out.append(buf.data(), static_cast<size_t>(written));
}

void AppendFieldValue(std::string& out, const LogValue& value) {
  std::visit(
      [&](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, std::string>) {
          AppendEscapedJsonString(out, std::string_view(v));
        } else if constexpr (std::is_same_v<T, bool>) {
          out.append(v ? "true" : "false");
        } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>) {
          out.append(std::to_string(v));
        } else if constexpr (std::is_same_v<T, double>) {
          AppendDouble(out, v);
        }
      },
      value);
}

void AppendTextFieldValue(std::string& out, const LogValue& value) {
  std::visit(
      [&](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, std::string>) {
          out.append(std::string_view(v));
        } else if constexpr (std::is_same_v<T, bool>) {
          out.append(v ? "true" : "false");
        } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>) {
          out.append(std::to_string(v));
        } else if constexpr (std::is_same_v<T, double>) {
          AppendDouble(out, v);
        }
      },
      value);
}

struct ThreadLocalContext {
  const LogField* data = nullptr;
  size_t size = 0;
};

ThreadLocalContext& Tls() noexcept {
  thread_local ThreadLocalContext ctx;
  return ctx;
}

class JsonFormatter : public spdlog::formatter {
 public:
  void format(const spdlog::details::log_msg& msg, spdlog::memory_buf_t& dest) override {
    std::string out;
    out.reserve(128);
    out.push_back('{');
    out.append(R"("ts":")");
    AppendIso8601(out, msg.time);
    out.push_back('"');
    out.append(",\"level\":");
    AppendEscapedJsonString(out, LevelSpelling(FromSpdlog(msg.level)));
    out.append(",\"component\":");
    AppendEscapedJsonString(out, std::string_view(msg.logger_name.data(), msg.logger_name.size()));
    out.append(",\"msg\":");
    AppendEscapedJsonString(out, std::string_view(msg.payload.data(), msg.payload.size()));
    const auto& tls = Tls();
    for (size_t i = 0; i < tls.size; ++i) {
      out.push_back(',');
      AppendEscapedJsonString(out, tls.data[i].key);
      out.push_back(':');
      AppendFieldValue(out, tls.data[i].value);
    }
    out.push_back('}');
    out.push_back('\n');
    dest.append(out);
  }

  std::unique_ptr<spdlog::formatter> clone() const override {
    return std::make_unique<JsonFormatter>();
  }
};

// Wraps spdlog's pattern_formatter; appends our thread-local structured
// fields after the pattern output. Spdlog's formatter has no hook for
// extending a line, so we run pattern_formatter against a scratch buffer
// and then splice the fields in before the trailing newline.
class TextFormatter : public spdlog::formatter {
 public:
  TextFormatter() = default;

  void format(const spdlog::details::log_msg& msg, spdlog::memory_buf_t& dest) override {
    spdlog::memory_buf_t scratch;
    inner_->format(msg, scratch);
    std::string_view line(scratch.data(), scratch.size());
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
      line.remove_suffix(1);
    }
    dest.append(line.data(), line.data() + line.size());

    const auto& tls = Tls();
    std::string fields;
    for (size_t i = 0; i < tls.size; ++i) {
      fields.push_back(' ');
      fields.append(tls.data[i].key);
      fields.push_back('=');
      AppendTextFieldValue(fields, tls.data[i].value);
    }
    fields.push_back('\n');
    dest.append(fields);
  }

  std::unique_ptr<spdlog::formatter> clone() const override {
    return std::make_unique<TextFormatter>();
  }

 private:
  static constexpr const char* kPattern = "%Y-%m-%dT%H:%M:%S.%fZ [%l] %n: %v";
  std::unique_ptr<spdlog::pattern_formatter> inner_ =
      std::make_unique<spdlog::pattern_formatter>(kPattern);
};

std::shared_ptr<spdlog::sinks::sink> MakeSink(std::string_view destination,
                                              std::string_view format) {
  std::shared_ptr<spdlog::sinks::sink> sink;
  if (destination == "stderr") {
    sink = std::make_shared<spdlog::sinks::stderr_sink_mt>();
  } else {
    sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
  }
  if (format == "text") {
    sink->set_formatter(std::make_unique<TextFormatter>());
  } else {
    sink->set_formatter(std::make_unique<JsonFormatter>());
  }
  return sink;
}

struct Registry {
  mutable std::mutex mu;
  std::shared_ptr<spdlog::sinks::sink> sink ABYSS_GUARDED_BY(mu);
  std::string sink_destination ABYSS_GUARDED_BY(mu) = "stderr";
  std::string sink_format ABYSS_GUARDED_BY(mu) = "text";
  std::atomic<Level> default_level{Level::kInfo};
  std::atomic<bool> initialized{false};
  std::unordered_map<std::string, std::unique_ptr<internal::LoggerImpl>> loggers
      ABYSS_GUARDED_BY(mu);
  std::vector<std::pair<std::string, Level>> component_levels ABYSS_GUARDED_BY(mu);
};

Registry& State() {
  static Registry instance;
  return instance;
}

Level EffectiveLevel(const Registry& r, std::string_view name) ABYSS_REQUIRES(r.mu) {
  for (const auto& [component, level] : r.component_levels) {
    if (component == name) return level;
  }
  return r.default_level.load(std::memory_order_relaxed);
}

std::shared_ptr<spdlog::sinks::sink> EnsureSink(Registry& r) ABYSS_REQUIRES(r.mu) {
  if (!r.sink) {
    r.sink = MakeSink(r.sink_destination, r.sink_format);
  }
  return r.sink;
}

}  // namespace

namespace internal {

struct LoggerImpl {
  std::string name;
  std::shared_ptr<spdlog::logger> spd;
  std::atomic<Level> level{Level::kInfo};
};

}  // namespace internal

struct Logger::Impl : internal::LoggerImpl {};

void Init(const config::LogConfig& config) {
  Registry& r = State();
  const std::lock_guard<std::mutex> lk(r.mu);

  r.sink_destination = config.sink;
  r.sink_format = config.format;
  r.sink = MakeSink(r.sink_destination, r.sink_format);
  r.default_level.store(config.default_level, std::memory_order_relaxed);
  r.component_levels.clear();
  r.component_levels.reserve(config.component_levels.size());
  for (const auto& entry : config.component_levels) {
    r.component_levels.emplace_back(entry.component, entry.level);
  }

  for (auto& [name, impl] : r.loggers) {
    impl->spd->sinks() = {r.sink};
    const Level eff = EffectiveLevel(r, name);
    impl->spd->set_level(ToSpdlog(eff));
    impl->level.store(eff, std::memory_order_relaxed);
  }

  r.initialized.store(true, std::memory_order_release);
}

bool Initialized() noexcept { return State().initialized.load(std::memory_order_acquire); }

Logger Get(std::string_view component) {
  Registry& r = State();
  const std::lock_guard<std::mutex> lk(r.mu);
  auto it = r.loggers.find(std::string(component));
  if (it != r.loggers.end()) {
    return Logger(reinterpret_cast<const Logger::Impl*>(it->second.get()));
  }
  auto impl = std::make_unique<internal::LoggerImpl>();
  impl->name = std::string(component);
  auto sink = EnsureSink(r);
  impl->spd = std::make_shared<spdlog::logger>(impl->name, sink);
  const Level eff = EffectiveLevel(r, impl->name);  // NOLINT(cppcoreguidelines-init-variables)
  impl->spd->set_level(ToSpdlog(eff));
  impl->level.store(eff, std::memory_order_relaxed);
  internal::LoggerImpl* raw = impl.get();
  r.loggers.emplace(impl->name, std::move(impl));
  return Logger(reinterpret_cast<const Logger::Impl*>(raw));
}

bool Logger::ShouldLog(Level level) const noexcept {
  if (impl_ == nullptr) {
    return static_cast<uint8_t>(level) >= static_cast<uint8_t>(Level::kInfo);
  }
  return static_cast<uint8_t>(level) >=
         static_cast<uint8_t>(impl_->level.load(std::memory_order_relaxed));
}

std::string_view Logger::Name() const noexcept {
  if (impl_ == nullptr) return {};
  return impl_->name;
}

void Logger::Log(Level level, std::string_view msg, std::span<const LogField> fields) const {
  auto& tls = Tls();
  const LogField* prev_data = tls.data;
  const size_t prev_size = tls.size;
  tls.data = fields.data();
  tls.size = fields.size();

  if (impl_ != nullptr) {
    impl_->spd->log(ToSpdlog(level), msg);
  } else {
    std::string line;
    line.append("[abyss:pre-init] ");
    line.append(LevelSpelling(level));
    line.push_back(' ');
    line.append(msg);
    for (const auto& f : fields) {
      line.push_back(' ');
      line.append(f.key);
      line.push_back('=');
      AppendTextFieldValue(line, f.value);
    }
    line.push_back('\n');
    std::fputs(line.c_str(), stderr);
  }

  tls.data = prev_data;
  tls.size = prev_size;
}

namespace internal {

void ResetForTesting() {
  Registry& r = State();
  const std::lock_guard<std::mutex> lk(r.mu);
  r.component_levels.clear();
  r.sink_destination = "stderr";
  r.sink_format = "text";
  r.sink = MakeSink(r.sink_destination, r.sink_format);
  r.default_level.store(Level::kInfo, std::memory_order_relaxed);
  r.initialized.store(false, std::memory_order_release);

  for (auto& [name, impl] : r.loggers) {
    impl->spd->sinks() = {r.sink};
    impl->spd->set_level(ToSpdlog(Level::kInfo));
    impl->level.store(Level::kInfo, std::memory_order_relaxed);
  }
}

void InstallTestSink(std::shared_ptr<spdlog::sinks::sink> sink) {
  Registry& r = State();
  const std::lock_guard<std::mutex> lk(r.mu);
  r.sink = std::move(sink);
  for (auto& [name, impl] : r.loggers) {
    (void)name;
    impl->spd->sinks() = {r.sink};
  }
}

std::span<const LogField> CurrentThreadFields() noexcept {
  const auto& tls = Tls();
  return {tls.data, tls.size};
}

namespace {
void RunFormatter(spdlog::formatter& fmt, Level level, std::string_view component,
                  std::string_view msg, std::span<const LogField> fields, std::string& out) {
  auto& tls = Tls();
  const LogField* prev_data = tls.data;
  const size_t prev_size = tls.size;
  tls.data = fields.data();
  tls.size = fields.size();

  const spdlog::string_view_t component_sv{component.data(), component.size()};
  const spdlog::string_view_t payload{msg.data(), msg.size()};
  const spdlog::details::log_msg log_msg(component_sv, ToSpdlog(level), payload);
  spdlog::memory_buf_t dest;
  fmt.format(log_msg, dest);
  out.assign(dest.data(), dest.size());

  tls.data = prev_data;
  tls.size = prev_size;
}
}  // namespace

std::string FormatJsonForTesting(Level level, std::string_view component, std::string_view msg,
                                 std::span<const LogField> fields) {
  JsonFormatter fmt;
  std::string out;
  RunFormatter(fmt, level, component, msg, fields, out);
  return out;
}

std::string FormatTextForTesting(Level level, std::string_view component, std::string_view msg,
                                 std::span<const LogField> fields) {
  TextFormatter fmt;
  std::string out;
  RunFormatter(fmt, level, component, msg, fields, out);
  return out;
}

}  // namespace internal

#else  // !ABYSS_WITH_LOGGING

struct Logger::Impl {};

void Init(const config::LogConfig& /*config*/) {}
bool Initialized() noexcept { return true; }
Logger Get(std::string_view /*component*/) { return Logger{}; }

bool Logger::ShouldLog(Level /*level*/) const noexcept { return false; }
std::string_view Logger::Name() const noexcept { return {}; }
void Logger::Log(Level /*level*/, std::string_view /*msg*/,
                 std::span<const LogField> /*fields*/) const {}

#endif  // ABYSS_WITH_LOGGING

void Logger::Trace(std::string_view msg, std::span<const LogField> fields) const {
  Log(Level::kTrace, msg, fields);
}
void Logger::Debug(std::string_view msg, std::span<const LogField> fields) const {
  Log(Level::kDebug, msg, fields);
}
void Logger::Info(std::string_view msg, std::span<const LogField> fields) const {
  Log(Level::kInfo, msg, fields);
}
void Logger::Warn(std::string_view msg, std::span<const LogField> fields) const {
  Log(Level::kWarn, msg, fields);
}
void Logger::Error(std::string_view msg, std::span<const LogField> fields) const {
  Log(Level::kError, msg, fields);
}
void Logger::Critical(std::string_view msg, std::span<const LogField> fields) const {
  Log(Level::kCritical, msg, fields);
}

}  // namespace abyss::log
