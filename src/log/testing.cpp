#include "abyss/log/testing.h"

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/thread_annotations.h"

#if ABYSS_WITH_LOGGING
#include <spdlog/sinks/base_sink.h>

#include "log_internal.h"
#endif

namespace abyss::log::testing {

#if ABYSS_WITH_LOGGING

namespace {

std::string ValueToString(const LogValue& value) {
  std::string out;
  std::visit(
      [&](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, std::string>) {
          out.assign(std::string_view(v));
        } else if constexpr (std::is_same_v<T, bool>) {
          out = v ? "true" : "false";
        } else if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>) {
          out = std::to_string(v);
        } else if constexpr (std::is_same_v<T, double>) {
          out = std::to_string(v);
        }
      },
      value);
  return out;
}

Level FromSpdlogForTesting(int level) noexcept {
  switch (level) {
    case 0:
      return Level::kTrace;
    case 1:
      return Level::kDebug;
    case 2:
      return Level::kInfo;
    case 3:
      return Level::kWarn;
    case 4:
      return Level::kError;
    case 5:
      return Level::kCritical;
    case 6:
      return Level::kOff;
    default:
      return Level::kOff;
  }
}

class CaptureSink : public spdlog::sinks::base_sink<std::mutex> {
 public:
  std::vector<CapturedRecord> Snapshot() const {
    const std::lock_guard<std::mutex> lk(snapshot_mu_);
    return records_;
  }

  size_t Size() const {
    const std::lock_guard<std::mutex> lk(snapshot_mu_);
    return records_.size();
  }

  void ClearAll() {
    const std::lock_guard<std::mutex> lk(snapshot_mu_);
    records_.clear();
  }

 protected:
  void sink_it_(const spdlog::details::log_msg& msg) override {
    CapturedRecord rec;
    rec.ts = msg.time;
    rec.level = FromSpdlogForTesting(static_cast<int>(msg.level));
    rec.component.assign(msg.logger_name.data(), msg.logger_name.size());
    rec.msg.assign(msg.payload.data(), msg.payload.size());

    const auto fields = internal::CurrentThreadFields();
    rec.fields.reserve(fields.size());
    for (const auto& f : fields) {
      rec.fields.emplace_back(std::string(f.key), ValueToString(f.value));
    }

    const std::lock_guard<std::mutex> lk(snapshot_mu_);
    records_.push_back(std::move(rec));
  }

  void flush_() override {}

 private:
  mutable std::mutex snapshot_mu_;
  std::vector<CapturedRecord> records_ ABYSS_GUARDED_BY(snapshot_mu_);
};

}  // namespace

#endif  // ABYSS_WITH_LOGGING

struct CapturingSink::Impl {
#if ABYSS_WITH_LOGGING
  std::shared_ptr<CaptureSink> sink;
#endif
};

#if ABYSS_WITH_LOGGING

CapturingSink::CapturingSink() : impl_(std::make_unique<Impl>()) {
  impl_->sink = std::make_shared<CaptureSink>();
  internal::InstallTestSink(impl_->sink);
}

CapturingSink::~CapturingSink() { internal::ResetForTesting(); }

std::vector<CapturedRecord> CapturingSink::Records() const {
  if (!impl_ || !impl_->sink) return {};
  return impl_->sink->Snapshot();
}

size_t CapturingSink::Size() const noexcept {
  if (!impl_ || !impl_->sink) return 0;
  return impl_->sink->Size();
}

void CapturingSink::Clear() {
  if (!impl_ || !impl_->sink) return;
  impl_->sink->ClearAll();
}

void Reset() { internal::ResetForTesting(); }

std::string FormatJson(Level level, std::string_view component, std::string_view msg,
                       std::span<const LogField> fields) {
  return internal::FormatJsonForTesting(level, component, msg, fields);
}

std::string FormatText(Level level, std::string_view component, std::string_view msg,
                       std::span<const LogField> fields) {
  return internal::FormatTextForTesting(level, component, msg, fields);
}

#else  // !ABYSS_WITH_LOGGING

CapturingSink::CapturingSink() : impl_(std::make_unique<Impl>()) {}
CapturingSink::~CapturingSink() = default;
std::vector<CapturedRecord> CapturingSink::Records() const { return {}; }
size_t CapturingSink::Size() const noexcept { return 0; }
void CapturingSink::Clear() {}
void Reset() {}

std::string FormatJson(Level, std::string_view, std::string_view, std::span<const LogField>) {
  return {};
}
std::string FormatText(Level, std::string_view, std::string_view, std::span<const LogField>) {
  return {};
}

#endif  // ABYSS_WITH_LOGGING

}  // namespace abyss::log::testing
