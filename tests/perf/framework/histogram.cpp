#include "histogram.h"

#include <hdr/hdr_histogram.h>
#include <hdr/hdr_histogram_log.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>

namespace abyss::perf {

Histogram::Histogram(int64_t lowest_ns, int64_t highest_ns, int significant_digits)
    : lowest_ns_(lowest_ns), highest_ns_(highest_ns) {
  if (hdr_init(lowest_ns, highest_ns, significant_digits, &hdr_) != 0) {
    throw std::runtime_error("hdr_init failed");
  }
}

Histogram::~Histogram() {
  if (hdr_ != nullptr) {
    hdr_close(hdr_);
  }
}

Histogram::Histogram(Histogram&& other) noexcept
    : hdr_(other.hdr_),
      lowest_ns_(other.lowest_ns_),
      highest_ns_(other.highest_ns_),
      saturated_(other.saturated_) {
  other.hdr_ = nullptr;
}

Histogram& Histogram::operator=(Histogram&& other) noexcept {
  if (this != &other) {
    if (hdr_ != nullptr) hdr_close(hdr_);
    hdr_ = other.hdr_;
    lowest_ns_ = other.lowest_ns_;
    highest_ns_ = other.highest_ns_;
    saturated_ = other.saturated_;
    other.hdr_ = nullptr;
  }
  return *this;
}

void Histogram::Record(int64_t value_ns) {
  if (value_ns > highest_ns_) {
    saturated_ = true;
    value_ns = highest_ns_;
  } else if (value_ns < 0) {
    value_ns = 0;
  }
  hdr_record_value(hdr_, value_ns);
}

void Histogram::RecordCorrected(int64_t value_ns, int64_t expected_interval_ns) {
  if (value_ns > highest_ns_) {
    saturated_ = true;
    value_ns = highest_ns_;
  } else if (value_ns < 0) {
    value_ns = 0;
  }
  hdr_record_corrected_value(hdr_, value_ns, expected_interval_ns);
}

int64_t Histogram::PercentileNs(double percentile) const {
  return hdr_value_at_percentile(hdr_, percentile);
}

int64_t Histogram::Count() const { return hdr_->total_count; }

int64_t Histogram::MinNs() const { return Count() > 0 ? hdr_min(hdr_) : 0; }

int64_t Histogram::MaxNs() const { return Count() > 0 ? hdr_max(hdr_) : 0; }

double Histogram::MeanNs() const { return Count() > 0 ? hdr_mean(hdr_) : 0.0; }

double Histogram::StdDevNs() const { return Count() > 0 ? hdr_stddev(hdr_) : 0.0; }

void Histogram::Reset() {
  hdr_reset(hdr_);
  saturated_ = false;
}

void Histogram::Merge(const Histogram& other) {
  hdr_add(hdr_, other.hdr_);
  saturated_ = saturated_ || other.saturated_;
}

std::string Histogram::EncodeBase64() const {
  char* encoded = nullptr;
  const int rc = hdr_log_encode(hdr_, &encoded);
  if (rc != 0 || encoded == nullptr) {
    return {};
  }
  const std::unique_ptr<char, decltype(&std::free)> owner(encoded, &std::free);
  return std::string{owner.get()};
}

void Histogram::WriteHgrmTo(std::ostream& out) const {
  out << "       Value     Percentile TotalCount 1/(1-Percentile)\n";
  hdr_iter iter;
  hdr_iter_percentile_init(&iter, hdr_, 5);
  while (hdr_iter_next(&iter)) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-union-access): public HdrHistogram iter API
    const double percentile = iter.specifics.percentiles.percentile / 100.0;
    const double inv = (percentile < 1.0) ? 1.0 / (1.0 - percentile) : 0.0;
    constexpr int kLineBufferBytes = 128;
    std::array<char, kLineBufferBytes> line{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    std::snprintf(line.data(), line.size(), "%12lld %14.6f %10lld %16.2f\n",
                  static_cast<long long>(iter.value), percentile,
                  static_cast<long long>(iter.cumulative_count), inv);
    out << line.data();
  }
  constexpr int kFooterLineBytes = 96;
  std::array<char, kFooterLineBytes> footer{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(footer.data(), footer.size(), "#[Mean    = %15.3f, StdDeviation   = %15.3f]\n",
                MeanNs(), StdDevNs());
  out << footer.data();
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(footer.data(), footer.size(), "#[Max     = %15lld, Total count    = %15lld]\n",
                static_cast<long long>(MaxNs()), static_cast<long long>(Count()));
  out << footer.data();
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  std::snprintf(footer.data(), footer.size(), "#[Buckets = %15d, SubBuckets     = %15d]\n",
                hdr_->bucket_count, hdr_->sub_bucket_count);
  out << footer.data();
}

}  // namespace abyss::perf
