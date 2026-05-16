#pragma once

#include <cstdint>
#include <iosfwd>
#include <string>

struct hdr_histogram;

namespace abyss::perf {

// Latency histogram backed by HdrHistogram_c. Values are stored as raw int64
// counts; the framework convention is nanoseconds. Instances are NOT
// internally thread-safe: use one per worker thread and Merge at run end.
class Histogram {
 public:
  static constexpr int64_t kDefaultLowestNs = 100;
  static constexpr int64_t kDefaultHighestNs = 60LL * 1'000'000'000LL;
  static constexpr int kDefaultSignificantDigits = 3;

  Histogram(int64_t lowest_ns, int64_t highest_ns, int significant_digits);
  Histogram() : Histogram(kDefaultLowestNs, kDefaultHighestNs, kDefaultSignificantDigits) {}
  ~Histogram();

  Histogram(const Histogram&) = delete;
  Histogram& operator=(const Histogram&) = delete;
  Histogram(Histogram&& other) noexcept;
  Histogram& operator=(Histogram&& other) noexcept;

  void Record(int64_t value_ns);

  // Records value_ns and, when value_ns > expected_interval_ns, additional
  // synthetic samples spaced by expected_interval_ns to compensate for
  // coordinated omission. See ADP-013 §Coordinated omission.
  void RecordCorrected(int64_t value_ns, int64_t expected_interval_ns);

  int64_t PercentileNs(double percentile) const;
  int64_t Count() const;
  int64_t MinNs() const;
  int64_t MaxNs() const;
  double MeanNs() const;
  double StdDevNs() const;

  // True once any value above highest_ns was recorded.
  bool Saturated() const { return saturated_; }

  void Reset();
  void Merge(const Histogram& other);

  std::string EncodeBase64() const;
  void WriteHgrmTo(std::ostream& out) const;

  int64_t LowestNs() const { return lowest_ns_; }
  int64_t HighestNs() const { return highest_ns_; }

 private:
  hdr_histogram* hdr_ = nullptr;
  int64_t lowest_ns_;
  int64_t highest_ns_;
  bool saturated_ = false;
};

}  // namespace abyss::perf
