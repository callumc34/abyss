#pragma once

#include <cstdint>
#include <memory>
#include <random>

namespace abyss::perf {

// Stream of keys over [0, key_count). Stateful; not thread-safe.
class KeyDistribution {
 public:
  KeyDistribution() = default;
  virtual ~KeyDistribution() = default;
  KeyDistribution(const KeyDistribution&) = delete;
  KeyDistribution& operator=(const KeyDistribution&) = delete;
  KeyDistribution(KeyDistribution&&) = delete;
  KeyDistribution& operator=(KeyDistribution&&) = delete;

  virtual uint64_t Next() = 0;
  virtual uint64_t KeyCount() const = 0;
};

class UniformDistribution final : public KeyDistribution {
 public:
  UniformDistribution(uint64_t key_count, uint64_t seed);
  uint64_t Next() override;
  uint64_t KeyCount() const override { return key_count_; }

 private:
  uint64_t key_count_;
  std::mt19937_64 rng_;
};

// YCSB Zipfian. theta in [0, 1); 0 → uniform, 0.99 → strong skew.
// Construction precomputes zeta in O(key_count); Next() is O(1).
class ZipfianDistribution final : public KeyDistribution {
 public:
  ZipfianDistribution(uint64_t key_count, double theta, uint64_t seed);
  uint64_t Next() override;
  uint64_t KeyCount() const override { return key_count_; }
  double Theta() const { return theta_; }

 private:
  uint64_t key_count_;
  double theta_;
  double zeta_n_;
  double zeta_2_;
  double alpha_;
  double eta_;
  std::mt19937_64 rng_;
  std::uniform_real_distribution<double> uniform_;
};

// YCSB Latest. Emphasises the highest-numbered keys.
class LatestDistribution final : public KeyDistribution {
 public:
  LatestDistribution(uint64_t key_count, double theta, uint64_t seed);
  uint64_t Next() override;
  uint64_t KeyCount() const override { return key_count_; }

 private:
  uint64_t key_count_;
  ZipfianDistribution zipf_;
};

struct KeyDistConfig {
  enum class Kind { kUniform, kZipfian, kLatest };
  Kind kind = Kind::kUniform;
  double theta = 0.99;
  uint64_t seed = 0;
};

std::unique_ptr<KeyDistribution> MakeKeyDistribution(const KeyDistConfig& config,
                                                     uint64_t key_count);

}  // namespace abyss::perf
