#include "key_dist.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <stdexcept>

namespace abyss::perf {

namespace {

double ComputeZeta(uint64_t n, double theta) {
  double sum = 0.0;
  // NOLINTNEXTLINE(bugprone-infinite-loop): false positive; ++i in update expression.
  for (uint64_t i = 1; i <= n; ++i) {
    sum += std::pow(1.0 / static_cast<double>(i), theta);
  }
  return sum;
}

// log1p(x) / x, and (exp(x) - 1) / x, accurate near 0.
double Log1pOverX(double x) {
  constexpr double kSmall = 1e-8;
  if (std::abs(x) > kSmall) return std::log1p(x) / x;
  return 1.0 - (x * (0.5 - (x * ((1.0 / 3.0) - (0.25 * x)))));
}

double Expm1OverX(double x) {
  constexpr double kSmall = 1e-8;
  if (std::abs(x) > kSmall) return std::expm1(x) / x;
  return 1.0 + (x * 0.5 * (1.0 + (x / 3.0 * (1.0 + (0.25 * x)))));
}

}  // namespace

ZipfRejectionInversion::ZipfRejectionInversion(uint64_t key_count, double exponent, uint64_t seed)
    : key_count_(key_count), exponent_(exponent), rng_(seed), uniform_(0.0, 1.0) {
  if (key_count_ == 0) {
    throw std::invalid_argument("ZipfRejectionInversion: key_count must be > 0");
  }
  if (exponent_ <= 0.0) {
    throw std::invalid_argument("ZipfRejectionInversion: exponent must be > 0");
  }
  h_integral_x1_ = HIntegral(1.5) - 1.0;
  h_integral_n_ = HIntegral(static_cast<double>(key_count_) + 0.5);
  s_ = 2.0 - HIntegralInverse(HIntegral(2.5) - H(2.0));
}

double ZipfRejectionInversion::H(double x) const { return std::exp(-exponent_ * std::log(x)); }

double ZipfRejectionInversion::HIntegral(double x) const {
  const double log_x = std::log(x);
  return Expm1OverX((1.0 - exponent_) * log_x) * log_x;
}

double ZipfRejectionInversion::HIntegralInverse(double x) const {
  const double t = std::max(x * (1.0 - exponent_), -1.0);
  return std::exp(Log1pOverX(t) * x);
}

// NOLINTNEXTLINE(readability-make-member-function-const): uniform_ and rng_ are mutated.
uint64_t ZipfRejectionInversion::Next() {
  const auto n = static_cast<double>(key_count_);
  for (;;) {
    const double u = h_integral_n_ + (uniform_(rng_) * (h_integral_x1_ - h_integral_n_));
    const double x = HIntegralInverse(u);
    const double k = std::clamp(std::floor(x + 0.5), 1.0, n);
    if (k - x <= s_ || u >= HIntegral(k + 0.5) - H(k)) return static_cast<uint64_t>(k) - 1;
  }
}

UniformDistribution::UniformDistribution(uint64_t key_count, uint64_t seed)
    : key_count_(key_count), rng_(seed) {
  if (key_count_ == 0) {
    throw std::invalid_argument("UniformDistribution: key_count must be > 0");
  }
}

uint64_t UniformDistribution::Next() {
  std::uniform_int_distribution<uint64_t> dist(0, key_count_ - 1);
  return dist(rng_);
}

ZipfianDistribution::ZipfianDistribution(uint64_t key_count, double theta, uint64_t seed)
    : key_count_(key_count), theta_(theta), rng_(seed), uniform_(0.0, 1.0) {
  if (key_count_ == 0) {
    throw std::invalid_argument("ZipfianDistribution: key_count must be > 0");
  }
  if (theta_ < 0.0 || theta_ >= 1.0) {
    throw std::invalid_argument("ZipfianDistribution: theta must be in [0, 1)");
  }
  zeta_n_ = ComputeZeta(key_count_, theta_);
  zeta_2_ = ComputeZeta(2, theta_);
  alpha_ = 1.0 / (1.0 - theta_);
  eta_ = (1.0 - std::pow(2.0 / static_cast<double>(key_count_), 1.0 - theta_)) /
         (1.0 - (zeta_2_ / zeta_n_));
}

// NOLINTNEXTLINE(readability-make-member-function-const): uniform_ and rng_ are mutated.
uint64_t ZipfianDistribution::Next() {
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables): false positive on function-call init.
  const double u = uniform_(rng_);
  const double uz = u * zeta_n_;
  if (uz < 1.0) {
    return 0;
  }
  if (uz < 1.0 + std::pow(0.5, theta_)) {
    return 1;
  }
  const auto idx = static_cast<uint64_t>(static_cast<double>(key_count_) *
                                         std::pow(((eta_ * u) - eta_) + 1.0, alpha_));
  return idx < key_count_ ? idx : key_count_ - 1;
}

LatestDistribution::LatestDistribution(uint64_t key_count, double theta, uint64_t seed)
    : key_count_(key_count), zipf_(key_count, theta, seed) {}

uint64_t LatestDistribution::Next() {
  const uint64_t z = zipf_.Next();
  return key_count_ - 1 - z;
}

std::unique_ptr<KeyDistribution> MakeKeyDistribution(const KeyDistConfig& config,
                                                     uint64_t key_count) {
  switch (config.kind) {
    case KeyDistConfig::Kind::kUniform:
      return std::make_unique<UniformDistribution>(key_count, config.seed);
    case KeyDistConfig::Kind::kZipfian:
      if (config.theta >= 1.0) {
        return std::make_unique<ZipfRejectionInversion>(key_count, config.theta, config.seed);
      }
      return std::make_unique<ZipfianDistribution>(key_count, config.theta, config.seed);
    case KeyDistConfig::Kind::kLatest:
      return std::make_unique<LatestDistribution>(key_count, config.theta, config.seed);
  }
  throw std::invalid_argument("MakeKeyDistribution: unknown distribution kind");
}

}  // namespace abyss::perf
