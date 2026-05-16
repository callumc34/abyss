#include "key_dist.h"

#include <cmath>
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

}  // namespace

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
      return std::make_unique<ZipfianDistribution>(key_count, config.theta, config.seed);
    case KeyDistConfig::Kind::kLatest:
      return std::make_unique<LatestDistribution>(key_count, config.theta, config.seed);
  }
  throw std::invalid_argument("MakeKeyDistribution: unknown distribution kind");
}

}  // namespace abyss::perf
