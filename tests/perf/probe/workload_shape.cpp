#include "workload_shape.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace abyss::perf::probe {

namespace {

constexpr uint64_t kLargeMin = uint64_t{16} << 10;
constexpr uint64_t kLargeMax = uint64_t{64} << 10;
constexpr uint64_t kMembers = 64;
constexpr size_t kFixedDigits = 20;

// A key's class bits: the same for every run and every worker.
uint64_t Mix(uint64_t x) {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30U)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27U)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31U);
}

uint64_t Between(std::mt19937_64& rng, uint64_t lo, uint64_t hi) {
  return std::uniform_int_distribution<uint64_t>(lo, hi)(rng);
}

// `prefix` then `index` zero-padded to `digits`.
std::string Padded(char prefix, uint64_t index, size_t digits) {
  std::array<char, 24> buf{};
  auto* const end = std::to_chars(buf.data(), buf.data() + buf.size(), index).ptr;
  const auto len = static_cast<size_t>(end - buf.data());
  std::string out(1, prefix);
  out.append(digits > len ? digits - len : 0, '0');
  out.append(buf.data(), len);
  return out;
}

std::string Member(std::mt19937_64& rng) {
  return "m" + std::to_string(Between(rng, 0, kMembers - 1));
}

}  // namespace

WorkloadShape::WorkloadShape(ShapeConfig config, uint64_t key_count)
    : config_(config), filler_(kLargeMax, 'x') {
  keys_.reserve(key_count);
  for (uint64_t i = 0; i < key_count; ++i) {
    if (!config_.trace) {
      keys_.push_back(Padded('k', i, kFixedDigits));
      continue;
    }
    // 10 to 50 bytes; one length per index, so keys stay distinct.
    const size_t length = 10 + static_cast<size_t>((Mix(i) >> 16U) % 41);
    keys_.push_back(Padded('t', i, length - 1));
  }
}

KeyType WorkloadShape::TypeOf(uint64_t index) const {
  if (!config_.trace) return KeyType::kString;
  constexpr uint64_t kSteps = 1000;
  const double u = static_cast<double>((Mix(index) >> 32U) % kSteps) / kSteps;
  if (u < config_.collection_fraction / 2) return KeyType::kSet;
  if (u < config_.collection_fraction) return KeyType::kZset;
  return KeyType::kString;
}

uint64_t WorkloadShape::TtlMs(uint64_t index, std::mt19937_64& rng) const {
  if (!config_.trace) return 0;
  // (share of keys up to, TTL in seconds); the first 15% have none.
  constexpr std::array<std::pair<uint64_t, uint64_t>, 4> kTtls{
      {{40, 60}, {55, 240}, {80, 3600}, {100, 86400}}};
  const uint64_t pick = Mix(index) % 100;
  if (pick < 15) return 0;
  for (const auto& [up_to, seconds] : kTtls) {
    if (pick >= up_to) continue;
    const uint64_t base_ms = seconds * 1000;
    return Between(rng, base_ms - (base_ms / 10), base_ms + (base_ms / 10));
  }
  return 0;
}

uint64_t WorkloadShape::ValueBytes(std::mt19937_64& rng) const {
  if (!config_.trace) {
    if (config_.large_fraction > 0.0 &&
        std::uniform_real_distribution<double>(0.0, 1.0)(rng) < config_.large_fraction) {
      return Between(rng, kLargeMin, kLargeMax);
    }
    return config_.value_bytes;
  }
  const uint64_t pick = Between(rng, 0, 99);
  if (pick < 30) return Between(rng, 16, 99);
  if (pick < 94) return Between(rng, 100, 400);
  if (pick < 95) return Between(rng, 401, kLargeMin - 1);
  return Between(rng, kLargeMin, kLargeMax);
}

core::RespCommand WorkloadShape::SetOf(uint64_t index, uint64_t value_bytes,
                                       std::mt19937_64& rng) const {
  core::RespCommand cmd{.args = {"SET", keys_[index], filler_.substr(0, value_bytes)}};
  if (const uint64_t ttl = TtlMs(index, rng); ttl > 0) {
    cmd.args.emplace_back("PX");
    cmd.args.push_back(std::to_string(ttl));
  }
  return cmd;
}

core::RespCommand WorkloadShape::Write(uint64_t index, std::mt19937_64& rng) const {
  const KeyType type = TypeOf(index);
  if (type == KeyType::kString) return SetOf(index, ValueBytes(rng), rng);
  if (const uint64_t ttl = TtlMs(index, rng); ttl > 0 && Between(rng, 0, 3) == 0) {
    return core::RespCommand{.args = {"PEXPIRE", keys_[index], std::to_string(ttl)}};
  }
  if (type == KeyType::kSet) {
    return core::RespCommand{.args = {"SADD", keys_[index], Member(rng)}};
  }
  return core::RespCommand{
      .args = {"ZADD", keys_[index], std::to_string(Between(rng, 0, 1'000'000)), Member(rng)}};
}

core::RespCommand WorkloadShape::Read(uint64_t index, std::mt19937_64& rng) const {
  switch (TypeOf(index)) {
    case KeyType::kSet:
      return core::RespCommand{.args = {"SISMEMBER", keys_[index], Member(rng)}};
    case KeyType::kZset:
      return core::RespCommand{.args = {"ZSCORE", keys_[index], Member(rng)}};
    case KeyType::kString:
      break;
  }
  return core::RespCommand{.args = {"GET", keys_[index]}};
}

std::vector<core::RespCommand> WorkloadShape::Create(uint64_t index, std::mt19937_64& rng) const {
  std::vector<core::RespCommand> out;
  switch (TypeOf(index)) {
    case KeyType::kSet:
      out.push_back({.args = {"SADD", keys_[index], Member(rng)}});
      break;
    case KeyType::kZset:
      out.push_back({.args = {"ZADD", keys_[index], "1", Member(rng)}});
      break;
    case KeyType::kString:
      out.push_back(SetOf(index, std::min<uint64_t>(ValueBytes(rng), 400), rng));
      return out;
  }
  if (const uint64_t ttl = TtlMs(index, rng); ttl > 0) {
    out.push_back({.args = {"PEXPIRE", keys_[index], std::to_string(ttl)}});
  }
  return out;
}

}  // namespace abyss::perf::probe
