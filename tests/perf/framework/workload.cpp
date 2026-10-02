#include "workload.h"

#include <yaml-cpp/yaml.h>

#include <chrono>
#include <cmath>
#include <fstream>
#include <sstream>
#include <string>

namespace abyss::perf {

namespace {

using core::Error;
using core::ErrorCode;
using core::Result;

constexpr double kMixWeightSumEpsilon = 1e-6;
// Bytes one connection may have in flight. The driver writes blocking,
// so a window near the server's read backpressure pause could stall it.
constexpr uint64_t kMaxPipelineWindowBytes = uint64_t{1} << 20;

[[nodiscard]] Error Invalid(std::string message) {
  return {ErrorCode::kInvalidArgument, std::move(message)};
}

Result<KeyDistConfig::Kind> ParseDistKind(const std::string& s) {
  if (s == "uniform") return KeyDistConfig::Kind::kUniform;
  if (s == "zipfian") return KeyDistConfig::Kind::kZipfian;
  if (s == "latest") return KeyDistConfig::Kind::kLatest;
  return std::unexpected(Invalid("unknown key_distribution.kind: " + s));
}

Result<KeyDistConfig> ParseKeyDist(const YAML::Node& node) {
  if (!node || !node.IsMap()) {
    return std::unexpected(Invalid("key_distribution must be a map"));
  }
  KeyDistConfig out;
  if (!node["kind"]) {
    return std::unexpected(Invalid("key_distribution.kind is required"));
  }
  auto kind = ParseDistKind(node["kind"].as<std::string>());
  if (!kind.has_value()) {
    return std::unexpected(kind.error());
  }
  out.kind = *kind;
  if (node["theta"]) {
    out.theta = node["theta"].as<double>();
  }
  if (node["seed"]) {
    out.seed = node["seed"].as<uint64_t>();
  }
  return out;
}

Result<OperationMix> ParseMix(const YAML::Node& node) {
  if (!node || !node.IsMap()) {
    return std::unexpected(Invalid("mix must be a map of op-name to weight"));
  }
  OperationMix mix;
  double sum = 0.0;
  for (const auto& entry : node) {
    const auto name = entry.first.as<std::string>();
    const auto weight = entry.second.as<double>();
    if (weight < 0.0 || weight > 1.0) {
      return std::unexpected(Invalid("mix weight for " + name + " must be in [0, 1]"));
    }
    mix.weights[name] = weight;
    sum += weight;
  }
  if (mix.weights.empty()) {
    return std::unexpected(Invalid("mix must contain at least one operation"));
  }
  if (std::fabs(sum - 1.0) > kMixWeightSumEpsilon) {
    return std::unexpected(Invalid("mix weights must sum to 1.0, got " + std::to_string(sum)));
  }
  return mix;
}

Result<PreloadConfig> ParsePreload(const YAML::Node& node) {
  PreloadConfig out;
  if (!node) return out;
  if (!node.IsMap()) {
    return std::unexpected(Invalid("preload must be a map"));
  }
  if (node["enabled"]) out.enabled = node["enabled"].as<bool>();
  if (node["key_count"]) out.key_count = node["key_count"].as<uint64_t>();
  if (node["value_size_bytes"]) {
    out.value_size_bytes = node["value_size_bytes"].as<uint64_t>();
  }
  return out;
}

Result<TargetSpec> ParseTargetSpec(const YAML::Node& node) {
  if (!node.IsMap()) {
    return std::unexpected(Invalid("target spec must be a map"));
  }
  TargetSpec spec;
  if (node["p50_us"]) spec.p50_us = node["p50_us"].as<int64_t>();
  if (node["p99_us"]) spec.p99_us = node["p99_us"].as<int64_t>();
  if (node["p999_us"]) spec.p999_us = node["p999_us"].as<int64_t>();
  return spec;
}

Result<WorkloadTargets> ParseTargets(const YAML::Node& node, const OperationMix& mix) {
  WorkloadTargets out;
  if (!node) return out;
  if (!node.IsMap()) {
    return std::unexpected(Invalid("targets must be a map"));
  }
  if (node["throughput_ops"]) {
    out.throughput_ops = node["throughput_ops"].as<int64_t>();
  }
  if (const auto& per_op = node["per_op"]; per_op) {
    if (!per_op.IsMap()) {
      return std::unexpected(Invalid("targets.per_op must be a map"));
    }
    for (const auto& entry : per_op) {
      const auto op_name = entry.first.as<std::string>();
      if (!mix.weights.contains(op_name)) {
        return std::unexpected(
            Invalid("targets.per_op references " + op_name + " not present in mix"));
      }
      auto spec = ParseTargetSpec(entry.second);
      if (!spec.has_value()) {
        return std::unexpected(spec.error());
      }
      out.per_op[op_name] = *spec;
    }
  }
  return out;
}

}  // namespace

Result<WorkloadConfig> ParseWorkloadYaml(const std::string& yaml) {
  YAML::Node root;
  try {
    root = YAML::Load(yaml);
    // NOLINTNEXTLINE(bugprone-empty-catch): false positive; the catch returns.
  } catch (const YAML::Exception& ex) {
    return std::unexpected(Invalid(std::string{"yaml parse failed: "} + ex.what()));
  }
  if (!root || !root.IsMap()) {
    return std::unexpected(Invalid("workload root must be a map"));
  }

  WorkloadConfig cfg;
  if (!root["name"]) return std::unexpected(Invalid("name is required"));
  cfg.name = root["name"].as<std::string>();
  if (cfg.name.empty()) return std::unexpected(Invalid("name must be non-empty"));

  if (root["description"]) cfg.description = root["description"].as<std::string>();

  if (!root["duration_seconds"]) {
    return std::unexpected(Invalid("duration_seconds is required"));
  }
  cfg.duration = std::chrono::seconds{root["duration_seconds"].as<int64_t>()};
  if (cfg.duration.count() <= 0) {
    return std::unexpected(Invalid("duration_seconds must be > 0"));
  }

  if (root["warmup_seconds"]) {
    cfg.warmup = std::chrono::seconds{root["warmup_seconds"].as<int64_t>()};
    if (cfg.warmup.count() < 0) {
      return std::unexpected(Invalid("warmup_seconds must be >= 0"));
    }
  }

  if (root["workers"]) cfg.workers = root["workers"].as<int>();
  if (cfg.workers < 1) return std::unexpected(Invalid("workers must be >= 1"));

  if (root["connections_per_worker"]) {
    cfg.connections_per_worker = root["connections_per_worker"].as<int>();
  }
  if (cfg.connections_per_worker < 1) {
    return std::unexpected(Invalid("connections_per_worker must be >= 1"));
  }

  if (root["pipeline_depth"]) cfg.pipeline_depth = root["pipeline_depth"].as<int>();
  if (cfg.pipeline_depth < 1) return std::unexpected(Invalid("pipeline_depth must be >= 1"));

  if (root["arrival"]) {
    const auto arrival = root["arrival"].as<std::string>();
    if (arrival == "burst") {
      cfg.arrival = Arrival::kBurst;
    } else if (arrival != "steady") {
      return std::unexpected(Invalid("arrival must be steady or burst, got " + arrival));
    }
  }

  if (root["target_rate_ops"]) {
    cfg.target_rate_ops = root["target_rate_ops"].as<uint64_t>();
  }

  if (!root["key_count"]) return std::unexpected(Invalid("key_count is required"));
  cfg.key_count = root["key_count"].as<uint64_t>();
  if (cfg.key_count == 0) return std::unexpected(Invalid("key_count must be > 0"));

  auto kd = ParseKeyDist(root["key_distribution"]);
  if (!kd.has_value()) return std::unexpected(kd.error());
  cfg.key_distribution = *kd;

  if (root["value_size_bytes"]) {
    cfg.value_size_bytes = root["value_size_bytes"].as<uint64_t>();
  }
  if (cfg.value_size_bytes == 0) {
    return std::unexpected(Invalid("value_size_bytes must be > 0"));
  }
  if (static_cast<uint64_t>(cfg.pipeline_depth) * cfg.value_size_bytes > kMaxPipelineWindowBytes) {
    return std::unexpected(Invalid(
        "pipeline_depth x value_size_bytes must be <= " + std::to_string(kMaxPipelineWindowBytes) +
        ": the load generator writes each window blocking, so a window approaching the "
        "server's read backpressure pause can stall the connection"));
  }
  if (cfg.arrival == Arrival::kBurst && cfg.target_rate_ops == 0) {
    return std::unexpected(Invalid("arrival: burst needs target_rate_ops > 0 (open loop)"));
  }

  auto mix = ParseMix(root["mix"]);
  if (!mix.has_value()) return std::unexpected(mix.error());
  cfg.mix = std::move(*mix);

  auto preload = ParsePreload(root["preload"]);
  if (!preload.has_value()) return std::unexpected(preload.error());
  cfg.preload = *preload;

  auto targets = ParseTargets(root["targets"], cfg.mix);
  if (!targets.has_value()) return std::unexpected(targets.error());
  cfg.targets = std::move(*targets);

  return cfg;
}

Result<WorkloadConfig> LoadWorkloadFile(const std::string& path) {
  std::ifstream stream{path};
  if (!stream.is_open()) {
    return std::unexpected(Invalid("could not open workload file: " + path));
  }
  std::stringstream buf;
  buf << stream.rdbuf();
  return ParseWorkloadYaml(buf.str());
}

}  // namespace abyss::perf
