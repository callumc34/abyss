#include "abyss/core/topology_manifest.h"

#include <charconv>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>

#include "abyss/core/atomic_file.h"

namespace abyss::core {
namespace {

// Bumped only when the manifest's own serialization changes. A persisted
// manifest_version newer than this binary understands is refuse-to-start.
constexpr uint32_t kManifestVersion = 1;

constexpr const char* kManifestFile = "topology.manifest";

Error Corruption(std::string msg) { return {ErrorCode::kCorruption, std::move(msg)}; }

std::string Serialize(uint32_t manifest_version, const TopologyDescriptor& d) {
  std::string out;
  out.append("manifest_version=").append(std::to_string(manifest_version)).push_back('\n');
  out.append("shard_count=").append(std::to_string(d.shard_count)).push_back('\n');
  out.append("cold_format_epoch=").append(std::to_string(d.cold_format_epoch)).push_back('\n');
  out.append("wire_slot_hash=").append(d.wire_slot_hash).push_back('\n');
  out.append("data_shard_hash=").append(d.data_shard_hash).push_back('\n');
  return out;
}

Result<std::unordered_map<std::string, std::string>> ParseFields(const std::string& text) {
  std::unordered_map<std::string, std::string> fields;
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    const auto eq = line.find('=');
    if (eq == std::string::npos) {
      return std::unexpected(Corruption("topology manifest line has no '=': '" + line + "'"));
    }
    fields.emplace(line.substr(0, eq), line.substr(eq + 1));
  }
  return fields;
}

Result<uint32_t> ParseU32(const std::string& field, const std::string& value) {
  uint32_t out = 0;
  const auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), out);
  if (ec != std::errc{} || ptr != value.data() + value.size()) {
    return std::unexpected(Corruption("topology manifest field '" + field +
                                      "' is not a valid integer: '" + value + "'"));
  }
  return out;
}

}  // namespace

Result<TopologyManifest> TopologyManifest::OpenOrValidate(const std::filesystem::path& data_dir,
                                                          const TopologyDescriptor& effective) {
  const auto path = data_dir / kManifestFile;

  std::error_code exists_ec;
  if (!std::filesystem::exists(path, exists_ec)) {
    // Fresh data dir: persist the effective topology durably.
    const std::string text = Serialize(kManifestVersion, effective);
    if (auto r = WriteFileAtomic(path, text); !r.has_value()) {
      return std::unexpected(r.error());
    }
    return TopologyManifest{effective};
  }

  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return std::unexpected(
        Error{ErrorCode::kInternal, "failed to open topology manifest '" + path.string() + "'"});
  }
  std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());

  auto fields = ParseFields(text);
  if (!fields.has_value()) return std::unexpected(fields.error());

  auto field = [&](const char* name) -> Result<std::string> {
    auto it = fields->find(name);
    if (it == fields->end()) {
      return std::unexpected(
          Corruption(std::string("topology manifest missing field '") + name + "'"));
    }
    return it->second;
  };

  auto version_str = field("manifest_version");
  if (!version_str.has_value()) return std::unexpected(version_str.error());
  auto version = ParseU32("manifest_version", *version_str);
  if (!version.has_value()) return std::unexpected(version.error());
  if (*version > kManifestVersion) {
    return std::unexpected(
        Corruption("topology manifest version " + std::to_string(*version) +
                   " is newer than this binary supports (" + std::to_string(kManifestVersion) +
                   "); refusing to start to avoid mis-decoding a future format"));
  }

  TopologyDescriptor persisted;

  auto shard_count_str = field("shard_count");
  if (!shard_count_str.has_value()) return std::unexpected(shard_count_str.error());
  auto shard_count = ParseU32("shard_count", *shard_count_str);
  if (!shard_count.has_value()) return std::unexpected(shard_count.error());
  persisted.shard_count = *shard_count;

  auto epoch_str = field("cold_format_epoch");
  if (!epoch_str.has_value()) return std::unexpected(epoch_str.error());
  auto epoch = ParseU32("cold_format_epoch", *epoch_str);
  if (!epoch.has_value()) return std::unexpected(epoch.error());
  persisted.cold_format_epoch = static_cast<uint16_t>(*epoch);

  auto wire = field("wire_slot_hash");
  if (!wire.has_value()) return std::unexpected(wire.error());
  persisted.wire_slot_hash = std::move(*wire);

  auto data_shard = field("data_shard_hash");
  if (!data_shard.has_value()) return std::unexpected(data_shard.error());
  persisted.data_shard_hash = std::move(*data_shard);

  // Refuse to start on ANY mismatch: starting against a topology the data was
  // not written under would silently re-route WAL dirs / re-encode cold keys.
  auto mismatch = [&](const char* what, const std::string& got,
                      const std::string& want) -> Result<TopologyManifest> {
    return std::unexpected(
        Corruption(std::string("topology mismatch: persisted ") + what + "=" + got +
                   " but configured " + what + "=" + want +
                   "; refusing to start (the on-disk data was written under a different topology — "
                   "restore the original configuration or wipe the data directory)"));
  };

  if (persisted.shard_count != effective.shard_count) {
    return mismatch("shard_count", std::to_string(persisted.shard_count),
                    std::to_string(effective.shard_count));
  }
  if (persisted.cold_format_epoch != effective.cold_format_epoch) {
    return mismatch("cold_format_epoch", std::to_string(persisted.cold_format_epoch),
                    std::to_string(effective.cold_format_epoch));
  }
  if (persisted.wire_slot_hash != effective.wire_slot_hash) {
    return mismatch("wire_slot_hash", persisted.wire_slot_hash, effective.wire_slot_hash);
  }
  if (persisted.data_shard_hash != effective.data_shard_hash) {
    return mismatch("data_shard_hash", persisted.data_shard_hash, effective.data_shard_hash);
  }

  return TopologyManifest{std::move(persisted)};
}

}  // namespace abyss::core
