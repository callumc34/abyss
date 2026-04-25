#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>

#include "abyss/core/resp_types.h"
#include "abyss/resp/admin_handlers.h"
#include "abyss/resp/server_stats.h"

namespace abyss::resp {
namespace {

enum class Section : uint8_t {
  kServer = 1U << 0U,
  kClients = 1U << 1U,
  kMemory = 1U << 2U,
  kKeyspace = 1U << 3U,
};

constexpr uint8_t kDefaultSections =
    static_cast<uint8_t>(Section::kServer) | static_cast<uint8_t>(Section::kClients) |
    static_cast<uint8_t>(Section::kMemory) | static_cast<uint8_t>(Section::kKeyspace);

std::string Lowercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return out;
}

// Returns the section bitmask, or 0 when the caller passed an unrecognised name.
uint8_t ResolveSection(std::string_view requested) {
  const auto lower = Lowercase(requested);
  if (lower == "all" || lower == "everything" || lower == "default" || lower.empty()) {
    return kDefaultSections;
  }
  if (lower == "server") return static_cast<uint8_t>(Section::kServer);
  if (lower == "clients") return static_cast<uint8_t>(Section::kClients);
  if (lower == "memory") return static_cast<uint8_t>(Section::kMemory);
  if (lower == "keyspace") return static_cast<uint8_t>(Section::kKeyspace);
  return 0;
}

void AppendField(std::string& out, std::string_view key, std::string_view value) {
  out.append(key);
  out.push_back(':');
  out.append(value);
  out.append("\r\n");
}

void AppendField(std::string& out, std::string_view key, uint64_t value) {
  AppendField(out, key, std::to_string(value));
}

void AppendServer(std::string& out, const ServerStats& s) {
  out.append("# Server\r\n");
  AppendField(out, "abyss_version", s.version);
  AppendField(out, "redis_version", "7.0.0");
  AppendField(out, "mode", s.mode);
  AppendField(out, "role", s.role);
  AppendField(out, "os", "abyss");
  AppendField(out, "tcp_port", static_cast<uint64_t>(s.tcp_port));
  AppendField(out, "process_id", s.process_id);
  AppendField(out, "uptime_in_seconds", static_cast<uint64_t>(s.uptime_seconds));
}

void AppendClients(std::string& out, const ServerStats& s) {
  out.append("# Clients\r\n");
  AppendField(out, "connected_clients", s.connected_clients);
}

void AppendMemory(std::string& out, const ServerStats& s) {
  out.append("# Memory\r\n");
  AppendField(out, "used_memory", s.hot_memory_bytes);
  AppendField(out, "used_memory_human", std::to_string(s.hot_memory_bytes) + "B");
}

void AppendKeyspace(std::string& out, const ServerStats& s) {
  out.append("# Keyspace\r\n");
  const auto total = s.hot_key_count + s.cold_key_count;
  if (total > 0) {
    std::string line = "db0:keys=";
    line.append(std::to_string(total));
    line.append(",expires=0,avg_ttl=0\r\n");
    out.append(line);
  }
}

}  // namespace

core::RespValue HandleInfo(const core::RespCommand& cmd, const ServerStatsProvider& stats) {
  const std::string_view requested = cmd.ArgCount() > 1 ? std::string_view(cmd.args[1]) : "";
  const uint8_t mask = ResolveSection(requested);
  if (mask == 0) {
    return core::RespValue::BulkString("");
  }

  const auto snapshot = stats.Snapshot();
  std::string out;
  out.reserve(512);
  bool first = true;
  auto maybe_newline = [&] {
    if (!first) out.append("\r\n");
    first = false;
  };

  if ((mask & static_cast<uint8_t>(Section::kServer)) != 0) {
    maybe_newline();
    AppendServer(out, snapshot);
  }
  if ((mask & static_cast<uint8_t>(Section::kClients)) != 0) {
    maybe_newline();
    AppendClients(out, snapshot);
  }
  if ((mask & static_cast<uint8_t>(Section::kMemory)) != 0) {
    maybe_newline();
    AppendMemory(out, snapshot);
  }
  if ((mask & static_cast<uint8_t>(Section::kKeyspace)) != 0) {
    maybe_newline();
    AppendKeyspace(out, snapshot);
  }
  return core::RespValue::BulkString(std::move(out));
}

core::RespValue HandleDbsize(const ServerStatsProvider& stats) {
  const auto snap = stats.Snapshot();
  return core::RespValue::Integer(static_cast<int64_t>(snap.hot_key_count + snap.cold_key_count));
}

}  // namespace abyss::resp
