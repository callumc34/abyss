#include "abyss/admin/status_handler.h"

#include <string>

#include "abyss/admin/http_response.h"
#include "json_writer.h"

namespace abyss::admin {

namespace {

constexpr const char* kJsonContentType = "application/json; charset=utf-8";

void WriteEndpoint(internal::JsonWriter& w, const StatusEndpoint& ep) {
  w.BeginObject();
  w.Key("bind");
  w.String(ep.bind);
  w.Key("port");
  w.UInt(ep.port);
  if (ep.emit_advertise) {
    w.Key("advertise_address");
    w.String(ep.advertise_address);
  }
  if (ep.emit_enabled) {
    w.Key("enabled");
    w.Bool(ep.enabled);
  }
  w.EndObject();
}

std::string Render(const StatusSnapshot& s) {
  internal::JsonWriter w;
  w.BeginObject();

  w.Key("schema_version");
  w.UInt(s.schema_version);

  w.Key("abyss");
  {
    w.BeginObject();
    w.Key("version");
    w.String(s.build.version);
    w.Key("build");
    {
      w.BeginObject();
      w.Key("commit");
      w.String(s.build.commit);
      w.Key("date");
      w.String(s.build.date);
      w.EndObject();
    }
    w.EndObject();
  }

  w.Key("server");
  {
    w.BeginObject();
    w.Key("node_id");
    w.String(s.server.node_id);
    w.Key("started_at_unix_ms");
    w.UInt(s.server.started_at_unix_ms);
    w.Key("uptime_seconds");
    w.UInt(s.server.uptime_seconds);
    w.Key("process_id");
    w.UInt(s.server.process_id);
    w.Key("ready");
    w.Bool(s.server.ready);
    w.Key("loading");
    w.Bool(s.server.loading);
    w.Key("shutting_down");
    w.Bool(s.server.shutting_down);
    w.Key("mode");
    w.String(s.server.mode);
    w.Key("role");
    w.String(s.server.role);
    w.EndObject();
  }

  w.Key("config");
  {
    w.BeginObject();
    w.Key("profile");
    w.String(s.config.profile);
    w.Key("shard_count");
    w.UInt(s.config.shard_count);
    w.Key("durability");
    w.String(s.config.durability);
    w.Key("default_eviction_seconds");
    w.UInt(s.config.default_eviction_seconds);
    w.EndObject();
  }

  w.Key("endpoints");
  {
    w.BeginObject();
    w.Key("resp");
    WriteEndpoint(w, s.endpoints.resp);
    w.Key("admin");
    WriteEndpoint(w, s.endpoints.admin);
    w.Key("metrics");
    WriteEndpoint(w, s.endpoints.metrics);
    w.EndObject();
  }

  w.Key("queue");
  {
    w.BeginObject();
    w.Key("backend");
    w.String(s.queue.backend);
    w.Key("head_seq");
    w.UInt(s.queue.head_seq);
    w.Key("first_seq");
    w.UInt(s.queue.first_seq);
    w.Key("total_entries");
    w.UInt(s.queue.total_entries);
    w.Key("total_bytes");
    w.UInt(s.queue.total_bytes);
    w.Key("reaper_failures");
    w.UInt(s.queue.reaper_failures);
    w.Key("oldest_eligible_unreaped_age_ms");
    w.UInt(s.queue.oldest_eligible_unreaped_age_ms);
    w.Key("unflushed_bytes");
    w.UInt(s.queue.unflushed_bytes);
    w.Key("durability_lag_ms");
    w.UInt(s.queue.durability_lag_ms);
    w.EndObject();
  }

  w.Key("hot");
  {
    w.BeginObject();
    w.Key("backend");
    w.String(s.hot.backend);
    w.Key("key_count");
    w.UInt(s.hot.key_count);
    w.Key("memory_bytes");
    w.UInt(s.hot.memory_bytes);
    w.EndObject();
  }

  w.Key("cold");
  {
    w.BeginObject();
    w.Key("backend");
    w.String(s.cold.backend);
    w.Key("key_count");
    w.UInt(s.cold.key_count);
    w.Key("disk_bytes");
    w.UInt(s.cold.disk_bytes);
    w.Key("buffer");
    {
      w.BeginObject();
      w.Key("entries");
      w.UInt(s.cold.buffer.entries);
      w.Key("bytes");
      w.UInt(s.cold.buffer.bytes);
      w.Key("oldest_entry_age_ms");
      w.UInt(s.cold.buffer.oldest_entry_age_ms);
      w.EndObject();
    }
    w.EndObject();
  }

  w.Key("consumers");
  {
    w.BeginObject();
    w.Key("hot");
    {
      w.BeginObject();
      w.Key("highest_settled_seq_min");
      w.UInt(s.consumers.hot.highest_settled_seq_min);
      w.Key("highest_settled_seq_max");
      w.UInt(s.consumers.hot.highest_settled_seq_max);
      w.EndObject();
    }
    w.Key("cold");
    {
      w.BeginObject();
      w.Key("last_commit_seq_min");
      w.UInt(s.consumers.cold.last_commit_seq_min);
      w.Key("last_commit_seq_max");
      w.UInt(s.consumers.cold.last_commit_seq_max);
      w.EndObject();
    }
    w.Key("resolver");
    {
      w.BeginObject();
      w.Key("last_commit_seq_min");
      w.UInt(s.consumers.resolver.last_commit_seq_min);
      w.Key("last_commit_seq_max");
      w.UInt(s.consumers.resolver.last_commit_seq_max);
      w.Key("cache_entries");
      w.UInt(s.consumers.resolver.cache_entries);
      w.Key("cache_bytes");
      w.UInt(s.consumers.resolver.cache_bytes);
      w.EndObject();
    }
    w.EndObject();
  }

  w.Key("lag");
  {
    w.BeginObject();
    w.Key("hot_max_entries");
    w.UInt(s.lag.hot_max_entries);
    w.Key("cold_max_entries");
    w.UInt(s.lag.cold_max_entries);
    w.Key("resolver_max_entries");
    w.UInt(s.lag.resolver_max_entries);
    w.EndObject();
  }

  w.Key("connections");
  {
    w.BeginObject();
    w.Key("active");
    w.UInt(s.connections.active);
    w.Key("read_buffer_high_water_bytes");
    w.UInt(s.connections.read_buffer_high_water_bytes);
    w.EndObject();
  }

  w.Key("recovery");
  {
    w.BeginObject();
    w.Key("phase");
    switch (s.recovery.phase) {
      case StatusRecoveryPhase::kQueueOpen:
        w.String("queue_open");
        break;
      case StatusRecoveryPhase::kResolverReplay:
        w.String("resolver_replay");
        break;
      case StatusRecoveryPhase::kColdHotReplay:
        w.String("cold_hot_replay");
        break;
      case StatusRecoveryPhase::kComplete:
        w.String("complete");
        break;
    }
    w.Key("resolver_entries_replayed");
    w.UInt(s.recovery.resolver_entries_replayed);
    w.Key("resolver_entries_target");
    w.UInt(s.recovery.resolver_entries_target);
    w.Key("cold_entries_replayed");
    w.UInt(s.recovery.cold_entries_replayed);
    w.Key("cold_entries_target");
    w.UInt(s.recovery.cold_entries_target);
    w.Key("hot_entries_replayed");
    w.UInt(s.recovery.hot_entries_replayed);
    w.Key("hot_entries_target");
    w.UInt(s.recovery.hot_entries_target);
    w.Key("elapsed_ms");
    w.UInt(s.recovery.elapsed_ms);
    w.EndObject();
  }

  // Reserved for Phase 2; explicit null preserves the slot now so the schema
  // doesn't change shape when cluster mode lands.
  w.Key("cluster");
  w.Null();

  w.EndObject();
  return w.Finish();
}

}  // namespace

StatusHandler::StatusHandler(const StatusProvider* provider) : provider_(provider) {}

HttpResponse StatusHandler::Handle(const HttpRequest& /*request*/) {
  if (provider_ == nullptr) {
    return HttpResponse::ServiceUnavailable("status provider unavailable\n");
  }
  return HttpResponse::Ok(Render(provider_->Snapshot()), kJsonContentType);
}

}  // namespace abyss::admin
