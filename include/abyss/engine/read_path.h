#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "abyss/core/cold_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/doorkeeper.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/sequencer.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"

namespace abyss::engine {

struct ReadPathConfig {
  // Bounds the wait for the writes a hot answer saw to become durable.
  std::chrono::milliseconds write_timeout{5000};
  // ADP-003's cold point-read SLA: a miss's point reads and probes.
  std::chrono::milliseconds cold_read_deadline{5};
  // A miss that loads a whole key: a cardinality or a scan.
  std::chrono::milliseconds cold_scan_deadline{50};
  // Fill hot only on a key's second miss within the doorkeeper's
  // window; off, every miss fills.
  bool fill_doorkeeper = true;
  size_t doorkeeper_window = size_t{1} << 20;
  // A member point read fills a collection only below this many.
  uint64_t fill_max_members = 1024;
  // Judges the TTL of what a miss loads.
  core::WallClockFn wall_clock = core::DefaultWallClock;
};

// Every read. A hot hit answers once what it saw is durable (the
// fence). A miss needs no wait: residency puts a non-resident key's
// whole state in buffer plus cold. The flush floor is checked first,
// under hot's lock, and makes a miss absent.
//
// Cache fill on a miss, under the doorkeeper and never past hot's
// backpressure limit:
// - GET loads the string (one point read) and fills;
// - SISMEMBER, ZSCORE, HGET, HEXISTS and HMGET read their members
//   only, and fill a collection under fill_max_members;
// - SCARD, ZCARD and HLEN count from cold's meta and the delta, and
//   fill likewise;
// - the scans load the key, filling hot;
// - EXISTS, TYPE, TTL and PTTL answer from a stub or a cold probe,
//   and fill nothing.
// Hot refuses a fill over fill_max_fraction of a shard's budget. Loads
// that are not filled are shared by concurrent misses (Loader::LoadAs).
// A key of another type answers WRONGTYPE without reading members.
class ReadPath {
 public:
  ReadPath(hot::ShardedHotStore& hot, Loader& loader, Sequencer& sequencer, ReadPathConfig config);

  core::Result<core::RespValue> Read(const core::ops::ReadOp& op);
  // Per key and not atomic (#170), each through Read.
  core::Result<core::RespValue> Mget(std::span<const std::string> keys);
  core::Result<core::RespValue> Exists(std::span<const std::string> keys);

 private:
  // Hot's answer, fenced; nullopt on a miss. After a fill, `filled_from`
  // is where the fill read it, and is counted instead of a hot hit.
  std::optional<core::Result<core::RespValue>> FromHot(
      const core::ops::ReadOp& op, std::optional<Loader::Source> filled_from = std::nullopt);
  core::Result<core::RespValue> Miss(const core::ops::ReadOp& op);
  // Loads `key` as `type`, filling hot when admitted.
  core::Result<core::RespValue> Fill(const core::ops::ReadOp& op, core::KeyType type,
                                     std::chrono::milliseconds budget);
  core::Result<core::RespValue> PointRead(const core::ops::ReadOp& op, core::KeyType type);
  // SCARD, ZCARD or HLEN: exact, reading no member but the delta's.
  core::Result<core::RespValue> CountRead(const core::ops::ReadOp& op, core::KeyType type);
  // Fills a collection under fill_max_members, when admitted.
  void FillSmall(std::string_view key, core::KeyType type, uint64_t members);
  core::Result<core::RespValue> Answer(const core::ops::ReadOp& op, const hot::LoadResult& loaded,
                                       Loader::Source source);
  core::Result<core::RespValue> Failed(const core::ops::ReadOp& op, const core::Error& error);
  void CountHit(Loader::Source source);
  void CountFill(const core::Result<Loader::Filled>& filled);
  bool Admit(std::string_view key);
  core::SteadyTime After(std::chrono::milliseconds budget) const;

  hot::ShardedHotStore& hot_;
  Loader& loader_;
  Sequencer& sequencer_;
  ReadPathConfig config_;
  std::unique_ptr<Doorkeeper> doorkeeper_;

  metrics::CounterHandle hits_hot_;
  metrics::CounterHandle hits_buffer_;
  metrics::CounterHandle hits_cold_;
  metrics::CounterHandle misses_;
  metrics::CounterHandle cold_scan_deadline_exceeded_;
  // By metrics::FillOutcome.
  std::array<metrics::CounterHandle, 6> fills_;
};

}  // namespace abyss::engine
