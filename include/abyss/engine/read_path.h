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
#include <vector>

#include "abyss/core/cold_store.h"
#include "abyss/core/command_dispatcher.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/decide.h"
#include "abyss/engine/doorkeeper.h"
#include "abyss/engine/io_pool.h"
#include "abyss/engine/loader.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"

namespace abyss::engine {

struct ReadPathConfig {
  // Fill hot only on a key's second miss within the doorkeeper's
  // window; off, every miss fills.
  bool fill_doorkeeper = true;
  size_t doorkeeper_window = size_t{1} << 20;
  // A member point read fills a collection only below this many.
  uint64_t fill_max_members = 1024;
  // Judges the TTL of what a miss loads.
  core::WallClockFn wall_clock = core::DefaultWallClock;
};

// A read's reply, and what it showed that may not yet be durable at
// the ack class: the reply waits for that seq (the fence).
struct ReadAnswer {
  core::Result<core::RespValue> reply;
  std::optional<ShardSeq> fence;
};

// MGET or EXISTS over its keys, per key and in order, and not atomic
// (#170). Hot answers on the caller's thread until a miss; the rest,
// that key on, is one job on the I/O pool.
struct FanOut {
  core::MultiKeyKind kind = core::MultiKeyKind::kMget;
  std::span<const std::string> keys;
  // The next key to read.
  size_t next = 0;
  std::vector<core::RespValue> values;
  int64_t count = 0;
  std::vector<ShardSeq> fences;
  std::optional<core::Error> error;

  void Reset(core::MultiKeyKind fan_kind, std::span<const std::string> fan_keys);
  bool done() const { return error.has_value() || next == keys.size(); }
  core::Result<core::RespValue> Reply();
};

// Every read, split where it would block. Hot answers on the caller's
// thread (TryHot), and a hit waits only for its fence, which the
// caller registers. A miss is answered on the I/O pool (Miss) and
// needs no wait: residency puts a non-resident key's whole state in
// buffer plus cold. The flush floor is checked first, under hot's
// lock, and makes a miss absent.
//
// Cache fill on a miss, under the doorkeeper and never past hot's
// backpressure limit:
// - GET loads the string (one point read) and fills: the fill is the
//   answer;
// - SISMEMBER, ZSCORE, HGET, HEXISTS and HMGET read their members
//   only, and fill a collection under fill_max_members afterwards, on
//   the pool's background queue;
// - SCARD, ZCARD and HLEN count from cold's meta and the delta, and
//   fill likewise;
// - the scans load the key, filling hot: the fill is the answer;
// - EXISTS, TYPE, TTL and PTTL answer from a stub or a cold probe,
//   and fill nothing.
// Hot refuses a fill over fill_max_fraction of a shard's budget. Loads
// that are not filled are shared by concurrent misses (Loader::LoadAs).
// A key of another type answers WRONGTYPE without reading members.
class ReadPath {
 public:
  ReadPath(hot::ShardedHotStore& hot, Loader& loader, IoPool& pool, ReadPathConfig config);

  // Hot's answer, on the caller's thread; nullopt on a miss.
  std::optional<ReadAnswer> TryHot(const core::ops::ReadOp& op);
  // A miss's answer from buffer and cold, on the pool, by `deadline`,
  // the command's. A background fill inherits it.
  ReadAnswer Miss(const core::ops::ReadOp& op, core::SteadyTime deadline);
  // Hot's answers from `fan.next` on, stopping at the first miss.
  void FanOutHot(FanOut& fan);
  // Every key left, on the pool: hot, then buffer and cold on a miss.
  void FanOutMiss(FanOut& fan, core::SteadyTime deadline);
  // A miss that will not be read: its error, counted as a read's.
  core::Result<core::RespValue> Failed(const core::ops::ReadOp& op, const core::Error& error);
  // Its miss's kind on the pool: a scan loads the whole key.
  static metrics::IoKind KindOf(const core::ops::ReadOp& op);

 private:
  class FillJob;

  // Hot's answer; nullopt on a miss. After a fill, `filled_from` is
  // where the fill read it, and is counted instead of a hot hit.
  std::optional<ReadAnswer> FromHot(const core::ops::ReadOp& op,
                                    std::optional<Loader::Source> filled_from = std::nullopt);
  // Loads `key` as `type`, filling hot when admitted.
  ReadAnswer Fill(const core::ops::ReadOp& op, core::KeyType type, core::SteadyTime deadline);
  core::Result<core::RespValue> PointRead(const core::ops::ReadOp& op, core::KeyType type,
                                          core::SteadyTime deadline);
  // SCARD, ZCARD or HLEN: exact, reading no member but the delta's.
  core::Result<core::RespValue> CountRead(const core::ops::ReadOp& op, core::KeyType type,
                                          core::SteadyTime deadline);
  // Queues a collection's fill under fill_max_members, when admitted.
  void FillSmall(std::string_view key, core::KeyType type, uint64_t members,
                 core::SteadyTime deadline);
  // One fan-out key's answer, folded in; false once it is an error.
  bool Fold(FanOut& fan, ReadAnswer answer);
  core::Result<core::RespValue> Answer(const core::ops::ReadOp& op, const hot::LoadResult& loaded,
                                       Loader::Source source);
  void CountHit(Loader::Source source);
  void CountFill(const core::Result<Loader::Filled>& filled);
  void CountFill(metrics::FillOutcome outcome);
  bool Admit(std::string_view key);
  std::optional<ShardSeq> FenceOf(const core::ops::ReadOp& op,
                                  std::optional<core::SequenceId> seq) const;

  hot::ShardedHotStore& hot_;
  Loader& loader_;
  IoPool& pool_;
  ReadPathConfig config_;
  std::unique_ptr<Doorkeeper> doorkeeper_;

  metrics::CounterHandle hits_hot_;
  metrics::CounterHandle hits_buffer_;
  metrics::CounterHandle hits_cold_;
  metrics::CounterHandle misses_;
  metrics::CounterHandle cold_scan_deadline_exceeded_;
  // By metrics::FillOutcome.
  std::array<metrics::CounterHandle, 8> fills_;
};

}  // namespace abyss::engine
