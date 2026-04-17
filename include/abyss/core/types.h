#pragma once

#include <chrono>
#include <cstdint>
#include <functional>

namespace abyss::core {

using ShardId = uint32_t;
using SequenceId = uint64_t;
using ConsumerId = uint32_t;

using SteadyClock = std::chrono::steady_clock;
using SteadyTime = SteadyClock::time_point;
using WallClock = std::chrono::system_clock;
using WallTime = WallClock::time_point;

using Duration = std::chrono::milliseconds;
using EvictionTTL = std::chrono::seconds;

using SteadyClockFn = std::function<SteadyTime()>;
using WallClockFn = std::function<WallTime()>;

inline SteadyTime DefaultSteadyClock() { return SteadyClock::now(); }
inline WallTime DefaultWallClock() { return WallClock::now(); }

inline constexpr ConsumerId kHotConsumer = 0;
inline constexpr ConsumerId kColdConsumer = 1;

}  // namespace abyss::core
