// What it costs the write path to parse a command once at the frontend and
// dispatch it in canonical form, measured against the work already happening
// either side of it -- so that trade is a number rather than an argument.
//
// The headline comparison is BM_ValidateAndCanonicalise (current) against
// BM_ValidateThenDeepCopy (what it replaced: validate, discard the result, then
// deep-copy the client's command into the dispatcher). Canonicalising replaces
// that copy rather than adding to it. BM_RespParseCommand is the anchor: the
// decode that produced the command in the first place, on the same thread.

#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/resp/parser.h"

namespace abyss::core::ops {
namespace {

RespCommand SetCmd(size_t value_size) {
  return RespCommand{{"SET", "user:1000:session", std::string(value_size, 'x')}};
}

RespCommand HsetCmd(size_t pairs) {
  std::vector<std::string> args{"HSET", "user:1000"};
  for (size_t i = 0; i < pairs; ++i) {
    args.push_back("field" + std::to_string(i));
    args.push_back("value" + std::to_string(i));
  }
  return RespCommand{std::move(args)};
}

RespCommand SaddCmd(size_t members) {
  std::vector<std::string> args{"SADD", "tags:1000"};
  for (size_t i = 0; i < members; ++i) args.push_back("member" + std::to_string(i));
  return RespCommand{std::move(args)};
}

// Current shape: gate on a capability gap, parse, then re-express canonically.
// The parse result is now used rather than discarded, so wall_now is the real
// clock -- the absolute TTL it produces is what reaches the WAL.
void BM_ValidateAndCanonicalise(benchmark::State& state, const RespCommand& cmd) {
  for (auto _ : state) {
    if (HasWriteParser(cmd.Name())) {
      auto parsed = ParseWriteOp(cmd.Name(), cmd);
      if (parsed.has_value()) {
        auto canonical = CanonicalCommand(*parsed);
        benchmark::DoNotOptimize(canonical.args.data());
      }
    }
  }
}

// Shape before canonicalisation: validate, discard, then deep-copy the client's
// command into the dispatcher. Canonicalising replaces that copy rather than
// adding to it, which is the comparison that matters.
void BM_ValidateThenDeepCopy(benchmark::State& state, const RespCommand& cmd) {
  for (auto _ : state) {
    if (HasWriteParser(cmd.Name())) {
      auto parsed = ParseWriteOp(cmd.Name(), cmd, 0);
      benchmark::DoNotOptimize(parsed.has_value());
    }
    RespCommand copy(cmd);
    benchmark::DoNotOptimize(copy.args.data());
  }
}

// The same shape before wall_now was pinned, i.e. paying a system_clock read
// per write for a timestamp that is thrown away.
void BM_ValidateWithClockRead(benchmark::State& state, const RespCommand& cmd) {
  for (auto _ : state) {
    bool ok = false;
    if (HasWriteParser(cmd.Name())) {
      auto parsed = ParseWriteOp(cmd.Name(), cmd);
      ok = parsed.has_value();
    }
    benchmark::DoNotOptimize(ok);
  }
}

// Floor if the capability-gap gate were folded into the parse to save the
// second table lookup. Not adopted: the gate encodes a distinction the cold
// consumer also depends on, and the delta does not pay for blurring it.
void BM_ValidateSingleLookup(benchmark::State& state, const RespCommand& cmd) {
  for (auto _ : state) {
    auto parsed = ParseWriteOp(cmd.Name(), cmd, 0);
    benchmark::DoNotOptimize(parsed.has_value());
  }
}

// Isolates the two costs the landed shape adds over the line above.
void BM_HasWriteParserOnly(benchmark::State& state, const RespCommand& cmd) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(HasWriteParser(cmd.Name()));
  }
}

void BM_WallNowMsOnly(benchmark::State& state, const RespCommand& cmd) {
  benchmark::DoNotOptimize(cmd.args.data());
  for (auto _ : state) {
    benchmark::DoNotOptimize(WallNowMs());
  }
}

// What canonicalising replaced. DispatchWrite takes RespCommand by value, so
// before the frontend produced a canonical command of its own it deep-copied
// the client's -- every argument string. Still the cost for a command with no
// parser, which now means replay-only paths rather than anything from the wire.
void BM_CommandDeepCopy(benchmark::State& state, const RespCommand& cmd) {
  for (auto _ : state) {
    RespCommand copy(cmd);
    benchmark::DoNotOptimize(copy.args.data());
  }
}

std::vector<uint8_t> Encode(const RespCommand& cmd) {
  std::string out = "*" + std::to_string(cmd.args.size()) + "\r\n";
  for (const auto& a : cmd.args) {
    out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  }
  return {out.begin(), out.end()};
}

// Already on the same path: the frontend RESP decode that produced the command.
void BM_RespParseCommand(benchmark::State& state, const RespCommand& cmd) {
  const auto wire = Encode(cmd);
  for (auto _ : state) {
    auto parsed = resp::Parser::ParseCommand(std::span<const uint8_t>(wire));
    benchmark::DoNotOptimize(parsed.has_value());
  }
}

const RespCommand& SetSmall() {
  static const RespCommand c = SetCmd(32);
  return c;
}
const RespCommand& SetLarge() {
  static const RespCommand c = SetCmd(1024);
  return c;
}
const RespCommand& Hset4() {
  static const RespCommand c = HsetCmd(4);
  return c;
}
const RespCommand& Sadd8() {
  static const RespCommand c = SaddCmd(8);
  return c;
}
const RespCommand& Del1() {
  static const RespCommand c{{"DEL", "user:1000"}};
  return c;
}
const RespCommand& Expire1h() {
  static const RespCommand c{{"EXPIRE", "user:1000", "3600"}};
  return c;
}

BENCHMARK_CAPTURE(BM_ValidateAndCanonicalise, set_32b, SetSmall());
BENCHMARK_CAPTURE(BM_ValidateAndCanonicalise, set_1kb, SetLarge());
BENCHMARK_CAPTURE(BM_ValidateAndCanonicalise, hset_4_pairs, Hset4());
BENCHMARK_CAPTURE(BM_ValidateAndCanonicalise, sadd_8_members, Sadd8());
BENCHMARK_CAPTURE(BM_ValidateAndCanonicalise, del_1_key, Del1());
BENCHMARK_CAPTURE(BM_ValidateAndCanonicalise, expire, Expire1h());

BENCHMARK_CAPTURE(BM_ValidateThenDeepCopy, set_32b, SetSmall());
BENCHMARK_CAPTURE(BM_ValidateThenDeepCopy, set_1kb, SetLarge());
BENCHMARK_CAPTURE(BM_ValidateThenDeepCopy, hset_4_pairs, Hset4());
BENCHMARK_CAPTURE(BM_ValidateThenDeepCopy, sadd_8_members, Sadd8());
BENCHMARK_CAPTURE(BM_ValidateThenDeepCopy, del_1_key, Del1());
BENCHMARK_CAPTURE(BM_ValidateThenDeepCopy, expire, Expire1h());

BENCHMARK_CAPTURE(BM_ValidateWithClockRead, set_32b, SetSmall());
BENCHMARK_CAPTURE(BM_ValidateWithClockRead, hset_4_pairs, Hset4());

BENCHMARK_CAPTURE(BM_ValidateSingleLookup, set_32b, SetSmall());
BENCHMARK_CAPTURE(BM_ValidateSingleLookup, set_1kb, SetLarge());
BENCHMARK_CAPTURE(BM_ValidateSingleLookup, hset_4_pairs, Hset4());
BENCHMARK_CAPTURE(BM_ValidateSingleLookup, sadd_8_members, Sadd8());
BENCHMARK_CAPTURE(BM_ValidateSingleLookup, del_1_key, Del1());
BENCHMARK_CAPTURE(BM_ValidateSingleLookup, expire, Expire1h());

BENCHMARK_CAPTURE(BM_HasWriteParserOnly, set_32b, SetSmall());
BENCHMARK_CAPTURE(BM_WallNowMsOnly, set_32b, SetSmall());

BENCHMARK_CAPTURE(BM_CommandDeepCopy, set_32b, SetSmall());
BENCHMARK_CAPTURE(BM_CommandDeepCopy, set_1kb, SetLarge());
BENCHMARK_CAPTURE(BM_CommandDeepCopy, hset_4_pairs, Hset4());
BENCHMARK_CAPTURE(BM_CommandDeepCopy, sadd_8_members, Sadd8());
BENCHMARK_CAPTURE(BM_CommandDeepCopy, del_1_key, Del1());
BENCHMARK_CAPTURE(BM_CommandDeepCopy, expire, Expire1h());

BENCHMARK_CAPTURE(BM_RespParseCommand, set_32b, SetSmall());
BENCHMARK_CAPTURE(BM_RespParseCommand, set_1kb, SetLarge());
BENCHMARK_CAPTURE(BM_RespParseCommand, hset_4_pairs, Hset4());
BENCHMARK_CAPTURE(BM_RespParseCommand, sadd_8_members, Sadd8());
BENCHMARK_CAPTURE(BM_RespParseCommand, del_1_key, Del1());
BENCHMARK_CAPTURE(BM_RespParseCommand, expire, Expire1h());

}  // namespace
}  // namespace abyss::core::ops
