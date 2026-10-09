#pragma once

#include <chrono>
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include "abyss/core/cold_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::testing {

inline int64_t WallNowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             core::WallClock::now().time_since_epoch())
      .count();
}

inline hot::Value HotValueOf(core::ColdValue value) {
  return std::visit(
      [](auto&& v) -> hot::Value {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, core::StringSet>) {
          return hot::SetValue{.members = std::forward<decltype(v)>(v)};
        } else if constexpr (std::is_same_v<T, core::StringMap<std::string>>) {
          return hot::HashValue{.fields = std::forward<decltype(v)>(v)};
        } else if constexpr (std::is_same_v<T, core::StringMap<double>>) {
          hot::ZsetValue zset;
          for (const auto& [member, score] : v) zset.score_members[score].insert(member);
          zset.member_scores = std::forward<decltype(v)>(v);
          return zset;
        } else {
          return std::forward<decltype(v)>(v);
        }
      },
      std::move(value));
}

// What `op` reads from cold alone at `now_ms`: each key loaded whole,
// then answered as hot answers it.
inline core::Result<core::RespValue> ColdRead(core::ColdStore& cold, const core::ops::ReadOp& op,
                                              int64_t now_ms = WallNowMs()) {
  const auto deadline = core::SteadyClock::now() + std::chrono::seconds(30);
  const auto answer = [&](std::string_view key,
                          const core::ops::ReadOp& one) -> core::Result<core::RespValue> {
    auto loaded = cold.LoadKey(key, deadline);
    if (!loaded.has_value()) return std::unexpected(loaded.error());
    if (!loaded->has_value() || ((*loaded)->abs_ttl_ms != 0 && (*loaded)->abs_ttl_ms <= now_ms)) {
      return hot::EmptyReadResponse(one);
    }
    const auto type = static_cast<hot::Entry::Type>((*loaded)->type);
    const hot::Value value = HotValueOf(std::move((*loaded)->value));
    return hot::AnswerRead(one, type, &value, (*loaded)->abs_ttl_ms, now_ms);
  };
  if (const auto* exists = std::get_if<core::ops::Exists>(&op)) {
    int64_t total = 0;
    for (const std::string_view key : exists->keys) {
      auto one = answer(key, core::ops::ReadOp{core::ops::Exists{.keys = {key}}});
      if (!one.has_value()) return one;
      total += one->AsInteger();
    }
    return core::RespValue::Integer(total);
  }
  return answer(core::ops::PrimaryKey(op), op);
}

}  // namespace abyss::testing
