#pragma once

#include <string>

#include "abyss/core/resp_types.h"

namespace abyss::core {

// One decided state change: a canonical write (ops::CanonicalCommand
// form, TTLs absolute) to one key, logged as one Write entry.
struct Effect {
  std::string key;
  RespCommand cmd;
  // The effect alone determines the key's state (frame kReplacesState).
  bool replaces_state = false;
  // SET ... GET: applying it replies with the value it replaces, moved
  // out of hot. Not logged.
  bool reply_old_value = false;
  // A DEL of a key the decision found past its TTL: counted as an
  // expiry, not a delete. Not logged.
  bool observed_expiry = false;
};

}  // namespace abyss::core
