#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::net::testing {

// Canned responses with a configurable read payload size.
class StubDispatcher : public core::CommandDispatcher {
 public:
  std::string read_payload = "hello";
  core::Outcome Dispatch(core::Request request, core::Slot& /*slot*/,
                         std::unique_ptr<core::Task>& /*task*/) override {
    if (request.kind == core::Request::Kind::kRead) {
      return {.reply = core::RespValue::BulkString(read_payload)};
    }
    return {.reply = core::RespValue::SimpleString("OK")};
  }
};

}  // namespace abyss::net::testing
