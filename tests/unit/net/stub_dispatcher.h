#pragma once

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
  core::Result<core::RespValue> DispatchRead(std::string_view /*name*/,
                                             const core::RespCommand& /*cmd*/) override {
    return core::RespValue::BulkString(read_payload);
  }
  core::Result<core::RespValue> DispatchWrite(std::string_view /*name*/,
                                              core::RespCommand /*cmd*/) override {
    return core::RespValue::SimpleString("OK");
  }
  core::Result<core::RespValue> DispatchConditional(std::string_view /*name*/,
                                                    core::RespCommand /*cmd*/,
                                                    core::PredicateFlags /*flags*/) override {
    return core::RespValue::SimpleString("OK");
  }
  core::Result<core::RespValue> DispatchFanOut(core::MultiKeyKind /*kind*/,
                                               core::RespCommand /*cmd*/) override {
    return core::RespValue::SimpleString("OK");
  }
};

}  // namespace abyss::net::testing
