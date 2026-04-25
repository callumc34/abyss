#include <cctype>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/resp/admin_handlers.h"
#include "abyss/resp/command_registry.h"

namespace abyss::resp {
namespace {

using core::ErrorPrefix;
using core::RespValue;

std::string Uppercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return out;
}

std::string Lowercase(std::string_view s) {
  std::string out;
  out.reserve(s.size());
  for (const char c : s) {
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
  }
  return out;
}

std::vector<RespValue> FlagsFor(const CommandSpec& spec) {
  std::vector<RespValue> flags;
  switch (spec.cls) {
    case CommandClass::kRead:
      flags.push_back(RespValue::SimpleString("readonly"));
      break;
    case CommandClass::kWrite:
      flags.push_back(RespValue::SimpleString("write"));
      break;
    case CommandClass::kAdmin:
      flags.push_back(RespValue::SimpleString("admin"));
      break;
  }
  if (spec.loading_safe) {
    flags.push_back(RespValue::SimpleString("loading"));
  }
  return flags;
}

RespValue SpecAsInfo(const CommandSpec& spec) {
  return RespValue::Array({
      RespValue::BulkString(Lowercase(spec.name)),
      RespValue::Integer(spec.arity),
      RespValue::Array(FlagsFor(spec)),
      RespValue::Integer(spec.first_key),
      RespValue::Integer(spec.last_key),
      RespValue::Integer(spec.key_step),
  });
}

RespValue SpecAsDocs(const CommandSpec& spec) {
  return RespValue::Array({
      RespValue::BulkString("summary"),
      RespValue::BulkString(std::string(spec.docs.summary)),
      RespValue::BulkString("since"),
      RespValue::BulkString(std::string(spec.docs.since)),
      RespValue::BulkString("group"),
      RespValue::BulkString(std::string(spec.docs.group)),
      RespValue::BulkString("arity"),
      RespValue::Integer(spec.arity),
      RespValue::BulkString("complexity"),
      RespValue::BulkString(std::string(spec.docs.complexity)),
  });
}

RespValue HandleList(const CommandRegistry& registry) {
  std::vector<RespValue> out;
  out.reserve(registry.All().size());
  for (const auto& spec : registry.All()) {
    out.push_back(SpecAsInfo(spec));
  }
  return RespValue::Array(std::move(out));
}

RespValue HandleCount(const CommandRegistry& registry) {
  return RespValue::Integer(static_cast<int64_t>(registry.Size()));
}

RespValue HandleInfoSub(const core::RespCommand& cmd, const CommandRegistry& registry) {
  if (cmd.ArgCount() == 2) {
    return HandleList(registry);
  }
  std::vector<RespValue> out;
  out.reserve(cmd.ArgCount() - 2);
  for (size_t i = 2; i < cmd.ArgCount(); ++i) {
    const auto* spec = registry.Find(cmd.args[i]);
    if (spec == nullptr) {
      out.push_back(RespValue::Null());
    } else {
      out.push_back(SpecAsInfo(*spec));
    }
  }
  return RespValue::Array(std::move(out));
}

RespValue HandleDocsSub(const core::RespCommand& cmd, const CommandRegistry& registry) {
  std::vector<RespValue> out;
  if (cmd.ArgCount() == 2) {
    out.reserve(registry.All().size() * 2);
    for (const auto& spec : registry.All()) {
      out.push_back(RespValue::BulkString(Lowercase(spec.name)));
      out.push_back(SpecAsDocs(spec));
    }
    return RespValue::Array(std::move(out));
  }
  out.reserve((cmd.ArgCount() - 2) * 2);
  for (size_t i = 2; i < cmd.ArgCount(); ++i) {
    const auto* spec = registry.Find(cmd.args[i]);
    if (spec == nullptr) continue;
    out.push_back(RespValue::BulkString(Lowercase(spec->name)));
    out.push_back(SpecAsDocs(*spec));
  }
  return RespValue::Array(std::move(out));
}

}  // namespace

core::RespValue HandleCommandIntrospect(const core::RespCommand& cmd,
                                        const CommandRegistry& registry) {
  if (cmd.ArgCount() == 1) {
    return HandleList(registry);
  }
  const auto sub = Uppercase(cmd.args[1]);
  if (sub == "COUNT") return HandleCount(registry);
  if (sub == "INFO") return HandleInfoSub(cmd, registry);
  if (sub == "DOCS") return HandleDocsSub(cmd, registry);

  std::string msg = "Unknown COMMAND subcommand or wrong number of arguments for '";
  msg.append(cmd.args[1]);
  msg.push_back('\'');
  return RespValue::Error(ErrorPrefix::kErr, std::move(msg));
}

}  // namespace abyss::resp
