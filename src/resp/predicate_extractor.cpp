#include "abyss/resp/predicate_extractor.h"

#include <string>
#include <string_view>

namespace abyss::resp {

namespace {

std::string AsciiUpper(std::string_view s) {
  std::string out(s);
  for (auto& c : out) {
    if (c >= 'a' && c <= 'z') c = static_cast<char>(c - 'a' + 'A');
  }
  return out;
}

core::Error SyntaxError(std::string detail) {
  return {core::ErrorCode::kInvalidArgument, std::move(detail)};
}

}  // namespace

core::Result<core::PredicateFlags> ExtractSetFlags(const core::RespCommand& cmd) {
  if (cmd.args.size() < 3) return core::PredicateFlags::kNone;
  core::PredicateFlags flags = core::PredicateFlags::kNone;
  bool has_nx = false;
  bool has_xx = false;
  for (size_t i = 3; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "NX") {
      flags |= core::PredicateFlags::kNx;
      has_nx = true;
    } else if (opt == "XX") {
      flags |= core::PredicateFlags::kXx;
      has_xx = true;
    } else if (opt == "GET") {
      flags |= core::PredicateFlags::kGet;
    } else if (opt == "KEEPTTL") {
      flags |= core::PredicateFlags::kKeepTtl;
    } else if (opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT") {
      ++i;  // skip TTL value; not a predicate flag
    }
    // Unknown opts: defer syntax errors to the parser.
  }
  if (has_nx && has_xx) return std::unexpected(SyntaxError("syntax error"));
  return flags;
}

core::Result<core::PredicateFlags> ExtractSetNxFlags(const core::RespCommand& /*cmd*/) {
  return core::PredicateFlags::kNx;
}

core::Result<core::PredicateFlags> ExtractMsetNxFlags(const core::RespCommand& /*cmd*/) {
  return core::PredicateFlags::kMsetNx;
}

core::Result<core::PredicateFlags> ExtractZAddFlags(const core::RespCommand& cmd) {
  core::PredicateFlags flags = core::PredicateFlags::kNone;
  bool nx = false;
  bool xx = false;
  bool gt = false;
  bool lt = false;
  for (size_t i = 2; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "NX") {
      flags |= core::PredicateFlags::kNx;
      nx = true;
    } else if (opt == "XX") {
      flags |= core::PredicateFlags::kXx;
      xx = true;
    } else if (opt == "GT") {
      flags |= core::PredicateFlags::kZAddGt;
      gt = true;
    } else if (opt == "LT") {
      flags |= core::PredicateFlags::kZAddLt;
      lt = true;
    } else if (opt == "CH") {
      flags |= core::PredicateFlags::kZAddCh;
    } else {
      break;
    }
  }
  if ((nx && xx) || (gt && lt) || (nx && (gt || lt))) {
    return std::unexpected(SyntaxError("syntax error"));
  }
  return flags;
}

core::Result<core::PredicateFlags> ExtractExpireFlags(const core::RespCommand& cmd) {
  core::PredicateFlags flags = core::PredicateFlags::kNone;
  bool nx = false;
  bool xx = false;
  bool gt = false;
  bool lt = false;
  for (size_t i = 3; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "NX") {
      flags |= core::PredicateFlags::kNx;
      nx = true;
    } else if (opt == "XX") {
      flags |= core::PredicateFlags::kXx;
      xx = true;
    } else if (opt == "GT") {
      flags |= core::PredicateFlags::kExpireGt;
      gt = true;
    } else if (opt == "LT") {
      flags |= core::PredicateFlags::kExpireLt;
      lt = true;
    }
  }
  if ((nx && xx) || (gt && lt) || (nx && (gt || lt))) {
    return std::unexpected(SyntaxError("syntax error"));
  }
  return flags;
}

core::Result<core::PredicateFlags> ExtractRenameNxFlags(const core::RespCommand& /*cmd*/) {
  return core::PredicateFlags::kNx;
}

core::Result<core::PredicateFlags> ExtractCopyFlags(const core::RespCommand& cmd) {
  bool replace = false;
  for (size_t i = 3; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "REPLACE") replace = true;
  }
  // COPY always routes through the resolver — even REPLACE needs to read src.
  return replace ? core::PredicateFlags::kNone : core::PredicateFlags::kNx;
}

core::Result<core::PredicateFlags> ExtractHsetNxFlags(const core::RespCommand& /*cmd*/) {
  return core::PredicateFlags::kNx;
}

}  // namespace abyss::resp
