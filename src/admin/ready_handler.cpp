#include "abyss/admin/ready_handler.h"

#include <string>
#include <utility>

namespace abyss::admin {

namespace {

void Append(std::string& out, std::string_view key, bool ok) {
  out += key;
  out += ": ";
  out += ok ? "true" : "false";
  out += '\n';
}

}  // namespace

ReadyHandler::ReadyHandler(ReadyChecks checks) : checks_(std::move(checks)) {}

HttpResponse ReadyHandler::Handle(const HttpRequest& /*request*/) {
  const bool tcp = checks_.tcp_bound && checks_.tcp_bound();
  const bool recovery = checks_.recovery_complete && checks_.recovery_complete();
  const bool not_down = checks_.not_shutting_down && checks_.not_shutting_down();
  const bool all = tcp && recovery && not_down;

  std::string body;
  body.reserve(96);
  Append(body, "tcp_bound", tcp);
  Append(body, "recovery_complete", recovery);
  Append(body, "not_shutting_down", not_down);

  if (all) return HttpResponse::Ok(std::move(body));
  return HttpResponse::ServiceUnavailable(std::move(body));
}

}  // namespace abyss::admin
