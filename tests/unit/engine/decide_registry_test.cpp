#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "abyss/engine/decide.h"
#include "abyss/resp/command_registry.h"
#include "decide_fixture.h"

namespace abyss::engine {
namespace {

std::string MessageOf(const Decision& d) { return d.error.has_value() ? d.error->message() : ""; }

bool IsArityError(const Decision& d) {
  return MessageOf(d).starts_with("wrong number of arguments for ");
}

bool IsWrite(resp::Dispatch dispatch) {
  return dispatch == resp::Dispatch::kWritePath || dispatch == resp::Dispatch::kConditionalWrite;
}

// Decide keeps its own command table; it must agree with the registry,
// which checks arity before a command reaches decide.
TEST(DecideRegistryTest, DecideKnowsEveryRegistryWriteAtItsArity) {
  size_t writes = 0;
  for (const resp::CommandSpec& spec : resp::GlobalRegistry().All()) {
    for (const resp::SubcommandSpec& sub : spec.subcommands) {
      EXPECT_FALSE(IsWrite(sub.dispatch)) << spec.name << " " << sub.name << ": decide has none";
    }
    if (!IsWrite(spec.dispatch)) continue;
    ++writes;
    SCOPED_TRACE(spec.name);
    // Exact when positive, a minimum when negative.
    const auto argc = static_cast<size_t>(std::abs(spec.arity));
    std::vector<std::string> args = {std::string(spec.name)};
    for (size_t i = 1; i < argc; ++i) args.push_back("arg" + std::to_string(i));

    const Decision at_arity = testing::DecideOn(testing::Keys{}, args);
    EXPECT_FALSE(MessageOf(at_arity).starts_with("unsupported write command"));
    EXPECT_FALSE(IsArityError(at_arity)) << MessageOf(at_arity);

    args.pop_back();
    const Decision short_by_one = testing::DecideOn(testing::Keys{}, args);
    EXPECT_TRUE(IsArityError(short_by_one)) << MessageOf(short_by_one);
  }
  EXPECT_GT(writes, 0U);
}

}  // namespace
}  // namespace abyss::engine
