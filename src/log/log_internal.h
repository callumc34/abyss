#pragma once

#include <memory>
#include <span>

#include "abyss/log/log.h"

namespace spdlog::sinks {
class sink;
}

namespace abyss::log::internal {

struct LoggerImpl;

void ResetForTesting();
void InstallTestSink(std::shared_ptr<spdlog::sinks::sink> sink);
std::span<const LogField> CurrentThreadFields() noexcept;

std::string FormatJsonForTesting(Level level, std::string_view component, std::string_view msg,
                                 std::span<const LogField> fields);
std::string FormatTextForTesting(Level level, std::string_view component, std::string_view msg,
                                 std::span<const LogField> fields);

}  // namespace abyss::log::internal
