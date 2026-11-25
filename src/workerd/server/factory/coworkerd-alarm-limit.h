// Copyright (c) 2026 Coworkerd contributors
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/debug.h>
#include <kj/time.h>

#include <cstdlib>

namespace workerd {

inline kj::Duration coworkerdAlarmLimit() {
  static const auto limit = []() {
    constexpr auto MAX_MS = 15 * 60 * 1000;
    const auto* setting = std::getenv("COWORKERD_TEST_ALARM_WALL_MS");
    if (setting == nullptr) return MAX_MS * kj::MILLISECONDS;
    auto milliseconds = kj::StringPtr(setting).parseAs<unsigned int>();
    KJ_REQUIRE(milliseconds > 0 && milliseconds <= MAX_MS, "invalid test alarm wall time");
    return milliseconds * kj::MILLISECONDS;
  }();
  return limit;
}

}  // namespace workerd
