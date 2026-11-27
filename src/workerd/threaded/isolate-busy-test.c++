// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "isolate-busy.h"

#include <kj/test.h>

namespace workerd::threaded {
namespace {

class TestClock final: public kj::MonotonicClock {
 public:
  kj::TimePoint now() const override {
    return kj::origin<kj::TimePoint>() + time;
  }

  kj::Duration time = 0 * kj::NANOSECONDS;
};

KJ_TEST("isolate busy time includes a live hold and rolls off bucket by bucket") {
  TestClock clock;
  IsolateBusyWindow window(clock);
  IsolateBusyWindow other(clock);

  clock.time = 50 * kj::MILLISECONDS;
  window.acquired();
  clock.time = 150 * kj::MILLISECONDS;
  KJ_EXPECT(window.sample().throughNs == 100000000);
  KJ_EXPECT(window.sample().busyNs == 50000000);
  window.releasing();
  clock.time = 200 * kj::MILLISECONDS;
  KJ_EXPECT(window.sample().busyNs == 100000000);
  KJ_EXPECT(other.sample().busyNs == 0);

  // The second hold leaves an idle gap even though it ends in the same window.
  clock.time = 225 * kj::MILLISECONDS;
  window.acquired();
  clock.time = 250 * kj::MILLISECONDS;
  window.releasing();
  clock.time = 3099 * kj::MILLISECONDS;
  KJ_EXPECT(window.sample().throughNs == 3000000000);
  KJ_EXPECT(window.sample().busyNs == 125000000);
  clock.time = 3100 * kj::MILLISECONDS;
  KJ_EXPECT(window.sample().busyNs == 75000000);
  clock.time = 3200 * kj::MILLISECONDS;
  KJ_EXPECT(window.sample().busyNs == 25000000);
  clock.time = 3300 * kj::MILLISECONDS;
  KJ_EXPECT(window.sample().busyNs == 0);

  window.acquired();
  clock.time = 6400 * kj::MILLISECONDS;
  KJ_EXPECT(window.sample().permille() == 1000);
  window.releasing();
  KJ_EXPECT(window.sample().permille() == 1000);
  clock.time = 9400 * kj::MILLISECONDS;
  KJ_EXPECT(window.sample().permille() == 0);
}

KJ_TEST("the published permille crosses an inclusive fractional threshold at the exact boundary") {
  TestClock clock;
  const auto readingAfter = [&](kj::Duration held) {
    IsolateBusyWindow window(clock);
    clock.time = 0 * kj::MILLISECONDS;
    window.acquired();
    clock.time = held;
    window.releasing();
    clock.time = 3 * kj::SECONDS;
    return window.sample().permille();
  };

  constexpr uint64_t threshold = 900;
  constexpr auto boundary = 2700 * kj::MILLISECONDS;
  const auto below = readingAfter(boundary - 1 * kj::NANOSECONDS);
  const auto at = readingAfter(boundary);
  const auto above = readingAfter(boundary + 1 * kj::NANOSECONDS);
  KJ_EXPECT(below == 899);
  KJ_EXPECT(below < threshold);
  KJ_EXPECT(at == threshold);
  KJ_EXPECT(at >= threshold);
  KJ_EXPECT(above >= threshold);
}

}  // namespace
}  // namespace workerd::threaded
