// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-durable-placement.h"

#include <kj/test.h>

namespace workerd::server {
namespace {

using namespace std::chrono_literals;
constexpr DurableHomeLoad::Time origin{};

KJ_TEST("durable placement excludes a busy copy before it fills") {
  DurableHomeLoad hot(4s);
  hot.begin(origin);
  DurableCopyCandidate candidates[] = {
    {1, "version-a", 2, 500, hot.busyFor(origin + 500ms), true},
    {2, "version-a", 1, 500, 0ms, true},
    {3, "version-b", 3, 500, 0ms, true},
  };
  const DurablePlacementLimits limits{4, 1000, 2s};
  KJ_EXPECT((chooseDurableCopy(candidates, "version-a", limits) == 1));
  candidates[0].busyFor = hot.busyFor(origin + 2s);
  KJ_EXPECT((chooseDurableCopy(candidates, "version-a", limits) == 2));
  candidates[1].memoryBytes = 1000;
  KJ_EXPECT(!chooseDurableCopy(candidates, "version-a", limits).has_value());
  hot.end(origin + 3s);
  candidates[0].busyFor = hot.busyFor(origin + 8s);
  KJ_EXPECT((chooseDurableCopy(candidates, "version-a", limits) == 1));
  candidates[0].residentTrees = 4;
  KJ_EXPECT(!chooseDurableCopy(candidates, "version-a", limits).has_value());
  candidates[0].residentTrees = 1;
  candidates[0].placing = false;
  KJ_EXPECT(!chooseDurableCopy(candidates, "version-a", limits).has_value());
}

KJ_TEST("durable home time counts execution across turns and ages out across idle time") {
  DurableHomeLoad home(4s);
  home.begin(origin + 1s);
  home.end(origin + 2s);
  home.begin(origin + 3s);
  KJ_EXPECT((home.busyFor(origin + 3500ms) == 1500ms));
  home.end(origin + 4s);
  KJ_EXPECT((home.busyFor(origin + 5500ms) == 1500ms));
  KJ_EXPECT((home.busyFor(origin + 10s) == 0ms));
  home.begin(origin + 11s);
  home.end(origin + 100s);
  KJ_EXPECT((home.busyFor(origin + 100s) == 4s));
  KJ_EXPECT((home.busyFor(origin + 105s) == 0ms));
}

KJ_TEST("durable home retains the partial bucket at the start of its rolling window") {
  DurableHomeLoad home(4s);
  home.begin(origin + 63ms);
  home.end(origin + 100ms);
  home.begin(origin + 4050ms);
  home.end(origin + 4070ms);
  KJ_EXPECT((home.busyFor(origin + 4070ms) == 57ms));
}

}  // namespace
}  // namespace workerd::server
