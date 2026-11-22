// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-durable-cpu.h"

#include <kj/test.h>

namespace workerd::server {
namespace {

KJ_TEST("Durable CPU follows the same event after interleaved work") {
  DurableCpuAccount first;
  DurableCpuAccount second;
  uint64_t clock = 0;
  auto run = [&](DurableCpuAccount& account, uint64_t cpu) {
    const auto start = clock;
    clock += cpu;
    account.charge(start, clock);
  };

  run(first, 8);
  run(second, 3);
  // A suspended event spends no CPU while other objects run on the home.
  clock += 400;
  run(first, 3);
  KJ_EXPECT(first.remaining(10) == 0);
  KJ_EXPECT(second.remaining(10) == 7);
  KJ_EXPECT(DurableCpuAccount().remaining(10) == 10);
}

KJ_TEST("A durable CPU account honors both lower and higher configured limits") {
  constexpr uint64_t second = 1000000000;
  DurableCpuAccount event;
  event.charge(4 * second, 29 * second);
  KJ_EXPECT(event.consumed() == 25 * second);
  KJ_EXPECT(event.remaining(5 * second) == 0);
  KJ_EXPECT(event.remaining(30 * second) == 5 * second);
}

KJ_TEST("A higher durable CPU budget permits legitimate work beyond thirty seconds") {
  constexpr uint64_t second = 1000000000;
  DurableCpuAccount event;
  event.charge(0, 35 * second);
  KJ_EXPECT(event.remaining(30 * second) == 0);
  KJ_EXPECT(event.remaining(45 * second) == 10 * second);
  // The same delivered event resumes; an await did not give it a fresh budget.
  event.charge(100 * second, 109 * second);
  KJ_EXPECT(event.remaining(45 * second) == second);
  event.charge(200 * second, 202 * second);
  KJ_EXPECT(event.remaining(45 * second) == 0);
  KJ_EXPECT(DurableCpuAccount().remaining(45 * second) == 45 * second);
}

}  // namespace
}  // namespace workerd::server
