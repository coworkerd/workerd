// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-durable-admission.h"

#include <kj/test.h>

namespace workerd::server {
namespace {

using Admission = DurableAdmission;
using Status = Admission::Status;
using Delivery = Admission::Delivery;
using Cancellation = Admission::Cancellation;
using namespace std::chrono_literals;

const Admission::Key first{"namespace", "first"};
const Admission::Key second{"namespace", "second"};
const Admission::Open open{1, 2, 3, 4, "definition", "Counter"};
constexpr Admission::Time now{};
constexpr Admission::Time deadline = now + 10s;
constexpr Admission::Limits limits{
  .capacity = {8, 50, 3000, 100},
  .tree = {1, 3, 50, 10},
  .event = {0, 1, 10, 0},
  .residentTarget = 4,
  .mailboxDepth = 2,
  .mailboxBytes = 100,
};

Admission::Ticket queued(Admission::Result result, bool needsHome) {
  KJ_REQUIRE(result.status == Status::QUEUED);
  KJ_EXPECT(result.needsHome == needsHome);
  KJ_REQUIRE(result.ticket.has_value());
  return *result.ticket;
}

KJ_TEST("durable admission fences the generation, identity, home and queued event") {
  Admission table(limits, 1);
  auto stale = open;
  stale.generation = 2;
  KJ_EXPECT(table.queue(first, stale, 40, deadline, now).status == Status::STALE);
  const auto event = queued(table.queue(first, open, 40, deadline, now), true);
  const auto queuedAfter = queued(table.queue(first, open, 30, deadline, now), false);
  KJ_EXPECT(!table.next(event.tree).has_value());
  KJ_EXPECT(table.begin(event, 9, first, open, now) == Delivery::STALE);
  KJ_EXPECT(table.publishHome(event.tree, 9));
  KJ_EXPECT(!table.publishHome(event.tree, 10));
  KJ_EXPECT((table.homeFor(event.tree) == 9));
  KJ_EXPECT((table.next(event.tree) == event));

  for (auto changed:
      {&Admission::Open::dataEpoch, &Admission::Open::shardEpoch, &Admission::Open::openId}) {
    stale = open;
    ++(stale.*changed);
    KJ_EXPECT(table.queue(first, stale, 10, deadline, now).status == Status::STALE);
    KJ_EXPECT(table.begin(event, 9, first, stale, now) == Delivery::STALE);
  }
  stale = open;
  stale.definition = "another definition";
  KJ_EXPECT(table.begin(event, 9, first, stale, now) == Delivery::STALE);
  stale = open;
  stale.className = "OtherCounter";
  KJ_EXPECT(table.begin(event, 9, first, stale, now) == Delivery::STALE);
  KJ_EXPECT(table.begin(event, 9, second, open, now) == Delivery::STALE);
  KJ_EXPECT(table.begin(event, 10, first, open, now) == Delivery::STALE);
  KJ_EXPECT(table.begin(queuedAfter, 9, first, open, now) == Delivery::STALE);
  KJ_EXPECT(table.begin(event, 9, first, open, now) == Delivery::ACCEPTED);
  KJ_EXPECT(table.begin(event, 9, first, open, now) == Delivery::STALE);
  KJ_EXPECT(table.begin(queuedAfter, 9, first, open, now) == Delivery::ACCEPTED);
  KJ_EXPECT(table.state().active == 2);
}

KJ_TEST("durable admission bounds waiting events and bytes per object") {
  Admission table(limits, 1);
  const auto firstEvent = queued(table.queue(first, open, 70, deadline, now), true);
  KJ_EXPECT(table.queue(first, open, 31, deadline, now).status == Status::OVERLOADED);
  const auto other = queued(table.queue(second, open, 100, deadline, now), true);
  const auto secondEvent = queued(table.queue(first, open, 30, deadline, now), false);
  KJ_EXPECT(table.queue(first, open, 0, deadline, now).status == Status::OVERLOADED);
  KJ_EXPECT(table.state().waiting == 3);
  KJ_EXPECT(table.state().waitingBytes == 200);
  KJ_EXPECT(table.publishHome(firstEvent.tree, 1));
  KJ_EXPECT(table.begin(firstEvent, 1, first, open, now) == Delivery::ACCEPTED);
  KJ_EXPECT((table.next(firstEvent.tree) == secondEvent));
  KJ_EXPECT(table.state().waitingBytes == 130);
  queued(table.queue(first, open, 70, deadline, now), false);
  KJ_EXPECT((table.failOpen(other.tree) == std::vector{other}));
  KJ_EXPECT(table.state().waitingBytes == 100);
  KJ_EXPECT(table.state().overloads == 2);
}

KJ_TEST("durable admission expires queued events and releases each reservation once") {
  Admission table(limits, 1);
  KJ_EXPECT(table.queue(first, open, 50, now, now).status == Status::EXPIRED);
  const auto active = queued(table.queue(first, open, 70, deadline, now), true);
  KJ_EXPECT(table.publishHome(active.tree, 1));
  KJ_EXPECT(table.begin(active, 1, first, open, now) == Delivery::ACCEPTED);
  const auto expiring = queued(table.queue(first, open, 80, deadline, now), false);
  const auto canceled = queued(table.queue(first, open, 20, deadline, now), false);
  KJ_EXPECT(table.cancel(canceled) == Cancellation::RELEASED);
  KJ_EXPECT(table.cancel(canceled) == Cancellation::GONE);
  KJ_EXPECT(table.expire(deadline - 1ms).empty());
  KJ_EXPECT((table.expire(deadline) == std::vector{expiring}));
  KJ_EXPECT(table.expire(deadline).empty());
  KJ_EXPECT(table.state().active == 1);
  KJ_EXPECT(table.state().waiting == 0);
  KJ_EXPECT(table.cancel(active) == Cancellation::SIGNAL_HOME);
  KJ_EXPECT(table.cancel(active) == Cancellation::GONE);
  KJ_EXPECT(table.state().reserved.descriptors == 4);
  KJ_EXPECT(!table.stopIdleTree(active.tree, 1));
  KJ_EXPECT(!table.complete(active, 2));
  KJ_EXPECT(table.complete(active, 1));
  KJ_EXPECT(!table.complete(active, 1));
  KJ_EXPECT((table.state().reserved == limits.tree));
  const auto late = queued(table.queue(first, open, 100, deadline + 1s, deadline), false);
  KJ_EXPECT(table.begin(late, 1, first, open, deadline + 1s) == Delivery::EXPIRED);
  KJ_EXPECT(table.state().expired == 3);
  KJ_EXPECT((table.state().reserved == limits.tree));
}

KJ_TEST("durable admission retains a closing tree until its home acknowledges release") {
  Admission table(limits, 1);
  const auto old = queued(table.queue(first, open, 30, deadline, now), true);
  KJ_EXPECT(table.publishHome(old.tree, 1));
  KJ_EXPECT(table.cancel(old) == Cancellation::RELEASED);
  KJ_EXPECT(!table.stopIdleTree(old.tree, 2));
  KJ_EXPECT(table.stopIdleTree(old.tree, 1));
  KJ_EXPECT(table.queue(first, open, 30, deadline, now).status == Status::STALE);
  KJ_EXPECT(!table.releaseTree(old.tree, 2));
  KJ_EXPECT(table.state().trees == 1);
  KJ_EXPECT((table.state().reserved == limits.tree));
  KJ_EXPECT(table.releaseTree(old.tree, 1));
  KJ_EXPECT((table.state().reserved == Admission::Resources{}));

  auto nextOpen = open;
  ++nextOpen.openId;
  const auto replacement = queued(table.queue(first, nextOpen, 30, deadline, now), true);
  KJ_EXPECT(replacement.tree != old.tree);
  KJ_EXPECT(table.publishHome(replacement.tree, 1));
  KJ_EXPECT(!table.releaseTree(old.tree, 1));
  KJ_EXPECT(table.begin(replacement, 1, first, nextOpen, now) == Delivery::ACCEPTED);
}

KJ_TEST("durable admission returns an unassigned tree only when its final waiting call leaves") {
  Admission table(limits, 1);
  const auto leaving = queued(table.queue(first, open, 30, deadline, now), true);
  const auto waiting = queued(table.queue(first, open, 40, deadline, now), false);
  KJ_EXPECT(table.cancel(leaving) == Cancellation::RELEASED);
  KJ_EXPECT(!table.releaseUnassigned(leaving.tree));
  KJ_EXPECT(table.state().trees == 1);
  KJ_EXPECT(table.cancel(waiting) == Cancellation::RELEASED);
  KJ_EXPECT(table.releaseUnassigned(waiting.tree));
  KJ_EXPECT(!table.releaseUnassigned(waiting.tree));
  KJ_EXPECT((table.state().reserved == Admission::Resources{}));
}

KJ_TEST("durable idle retirement fences reentry and cancellation at the clock boundary") {
  Admission table(limits, 1);
  auto clock = now;
  auto lastUse = clock;
  const auto firstCall = queued(table.queue(first, open, 0, clock + 30s, clock), true);
  KJ_ASSERT(table.publishHome(firstCall.tree, 1));
  KJ_ASSERT(table.begin(firstCall, 1, first, open, clock) == Delivery::ACCEPTED);
  KJ_ASSERT(table.complete(firstCall, 1));

  clock = now + 10s - 1ns;
  KJ_EXPECT(!table.stopIdleTree(firstCall.tree, 1, lastUse, clock));
  clock += 1ns;
  // Reentry wins the routing lock at the idle deadline. A timer with an old
  // last-use sample cannot close a tree that now owns a waiting event.
  const auto reentry = queued(table.queue(first, open, 20, clock + 30s, clock), false);
  KJ_EXPECT(!table.stopIdleTree(firstCall.tree, 1, lastUse, clock));
  KJ_ASSERT(table.begin(reentry, 1, first, open, clock) == Delivery::ACCEPTED);
  clock += 20s;
  KJ_EXPECT(!table.stopIdleTree(firstCall.tree, 1, lastUse, clock));
  KJ_EXPECT(table.cancel(reentry) == Cancellation::SIGNAL_HOME);
  KJ_EXPECT(!table.stopIdleTree(firstCall.tree, 1, lastUse, clock));
  KJ_ASSERT(table.complete(reentry, 1));
  lastUse = clock;
  KJ_EXPECT(!table.stopIdleTree(firstCall.tree, 1, lastUse, clock + 10s - 1ns));

  clock += 10s;
  // Retirement wins this time. The old reservation is held until home close;
  // a stale completion must not affect the replacement's independent identity.
  KJ_ASSERT(table.stopIdleTree(firstCall.tree, 1, lastUse, clock));
  KJ_EXPECT(table.queue(first, open, 0, clock + 30s, clock).status == Status::STALE);
  KJ_EXPECT((table.state().reserved == limits.tree));
  KJ_ASSERT(table.releaseTree(firstCall.tree, 1));
  const auto reopened = queued(table.queue(first, open, 0, clock + 30s, clock), true);
  KJ_EXPECT(reopened.tree != firstCall.tree);
  KJ_ASSERT(table.publishHome(reopened.tree, 2));
  KJ_EXPECT(!table.complete(reentry, 1));
  KJ_EXPECT(!table.releaseTree(firstCall.tree, 1));
  KJ_EXPECT(table.begin(reopened, 2, first, open, clock) == Delivery::ACCEPTED);
  KJ_ASSERT(table.complete(reopened, 2));
  KJ_ASSERT(table.stopIdleTree(reopened.tree, 2, clock, clock + 10s));
  KJ_ASSERT(table.releaseTree(reopened.tree, 2));
  KJ_EXPECT((table.state().reserved == Admission::Resources{}));
}

KJ_TEST("each durable reservation ceiling refuses before publishing a new tree") {
  auto below = limits;
  // The resident count is disabled. Cap capacity at one tree and event in each dimension in turn.
  below.residentTarget = 0;
  for (auto dimension: {&Admission::Resources::threads, &Admission::Resources::descriptors,
         &Admission::Resources::memory, &Admission::Resources::addressSpace}) {
    below.capacity = limits.capacity;
    below.capacity.*dimension = limits.tree.*dimension + limits.event.*dimension;
    Admission table(below, 1);
    const auto reservation = queued(table.queue(first, open, 0, deadline, now), true);
    const auto before = table.state().reserved;
    KJ_EXPECT(table.queue(second, open, 0, deadline, now).status == Status::OVERLOADED);
    KJ_EXPECT(table.state().trees == 1);
    KJ_EXPECT((table.state().reserved == before));
    KJ_EXPECT((table.failOpen(reservation.tree) == std::vector{reservation}));
    KJ_EXPECT((table.state().reserved == Admission::Resources{}));
    queued(table.queue(second, open, 0, deadline, now), true);
  }
  below = limits;
  below.residentTarget = 1;
  Admission table(below, 1);
  const auto reservation = queued(table.queue(first, open, 0, deadline, now), true);
  KJ_EXPECT(table.queue(second, open, 0, deadline, now).status == Status::OVERLOADED);
  KJ_EXPECT((table.failOpen(reservation.tree) == std::vector{reservation}));
  queued(table.queue(second, open, 0, deadline, now), true);
}

}  // namespace
}  // namespace workerd::server
