// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-durable-admission.h"

#include <kj/debug.h>

#include <algorithm>

namespace workerd::server {
namespace {

using Resources = DurableAdmission::Resources;

bool reserve(Resources& used, Resources limit, Resources amount) {
  // Compare against the remaining capacity to avoid overflow.
  if (amount.threads > limit.threads - used.threads ||
      amount.descriptors > limit.descriptors - used.descriptors ||
      amount.memory > limit.memory - used.memory ||
      amount.addressSpace > limit.addressSpace - used.addressSpace) {
    return false;
  }
  used.threads += amount.threads;
  used.descriptors += amount.descriptors;
  used.memory += amount.memory;
  used.addressSpace += amount.addressSpace;
  return true;
}

void release(Resources& used, Resources amount) {
  KJ_ASSERT(used.threads >= amount.threads && used.descriptors >= amount.descriptors &&
      used.memory >= amount.memory && used.addressSpace >= amount.addressSpace);
  used.threads -= amount.threads;
  used.descriptors -= amount.descriptors;
  used.memory -= amount.memory;
  used.addressSpace -= amount.addressSpace;
}

}  // namespace

DurableAdmission::DurableAdmission(Limits limits, uint64_t generation)
    : limits(limits),
      generation(generation) {
  KJ_REQUIRE(limits.mailboxDepth > 0 && limits.mailboxBytes > 0 && generation > 0,
      "invalid durable admission limits or generation");
  KJ_REQUIRE(limits.tree.memory > 0 && limits.event.memory > 0,
      "durable tree and event metadata require memory reservations");
}

DurableAdmission::Result DurableAdmission::queue(
    const Key& key, const Open& open, uint64_t bytes, Time deadline, Time now) {
  if (open.generation != generation) return {Status::STALE};
  if (deadline <= now) {
    ++counters.expired;
    return {Status::EXPIRED};
  }
  auto existing = objects.find(key);
  auto* tree = existing == objects.end() ? nullptr : &trees.at(existing->second);
  if (tree != nullptr && (tree->open != open || tree->closing)) return {Status::STALE};

  auto refuse = [&]() -> Result {
    ++counters.overloads;
    return {Status::OVERLOADED};
  };
  if (bytes > limits.mailboxBytes ||
      (tree != nullptr &&
          (tree->waiting >= limits.mailboxDepth ||
              bytes > limits.mailboxBytes - tree->waitingBytes))) {
    return refuse();
  }
  auto reserved = counters.reserved;
  if (!reserve(reserved, limits.capacity, limits.event) ||
      !reserve(reserved, limits.capacity, {.memory = bytes})) {
    return refuse();
  }
  const bool newTree = tree == nullptr;
  if (newTree &&
      ((limits.residentTarget != 0 && counters.trees >= limits.residentTarget) ||
          !reserve(reserved, limits.capacity, limits.tree))) {
    return refuse();
  }

  KJ_ASSERT(nextTree != 0 && nextEvent != 0);
  const uint64_t treeId = newTree ? nextTree++ : existing->second;
  const uint64_t eventId = nextEvent++;
  if (newTree) {
    Tree fresh{key, open, {}};
    fresh.events.emplace(eventId, Event{bytes, deadline});
    auto index = objects.emplace(key, treeId).first;
    try {
      tree = &trees.emplace(treeId, std::move(fresh)).first->second;
    } catch (...) {
      objects.erase(index);
      throw;
    }
    ++counters.trees;
  } else {
    tree->events.emplace(eventId, Event{bytes, deadline});
  }
  counters.reserved = reserved;
  ++tree->waiting;
  tree->waitingBytes += bytes;
  ++counters.waiting;
  counters.waitingBytes += bytes;
  return {Status::QUEUED, Ticket{treeId, eventId}, newTree};
}

bool DurableAdmission::publishHome(uint64_t tree, uint64_t home) {
  auto entry = trees.find(tree);
  if (entry == trees.end() || entry->second.home != 0 || home == 0) return false;
  entry->second.home = home;
  return true;
}

bool DurableAdmission::reserveAuxiliary(Resources amount) {
  if (reserve(counters.reserved, limits.capacity, amount)) return true;
  ++counters.overloads;
  return false;
}

void DurableAdmission::releaseAuxiliary(Resources amount) {
  release(counters.reserved, amount);
}

std::optional<uint64_t> DurableAdmission::homeFor(uint64_t tree) const {
  auto entry = trees.find(tree);
  if (entry == trees.end() || entry->second.home == 0) return std::nullopt;
  return entry->second.home;
}

std::optional<DurableAdmission::Ticket> DurableAdmission::next(uint64_t tree) const {
  auto entry = trees.find(tree);
  if (entry == trees.end() || entry->second.home == 0 || entry->second.closing) {
    return std::nullopt;
  }
  const auto& events = entry->second.events;
  auto pending = std::find_if(events.begin(), events.end(),
      [](const auto& event) { return event.second.phase == Phase::WAITING; });
  if (pending == events.end()) return std::nullopt;
  return Ticket{tree, pending->first};
}

DurableAdmission::Delivery DurableAdmission::begin(
    Ticket ticket, uint64_t home, const Key& key, const Open& open, Time now) {
  auto entry = trees.find(ticket.tree);
  if (entry == trees.end() || home == 0 || entry->second.home != home || entry->second.key != key ||
      entry->second.open != open || next(ticket.tree) != ticket) {
    return Delivery::STALE;
  }
  auto& tree = entry->second;
  auto event = tree.events.find(ticket.event);
  if (event->second.deadline <= now) {
    ++counters.expired;
    releaseEvent(tree, event);
    return Delivery::EXPIRED;
  }
  releaseWaiting(tree, event->second);
  event->second.phase = Phase::ACTIVE;
  ++counters.active;
  return Delivery::ACCEPTED;
}

DurableAdmission::Cancellation DurableAdmission::cancel(Ticket ticket) {
  auto entry = trees.find(ticket.tree);
  if (entry == trees.end()) return Cancellation::GONE;
  auto& tree = entry->second;
  auto event = tree.events.find(ticket.event);
  if (event == tree.events.end() || event->second.phase == Phase::CANCELING) {
    return Cancellation::GONE;
  }
  if (event->second.phase == Phase::ACTIVE) {
    event->second.phase = Phase::CANCELING;
    return Cancellation::SIGNAL_HOME;
  }
  releaseEvent(tree, event);
  return Cancellation::RELEASED;
}

bool DurableAdmission::complete(Ticket ticket, uint64_t home) {
  auto entry = trees.find(ticket.tree);
  if (entry == trees.end() || home == 0 || entry->second.home != home) return false;
  auto& tree = entry->second;
  auto event = tree.events.find(ticket.event);
  if (event == tree.events.end() || event->second.phase == Phase::WAITING) return false;
  releaseEvent(tree, event);
  return true;
}

std::vector<DurableAdmission::Ticket> DurableAdmission::expire(Time now) {
  std::vector<Ticket> expired;
  for (auto& [treeId, tree]: trees) {
    for (auto it = tree.events.begin(); it != tree.events.end();) {
      auto event = it++;
      if (event->second.phase != Phase::WAITING || event->second.deadline > now) continue;
      expired.push_back({treeId, event->first});
      releaseEvent(tree, event);
      ++counters.expired;
    }
  }
  return expired;
}

bool DurableAdmission::stopIdleTree(uint64_t tree, uint64_t home) {
  auto entry = trees.find(tree);
  if (entry == trees.end() || home == 0 || entry->second.home != home || entry->second.closing ||
      !entry->second.events.empty()) {
    return false;
  }
  entry->second.closing = true;
  return true;
}

bool DurableAdmission::stopIdleTree(uint64_t tree, uint64_t home, Time lastUse, Time now) {
  return now - lastUse >= IDLE_DELAY && stopIdleTree(tree, home);
}

bool DurableAdmission::releaseTree(uint64_t tree, uint64_t home) {
  auto entry = trees.find(tree);
  if (entry == trees.end() || !entry->second.closing || entry->second.home != home) {
    return false;
  }
  eraseTree(entry);
  return true;
}

std::vector<DurableAdmission::Ticket> DurableAdmission::failOpen(uint64_t tree) {
  auto entry = trees.find(tree);
  if (entry == trees.end() || entry->second.home != 0) return {};
  auto& pending = entry->second;
  std::vector<Ticket> refused;
  refused.reserve(pending.events.size());
  while (!pending.events.empty()) {
    auto event = pending.events.begin();
    refused.push_back({tree, event->first});
    releaseEvent(pending, event);
  }
  eraseTree(entry);
  return refused;
}

bool DurableAdmission::releaseUnassigned(uint64_t tree) {
  auto entry = trees.find(tree);
  if (entry == trees.end() || entry->second.home != 0 || !entry->second.events.empty())
    return false;
  eraseTree(entry);
  return true;
}

void DurableAdmission::releaseWaiting(Tree& tree, const Event& event) {
  --tree.waiting;
  tree.waitingBytes -= event.bytes;
  --counters.waiting;
  counters.waitingBytes -= event.bytes;
  release(counters.reserved, {.memory = event.bytes});
}

void DurableAdmission::releaseEvent(Tree& tree, std::map<uint64_t, Event>::iterator event) {
  if (event->second.phase == Phase::WAITING) {
    releaseWaiting(tree, event->second);
  } else {
    --counters.active;
  }
  release(counters.reserved, limits.event);
  tree.events.erase(event);
}

void DurableAdmission::eraseTree(std::map<uint64_t, Tree>::iterator entry) {
  KJ_ASSERT(entry->second.events.empty());
  objects.erase(entry->second.key);
  trees.erase(entry);
  --counters.trees;
  release(counters.reserved, limits.tree);
}

}  // namespace workerd::server
