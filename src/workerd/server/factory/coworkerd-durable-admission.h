// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <chrono>
#include <compare>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace workerd::server {

// One router owns this table. The home loops report closure and completion by ID.
// Neither the table nor its event credits own actor or isolate state.
class DurableAdmission {
 public:
  using Clock = std::chrono::steady_clock;
  using Time = Clock::time_point;
  static constexpr auto IDLE_DELAY = std::chrono::seconds(10);

  struct Resources {
    uint64_t threads = 0;
    uint64_t descriptors = 0;
    uint64_t memory = 0;
    uint64_t addressSpace = 0;
    bool operator==(const Resources&) const = default;
  };

  struct Limits {
    // Capacity available to the dispatcher after withholding control and shutdown headroom.
    Resources capacity;
    Resources tree;
    Resources event;
    uint32_t residentTarget;
    uint32_t mailboxDepth;
    uint64_t mailboxBytes;
  };

  struct Key {
    std::string namespaceId;
    std::string objectId;
    auto operator<=>(const Key&) const = default;
  };

  struct Open {
    uint64_t generation;
    uint64_t dataEpoch;
    uint64_t shardEpoch;
    uint64_t openId;
    std::string definition;
    std::string className;
    bool operator==(const Open&) const = default;
  };

  struct Ticket {
    uint64_t tree;
    uint64_t event;
    bool operator==(const Ticket&) const = default;
  };

  enum class Status { QUEUED, OVERLOADED, EXPIRED, STALE };
  struct Result {
    Status status;
    std::optional<Ticket> ticket = std::nullopt;
    bool needsHome = false;
  };

  enum class Cancellation { GONE, RELEASED, SIGNAL_HOME };
  enum class Delivery { ACCEPTED, EXPIRED, STALE };

  struct State {
    Resources reserved = {};
    uint64_t trees = 0;
    uint64_t waiting = 0;
    uint64_t waitingBytes = 0;
    uint64_t active = 0;
    uint64_t overloads = 0;
    uint64_t expired = 0;
  };

  DurableAdmission(Limits limits, uint64_t generation);

  Result queue(const Key& key, const Open& open, uint64_t bytes, Time deadline, Time now);
  bool publishHome(uint64_t tree, uint64_t home);
  std::optional<uint64_t> homeFor(uint64_t tree) const;
  // Check the full descriptor and route the first live waiting event to an already published home.
  // Starting an event returns waiting credit while retaining its execution reservation.
  std::optional<Ticket> next(uint64_t tree) const;
  Delivery begin(Ticket ticket, uint64_t home, const Key& key, const Open& open, Time now);
  Cancellation cancel(Ticket ticket);
  // Refuse a waiting call before acknowledging it, returning its event reservation.
  bool refuseWaiting(Ticket ticket);
  bool complete(Ticket ticket, uint64_t home);
  bool markSocket(Ticket ticket, uint64_t home);
  std::vector<Ticket> expire(Time now);

  // Reset closes admission immediately, releases waiting calls, and retains active credits
  // until their home completes them. The home still owns every actor handle until releaseTree().
  bool beginReset(uint64_t tree, uint64_t home);
  // Idle close requires an empty mailbox and no active calls. It is also the drain check for
  // a reset already in progress. Keep the tree reservation until the home releases its handles.
  bool stopIdleTree(uint64_t tree, uint64_t home);
  bool stopIdleTreeWithSockets(uint64_t tree, uint64_t home);
  // Normal retirement checks the idle deadline as well as mailbox/active ownership.
  // Pressure retirement uses the unconditional overload above.
  bool stopIdleTree(uint64_t tree, uint64_t home, Time lastUse, Time now);
  bool releaseTree(uint64_t tree, uint64_t home);
  // The home failed to open. Releases all waiting credits without a close acknowledgement.
  std::vector<Ticket> failOpen(uint64_t tree);
  bool releaseUnassigned(uint64_t tree);
  bool reserveAuxiliary(Resources amount);
  void releaseAuxiliary(Resources amount);

  State state() const {
    return counters;
  }

 private:
  enum class Phase { WAITING, ACTIVE, CANCELING };

  struct Event {
    uint64_t bytes;
    Time deadline;
    Phase phase = Phase::WAITING;
    bool socket = false;
  };

  struct Tree {
    Key key;
    Open open;
    std::map<uint64_t, Event> events;
    uint64_t home = 0;
    uint64_t waitingBytes = 0;
    uint32_t waiting = 0;
    bool closing = false;
  };

  void releaseWaiting(Tree& tree, const Event& event);
  void releaseEvent(Tree& tree, std::map<uint64_t, Event>::iterator event);
  void eraseTree(std::map<uint64_t, Tree>::iterator tree);

  const Limits limits;
  const uint64_t generation;
  State counters;
  uint64_t nextTree = 1, nextEvent = 1;
  std::map<Key, uint64_t> objects;
  std::map<uint64_t, Tree> trees;
};

// Owned by the home loop. Admission and socket handoff still have to succeed
// before a decision to retire can close the actor.
class DurableIdleLifecycle {
 public:
  using Time = DurableAdmission::Time;
  using Pending = std::multiset<Time>::iterator;
  enum class Decision { WAIT, RETIRE, CANCEL_PENDING };

  DurableIdleLifecycle(Time opened, std::chrono::seconds nonHibernatingDelay);
  void eventFinished(Time now);
  Pending pendingStarted(Time now);
  void pendingFinished(Pending pending, Time now);
  Decision check(Time now, bool hasOrdinarySocket, bool pressured);

 private:
  Time lastEvent, lastUnblocked;
  std::chrono::seconds nonHibernatingDelay;
  std::multiset<Time> pending;
  bool ordinarySocket = false;
};

}  // namespace workerd::server
