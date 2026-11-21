// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

namespace workerd::server {

// A bounded record of synchronous work on one home. A suspended event does not count as busy.
// The router reads completed turns together with the current turn, if one is still executing.
class DurableHomeLoad {
 public:
  using Clock = std::chrono::steady_clock;
  using Time = Clock::time_point;
  using Duration = Clock::duration;

  explicit DurableHomeLoad(Duration window);

  void begin(Time now);
  void end(Time now);
  Duration busyFor(Time now) const;
  Duration window() const;

 private:
  static constexpr int64_t BUCKETS = 16;
  struct Bucket {
    int64_t index = -1;
    Duration busy{};
  };

  const Duration width;
  std::array<Bucket, BUCKETS + 1> buckets;
  std::optional<Time> running;
};

struct DurableCopyCandidate {
  uint64_t home;
  std::string_view definition;
  uint32_t residentTrees;
  uint64_t memoryBytes;
  DurableHomeLoad::Duration busyFor;
  bool placing;
};

struct DurablePlacementLimits {
  uint32_t treesPerCopy;
  uint64_t placementBytes;
  DurableHomeLoad::Duration busyThreshold;
};

// An existing tree keeps its placement. Call this only for a fresh or newly woken tree.
// An empty result requires a separately reserved new home, or an overload refusal.
std::optional<uint64_t> chooseDurableCopy(std::span<const DurableCopyCandidate> candidates,
    std::string_view definition,
    DurablePlacementLimits limits);

// The caller samples again after idle trees are destroyed. A distinct observation
// after closing placement is required before it can reset the copy.
class DurablePressure {
 public:
  enum class Action { ACCEPT, EVICT_IDLE, CLOSE_PLACEMENT, WAIT, RESET };

  Action sample(bool abovePlacement, bool aboveHardLimit, bool idleTree);
  bool accepting() const { return phase == Phase::OPEN; }
  bool resetting() const { return phase == Phase::RESETTING; }
  void closed() { phase = Phase::OPEN; }

 private:
  enum class Phase { OPEN, CLOSED, RESETTING };
  Phase phase = Phase::OPEN;
};

}  // namespace workerd::server
