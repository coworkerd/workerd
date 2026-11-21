// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-durable-placement.h"

#include <kj/debug.h>

#include <algorithm>

namespace workerd::server {

DurableHomeLoad::DurableHomeLoad(Duration window): width(window / BUCKETS) {
  KJ_REQUIRE(width > Duration::zero(), "durable placement busy window is too short");
}

void DurableHomeLoad::begin(Time now) {
  KJ_ASSERT(!running && now >= Time{});
  running = now;
}

void DurableHomeLoad::end(Time now) {
  KJ_ASSERT(running.has_value() && now >= *running);
  auto start = std::max(*running, now - window());
  while (start < now) {
    const auto index = start.time_since_epoch() / width;
    const auto end = std::min(now, Time{} + width * (index + 1));
    auto& bucket = buckets[index % buckets.size()];
    if (bucket.index != index) bucket = {index};
    bucket.busy += end - start;
    start = end;
  }
  running.reset();
}

DurableHomeLoad::Duration DurableHomeLoad::busyFor(Time now) const {
  const auto oldest = now - window();
  Duration busy{};
  for (const auto& bucket: buckets) {
    if (bucket.index < 0) continue;
    const auto start = Time{} + width * bucket.index;
    const auto end = std::min(now, start + width);
    if (end <= oldest || end <= start) continue;
    // At most one historical bucket straddles the cutoff. Its contribution
    // conservatively assumes that busy time was at the end of the bucket.
    busy += std::min(bucket.busy, end - std::max(start, oldest));
  }
  if (running && *running <= now) busy += now - std::max(*running, oldest);
  return std::min(busy, window());
}

DurableHomeLoad::Duration DurableHomeLoad::window() const {
  return width * BUCKETS;
}

std::optional<uint64_t> chooseDurableCopy(std::span<const DurableCopyCandidate> candidates,
    std::string_view definition,
    DurablePlacementLimits limits) {
  const DurableCopyCandidate* selected = nullptr;
  for (const auto& candidate: candidates) {
    if (!candidate.placing || candidate.definition != definition ||
        candidate.residentTrees >= limits.treesPerCopy ||
        candidate.memoryBytes >= limits.placementBytes ||
        candidate.busyFor >= limits.busyThreshold) {
      continue;
    }
    // Pack quiet, warm copies first to bound isolate and thread growth.
    if (selected == nullptr || candidate.residentTrees > selected->residentTrees ||
        (candidate.residentTrees == selected->residentTrees &&
            candidate.busyFor < selected->busyFor)) {
      selected = &candidate;
    }
  }
  return selected == nullptr ? std::nullopt : std::optional(selected->home);
}

}  // namespace workerd::server
