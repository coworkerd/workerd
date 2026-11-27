// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/mutex.h>
#include <kj/time.h>

namespace workerd::threaded {

// A sliding window of wall time under the isolate lock. Each sample ends at the latest
// completed bucket boundary. The extra bucket retains work since that boundary.
class IsolateBusyWindow final {
 public:
  static constexpr uint64_t BUCKET_NS = 100 * 1000000;
  static constexpr uint64_t WINDOW_BUCKETS = 30;
  static constexpr uint64_t WINDOW_NS = BUCKET_NS * WINDOW_BUCKETS;

  struct Sample {
    uint64_t throughNs;
    uint64_t busyNs;

    uint64_t permille() const {
      return busyNs * 1000 / WINDOW_NS;
    }
  };

  explicit IsolateBusyWindow(const kj::MonotonicClock& clock): clock(clock) {}

  void reset() {
    auto locked = state.lockExclusive();
    *locked = State{};
  }

  // Serialize each clock read with the state it describes. The snapshot thread never
  // takes the V8 locker. Only the outer V8 lock owner calls acquired() and releasing().
  void acquired() {
    auto locked = state.lockExclusive();
    locked->heldSince = nowNs();
  }

  void releasing() {
    auto locked = state.lockExclusive();
    const auto heldSince = KJ_UNWRAP_OR(locked->heldSince, return);
    const auto end = nowNs();
    const auto oldest = firstBucket(end / BUCKET_NS) * BUCKET_NS;
    auto start = kj::max(heldSince, oldest);
    while (start < end) {
      const auto index = start / BUCKET_NS;
      const auto through = kj::min(end, (index + 1) * BUCKET_NS);
      auto& bucket = locked->buckets[index % (WINDOW_BUCKETS + 1)];
      if (bucket.index != index) bucket = {index, 0};
      bucket.busyNs += through - start;
      start = through;
    }
    locked->heldSince = kj::none;
  }

  Sample sample() const {
    auto locked = state.lockShared();
    const auto end = nowNs() / BUCKET_NS;
    const auto start = firstBucket(end);
    Sample sample{end * BUCKET_NS, 0};
    for (const auto& bucket: locked->buckets) {
      if (bucket.index >= start && bucket.index < end) sample.busyNs += bucket.busyNs;
    }
    KJ_IF_SOME(heldSince, locked->heldSince) {
      const auto heldFrom = kj::max(heldSince, start * BUCKET_NS);
      if (heldFrom < sample.throughNs) sample.busyNs += sample.throughNs - heldFrom;
    }
    return sample;
  }

 private:
  static uint64_t firstBucket(uint64_t end) {
    return end - kj::min(end, WINDOW_BUCKETS);
  }

  uint64_t nowNs() const {
    return (clock.now() - kj::origin<kj::TimePoint>()) / kj::NANOSECONDS;
  }

  struct Bucket {
    uint64_t index;
    uint64_t busyNs;
  };
  struct State {
    Bucket buckets[WINDOW_BUCKETS + 1]{};
    kj::Maybe<uint64_t> heldSince;
  };
  const kj::MonotonicClock& clock;
  kj::MutexGuarded<State> state;
};

}  // namespace workerd::threaded
