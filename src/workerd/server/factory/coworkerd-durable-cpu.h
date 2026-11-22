// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <algorithm>
#include <cstdint>

namespace workerd::server {

// Each delivered event owns one account. The home supplies thread CPU clock samples at
// execution boundaries, including native SQLite work within a JavaScript turn.
class DurableCpuAccount {
 public:
  void charge(uint64_t start, uint64_t end) {
    used += end - start;
  }

  uint64_t consumed() const {
    return used;
  }

  uint64_t remaining(uint64_t maximum) const {
    return maximum - std::min(maximum, used);
  }

 private:
  uint64_t used = 0;
};

}  // namespace workerd::server
