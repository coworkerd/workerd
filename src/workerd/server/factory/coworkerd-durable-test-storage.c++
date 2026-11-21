// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-durable-test-storage.h"

#include <kj/debug.h>

#include <algorithm>

namespace workerd::server {
namespace {

constexpr kj::StringPtr ROOT_PREFIX = "coworkerd-do-root-v1-"_kj;

bool isDigest(kj::StringPtr value) {
  return value.size() == 64 && std::all_of(value.begin(), value.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}

}  // namespace

DurableTestStorage::DurableTestStorage()
    : directory(kj::newInMemoryDirectory(kj::systemPreciseCalendarClock())),
      vfs(*directory) {}

kj::Own<SqliteDatabase> DurableTestStorage::open(
    kj::StringPtr uniqueKey, kj::StringPtr storageId, SqliteObserver& observer) const {
  KJ_REQUIRE(uniqueKey.startsWith(ROOT_PREFIX) && isDigest(uniqueKey.slice(ROOT_PREFIX.size())) &&
          isDigest(storageId),
      "invalid durable test storage identity");
  const auto mode = kj::WriteMode::CREATE | kj::WriteMode::MODIFY;
  directory->openSubdir(kj::Path({kj::str(uniqueKey)}), mode);
  return kj::heap<SqliteDatabase>(vfs,
      kj::Path({kj::str(uniqueKey), kj::str(storageId, ".sqlite")}), mode, kj::maxValue,
      kj::maxValue, observer);
}

}  // namespace workerd::server
