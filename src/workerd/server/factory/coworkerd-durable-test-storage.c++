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

kj::Path databasePath(
    kj::StringPtr uniqueKey, kj::StringPtr storageId, uint facet, kj::StringPtr suffix = ""_kj) {
  auto file = facet == 0 ? kj::str(storageId, ".sqlite", suffix)
                         : kj::str(storageId, '.', facet, ".sqlite", suffix);
  return kj::Path({kj::str(uniqueKey), kj::mv(file)});
}

}  // namespace

DurableTestStorage::DurableTestStorage()
    : DurableTestStorage(kj::newInMemoryDirectory(kj::systemPreciseCalendarClock())) {}

DurableTestStorage::DurableTestStorage(kj::Own<const kj::Directory> directory)
    : directory(kj::mv(directory)),
      vfs(*this->directory) {}

void DurableTestStorage::ensureNamespace(kj::StringPtr uniqueKey, kj::StringPtr storageId) const {
  KJ_REQUIRE(uniqueKey.startsWith(ROOT_PREFIX) && isDigest(uniqueKey.slice(ROOT_PREFIX.size())) &&
          isDigest(storageId),
      "invalid durable test storage identity");
  directory->openSubdir(
      kj::Path({kj::str(uniqueKey)}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
}

kj::Own<SqliteDatabase> DurableTestStorage::open(
    kj::StringPtr uniqueKey, kj::StringPtr storageId, SqliteObserver& observer, uint facet) const {
  ensureNamespace(uniqueKey, storageId);
  const auto mode = kj::WriteMode::CREATE | kj::WriteMode::MODIFY;
  return kj::heap<SqliteDatabase>(
      vfs, databasePath(uniqueKey, storageId, facet), mode, kj::maxValue, kj::maxValue, observer);
}

kj::Own<FacetTreeIndex> DurableTestStorage::openFacetIndex(
    kj::StringPtr uniqueKey, kj::StringPtr storageId) const {
  ensureNamespace(uniqueKey, storageId);
  auto path = kj::Path({kj::str(uniqueKey), kj::str(storageId, ".facets")});
  return kj::heap<FacetTreeIndex>(
      directory->openFile(path, kj::WriteMode::CREATE | kj::WriteMode::MODIFY));
}

void DurableTestStorage::deleteFacet(
    kj::StringPtr uniqueKey, kj::StringPtr storageId, FacetTreeIndex& index, uint facet) const {
  KJ_REQUIRE(facet != 0, "expected a facet ID");
  index.forEachChild(
      facet, [&](uint child, kj::StringPtr) { deleteFacet(uniqueKey, storageId, index, child); });
  for (auto suffix: {""_kj, "-wal"_kj, "-shm"_kj}) {
    directory->tryRemove(databasePath(uniqueKey, storageId, facet, suffix));
  }
}

void DurableTestStorage::cloneFacet(kj::StringPtr uniqueKey,
    kj::StringPtr storageId,
    FacetTreeIndex& index,
    uint source,
    uint destination) const {
  KJ_REQUIRE(
      source != 0 && destination != 0 && source != destination, "expected distinct facet IDs");
  // Collect first because adding destination entries can invalidate index iterators.
  struct Child {
    uint id;
    kj::String name;
  };
  kj::Vector<Child> children;
  index.forEachChild(
      source, [&](uint child, kj::StringPtr name) { children.add(Child{child, kj::str(name)}); });
  for (auto& child: children) {
    cloneFacet(uniqueKey, storageId, index, child.id, index.getId(destination, child.name));
  }
  for (auto suffix: {""_kj, "-wal"_kj}) {
    auto from = databasePath(uniqueKey, storageId, source, suffix);
    if (directory->exists(from)) {
      directory->transfer(databasePath(uniqueKey, storageId, destination, suffix),
          kj::WriteMode::CREATE, from, kj::TransferMode::COPY);
    }
  }
}

}  // namespace workerd::server
