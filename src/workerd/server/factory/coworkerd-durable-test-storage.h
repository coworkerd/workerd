// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include "coworkerd-wal-shipper.h"

#include <workerd/server/facet-tree-index.h>
#include <workerd/util/sqlite.h>

namespace workerd::server {

// The disposable role owns a temporary volume. The persistent role uses the
// node-created volume and attaches its home-local WAL channel to each connection.
class DurableStorage {
 public:
  DurableStorage();
  explicit DurableStorage(kj::Own<const kj::Directory> directory);

  kj::Own<SqliteDatabase> open(kj::StringPtr uniqueKey,
      kj::StringPtr storageId,
      SqliteObserver& observer = SqliteObserver::DEFAULT,
      uint facet = 0,
      uint64_t sqlWalFrameLimitBytes = 0) const;

  kj::Own<DurableLogShipper> ship(kj::StringPtr uniqueKey,
      DurableObjectName name, SqliteDatabase& database, kj::Timer& timer,
      DurableWalChannel& channel, DurableWalPosition position, uint64_t localOpenStartedNs) const;
  struct WalImage {
    const kj::Directory& directory;
    kj::Path path;
  };
  WalImage walImage(kj::StringPtr uniqueKey, kj::StringPtr storageId, uint facet) const;

  kj::Own<FacetTreeIndex> openFacetIndex(kj::StringPtr uniqueKey, kj::StringPtr storageId) const;
  void deleteFacet(kj::StringPtr uniqueKey, kj::StringPtr storageId, FacetTreeIndex& index, uint facet,
      kj::Maybe<kj::Function<void(uint)>&> report = kj::none) const;
  void cloneFacet(kj::StringPtr uniqueKey,
      kj::StringPtr storageId,
      FacetTreeIndex& index,
      uint source,
      uint destination) const;

 private:
  void ensureNamespace(kj::StringPtr uniqueKey, kj::StringPtr storageId) const;
  kj::Own<const kj::Directory> directory;
  SqliteDatabase::Vfs vfs;
};

}  // namespace workerd::server
