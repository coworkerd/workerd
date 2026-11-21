// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/util/sqlite.h>

namespace workerd::server {

// A child-generation-local SQLite volume. It never receives a host path, file descriptor,
// VFS name, or replication service. Keep the volume alive across actor retirement and retain
// only the SQLite connection on the actor's home.
class DurableTestStorage {
 public:
  DurableTestStorage();

  kj::Own<SqliteDatabase> open(kj::StringPtr uniqueKey,
      kj::StringPtr storageId,
      SqliteObserver& observer = SqliteObserver::DEFAULT) const;

 private:
  kj::Own<const kj::Directory> directory;
  SqliteDatabase::Vfs vfs;
};

}  // namespace workerd::server
