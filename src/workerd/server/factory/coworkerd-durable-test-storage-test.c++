// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-durable-test-storage.h"

#include <kj/test.h>

namespace workerd::server {
namespace {

constexpr auto key =
    "coworkerd-do-root-v1-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"_kj;
constexpr auto id = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"_kj;
constexpr auto other = "f000000000000000000000000000000000000000000000000000000000000000"_kj;

KJ_TEST("disposable durable SQLite files reopen inside a generation") {
  DurableTestStorage storage;
  {
    auto database = storage.open(key, id);
    database->run("CREATE TABLE example (value INTEGER)");
    database->run("INSERT INTO example VALUES (42)");
    database->run("BEGIN");
    database->run("INSERT INTO example VALUES (43)");
  }
  auto reopened = storage.open(key, id);
  auto rows = reopened->run("SELECT value FROM example");
  KJ_EXPECT(rows.getInt(0) == 42);
  rows.nextRow();
  KJ_EXPECT(rows.isDone());
  auto unrelated = storage.open(key, other);
  KJ_EXPECT(unrelated->run("SELECT count(*) FROM sqlite_schema").getInt(0) == 0);
  DurableTestStorage replacementGeneration;
  auto afterRestart = replacementGeneration.open(key, id);
  KJ_EXPECT(afterRestart->run("SELECT count(*) FROM sqlite_schema").getInt(0) == 0);
}

KJ_TEST("disposable durable SQLite refuses paths and malformed identities") {
  DurableTestStorage storage;
  KJ_EXPECT_THROW_MESSAGE(
      "invalid durable test storage identity", storage.open("/var/lib/data", id));
  KJ_EXPECT_THROW_MESSAGE("invalid durable test storage identity", storage.open(key, "../other"));
  KJ_EXPECT_THROW_MESSAGE("invalid durable test storage identity", storage.open(key, "other"));
}

}  // namespace
}  // namespace workerd::server
