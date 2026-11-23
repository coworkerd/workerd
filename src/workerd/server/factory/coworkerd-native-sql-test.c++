// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-native-sql.h"

#include <sys/socket.h>

#include <kj/io.h>
#include <kj/test.h>

#include <thread>

namespace workerd::server {
namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

struct Fixture {
  kj::Own<kj::Directory> directory = kj::newInMemoryDirectory(kj::systemPreciseCalendarClock());
  SqliteDatabase::Vfs vfs{*directory};
  SqliteDatabase db{vfs, kj::Path({"sql"}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY};

  NativeSqlReply query(kj::StringPtr input,
      NativeSqlResults format = NativeSqlResults::OBJECTS,
      Clock::duration limit = 2s,
      int fd = -1) {
    return runNativeSql(db, input, true, format, Clock::now() + limit, fd);
  }

  NativeSqlReply execute(kj::StringPtr input) {
    return runNativeSql(db, input, false, NativeSqlResults::OBJECTS, Clock::now() + 2s);
  }

  size_t count() {
    return db.run("SELECT count(*) FROM ledger").getInt(0);
  }
};

KJ_TEST("native SQL handles result modes, duplicate names, and JSON input and output") {
  Fixture f;
  auto body =
      R"SQL({"sql":"SELECT ? AS a, ? AS a, ? AS z, ? AS '10', ? AS '2'","params":[null,[0,255],"\ud83d\udc1d\u0000\n",1e-7,9007199254740993]})SQL"_kj;
  auto result = f.query(body);
  KJ_EXPECT(result.status == 200);
  KJ_EXPECT(result.body.starts_with(
      "{\"success\":true,\"results\":[{\"2\":9007199254740992,\"10\":1e-7,\"a\":[0,255],\"z\":\"🐝\\u0000\\n\"}]"));
  auto columns = f.query(body, NativeSqlResults::COLUMNS);
  KJ_EXPECT(columns.body.starts_with(
      "{\"success\":true,\"results\":{\"columns\":[\"a\",\"a\",\"z\",\"10\",\"2\"],"
      "\"rows\":[[null,[0,255],\"🐝\\u0000\\n\",1e-7,9007199254740992]]}"));
  KJ_EXPECT(f.query("{\"sql\":\"SELECT\n1\"}").status == 400);
  KJ_EXPECT(f.query("{\"sql\":\"SELECT ?\",\"params\":[1.]}").status == 400);
  KJ_EXPECT(f.query(R"({"sql":"SELECT ?","params":[[0,256]]})").body.find("D1_TYPE_ERROR") !=
      std::string::npos);
  KJ_EXPECT(f.query(R"({"sql":"SELECT ?","params":[{"a":1}]})").body.find("D1_TYPE_ERROR") !=
      std::string::npos);
  KJ_EXPECT(f.query(R"({"sql":"SELECT ?","params":[]})")
                .body.find("Wrong number of parameter bindings") != std::string::npos);
  KJ_EXPECT(f.query(R"SQL({"sql":"SELECT typeof(?) AS kind","params":[[]]})SQL")
                .body.find("\"kind\":\"blob\"") != std::string::npos);
}

KJ_TEST("native SQL rolls back a failed query batch and keeps the execute prefix") {
  Fixture f;
  f.db.run("CREATE TABLE ledger(id INTEGER PRIMARY KEY)");
  auto batch =
      R"SQL([{"sql":"INSERT INTO ledger VALUES(1)"},{"sql":"INSERT INTO ledger VALUES(1)"},{"sql":"INSERT INTO ledger VALUES(2)"}])SQL"_kj;
  const auto rejected = f.query(batch);
  KJ_EXPECT(rejected.body.starts_with("[{\"success\":false,"));
  KJ_EXPECT(rejected.body.find("UNIQUE constraint failed") != std::string::npos);
  KJ_EXPECT(f.count() == 0);
  auto executed = f.execute(batch);
  KJ_EXPECT(executed.body.starts_with("[{\"success\":true,"));
  KJ_EXPECT(executed.body.find("\"changes\":1,\"last_row_id\":1,\"changed_db\":true") !=
      std::string::npos);
  KJ_EXPECT(executed.body.find("UNIQUE constraint failed") != std::string::npos);
  KJ_EXPECT(f.count() == 1);

  auto oversized =
      f.query(R"SQL({"sql":"INSERT INTO ledger VALUES(2) RETURNING zeroblob(4000000)"})SQL");
  KJ_EXPECT(oversized.body.find("D1_RESULT_TOO_LARGE") != std::string::npos);
  KJ_EXPECT(f.count() == 1);
}

KJ_TEST("native SQL counts the complete JSON response at the exact byte limit") {
  Fixture f;
  for (auto format: {NativeSqlResults::OBJECTS, NativeSqlResults::COLUMNS}) {
    const auto empty = f.query(R"SQL({"sql":"SELECT '' AS value"})SQL", format);
    KJ_ASSERT(empty.body.starts_with("{\"success\":true,"));
    const auto bytes = 7340032 - empty.body.size();
    auto statement = [](size_t bytes) {
      return kj::str(
          R"SQL({"sql":"SELECT substr(hex(zeroblob((? + 1) / 2)), 1, ?) AS value","params":[)SQL",
          bytes, ',', bytes, "]}");
    };
    auto exact = f.query(statement(bytes), format);
    KJ_EXPECT(exact.body.size() == 7340032);
    auto tooLarge = f.query(statement(bytes + 1), format);
    KJ_EXPECT(tooLarge.body.find("D1_RESULT_TOO_LARGE") != std::string::npos);
  }
}

KJ_TEST("native SQL retains the storage authorizer and the standalone ingest contract") {
  Fixture f;
  f.db.run("CREATE TABLE _cf_private(value)");
  auto forbidden = f.query(R"({"sql":"SELECT * FROM _CF_PRIVATE"})");
  KJ_EXPECT(forbidden.body.find("prohibited") != std::string::npos ||
      forbidden.body.find("SQLITE_AUTH") != std::string::npos);
  KJ_EXPECT(
      f.query(R"({"sql":"BEGIN"})").body.find("state.storage.transaction()") != std::string::npos);
  f.db.run("CREATE TABLE ledger(id INTEGER PRIMARY KEY)");
  auto imported = f.query(R"({"ingest":"INSERT INTO ledger VALUES (1); INSERT INTO ledger"})");
  KJ_EXPECT(imported.body.find("\"statementCount\":1") != std::string::npos);
  KJ_EXPECT(imported.body.find("INSERT INTO ledger") != std::string::npos);
  const auto rejected =
      f.query(R"({"ingest":"INSERT INTO ledger VALUES (2); INSERT INTO ledger VALUES (1);"})");
  KJ_EXPECT(rejected.body.find("UNIQUE constraint failed") != std::string::npos);
  KJ_EXPECT(f.query(R"([{"ingest":"INSERT INTO ledger VALUES (3);"}])")
                .body.find("standalone /query object") != std::string::npos);
  KJ_EXPECT(f.count() == 1);
}

KJ_TEST("native SQL timeout interrupts a write, rolls the batch back, and permits a clean retry") {
  Fixture f;
  f.db.run("CREATE TABLE ledger(id INTEGER PRIMARY KEY)");
  auto result = f.query(
      R"SQL([{"sql":"INSERT INTO ledger VALUES(-1)"},{"sql":"WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n<1000000000) INSERT INTO ledger SELECT n FROM r"}])SQL",
      NativeSqlResults::OBJECTS, 20ms);
  KJ_EXPECT(result.status == 503);
  KJ_EXPECT(result.body.find("SQL_LIMIT_EXCEEDED") != std::string::npos);
  KJ_EXPECT(f.count() == 0);
  // SQLite aborts an interrupted write transaction and marks the connection
  // unusable. The home closes it after settlement, then opens the same image.
  KJ_EXPECT(f.db.observedCriticalError());
  f.db.close();
  SqliteDatabase retry(f.vfs, kj::Path({"sql"}), kj::WriteMode::MODIFY);
  auto accepted = runNativeSql(retry, R"SQL({"sql":"INSERT INTO ledger VALUES(7)"})SQL", true,
      NativeSqlResults::OBJECTS, Clock::now() + 1s);
  KJ_EXPECT(accepted.body.starts_with("{\"success\":true,"));
  KJ_EXPECT(retry.run("SELECT count(*) FROM ledger").getInt(0) == 1);
}

KJ_TEST(
    "native SQL cancellation during a query prevents prior batch writes from becoming visible") {
  Fixture f;
  f.db.run("CREATE TABLE ledger(id INTEGER PRIMARY KEY)");
  int descriptors[2];
  KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors));
  kj::AutoCloseFd service(descriptors[0]);
  std::jthread stop([client = kj::AutoCloseFd(descriptors[1])]() mutable {
    std::this_thread::sleep_for(20ms);
    client = nullptr;
  });
  auto reply = f.query(
      R"SQL([{"sql":"INSERT INTO ledger VALUES(1)"},{"sql":"WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n<1000000000) SELECT sum(n) FROM r"}])SQL",
      NativeSqlResults::OBJECTS, 2s, service.get());
  KJ_EXPECT(reply.status == 0);
  KJ_EXPECT(f.count() == 0);
}

}  // namespace
}  // namespace workerd::server
