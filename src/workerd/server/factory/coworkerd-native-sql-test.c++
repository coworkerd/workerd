// Copyright (c) 2026 Coworkerd contributors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-native-sql.h"
#include "coworkerd-native-sql-spill.h"

#include <sqlite3.h>
#include <sys/socket.h>
#include <unistd.h>

#include <kj/io.h>
#include <kj/test.h>

#include <cstdlib>
#include <thread>

namespace workerd::server {
namespace {

const bool allocatorInstalled KJ_UNUSED = [] {
  installSqliteCustomAllocator();
  return true;
}();

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

class SqlTestDirectory {
 public:
  ~SqlTestDirectory() noexcept(false) {
    directory = nullptr;
    filesystem->getRoot().remove(path);
  }

  const kj::Directory& get() const { return *directory; }

 private:
  kj::Own<kj::Filesystem> filesystem = kj::newDiskFilesystem();
  kj::Path path = createPath();
  kj::Own<const kj::Directory> directory = filesystem->getRoot().openSubdir(path, kj::WriteMode::MODIFY);

  kj::Path createPath() {
    const auto* temporaryDirectory = getenv("TEST_TMPDIR");
    auto scratch = kj::str(temporaryDirectory != nullptr ? temporaryDirectory : "/tmp",
        "/workerd-native-sql.XXXXXX");
    KJ_REQUIRE(mkdtemp(scratch.begin()) != nullptr, "could not make a SQLite spill test directory");
    return filesystem->getCurrentPath().evalNative(scratch);
  }
};

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

KJ_TEST("native SQL platform errors retain the code consumed by the binding") {
  auto body = nativeSqlFailure("STORAGE_UNAVAILABLE", "the write was unconfirmed");
  KJ_EXPECT(body ==
      "{\"code\":\"STORAGE_UNAVAILABLE\",\"reason\":\"the write was unconfirmed\","
      "\"error\":\"STORAGE_UNAVAILABLE: the write was unconfirmed\"}");
}

KJ_TEST("native SQL handles a statement that rolls back its whole transaction") {
  Fixture f;
  f.db.run("CREATE TABLE prefix(id INTEGER PRIMARY KEY);"
           "CREATE TABLE ordinary(id INTEGER PRIMARY KEY);"
           "INSERT INTO ordinary VALUES(1);"
           "CREATE TABLE on_conflict(id INTEGER PRIMARY KEY ON CONFLICT ROLLBACK);"
           "INSERT INTO on_conflict VALUES(1);"
           "CREATE TABLE on_trigger(id INTEGER);"
           "CREATE TRIGGER reject_insert BEFORE INSERT ON on_trigger "
           "BEGIN SELECT RAISE(ROLLBACK, 'trigger rolled back'); END");
  for (auto sql: {"INSERT OR ROLLBACK INTO ordinary VALUES(1)"_kj,
           "INSERT INTO on_conflict VALUES(1)"_kj, "INSERT INTO on_trigger VALUES(1)"_kj}) {
    f.db.run("DELETE FROM prefix");
    auto batch = kj::str(
        "[{\"sql\":\"INSERT INTO prefix VALUES(1)\"},{\"sql\":\"", sql,
        "\"},{\"sql\":\"INSERT INTO prefix VALUES(2)\"}]");
    auto expected = sql.contains("on_trigger"_kj) ? "trigger rolled back" : "UNIQUE constraint failed";
    auto query = f.query(batch);
    KJ_EXPECT(query.body.starts_with("[{\"success\":false,"));
    KJ_EXPECT(query.body.find(expected) != std::string::npos);
    KJ_EXPECT(f.db.run("SELECT count(*) FROM prefix").getInt(0) == 0);
    auto execute = f.execute(batch);
    KJ_EXPECT(execute.body.starts_with("[{\"success\":true,"));
    KJ_EXPECT(execute.body.find(expected) != std::string::npos);
    KJ_EXPECT(f.db.run("SELECT sum(id) FROM prefix").getInt(0) == 1);
    KJ_EXPECT(f.query(R"SQL({"sql":"INSERT INTO prefix VALUES(3)"})SQL").body.starts_with(
        "{\"success\":true,"));
  }
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
  KJ_EXPECT(result.body.find("\"code\":\"SQL_LIMIT_EXCEEDED\"") != std::string::npos);
  KJ_EXPECT(result.body.find("\"error\":\"SQL_LIMIT_EXCEEDED: ") != std::string::npos);
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

KJ_TEST("native SQL rejects a commit if its deadline has elapsed") {
  Fixture f;
  f.db.run("CREATE TABLE ledger(id INTEGER PRIMARY KEY)");
  auto end = Clock::now() + 100ms;
  sqlite3_trace_v2(f.db, SQLITE_TRACE_STMT,
      [](unsigned, void* context, void* statement, void*) {
    auto sql = sqlite3_sql(static_cast<sqlite3_stmt*>(statement));
    if (sql != nullptr && kj::StringPtr(sql) == "COMMIT") {
      std::this_thread::sleep_until(*static_cast<const Clock::time_point*>(context) + 1ms);
    }
    return 0;
  }, &end);
  auto result = runNativeSql(f.db, R"SQL({"sql":"INSERT INTO ledger VALUES(1)"})SQL",
      true, NativeSqlResults::OBJECTS, end);
  sqlite3_trace_v2(f.db, 0, nullptr, nullptr);
  KJ_EXPECT(result.status == 503);
  KJ_EXPECT(result.body.find("SQL_LIMIT_EXCEEDED") != std::string::npos);
  KJ_EXPECT(f.count() == 0);
}

KJ_TEST("native SQL keeps the committed execute prefix on deadline") {
  Fixture f;
  f.db.run("CREATE TABLE ledger(id INTEGER PRIMARY KEY)");
  auto result = runNativeSql(f.db,
      R"SQL([{"sql":"INSERT INTO ledger VALUES(-1)"},{"sql":"WITH RECURSIVE r(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM r WHERE n<1000000000) INSERT INTO ledger SELECT n FROM r"}])SQL",
      false, NativeSqlResults::OBJECTS, Clock::now() + 20ms);
  KJ_EXPECT(result.status == 503);
  KJ_EXPECT(result.body.find("SQL_LIMIT_EXCEEDED") != std::string::npos);
  KJ_EXPECT(f.count() == 1);
  KJ_EXPECT(f.db.run("SELECT id FROM ledger").getInt(0) == -1);
}

KJ_TEST("bounded native WAL refuses a large commit without publishing its rows") {
  constexpr uint64_t walLimit = 64 * 1024;
  auto directory = kj::newInMemoryDirectory(kj::systemPreciseCalendarClock());
  SqliteDatabase::Vfs vfs(*directory, {.maxWalBytes = walLimit});
  SqliteDatabase db(vfs, kj::Path({"sql"}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
  KJ_REQUIRE(db.run("PRAGMA journal_mode=WAL").getText(0) == "wal");
  db.run("PRAGMA temp_store=MEMORY; PRAGMA cache_spill=OFF; PRAGMA wal_autocheckpoint=0");
  db.run("CREATE TABLE blobs(value BLOB)");
  db.run("PRAGMA wal_checkpoint(TRUNCATE)");

  auto wal = directory->openFile(kj::Path({"sql-wal"}));
  const auto initial = wal->stat().size;
  db.run("BEGIN");
  for (unsigned row = 0; row < 128; ++row) {
    db.run("INSERT INTO blobs VALUES(randomblob(4096))");
  }
  // Sample while the pages are still uncommitted, before the attempted flush.
  KJ_EXPECT(wal->stat().size == initial);
  KJ_EXPECT_THROW_MESSAGE("database or disk is full", db.run("COMMIT"));
  KJ_EXPECT(wal->stat().size <= walLimit);
  if (sqlite3_get_autocommit(db) == 0) db.run("ROLLBACK");
  else db.acknowledgeAutomaticRollback();
  KJ_EXPECT(db.run("SELECT count(*) FROM blobs").getInt(0) == 0);
}

KJ_TEST("native SQL budgets new WAL frames on disk and removes abandoned bytes") {
  SqlTestDirectory storage;
  const auto& directory = storage.get();
  NativeSqlSpillPool pool(4ull << 20);
  NativeSqlSpill spill(pool, 2ull << 20, directory, kj::Path({"sql-wal"}));
  SqliteDatabase::Vfs vfs(directory, {.spillTracker = &spill});
  SqliteDatabase db(vfs, kj::Path({"sql"}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
  KJ_REQUIRE(db.run("PRAGMA journal_mode=WAL").getText(0) == "wal");
  db.run("PRAGMA cache_size=-512; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=OFF");
  db.run("CREATE TABLE blobs(value BLOB)");
  auto wal = directory.openFile(kj::Path({"sql-wal"}));
  auto query = [&](kj::StringPtr body) {
    return runNativeSql(db, body, true, NativeSqlResults::OBJECTS, Clock::now() + 10s, -1, &spill);
  };
  auto insert = R"SQL({"sql":"INSERT INTO blobs VALUES(zeroblob(1048576))"})SQL"_kj;
  for (unsigned i = 0; i < 65; ++i) KJ_REQUIRE(query(insert).body.starts_with("{\"success\":true"));
  const auto initial = wal->stat().size;
  KJ_EXPECT(initial > (64ull << 20));
  auto refused = query(R"SQL({"sql":"INSERT INTO blobs VALUES(zeroblob(4194304))"})SQL");
  KJ_EXPECT(refused.status == 507, refused.body.c_str());
  KJ_EXPECT(refused.body.find("SQL_SPILL_LIMIT_EXCEEDED") != std::string::npos);
  KJ_EXPECT(wal->stat().size == initial);
  KJ_EXPECT(pool.size() == 0);
  KJ_EXPECT(db.run("SELECT count(*) FROM blobs").getInt(0) == 65);
  KJ_EXPECT(query(insert).body.starts_with("{\"success\":true"));
  KJ_EXPECT(db.run("SELECT count(*) FROM blobs").getInt(0) == 66);
}

KJ_TEST("native SQL refuses a commit above its WAL record capacity and keeps the connection") {
  SqlTestDirectory storage;
  const auto& directory = storage.get();
  NativeSqlSpillPool pool(4ull << 20);
  NativeSqlSpill spill(pool, 2ull << 20, directory, kj::Path({"sql-wal"}));
  SqliteDatabase::Vfs vfs(directory, {.spillTracker = &spill});
  SqliteDatabase db(vfs, kj::Path({"sql"}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY);
  db.run("PRAGMA page_size=4096; CREATE TABLE ledger(payload BLOB); PRAGMA journal_mode=WAL");
  db.run("PRAGMA cache_spill=OFF; PRAGMA wal_autocheckpoint=0; PRAGMA synchronous=OFF");
  spill.limitTransactionFrames(16 * (4096 + 24), 4096);
  auto query = [&](kj::StringPtr body) {
    return runNativeSql(db, body, true, NativeSqlResults::OBJECTS, Clock::now() + 2s, -1, &spill);
  };
  constexpr auto insert = R"SQL({"sql":"INSERT INTO ledger VALUES(zeroblob(50000))"})SQL"_kj;
  KJ_REQUIRE(query(insert).body.starts_with("{\"success\":true"));
  const auto wal = directory.openFile(kj::Path({"sql-wal"}));
  const auto before = wal->stat().size;
  const auto reply = query(R"SQL([
    {"sql":"INSERT INTO ledger VALUES(zeroblob(50000))"},
    {"sql":"INSERT INTO ledger VALUES(zeroblob(50000))"}
  ])SQL");
  KJ_EXPECT(reply.status == 422, reply.body.c_str());
  KJ_EXPECT(reply.body.find("SQL_TRANSACTION_TOO_LARGE") != std::string::npos);
  KJ_EXPECT(wal->stat().size == before);
  KJ_EXPECT(pool.size() == 0);
  KJ_EXPECT(!db.observedCriticalError());
  KJ_EXPECT(db.run("SELECT count(*) FROM ledger").getInt(0) == 1);
  KJ_EXPECT(query(insert).body.starts_with("{\"success\":true"));
}

class RefusingBudget final: public SqliteMemoryBudget {
 public:
  Allocation acquire(size_t bytes, bool rollback) noexcept override {
    if (rollback) ++rollbackAllocations;
    if (!rollback && bytes >= refuseAt) {
      if (sqliteBudget != nullptr) sqliteBudget->refuseAt = 1;
      return Allocation::BUSY;
    }
    used += bytes;
    return Allocation::ACCEPTED;
  }

  void release(size_t bytes) noexcept override {
    KJ_ASSERT(bytes <= used);
    used -= bytes;
  }

  size_t refuseAt = SIZE_MAX;
  size_t used = 0;
  size_t rollbackAllocations = 0;
  RefusingBudget* sqliteBudget = nullptr;
};

KJ_TEST("native SQL treats process memory pressure as retryable and reserves memory for rollback") {
  auto directory = kj::newInMemoryDirectory(kj::nullClock());
  RefusingBudget sqlite;
  SqliteDatabase::Vfs vfs(*directory, {.memoryBudget = &sqlite});
  SqliteDatabase db(vfs, kj::Path({"sql"}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY, 32ull << 20);
  {
    auto memory = db.enterMemoryScope();
    KJ_REQUIRE(sqlite3_db_config(db, SQLITE_DBCONFIG_LOOKASIDE, nullptr, 0, 0) == SQLITE_OK);
  }
  db.run("CREATE TABLE ledger(value BLOB)");
  auto query = [&](kj::StringPtr body, SqliteMemoryBudget* request = nullptr) {
    return runNativeSql(db, body, true, NativeSqlResults::OBJECTS, Clock::now() + 2s, -1, nullptr, request);
  };
  auto expectRefusal = [&](const NativeSqlReply& reply) {
    KJ_EXPECT(reply.status == 503, reply.body.c_str());
    KJ_EXPECT(reply.body.find("DO_UNAVAILABLE") != std::string::npos);
    KJ_EXPECT(reply.memoryPressure);
    KJ_EXPECT(!db.observedCriticalError());
    sqlite.refuseAt = SIZE_MAX;
    KJ_EXPECT(db.run("SELECT count(*) FROM ledger").getInt(0) == 0);
    KJ_EXPECT(query(R"SQL({"sql":"SELECT 1"})SQL").body.starts_with("{\"success\":true"));
  };
  sqlite.refuseAt = 512ull << 10;
  expectRefusal(query(R"SQL([
    {"sql":"INSERT INTO ledger VALUES(1)"},
    {"sql":"INSERT INTO ledger VALUES(randomblob(1048576))"}
  ])SQL"));

  RefusingBudget request;
  request.refuseAt = 128ull << 10;
  request.sqliteBudget = &sqlite;
  const auto priorRollbacks = sqlite.rollbackAllocations;
  expectRefusal(query(R"SQL([
    {"sql":"INSERT INTO ledger VALUES(2)"},
    {"sql":"SELECT zeroblob(1048576)"}
  ])SQL", &request));
  KJ_EXPECT(sqlite.rollbackAllocations > priorRollbacks);
  KJ_EXPECT(sqlite.used == db.getSqliteMemoryBytes());
}

KJ_TEST("native SQL builds a disk-spilling index on a table larger than the SQLite heap cap") {
  SqlTestDirectory storage;
  const auto& directory = storage.get();
  NativeSqlSpillPool pool(256ull << 20);
  NativeSqlSpill spill(pool, 256ull << 20, directory, kj::Path({"sql-wal"}));
  SqliteDatabase::Vfs vfs(directory, {.spillTracker = &spill});
  constexpr auto heapCap = 32ull << 20;
  SqliteDatabase db(vfs, kj::Path({"sql"}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY, heapCap);
  KJ_REQUIRE(db.run("PRAGMA journal_mode=WAL").getText(0) == "wal");
  db.run("PRAGMA temp_store=FILE; PRAGMA cache_spill=ON; PRAGMA cache_size=-512; PRAGMA synchronous=OFF");
  db.run("CREATE TABLE blobs(payload BLOB)");
  auto loaded = runNativeSql(db, R"SQL({"sql":"WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<12288) INSERT INTO blobs SELECT randomblob(8192) FROM n"})SQL",
      true, NativeSqlResults::OBJECTS, Clock::now() + 30s, -1, &spill);
  KJ_REQUIRE(loaded.body.starts_with("{\"success\":true"), loaded.body.c_str());
  KJ_REQUIRE(db.run("SELECT sum(length(payload)) FROM blobs").getInt64(0) == 3 * heapCap);
  const auto idle = db.getSqliteMemoryBytes();
  sqlite3_int64 initial, peak;
  sqlite3_status64(SQLITE_STATUS_MEMORY_USED, &initial, &peak, 1);
  auto reply = runNativeSql(db, R"SQL({"sql":"CREATE INDEX payload_order ON blobs(payload)"})SQL",
      true, NativeSqlResults::OBJECTS, Clock::now() + 30s, -1, &spill);
  sqlite3_int64 end;
  sqlite3_status64(SQLITE_STATUS_MEMORY_USED, &end, &peak, 0);
  KJ_EXPECT(reply.body.starts_with("{\"success\":true"), reply.body.c_str());
  KJ_EXPECT(spill.temporaryHighWater() > 0);
  KJ_EXPECT(pool.size() == 0);
  KJ_EXPECT(idle > 0 && idle < heapCap);
  KJ_LOG(INFO, "native SQL index memory and spill", idle, initial, peak, end, spill.temporaryHighWater());
}

KJ_TEST("native SQL spill accounts for each database and the process together") {
  auto directory = kj::newInMemoryDirectory(kj::systemPreciseCalendarClock());
  NativeSqlSpillPool pool(1024);
  NativeSqlSpill first(pool, 768, *directory, kj::Path({"one-wal"}));
  NativeSqlSpill second(pool, 768, *directory, kj::Path({"two-wal"}));
  first.begin();
  second.begin();
  KJ_EXPECT(first.writeWal(0, 640));
  KJ_EXPECT(first.writeWal(32, 256));
  KJ_EXPECT(!first.resizeTemporary(0, 129));
  KJ_EXPECT(second.resizeTemporary(0, 384));
  KJ_EXPECT(!second.writeWal(0, 32));
  KJ_EXPECT(pool.size() == 1024);
  first.end(false);
  KJ_EXPECT(second.writeWal(0, 32));
  KJ_EXPECT(second.resizeTemporary(384, 0));
  second.end(true);
  KJ_EXPECT(pool.size() == 0);
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
