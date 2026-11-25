// Copyright 2026 The Coworkerd Authors
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-wal-shipper.h"
#include "coworkerd-wal-wire.h"

#include <sqlite3.h>

#include <kj/test.h>
#include <kj/vector.h>

namespace workerd::server {
namespace {

constexpr size_t WAL_HEADER_SIZE = 32;
constexpr size_t FRAME_HEADER_SIZE = 24;
constexpr size_t PAGE_SIZE = 4096;
constexpr size_t FRAME_SIZE = FRAME_HEADER_SIZE + PAGE_SIZE;
constexpr auto OPEN_ID = 1;
constexpr auto OBJECT_NAME = "namespace/object/epoch"_kj;
constexpr auto STORAGE_ID = "storage-id"_kj;
constexpr auto CREATE_OR_MODIFY = kj::WriteMode::CREATE | kj::WriteMode::MODIFY;

uint32_t readBigEndian(kj::ArrayPtr<const kj::byte> bytes) {
  return uint32_t(bytes[0]) << 24 | uint32_t(bytes[1]) << 16 | uint32_t(bytes[2]) << 8 | bytes[3];
}

void writeRootIdentity(wal::Writer& message) {
  message.str(kj::str(OBJECT_NAME, "/0"));
  message.str(OBJECT_NAME);
  message.str(STORAGE_ID);
  message.u32(0);  // Facet ID.
  message.u32(0);  // Parent facet ID.
  message.str(""_kj);
}

class RecordingChannel final: public DurableWalChannel {
 public:
  kj::Array<kj::byte> open;
  kj::Vector<kj::Array<kj::byte>> frames;

  bool enabled() const override {
    return true;
  }
  uint64_t nextRequest() override {
    return ++request;
  }
  uint64_t registerShipper(DurableLogShipper&) override {
    return OPEN_ID;
  }
  void unregisterShipper(DurableLogShipper&) override {}
  void send(uint64_t, kj::Array<kj::byte> message) override {
    if (message[4] == wal::MSG_OPEN) open = kj::mv(message);
  }
  kj::Promise<void> sendFrames(
      DurableLogShipper&, kj::Array<kj::byte> message, uint64_t, kj::Timer&) override {
    frames.add(kj::mv(message));
    return kj::READY_NOW;
  }

 private:
  uint64_t request = 0;
};

void expectOpen(const RecordingChannel& channel,
    kj::ArrayPtr<const kj::byte> header,
    uint32_t committedFrames) {
  wal::Writer expectedOpen;
  expectedOpen.u8(wal::MSG_OPEN);
  writeRootIdentity(expectedOpen);
  expectedOpen.u32(readBigEndian(header.slice(16)));
  expectedOpen.u32(readBigEndian(header.slice(20)));
  auto openPrefix = expectedOpen.finish(OPEN_ID);
  KJ_ASSERT(channel.open.size() >= openPrefix.size() + sizeof(uint32_t));
  KJ_EXPECT(channel.open.slice(4, openPrefix.size()) == openPrefix.slice(4));
  KJ_EXPECT(wal::getLe32(channel.open.begin() + openPrefix.size()) == committedFrames);
}

void expectFrames(kj::ArrayPtr<const kj::byte> message,
    kj::ArrayPtr<const kj::byte> header,
    uint32_t firstFrame,
    kj::ArrayPtr<const kj::byte> frames,
    uint64_t request,
    bool withHeader) {
  wal::Writer expectedFrames;
  expectedFrames.u8(wal::MSG_FRAMES);
  expectedFrames.u64(request);
  writeRootIdentity(expectedFrames);
  expectedFrames.u64(0);  // Generation.
  expectedFrames.u32(readBigEndian(header.slice(16)));
  expectedFrames.u32(readBigEndian(header.slice(20)));
  expectedFrames.u32(PAGE_SIZE);
  expectedFrames.u32(firstFrame);
  expectedFrames.u32(frames.size() / FRAME_SIZE);
  expectedFrames.u32(1);  // Commit count.
  auto framePrefix = expectedFrames.finish(OPEN_ID);
  const auto headerFlag = framePrefix.size() + 3 * sizeof(uint64_t);  // Skip timing fields.
  const auto framesStart = headerFlag + 1 + (withHeader ? WAL_HEADER_SIZE : 0);
  KJ_ASSERT(message.size() == framesStart + frames.size());
  KJ_EXPECT(message.slice(4, framePrefix.size()) == framePrefix.slice(4));
  KJ_EXPECT(message[headerFlag] == withHeader);
  if (withHeader) KJ_EXPECT(message.slice(headerFlag + 1, framesStart) == header);
  KJ_EXPECT(message.slice(framesStart) == frames);
}

void expectCommittedPrefix(const kj::ReadableFile& database,
    kj::ArrayPtr<const kj::byte> image,
    uint32_t committedFrames,
    kj::ArrayPtr<const kj::byte> fullWal,
    kj::AsyncIoContext& io) {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  const auto dbPath = kj::Path({"test.db"});
  const auto walPath = kj::Path({"test.db-wal"});
  dir->openFile(dbPath, kj::WriteMode::CREATE)->writeAll(database.readAllBytes());
  auto walFile = dir->openFile(walPath, CREATE_OR_MODIFY);
  walFile->writeAll(image);

  const auto header = image.first(WAL_HEADER_SIZE);
  const auto salt1 = readBigEndian(header.slice(16));
  const auto salt2 = readBigEndian(header.slice(20));
  const auto committedEnd = WAL_HEADER_SIZE + committedFrames * FRAME_SIZE;
  const auto commit = image.slice(committedEnd - FRAME_SIZE, committedEnd);
  KJ_ASSERT(readBigEndian(commit.slice(4)) != 0);

  auto cold = readDurableWalPosition(*dir, walPath);
  KJ_EXPECT(cold.frames == committedFrames);
  KJ_EXPECT(cold.pageSize == PAGE_SIZE);
  KJ_EXPECT(kj::arrayPtr(cold.header.data(), cold.header.size()).asConst() == header);
  KJ_EXPECT(cold.salt1 == salt1 && cold.salt2 == salt2);
  KJ_EXPECT(cold.checksum1 == readBigEndian(commit.slice(16)));
  KJ_EXPECT(cold.checksum2 == readBigEndian(commit.slice(20)));

  SqliteDatabase::Vfs vfs(*dir);
  SqliteDatabase db(vfs, dbPath.clone(), kj::WriteMode::MODIFY);
  RecordingChannel channel;
  // The constructor scans the existing WAL when no prepared position is supplied.
  DurableLogShipper shipper(*dir, walPath.clone(), dbPath.clone(),
      {.objectName = kj::str(OBJECT_NAME), .storageId = kj::str(STORAGE_ID)}, db,
      io.provider->getTimer(), channel);
  KJ_ASSERT(walFile->readAllBytes().asPtr() == image);

  expectOpen(channel, header, committedFrames);

  // Completing the tail must resume from the checksum of the last complete commit.
  shipper.onCommit().wait(io.waitScope);
  KJ_ASSERT(channel.frames.size() == 0);
  walFile->writeAll(fullWal);
  shipper.onCommit().wait(io.waitScope);
  KJ_ASSERT(channel.frames.size() == 1);
  expectFrames(
      channel.frames[0], header, committedFrames + 1, fullWal.slice(committedEnd), 1, true);
}

void configureWal(SqliteDatabase& db, uint32_t& lastCommit) {
  db.run("PRAGMA page_size = 4096");
  db.run("CREATE TABLE entries (value BLOB)");
  KJ_REQUIRE(db.run("PRAGMA journal_mode = WAL").getText(0) == "wal"_kj);
  db.run("PRAGMA synchronous = OFF");
  KJ_REQUIRE(sqlite3_wal_autocheckpoint(db, 0) == SQLITE_OK);

  // SQLite reports the last committed frame independently of either scanner.
  sqlite3_wal_hook(db, [](void* context, sqlite3*, const char*, int frames) {
    *static_cast<uint32_t*>(context) = frames;
    return SQLITE_OK;
  }, &lastCommit);
}

KJ_TEST("a SQL WAL transaction at the record limit commits and an extra frame rolls back") {
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  const auto walPath = kj::Path({"test.db-wal"});
  // Enough bytes for four complete frames and the next frame's header.
  SqliteDatabase::WalTransactionLimit limit(4 * FRAME_SIZE + FRAME_HEADER_SIZE);
  SqliteDatabase::Vfs vfs(*dir, {.walTransactionLimit = &limit});
  SqliteDatabase db(vfs, kj::Path({"test.db"}), CREATE_OR_MODIFY);
  db.run("PRAGMA page_size = 4096; CREATE TABLE ledger (id INTEGER PRIMARY KEY, value BLOB)");
  db.run("CREATE TABLE fixture_ids (id INTEGER PRIMARY KEY); BEGIN");
  for (int id = 10; id <= 100; ++id) db.run("INSERT INTO fixture_ids VALUES (?)", id);
  db.run("COMMIT");
  db.run("PRAGMA journal_mode = WAL; PRAGMA synchronous = OFF; PRAGMA wal_autocheckpoint = 0");
  limit.setPageSize(PAGE_SIZE);
  db.run("INSERT INTO ledger VALUES (1, x'01')");
  const auto before = readDurableWalPosition(*dir, walPath).frames;

  limit.begin();
  db.run("SAVEPOINT ledger; INSERT INTO ledger VALUES (2, zeroblob(8184)); RELEASE ledger");
  KJ_EXPECT(!limit.wasRefused());
  limit.end(db);
  KJ_EXPECT(readDurableWalPosition(*dir, walPath).frames == before + 4);
  const auto committed = dir->openFile(walPath)->readAllBytes();

  limit.begin();
  db.run("SAVEPOINT ledger; INSERT INTO ledger VALUES (3, zeroblob(12276))");
  KJ_EXPECT_THROW_MESSAGE("database or disk is full", db.run("RELEASE ledger"));
  KJ_EXPECT(limit.wasRefused() && !db.observedCriticalError());
  KJ_EXPECT(dir->openFile(walPath)->stat().size <= committed.size() + 4 * FRAME_SIZE);
  db.rollbackWalTransaction();
  limit.end(db);
  KJ_EXPECT(dir->openFile(walPath)->readAllBytes() == committed.asPtr());

  db.run("PRAGMA cache_size = 5; PRAGMA cache_spill = 5");
  limit.begin();
  db.run("SAVEPOINT ledger");
  // This statement spills during sqlite3_step(), without attempting a commit.
  KJ_EXPECT_THROW_MESSAGE("database or disk is full",
      db.run("INSERT INTO ledger SELECT id, zeroblob(3000) FROM fixture_ids"));
  KJ_EXPECT(limit.wasRefused() && !db.observedCriticalError());
  db.rollbackWalTransaction();
  limit.end(db);
  KJ_EXPECT(dir->openFile(walPath)->readAllBytes() == committed.asPtr());
  KJ_EXPECT(db.run("SELECT count(*) FROM ledger WHERE id IN (1, 2)").getInt(0) == 2);
  KJ_EXPECT(db.run("SELECT count(*) FROM ledger WHERE id >= 3").getInt(0) == 0);

  limit.begin();
  db.run("SAVEPOINT ledger; INSERT INTO ledger VALUES (3, x'03'); RELEASE ledger");
  KJ_EXPECT(!limit.wasRefused());
  limit.end(db);
  KJ_EXPECT(db.run("SELECT count(*) FROM ledger").getInt(0) == 3);
}

KJ_TEST("the SQL WAL shipper splits a group of commits that exceeds the record limit") {
  auto io = kj::setupAsyncIo();
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  const auto dbPath = kj::Path({"test.db"});
  const auto walPath = kj::Path({"test.db-wal"});
  SqliteDatabase::WalTransactionLimit limit(FRAME_SIZE);
  SqliteDatabase::Vfs vfs(*dir, {.walTransactionLimit = &limit});
  SqliteDatabase db(vfs, dbPath.clone(), CREATE_OR_MODIFY);
  db.run("PRAGMA page_size = 4096; CREATE TABLE entries (value INTEGER); INSERT INTO entries VALUES (0)");
  db.run("PRAGMA journal_mode = WAL");
  RecordingChannel channel;
  DurableLogShipper shipper(*dir, walPath.clone(), dbPath.clone(),
      {.objectName = kj::str(OBJECT_NAME), .storageId = kj::str(STORAGE_ID)}, db,
      io.provider->getTimer(), channel);

  // Leave both commits for a single shipper callback. Each changes the same
  // leaf page and fits on its own.
  for (int value: {1, 2}) {
    limit.begin();
    db.run("SAVEPOINT ledger");
    db.run("UPDATE entries SET value = ?", value);
    db.run("RELEASE ledger");
    KJ_EXPECT(!limit.wasRefused());
    limit.end(db);
  }
  const auto wal = dir->openFile(walPath)->readAllBytes();
  KJ_ASSERT(wal.size() == WAL_HEADER_SIZE + 2 * FRAME_SIZE);
  const auto header = wal.first(WAL_HEADER_SIZE);
  const auto frames = wal.slice(WAL_HEADER_SIZE);
  shipper.onCommit().wait(io.waitScope);
  KJ_ASSERT(channel.frames.size() == 2);
  expectFrames(channel.frames[0], header, 1, frames.first(FRAME_SIZE), 1, true);
  expectFrames(channel.frames[1], header, 2, frames.slice(FRAME_SIZE), 2, false);
}

KJ_TEST("cold WAL scan and shipper agree on the last complete SQLite commit") {
  auto io = kj::setupAsyncIo();
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  const auto dbPath = kj::Path({"test.db"});
  const auto walPath = kj::Path({"test.db-wal"});
  SqliteDatabase::Vfs vfs(*dir);
  SqliteDatabase db(vfs, dbPath.clone(), CREATE_OR_MODIFY);
  uint32_t lastCommit = 0;
  configureWal(db, lastCommit);
  KJ_DEFER(sqlite3_wal_hook(db, nullptr, nullptr));
  db.run("INSERT INTO entries VALUES (x'00')");
  const uint32_t smallCommit = lastCommit;
  KJ_ASSERT(smallCommit > 0 && smallCommit * FRAME_SIZE < (1u << 20));
  db.run("INSERT INTO entries VALUES (zeroblob(1536000))");
  const uint32_t largeCommit = lastCommit;
  const auto committedEnd = WAL_HEADER_SIZE + largeCommit * FRAME_SIZE;
  const auto committed = dir->openFile(walPath)->readAllBytes();
  KJ_ASSERT(committed.size() == committedEnd && committedEnd > WAL_HEADER_SIZE + (1u << 20));

  db.run("BEGIN");
  db.run("INSERT INTO entries VALUES (zeroblob(65536))");
  KJ_REQUIRE(sqlite3_db_cacheflush(db) == SQLITE_OK);
  const auto spilled = dir->openFile(walPath)->readAllBytes();
  KJ_ASSERT(lastCommit == largeCommit);
  KJ_ASSERT(spilled.size() >= committedEnd + FRAME_SIZE);
  KJ_ASSERT((spilled.size() - committedEnd) % FRAME_SIZE == 0);
  for (size_t offset = committedEnd; offset < spilled.size(); offset += FRAME_SIZE) {
    KJ_ASSERT(readBigEndian(spilled.slice(offset + 4)) == 0);
  }

  db.run("COMMIT");
  const auto complete = dir->openFile(walPath)->readAllBytes();
  KJ_ASSERT(lastCommit > largeCommit);
  KJ_ASSERT(complete.size() == WAL_HEADER_SIZE + lastCommit * FRAME_SIZE);
  KJ_ASSERT(complete.first(committedEnd) == committed.asPtr());
  KJ_ASSERT(spilled.first(committedEnd) == committed.asPtr());
  const auto lastFrame = complete.size() - FRAME_SIZE;
  KJ_ASSERT(readBigEndian(complete.slice(lastFrame + 4)) != 0);
  const auto interrupted = complete.first(lastFrame + FRAME_HEADER_SIZE + PAGE_SIZE / 2);

  auto database = dir->openFile(dbPath);
  {
    KJ_CONTEXT("complete multi-buffer commit");
    expectCommittedPrefix(*database, committed, largeCommit, complete, io);
  }
  {
    KJ_CONTEXT("complete uncommitted frames");
    expectCommittedPrefix(*database, spilled, largeCommit, complete, io);
  }
  {
    KJ_CONTEXT("final commit frame interrupted halfway through its page");
    expectCommittedPrefix(*database, interrupted, largeCommit, complete, io);
  }
}

KJ_TEST("opening a multi-record WAL reports SQLite's last commit") {
  constexpr size_t DEFAULT_RECORD_BYTES = 8u << 20;
  auto io = kj::setupAsyncIo();
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  const auto dbPath = kj::Path({"test.db"});
  const auto walPath = kj::Path({"test.db-wal"});
  SqliteDatabase::Vfs vfs(*dir);
  SqliteDatabase db(vfs, dbPath.clone(), CREATE_OR_MODIFY);
  uint32_t lastCommit = 0;
  configureWal(db, lastCommit);
  KJ_DEFER(sqlite3_wal_hook(db, nullptr, nullptr));

  db.run("INSERT INTO entries VALUES (x'00')");
  const auto firstCommit = lastCommit;
  db.run("INSERT INTO entries VALUES (zeroblob(5000000)), (zeroblob(5000000))");
  const auto largeCommit = lastCommit;
  db.run("INSERT INTO entries VALUES (x'01')");
  KJ_ASSERT(lastCommit > largeCommit && largeCommit > firstCommit);
  KJ_ASSERT((largeCommit - firstCommit) * FRAME_SIZE > DEFAULT_RECORD_BYTES);
  const auto fullWal = dir->openFile(walPath)->readAllBytes();
  KJ_ASSERT(fullWal.size() == WAL_HEADER_SIZE + lastCommit * FRAME_SIZE);
  const auto header = fullWal.first(WAL_HEADER_SIZE);

  auto cold = readDurableWalPosition(*dir, walPath);
  KJ_EXPECT(cold.frames == lastCommit);
  for (bool usePrepared: {false, true}) {
    KJ_CONTEXT(usePrepared);
    kj::Maybe<DurableWalPosition> prepared;
    if (usePrepared) prepared = cold;
    RecordingChannel channel;
    DurableLogShipper shipper(*dir, walPath.clone(), dbPath.clone(),
        {.objectName = kj::str(OBJECT_NAME), .storageId = kj::str(STORAGE_ID)}, db,
        io.provider->getTimer(), channel, prepared);
    expectOpen(channel, header, lastCommit);
    shipper.onCommit().wait(io.waitScope);
    KJ_EXPECT(channel.frames.size() == 0);
  }

  // Open on the first commit, then present the same WAL with both later commits available.
  auto replica = kj::newInMemoryDirectory(kj::nullClock());
  replica->openFile(dbPath, kj::WriteMode::CREATE)->writeAll(dir->openFile(dbPath)->readAllBytes());
  auto replicaWal = replica->openFile(walPath, CREATE_OR_MODIFY);
  const auto firstEnd = WAL_HEADER_SIZE + firstCommit * FRAME_SIZE;
  const auto largeEnd = WAL_HEADER_SIZE + largeCommit * FRAME_SIZE;
  replicaWal->writeAll(fullWal.first(firstEnd));
  SqliteDatabase::Vfs replicaVfs(*replica);
  SqliteDatabase replicaDb(replicaVfs, dbPath.clone(), kj::WriteMode::MODIFY);
  RecordingChannel channel;
  DurableLogShipper shipper(*replica, walPath.clone(), dbPath.clone(),
      {.objectName = kj::str(OBJECT_NAME), .storageId = kj::str(STORAGE_ID)}, replicaDb,
      io.provider->getTimer(), channel);
  replicaWal->writeAll(fullWal);
  shipper.onCommit().wait(io.waitScope);
  KJ_ASSERT(channel.frames.size() == 2);
  expectFrames(
      channel.frames[0], header, firstCommit + 1, fullWal.slice(firstEnd, largeEnd), 1, true);
  expectFrames(channel.frames[1], header, largeCommit + 1, fullWal.slice(largeEnd), 2, false);
}

}  // namespace
}  // namespace workerd::server
