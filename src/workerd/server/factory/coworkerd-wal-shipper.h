// Coworkerd: a Durable Object's data does not stay on the node that owns it. Every database the localDisk actor
// storage factory opens gets a shipper: it reads the WAL frames of each committed transaction and hands them to the
// node's replicator over the descriptor inherited at startup, and the output gate holds the object's answers until
// the replicator confirms them (the node has them on K peers). The node merges the frames of all its objects into
// segments on the object store and rebuilds an object from them when another node takes its shard over, so the
// frames, not this machine's files, are the object's durable state: the owner keeps `synchronous=OFF`, checkpoints
// only frames it has handed on, and never lets SQLite checkpoint or truncate the WAL on its own.
//
// What the node sets for a durable process:
//   COWORKERD_DO_WAL_FD                the replicator's end of the socket, inherited (nothing is opened by path)
//   COWORKERD_DO_CONFIRM_TIMEOUT_MS    how long a commit waits for its confirmation (default 5000, below workerd's
//                                      own 30 s output gate timeout) before the write fails with STORAGE_UNAVAILABLE
//   COWORKERD_DO_CHECKPOINT_BYTES      the WAL size at which a checkpoint is attempted (default 4 MiB)
#pragma once

#include <workerd/util/sqlite.h>

#include <kj/async-io.h>
#include <kj/filesystem.h>
#include <kj/map.h>
#include <kj/timer.h>

#include <array>

namespace workerd::server {

// Registered by Bootstrap before serving; the inherited descriptor uses this provider.
void setDurableLogProvider(kj::LowLevelAsyncIoProvider& provider);

// Release the channel and its tasks while Bootstrap's provider and event loop still exist.
void shutdownDurableLog();

// Whether the actor storage factory ships what it opens: the descriptor is there.
bool durableLogEnabled();

// A facet's files were removed outside its own transactions (deleteFacet(), or the parent's deleteAll()).
void durableLogFacetDeleted(kj::StringPtr objectName, uint facetId);

// The alarm of one database, as that database itself holds it (_cf_METADATA): the node schedules the firing, so it
// is told at every change (ActorSqlite's scheduleRun hook) and whenever a database is opened -- which is how a node
// that took the shard over learns, from the object's own files, what is still due and what has already run.
void durableLogAlarm(kj::StringPtr objectName, uint facetId, kj::Maybe<kj::Date> scheduledTime);
kj::Maybe<kj::Date> readDurableAlarm(SqliteDatabase& db);

// How many objects this process holds resident, how many it has evicted for the resident limit and how many it has
// woken again, with the time the wakes took. Sent when the numbers change, at most once a second.
struct DurableResidency {
  uint64_t resident = 0, limit = 0, evicted = 0, woken = 0, wakes = 0;
  // Microseconds: waking an object whose class is still loaded takes well under a millisecond.
  uint64_t wakeUsTotal = 0, wakeUsMax = 0;
};
void durableLogResidency(const DurableResidency& residency);

// The resident object limit of this process (COWORKERD_DO_RESIDENT_OBJECTS; 0: no limit).
uint64_t durableResidentLimit();

// Which database this is, in the node's terms. `objectName` is the name the object's Host was made with --
// "<namespace>/<object>/<data epoch>" -- so the node needs no map from a path to an object; `storageId` is the
// hexadecimal actor id its files are named after. `facetId` is 0 for the object itself, and a facet also carries its
// name and its parent, from which the node rebuilds the facet tree index of an object it rebuilds.
struct DurableObjectName {
  kj::String objectName;
  kj::String storageId;
  uint facetId = 0;
  uint parentFacetId = 0;
  kj::String facetName;

  kj::String key() const {
    return kj::str(objectName, "/", facetId);
  }
};

struct DurableWalPosition {
  std::array<kj::byte, 32> header{};
  uint32_t magic = 0, pageSize = 0, salt1 = 0, salt2 = 0;
  uint32_t checksum1 = 0, checksum2 = 0, frames = 0;
};

// Reads the committed prefix of a cold image before its home opens SQLite.
DurableWalPosition readDurableWalPosition(const kj::Directory& directory, const kj::Path& path);

class DurableLogShipper;

// A WAL shipper and its notifications stay on the SQLite connection's event loop.
// The channel is responsible for the wire, request IDs and confirmation deadlines.
class DurableWalChannel {
 public:
  virtual bool enabled() const = 0;
  virtual uint64_t nextRequest() = 0;
  virtual uint64_t registerShipper(DurableLogShipper& shipper) = 0;
  virtual void unregisterShipper(DurableLogShipper& shipper) = 0;
  virtual kj::Promise<void> waitForOpen(uint64_t, kj::Timer&) { return kj::READY_NOW; }
  virtual void send(uint64_t openId, kj::Array<kj::byte> message) = 0;
  virtual kj::Promise<void> sendFrames(DurableLogShipper& shipper,
      kj::Array<kj::byte> message, uint64_t request, kj::Timer& timer) = 0;

  virtual ~DurableWalChannel() noexcept(false) = default;
};

class DurableLogShipper {
 public:
  DurableLogShipper(const kj::Directory& directory,
      kj::Path walPath,
      kj::Path databasePath,
      DurableObjectName name,
      SqliteDatabase& db,
      kj::Timer& timer);
  DurableLogShipper(const kj::Directory& directory,
      kj::Path walPath,
      kj::Path databasePath,
      DurableObjectName name,
      SqliteDatabase& db,
      kj::Timer& timer,
      DurableWalChannel& channel,
      kj::Maybe<DurableWalPosition> prepared = kj::none,
      uint64_t localOpenStartedNs = 0);
  ~DurableLogShipper() noexcept(false);
  KJ_DISALLOW_COPY_AND_MOVE(DurableLogShipper);

  // Owned by the SQLite afterClose callback. ActorSqlite drops the shipper
  // first, so the report can be sent once SQLite confirms synchronous close.
  class DeferredClose {
   public:
    void send();

   private:
    friend class DurableLogShipper;
    DurableWalChannel* channel = nullptr;
    uint64_t openId = 0;
    kj::Array<kj::byte> message;
  };
  void reportAfterDatabaseClose(DeferredClose& report);

  // ActorSqlite's commit callback: the frames of everything committed since the last one go to the replicator and
  // the promise resolves when it confirms them. It is rejected -- which breaks the output gate, so the object is
  // reset and every answer it was holding fails -- when the replicator refuses them or does not confirm them within
  // the confirmation timeout. The node then isolates the frames it did not confirm, drops this machine's copy of
  // the object and rebuilds it from the replicated history.
  kj::Promise<void> onCommit();

  // SqliteDatabase::afterReset (the application called deleteAll()): the files were replaced, so the settings go on
  // again and the node is told to start the object from empty.
  void afterReset(SqliteDatabase& db);
  void reportAlarm(kj::Maybe<kj::Date> scheduledTime);
  void reportActorReset(kj::StringPtr code);
  void reportFacetDeleted(uint facet);

  kj::StringPtr getKey() {
    return key;
  }
  uint64_t getOpenId() const {
    return openId;
  }
  bool hasPendingWrites() const {
    return inFlight.size() != 0;
  }

  // From the replicator.
  void onAcked(uint64_t request);
  void onRefused(uint64_t request);
  void onSealed(uint32_t salt1, uint32_t salt2, uint32_t upToFrame);
  void onBreak(kj::StringPtr reason);
  void onCheckpointAsked();

 private:
  const kj::Directory& directory;
  kj::Path walPath, databasePath;
  kj::Maybe<kj::Own<const kj::ReadableFile>> walFile;  // open while a generation is being read
  DurableObjectName name;
  kj::String key;
  SqliteDatabase* db;
  kj::Timer& timer;
  DurableWalChannel& channel;
  uint64_t openId = 0;
  kj::Array<kj::byte>* deferredClose = nullptr;

  // The WAL generation being read: its header, how far it has been read (always a commit frame) and the running
  // checksum there.
  bool haveHeader = false;
  uint32_t magic = 0, pageSize = 0, salt1 = 0, salt2 = 0;
  uint32_t cksum1 = 0, cksum2 = 0;
  kj::Array<kj::byte> header;
  bool headerSent = false;
  uint32_t readFrames = 0;   // frames of this generation read and handed on
  uint32_t ackedFrames = 0;  // of those, confirmed
  uint32_t hookFrames = 0;   // sqlite3_wal_hook's exact mxFrame: what the WAL holds, read or not
  uint64_t generation = 0;   // checkpoints done: a generation of the WAL

  uint32_t sealedFrames = 0;  // in a segment on the object store, as the replicator reported
  bool checkpointAsked = false;

  // Sent and not yet confirmed: the request's generation and the frame of it the request ends at.
  struct InFlight {
    uint64_t generation;
    uint32_t endsAt;
  };
  kj::HashMap<uint64_t, InFlight> inFlight;
  kj::Maybe<kj::ForkedPromise<void>> lastAck;
  // Set by the replicator: no commit of this object is confirmed again (the position it found did not match what it
  // had confirmed, or the object is past its size limit). The object is rebuilt by the node.
  kj::Maybe<kj::String> broken;

  // Frames read and not yet sent (one commit callback can cover several commits).
  kj::Vector<kj::byte> pendingFrames;
  uint32_t pendingFirst = 0, pendingCount = 0, pendingCommits = 0;
  int64_t pendingCommitNs = 0, pendingReadNs = 0;

  void configure(SqliteDatabase& db);
  static int walHook(void* arg, sqlite3* db, const char* name, int frames);
  void readNewFrames();
  kj::Promise<void> sendPending();
  void maybeCheckpoint(kj::StringPtr reason);
  uint64_t walBytes();
  uint64_t databaseBytes();
  void sendOpen(uint64_t localOpenNs);
};

}  // namespace workerd::server
