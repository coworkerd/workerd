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

namespace workerd::server {

// Called once before serving, from cli-main: the inherited descriptor is wrapped with this provider.
void setDurableLogProvider(kj::LowLevelAsyncIoProvider& provider);

// Whether the actor storage factory ships what it opens: the descriptor is there.
bool durableLogEnabled();

// A facet's files were removed outside its own transactions (deleteFacet(), or the parent's deleteAll()).
void durableLogFacetDeleted(kj::StringPtr objectName, uint facetId);

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

class DurableLogShipper {
 public:
  DurableLogShipper(const kj::Directory& directory,
      kj::Path walPath,
      kj::Path databasePath,
      DurableObjectName name,
      SqliteDatabase& db,
      kj::Timer& timer);
  ~DurableLogShipper() noexcept(false);
  KJ_DISALLOW_COPY_AND_MOVE(DurableLogShipper);

  // ActorSqlite's commit callback: the frames of everything committed since the last one go to the replicator and
  // the promise resolves when it confirms them. It is rejected -- which breaks the output gate, so the object is
  // reset and every answer it was holding fails -- when the replicator refuses them or does not confirm them within
  // the confirmation timeout. The node then isolates the frames it did not confirm, drops this machine's copy of
  // the object and rebuilds it from the replicated history.
  kj::Promise<void> onCommit();

  // SqliteDatabase::afterReset (the application called deleteAll()): the files were replaced, so the settings go on
  // again and the node is told to start the object from empty.
  void afterReset(SqliteDatabase& db);

  kj::StringPtr getKey() {
    return key;
  }

  // From the replicator.
  void onAcked(uint64_t request);
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
  void sendOpen();
};

}  // namespace workerd::server
