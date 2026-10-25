// Coworkerd: see coworkerd-wal-shipper.h.
#include "coworkerd-wal-shipper.h"

#include <workerd/jsg/exception.h>

#include <fcntl.h>
#include <sqlite3.h>
#include <unistd.h>

#include <kj/debug.h>
#include <kj/vector.h>

#include <chrono>
#include <cstdlib>

namespace workerd::server {
namespace {

struct Settings {
  int fd = -1;
  uint32_t confirmTimeoutMs = 5000;
  uint64_t checkpointBytes = 4u << 20;
  // One message carries at most this many bytes of frames, cut at a commit frame: a request that commits many
  // transactions before the callback runs would otherwise hand the node one message of all of them.
  uint64_t recordBytes = 8u << 20;
};

uint64_t numberFromEnvironment(const char* variable, uint64_t fallback, uint64_t max) {
  const char* text = std::getenv(variable);
  if (text == nullptr || *text == '\0') return fallback;
  uint64_t value = 0;
  for (const char* p = text; *p; ++p) {
    KJ_REQUIRE(*p >= '0' && *p <= '9' && value <= max, variable, "must be a number");
    value = value * 10 + uint64_t(*p - '0');
  }
  KJ_REQUIRE(value <= max, variable, "is too large");
  return value;
}

const Settings& settings() {
  static const Settings value = []() {
    Settings s;
    const char* text = std::getenv("COWORKERD_DO_WAL_FD");
    if (text != nullptr && *text != '\0') {
      int fd = int(numberFromEnvironment("COWORKERD_DO_WAL_FD", 0, 1000000));
      KJ_REQUIRE(fd > STDERR_FILENO && fcntl(fd, F_GETFD) >= 0, "COWORKERD_DO_WAL_FD is not an open descriptor");
      // Kept from any process this one might start.
      fcntl(fd, F_SETFD, FD_CLOEXEC);
      s.fd = fd;
    }
    s.confirmTimeoutMs = uint32_t(numberFromEnvironment("COWORKERD_DO_CONFIRM_TIMEOUT_MS", 5000, 3600000));
    s.checkpointBytes = numberFromEnvironment("COWORKERD_DO_CHECKPOINT_BYTES", 4u << 20, 1ull << 40);
    s.recordBytes = numberFromEnvironment("COWORKERD_DO_RECORD_BYTES", 8u << 20, 1ull << 30);
    return s;
  }();
  return value;
}

int64_t monoNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void logRow(kj::StringPtr row) {
  auto line = kj::str("[do-log] ", row, "\n");
  size_t done = 0;
  while (done < line.size()) {
    ssize_t n = ::write(STDERR_FILENO, line.begin() + done, line.size() - done);
    if (n <= 0) break;
    done += n;
  }
}

kj::String jsonString(kj::StringPtr text) {
  kj::Vector<char> out;
  out.add('"');
  for (char c: text) {
    if (c == '"' || c == '\\') {
      out.add('\\');
      out.add(c);
    } else if (uint8_t(c) < 0x20) {
      out.add(' ');
    } else {
      out.add(c);
    }
  }
  out.add('"');
  out.add('\0');
  return kj::String(out.releaseAsArray());
}

uint32_t getBe32(const kj::byte* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}
uint32_t getLe32(const kj::byte* p) {
  return uint32_t(p[0]) | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
}
uint64_t getLe64(const kj::byte* p) {
  return uint64_t(getLe32(p)) | (uint64_t(getLe32(p + 4)) << 32);
}

// SQLite's walChecksumBytes(): pairs of 32-bit words, native byte order unless the WAL magic's low bit asks for
// big-endian words.
void walChecksum(bool bigEndianWords, const kj::byte* data, size_t size, uint32_t& s1, uint32_t& s2) {
  KJ_ASSERT(size % 8 == 0);
  uint32_t a = s1, b = s2;
  if (bigEndianWords) {
    for (size_t i = 0; i < size; i += 8) {
      a += getBe32(data + i) + b;
      b += getBe32(data + i + 4) + a;
    }
  } else {
    for (size_t i = 0; i < size; i += 8) {
      uint32_t w[2];
      memcpy(w, data + i, 8);
      a += w[0] + b;
      b += w[1] + a;
    }
  }
  s1 = a;
  s2 = b;
}

// Little-endian message building: u32 length, then the body, whose first byte is the type.
struct Writer {
  kj::Vector<kj::byte> out;
  void u8(uint8_t v) {
    out.add(v);
  }
  void u32(uint32_t v) {
    for (int i = 0; i < 4; i++) out.add(kj::byte(v >> (8 * i)));
  }
  void u64(uint64_t v) {
    for (int i = 0; i < 8; i++) out.add(kj::byte(v >> (8 * i)));
  }
  void str(kj::StringPtr s) {
    KJ_REQUIRE(s.size() <= 0xffff);
    out.add(kj::byte(s.size()));
    out.add(kj::byte(s.size() >> 8));
    out.addAll(s.asBytes());
  }
  void bytes(kj::ArrayPtr<const kj::byte> b) {
    out.addAll(b);
  }
  kj::Array<kj::byte> finish() {
    auto body = out.releaseAsArray();
    auto result = kj::heapArray<kj::byte>(body.size() + 4);
    uint32_t n = body.size();
    for (int i = 0; i < 4; i++) result[i] = kj::byte(n >> (8 * i));
    memcpy(result.begin() + 4, body.begin(), body.size());
    return result;
  }
};

// To the replicator.
constexpr uint8_t MSG_HELLO = 1, MSG_FRAMES = 2, MSG_OPEN = 3, MSG_CHECKPOINT = 4, MSG_RESET = 5,
                  MSG_DELETED = 6, MSG_CLOSED = 7, MSG_GAVE_UP = 8;
// From the replicator.
constexpr uint8_t MSG_ACK = 1, MSG_SEALED = 2, MSG_BREAK = 3, MSG_CHECKPOINT_ASKED = 4;
// An ACK's status.
constexpr uint8_t ACK_OK = 0;

kj::LowLevelAsyncIoProvider* logProvider = nullptr;

// The one channel to the node's replicator: the descriptor the process inherited. Every database's shipper writes
// its messages to it and takes its confirmations from it.
class Replicator final: private kj::TaskSet::ErrorHandler {
 public:
  static Replicator& get() {
    // Deliberately leaked: the channel lives as long as the process.
    static Replicator* instance = new Replicator();
    return *instance;
  }

  bool enabled() {
    return settings().fd >= 0;
  }

  uint64_t nextRequest() {
    return ++requestCounter;
  }

  void send(kj::Array<kj::byte> message) {
    if (!enabled()) return;
    ensureConnected();
    if (failure != kj::none) return;
    auto bytes = kj::mv(message);
    writeChain = writeChain
                     .then([this, bytes = kj::mv(bytes)]() mutable -> kj::Promise<void> {
      auto& s = *KJ_ASSERT_NONNULL(stream);
      auto promise = s.write(bytes.asPtr());
      return promise.attach(kj::mv(bytes));
    }).eagerlyEvaluate([this](kj::Exception&& e) { fail(kj::mv(e)); });
  }

  // Sends a FRAMES message and resolves when the replicator confirms `request`; rejects when it refuses them or
  // does not answer within the confirmation timeout.
  kj::Promise<void> sendFrames(
      kj::Array<kj::byte> message, uint64_t request, kj::StringPtr key, kj::Timer& timer) {
    if (!enabled()) return kj::READY_NOW;
    KJ_IF_SOME(e, failure) {
      return kj::Promise<void>(e.clone());
    }
    auto paf = kj::newPromiseAndFulfiller<void>();
    pending.insert(request, Pending{kj::mv(paf.fulfiller), kj::str(key)});
    send(kj::mv(message));
    // This object's own deadline, below workerd's 30 s output gate timeout: when it runs out the node is told to
    // isolate the frames it has not confirmed and the write is failed with a code the caller can act on. The waiter
    // is rejected rather than cancelled, so the answer carries that code and not a cancellation.
    tasks.add(timer.afterDelay(settings().confirmTimeoutMs * kj::MILLISECONDS)
                  .then([this, request, key = kj::str(key)]() mutable { gaveUp(request, key); }));
    return kj::mv(paf.promise);
  }

  void registerShipper(DurableLogShipper& shipper) {
    shippers.upsert(kj::str(shipper.getKey()), &shipper,
        [](auto& existing, auto&& replacement) { existing = replacement; });
  }
  void unregisterShipper(DurableLogShipper& shipper) {
    KJ_IF_SOME(s, shippers.find(shipper.getKey())) {
      if (s == &shipper) shippers.erase(shipper.getKey());
    }
  }

 private:
  struct Pending {
    kj::Own<kj::PromiseFulfiller<void>> fulfiller;
    kj::String key;
  };

  kj::TaskSet tasks;
  bool started = false;
  kj::Maybe<kj::Own<kj::AsyncIoStream>> stream;
  kj::Promise<void> writeChain = nullptr;
  kj::Maybe<kj::Exception> failure;
  uint64_t requestCounter = 0;
  kj::HashMap<uint64_t, Pending> pending;
  kj::HashMap<kj::String, DurableLogShipper*> shippers;
  kj::Vector<kj::byte> inbox;
  kj::byte readBuffer[65536];

  Replicator(): tasks(*this) {}

  void taskFailed(kj::Exception&& exception) override {
    fail(kj::mv(exception));
  }

  // The channel is gone: nothing can be confirmed again, so every commit waiting and every commit after it fails
  // and the objects of this process are rebuilt by the node from the replicated history.
  void fail(kj::Exception&& e) {
    if (failure != kj::none) return;
    logRow(kj::str("{\"event\":\"channel-failed\",\"monoNs\":", monoNs(), ",\"error\":",
        jsonString(e.getDescription()), "}"));
    auto reason = KJ_EXCEPTION(FAILED,
        "jsg.Error: Durable Object storage is unavailable: the node's replicator is not reachable "
        "(STORAGE_UNAVAILABLE)");
    for (auto& entry: pending) {
      entry.value.fulfiller->reject(reason.clone());
    }
    pending.clear();
    failure = kj::mv(reason);
  }

  void gaveUp(uint64_t request, kj::StringPtr key) {
    KJ_IF_SOME(p, pending.find(request)) {
      p.fulfiller->reject(KJ_EXCEPTION(FAILED,
          "jsg.Error: Durable Object storage is unavailable: the node did not confirm the write "
          "(STORAGE_UNAVAILABLE)"));
      pending.erase(request);
    } else {
      return;
    }
    Writer w;
    w.u8(MSG_GAVE_UP);
    w.u64(request);
    w.str(key);
    w.u64(monoNs());
    send(w.finish());
    logRow(kj::str("{\"event\":\"gave-up\",\"object\":", jsonString(key), ",\"request\":", request,
        ",\"monoNs\":", monoNs(), "}"));
  }

  void ensureConnected() {
    if (started) return;
    started = true;
    KJ_REQUIRE(logProvider != nullptr, "the Durable Object log channel has no I/O provider");
    stream = logProvider->wrapSocketFd(
        static_cast<kj::LowLevelAsyncIoProvider::Fd>(settings().fd),
        kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
            kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC);
    Writer w;
    w.u8(MSG_HELLO);
    w.u32(getpid());
    w.u64(monoNs());
    auto hello = w.finish();
    auto& st = *KJ_ASSERT_NONNULL(stream);
    tasks.add(readLoop());
    writeChain = st.write(hello.asPtr()).attach(kj::mv(hello));
  }

  kj::Promise<void> readLoop() {
    auto& s = *KJ_ASSERT_NONNULL(stream);
    return s.tryRead(readBuffer, 1, sizeof(readBuffer)).then([this](size_t n) -> kj::Promise<void> {
      if (n == 0) {
        fail(KJ_EXCEPTION(DISCONNECTED, "the node's replicator closed the Durable Object log channel"));
        return kj::READY_NOW;
      }
      inbox.addAll(kj::arrayPtr(readBuffer, n));
      size_t at = 0;
      while (inbox.size() - at >= 4) {
        uint32_t length = getLe32(inbox.begin() + at);
        if (inbox.size() - at - 4 < length) break;
        dispatch(kj::arrayPtr(inbox.begin() + at + 4, length));
        at += 4 + length;
      }
      if (at > 0) {
        kj::Vector<kj::byte> rest;
        rest.addAll(kj::arrayPtr(inbox.begin() + at, inbox.size() - at));
        inbox = kj::mv(rest);
      }
      return readLoop();
    });
  }

  // A message's object key: u16 length, the bytes.
  static kj::String keyOf(kj::ArrayPtr<const kj::byte> m, size_t at, size_t& end) {
    KJ_REQUIRE(m.size() >= at + 2);
    size_t length = size_t(m[at]) | (size_t(m[at + 1]) << 8);
    KJ_REQUIRE(m.size() >= at + 2 + length);
    end = at + 2 + length;
    return kj::str(kj::arrayPtr(reinterpret_cast<const char*>(m.begin() + at + 2), length));
  }

  void dispatch(kj::ArrayPtr<const kj::byte> m) {
    KJ_REQUIRE(m.size() >= 1);
    switch (m[0]) {
      case MSG_ACK: {
        KJ_REQUIRE(m.size() >= 10);
        uint64_t request = getLe64(m.begin() + 1);
        uint8_t status = m[9];
        KJ_IF_SOME(p, pending.find(request)) {
          auto key = kj::mv(p.key);
          if (status == ACK_OK) {
            p.fulfiller->fulfill();
          } else {
            p.fulfiller->reject(KJ_EXCEPTION(FAILED,
                "jsg.Error: Durable Object storage is unavailable: the node refused the write "
                "(STORAGE_UNAVAILABLE)"));
          }
          pending.erase(request);
          KJ_IF_SOME(shipper, shippers.find(key)) {
            if (status == ACK_OK) shipper->onAcked(request);
          }
        }
        break;
      }
      case MSG_SEALED: {
        size_t end = 0;
        auto key = keyOf(m, 1, end);
        KJ_REQUIRE(m.size() >= end + 12);
        const kj::byte* p = m.begin() + end;
        KJ_IF_SOME(shipper, shippers.find(key)) {
          shipper->onSealed(getLe32(p), getLe32(p + 4), getLe32(p + 8));
        }
        break;
      }
      case MSG_BREAK: {
        size_t end = 0;
        auto key = keyOf(m, 1, end);
        size_t reasonEnd = 0;
        auto reason = keyOf(m, end, reasonEnd);
        KJ_IF_SOME(shipper, shippers.find(key)) {
          shipper->onBreak(reason);
        }
        break;
      }
      case MSG_CHECKPOINT_ASKED: {
        size_t end = 0;
        auto key = keyOf(m, 1, end);
        KJ_IF_SOME(shipper, shippers.find(key)) {
          shipper->onCheckpointAsked();
        }
        break;
      }
      default:
        KJ_FAIL_REQUIRE("unknown message from the node's replicator", m[0]);
    }
  }
};

}  // namespace

void setDurableLogProvider(kj::LowLevelAsyncIoProvider& provider) {
  logProvider = &provider;
}

bool durableLogEnabled() {
  return settings().fd >= 0;
}

void durableLogFacetDeleted(kj::StringPtr objectName, uint facetId) {
  if (!durableLogEnabled()) return;
  Writer w;
  w.u8(MSG_DELETED);
  w.str(kj::str(objectName, "/", facetId));
  w.u64(monoNs());
  Replicator::get().send(w.finish());
}

DurableLogShipper::DurableLogShipper(const kj::Directory& directory,
    kj::Path walPath,
    kj::Path databasePath,
    DurableObjectName name,
    SqliteDatabase& db,
    kj::Timer& timer)
    : directory(directory),
      walPath(kj::mv(walPath)),
      databasePath(kj::mv(databasePath)),
      name(kj::mv(name)),
      key(this->name.key()),
      db(&db),
      timer(timer) {
  configure(db);
  Replicator::get().registerShipper(*this);

  // The files of an earlier instance of this object may still hold a WAL: its frames went to the replicator then,
  // so this instance starts after them -- and reports where it starts, because the earlier instance may have died
  // between a commit and its confirmation. The node compares the position with what it confirmed and breaks this
  // object when they differ: the frames it never confirmed must not be read by anybody.
  if (walBytes() >= 32) {
    readNewFrames();
    pendingFrames.clear();
    pendingCount = 0;
    pendingCommits = 0;
    pendingReadNs = 0;
    ackedFrames = readFrames;
  }
  sendOpen();
}

DurableLogShipper::~DurableLogShipper() noexcept(false) {
  Replicator::get().unregisterShipper(*this);
  sqlite3_wal_hook(*db, nullptr, nullptr);
  if (!durableLogEnabled()) return;
  Writer w;
  w.u8(MSG_CLOSED);
  w.str(key);
  w.u64(monoNs());
  Replicator::get().send(w.finish());
}

void DurableLogShipper::configure(SqliteDatabase& database) {
  db = &database;
  sqlite3* handle = database;
  // No checkpoint but the ones below: not after a commit, and not when the last connection closes -- the WAL must
  // keep every frame that has not been handed on.
  sqlite3_wal_autocheckpoint(handle, 0);
  int previous = -1;
  sqlite3_db_config(handle, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, &previous);
  // The owner does not wait for its own disk: this machine's files are a cache, and the write is durable once the
  // node's peers hold its frames (docs/s3-truth-and-durable-objects.md, section 5.3).
  database.run(SqliteDatabase::QueryOptions{.regulator = SqliteDatabase::TRUSTED},
      "PRAGMA synchronous=OFF;"_kj);
  // Only to count: the hook's frame count is SQLite's own mxFrame, which says whether the WAL holds a frame this
  // shipper has not read (a rolled back overflow frame stays in the file, so its size does not say).
  sqlite3_wal_hook(handle, &DurableLogShipper::walHook, this);
}

void DurableLogShipper::afterReset(SqliteDatabase& database) {
  configure(database);
  walFile = kj::none;
  haveHeader = false;
  headerSent = false;
  readFrames = 0;
  ackedFrames = 0;
  hookFrames = 0;
  sealedFrames = 0;
  checkpointAsked = false;
  pendingFrames.clear();
  pendingCount = 0;
  pendingCommits = 0;
  inFlight.clear();
  ++generation;
  Writer w;
  w.u8(MSG_RESET);
  w.str(key);
  w.str(name.objectName);
  w.str(name.storageId);
  w.u32(name.facetId);
  w.u32(name.parentFacetId);
  w.str(name.facetName);
  w.u64(monoNs());
  Replicator::get().send(w.finish());
}

int DurableLogShipper::walHook(void* arg, sqlite3*, const char*, int frames) {
  auto& self = *reinterpret_cast<DurableLogShipper*>(arg);
  if (frames >= 0) self.hookFrames = uint32_t(frames);
  return SQLITE_OK;
}

// Reads the frames committed after `readFrames` and appends them to `pendingFrames`. Every frame is checked as
// SQLite's own WAL recovery checks it -- the generation's salt, a page number, the chained checksum -- and only the
// frames up to the last commit frame are taken: a transaction is handed on whole or not at all.
void DurableLogShipper::readNewFrames() {
  int64_t start = monoNs();
  if (!haveHeader || walFile == kj::none) {
    walFile = directory.tryOpenFile(walPath);
  }
  auto& file = KJ_UNWRAP_OR(walFile, return);
  uint64_t size = file->stat().size;
  if (!haveHeader) {
    if (size < 32) return;
    auto h = kj::heapArray<kj::byte>(32);
    KJ_REQUIRE(file->read(0, h) == 32);
    uint32_t m = getBe32(h.begin());
    KJ_REQUIRE((m & 0xFFFFFFFE) == 0x377f0682, "not a WAL header", m);
    uint32_t s1 = 0, s2 = 0;
    walChecksum(m & 1, h.begin(), 24, s1, s2);
    KJ_REQUIRE(s1 == getBe32(h.begin() + 24) && s2 == getBe32(h.begin() + 28), "WAL header checksum");
    magic = m;
    pageSize = getBe32(h.begin() + 8);
    salt1 = getBe32(h.begin() + 16);
    salt2 = getBe32(h.begin() + 20);
    cksum1 = s1;
    cksum2 = s2;
    header = kj::mv(h);
    haveHeader = true;
    headerSent = false;
    readFrames = 0;
    ackedFrames = 0;
    sealedFrames = 0;
  }
  uint64_t frameSize = 24 + pageSize;
  uint64_t offset = 32 + uint64_t(readFrames) * frameSize;
  if (size <= offset) return;
  uint64_t available = (size - offset) / frameSize;
  if (available == 0) return;
  auto buffer = kj::heapArray<kj::byte>(available * frameSize);
  size_t got = file->read(offset, buffer);
  available = got / frameSize;

  uint32_t s1 = cksum1, s2 = cksum2;
  uint64_t lastCommitEnd = 0;
  uint32_t lastCommitCk1 = cksum1, lastCommitCk2 = cksum2;
  uint32_t commitsHere = 0;
  for (uint64_t i = 0; i < available; i++) {
    const kj::byte* f = buffer.begin() + i * frameSize;
    if (getBe32(f + 8) != salt1 || getBe32(f + 12) != salt2) break;
    if (getBe32(f) == 0) break;
    walChecksum(magic & 1, f, 8, s1, s2);
    walChecksum(magic & 1, f + 24, pageSize, s1, s2);
    if (s1 != getBe32(f + 16) || s2 != getBe32(f + 20)) break;
    if (getBe32(f + 4) != 0) {
      lastCommitEnd = (i + 1) * frameSize;
      lastCommitCk1 = s1;
      lastCommitCk2 = s2;
      ++commitsHere;
      // One message's worth: the rest of the frames stay in the file and go in the next one. A transaction larger
      // than this is still taken whole -- it is one commit.
      if (pendingFrames.size() + lastCommitEnd >= settings().recordBytes) break;
    }
  }
  if (lastCommitEnd == 0) return;
  uint32_t frames = lastCommitEnd / frameSize;
  if (pendingCount == 0) {
    pendingFirst = readFrames + 1;
    pendingCommitNs = start;
  }
  pendingFrames.addAll(buffer.first(lastCommitEnd));
  pendingCount += frames;
  pendingCommits += commitsHere;
  readFrames += frames;
  cksum1 = lastCommitCk1;
  cksum2 = lastCommitCk2;
  pendingReadNs += monoNs() - start;
}

kj::Promise<void> DurableLogShipper::sendPending() {
  if (pendingCount == 0) {
    // Nothing new: this commit's frames went with an earlier message (one callback can cover several commits), or
    // it wrote no page. Either way the answer waits for the latest confirmation.
    KJ_IF_SOME(last, lastAck) {
      return last.addBranch();
    }
    return kj::READY_NOW;
  }
  auto& replicator = Replicator::get();
  uint64_t request = replicator.nextRequest();
  Writer w;
  w.u8(MSG_FRAMES);
  w.u64(request);
  w.str(key);
  w.str(name.objectName);
  w.str(name.storageId);
  w.u32(name.facetId);
  w.u32(name.parentFacetId);
  w.str(name.facetName);
  w.u64(generation);
  w.u32(salt1);
  w.u32(salt2);
  w.u32(pageSize);
  w.u32(pendingFirst);
  w.u32(pendingCount);
  w.u32(pendingCommits);
  w.u64(pendingCommitNs);
  w.u64(pendingReadNs);
  w.u64(monoNs());
  // A generation's header goes with its first frames, so every message can be checked on its own.
  if (!headerSent) {
    w.u8(1);
    w.bytes(header);
    headerSent = true;
  } else {
    w.u8(0);
  }
  w.bytes(pendingFrames.asPtr());
  uint32_t endsAt = pendingFirst + pendingCount - 1;
  pendingFrames.clear();
  pendingCount = 0;
  pendingCommits = 0;
  pendingReadNs = 0;
  inFlight.insert(request, InFlight{generation, endsAt});
  auto promise = replicator.sendFrames(w.finish(), request, key, timer).fork();
  lastAck = promise.addBranch().fork();
  return promise.addBranch();
}

kj::Promise<void> DurableLogShipper::onCommit() {
  KJ_IF_SOME(reason, broken) {
    // The node took this object away from this instance: nothing it commits is confirmed again.
    return kj::Promise<void>(KJ_EXCEPTION(FAILED, kj::str("jsg.Error: ", reason)));
  }
  // Everything committed since the last callback, in messages of at most COWORKERD_DO_RECORD_BYTES: the answer waits
  // for the last of them, and the node confirms an object's messages in the order they were sent.
  kj::Maybe<kj::Promise<void>> answer;
  for (;;) {
    const auto before = readFrames;
    readNewFrames();
    if (pendingCount == 0) break;
    answer = sendPending();
    if (readFrames == before) break;
  }
  KJ_IF_SOME(promise, answer) {
    return kj::mv(promise);
  }
  // Nothing new: this commit's frames went with an earlier message, or it wrote no page.
  return sendPending();
}

void DurableLogShipper::onAcked(uint64_t request) {
  KJ_IF_SOME(f, inFlight.find(request)) {
    if (f.generation == generation) ackedFrames = kj::max(ackedFrames, f.endsAt);
    inFlight.erase(request);
  }
  maybeCheckpoint("confirmed"_kj);
}

void DurableLogShipper::onSealed(uint32_t s1, uint32_t s2, uint32_t upToFrame) {
  if (!haveHeader || s1 != salt1 || s2 != salt2) return;
  sealedFrames = kj::max(sealedFrames, upToFrame);
  maybeCheckpoint("sealed"_kj);
}

void DurableLogShipper::onBreak(kj::StringPtr reason) {
  if (broken == kj::none) {
    broken = kj::str(reason);
    logRow(kj::str("{\"event\":\"broken\",\"object\":", jsonString(key), ",\"reason\":",
        jsonString(reason), ",\"monoNs\":", monoNs(), "}"));
  }
}

void DurableLogShipper::onCheckpointAsked() {
  checkpointAsked = true;
  maybeCheckpoint("idle"_kj);
}

uint64_t DurableLogShipper::walBytes() {
  KJ_IF_SOME(file, directory.tryOpenFile(walPath)) {
    return file->stat().size;
  }
  return 0;
}

uint64_t DurableLogShipper::databaseBytes() {
  KJ_IF_SOME(file, directory.tryOpenFile(databasePath)) {
    return file->stat().size;
  }
  return 0;
}

void DurableLogShipper::sendOpen() {
  if (!durableLogEnabled()) return;
  Writer w;
  w.u8(MSG_OPEN);
  w.str(key);
  w.str(name.objectName);
  w.str(name.storageId);
  w.u32(name.facetId);
  w.u32(name.parentFacetId);
  w.str(name.facetName);
  w.u32(salt1);
  w.u32(salt2);
  w.u32(readFrames);
  w.u64(walBytes());
  w.u64(databaseBytes());
  w.u64(monoNs());
  Replicator::get().send(w.finish());
}

// A checkpoint moves the WAL's pages into the database file and truncates it. It runs on this process's event loop
// and holds every object of the process while it does, so it is done when the WAL has grown past
// COWORKERD_DO_CHECKPOINT_BYTES or when the node says the object is idle. Everything read must have been handed to
// the replicator (frames in flight are in its hands, which is what lets a busy object checkpoint at all), the
// connection must be idle, and the WAL must hold nothing this shipper has not read.
void DurableLogShipper::maybeCheckpoint(kj::StringPtr reason) {
  if (!haveHeader || broken != kj::none) return;
  uint64_t bytes = walBytes();
  if (!checkpointAsked && bytes < settings().checkpointBytes) return;
  if (pendingCount != 0) return;
  sqlite3* handle = *db;
  if (sqlite3_get_autocommit(handle) == 0) return;
  if (hookFrames != readFrames) return;
  int64_t start = monoNs();
  int logFrames = -1, checkpointed = -1;
  int rc = sqlite3_wal_checkpoint_v2(handle, "main", SQLITE_CHECKPOINT_TRUNCATE, &logFrames, &checkpointed);
  int64_t took = monoNs() - start;
  if (rc != SQLITE_OK) {
    logRow(kj::str("{\"event\":\"checkpoint-failed\",\"object\":", jsonString(key), ",\"rc\":", rc,
        ",\"frames\":", readFrames, ",\"walBytes\":", bytes, "}"));
    return;
  }
  checkpointAsked = false;
  Writer w;
  w.u8(MSG_CHECKPOINT);
  w.str(key);
  w.u64(generation);
  w.u32(salt1);
  w.u32(salt2);
  w.u32(readFrames);
  w.u32(ackedFrames);
  w.u64(databaseBytes());
  w.u64(start);
  w.u64(took);
  w.str(reason);
  Replicator::get().send(w.finish());
  // The next commit opens a new generation: a new header at the head of the truncated file, new salts, frames from
  // one again.
  haveHeader = false;
  headerSent = false;
  readFrames = 0;
  ackedFrames = 0;
  hookFrames = 0;
  sealedFrames = 0;
  walFile = kj::none;
  ++generation;
}

}  // namespace workerd::server
