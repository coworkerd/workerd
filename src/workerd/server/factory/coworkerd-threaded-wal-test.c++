#include "coworkerd-durable-test-storage.h"
#include "coworkerd-threaded-wal.c++"

#include <fcntl.h>
#include <sqlite3.h>
#include <sys/socket.h>

#include <kj/test.h>

namespace workerd::server {
namespace {

// This half is the private node protocol, driven one reply at a time by the test.
struct WirePair {
  kj::AsyncIoContext io = kj::setupAsyncIo();
  kj::Own<kj::AsyncIoStream> node;
  kj::Own<WalWire> child;

  explicit WirePair(bool limitSendBuffer = false, kj::Maybe<kj::AutoCloseFd&> external = kj::none) {
    int fds[2];
    KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    if (limitSendBuffer) {
      int sendBuffer = 4096;
      KJ_SYSCALL(setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &sendBuffer, sizeof(sendBuffer)));
    }
    KJ_SYSCALL(fcntl(fds[0], F_SETFD, FD_CLOEXEC));
    KJ_IF_SOME(fd, external) {
      fd = kj::AutoCloseFd(fds[0]);
    } else {
      child = kj::heap<WalWire>(*io.lowLevelProvider, fds[0]);
    }
    node = io.lowLevelProvider->wrapSocketFd(fds[1], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
  }

  kj::Array<kj::byte> receive() {
    kj::byte header[4];
    auto& timer = io.provider->getTimer();
    timer.timeoutAfter(kj::SECONDS, node->read(kj::arrayPtr(header, sizeof(header))))
        .wait(io.waitScope);
    auto body = kj::heapArray<kj::byte>(getLe32(header));
    timer.timeoutAfter(kj::SECONDS, node->read(body.asPtr())).wait(io.waitScope);
    return body;
  }

  kj::Array<kj::byte> receive(uint8_t type, uint64_t openId) {
    auto body = receive();
    KJ_REQUIRE(body.size() >= 9);
    KJ_REQUIRE(body[0] == type, body[0], type);
    KJ_EXPECT(getLe64(body.begin() + 1) == openId);
    return body;
  }

  void reply(kj::Array<kj::byte> bytes) {
    node->write(bytes.asPtr()).wait(io.waitScope);
  }

  template <typename T>
  T wait(kj::Promise<T> promise) {
    return io.provider->getTimer().timeoutAfter(kj::SECONDS, kj::mv(promise)).wait(io.waitScope);
  }

  void ready(uint64_t generation) {
    auto hello = receive();
    KJ_ASSERT(hello.size() == 25 && hello[0] == MSG_HELLO);
    KJ_ASSERT(getLe32(hello.begin() + 13) == THREADED_WAL_REVISION);
    KJ_ASSERT(getLe64(hello.begin() + 17) == generation);
    Writer response;
    response.u8(MSG_READY);
    response.u32(THREADED_WAL_REVISION);
    response.u64(generation);
    reply(response.finish());
  }

  void ack(uint64_t request) {
    Writer response;
    response.u8(MSG_ACK);
    response.u64(request);
    response.u8(ACK_OK);
    reply(response.finish());
  }

  void opened(uint64_t openId, uint8_t status = ACK_OK) {
    Writer response;
    response.u8(MSG_OPENED);
    response.u8(status);
    reply(response.finish(openId));
  }

  void requestClose(uint64_t openId, uint8_t type) {
    Writer response;
    response.u8(type);
    reply(response.finish(openId));
  }

  void checkpoint(uint64_t openId) {
    Writer response;
    response.u8(MSG_CHECKPOINT_ASKED);
    reply(response.finish(openId));
  }

  void expectGiveUp(uint64_t openId, uint64_t request, kj::StringPtr key) {
    auto body = receive(MSG_GAVE_UP, openId);
    Writer expected;
    expected.u8(MSG_GAVE_UP);
    expected.u64(request);
    expected.str(key);
    auto prefix = expected.finish(openId);
    // The monotonic timestamp comes after the request and storage key.
    KJ_REQUIRE(body.size() == prefix.size() - 4 + sizeof(uint64_t));
    KJ_EXPECT(body.first(prefix.size() - 4) == prefix.slice(4));
  }
};

KJ_TEST("threaded WAL routes interleaved home writes and ignores an old open and reply") {
  WirePair link;
  Shared shared(kj::SECONDS);
  auto session = kj::heap<Session>(shared, *link.child, 7);
  auto running = session->run().eagerlyEvaluate(nullptr);
  KJ_ASSERT(!shared.negotiated());
  link.ready(7);
  link.io.waitScope.poll();
  KJ_ASSERT(shared.negotiated());

  auto first = std::make_shared<Inbox>();
  auto second = std::make_shared<Inbox>();
  auto post = [&](uint8_t type, uint64_t openId, const std::shared_ptr<Inbox>& inbox) {
    if (type == MSG_OPEN) {
      KJ_REQUIRE(inbox->tryRegister(openId));
    }
    if (type == MSG_CLOSED) inbox->unregister(openId);
    Writer frame;
    frame.u8(type);
    KJ_REQUIRE(shared.post({.bytes = frame.finish(openId), .openId = openId, .inbox = inbox}, true));
    auto observed = link.receive();
    KJ_ASSERT(observed[0] == type && getLe64(observed.begin() + 1) == openId);
  };
  auto append = [&](uint64_t openId, uint64_t request) {
    auto paf = kj::newPromiseAndCrossThreadFulfiller<uint8_t>();
    Writer frame;
    frame.u8(MSG_FRAMES);
    frame.u64(request);
    KJ_REQUIRE(shared.post({.bytes = frame.finish(openId), .openId = openId,
                            .request = request, .ack = kj::mv(paf.fulfiller)}, false));
    auto observed = link.receive();
    KJ_ASSERT(observed[0] == MSG_FRAMES && getLe64(observed.begin() + 1) == openId &&
        getLe64(observed.begin() + 9) == request);
    return kj::mv(paf.promise);
  };
  post(MSG_OPEN, 1, first);
  post(MSG_OPEN, 2, second);
  auto held = append(1, 3);
  auto independent = append(2, 4);
  link.ack(4);
  KJ_EXPECT(link.wait(kj::mv(independent)) == ACK_OK);
  KJ_EXPECT(!held.poll(link.io.waitScope));
  link.ack(3);
  KJ_EXPECT(link.wait(kj::mv(held)) == ACK_OK);

  post(MSG_CLOSED, 1, first);
  post(MSG_OPEN, 5, first);
  auto reopened = append(5, 6);
  link.ack(3);
  link.checkpoint(1);
  link.checkpoint(5);
  auto notice = link.wait(first->next());
  KJ_EXPECT(KJ_ASSERT_NONNULL(notice).openId == 5);
  KJ_EXPECT(!reopened.poll(link.io.waitScope));

  // EOF rejects the remaining write and invalidates readiness.
  link.node = nullptr;
  KJ_EXPECT_THROW_MESSAGE("replicator closed", link.wait(kj::mv(running)));
  session = nullptr;
  KJ_EXPECT(link.wait(kj::mv(reopened)) == ACK_DISCONNECTED);
  KJ_EXPECT(!shared.negotiated());
}

KJ_TEST("threaded WAL reserves a control allowance and returns byte credit on consumption") {
  auto io = kj::setupAsyncIo();
  Mailbox<int> mailbox;
  KJ_EXPECT(mailbox.push(1, QUEUED_RECORD_BYTES, QUEUED_RECORD_BYTES));
  KJ_EXPECT(!mailbox.push(2, 1, QUEUED_RECORD_BYTES));
  KJ_EXPECT(mailbox.push(3, 1, QUEUED_RECORD_BYTES + QUEUED_CONTROL_BYTES));
  KJ_EXPECT(KJ_ASSERT_NONNULL(mailbox.next().wait(io.waitScope)) == 1);
  KJ_EXPECT(mailbox.push(2, QUEUED_RECORD_BYTES - 1, QUEUED_RECORD_BYTES));
  KJ_EXPECT(KJ_ASSERT_NONNULL(mailbox.next().wait(io.waitScope)) == 3);
  KJ_EXPECT(KJ_ASSERT_NONNULL(mailbox.next().wait(io.waitScope)) == 2);
  auto waiting = mailbox.next();
  KJ_EXPECT(!waiting.poll(io.waitScope));
  mailbox.close();
  KJ_EXPECT(waiting.wait(io.waitScope) == kj::none);
  KJ_EXPECT(!mailbox.push(1, 1, QUEUED_RECORD_BYTES));
}

KJ_TEST("threaded WAL rejects readiness for another generation") {
  WirePair link;
  Shared shared(kj::SECONDS);
  Session session(shared, *link.child, 7);
  auto running = session.run().eagerlyEvaluate(nullptr);
  (void)link.receive();
  Writer reply;
  reply.u8(MSG_READY);
  reply.u32(THREADED_WAL_REVISION);
  reply.u64(8);
  link.reply(reply.finish());
  KJ_EXPECT_THROW_MESSAGE("threaded WAL readiness mismatch", link.wait(kj::mv(running)));
  KJ_EXPECT(!shared.negotiated());
}

using CloseReason = ThreadedWalLog::CloseReason;
constexpr auto CONFIRMATION_TIMEOUT = kj::SECONDS;
constexpr auto JUST_BEFORE_TIMEOUT = CONFIRMATION_TIMEOUT - kj::MILLISECONDS;

// Run Session and Home on one loop. Only the watchdog for socket I/O uses real time.
struct HomeSession {
  WirePair link;
  kj::TimerImpl clock{kj::origin<kj::TimePoint>()};
  std::shared_ptr<Shared> shared = std::make_shared<Shared>(CONFIRMATION_TIMEOUT);
  Session session{*shared, *link.child, 7};
  kj::Promise<void> running = session.run().eagerlyEvaluate(nullptr);

  explicit HomeSession(bool limitSendBuffer = false): link(limitSendBuffer) {
    link.ready(7);
    link.io.waitScope.poll();
    KJ_REQUIRE(shared->negotiated());
  }

  kj::Own<DurableWalChannel> attachHome(
      kj::Function<void(uint64_t, CloseReason)> onClose = [](uint64_t, CloseReason) {
        KJ_FAIL_EXPECT("unexpected close request");
      }) {
    return kj::heap<Home>(shared, kj::mv(onClose), clock);
  }

  void advance(kj::Duration duration) {
    clock.advanceTo(clock.now() + duration);
    link.io.waitScope.poll();
  }
};

struct StorageSession {
  static constexpr uint64_t GENERATION = 7;
  static constexpr auto NAMESPACE =
      "coworkerd-do-root-v1-1111111111111111111111111111111111111111111111111111111111111111"_kj;
  static constexpr auto ID = "2222222222222222222222222222222222222222222222222222222222222222"_kj;
  static constexpr auto OTHER =
      "3333333333333333333333333333333333333333333333333333333333333333"_kj;

  kj::AutoCloseFd fd;
  WirePair link{false, fd};
  DurableStorage storage;
  ThreadedWalLog log{fd.release(), GENERATION, CONFIRMATION_TIMEOUT};
  kj::Own<DurableWalChannel> oldHome = log.attachHome([](uint64_t, CloseReason) {});
  kj::Own<DurableWalChannel> nextHome = log.attachHome([](uint64_t, CloseReason) {});

  StorageSession() {
    link.ready(GENERATION);
    link.wait(waitForReady());
  }

  kj::Promise<void> waitForReady() {
    while (!log.ready()) co_await link.io.provider->getTimer().afterDelay(kj::MILLISECONDS);
  }

  kj::Promise<ThreadedWalLog::PreparedOpen> prepare(
      uint64_t tree, uint facet, kj::StringPtr id = ID) {
    auto image = storage.walImage(NAMESPACE, id, facet);
    return log.prepareOpen(
        image.directory, kj::mv(image.path), kj::mv(image.storageKey), tree, facet);
  }

  struct Connection {
    kj::Own<SqliteDatabase> database;
    kj::Own<DurableLogShipper> shipper;

    void close() {
      shipper = nullptr;
      database = nullptr;
    }
  };

  Connection open(DurableWalChannel& home,
      const ThreadedWalLog::PreparedOpen& prepared,
      uint facet,
      kj::StringPtr id = ID) {
    const auto position = prepared.positionForOpen();
    auto database = storage.open(NAMESPACE, id, SqliteObserver::DEFAULT, facet);
    auto shipper = storage.ship(NAMESPACE,
        {.objectName = kj::str(id), .storageId = kj::str(id), .facetId = facet}, *database,
        link.io.provider->getTimer(), home, position, 0, prepared.trackClose());
    const auto openId = shipper->getOpenId();
    link.receive(MSG_OPEN, openId);
    auto opening = home.waitForOpen(openId, link.io.provider->getTimer());
    link.opened(openId);
    link.wait(kj::mv(opening));
    return {kj::mv(database), kj::mv(shipper)};
  }
};

KJ_TEST("threaded WAL waits before scanning for a retained root and facet") {
  StorageSession test;
  auto oldPreparation = test.link.wait(test.prepare(1, 0));
  auto root = test.open(*test.oldHome, oldPreparation, 0);
  auto facet = test.open(*test.oldHome, test.link.wait(test.prepare(1, 1)), 1);
  auto reopening = test.prepare(2, 0).eagerlyEvaluate(nullptr);
  {
    // A canceled preparation never opens SQLite or owns the next tree.
    auto canceled = test.prepare(3, 0);
    KJ_REQUIRE(!canceled.poll(test.link.io.waitScope));
  }

  // The unrelated scan passes through the same scanner while the new tree is
  // waiting. It also lets us check that no earlier scan completed on that queue.
  auto neighbor = test.link.wait(test.prepare(4, 0, StorageSession::OTHER));
  KJ_REQUIRE(!reopening.poll(test.link.io.waitScope));
  const auto rootId = root.shipper->getOpenId();
  root.close();
  test.link.receive(MSG_CLOSED, rootId);
  auto neighborFacet = test.link.wait(test.prepare(4, 1, StorageSession::OTHER));
  KJ_REQUIRE(
      !reopening.poll(test.link.io.waitScope), "the previous facet still holds this storage");

  const auto facetId = facet.shipper->getOpenId();
  facet.shipper = nullptr;
  auto unrelated = test.open(*test.nextHome, neighbor, 0, StorageSession::OTHER);
  auto unrelatedFacet = test.open(*test.nextHome, neighborFacet, 1, StorageSession::OTHER);
  KJ_REQUIRE(
      !reopening.poll(test.link.io.waitScope), "deregistration does not prove SQLite closed");
  facet.database = nullptr;
  auto prepared = test.link.wait(kj::mv(reopening));
  test.link.receive(MSG_CLOSED, facetId);
  KJ_EXPECT_THROW_MESSAGE("storage preparation was already used", oldPreparation.positionForOpen());
  auto reopened = test.open(*test.nextHome, prepared, 0);
  KJ_EXPECT(reopened.shipper->getOpenId() != rootId);
}

KJ_TEST("a BUSY SQLite close cannot release threaded WAL preparation") {
  StorageSession test;
  auto old = test.open(*test.oldHome, test.link.wait(test.prepare(1, 0)), 0);
  auto reopening = test.prepare(2, 0).eagerlyEvaluate(nullptr);
  KJ_REQUIRE(!reopening.poll(test.link.io.waitScope));
  sqlite3_stmt* statement = nullptr;
  // This fixture bypasses the wrapper's SQL preparation only to keep one
  // untracked dependent handle. No SQL runs after the statement is prepared.
  KJ_REQUIRE(sqlite3_set_authorizer(*old.database, nullptr, nullptr) == SQLITE_OK);
  KJ_REQUIRE(sqlite3_prepare_v2(*old.database, "SELECT 1", -1, &statement, nullptr) == SQLITE_OK);
  KJ_DEFER(if (statement != nullptr) KJ_EXPECT(sqlite3_finalize(statement) == SQLITE_OK));
  {
    KJ_EXPECT_LOG(ERROR, "sqlite database destroyed while dependent objects still exist");
    old.close();
  }

  KJ_EXPECT_THROW_RECOVERABLE_MESSAGE(
      "did not confirm close in this generation", test.link.wait(kj::mv(reopening)));
  KJ_EXPECT(test.log.unconfirmedCloseIdentities() == 1);
  KJ_EXPECT_THROW_RECOVERABLE_MESSAGE(
      "did not confirm close in this generation", test.link.wait(test.prepare(2, 0)));
  auto neighbor = test.open(*test.nextHome,
      test.link.wait(test.prepare(3, 0, StorageSession::OTHER)), 0, StorageSession::OTHER);
  const auto finalized = sqlite3_finalize(std::exchange(statement, nullptr));
  KJ_REQUIRE(finalized == SQLITE_OK);
  KJ_EXPECT_THROW_RECOVERABLE_MESSAGE(
      "did not confirm close in this generation", test.link.wait(test.prepare(4, 0)));
}

KJ_TEST("a reset failure after SQLite closed still confirms final close") {
  StorageSession test;
  auto old = test.open(*test.oldHome, test.link.wait(test.prepare(1, 0)), 0);
  auto reopen = test.prepare(2, 0).eagerlyEvaluate(nullptr);
  auto id = old.shipper->getOpenId();
  auto image = test.storage.walImage(StorageSession::NAMESPACE, StorageSession::ID, 0);
  // Force the unlink in reset() to fail after the old SQLite handle closes.
  image.directory.remove(
      kj::Path({kj::str(StorageSession::NAMESPACE), kj::str(StorageSession::ID, ".sqlite")}));
  KJ_EXPECT_THROW_RECOVERABLE_MESSAGE("path to remove doesn't exist", old.database->reset());
  KJ_EXPECT(!reopen.poll(test.link.io.waitScope));
  old.close();
  test.link.receive(MSG_CLOSED, id);
  auto next = test.open(*test.nextHome, test.link.wait(kj::mv(reopen)), 0);
  KJ_EXPECT(test.log.unconfirmedCloseIdentities() == 0);
}

KJ_TEST("dropping a reserved WAL preparation allows the same identity to reopen") {
  StorageSession test;
  {
    auto abandoned = test.link.wait(test.prepare(1, 0));
    abandoned.positionForOpen();
  }
  auto reopened = test.open(*test.nextHome, test.link.wait(test.prepare(2, 0)), 0);
}

KJ_TEST("stopping threaded WAL wakes preparations waiting for close") {
  // The peer disconnect is reported on the WAL owner thread.
  class ExpectedDisconnect final: public kj::ExceptionCallback {
   public:
    explicit ExpectedDisconnect(std::atomic<bool>& seen): seen(seen) {}

    void logMessage(kj::LogSeverity severity, const char* file, int line, int depth,
        kj::String&& message) override {
      if (severity == kj::LogSeverity::ERROR && message.contains("threaded WAL channel closed"_kj)) {
        seen = true;
        return;
      }
      ExceptionCallback::logMessage(severity, file, line, depth, kj::mv(message));
    }

    kj::Function<void(kj::Function<void()>)> getThreadInitializer() override {
      return [next = ExceptionCallback::getThreadInitializer(), &seen = seen](
                 kj::Function<void()> run) mutable {
        next([&] {
          ExpectedDisconnect expected(seen);
          run();
        });
      };
    }

   private:
    std::atomic<bool>& seen;
  };
  std::atomic<bool> sawDisconnect = false;
  {
    ExpectedDisconnect expected(sawDisconnect);
    StorageSession test;
    auto old = test.open(*test.oldHome, test.link.wait(test.prepare(1, 0)), 0);
    auto reopening = test.prepare(2, 0).eagerlyEvaluate(nullptr);
    KJ_EXPECT(!reopening.poll(test.link.io.waitScope));
    test.link.node = nullptr;
    KJ_EXPECT_THROW_RECOVERABLE_MESSAGE(
        "replicator is not reachable", test.link.wait(kj::mv(reopening)));
  }
  KJ_EXPECT(sawDisconnect.load());
}

class BeforeClosedChannel final: public DurableWalChannel {
 public:
  explicit BeforeClosedChannel(DurableWalChannel& inner): inner(inner) {}
  kj::Maybe<kj::Function<void()>> beforeClosed;

  bool enabled() const override {
    return inner.enabled();
  }
  uint64_t nextRequest() override {
    return inner.nextRequest();
  }
  uint64_t registerShipper(DurableLogShipper& shipper) override {
    return inner.registerShipper(shipper);
  }
  void unregisterShipper(DurableLogShipper& shipper) override {
    inner.unregisterShipper(shipper);
  }
  kj::Promise<void> waitForOpen(uint64_t id, kj::Timer& timer) override {
    return inner.waitForOpen(id, timer);
  }
  void send(uint64_t id, kj::Array<kj::byte> message) override {
    if (message[4] == MSG_CLOSED) {
      KJ_IF_SOME(check, beforeClosed) check();
    }
    inner.send(id, kj::mv(message));
  }
  kj::Promise<void> sendFrames(DurableLogShipper& shipper,
      kj::Array<kj::byte> message,
      uint64_t request,
      kj::Timer& timer) override {
    return inner.sendFrames(shipper, kj::mv(message), request, timer);
  }

 private:
  DurableWalChannel& inner;
};

KJ_TEST("threaded WAL queues CLOSED before waking a replacement on another home") {
  StorageSession test;
  BeforeClosedChannel closing(*test.oldHome);
  auto old = test.open(closing, test.link.wait(test.prepare(1, 0)), 0);
  const auto id = old.shipper->getOpenId();

  struct Waiter {
    const kj::Executor& executor;
    kj::Timer& timer;
    kj::Own<kj::PromiseFulfiller<void>> quit;
    bool prepared = false;
    kj::Promise<ThreadedWalLog::PreparedOpen> opening = nullptr;
  };
  auto started = kj::newPromiseAndCrossThreadFulfiller<Waiter*>();
  kj::Thread home([&test, ready = kj::mv(started.fulfiller)]() mutable {
    auto io = kj::setupAsyncIo();
    auto stop = kj::newPromiseAndFulfiller<void>();
    Waiter waiter{kj::getCurrentThreadExecutor(), io.provider->getTimer(), kj::mv(stop.fulfiller)};
    waiter.opening = test.prepare(2, 0)
                         .then([&waiter](auto prepared) {
      waiter.prepared = true;
      return prepared;
    }).eagerlyEvaluate(nullptr);
    KJ_EXPECT(!waiter.opening.poll(io.waitScope));
    ready->fulfill(&waiter);
    stop.promise.wait(io.waitScope);
  });
  auto& waiter = *test.link.wait(kj::mv(started.promise));
  KJ_DEFER({
    closing.beforeClosed = kj::none;
    waiter.executor.executeSync([&] { waiter.quit->fulfill(); });
  });
  closing.beforeClosed = [&] {
    // Run the other home's queued work while CLOSED is held before enqueue.
    // An independent scan drains the scanner queue after any early wakeup.
    const auto prepared = waiter.executor.executeSync([&]() -> kj::Promise<bool> {
      co_await kj::evalLast([] {});
      auto barrier = co_await waiter.timer.timeoutAfter(
          kj::SECONDS, test.prepare(3, 0, StorageSession::OTHER));
      co_await kj::evalLast([] {});
      co_return waiter.prepared;
    });
    KJ_EXPECT(!prepared, "CLOSED must be queued before the replacement is prepared");
  };
  old.close();
  auto prepared = waiter.executor.executeSync(
      [&] { return waiter.timer.timeoutAfter(kj::SECONDS, kj::mv(waiter.opening)); });
  test.link.receive(MSG_CLOSED, id);
  auto reopened = test.open(*test.nextHome, prepared, 0);
}

struct TestRoot {
  kj::Own<kj::Directory> directory = kj::newInMemoryDirectory(kj::nullClock());
  SqliteDatabase::Vfs vfs{*directory};
  SqliteDatabase db{vfs, kj::Path({"root.db"}), kj::WriteMode::CREATE | kj::WriteMode::MODIFY};
  DurableLogShipper shipper;

  TestRoot(HomeSession& test, DurableWalChannel& home, kj::StringPtr name)
      : shipper(*directory,
            kj::Path({"root.db-wal"}),
            kj::Path({"root.db"}),
            {.objectName = kj::str(name), .storageId = kj::str(name)},
            db,
            test.clock,
            home) {
    test.link.receive(MSG_OPEN, id());
  }

  uint64_t id() const {
    return shipper.getOpenId();
  }
};

void acceptOpen(HomeSession& test, DurableWalChannel& home, const TestRoot& root) {
  auto opening = home.waitForOpen(root.id(), test.clock);
  test.link.opened(root.id());
  test.link.wait(kj::mv(opening));
}

kj::Promise<void> sendFrames(
    HomeSession& test, DurableWalChannel& home, TestRoot& root, uint64_t request) {
  // Call Home directly so dropping its return value drops the caller's only branch.
  // The fake peer needs only the type, open ID and request ID to acknowledge a record.
  Writer frame;
  frame.u8(MSG_FRAMES);
  frame.u64(request);
  auto answer = home.sendFrames(root.shipper, frame.finish(root.id()), request, test.clock);
  auto observed = test.link.receive(MSG_FRAMES, root.id());
  KJ_REQUIRE(observed.size() == 17 && getLe64(observed.begin() + 9) == request);
  return answer;
}

KJ_TEST("a full threaded WAL home inbox leaves another home ready and drains in order") {
  HomeSession test;
  auto paused = std::make_shared<Inbox>(1);
  const auto heldId = ++test.shared->nextId;
  KJ_REQUIRE(paused->tryRegister(heldId));
  Writer open;
  open.u8(MSG_OPEN);
  KJ_REQUIRE(
      test.shared->post({.bytes = open.finish(heldId), .openId = heldId, .inbox = paused}, true));
  test.link.receive(MSG_OPEN, heldId);

  auto closed = kj::newPromiseAndFulfiller<uint64_t>();
  auto ready = test.attachHome([&](uint64_t id, CloseReason reason) {
    KJ_EXPECT(reason == CloseReason::DRAIN);
    closed.fulfiller->fulfill(kj::mv(id));
  });
  TestRoot other(test, *ready, "namespace/ready/epoch"_kj);
  acceptOpen(test, *ready, other);

  auto tellSeal = [&](uint32_t salt, uint32_t frame) {
    Writer seal;
    seal.u8(MSG_SEALED);
    seal.u32(salt);
    seal.u32(0);
    seal.u32(frame);
    test.link.reply(seal.finish(heldId));
  };
  auto tellBreak = [&](kj::StringPtr reason) {
    Writer broken;
    broken.u8(MSG_BREAK);
    broken.str(reason);
    test.link.reply(broken.finish(heldId));
  };
  test.link.opened(heldId, ACK_QUOTA);
  tellSeal(1, 12);
  test.link.checkpoint(heldId);
  const std::string heldReason(1024, 'x');
  tellBreak(heldReason.c_str());
  test.link.requestClose(heldId, MSG_CLOSE_ASKED);
  tellSeal(2, 20);
  test.link.requestClose(heldId, MSG_CLOSE_FOR_CODE_UPDATE);

  // Every kind now has a pending slot. Repeated advisory traffic still fits,
  // and a fresh open cannot claim another slot on the paused home.
  tellSeal(2, 18);
  tellBreak("a second report"_kj);
  for (auto i KJ_UNUSED: kj::zeroTo(16)) {
    test.link.checkpoint(heldId);
  }
  const auto freshId = ++test.shared->nextId;
  KJ_EXPECT(!paused->tryRegister(freshId));

  KJ_REQUIRE(other.db.run("PRAGMA journal_mode = WAL").getText(0) == "wal"_kj);
  other.db.run("CREATE TABLE entries (value INTEGER)");
  other.db.run("INSERT INTO entries VALUES (42)");
  auto confirmation = other.shipper.onCommit();
  auto frames = test.link.receive(MSG_FRAMES, other.id());
  KJ_REQUIRE(frames.size() > 17);
  KJ_EXPECT(other.shipper.hasPendingWrites());
  KJ_EXPECT(!confirmation.poll(test.link.io.waitScope));
  test.link.ack(getLe64(frames.begin() + 9));
  test.link.wait(kj::mv(confirmation));
  KJ_EXPECT(!other.shipper.hasPendingWrites());
  KJ_EXPECT(other.db.run("SELECT value FROM entries").getInt(0) == 42);
  test.link.requestClose(other.id(), MSG_CLOSE_ASKED);
  KJ_EXPECT(test.link.wait(kj::mv(closed.promise)) == other.id());
  KJ_REQUIRE(test.shared->negotiated());

  auto take = [&](uint8_t type) {
    auto next = test.link.wait(paused->next());
    auto notice = kj::mv(KJ_ASSERT_NONNULL(next));
    KJ_EXPECT(notice.type == type, notice.type, type);
    KJ_EXPECT(notice.openId == heldId);
    return notice;
  };
  KJ_EXPECT(take(MSG_OPENED).status == ACK_QUOTA);
  take(MSG_CHECKPOINT_ASKED);
  const auto broken = take(MSG_BREAK);
  KJ_EXPECT(broken.reason == kj::StringPtr(heldReason.c_str()).first(MAX_BREAK_REASON_BYTES));
  take(MSG_CLOSE_ASKED);
  auto seal = take(MSG_SEALED);
  KJ_EXPECT(seal.salt1 == 2 && seal.upToFrame == 20);
  take(MSG_CLOSE_FOR_CODE_UPDATE);
  auto noMore = paused->next();
  KJ_EXPECT(!noMore.poll(test.link.io.waitScope));

  // Retiring the local shipper discards queued and late notices for its ID.
  noMore = nullptr;
  paused->push({.type = MSG_CLOSE_ASKED, .openId = heldId});
  paused->unregister(heldId);
  paused->push({.type = MSG_CLOSE_FOR_CODE_UPDATE, .openId = heldId});
  KJ_EXPECT(paused->tryRegister(freshId));
  paused->push({.type = MSG_OPENED, .openId = freshId});
  auto fresh = test.link.wait(paused->next());
  KJ_EXPECT(KJ_ASSERT_NONNULL(fresh).openId == freshId);
}

KJ_TEST("threaded WAL Home publishes its registration limit and closes its inbox") {
  HomeSession test;
  std::atomic<bool> full{false};
  auto inbox = std::make_shared<Inbox>(1);
  auto home = kj::heap<Home>(test.shared, [](uint64_t, CloseReason) {}, test.clock, full, inbox);
  KJ_EXPECT(!full.load());
  uint64_t openId;
  {
    TestRoot root(test, *home, "namespace/capacity/epoch"_kj);
    openId = root.id();
    acceptOpen(test, *home, root);
    KJ_EXPECT(full.load());
    auto refusal = kj::runCatchingExceptions(
        [&] { TestRoot rejected(test, *home, "namespace/refused/epoch"_kj); });
    const auto& error = KJ_ASSERT_NONNULL(refusal);
    KJ_EXPECT(error.getType() == kj::Exception::Type::OVERLOADED);
    KJ_EXPECT(error.getDescription() == HOME_CAPACITY_REFUSAL);
    KJ_EXPECT(error.getDetail(ThreadedWalLog::HOME_CAPACITY_DETAIL) != kj::none);
  }
  test.link.receive(MSG_CLOSED, openId);
  KJ_EXPECT(!full.load());
  home = nullptr;
  KJ_EXPECT(full.load());
  KJ_EXPECT(test.link.wait(inbox->next()) == kj::none);
  KJ_EXPECT(test.shared->negotiated());
}

KJ_TEST("threaded WAL Home returns capacity across more opens than its slot limit") {
  HomeSession test;
  auto home = test.attachHome();
  for (auto i KJ_UNUSED: kj::zeroTo(HOME_OPEN_LIMIT + 1)) {
    uint64_t openId;
    {
      TestRoot root(test, *home, "namespace/retired/epoch"_kj);
      openId = root.id();
      acceptOpen(test, *home, root);
    }
    test.link.receive(MSG_CLOSED, openId);
  }
  KJ_EXPECT(test.shared->negotiated());
}

KJ_TEST("threaded WAL home gives up a pending write at its deadline after caller cancellation") {
  HomeSession test;
  auto home = test.attachHome();
  TestRoot root(test, *home, "namespace/canceled/epoch"_kj);
  acceptOpen(test, *home, root);

  const auto request = home->nextRequest();
  {
    auto caller = sendFrames(test, *home, root, request);
    KJ_EXPECT(!caller.poll(test.link.io.waitScope));
  }
  test.advance(JUST_BEFORE_TIMEOUT);
  root.shipper.reportAlarm(kj::none);
  test.link.receive(MSG_ALARM, root.id());

  test.advance(kj::MILLISECONDS);
  test.link.expectGiveUp(root.id(), request, root.shipper.getKey());
  root.shipper.reportAlarm(kj::none);
  test.link.receive(MSG_ALARM, root.id());
}

KJ_TEST("threaded WAL starts confirmation after synchronous work with a stale home timer") {
  HomeSession test;
  kj::TimerImpl precise{test.clock.now()};
  Home home(test.shared, [](uint64_t, CloseReason) { KJ_FAIL_EXPECT("unexpected close request"); },
      precise);
  TestRoot root(test, home, "namespace/after-sql/epoch"_kj);
  acceptOpen(test, home, root);

  // Model a turn that ran SQLite without polling the event port.
  const auto synchronousTime = 30 * kj::SECONDS;
  precise.advanceTo(precise.now() + synchronousTime);
  const auto request = home.nextRequest();
  auto caller = sendFrames(test, home, root, request);
  test.advance(synchronousTime + JUST_BEFORE_TIMEOUT);
  KJ_EXPECT(!caller.poll(test.link.io.waitScope));
  test.link.ack(request);
  test.link.wait(kj::mv(caller));
}

KJ_TEST("threaded WAL home suppresses a timed-out give-up when the session receives a late ACK") {
  HomeSession test(true);
  auto close = kj::newPromiseAndFulfiller<void>();
  auto home = test.attachHome([&](uint64_t, CloseReason) { close.fulfiller->fulfill(); });
  TestRoot root(test, *home, "namespace/late-ack/epoch"_kj);
  acceptOpen(test, *home, root);

  const auto request = home->nextRequest();
  auto caller = sendFrames(test, *home, root, request);
  test.advance(JUST_BEFORE_TIMEOUT);
  // Put a larger record ahead of the give-up so the writer cannot reach it while the peer
  // leaves that record unread. The reader remains free to consume the incoming ACK.
  KJ_REQUIRE(root.db.run("PRAGMA journal_mode = WAL").getText(0) == "wal"_kj);
  root.db.run("CREATE TABLE entries (data BLOB)");
  root.db.run("INSERT INTO entries VALUES (zeroblob(262144));");
  auto bulkConfirmation = root.shipper.onCommit();
  test.advance(kj::MILLISECONDS);
  KJ_EXPECT_THROW_MESSAGE("the node did not confirm the write", test.link.wait(kj::mv(caller)));
  test.link.ack(request);
  // On the same inbound stream, this notification follows the ACK that removed the request.
  test.link.requestClose(root.id(), MSG_CLOSE_ASKED);
  test.link.wait(kj::mv(close.promise));

  root.shipper.reportAlarm(kj::none);
  auto bulk = test.link.receive(MSG_FRAMES, root.id());
  KJ_REQUIRE(bulk.size() > 262144);
  test.link.receive(MSG_ALARM, root.id());
  test.link.ack(getLe64(bulk.begin() + 9));
  test.link.wait(kj::mv(bulkConfirmation));
}

KJ_TEST("threaded WAL home refuses frames at capacity and sends an unconditional give-up") {
  HomeSession test;
  auto home = test.attachHome();
  TestRoot root(test, *home, "namespace/full/epoch"_kj);
  acceptOpen(test, *home, root);

  // The mailbox's byte accounting is covered above. Simulate an occupied data allowance
  // without allocating and transferring 72 MiB, and keep the real control allowance available.
  Writer marker;
  marker.u8(MSG_ALARM);
  KJ_REQUIRE(test.shared->commands.push({.bytes = marker.finish(root.id()), .openId = root.id()},
      QUEUED_RECORD_BYTES, QUEUED_RECORD_BYTES));
  KJ_REQUIRE(root.db.run("PRAGMA journal_mode = WAL").getText(0) == "wal"_kj);
  root.db.run("CREATE TABLE entries (data BLOB)");
  auto refused = root.shipper.onCommit();
  KJ_EXPECT(!root.shipper.hasPendingWrites());
  KJ_EXPECT_THROW_MESSAGE("STORAGE_UNAVAILABLE", test.link.wait(kj::mv(refused)));

  test.link.receive(MSG_ALARM, root.id());
  // OPEN consumed the first ID. The refused commit consumed the next, without sending FRAMES.
  test.link.expectGiveUp(root.id(), root.id() + 1, root.shipper.getKey());
  TestRoot fresh(test, *home, "namespace/fresh/epoch"_kj);
  acceptOpen(test, *home, fresh);
}

KJ_TEST("threaded WAL home distinguishes open denial from its confirmation deadline") {
  HomeSession test;
  auto home = test.attachHome();
  TestRoot denied(test, *home, "namespace/denied/epoch"_kj);
  TestRoot unanswered(test, *home, "namespace/unanswered/epoch"_kj);
  auto denial = home->waitForOpen(denied.id(), test.clock);
  auto timeout = home->waitForOpen(unanswered.id(), test.clock);
  KJ_EXPECT(!denial.poll(test.link.io.waitScope));

  test.link.opened(denied.id(), ACK_QUOTA);
  KJ_EXPECT_THROW_MESSAGE("QUOTA_EXCEEDED", test.link.wait(kj::mv(denial)));
  test.advance(JUST_BEFORE_TIMEOUT);
  KJ_EXPECT(!timeout.poll(test.link.io.waitScope));
  test.advance(kj::MILLISECONDS);
  KJ_EXPECT(timeout.poll(test.link.io.waitScope));
  KJ_EXPECT_THROW_MESSAGE("STORAGE_UNAVAILABLE", test.link.wait(kj::mv(timeout)));
}

KJ_TEST("threaded WAL home delivers both close reasons to the intended root") {
  HomeSession test;
  struct Notice {
    uint64_t id;
    CloseReason reason;
  };
  kj::Vector<Notice> notices;
  auto delivered = kj::newPromiseAndFulfiller<void>();
  auto home = test.attachHome([&](uint64_t id, CloseReason reason) {
    notices.add(Notice{id, reason});
    if (notices.size() == 2) delivered.fulfiller->fulfill();
  });
  auto otherHome = test.attachHome();
  TestRoot first(test, *home, "namespace/first/epoch"_kj);
  TestRoot second(test, *home, "namespace/second/epoch"_kj);
  TestRoot other(test, *otherHome, "namespace/other-home/epoch"_kj);
  acceptOpen(test, *home, first);
  acceptOpen(test, *home, second);
  acceptOpen(test, *otherHome, other);

  test.link.requestClose(second.id(), MSG_CLOSE_ASKED);
  test.link.requestClose(first.id(), MSG_CLOSE_FOR_CODE_UPDATE);
  test.link.wait(kj::mv(delivered.promise));
  test.link.io.waitScope.poll();
  KJ_REQUIRE(notices.size() == 2);
  KJ_EXPECT(notices[0].id == second.id() && notices[0].reason == CloseReason::DRAIN);
  KJ_EXPECT(notices[1].id == first.id() && notices[1].reason == CloseReason::CODE_UPDATED);
}

}  // namespace
}  // namespace workerd::server
