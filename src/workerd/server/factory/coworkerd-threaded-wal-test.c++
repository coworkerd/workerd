#include "coworkerd-threaded-wal.c++"

#include <kj/test.h>

#include <fcntl.h>
#include <sys/socket.h>

namespace workerd::server {
namespace {

// This half is the private node protocol, driven one reply at a time by the test.
struct WirePair {
  kj::AsyncIoContext io = kj::setupAsyncIo();
  kj::Own<kj::AsyncIoStream> node;
  kj::Own<WalWire> child;

  explicit WirePair(bool limitSendBuffer = false) {
    int fds[2];
    KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    if (limitSendBuffer) {
      int sendBuffer = 4096;
      KJ_SYSCALL(setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &sendBuffer, sizeof(sendBuffer)));
    }
    KJ_SYSCALL(fcntl(fds[0], F_SETFD, FD_CLOEXEC));
    child = kj::heap<WalWire>(*io.lowLevelProvider, fds[0]);
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
  auto checkpoint = [&](uint64_t openId) {
    Writer reply;
    reply.u8(MSG_CHECKPOINT_ASKED);
    link.reply(reply.finish(openId));
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
  checkpoint(1);
  checkpoint(5);
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
    return kj::heap<Home>(shared, kj::mv(onClose));
  }

  void advance(kj::Duration duration) {
    clock.advanceTo(clock.now() + duration);
    link.io.waitScope.poll();
  }
};

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
