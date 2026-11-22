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

  WirePair() {
    int fds[2];
    KJ_SYSCALL(socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    KJ_SYSCALL(fcntl(fds[0], F_SETFD, FD_CLOEXEC));
    child = kj::heap<WalWire>(*io.lowLevelProvider, fds[0]);
    node = io.lowLevelProvider->wrapSocketFd(fds[1], kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
  }

  kj::Array<kj::byte> receive() {
    kj::byte header[4];
    auto& timer = io.provider->getTimer();
    timer.timeoutAfter(kj::SECONDS, node->read(kj::arrayPtr(header, sizeof(header)))).wait(io.waitScope);
    auto body = kj::heapArray<kj::byte>(getLe32(header));
    timer.timeoutAfter(kj::SECONDS, node->read(body.asPtr())).wait(io.waitScope);
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

}  // namespace
}  // namespace workerd::server
