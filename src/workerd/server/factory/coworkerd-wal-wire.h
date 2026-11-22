// The private child-to-node WAL wire. The peer and persisted encodings are independent of this channel.
#pragma once

#include <kj/async-io.h>
#include <kj/debug.h>
#include <kj/function.h>
#include <kj/vector.h>

namespace workerd::server::wal {

inline uint32_t getLe32(const kj::byte* p) {
  return uint32_t(p[0]) | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) | (uint32_t(p[3]) << 24);
}
inline uint64_t getLe64(const kj::byte* p) {
  return uint64_t(getLe32(p)) | (uint64_t(getLe32(p + 4)) << 32);
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
  // The threaded revision prefixes per-database payloads with the open ID.
  // A zero ID emits the original private wire format.
  kj::Array<kj::byte> finish(uint64_t openId = 0) {
    auto body = out.releaseAsArray();
    const size_t prefix = openId ? 8 : 0;
    uint32_t n = body.size() + prefix;
    auto result = kj::heapArray<kj::byte>(n + 4);
    for (int i = 0; i < 4; i++) result[i] = kj::byte(n >> (8 * i));
    result[4] = body[0];
    if (openId) {
      for (int i = 0; i < 8; ++i) result[5 + i] = kj::byte(openId >> (8 * i));
    }
    memcpy(result.begin() + 5 + prefix, body.begin() + 1, body.size() - 1);
    return result;
  }
};

// To the replicator.
constexpr uint8_t MSG_HELLO = 1, MSG_FRAMES = 2, MSG_OPEN = 3, MSG_CHECKPOINT = 4, MSG_RESET = 5,
                  MSG_DELETED = 6, MSG_CLOSED = 7, MSG_GAVE_UP = 8, MSG_ALARM = 9, MSG_RESIDENCY = 10;
// From the replicator.
constexpr uint8_t MSG_ACK = 1, MSG_SEALED = 2, MSG_BREAK = 3, MSG_CHECKPOINT_ASKED = 4;
constexpr uint8_t MSG_READY = 5, MSG_CLOSE_ASKED = 6;
constexpr uint32_t THREADED_WAL_REVISION = 1;
// An ACK's status. A refusal carries the code the caller sees: the node could not make the write durable, the
// object is past its own size limit, or the tenant's Durable Object storage quota is exhausted. Reads are not
// refused for any of them -- only the write is.
constexpr uint8_t ACK_OK = 0, ACK_TOO_LARGE = 3, ACK_QUOTA = 4;

inline kj::StringPtr ackRefusal(uint8_t status) {
  switch (status) {
    case ACK_TOO_LARGE:
      return "jsg.Error: Durable Object is too large: the write would take it past "
             "durableObjects.objectBytes (DO_OBJECT_TOO_LARGE)"_kj;
    case ACK_QUOTA:
      return "jsg.Error: the tenant's Durable Object storage quota is exhausted: the write was refused "
             "(QUOTA_EXCEEDED)"_kj;
    default:
      return "jsg.Error: Durable Object storage is unavailable: the node refused the write "
             "(STORAGE_UNAVAILABLE)"_kj;
  }
}

class WalWire {
 public:
  WalWire(kj::LowLevelAsyncIoProvider& provider, int fd)
      : stream(provider.wrapSocketFd(static_cast<kj::LowLevelAsyncIoProvider::Fd>(fd),
            kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
                kj::LowLevelAsyncIoProvider::ALREADY_CLOEXEC)) {}

  kj::Promise<void> write(kj::Array<kj::byte> bytes) {
    return stream->write(bytes.asPtr()).attach(kj::mv(bytes));
  }

  kj::Promise<void> read(kj::Function<void(kj::ArrayPtr<const kj::byte>)> dispatch) {
    for (;;) {
      const size_t n = co_await stream->tryRead(readBuffer, 1, sizeof(readBuffer));
      if (n == 0) {
        kj::throwRecoverableException(
            KJ_EXCEPTION(DISCONNECTED, "the node's replicator closed the Durable Object log channel"));
      }
      inbox.addAll(kj::arrayPtr(readBuffer, n));
      size_t at = 0;
      while (inbox.size() - at >= 4) {
        const uint32_t length = getLe32(inbox.begin() + at);
        // The node sends only private control messages. At most two u16-length strings
        // and a fixed-size header can occur in any one reply.
        KJ_REQUIRE(length <= 2 * UINT16_MAX + 64, "invalid Durable Object log reply size");
        if (inbox.size() - at - 4 < length) break;
        dispatch(kj::arrayPtr(inbox.begin() + at + 4, length));
        at += 4 + length;
      }
      if (at > 0) {
        kj::Vector<kj::byte> rest;
        rest.addAll(kj::arrayPtr(inbox.begin() + at, inbox.size() - at));
        inbox = kj::mv(rest);
      }
    }
  }

 private:
  kj::Own<kj::AsyncIoStream> stream;
  kj::Vector<kj::byte> inbox;
  kj::byte readBuffer[65536];
};

}  // namespace workerd::server::wal
