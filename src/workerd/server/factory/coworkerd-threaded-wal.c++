#include "coworkerd-threaded-wal.h"

#include "coworkerd-wal-wire.h"

#include <unistd.h>

#include <kj/thread.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace workerd::server {
namespace {

using namespace wal;

// The node caps a record at its 64 MiB RPC traversal bound, including when the
// WAL has one large transaction. Leave room for its private wire header.
constexpr size_t QUEUED_RECORD_BYTES = 72u << 20;
// Reserve room for open/close and give-up commands when the data queue is full.
constexpr size_t QUEUED_CONTROL_BYTES = 1u << 20;
constexpr size_t HOME_OPEN_LIMIT = 4096;
constexpr size_t MAX_BREAK_REASON_BYTES = 256;
constexpr uint8_t ACK_TIMEOUT = 255;
constexpr uint8_t ACK_DISCONNECTED = 254;
constexpr auto STORAGE_UNAVAILABLE =
    "jsg.Error: Durable Object storage is unavailable: the node's replicator is not reachable "
    "(STORAGE_UNAVAILABLE)"_kj;
constexpr auto HOME_CAPACITY_REFUSAL =
    "jsg.Error: Durable Object storage is unavailable: its Home has no notification capacity "
    "(STORAGE_UNAVAILABLE)"_kj;
constexpr auto CLOSE_UNCONFIRMED =
    "the previous SQLite connection did not confirm close in this generation"_kj;

// The producer transfers owned data and wakes the consumer without entering its
// loop. A queue has one consumer. Its mutex is released before any I/O or await.
template <typename T>
class Mailbox {
 public:
  bool push(T item, size_t cost, size_t capacity) {
    kj::Own<kj::CrossThreadPromiseFulfiller<void>> wake;
    {
      std::lock_guard lock(mutex);
      if (closed || cost > capacity || used > capacity - cost) return false;
      items.push_back({kj::mv(item), cost});
      used += cost;
      wake = kj::mv(waiting);
    }
    if (wake.get() != nullptr) wake->fulfill();
    return true;
  }

  kj::Promise<kj::Maybe<T>> next() {
    for (;;) {
      kj::Promise<void> signal = nullptr;
      {
        std::lock_guard lock(mutex);
        if (closed) co_return kj::none;
        if (!items.empty()) {
          auto entry = kj::mv(items.front());
          items.pop_front();
          used -= entry.cost;
          co_return kj::mv(entry.item);
        }
        auto paf = kj::newPromiseAndCrossThreadFulfiller<void>();
        signal = kj::mv(paf.promise);
        waiting = kj::mv(paf.fulfiller);
      }
      co_await signal;
    }
  }

  void close() {
    kj::Own<kj::CrossThreadPromiseFulfiller<void>> wake;
    std::deque<Entry> discarded;
    {
      std::lock_guard lock(mutex);
      closed = true;
      used = 0;
      items.swap(discarded);
      wake = kj::mv(waiting);
    }
    if (wake.get() != nullptr) wake->fulfill();
  }

 private:
  struct Entry {
    T item;
    size_t cost;
  };
  std::mutex mutex;
  std::deque<Entry> items;
  size_t used = 0;
  bool closed = false;
  kj::Own<kj::CrossThreadPromiseFulfiller<void>> waiting;
};

struct Notification {
  uint8_t type = 0;
  uint8_t status = 0;
  uint64_t openId = 0;
  uint32_t salt1 = 0, salt2 = 0, upToFrame = 0;
  kj::String reason;
};

class Inbox {
 public:
  explicit Inbox(size_t openLimit = HOME_OPEN_LIMIT): openLimit(openLimit) {}

  bool tryRegister(uint64_t openId) {
    std::lock_guard lock(mutex);
    if (closed || opens.size() == openLimit) return false;
    KJ_ASSERT(opens.try_emplace(openId).second);
    return true;
  }

  bool atCapacity() {
    std::lock_guard lock(mutex);
    return closed || opens.size() == openLimit;
  }

  void unregister(uint64_t openId) {
    std::lock_guard lock(mutex);
    auto entry = opens.find(openId);
    if (entry == opens.end()) return;
    for (auto& slot: entry->second) {
      KJ_IF_SOME(notice, slot) {
        notices.erase(notice);
      }
    }
    opens.erase(entry);
  }

  void push(Notification notice) {
    std::lock_guard lock(mutex);
    auto entry = opens.find(notice.openId);
    if (entry == opens.end()) return;
    auto& slot = entry->second[notice.type];
    KJ_IF_SOME(queued, slot) {
      // There is one open result and one request for each close reason. The
      // shipper keeps the first break reason and latches a checkpoint request.
      if (notice.type != MSG_SEALED) return;
      if (queued->salt1 == notice.salt1 && queued->salt2 == notice.salt2 &&
          queued->upToFrame >= notice.upToFrame) {
        return;
      }
      // A seal is advisory. Keep its newest position and let earlier controls
      // run first. The shipper checks the salts against its live WAL on delivery.
      *queued = kj::mv(notice);
      notices.splice(notices.end(), notices, queued);
      return;
    }
    const bool wasEmpty = notices.empty();
    slot = notices.insert(notices.end(), kj::mv(notice));
    // Use the bounded mailbox as a wake latch. It may hold an earlier wake
    // when unregistering a shipper clears the last notification.
    if (wasEmpty) wake.push(true, 1, 1);
  }

  kj::Promise<kj::Maybe<Notification>> next() {
    for (;;) {
      {
        std::lock_guard lock(mutex);
        if (closed) co_return kj::none;
        if (!notices.empty()) {
          auto& notice = notices.front();
          opens.at(notice.openId)[notice.type] = kj::none;
          auto result = kj::mv(notice);
          notices.pop_front();
          co_return kj::mv(result);
        }
      }
      if (co_await wake.next() == kj::none) co_return kj::none;
    }
  }

  void close() {
    std::lock_guard lock(mutex);
    closed = true;
    opens.clear();
    notices.clear();
    wake.close();
  }

 private:
  using Notices = std::list<Notification>;
  // Each live registration reserves one slot per routed message kind. Only
  // BREAK owns variable-length data, bounded by MAX_BREAK_REASON_BYTES.
  using Slots = std::array<kj::Maybe<Notices::iterator>, MSG_CLOSE_FOR_CODE_UPDATE + 1>;
  const size_t openLimit;
  std::mutex mutex;
  bool closed = false;
  Notices notices;
  std::map<uint64_t, Slots> opens;
  Mailbox<bool> wake;
};

struct Command {
  kj::Array<kj::byte> bytes;
  uint64_t openId;
  uint64_t request = 0;
  bool unconditional = false;
  kj::Own<kj::CrossThreadPromiseFulfiller<uint8_t>> ack;
  std::shared_ptr<Inbox> inbox;
};

struct Shared {
  explicit Shared(kj::Duration confirmationTimeout): confirmationTimeout(confirmationTimeout) {}

  void stop() {
    if (stopped.exchange(true, std::memory_order_acq_rel)) return;
    ready.store(false, std::memory_order_release);
    commands.close();
    stopSignal.close();
    std::lock_guard lock(openMutex);
    for (auto& [key, storage]: openStorage) wakePreparations(storage);
  }

  bool post(Command command, bool control) {
    const auto size = command.bytes.size();
    const auto capacity = QUEUED_RECORD_BYTES + (control ? QUEUED_CONTROL_BYTES : 0);
    return commands.push(kj::mv(command), size, capacity);
  }

  bool negotiated() const {
    return !stopped.load(std::memory_order_acquire) && ready.load(std::memory_order_acquire);
  }

  void checkReservedOpen(const std::string& key) {
    std::lock_guard lock(openMutex);
    KJ_REQUIRE(negotiated(), STORAGE_UNAVAILABLE);
    auto storage = openStorage.find(key);
    KJ_ASSERT(storage != openStorage.end());
    requireConfirmedClose(storage->second);
  }

  bool reserveOpen(const std::string& key,
      uint64_t tree,
      uint facet,
      kj::Own<kj::CrossThreadPromiseFulfiller<void>> changed) {
    std::lock_guard lock(openMutex);
    KJ_REQUIRE(negotiated(), STORAGE_UNAVAILABLE);
    auto& storage = openStorage.try_emplace(key, tree).first->second;
    requireConfirmedClose(storage);
    if (storage.tree == tree && storage.facets.insert(facet).second) return true;
    // Coalesced root requests and the facet startup each have one shared
    // preparation. Drop canceled waiters as later requests replace them.
    std::erase_if(storage.waiters, [](auto& waiter) { return !waiter->isWaiting(); });
    storage.waiters.push_back(kj::mv(changed));
    return false;
  }

  void finishOpen(
      const std::string& key, uint64_t tree, uint facet, SqliteDatabase::CloseOutcome outcome) {
    std::lock_guard lock(openMutex);
    auto storage = openStorage.find(key);
    KJ_ASSERT(storage != openStorage.end() && storage->second.tree == tree);
    KJ_ASSERT(storage->second.facets.erase(facet) == 1);
    if (outcome == SqliteDatabase::CloseOutcome::UNCONFIRMED &&
        !std::exchange(storage->second.closeUnconfirmed, true)) {
      unconfirmedCloseIdentities.fetch_add(1, std::memory_order_relaxed);
    }
    wakePreparations(storage->second);
    if (storage->second.facets.empty() && !storage->second.closeUnconfirmed) {
      openStorage.erase(storage);
    }
  }

  const kj::Duration confirmationTimeout;
  Mailbox<Command> commands;
  Mailbox<bool> stopSignal;
  std::atomic<uint64_t> nextId{0};
  std::atomic<bool> ready{false};
  std::atomic<bool> stopped{false};
  std::atomic<uint64_t> unconfirmedCloseIdentities{0};

 private:
  struct OpenStorage {
    uint64_t tree;
    bool closeUnconfirmed = false;
    std::set<uint> facets;
    std::vector<kj::Own<kj::CrossThreadPromiseFulfiller<void>>> waiters;
  };
  std::mutex openMutex;
  std::map<std::string, OpenStorage> openStorage;

  static void requireConfirmedClose(const OpenStorage& storage) {
    if (storage.closeUnconfirmed) {
      auto error = KJ_EXCEPTION(OVERLOADED, CLOSE_UNCONFIRMED);
      error.setDetail(ThreadedWalLog::CLOSE_UNCONFIRMED_DETAIL, kj::heapArray<kj::byte>(0));
      kj::throwRecoverableException(kj::mv(error));
    }
  }

  static void wakePreparations(OpenStorage& storage) {
    for (auto& waiter: storage.waiters) waiter->fulfill();
    storage.waiters.clear();
  }
};

class Session {
 public:
  Session(Shared& shared, WalWire& wire, uint64_t generation)
      : shared(shared), wire(wire), generation(generation) {}

  ~Session() noexcept(false) {
    shared.stop();
    for (auto& [request, fulfiller]: pending) fulfiller->fulfill(uint8_t(ACK_DISCONNECTED));
    for (auto& [openId, inbox]: routes) inbox->close();
  }

  kj::Promise<void> run() {
    return wire.read([this](auto bytes) { receive(bytes); })
        .exclusiveJoin(write())
        .exclusiveJoin(shared.stopSignal.next().ignoreResult());
  }

 private:
  Shared& shared;
  WalWire& wire;
  uint64_t generation;
  std::map<uint64_t, std::shared_ptr<Inbox>> routes;
  std::map<uint64_t, kj::Own<kj::CrossThreadPromiseFulfiller<uint8_t>>> pending;

  kj::Promise<void> write() {
    Writer hello;
    hello.u8(MSG_HELLO);
    hello.u32(getpid());
    hello.u64(std::chrono::steady_clock::now().time_since_epoch() / std::chrono::nanoseconds(1));
    hello.u32(THREADED_WAL_REVISION);
    hello.u64(generation);
    co_await wire.write(hello.finish());
    for (;;) {
      auto next = co_await shared.commands.next();
      if (next == kj::none) co_return;
      auto command = kj::mv(KJ_ASSERT_NONNULL(next));
      const auto type = command.bytes[4];
      if (type == MSG_GAVE_UP) {
        const auto waiting = pending.erase(command.request);
        if (!waiting && !command.unconditional) continue;
      }
      if (type == MSG_OPEN) routes.emplace(command.openId, kj::mv(command.inbox));
      if (command.ack.get() != nullptr) pending.emplace(command.request, kj::mv(command.ack));
      co_await wire.write(kj::mv(command.bytes));
      // Only the SQLite close path posts CLOSED. Unregistering a shipper just
      // releases its Home inbox slots. The command FIFO keeps CLOSED before OPEN.
      if (type == MSG_CLOSED) routes.erase(command.openId);
    }
  }

  void receive(kj::ArrayPtr<const kj::byte> reply) {
    KJ_REQUIRE(reply.size() >= 1, "empty threaded WAL reply");
    const auto type = reply[0];
    if (!shared.negotiated()) {
      KJ_REQUIRE(type == MSG_READY && reply.size() == 13 &&
              getLe32(reply.begin() + 1) == THREADED_WAL_REVISION &&
              getLe64(reply.begin() + 5) == generation,
          "threaded WAL readiness mismatch");
      shared.ready.store(true, std::memory_order_release);
      return;
    }
    if (type == MSG_ACK) {
      KJ_REQUIRE(reply.size() == 10, "invalid threaded WAL acknowledgement");
      const auto request = getLe64(reply.begin() + 1);
      auto entry = pending.find(request);
      if (entry == pending.end()) return;
      entry->second->fulfill(uint8_t(reply[9]));
      pending.erase(entry);
      return;
    }
    KJ_REQUIRE(reply.size() >= 9, "missing threaded WAL open ID");
    const auto openId = getLe64(reply.begin() + 1);
    const auto entry = routes.find(openId);
    if (entry == routes.end()) return;
    const auto payload = reply.slice(9);
    Notification notice{.type = type, .openId = openId};
    switch (type) {
      case MSG_SEALED:
        KJ_REQUIRE(payload.size() == 12, "invalid threaded WAL seal");
        notice.salt1 = getLe32(payload.begin());
        notice.salt2 = getLe32(payload.begin() + 4);
        notice.upToFrame = getLe32(payload.begin() + 8);
        break;
      case MSG_BREAK: {
        KJ_REQUIRE(payload.size() >= 2, "invalid threaded WAL break");
        const size_t length = size_t(payload[0]) | (size_t(payload[1]) << 8);
        KJ_REQUIRE(payload.size() == length + 2, "invalid threaded WAL break length");
        notice.reason = kj::str(kj::arrayPtr(reinterpret_cast<const char*>(payload.begin() + 2),
            kj::min(length, MAX_BREAK_REASON_BYTES)));
        break;
      }
      case MSG_CHECKPOINT_ASKED:
        KJ_REQUIRE(payload.size() == 0, "invalid threaded WAL checkpoint");
        break;
      case MSG_CLOSE_ASKED:
      case MSG_CLOSE_FOR_CODE_UPDATE:
        KJ_REQUIRE(payload.size() == 0, "invalid threaded WAL close request");
        break;
      case MSG_OPENED:
        KJ_REQUIRE(payload.size() == 1, "invalid threaded WAL open result");
        notice.status = payload[0];
        break;
      default:
        KJ_FAIL_REQUIRE("unknown threaded WAL reply", type);
    }
    entry->second->push(kj::mv(notice));
  }
};

class Home final: public DurableWalChannel, private kj::TaskSet::ErrorHandler {
 public:
  Home(std::shared_ptr<Shared> shared,
      kj::Function<void(uint64_t, ThreadedWalLog::CloseReason)> requestClose,
      const kj::MonotonicClock& clock,
      kj::Maybe<std::atomic<bool>&> walFull = kj::none,
      std::shared_ptr<Inbox> inbox = std::make_shared<Inbox>())
      : shared(kj::mv(shared)),
        clock(clock),
        inbox(kj::mv(inbox)),
        walFull(walFull),
        requestClose(kj::mv(requestClose)),
        tasks(*this) {
    publishCapacity();
    tasks.add(read());
  }

  ~Home() noexcept(false) override {
    inbox->close();
    publishCapacity();
  }

  bool enabled() const override { return true; }
  uint64_t nextRequest() override { return ++shared->nextId; }

  uint64_t registerShipper(DurableLogShipper& shipper) override {
    if (!shared->negotiated()) {
      kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str(STORAGE_UNAVAILABLE)));
    }
    const auto openId = nextRequest();
    auto result = kj::newPromiseAndFulfiller<void>();
    shippers.emplace(openId, Registration{&shipper, kj::mv(result.fulfiller), result.promise.fork()});
    if (!inbox->tryRegister(openId)) {
      shippers.erase(openId);
      if (!shared->negotiated()) {
        kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str(STORAGE_UNAVAILABLE)));
      }
      auto error = KJ_EXCEPTION(OVERLOADED, kj::str(HOME_CAPACITY_REFUSAL));
      error.setDetail(ThreadedWalLog::HOME_CAPACITY_DETAIL, kj::heapArray<kj::byte>(0));
      kj::throwRecoverableException(kj::mv(error));
    }
    publishCapacity();
    return openId;
  }

  void unregisterShipper(DurableLogShipper& shipper) override {
    const auto openId = shipper.getOpenId();
    inbox->unregister(openId);
    shippers.erase(openId);
    publishCapacity();
  }

  kj::Promise<void> waitForOpen(uint64_t openId, kj::Timer& timer) override {
    const auto found = shippers.find(openId);
    if (!shared->negotiated() || found == shippers.end()) {
      kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str(STORAGE_UNAVAILABLE)));
    }
    auto timeout = timer.afterDelay(shared->confirmationTimeout).then([] {
      kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str(STORAGE_UNAVAILABLE)));
    });
    return found->second.ready.addBranch().exclusiveJoin(kj::mv(timeout));
  }

  void send(uint64_t openId, kj::Array<kj::byte> message) override {
    const auto type = message[4];
    if (!shared->post({.bytes = kj::mv(message), .openId = openId, .inbox = inbox}, true)) {
      // Losing a control record could leave a dirty image registered. Fail the whole
      // channel so the node uses generation recovery.
      shared->stop();
      if (type == MSG_OPEN) kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str(STORAGE_UNAVAILABLE)));
    }
  }

  kj::Promise<void> sendFrames(DurableLogShipper& shipper,
      kj::Array<kj::byte> message, uint64_t request, kj::Timer& timer) override {
    const auto openId = shipper.getOpenId();
    auto paf = kj::newPromiseAndCrossThreadFulfiller<uint8_t>();
    if (!shared->post({.bytes = kj::mv(message), .openId = openId,
          .request = request, .ack = kj::mv(paf.fulfiller)}, false)) {
      gaveUp(openId, request, shipper.getKey(), true);
      shipper.onRefused(request);
      return kj::Promise<void>(KJ_EXCEPTION(FAILED, kj::str(STORAGE_UNAVAILABLE)));
    }
    auto ack = waitForAck(openId, request, kj::str(shipper.getKey()), timer, kj::mv(paf.promise)).fork();
    // A canceled caller releases its branch. The committed transaction still needs
    // a definite acknowledgement or the give-up record, independent of that caller.
    tasks.add(ack.addBranch().catch_([](kj::Exception&&) {}));
    return ack.addBranch();
  }

 private:
  struct Registration {
    DurableLogShipper* shipper;
    kj::Own<kj::PromiseFulfiller<void>> opening;
    kj::ForkedPromise<void> ready;
  };
  std::shared_ptr<Shared> shared;
  const kj::MonotonicClock& clock;
  std::shared_ptr<Inbox> inbox;
  kj::Maybe<std::atomic<bool>&> walFull;
  std::map<uint64_t, Registration> shippers;
  kj::Function<void(uint64_t, ThreadedWalLog::CloseReason)> requestClose;
  kj::TaskSet tasks;

  void publishCapacity() {
    KJ_IF_SOME(full, walFull) {
      full.store(inbox->atCapacity(), std::memory_order_relaxed);
    }
  }

  void taskFailed(kj::Exception&&) override { shared->stop(); }

  void gaveUp(uint64_t openId, uint64_t request, kj::StringPtr key, bool unsent) {
    Writer writer;
    writer.u8(MSG_GAVE_UP);
    writer.u64(request);
    writer.str(key);
    writer.u64(std::chrono::steady_clock::now().time_since_epoch() / std::chrono::nanoseconds(1));
    if (!shared->post({.bytes = writer.finish(openId), .openId = openId,
            .request = request, .unconditional = unsent}, true)) {
      shared->stop();
    }
  }

  kj::Promise<void> waitForAck(uint64_t openId, uint64_t request, kj::String key,
      kj::Timer& timer, kj::Promise<uint8_t> answer) {
    // The home timer can still hold its pre-JavaScript time after synchronous SQL.
    const auto deadline = kj::max(clock.now(), timer.now()) + shared->confirmationTimeout;
    const auto status = co_await answer.catch_([](kj::Exception&&) {
      return uint8_t(ACK_DISCONNECTED);
    }).exclusiveJoin(timer.atTime(deadline).then([] { return uint8_t(ACK_TIMEOUT); }));
    const auto found = shippers.find(openId);
    if (status != ACK_OK && found != shippers.end()) found->second.shipper->onRefused(request);
    if (status == ACK_TIMEOUT) {
      gaveUp(openId, request, key, false);
      kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str(
          "jsg.Error: Durable Object storage is unavailable: the node did not confirm the write "
          "(STORAGE_UNAVAILABLE)")));
    }
    if (status == ACK_DISCONNECTED) {
      kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str(STORAGE_UNAVAILABLE)));
    }
    if (status != ACK_OK) {
      kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str(ackRefusal(status))));
    }
    if (found != shippers.end()) found->second.shipper->onAcked(request);
  }

  kj::Promise<void> read() {
    for (;;) {
      auto next = co_await inbox->next();
      if (next == kj::none) {
        for (auto& [id, registration]: shippers) {
          if (registration.opening->isWaiting()) {
            registration.opening->reject(KJ_EXCEPTION(DISCONNECTED, kj::str(STORAGE_UNAVAILABLE)));
          }
        }
        co_return;
      }
      auto& notice = KJ_ASSERT_NONNULL(next);
      const auto found = shippers.find(notice.openId);
      if (found == shippers.end()) continue;
      auto& registration = found->second;
      auto& shipper = *registration.shipper;
      switch (notice.type) {
        case MSG_SEALED: shipper.onSealed(notice.salt1, notice.salt2, notice.upToFrame); break;
        case MSG_BREAK: shipper.onBreak(notice.reason); break;
        case MSG_CHECKPOINT_ASKED: shipper.onCheckpointAsked(); break;
        case MSG_CLOSE_ASKED:
          requestClose(notice.openId, ThreadedWalLog::CloseReason::DRAIN);
          break;
        case MSG_CLOSE_FOR_CODE_UPDATE:
          requestClose(notice.openId, ThreadedWalLog::CloseReason::CODE_UPDATED);
          break;
        case MSG_OPENED:
          if (!registration.opening->isWaiting()) break;
          if (notice.status == ACK_OK) registration.opening->fulfill();
          else registration.opening->reject(KJ_EXCEPTION(FAILED, kj::str(ackRefusal(notice.status))));
          break;
        default: KJ_UNREACHABLE;
      }
    }
  }
};

}  // namespace

struct ThreadedWalLog::OpenReservation {
  std::shared_ptr<Shared> shared;
  std::string key;
  uint64_t tree;
  uint facet;
  enum class State { WAITING, RESERVED, OPENED };
  State state = State::WAITING;

  OpenReservation(std::shared_ptr<Shared> shared, kj::StringPtr key, uint64_t tree, uint facet)
      : shared(std::move(shared)),
        key(key.cStr(), key.size()),
        tree(tree),
        facet(facet) {}

  ~OpenReservation() {
    // Dropping a preparation that never reached SQLite needs no close proof.
    if (state == State::RESERVED) {
      shared->finishOpen(key, tree, facet, SqliteDatabase::CloseOutcome::CONFIRMED);
    }
  }

  void closed(SqliteDatabase::CloseOutcome outcome) {
    KJ_ASSERT(state == State::OPENED);
    state = State::WAITING;
    shared->finishOpen(key, tree, facet, outcome);
  }
};

DurableWalPosition ThreadedWalLog::PreparedOpen::positionForOpen() const {
  KJ_REQUIRE(reservation->state == OpenReservation::State::RESERVED,
      "jsg.Error: Durable Object storage preparation was already used (STORAGE_UNAVAILABLE)");
  reservation->shared->checkReservedOpen(reservation->key);
  return position;
}

kj::Function<void(SqliteDatabase::CloseOutcome)> ThreadedWalLog::PreparedOpen::trackClose() const {
  KJ_ASSERT(reservation->state == OpenReservation::State::RESERVED);
  reservation->state = OpenReservation::State::OPENED;
  return [reservation = reservation](
             SqliteDatabase::CloseOutcome outcome) { reservation->closed(outcome); };
}

struct ThreadedWalLog::Impl {
  struct OpenPreparation {
    const kj::Directory& directory;
    kj::Path path;
    kj::Own<kj::CrossThreadPromiseFulfiller<DurableWalPosition>> result;
    // A canceled caller may leave its scan running until this item is dropped.
    std::shared_ptr<OpenReservation> reservation;
  };

  Impl(int fd, uint64_t generation, kj::Duration confirmationTimeout)
      : shared(std::make_shared<Shared>(confirmationTimeout)),
        owner([state = shared, descriptor = kj::AutoCloseFd(fd), generation]() mutable {
    KJ_DEFER(state->stop());
    try {
      auto io = kj::setupAsyncIo();
      WalWire wire(*io.lowLevelProvider, descriptor.release());
      Session session(*state, wire, generation);
      session.run().wait(io.waitScope);
    } catch (const kj::Exception& error) {
      KJ_LOG(ERROR, "threaded WAL channel closed", error);
    }
  }), prepare([this] {
    auto io = kj::setupAsyncIo();
    for (;;) {
      auto next = opens.next().wait(io.waitScope);
      if (next == kj::none) return;
      auto& open = KJ_ASSERT_NONNULL(next);
      if (!open.result->isWaiting()) continue;
      try {
        open.result->fulfill(readDurableWalPosition(open.directory, open.path));
      } catch (kj::Exception& error) {
        open.result->reject(kj::mv(error));
      }
    }
  }) {}

  ~Impl() noexcept(false) {
    opens.close();
    shared->stop();
  }

  std::shared_ptr<Shared> shared;
  Mailbox<OpenPreparation> opens;
  kj::Thread owner;
  kj::Thread prepare;
};

ThreadedWalLog::ThreadedWalLog(int fd, uint64_t generation, kj::Duration confirmationTimeout)
    : impl(kj::heap<Impl>(fd, generation, confirmationTimeout)) {}
ThreadedWalLog::~ThreadedWalLog() noexcept(false) = default;

bool ThreadedWalLog::ready() const { return impl->shared->negotiated(); }

kj::Duration ThreadedWalLog::confirmationTimeout() const { return impl->shared->confirmationTimeout; }

uint64_t ThreadedWalLog::unconfirmedCloseIdentities() const {
  return impl->shared->unconfirmedCloseIdentities.load(std::memory_order_relaxed);
}

kj::Own<DurableWalChannel> ThreadedWalLog::attachHome(
    kj::Function<void(uint64_t, CloseReason)> requestClose, kj::Maybe<std::atomic<bool>&> walFull) {
  return kj::heap<Home>(
      impl->shared, kj::mv(requestClose), kj::systemPreciseMonotonicClock(), walFull);
}

kj::Promise<ThreadedWalLog::PreparedOpen> ThreadedWalLog::prepareOpen(
    const kj::Directory& directory,
    kj::Path path,
    kj::String storageKey,
    uint64_t tree,
    uint facet) {
  auto reservation = std::make_shared<OpenReservation>(impl->shared, storageKey, tree, facet);
  for (;;) {
    auto changed = kj::newPromiseAndCrossThreadFulfiller<void>();
    if (impl->shared->reserveOpen(reservation->key, tree, facet, kj::mv(changed.fulfiller))) {
      reservation->state = OpenReservation::State::RESERVED;
      break;
    }
    co_await changed.promise;
  }
  auto paf = kj::newPromiseAndCrossThreadFulfiller<DurableWalPosition>();
  KJ_REQUIRE(ready() &&
          impl->opens.push({directory, kj::mv(path), kj::mv(paf.fulfiller), reservation}, 1, 1024),
      STORAGE_UNAVAILABLE);
  auto position = co_await paf.promise;
  KJ_REQUIRE(ready(), STORAGE_UNAVAILABLE);
  co_return PreparedOpen(position, std::move(reservation));
}

}  // namespace workerd::server
