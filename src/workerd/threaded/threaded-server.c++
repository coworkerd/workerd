// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "fatal-attribution.h"
#include "isolate-busy.h"
#include "protocol.h"

#include <workerd/api/actor-state.h>
#include <workerd/api/memory-cache.h>
#include <workerd/api/worker-rpc.h>
#include <workerd/io/actor-cache.h>
#include <workerd/io/actor-sqlite.h>
#include <workerd/io/compatibility-date.h>
#include <workerd/io/io-channels.h>
#include <workerd/io/io-context.h>
#include <workerd/io/io-thread-context.h>
#include <workerd/io/legacy-hibernation-manager.h>
#include <workerd/io/limit-enforcer.h>
#include <workerd/io/observer.h>
#include <workerd/io/tracer.h>
#include <workerd/io/tracked-wasm-instance.h>
#include <workerd/io/worker-entrypoint.h>
#include <workerd/io/worker-interface.h>
#include <workerd/jsg/async-context.h>
#include <workerd/jsg/coworkerd-rpc-code.h>
#include <workerd/jsg/setup.h>
#include <workerd/server/factory/coworkerd-alarm-limit.h>
#include <workerd/server/factory/coworkerd-durable-admission.h>
#include <workerd/server/factory/coworkerd-durable-cpu.h>
#include <workerd/server/factory/coworkerd-durable-placement.h>
#include <workerd/server/factory/coworkerd-durable-test-storage.h>
#include <workerd/server/factory/coworkerd-sandbox.h>
#include <workerd/server/factory/coworkerd-steady.h>
#include <workerd/server/factory/coworkerd-threaded-wal.h>
#include <workerd/server/v8-platform-impl.h>
#include <workerd/server/workerd-api.h>
#include <workerd/threaded/protocol.capnp.h>
#include <workerd/util/autogate.h>
#include <workerd/util/entropy.h>
#include <workerd/util/sqlite-metering.h>
#include <workerd/util/stream-utils.h>

#include <absl/time/time.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/tcp.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <tcmalloc/malloc_extension.h>
#include <time.h>
#include <unistd.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <capnp/rpc-twoparty.h>
#include <kj/async-io.h>
#include <kj/async-unix.h>
#include <kj/compat/http.h>
#include <kj/debug.h>
#include <kj/encoding.h>
#include <kj/mutex.h>
#include <kj/thread.h>
#include <kj/vector.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace workerd {
namespace {

uint64_t nowNs(clockid_t clock = CLOCK_MONOTONIC) {
  timespec t;
  KJ_SYSCALL(clock_gettime(clock, &t));
  return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}

kj::Own<const kj::Directory> newDurableTestDirectory(int listener) {
  sockaddr_un address{};
  socklen_t size = sizeof(address);
  KJ_SYSCALL(getsockname(listener, reinterpret_cast<sockaddr *>(&address), &size));
  KJ_REQUIRE(address.sun_family == AF_UNIX && size <= sizeof(address) && address.sun_path[0] == '/',
      "durable test storage requires a filesystem listener");
  auto filesystem = kj::newDiskFilesystem();
  const auto socket = filesystem->getCurrentPath().evalNative(
      kj::heapString(address.sun_path, strnlen(address.sun_path, sizeof(address.sun_path))));
  const auto parent = socket.parent();
  // The node owns this run directory under the new fixture stack. Create a
  // new volume even if the child restarts on the same listener address.
  kj::String path = kj::str("/", parent.toString(), "/durable-test.XXXXXX");
  if (mkdtemp(path.begin()) == nullptr) KJ_FAIL_SYSCALL("mkdtemp", errno);
  return filesystem->getRoot().openSubdir(
      filesystem->getCurrentPath().evalNative(path), kj::WriteMode::MODIFY);
}

bool prepareDescriptors(uint32_t threads) {
  rlimit current;
  auto required = threaded::descriptorBudget(threads);
  if (getrlimit(RLIMIT_NOFILE, &current) != 0 || current.rlim_max < required) {
    fprintf(stderr,
        "[threaded-start] "
        "{\"event\":\"startup-rejected\",\"resource\":\"descriptors\","
        "\"required\":%llu}\n",
        (unsigned long long)required);
    return false;
  }
  if (current.rlim_cur < required) {
    current.rlim_cur = required;
    if (setrlimit(RLIMIT_NOFILE, &current) != 0) {
      fprintf(stderr,
          "[threaded-start] "
          "{\"event\":\"startup-rejected\",\"resource\":"
          "\"descriptor-soft-limit\"}\n");
      return false;
    }
  }
  return true;
}

struct Entropy final: kj::EntropySource {
  void generate(kj::ArrayPtr<kj::byte> bytes) override {
    getEntropy(bytes);
  }
};

// KJ's native timer advances at event-port polls, never while JavaScript runs.
struct Timer final: TimerChannel {
  explicit Timer(kj::Timer &timer)
      : timer(timer),
        originMono(timer.now()),
        originWall(kj::systemPreciseCalendarClock().now()),
        observed(originWall) {}
  void syncTime() override {
    auto mono = timer.now();
    if (mono != originMono) {
      originMono = mono;
      originWall = kj::systemPreciseCalendarClock().now();
    }
    observed = originWall;
  }
  kj::Date now(kj::Maybe<kj::Date> clamp) override {
    KJ_IF_SOME(deadline, clamp) {
      return kj::min(observed, deadline);
    }
    return observed;
  }
  kj::Promise<void> atTime(kj::Date when) override {
    return timer.atTime(originMono + (when - originWall));
  }
  kj::Promise<void> afterLimitTimeout(kj::Duration delay) override {
    return timer.afterDelay(delay);
  }
  kj::Timer &timer;
  kj::TimePoint originMono;
  kj::Date originWall, observed;
};

bool traceForTest() {
  static const bool enabled = getenv("COWORKERD_THREADED_TRACE_FOR_TEST") != nullptr;
  return enabled;
}

void reportDurableFailureForTest(kj::StringPtr stage, const kj::Exception &error) {
  if (traceForTest()) {
    auto line = kj::str("[threaded-durable-test] ", stage, ": ", error.getDescription(), "\n");
    (void)!write(STDERR_FILENO, line.begin(), line.size());
  }
}

// Tests of the call threads' background backstop only: with the drain limit
// off, nothing but the backstop ends waitUntil work that never finishes.
bool drainLimitDisabledForTest() {
  static const bool disabled = getenv("COWORKERD_THREADED_NO_DRAIN_LIMIT_FOR_TEST") != nullptr;
  return disabled;
}

void noteUnsupported(kj::StringPtr feature);

kj::Exception unsupportedError(kj::StringPtr feature) {
  auto info = threaded::refusalInfo(threaded::Refusal::UNSUPPORTED);
  auto code = kj::StringPtr(info.code.data(), info.code.size());
  return jsg::tagCoworkerdRpcError(
      KJ_EXCEPTION(UNIMPLEMENTED, kj::str("jsg.Error: ", code, ": ", feature)), code);
}

[[noreturn]] void unsupported(kj::StringPtr feature) {
  noteUnsupported(feature);
  kj::throwRecoverableException(unsupportedError(feature));
  KJ_UNREACHABLE;
}

struct Cache final: CacheClient {
  kj::Own<kj::HttpClient> getDefault(SubrequestMetadata) override {
    unsupported("cache");
  }
  kj::Own<kj::HttpClient> getNamespace(kj::StringPtr, SubrequestMetadata) override {
    unsupported("cache namespace");
  }
};

struct Channels;

enum class LimitCause { NONE, CPU, MEMORY, PROCESS_MEMORY };
using LimitState = std::shared_ptr<std::atomic<LimitCause>>;
// waitUntil work may run this long after its response ended (as on
// Cloudflare); the backstop adds a margin for cancellation to complete.
constexpr kj::Duration WAIT_UNTIL_LIMIT = 30 * kj::SECONDS;
constexpr kj::Duration BACKGROUND_BACKSTOP = WAIT_UNTIL_LIMIT + 5 * kj::SECONDS;
constexpr uint64_t MAX_FIXED_ARRAY_ELEMENTS = 128ull * 1024 * 1024;
constexpr uint64_t MAX_SINGLE_HEAP_OBJECT =
    (uint64_t(v8::String::kMaxLength) * sizeof(uint16_t) > MAX_FIXED_ARRAY_ELEMENTS * sizeof(double)
            ? uint64_t(v8::String::kMaxLength) * sizeof(uint16_t)
            : MAX_FIXED_ARRAY_ELEMENTS * sizeof(double)) +
    (1ull << 20);
static_assert(MAX_SINGLE_HEAP_OBJECT < (2ull << 30));
// The tenant limit counts live bytes, but V8's ceiling counts pages, and pages
// are not filled exactly: regular objects never span a 256 KiB page, so large
// ones waste up to half of it (cwkd-p40: 126 MiB live in 88 KB double arrays
// took 1212.0 MB of pages with the maximum string, 1150.1 MB live). Pages stay
// under twice the limit, so the ceiling keeps the limit itself plus 32 MiB for
// the other spaces on top of one maximum object: finishing that object never
// ends the process, whatever the object sizes.
constexpr uint64_t fragmentationReserve(uint64_t maximum) {
  return maximum + (32ull << 20);
}

int limitEventFd();
using threaded::FatalInvocation;

thread_local const char *currentOomReport = nullptr;
// The executing isolate's heap at its last collection. V8 collects before a
// fatal OOM, so this is the heap of the isolate whose allocation failed.
thread_local const std::atomic<uint64_t> *currentOomHeap = nullptr;
kj::String oomReportText(kj::StringPtr identity, uint32_t memoryMiB) {
  static const auto pid = getpid();
  return kj::str("{\"event\":\"memory-culprit\",\"pid\":", pid, ",", identity,
      "\"memoryLimitMiB\":", memoryMiB, "}\n");
}
struct OomReportScope {
  explicit OomReportScope(
      kj::StringPtr identity, uint32_t memoryMiB, const std::atomic<uint64_t> *heap = nullptr)
      : previous(currentOomReport),
        previousHeap(currentOomHeap),
        owned(oomReportText(identity, memoryMiB)) {
    (void)limitEventFd();
    currentOomReport = owned.cStr();
    currentOomHeap = heap;
  }
  // A report formatted once for a whole call (RequestLimits), not at every
  // JavaScript entry; it outlives this scope.
  OomReportScope(const kj::String &report, const std::atomic<uint64_t> *heap)
      : previous(currentOomReport),
        previousHeap(currentOomHeap) {
    (void)limitEventFd();
    currentOomReport = report.cStr();
    currentOomHeap = heap;
  }
  ~OomReportScope() {
    currentOomReport = previous;
    currentOomHeap = previousHeap;
  }
  const char *previous;
  const std::atomic<uint64_t> *previousHeap;
  kj::String owned;
};

kj::Exception limitException(LimitCause cause);

// Each resident owns its measured heap, external allocations and native
// overhead. Invocation ownership survives yields; only new admission uses the
// lower watermark. Growth over the upper watermark belongs to the growing call.
// The heap one isolate's own pointer cage holds when each isolate group has a cage of its own (the
// V8 sandbox build): the 4 GiB cage less what V8 keeps in it, as for the shared cage below.
constexpr uint64_t OWN_CAGE_HEAP = (4ull << 30) - (256ull << 20);

struct DatabasePreparationReplacement {
  const uint64_t ceiling;
  bool used = false;  // Protected by the resource budget mutex.
};

struct DatabaseReservationReturn {
  uint64_t memoryBeforeRelease = 0;
  // Published last, after the memory charge and resident count are both returned.
  std::atomic<uint64_t> releasedMemory{0};
};

struct ResourceBudget {
  enum class ReclaimMode { BACKGROUND, NONE };
  static constexpr uint64_t MiB = 1ull << 20;
  static constexpr uint64_t NATIVE_OVERHEAD = 512ull << 10;
  static constexpr uint64_t PREPARATION_HEAP = 2 * MiB;
  static constexpr uint64_t PREPARATION_MEMORY = NATIVE_OVERHEAD + PREPARATION_HEAP;
  ResourceBudget(uint64_t maximum,
      uint32_t residents,
      uint32_t callThreads,
      ReclaimMode reclaimMode = ReclaimMode::BACKGROUND)
      : reclaimMode(reclaimMode),
        memoryMaximum(maximum),
        admissionMaximum(maximum -
            kj::min(maximum / 10, kj::max<uint64_t>(8 * MiB, uint64_t(callThreads) * MiB))),
        addressMaximum(v8::IsolateGroup::CanCreateNewGroups()
                ? UINT64_MAX
                : (4ull << 30) - MAX_SINGLE_HEAP_OBJECT - (256ull << 20)),
        baselineMemory(64 * MiB + uint64_t(callThreads + 16) * (256ull << 10)),
        configuredResidentMaximum(residents),
        residentMaximum(residents),
        memoryReserved(baselineMemory) {
    KJ_REQUIRE(admissionMaximum > baselineMemory + 2 * MiB,
        "native thread budget leaves no memory for an active Worker");
    // One shared pointer cage holds at most 1,840 residents; with a cage and a V8 sandbox per
    // isolate the address space bounds them instead: the node passes its per-process limit for the
    // mode (sandbox.protectionKeys) and isolate group creation refuses beyond it.
    KJ_REQUIRE(residents > 0 && residents <= (v8::IsolateGroup::CanCreateNewGroups() ? 65536 : 1840),
        "invalid resident budget for this build");
  }
  const ReclaimMode reclaimMode;
  const uint64_t memoryMaximum, admissionMaximum, addressMaximum, baselineMemory;
  // The node's per-process limit, and the one in force: the first time the address space has no
  // room for another isolate's V8 sandbox, the limit is lowered to the residents it does hold
  // (lowerResidentMaximum(), r50 §6.5 B3), so that the next Worker evicts an idle one instead of
  // being refused again.
  const uint32_t configuredResidentMaximum;
  std::atomic<uint32_t> residentMaximum;
  std::mutex mutex;
  server::DurableAdmission::Time databasePreparationPreferredUntil{};
  std::atomic<uint64_t> memoryReserved{0}, addressReserved{0}, sqliteMemoryReserved{0};
  // Heap headroom near-heap-limit raises granted above what their isolates
  // used, until each call's termination takes effect (MemoryBudget::raise()):
  // two isolates raised at once never both count on the same remainder.
  uint64_t lent = 0;  // Protected by mutex.
  std::atomic<uint32_t> residents{0};
  std::atomic<uint64_t> rejections{0}, evictions{0}, pressureTerminations{0};
  std::condition_variable reclaimed;
  std::atomic<bool> reclaimRequested{false};
  int reclaimWake = -1;  // assigned before the server starts any threads
  uint64_t reclaimEpoch = 0, targetMemory = UINT64_MAX, targetAddress = UINT64_MAX;
  bool noIdleAtLastReclaim = false;
  void requestReclaimLocked(uint64_t additionalMemory = 0) {
    targetMemory = kj::min(targetMemory,
        kj::min(admissionMaximum, memoryMaximum - kj::min(memoryMaximum, additionalMemory)));
    targetAddress = kj::min(targetAddress, addressMaximum);
    reclaimRequested.store(true);
    if (reclaimWake >= 0) {
      uint64_t one = 1;
      (void)!write(reclaimWake, &one, sizeof(one));
    }
  }
  // Set while this thread runs a maintenance pass, the only place a reclaim
  // completes. An eviction there can enter GC callbacks and the allocator.
  static inline thread_local bool maintaining = false;
  template <typename Pass>
  void runMaintenance(Pass &&pass) {
    maintaining = true;
    KJ_DEFER(maintaining = false);
    pass();
  }
  bool reclaim(uint64_t additionalMemory = 0) {
    // Durable homes reclaim their own actors on the event loop; none can finish this wait.
    if (reclaimMode == ReclaimMode::NONE) return false;
    std::unique_lock lock(mutex);
    auto epoch = reclaimEpoch;
    requestReclaimLocked(additionalMemory);
    // Never wait for the pass this thread is running; the request stays
    // pending for the next pass and this caller sees no idle Worker.
    if (maintaining) return false;
    reclaimed.wait(lock, [&] { return reclaimEpoch != epoch; });
    return !noIdleAtLastReclaim;
  }
  bool needsReclaim() {
    std::lock_guard lock(mutex);
    return memoryReserved.load() > targetMemory || addressReserved.load() > targetAddress;
  }
  bool withinMaximum() const {
    return memoryReserved.load() <= memoryMaximum && addressReserved.load() <= addressMaximum;
  }
  void finishReclaim(bool noIdle) {
    std::lock_guard lock(mutex);
    noIdleAtLastReclaim = noIdle;
    ++reclaimEpoch;
    if (!reclaimRequested.load()) targetMemory = targetAddress = UINT64_MAX;
    reclaimed.notify_all();
  }

  bool fits(uint64_t memory, uint64_t newMemory, uint64_t address, uint64_t newAddress,
      uint64_t ceiling) const {
    return newMemory <= ceiling - kj::min(memory, ceiling) &&
        newAddress <= addressMaximum - kj::min(address, addressMaximum);
  }

  std::optional<uint64_t> replacementCeiling(uint64_t returnedMemory, uint64_t returnedAddress) {
    std::lock_guard lock(mutex);
    if (residents.load() > residentMaximum.load()) return std::nullopt;
    const auto memory = memoryReserved.load();
    const auto addressAfterRelease = addressReserved.load() - returnedAddress;
    const auto memoryAfterRelease = memory - returnedMemory;
    if (fits(memoryAfterRelease, PREPARATION_MEMORY,
            addressAfterRelease, PREPARATION_HEAP, admissionMaximum)) {
      return admissionMaximum;
    }
    // Above the admission ceiling, a single replacement may reuse the returning
    // charge. It cannot add to that total or exceed the hard memory budget.
    const auto hardCeiling = memoryMaximum - kj::min(lent, memoryMaximum);
    if (returnedMemory < PREPARATION_MEMORY ||
        !fits(memoryAfterRelease, PREPARATION_MEMORY,
            addressAfterRelease, PREPARATION_HEAP, hardCeiling)) {
      return std::nullopt;
    }
    return kj::min(memory, memoryMaximum);
  }

  void preferDatabasePreparation(server::DurableAdmission::Time until) {
    std::lock_guard lock(mutex);
    databasePreparationPreferredUntil = until;
  }

  void limitDatabasePreparationPreference(server::DurableAdmission::Time until) {
    std::lock_guard lock(mutex);
    databasePreparationPreferredUntil = kj::min(until, databasePreparationPreferredUntil);
  }

  bool registerPreparation(bool database, DatabasePreparationReplacement* replacement,
      const char** refusal) {
    std::lock_guard lock(mutex);
    const auto count = residents.load();
    const auto memory = memoryReserved.load();
    const auto address = addressReserved.load();
    bool keepCredit = false;
    if (!database && databasePreparationPreferredUntil != server::DurableAdmission::Time{}) {
      keepCredit = server::DurableAdmission::Clock::now() < databasePreparationPreferredUntil;
      if (!keepCredit) databasePreparationPreferredUntil = {};
    }
    const uint64_t preparations = keepCredit ? 2 : 1;
    const bool mayReplace = database && replacement != nullptr && !replacement->used;
    auto ceiling = admissionMaximum;
    if (mayReplace) {
      // Lent headroom reduces only the band available for a replacement.
      const auto hardCeiling = memoryMaximum - kj::min(lent, memoryMaximum);
      ceiling = kj::max(admissionMaximum, kj::min(replacement->ceiling, hardCeiling));
    }
    const char* reason = nullptr;
    if (count + preparations > residentMaximum.load()) {
      reason = "copy-residents";
    } else if (preparations * PREPARATION_HEAP > addressMaximum - kj::min(address, addressMaximum)) {
      reason = "copy-address-space";
    } else if (preparations * PREPARATION_MEMORY > ceiling - kj::min(memory, ceiling)) {
      reason = "copy-memory";
    }
    if (reason != nullptr) {
      if (refusal != nullptr) *refusal = reason;
      return false;
    }
    memoryReserved.store(memory + PREPARATION_MEMORY);
    addressReserved.store(address + PREPARATION_HEAP);
    residents.fetch_add(1);
    if (database) databasePreparationPreferredUntil = {};
    if (mayReplace) replacement->used = true;
    return true;
  }

  bool change(uint64_t oldMemory,
      uint64_t newMemory,
      uint64_t oldAddress,
      uint64_t newAddress,
      bool admission = false,
      bool observe = false,
      uint64_t *memoryBeforeRelease = nullptr,
      int64_t sqliteDelta = 0) {
    std::lock_guard lock(mutex);
    const auto reserved = memoryReserved.load();
    if (memoryBeforeRelease != nullptr) *memoryBeforeRelease = reserved;
    auto memory = reserved - oldMemory;
    auto address = addressReserved.load() - oldAddress;
    auto ceiling = admission ? admissionMaximum : memoryMaximum;
    const bool accepted = (!admission && newMemory <= oldMemory && newAddress <= oldAddress) ||
        fits(memory, newMemory, address, newAddress, ceiling);
    if (!accepted && !observe) return false;
    memoryReserved.store(memory + newMemory);
    addressReserved.store(address + newAddress);
    if (sqliteDelta != 0) sqliteMemoryReserved.fetch_add(sqliteDelta);
    if (memory + newMemory > admissionMaximum || address + newAddress > addressMaximum)
      requestReclaimLocked();
    return accepted;
  }
};

struct ResourceReservation final: SqliteProcessMemoryBudget {
  static constexpr uint64_t MiB = ResourceBudget::MiB;
  // The capacity acquisition records heap and RSS deltas to validate this seed
  // and fixed native overhead separately from each isolate's own heap ceiling.
  static constexpr uint64_t NATIVE_OVERHEAD = ResourceBudget::NATIVE_OVERHEAD;
  static constexpr uint64_t PREPARATION_HEAP = ResourceBudget::PREPARATION_HEAP;
  ResourceReservation(std::shared_ptr<ResourceBudget> budget, uint32_t memoryMiB,
      bool platformDatabase = false)
      : budget(std::move(budget)),
        maximumHeap(uint64_t(memoryMiB == 0 ? 128 : memoryMiB) * MiB),
        returned(platformDatabase ? std::make_shared<DatabaseReservationReturn>() : nullptr) {}
  ~ResourceReservation() noexcept override {
    if (!registered) return;
    if (sqliteBytes != 0) {
      KJ_LOG(ERROR, "SQLite heap charge remained when releasing a copy reservation", sqliteBytes);
    }
    (void)budget->change(memoryCharge, 0, addressCharge, 0, false, false,
        returned ? &returned->memoryBeforeRelease : nullptr, -int64_t(sqliteBytes));
    budget->residents.fetch_sub(1);
    if (returned) returned->releasedMemory.store(memoryCharge);
  }
  std::shared_ptr<ResourceBudget> budget;
  const uint64_t maximumHeap;
  const std::shared_ptr<DatabaseReservationReturn> returned;
  std::mutex mutex;
  uint64_t heap = PREPARATION_HEAP, heapPhysical = 0, otherExternal = 0, ownedExternal = 0,
           source = 0, logs = 0, sqliteBytes = 0;
  uint64_t memoryCharge = 0, addressCharge = 0;
  uint32_t preparing = 0;
  bool registered = false;
  std::optional<uint64_t> replacementCeiling() {
    std::lock_guard lock(mutex);
    return registered ? budget->replacementCeiling(memoryCharge, addressCharge) : std::nullopt;
  }
  bool resize(bool admission = false, bool observe = false, int64_t sqliteDelta = 0) {
    auto memory =
        NATIVE_OVERHEAD + source + heap + otherExternal + ownedExternal + logs + sqliteBytes;
    auto address = kj::max(2 * MiB, heap);
    bool fits = budget->change(
        memoryCharge, memory, addressCharge, address, admission, observe, nullptr, sqliteDelta);
    if (fits || observe) {
      memoryCharge = memory;
      addressCharge = address;
    }
    return fits;
  }
  std::string metrics() {
    std::lock_guard lock(mutex);
    return "{\"heapBytes\":" + std::to_string(heap) +
        ",\"heapPhysicalBytes\":" + std::to_string(heapPhysical) +
        ",\"ownedExternalBytes\":" + std::to_string(ownedExternal) +
        ",\"otherExternalBytes\":" + std::to_string(otherExternal) +
        ",\"sourceBytes\":" + std::to_string(source) + ",\"logBytes\":" + std::to_string(logs) +
        ",\"fixedNativeBytes\":" + std::to_string(NATIVE_OVERHEAD) +
        ",\"memoryCharge\":" + std::to_string(memoryCharge) +
        ",\"preparations\":" + std::to_string(preparing) + "}";
  }
  bool registerPreparation(DatabasePreparationReplacement* replacement = nullptr,
      const char** refusal = nullptr) {
    std::lock_guard lock(mutex);
    KJ_ASSERT(!registered);
    if (!budget->registerPreparation(bool(returned), replacement, refusal)) return false;
    memoryCharge = ResourceBudget::PREPARATION_MEMORY;
    addressCharge = PREPARATION_HEAP;
    registered = true;
    preparing = 1;
    return true;
  }
  bool prepare() {
    std::lock_guard lock(mutex);
    if (!resize(true)) return false;
    ++preparing;
    return true;
  }
  void prepared() {
    std::lock_guard lock(mutex);
    KJ_ASSERT(preparing > 0);
    --preparing;
  }
  bool observe(v8::HeapStatistics &stats, uint64_t owned) {
    std::lock_guard lock(mutex);
    heap = stats.total_heap_size();
    heapPhysical = stats.total_physical_size();
    otherExternal = stats.external_memory() > owned ? stats.external_memory() - owned : 0;
    return resize(false, true);
  }
  bool retainSource(uint64_t length) {
    std::lock_guard lock(mutex);
    if (length > (256ull << 20)) return false;
    auto old = source;
    source = length * 5;
    if (resize()) return true;
    source = old;
    return false;
  }
  bool allocateExternal(uint64_t size) {
    std::lock_guard lock(mutex);
    ownedExternal += size;
    if (resize()) return true;
    ownedExternal -= size;
    return false;
  }
  void freeExternal(uint64_t size) {
    std::lock_guard lock(mutex);
    KJ_ASSERT(size <= ownedExternal);
    ownedExternal -= size;
    (void)resize(false, true);
  }
  bool resizeSqliteMemory(size_t oldCharge, size_t newCharge, bool observe) noexcept override {
    std::lock_guard lock(mutex);
    if (!registered) {
      KJ_LOG(ERROR, "SQLite heap changed before its copy reservation was registered");
      return observe || newCharge <= oldCharge;
    }
    if (oldCharge > sqliteBytes) {
      KJ_LOG(ERROR, "SQLite heap change exceeded the copy's recorded charge", oldCharge, sqliteBytes);
    }
    const auto previous = sqliteBytes;
    if (newCharge >= oldCharge) {
      sqliteBytes += newCharge - oldCharge;
    } else {
      sqliteBytes -= kj::min(sqliteBytes, oldCharge - newCharge);
    }
    // Observe trusted allocations even at the process ceiling.
    if (resize(false, observe, int64_t(sqliteBytes) - int64_t(previous)) || observe) return true;
    sqliteBytes = previous;
    return false;
  }
  size_t sqliteMemoryCharge() {
    std::lock_guard lock(mutex);
    return sqliteBytes;
  }
  bool retainLogs(uint64_t bytes) {
    std::lock_guard lock(mutex);
    logs += bytes;
    if (resize()) return true;
    logs -= bytes;
    return false;
  }
  void releaseLogs(uint64_t bytes) {
    std::lock_guard lock(mutex);
    KJ_ASSERT(bytes <= logs);
    logs -= bytes;
    (void)resize(false, true);
  }
};

struct ResourcePreparation {
  explicit ResourcePreparation(std::shared_ptr<ResourceReservation> reservation)
      : reservation(std::move(reservation)) {}
  ~ResourcePreparation() {
    reservation->prepared();
  }
  std::shared_ptr<ResourceReservation> reservation;
};

// Workers refused because the address space had no room for another isolate's V8 sandbox
// (tryNewIsolateGroup()); one [coworkerd-sandbox] row each. The refused Worker is not resident:
// the process's resident limit is lowered to the residents besides it (never raised again), and
// the row says so.
std::atomic<uint64_t> sandboxRefusals{0};
void noSandboxRoom(ResourceBudget &budget) {
  auto refusals = sandboxRefusals.fetch_add(1) + 1;
  // The refused Worker was counted when its load registered.
  const uint32_t residents = budget.residents.load();
  const uint32_t others = residents > 1 ? residents - 1 : 1;
  auto previous = budget.residentMaximum.load();
  while (others < previous && !budget.residentMaximum.compare_exchange_weak(previous, others)) {
  }
  fprintf(stderr,
      "[coworkerd-sandbox] {\"event\":\"group-refused\",\"refusals\":%llu,\"residents\":%u,"
      "\"residentMaximum\":%u,\"configuredResidentMaximum\":%u}\n",
      static_cast<unsigned long long>(refusals), residents > 0 ? residents - 1 : 0,
      budget.residentMaximum.load(), budget.configuredResidentMaximum);
}

constexpr kj::Exception::DetailTypeId CAPACITY_DETAIL = 0x9f232974b0ac7143ull;
// The node reads this detail on the private durable bootstrap, before it has an event dispatcher.
constexpr kj::Exception::DetailTypeId DURABLE_COPY_RESERVATION_REFUSED = 0xe05c5dbb242a7eb4ull;
enum class CapacityScope { PROCESS, TENANT };
bool isCapacityError(const kj::Exception &error) {
  return error.getDetail(CAPACITY_DETAIL) != kj::none;
}
kj::Exception capacityError(CapacityScope scope = CapacityScope::PROCESS) {
  const auto tenant = scope == CapacityScope::TENANT;
  auto error = jsg::tagCoworkerdRpcError(tenant
          ? KJ_EXCEPTION(OVERLOADED,
                "jsg.Error: OVERLOADED: tenant execution concurrency limit reached in process")
          : KJ_EXCEPTION(OVERLOADED, "jsg.Error: OVERLOADED: process admission budget is full"),
      "OVERLOADED");
  auto detail = kj::heapArray<kj::byte>(tenant ? 1 : 0);
  if (tenant) detail[0] = 1;
  error.setDetail(CAPACITY_DETAIL, kj::mv(detail));
  return error;
}

// Whether a finished load's failure stays out of the registry, so that the next request loads
// the Worker again instead of receiving this failure: one before the source was fetched, a caller
// failure (DISCONNECTED), memory pressure at startup (which becomes capacityError()), and any
// capacity refusal -- in particular no room for another isolate's V8 sandbox. Kept, a refused
// group answered 503 for this Worker in this process until its slot idled out (r50 §6.5 B1).
// A tenant's own failure (its script throws, a binding is invalid) stays the Worker's failure.
bool loadFailureRetryable(kj::Maybe<kj::Exception> &error, bool fetched, bool memoryPressure) {
  if (memoryPressure) error = capacityError();
  bool retryable = !fetched || memoryPressure;
  KJ_IF_SOME(failure, error) {
    retryable = retryable || failure.getType() == kj::Exception::Type::DISCONNECTED ||
        isCapacityError(failure);
  }
  return retryable;
}

struct TenantBudget {
  explicit TenantBudget(uint32_t limit): limit(limit) {
    KJ_REQUIRE(limit <= 16384, "invalid per-tenant in-flight limit");
  }
  const uint32_t limit;
  std::mutex mutex;
  std::map<std::string, uint32_t> active;
  std::atomic<uint64_t> rejected{0};
};
struct TenantPermit {
  TenantPermit(std::shared_ptr<TenantBudget> budget, std::string tenant)
      : budget(std::move(budget)),
        tenant(std::move(tenant)) {
    std::lock_guard lock(this->budget->mutex);
    auto found = this->budget->active.find(this->tenant);
    if (this->budget->limit && this->budget->active.size() > 1 &&
        found != this->budget->active.end() && found->second >= this->budget->limit) {
      this->budget->rejected.fetch_add(1);
      kj::throwRecoverableException(capacityError(CapacityScope::TENANT));
    }
    ++this->budget->active[this->tenant];
  }
  ~TenantPermit() {
    std::lock_guard lock(budget->mutex);
    auto found = budget->active.find(tenant);
    if (found != budget->active.end() && --found->second == 0) budget->active.erase(found);
  }
  std::shared_ptr<TenantBudget> budget;
  std::string tenant;
};

std::shared_ptr<TenantPermit> makeTenantPermit(
    const std::shared_ptr<TenantBudget> &budget, std::string_view tenant) {
  if (budget->limit == 0) return nullptr;
  return std::make_shared<TenantPermit>(budget, std::string(tenant));
}

uint64_t configuredCount(const char *name, uint64_t fallback) {
  auto text = getenv(name);
  if (!text) return fallback;
  char *end;
  errno = 0;
  auto result = strtoull(text, &end, 10);
  KJ_REQUIRE(
      *text && *text != '-' && end && !*end && errno == 0, "invalid native configuration", name);
  return result;
}

kj::StringPtr executionIdentity(kj::StringPtr fallback);
struct IsolateLimits;
kj::Maybe<kj::StringPtr> scopedExecutionIdentity(const IsolateLimits &owner, v8::Isolate *isolate);

kj::Exception limitException(LimitCause cause) {
  if (cause == LimitCause::PROCESS_MEMORY)
    return jsg::tagCoworkerdRpcError(
        KJ_EXCEPTION(OVERLOADED, "jsg.Error: PROCESS_MEMORY_PRESSURE"), "PROCESS_MEMORY_PRESSURE");
  auto info = threaded::refusalInfo(
      cause == LimitCause::MEMORY ? threaded::Refusal::MEMORY : threaded::Refusal::CPU);
  auto code = kj::StringPtr(info.code.data(), info.code.size());
  return jsg::tagCoworkerdRpcError(KJ_EXCEPTION(OVERLOADED, kj::str("jsg.Error: ", code)), code);
}

std::atomic<uint64_t> limitRowsDropped{0};
int limitEventFd() {
  static int fd = [] {
    auto value = getenv("COWORKERD_EVENT_FD");
    return value == nullptr ? -1 : int(strtol(value, nullptr, 10));
  }();
  return fd;
}
bool writeLimitRow(kj::StringPtr event,
    kj::StringPtr identity,
    uint64_t used,
    uint32_t cpuMs,
    uint32_t memoryMiB,
    uint64_t peakUsedBytes = 0) {
  int fd = limitEventFd();
  if (fd < 0) return false;
  auto row = kj::str("{\"event\":\"", event, "\",", identity, "\"cpuUsedNs\":", used,
      ",\"cpuLimitMs\":", cpuMs, ",\"memoryLimitMiB\":", memoryMiB,
      ",\"peakUsedBytes\":", peakUsedBytes, ",\"pid\":", getpid(), "}\n");
  if (row.size() > 4096) {
    limitRowsDropped.fetch_add(1);
    return false;
  }
  ssize_t result;
  do {
    result = write(fd, row.begin(), row.size());
  } while (result < 0 && errno == EINTR);
  if (result != ssize_t(row.size())) {
    limitRowsDropped.fetch_add(1);
    return false;
  }
  return true;
}

void writeCrashRow(int fd, const char *data, size_t size) {
  if (fd < 0 || size > PIPE_BUF) return;
  for (unsigned retry = 0; retry < 10; ++retry) {
    const auto n = write(fd, data, size);
    if (n == ssize_t(size)) return;
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      pollfd pending{fd, POLLOUT, 0};
      (void)poll(&pending, 1, 10);
      continue;
    }
    return;
  }
}

void writeFatalRecord(int fd) {
  char row[PIPE_BUF];
  const auto *invocation = FatalInvocation::forIsolate(v8::Isolate::TryGetCurrent());
  const auto identity = invocation == nullptr ? ""_kj : invocation->getIdentity();
  if (identity.size() > 1 && identity.size() < sizeof(row)) {
    // Identity fields carry a trailing comma for use in the kernel's event rows.
    const auto size = snprintf(row, sizeof(row), "{\"event\":\"v8-fatal\",\"pid\":%d,%.*s}\n",
        getpid(), int(identity.size() - 1), identity.begin());
    if (size > 0 && size_t(size) < sizeof(row)) {
      writeCrashRow(fd, row, size);
      return;
    }
  }
  const auto size =
      snprintf(row, sizeof(row), "{\"event\":\"v8-fatal-undetermined\",\"pid\":%d}\n", getpid());
  if (size > 0 && size_t(size) < sizeof(row)) writeCrashRow(fd, row, size);
}

[[noreturn]] void exitFromV8Fatal(const char *location, int line, const char *message) {
  const int fd = limitEventFd();
  writeFatalRecord(fd);
  if (fd >= 0) {
    static_assert(threaded::FATAL_DIAGNOSTIC_ROW_BYTES <= PIPE_BUF);
    char diagnostic[threaded::FATAL_DIAGNOSTIC_ROW_BYTES];
    const auto size =
        threaded::formatFatalDiagnostic(diagnostic, getpid(), location, line, message);
    if (size != 0) writeCrashRow(fd, diagnostic, size);
  }
  // Leave directly, as for native OOM, without unwinding or entering V8 again.
  _exit(75);
}

[[noreturn]] void nativeFatal(const char *location, const char *message) {
  exitFromV8Fatal(location, 0, message);
}

// The process-wide callback of this process's role, through `registrar` (V8's own outside a unit
// test): the ordinary and quarantine roles replace the one workerd installs with V8; durable roles keep it.
template <typename Registrar>
void registerRoleFatalHandler(bool durable, Registrar registrar) {
  threaded::registerProcessFatalHandler(durable ? threaded::FatalHandlerRole::DURABLE
                                                : threaded::FatalHandlerRole::ORDINARY_OR_QUARANTINE,
      registrar, &exitFromV8Fatal);
}

[[noreturn]] void nativeOom(const char *location, const v8::OOMDetails &details) {
  const int fd = limitEventFd();
  char unknown[96];
  int length = snprintf(
      unknown, sizeof(unknown), "{\"event\":\"memory-oom-undetermined\",\"pid\":%d}\n", getpid());
  if (length > 0 && size_t(length) < sizeof(unknown)) writeCrashRow(fd, unknown, length);
  if (currentOomReport) {
    // These V8 heap limit checks identify a per-isolate failure even with a shared cage.
    // Other heap OOM sites need an isolate-owned cage for the same conclusion.
    const bool heapLimit = location != nullptr &&
        (strcmp(location, "Reached heap limit") == 0 ||
            strcmp(location, "Ineffective mark-compacts near heap limit") == 0);
    const bool isolateLocal = details.is_heap_oom &&
        (heapLimit || v8::IsolateGroup::CanCreateNewGroups()) &&
        FatalInvocation::forIsolate(v8::Isolate::TryGetCurrent()) != nullptr;
    auto size = strlen(currentOomReport);
    char row[PIPE_BUF];
    int written = currentOomHeap && size >= 2 && size < sizeof(row)
        ? snprintf(row, sizeof(row),
              "%.*s,\"oomIsolateHeapReported\":true,\"oomIsolateHeapBytes\":%llu,\"oomIsolateLocal\":%s}\n",
              int(size - 2), currentOomReport, (unsigned long long)currentOomHeap->load(),
              isolateLocal ? "true" : "false")
        : -1;
    if (written > 0 && size_t(written) < sizeof(row))
      writeCrashRow(fd, row, written);
    else
      writeCrashRow(fd, currentOomReport, size);
  }
  _exit(74);
}

#ifdef COWORKERD_FATAL_TEST_BUILD
// `load`: called while the Worker loads, after its top level ran, where only the load's own scope
// names it; otherwise at a call's JavaScript entry.
void injectSyntheticCrash(kj::StringPtr worker, bool load = false) {
  auto path = kj::str("/tmp/coworkerd-fatal-test/command.", getpid());
  const int fd = open(path.cStr(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (fd < 0) return;
  char command[1024];
  const auto count = read(fd, command, sizeof(command) - 1);
  close(fd);
  if (count <= 0 || size_t(count) == sizeof(command) - 1) return;
  command[count] = '\0';
  auto input = kj::StringPtr(command, count);
  KJ_IF_SOME(separator, input.findFirst('\n')) {
    if (input.slice(separator + 1) != worker) return;
    command[separator] = '\0';
    auto event = kj::StringPtr(command, separator);
    if (load) {
      if (event == "load-api-check") (void)v8::MaybeLocal<v8::Value>().ToLocalChecked();
      return;
    }
    if (event == "fatal") nativeFatal(nullptr, nullptr);
    // A real dispatch: V8's own API check calls whichever fatal handler this isolate ended up with.
    if (event == "api-check") (void)v8::MaybeLocal<v8::Value>().ToLocalChecked();
    if (event == "shared-oom" && !v8::IsolateGroup::CanCreateNewGroups()) {
      nativeOom(nullptr, v8::OOMDetails{.is_heap_oom = true, .detail = nullptr});
    }
  }
}
#endif

// V8's allocator for `group`: with the sandbox and one pointer cage per group, backing stores
// must lie in the isolate's own sandbox (V8 aborts the process on the first one otherwise).
v8::ArrayBuffer::Allocator *newGroupAllocator(const v8::IsolateGroup &group) {
#ifdef V8_COMPRESS_POINTERS_IN_MULTIPLE_CAGES
  return v8::ArrayBuffer::Allocator::NewDefaultAllocator(group);
#else
  return v8::ArrayBuffer::Allocator::NewDefaultAllocator();
#endif
}

struct MemoryBudget final: v8::ArrayBuffer::Allocator {
  // `group` is the isolate's group; the budget allocates in its sandbox and keeps it.
  MemoryBudget(uint32_t mib,
      LimitState cause,
      std::shared_ptr<ResourceReservation> reservation,
      const v8::IsolateGroup &group)
      : maximum(uint64_t(mib == 0 ? 128 : mib) << 20),
        cause(std::move(cause)),
        reservation(std::move(reservation)),
        group(group),
        inner(newGroupAllocator(this->group)) {}
  ~MemoryBudget() noexcept override {
    returnLent();
    delete inner;
  }
  // With a V8 sandbox per isolate group, no single buffer and no resizable buffer's reservation
  // beyond the group's pool (V8's default is 32 GiB, more than the buffer half of an 8 GiB
  // sandbox). Not the Worker's limit: a buffer above it reaches reserve(), which marks MEMORY and
  // ends the execution (r50 §6.5 B2).
  size_t MaxAllocationSize() const override {
    return server::sandboxMaxAllocationSize(inner->MaxAllocationSize());
  }
  v8::PageAllocator *GetPageAllocator() override {
    return inner->GetPageAllocator();
  }
  const uint64_t maximum;
  LimitState cause;
  std::shared_ptr<ResourceReservation> reservation;
  std::atomic<uint64_t> external{0}, heap{0};
  std::atomic<uint64_t> peakUsedBytes{0};
  v8::IsolateGroup group;
  v8::ArrayBuffer::Allocator *inner;
  v8::Isolate *isolate = nullptr;
  std::atomic<bool> pendingPressure{false};
  std::atomic<bool> durableProcessPressure{false};
  // How many times this isolate's ceiling was raised (install()).
  std::atomic<uint32_t> ceilingRaised{0};
  std::atomic<LimitCause> *currentCause = nullptr;  // isolate-lock protected
  void pressure() {
    if (reservation && reservation->budget->reclaimMode == ResourceBudget::ReclaimMode::NONE)
      durableProcessPressure.store(true);
    if (currentCause) {
      auto expected = LimitCause::NONE;
      if (currentCause->compare_exchange_strong(expected, LimitCause::PROCESS_MEMORY))
        reservation->budget->pressureTerminations.fetch_add(1);
    }
    if (currentCause) {
      if (isolate) isolate->TerminateExecution();
    } else
      pendingPressure.store(true);
  }
  bool reserve(size_t size) {
    if (currentCause && currentCause->load() == LimitCause::PROCESS_MEMORY) return false;
    auto used = external.load();
    for (;;) {
      auto base = heap.load();
      if (size > maximum || base > maximum - size || used > maximum - size - base) {
        cause->store(LimitCause::MEMORY);
        if (isolate) isolate->TerminateExecution();
        return false;
      }
      if (external.compare_exchange_weak(used, used + size)) break;
    }
    if (reservation) {
      while (!reservation->allocateExternal(size)) {
        // The reservation lock has been released before waiting. Only the
        // maintenance loop disposes idle Workers, including during a GC callback.
        if (!reservation->budget->reclaim(size)) {
          if (reservation->allocateExternal(size)) break;
          external.fetch_sub(size);
          pressure();
          return false;
        }
      }
    }
    return true;
  }
  void *Allocate(size_t size) override {
    if (!reserve(size)) return nullptr;
    auto result = inner->Allocate(size);
    if (result == nullptr) {
      external.fetch_sub(size);
      if (reservation) reservation->freeExternal(size);
    }
    return result;
  }
  void *AllocateUninitialized(size_t size) override {
    if (!reserve(size)) return nullptr;
    auto result = inner->AllocateUninitialized(size);
    if (result == nullptr) {
      external.fetch_sub(size);
      if (reservation) reservation->freeExternal(size);
    }
    return result;
  }
  void Free(void *pointer, size_t size) override {
    inner->Free(pointer, size);
    external.fetch_sub(size);
    if (reservation) reservation->freeExternal(size);
  }
  void observe(bool afterGc = false) {
    v8::HeapStatistics stats;
    isolate->GetHeapStatistics(&stats);
    auto owned = external.load();
    auto other = stats.external_memory() > owned ? stats.external_memory() - owned : 0;
    auto bytes = stats.used_heap_size() + other + owned;
    auto peak = peakUsedBytes.load();
    while (peak < bytes && !peakUsedBytes.compare_exchange_weak(peak, bytes)) {}
    // The allocator compares against live bytes observed after collection;
    // unreclaimed garbage from an ordinary turn is not an isolate violation.
    if (afterGc) heap.store(stats.used_heap_size() + other);
    if (reservation && !reservation->observe(stats, owned)) {
      while (!reservation->budget->withinMaximum()) {
        if (!reservation->budget->reclaim() && !reservation->budget->withinMaximum()) {
          pressure();
          break;
        }
      }
    }
    if (pendingPressure.load() ||
        (currentCause && currentCause->load() == LimitCause::PROCESS_MEMORY))
      return;
    if (maximum != 0 && stats.used_heap_size() + other + owned >= maximum) {
      if (!afterGc) {
        isolate->LowMemoryNotification();
        observe(true);
        return;
      }
      cause->store(LimitCause::MEMORY);
      isolate->TerminateExecution();
    }
  }
  void install(v8::Isolate *value) {
    isolate = value;
    // Soft tenant checks run after collection. The fixed hard reserve covers
    // one maximum-sized V8 object.
    value->SetOOMErrorHandler(&nativeOom);
    value->AddGCEpilogueCallback([](v8::Isolate *, v8::GCType, v8::GCCallbackFlags, void *opaque) {
      static_cast<MemoryBudget *>(opaque)->observe(true);
    }, this);
    // V8 takes the first object of the young large-object space without a
    // collection, so no tenant check sees it until the next one, and the next
    // large allocation can then meet the fixed ceiling with that object live
    // (cwkd-p40: a 500 MB string, then a maximum string, ended the process,
    // raw/s27-x500). Near the ceiling the isolate is over its limit: it is
    // terminated as such, and its ceiling is raised by one maximum object so
    // that the allocation in progress finishes; its call ends with
    // MEMORY_LIMIT_EXCEEDED and the isolate is retired, as any isolate over
    // its limit.
    //
    // The termination takes effect at V8's next interrupt check. Code that
    // reaches none keeps allocating meanwhile: straight-line code making one
    // large string after another, or one long builtin (V8's JSON.parse and
    // String.prototype.split check interrupts with patch 0056). So while it is
    // pending the ceiling may rise again (raise()), within what the process
    // can still give; only past that does V8's OOM end the process.
    value->AddNearHeapLimitCallback([](void *opaque, size_t current, size_t) -> size_t {
      auto &budget = *static_cast<MemoryBudget *>(opaque);
      budget.cause->store(LimitCause::MEMORY);
      budget.isolate->TerminateExecution();
      return budget.raise(current);
    }, this);
  }
  // The new ceiling. The heap can already be past the old one: young large
  // objects are promoted without a limit check (cwkd-p40, v138: a structured
  // clone of three maximum strings had 2.0 GiB live at a 1313 MiB ceiling, so
  // raising the ceiling itself by one maximum object left no room for the
  // third and V8's OOM ended the process). So the room is counted from what is
  // in use. The first raise is one maximum object, whatever else is in use. A
  // later one (the termination requested at the first is still pending) grants
  // at most what is left under the process memory maximum and, with one
  // pointer cage for every isolate of the process, in that cage (less the
  // 256 MiB its other isolates keep); with nothing left the ceiling stays.
  // What other isolates' raises hold (ResourceBudget::lent) is not left. The
  // headroom granted above this isolate's heap is lent until its call's
  // execution ends. A replacement registration and a raise consult the same
  // counters under the budget lock.
  size_t raise(size_t current) {
    v8::HeapStatistics stats;
    isolate->GetHeapStatistics(&stats);
    const uint64_t used = stats.total_heap_size();
    // With a V8 sandbox per isolate group the isolate's heap has a pointer cage of its own, which
    // bounds every raise whatever the process budget has left (with one shared cage its remainder
    // over all isolates does, below: v138 §8.1's interface with r50's S3a).
    const uint64_t wanted = ownCageCeiling(kj::max(uint64_t(current), used) + MAX_SINGLE_HEAP_OBJECT);
    const bool first = ceilingRaised.load() == 0;
    if (!first && !reservation) return current;
    if (!reservation) {
      ceilingRaised.fetch_add(1);
      return wanted;
    }
    auto &resources = *reservation->budget;
    std::lock_guard lock(resources.mutex);
    const auto others = resources.lent - lentHere;
    auto ceiling = wanted;
    if (!first) {
      uint64_t room = resources.memoryMaximum -
          kj::min(resources.memoryReserved.load() + others, resources.memoryMaximum);
      if (resources.addressMaximum != UINT64_MAX) {
        const auto cage = resources.addressMaximum + MAX_SINGLE_HEAP_OBJECT;
        room = kj::min(room, cage - kj::min(resources.addressReserved.load() + others, cage));
      }
      ceiling = kj::min(ceiling, used + room);
    }
    if (ceiling <= current) return current;
    lentHere = ceiling - used;
    resources.lent = others + lentHere;
    ceilingRaised.fetch_add(1);
    return ceiling;
  }
  static uint64_t ownCageCeiling(uint64_t ceiling) {
    return v8::IsolateGroup::CanCreateNewGroups() ? kj::min(ceiling, OWN_CAGE_HEAP) : ceiling;
  }
  void returnLent() {
    if (lentHere == 0 || !reservation) return;
    auto& resources = *reservation->budget;
    std::lock_guard lock(resources.mutex);
    resources.lent -= lentHere;
    lentHere = 0;
  }
  uint64_t lentHere = 0;  // this isolate's share of ResourceBudget::lent (its lock's thread only)
};

// Only native state, a thread CPU clock and V8's thread-safe termination API
// are shared with the watchdog. Promises and IoContexts remain on the call
// thread.
class CpuWatchdog {
 public:
  struct Entry {
    clockid_t clock;
    v8::Isolate *isolate;
    std::atomic<LimitCause> &cause;
    LimitState memory;
    uint64_t started, allowance, due, terminatedAt = 0, previousCpu = 0;
    // The execution's identity, owned by the call or startup that outlives the entry.
    kj::StringPtr identity;
    uint32_t cpuMs, memoryMiB;
    bool culpritReported = false;
  };
  static CpuWatchdog &instance() {
    static auto *watchdog = new CpuWatchdog();
    return *watchdog;
  }
  void add(Entry &entry) {
    std::lock_guard lock(mutex);
    entries.push_back(&entry);
    changed.notify_one();
  }
  void remove(Entry &entry) {
    std::lock_guard lock(mutex);
    auto found = std::find(entries.begin(), entries.end(), &entry);
    KJ_REQUIRE(found != entries.end());
    entries.erase(found);
  }
  bool switchAccount(Entry &entry, uint64_t started, uint64_t used, kj::StringPtr identity,
      bool sqlDeadline) {
    std::lock_guard lock(mutex);
    if (entry.cause.load() != LimitCause::NONE) return false;
    const uint64_t maximum = uint64_t(entry.cpuMs) * 1000000;
    entry.started = started;
    entry.previousCpu = used;
    entry.allowance = maximum == 0 || sqlDeadline ? UINT64_MAX : maximum - std::min(maximum, used);
    entry.due = nowNs() + (entry.allowance == UINT64_MAX ? 1000000 : entry.allowance);
    entry.identity = identity;
    changed.notify_one();
    return true;
  }

 private:
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<Entry *> entries;
  std::thread thread;
  CpuWatchdog(): thread([this] { run(); }) {}
  void run() {
    pthread_setname_np(pthread_self(), "cwkd-limits");
    std::unique_lock lock(mutex);
    for (;;) {
      auto now = nowNs();
      uint64_t next = now + 1000000000;
      for (auto *entry: entries) {
        if (now < entry->due && entry->memory->load() == LimitCause::NONE) {
          next = std::min(next, entry->due);
          continue;
        }
        auto used = nowNs(entry->clock) - entry->started;
        if (entry->memory->load() == LimitCause::MEMORY) entry->cause.store(LimitCause::MEMORY);
        if (entry->allowance != UINT64_MAX && used >= entry->allowance) {
          auto expected = LimitCause::NONE;
          entry->cause.compare_exchange_strong(expected, LimitCause::CPU);
        }
        if (entry->cause.load() != LimitCause::NONE) {
          if (entry->terminatedAt == 0) {
            entry->isolate->TerminateExecution();
            entry->terminatedAt = now;
          } else if (!entry->culpritReported && now - entry->terminatedAt >= 100000000) {
            entry->culpritReported = writeLimitRow("culprit", entry->identity,
                used + entry->previousCpu, entry->cpuMs, entry->memoryMiB);
          }
          entry->due = now + 1000000;
        } else {
          entry->due = now +
              std::max<uint64_t>(
                  100000, entry->allowance == UINT64_MAX ? 1000000 : entry->allowance - used);
        }
        next = std::min(next, entry->due);
      }
      if (entries.empty())
        changed.wait(lock);
      else {
        auto sampled = nowNs();
        changed.wait_for(lock, std::chrono::nanoseconds(next > sampled ? next - sampled : 0));
      }
    }
  }
};

struct CpuExecution {
  std::atomic<LimitCause> &cause;
  std::shared_ptr<MemoryBudget> memory;
  std::atomic<LimitCause> *previousCause;
  OomReportScope oom;
  CpuWatchdog::Entry entry;
  bool registered;
  bool finished = false;
  CpuExecution(v8::Isolate *isolate,
      std::atomic<LimitCause> &cause,
      LimitState memoryState,
      std::shared_ptr<MemoryBudget> memory,
      uint64_t used,
      uint32_t cpuMs,
      uint32_t memoryMiB,
      kj::StringPtr identity,
      const kj::String *oomReport = nullptr)
      : cause(cause),
        memory(std::move(memory)),
        previousCause(this->memory ? this->memory->currentCause : nullptr),
        oom(oomReport ? OomReportScope(*oomReport, this->memory ? &this->memory->heap : nullptr)
                      : OomReportScope(identity, memoryMiB, this->memory ? &this->memory->heap : nullptr)),
        entry{.isolate = isolate,
          .cause = cause,
          .memory = std::move(memoryState),
          .allowance = cpuMs == 0                ? UINT64_MAX
              : uint64_t(cpuMs) * 1000000 > used ? uint64_t(cpuMs) * 1000000 - used
                                                 : 0,
          .previousCpu = used,
          .identity = identity,
          .cpuMs = cpuMs,
          .memoryMiB = memoryMiB},
        registered(cpuMs != 0 || memoryMiB != 0) {
    KJ_REQUIRE(pthread_getcpuclockid(pthread_self(), &entry.clock) == 0);
    entry.started = nowNs(entry.clock);
    entry.due = nowNs() + (entry.allowance == UINT64_MAX ? 1000000 : entry.allowance);
    if (registered) CpuWatchdog::instance().add(entry);
    if (this->memory) {
      this->memory->currentCause = &cause;
      if (this->memory->pendingPressure.exchange(false)) {
        auto expected = LimitCause::NONE;
        if (cause.compare_exchange_strong(expected, LimitCause::PROCESS_MEMORY) &&
            this->memory->reservation)
          this->memory->reservation->budget->pressureTerminations.fetch_add(1);
        isolate->TerminateExecution();
      }
    }
  }
  void finish() {
    if (finished) return;
    finished = true;
    if (registered) {
      CpuWatchdog::instance().remove(entry);
      registered = false;
    }
    // remove() synchronizes with the watchdog before a new invocation can take
    // this isolate lock. A late termination must not leak into that invocation.
    if (memory) {
      if (cause.load() == LimitCause::PROCESS_MEMORY) {
        entry.isolate->CancelTerminateExecution();
        entry.isolate->LowMemoryNotification();
      }
      // The termination (or the call) has taken effect: what a raise lent is
      // given back.
      memory->returnLent();
      memory->observe();
      memory->currentCause = previousCause;
    }
    if (entry.terminatedAt != 0 || cause.load() != LimitCause::NONE ||
        entry.memory->load() != LimitCause::NONE)
      entry.isolate->CancelTerminateExecution();
  }
  ~CpuExecution() noexcept(false) {
    finish();
  }
};

struct IsolateLimits final: IsolateLimitEnforcer {
  // `group` is the group the isolate is created in (tryNewIsolateGroup()).
  IsolateLimits(uint32_t cpuMs,
      uint32_t memoryMiB,
      kj::String identity,
      std::shared_ptr<ResourceReservation> reservation,
      v8::IsolateGroup groupParam)
      : cpuMs(cpuMs),
        memoryMiB(memoryMiB),
        identity(kj::mv(identity)),
        group(kj::mv(groupParam)) {
    memory = std::make_shared<MemoryBudget>(memoryMiB, memoryState, std::move(reservation), group);
  }
  uint32_t cpuMs, memoryMiB;
  kj::String identity;
  v8::IsolateGroup group;
  // The name in this isolate's [coworkerd-sandbox] row.
  std::string sandboxName = "threaded";
  bool durableEvents = false;
  LimitState memoryState = std::make_shared<std::atomic<LimitCause>>(LimitCause::NONE);
  std::shared_ptr<MemoryBudget> memory;
  mutable std::atomic<LimitCause> startupCause{LimitCause::NONE};
  mutable uint64_t startupCpu = 0;
  // Set before this isolate compiles tenant source.
  v8::Isolate *isolate = nullptr;
  v8::Isolate::CreateParams getCreateParams() override {
    v8::Isolate::CreateParams params;
    if (memory) {
      auto hard = memory->maximum + MAX_SINGLE_HEAP_OBJECT + fragmentationReserve(memory->maximum);
      params.constraints.ConfigureDefaultsFromHeapSize(0, hard);
      params.constraints.set_max_old_generation_size_in_bytes(hard);
      params.array_buffer_allocator_shared = memory;
    }
    return params;
  }
  // Called with the isolate entered (Worker::Isolate, sourceJsonFailure): with the V8 sandbox,
  // its own sandbox must be secure before it runs any code.
  void customizeIsolate(v8::Isolate *value) override {
    isolate = value;
    server::requireSecureIsolateSandbox(value, sandboxName.c_str());
    if (memory) memory->install(value);
    if (!durableEvents) value->SetFatalErrorHandler(&nativeFatal);
    if (durableEvents) {
      // V8 restores continuation-preserved data before kBefore and retains it
      // through kAfter. The full amount belongs to the reaction's event.
      value->SetPromiseHook(
          [](v8::PromiseHookType type, v8::Local<v8::Promise>, v8::Local<v8::Value>) {
        auto *isolate = v8::Isolate::GetCurrent();
        if (type == v8::PromiseHookType::kBefore) jsg::AsyncContextFrame::notifyObserver(isolate);
        if (type == v8::PromiseHookType::kAfter)
          jsg::AsyncContextFrame::notifyObserver(isolate, false);
      });
    }
  }
  ActorCacheSharedLruOptions getActorCacheLruOptions() override {
    return {};
  }
  struct Startup {
    const IsolateLimits &owner;
    kj::OneOf<kj::Exception, kj::Duration> &result;
    uint64_t localCpu = 0;
    std::atomic<LimitCause> localCause{LimitCause::NONE};
    uint64_t &totalCpu;
    std::atomic<LimitCause> &cause;
    bool scoped;
    kj::String identity;
    uint64_t start = nowNs(CLOCK_THREAD_CPUTIME_ID);
    FatalInvocation fatal;
    CpuExecution execution;
    Startup(const IsolateLimits &owner,
        jsg::Lock &js,
        kj::OneOf<kj::Exception, kj::Duration> &result,
        bool startup)
        : Startup(owner, js, result, startup, scopedExecutionIdentity(owner, js.v8Isolate)) {}
    Startup(const IsolateLimits &owner,
        jsg::Lock &js,
        kj::OneOf<kj::Exception, kj::Duration> &result,
        bool startup,
        kj::Maybe<kj::StringPtr> scopedIdentity)
        : owner(owner),
          result(result),
          totalCpu(startup ? owner.startupCpu : localCpu),
          cause(startup ? owner.startupCause : localCause),
          scoped(scopedIdentity != kj::none),
          identity(kj::str(scopedIdentity.orDefault(executionIdentity(owner.identity)))),
          fatal(scoped ? js.v8Isolate : nullptr, identity),
          execution(js.v8Isolate,
              cause,
              owner.memoryState,
              owner.memory,
              totalCpu,
              owner.cpuMs,
              owner.memoryMiB,
              identity) {}
    ~Startup() noexcept(false) {
      execution.finish();
      totalCpu += nowNs(CLOCK_THREAD_CPUTIME_ID) - start;
      auto exceeded =
          owner.memoryState->load() == LimitCause::MEMORY ? LimitCause::MEMORY : cause.load();
      if (exceeded == LimitCause::NONE)
        result = int64_t(totalCpu) * kj::NANOSECONDS;
      else {
        result = limitException(exceeded);
        writeLimitRow(exceeded == LimitCause::CPU        ? "terminated"
                : exceeded == LimitCause::PROCESS_MEMORY ? "process-memory-pressure"
                                                         : "memory-terminated",
            identity, totalCpu, owner.cpuMs, owner.memoryMiB, owner.memory->peakUsedBytes.load());
      }
    }
  };
  kj::Own<void> enterStartupJs(
      jsg::Lock &js, kj::OneOf<kj::Exception, kj::Duration> &result) const override {
    return kj::heap<Startup>(*this, js, result, true);
  }
  kj::Own<void> enterStartupPython(
      jsg::Lock &js, kj::OneOf<kj::Exception, kj::Duration> &r) const override {
    return enterStartupJs(js, r);
  }
  kj::Own<void> enterDynamicImportJs(
      jsg::Lock &js, kj::OneOf<kj::Exception, kj::Duration> &r) const override {
    return kj::heap<Startup>(*this, js, r, false);
  }
  kj::Own<void> enterLoggingJs(
      jsg::Lock &js, kj::OneOf<kj::Exception, kj::Duration> &r) const override {
    return kj::heap<Startup>(*this, js, r, false);
  }
  kj::Own<void> enterInspectorJs(
      jsg::Lock &js, kj::OneOf<kj::Exception, kj::Duration> &r) const override {
    return kj::heap<Startup>(*this, js, r, false);
  }
  void completedRequest(kj::StringPtr) const override {}
  bool exitJs(jsg::Lock &) const override {
    return memoryState->load() == LimitCause::MEMORY;
  }
  void reportMetrics(IsolateObserver &) const override {}
  bool hasExcessivelyExceededHeapLimit() const override {
    return memoryState->load() == LimitCause::MEMORY;
  }
  const TrackedWasmInstanceList &getTrackedWasmInstances() const override {
    return instances;
  }
  TrackedWasmInstanceList instances;
};

constexpr kj::Exception::DetailTypeId LOAD_STAGE_DETAIL = 0xd6eae4a9047382b1ull;

struct Reporter final: Worker::ValidationErrorReporter {
  std::set<std::string> httpEntrypoints, entrypoints, actorClasses;
  kj::Maybe<kj::String> failure;
  void addError(kj::String error) override {
    if (failure == kj::none) failure = kj::mv(error);
  }
  void requireValid() {
    KJ_IF_SOME(error, failure) {
      kj::throwRecoverableException(
          kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::str(error)));
    }
  }
  void addEntrypoint(kj::Maybe<kj::StringPtr> name, kj::Array<kj::String> handlers) override {
    std::string key = "default";
    KJ_IF_SOME(value, name) {
      key = value.cStr();
    }
    entrypoints.insert(key);
    for (auto &handler: handlers)
      if (handler == "fetch") httpEntrypoints.insert(key);
  }
  void addActorClass(kj::StringPtr name) override {
    actorClasses.insert(name.cStr());
  }
  void addWorkflowClass(kj::StringPtr, kj::Array<kj::String>) override {}
};

class BusyIsolateObserver final: public JsgIsolateObserver {
 public:
  explicit BusyIsolateObserver(const kj::MonotonicClock &clock): window(clock) {}

  void onLockAcquired() noexcept override {
    window.acquired();
  }
  void onLockReleasing() noexcept override {
    window.releasing();
  }

  threaded::IsolateBusyWindow window;
};

struct Slot {
  capnp::MallocMessageBuilder configArena, flagsArena;
  api::MemoryCacheProvider cache;
  std::mutex mutex;
  std::atomic<bool> complete{false};
  std::atomic<bool> retryableLoad{false};
  std::shared_ptr<ResourceReservation> reservation;
  kj::Maybe<kj::Own<BusyIsolateObserver>> busy;
  std::string workerId, versionId, planRevision, isolateLabel;
  uint64_t instance = 0;
  std::atomic<uint64_t> lastUseNs{nowNs()};
  std::atomic<uint32_t> contexts{0};
  kj::Maybe<kj::Own<Worker>> worker;
  kj::Own<IsolateLimits> limitsOwner;
  IsolateLimits *limits = nullptr;
  threaded::LoadStage loadStage = threaded::LoadStage::LOAD;
  kj::Maybe<kj::Exception> error;
  std::vector<kj::Own<kj::CrossThreadPromiseFulfiller<void>>> waiters;
  std::set<std::string> httpEntrypoints, entrypoints, actorClasses;
  std::vector<std::string> loopbacks, loopbackActors;
  std::atomic<uint> active{0}, maxActive{0}, creates{0};
  std::function<void(bool)> homeTurn;
  std::function<void(Worker::Actor &)> resetDurableRoot;
};

struct Entry {
  uint64_t startNs, endNs, cpuNs;
  uintptr_t isolate;
};

struct Measurement {
  kj::Vector<Entry> entries;
  uint64_t totalCpu = 0;
  std::atomic<LimitCause> limitCause{LimitCause::NONE};
  std::shared_ptr<ResourcePreparation> preparation;
  kj::Maybe<kj::String> unsupportedFeature;
  // An HTTP call's response end: its body sent to the end, or the client gone
  // (Invocation::request). Without it the drain limit runs from its call.
  kj::Maybe<kj::ForkedPromise<void>> responseEnded;
  kj::Maybe<kj::ForkedPromise<void>> requestLeaseEnded;
};

void watchRequestLease(Measurement &measurement, kj::Maybe<threaded::rpc::NativeLeaseControl::Client> lease) {
  KJ_IF_SOME(control, lease) {
    auto ended = control.whenEndedRequest().send().ignoreResult().catch_([](kj::Exception &&) {});
    measurement.requestLeaseEnded = ended.attach(kj::mv(control)).fork();
  }
}

struct DurableEventCpu {
  DurableEventCpu(kj::String identity, uint32_t memoryMiB)
      : identity(kj::mv(identity)),
        oomReport(oomReportText(this->identity, memoryMiB)) {}

  kj::String identity, oomReport;
  kj::Maybe<kj::Weak<IoContext::IncomingRequest>> request;
  server::DurableCpuAccount account;
  std::shared_ptr<SqlExecutionBudget> sqlDeadline;
};

class DurableEventObserver final: public RequestObserver {
 public:
  explicit DurableEventObserver(std::shared_ptr<DurableEventCpu> cpu): cpu(std::move(cpu)) {}
  std::shared_ptr<DurableEventCpu> cpu;
};

struct RequestLimits final: LimitEnforcer {
  RequestLimits(Slot &slot, Measurement &measurement, Timer &timer, kj::String identity)
      : slot(slot),
        measurement(measurement),
        timer(timer),
        identity(kj::mv(identity)),
        oomReport(oomReportText(this->identity, slot.limits->memoryMiB)),
        failure(failurePair.promise.fork()) {}
  ~RequestLimits() noexcept(false) {
    writeLimitRow("completed", identity, measurement.totalCpu, slot.limits->cpuMs,
        slot.limits->memoryMiB, slot.limits->memory->peakUsedBytes.load());
  }
  struct Scope {
    Scope(RequestLimits &owner, IoContext &context, v8::Isolate *isolate)
        : owner(owner),
          context(context),
          slot(owner.slot),
          measurement(owner.measurement),
          isolate(isolate),
          start(nowNs()),
          cpu(nowNs(CLOCK_THREAD_CPUTIME_ID)),
          fatal(slot.resetDurableRoot ? nullptr : isolate, owner.identity),
          execution(isolate,
              measurement.limitCause,
              slot.limits->memoryState,
              slot.limits->memory,
              slot.resetDurableRoot ? 0 : measurement.totalCpu,
              slot.limits->cpuMs,
              slot.limits->memoryMiB,
              owner.identity,
              &owner.oomReport) {
      uint expected = 0;
      KJ_REQUIRE(slot.active.compare_exchange_strong(expected, 1), "overlapping JS entries");
      slot.maxActive.store(1);
      if (slot.homeTurn) slot.homeTurn(true);
      if (slot.resetDurableRoot) {
        owner.scope = this;
        observer.emplace(this, &contextChanged, owner.eventKey());
      }
    }
    ~Scope() noexcept(false) {
      KJ_DEFER(if (slot.homeTurn) slot.homeTurn(false));
      observer.reset();
      owner.scope = nullptr;
      if (slot.resetDurableRoot) {
        charge(nowNs(CLOCK_THREAD_CPUTIME_ID));
        if (measurement.limitCause.load() == LimitCause::CPU) owner.failedEvent = event;
      }
      execution.finish();
      auto used = nowNs(CLOCK_THREAD_CPUTIME_ID) - cpu;
      measurement.totalCpu += used;
      if (owner.sqlBudget && owner.sqlBudget->getCause() == SqlExecutionBudget::Cause::CPU) {
        auto expected = LimitCause::NONE;
        measurement.limitCause.compare_exchange_strong(expected, LimitCause::CPU);
      }
      owner.sqlBudget.reset();
      if (traceForTest()) {
        char row[320];
        int size = snprintf(row, sizeof(row),
            "[threaded-js] "
            "{\"tid\":%ld,\"isolate\":%llu,\"startNs\":%llu,\"endNs\":"
            "%llu,\"cpuNs\":%llu}\n",
            syscall(SYS_gettid), (unsigned long long)reinterpret_cast<uintptr_t>(isolate),
            (unsigned long long)start, (unsigned long long)nowNs(), (unsigned long long)used);
        if (size > 0) (void)!write(STDERR_FILENO, row, size);
      }
      slot.active.fetch_sub(1);
      owner.publish(context);
    }
    server::DurableCpuAccount &account() {
      return event ? event->account : turnOverhead;
    }
    void charge(uint64_t now) {
      auto &charged = account();
      charged.charge(segmentStart, now);
      const auto maximum = uint64_t(slot.limits->cpuMs) * 1000000;
      if ((maximum != 0 && !(event && event->sqlDeadline) && charged.remaining(maximum) == 0) ||
          (owner.sqlBudget && owner.sqlBudget->getCause() == SqlExecutionBudget::Cause::CPU)) {
        auto expected = LimitCause::NONE;
        measurement.limitCause.compare_exchange_strong(expected, LimitCause::CPU);
      }
      segmentStart = now;
      owner.sqlBudget.reset();
    }
    static void contextChanged(void *ptr, v8::Isolate *isolate, bool useFrame) noexcept {
      auto &self = *static_cast<Scope *>(ptr);
      if (self.isolate != isolate || self.measurement.limitCause.load() != LimitCause::NONE) return;
      // This also runs inside V8's promise hook. Only native bookkeeping
      // and reads of the frame's opaque token are allowed here.
      KJ_IF_SOME(exception, kj::runCatchingExceptions([&] {
        self.selectEvent(useFrame ? self.owner.eventFromFrame(isolate) : nullptr);
      })) {
        KJ_LOG(ERROR, "cannot account for durable event continuation", exception);
        self.measurement.limitCause.store(LimitCause::CPU);
        isolate->TerminateExecution();
      }
    }
    void selectEvent(std::shared_ptr<DurableEventCpu> next) {
      if (event == next) return;
      const auto now = nowNs(CLOCK_THREAD_CPUTIME_ID);
      charge(now);
      if (measurement.limitCause.load() == LimitCause::CPU) {
        isolate->TerminateExecution();
        return;
      }
      const auto used = (next ? next->account : turnOverhead).consumed();
      if (execution.registered &&
          !CpuWatchdog::instance().switchAccount(
              execution.entry, now, used, next ? next->identity : owner.identity,
              next && next->sqlDeadline)) {
        return;
      }
      currentOomReport = next ? next->oomReport.cStr() : owner.oomReport.cStr();
      event = std::move(next);
    }
    RequestLimits &owner;
    IoContext &context;
    Slot &slot;
    Measurement &measurement;
    v8::Isolate *isolate;
    uint64_t start, cpu;
    FatalInvocation fatal;
    CpuExecution execution;
    uint64_t segmentStart = cpu;
    server::DurableCpuAccount turnOverhead;
    std::shared_ptr<DurableEventCpu> event;
    std::optional<jsg::AsyncContextFrame::ChangeObserver> observer;
  };
  kj::Own<void> enterJs(jsg::Lock &js, IoContext &context) override {
    requireLimitsNotExceeded();
    // runImpl synchronizes before waiting for the isolate; sample again after
    // that wait, while its lock is held and before this turn enters JavaScript.
    timer.syncTime();
    auto scope = kj::heap<Scope>(*this, context, js.v8Isolate);
#ifdef COWORKERD_FATAL_TEST_BUILD
    if (!slot.resetDurableRoot) injectSyntheticCrash(slot.workerId.c_str());
#endif
    return scope;
  }
  void topUpActor() override {}
  kj::Own<void> enterActorEvent(jsg::Lock &js, IoContext::IncomingRequest &request) override {
    if (!slot.resetDurableRoot) return {};
    auto *observer = dynamic_cast<DurableEventObserver *>(&request.getMetrics());
    KJ_REQUIRE(observer != nullptr, "durable event has no CPU account");
    auto &event = observer->cpu;
    if (event->request == kj::none) event->request = request.addWeak();
    auto token = jsg::wrapOpaque(js.v8Context(), EventToken{eventOwner, event});
    return kj::heap<jsg::AsyncContextFrame::StorageScope>(js, eventKey().addRef(), js.v8Ref(token));
  }
  kj::Maybe<IoContext::IncomingRequest &> getActorEventRequest() override {
    if (scope == nullptr || !scope->event) return kj::none;
    KJ_IF_SOME(request, scope->event->request) { return request.tryGet(); }
    return kj::none;
  }
  struct EventToken {
    const std::weak_ptr<int> owner;
    const std::shared_ptr<DurableEventCpu> event;
  };
  static const kj::Arc<jsg::AsyncContextFrame::StorageKey> &eventKey() {
    static const auto key = kj::arc<jsg::AsyncContextFrame::StorageKey>();
    return key;
  }
  std::shared_ptr<DurableEventCpu> eventFromFrame(v8::Isolate *isolate) {
    v8::HandleScope handles(isolate);
    KJ_IF_SOME(frame, jsg::AsyncContextFrame::current(isolate)) {
      KJ_IF_SOME(value, frame.get(*eventKey())) {
        auto handle = value.getHandle(jsg::Lock::from(isolate));
        if (handle->IsUndefined()) return nullptr;
        const auto &token = jsg::unwrapOpaqueRef<EventToken>(isolate, handle);
        if (token.owner.lock() == eventOwner) return token.event;
      }
    }
    return nullptr;
  }
  std::shared_ptr<SqlExecutionBudget> getSqlExecutionBudget() override {
    if (!scope) return {};
    if (scope->event && scope->event->sqlDeadline) return scope->event->sqlDeadline;
    if (slot.limits->cpuMs == 0) return {};
    if (!sqlBudget) {
      const uint64_t maximum = uint64_t(slot.limits->cpuMs) * 1000000;
      const auto left = scope->account().remaining(maximum);
      sqlBudget = std::make_shared<SqlExecutionBudget>(maximum, scope->segmentStart + left);
    }
    return sqlBudget;
  }
  void newSubrequest(bool) override {}
  void newKvRequest(KvOpType) override {
    unsupported("kv");
  }
  void newAnalyticsEngineRequest() override {
    unsupported("analytics");
  }
  kj::Promise<void> limitDrain() override {
    // This invocation keeps its CPU and heap limits while draining. A live
    // request may end the drain sooner than the per-invocation ceiling. That
    // ceiling starts once the response body finishes or the client disconnects
    // (v138 section 4). KJ's timer does not advance while JavaScript runs, so
    // take the deadline from the monotonic clock (or the timer if it is ahead).
    if (drainLimitDisabledForTest()) return kj::NEVER_DONE;
    auto deadline = [this]() {
      return timer.timer.atTime(kj::max(kj::systemPreciseMonotonicClock().now(), timer.timer.now()) + WAIT_UNTIL_LIMIT);
    };
    kj::Promise<void> wait = nullptr;
    KJ_IF_SOME(ended, measurement.responseEnded) {
      wait = ended.addBranch().then(kj::mv(deadline));
    } else {
      wait = deadline();
    }
    KJ_IF_SOME(ended, measurement.requestLeaseEnded) {
      return kj::mv(wait).exclusiveJoin(ended.addBranch());
    }
    return wait;
  }
  kj::Promise<void> limitScheduled() override {
    return kj::NEVER_DONE;
  }
  kj::Duration getAlarmLimit() override {
    return coworkerdAlarmLimit();
  }
  size_t getBufferingLimit() override {
    return 1 << 20;
  }
  LimitCause cause() {
    return slot.limits->memoryState->load() == LimitCause::MEMORY ? LimitCause::MEMORY
                                                                  : measurement.limitCause.load();
  }
  void publish(IoContext &context) {
    if (!slot.resetDurableRoot && slot.limits->cpuMs != 0 &&
        measurement.totalCpu >= uint64_t(slot.limits->cpuMs) * 1000000) {
      auto expected = LimitCause::NONE;
      measurement.limitCause.compare_exchange_strong(expected, LimitCause::CPU);
    }
    auto value = cause();
    if (value == LimitCause::NONE || published) return;
    published = true;
    writeLimitRow(value == LimitCause::MEMORY     ? "memory-terminated"
            : value == LimitCause::PROCESS_MEMORY ? "process-memory-pressure"
                                                  : "terminated",
        failedEvent ? failedEvent->identity : identity,
        failedEvent ? failedEvent->account.consumed() : measurement.totalCpu, slot.limits->cpuMs,
        slot.limits->memoryMiB, slot.limits->memory->peakUsedBytes.load());
    if (failurePair.fulfiller->isWaiting()) failurePair.fulfiller->reject(limitException(value));
    if (value == LimitCause::CPU && slot.resetDurableRoot) {
      KJ_IF_SOME(actor, context.getActor()) {
        slot.resetDurableRoot(actor);
      }
    }
    context.abort(limitException(value));
  }
  kj::Maybe<EventOutcome> getLimitsExceeded() override {
    auto value = cause();
    if (value == LimitCause::NONE) return kj::none;
    if (value == LimitCause::PROCESS_MEMORY) return EventOutcome::EXCEPTION;
    return value == LimitCause::MEMORY ? EventOutcome::EXCEEDED_MEMORY : EventOutcome::EXCEEDED_CPU;
  }
  kj::Promise<void> onLimitsExceeded() override {
    return failure.addBranch();
  }
  void setCpuLimitNearlyExceededCallback(kj::Function<void()>) override {}
  void requireLimitsNotExceeded() override {
    if (cause() != LimitCause::NONE) kj::throwFatalException(limitException(cause()));
  }
  void reportMetrics(RequestObserver &) override {}
  kj::Duration consumeTimeElapsedForPeriodicLogging() override {
    return 0 * kj::SECONDS;
  }
  size_t getSqliteMemoryUsage() const override {
    // Report the copy's committed SQLite heap reservation.
    return slot.reservation->sqliteMemoryCharge();
  }
  Slot &slot;
  Measurement &measurement;
  Timer &timer;
  kj::String identity;
  kj::String oomReport;  // formatted once for the call's every JavaScript entry
  kj::PromiseFulfillerPair<void> failurePair = kj::newPromiseAndFulfiller<void>();
  kj::ForkedPromise<void> failure;
  bool published = false;
  // The weak token distinguishes actor contexts even if an old frame is restored later.
  std::shared_ptr<int> eventOwner = slot.resetDurableRoot ? std::make_shared<int>() : nullptr;
  std::shared_ptr<DurableEventCpu> failedEvent;
  Scope *scope = nullptr;
  std::shared_ptr<SqlExecutionBudget> sqlBudget;
};

kj::StringPtr executionIdentity(kj::StringPtr fallback) {
  if (IoContext::hasCurrent()) {
    return static_cast<RequestLimits &>(IoContext::current().getLimitEnforcer()).identity;
  }
  return fallback;
}

// Whom a fatal error belongs to while `owner`'s isolate runs a startup, logging or dynamic import
// entry, from the facts at entry: none in durable roles, or on an isolate other than the Worker's
// own; else the enclosing scope of this isolate (the load, or the call); else the call of the
// current I/O context when that call runs this Worker (`callOwner`); else the Worker's load
// identity, since the isolate runs only that Worker's code. The node attributes a record only to
// a dispatch of the named invocation in the same process generation.
kj::Maybe<kj::StringPtr> entryFatalIdentity(const IsolateLimits &owner,
    v8::Isolate *entered,
    kj::Maybe<kj::StringPtr> enclosing,
    const IsolateLimits *callOwner,
    kj::StringPtr callIdentity) {
  if (owner.durableEvents) return kj::none;
  if (owner.isolate != nullptr && owner.isolate != entered) return kj::none;
  KJ_IF_SOME(identity, enclosing) return identity;
  if (callOwner == &owner) return callIdentity;
  return owner.identity.asPtr();
}

kj::Maybe<kj::StringPtr> scopedExecutionIdentity(const IsolateLimits &owner, v8::Isolate *isolate) {
  kj::Maybe<kj::StringPtr> enclosing;
  if (const auto *scope = FatalInvocation::forIsolate(isolate)) enclosing = scope->getIdentity();
  const IsolateLimits *callOwner = nullptr;
  kj::StringPtr callIdentity;
  KJ_IF_SOME(context, IoContext::tryCurrent()) {
    if (auto *request = dynamic_cast<RequestLimits *>(&context.getLimitEnforcer())) {
      callOwner = request->slot.limits;
      callIdentity = request->identity;
    }
  }
  return entryFatalIdentity(owner, isolate, enclosing, callOwner, callIdentity);
}

void noteUnsupported(kj::StringPtr feature) {
  if (IoContext::hasCurrent()) {
    auto &limits = static_cast<RequestLimits &>(IoContext::current().getLimitEnforcer());
    limits.measurement.unsupportedFeature = kj::str(feature);
  }
}

struct Errors final: kj::TaskSet::ErrorHandler {
  void taskFailed(kj::Exception &&error) override {
    KJ_LOG(ERROR, "threaded background task failed");
  }
};

// The drain limit above normally ends every waitUntil task. A call thread
// still never waits longer for background work after its invocation ended:
// past this bound the remaining tasks are canceled and the thread returns.
kj::Promise<void> settleBackground(kj::Timer &timer, kj::TaskSet &tasks) {
  return timer.timeoutAfter(BACKGROUND_BACKSTOP, tasks.onEmpty())
      .catch_([&tasks](kj::Exception &&) { tasks.clear(); });
}

struct DurableSessionScope;

enum class SocketParkResult { REFUSED, PARKED, UNKNOWN };

const char *durableLifecycleTestDirectory() {
  static const auto directory = getenv("COWORKERD_DO_TEST_LIFECYCLE_DIR");
  return directory;
}

uint32_t readDurableLifecycleSetting(kj::StringPtr name) {
  const auto *directory = durableLifecycleTestDirectory();
  if (directory == nullptr) return 0;
  auto path = kj::str(directory, "/", name);
  int fd = open(path.cStr(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
  if (fd < 0) return 0;
  KJ_DEFER(close(fd));
  char bytes[16];
  auto size = read(fd, bytes, sizeof(bytes));
  if (size <= 0) return 0;
  uint32_t value = 0;
  auto parsed = std::from_chars(bytes, bytes + size, value);
  return parsed.ec == std::errc() ? value : 0;
}

bool refuseCopyReservationForTest() {
  const auto request = readDurableLifecycleSetting("copy-reservation-refusal");
  static std::atomic<uint32_t> lastRefusal = 0;
  auto last = lastRefusal.load();
  // A fixture request is consumed once, even when several homes attempt a cold copy.
  while (request > last) {
    if (lastRefusal.compare_exchange_weak(last, request)) return true;
  }
  return false;
}

server::DurableIdleLifecycle::Time durableIdleNow() {
  auto now = server::DurableAdmission::Clock::now();
  const auto offset = readDurableLifecycleSetting("idle-offset-ms");
  if (offset <= 86'400'000) now += std::chrono::milliseconds(offset);
  return now;
}

struct DurableTreeContext;
void trimIdleObjectSqlite(DurableTreeContext &tree);
bool hasDurableWalRegistrations(const DurableTreeContext& tree);
class DurableActorHooks;

struct ThreadState {
  kj::AsyncIoContext io = kj::setupAsyncIo();
  Entropy entropy;
  capnp::ByteStreamFactory bytes;
  kj::HttpHeaderTable::Builder headerBuilder;
  kj::HttpHeaderId egressSource = headerBuilder.add("x-coworkerd-egress-source");
  kj::HttpHeaderId egressError = headerBuilder.add("x-coworkerd-egress-error");
  ThreadContext::HeaderIdBundle ids{headerBuilder};
  kj::HttpHeaderId placementCallId = headerBuilder.add("x-placement-call-id");
  capnp::HttpOverCapnpFactory http{bytes,
    capnp::HttpOverCapnpFactory::HeaderIdBundle(headerBuilder),
    capnp::HttpOverCapnpFactory::LEVEL_2};
  kj::Own<kj::HttpHeaderTable> headers = headerBuilder.build();
  ThreadContext context{io.provider->getTimer(), entropy, ids, http, bytes};
  Timer timer{io.provider->getTimer()};
  Errors errors;
  kj::AutoCloseFd decisionSpare{open("/dev/null", O_RDONLY | O_CLOEXEC)};
  bool rejectOnly = false;
  uint64_t durableHome = 0;
  // Shippers and SQLite's deferred close callbacks borrow this channel. Keep it alive
  // until the home has canceled its tasks and destroyed its actors and isolate copy.
  kj::Own<server::DurableWalChannel> wal;
  std::shared_ptr<Slot> durableSlot;
  // The cold load in flight on this home. Its waiters all want its definition, and it
  // ends with the last of them, so a canceled load is never left for a later tree.
  struct DurableLoad {
    std::string definition;
    kj::ForkedPromise<std::shared_ptr<Slot>> promise;
    uint waiters = 0;
    bool settled = false;
  };
  std::optional<DurableLoad> durableLoad;
  struct WalPreparation {
    explicit WalPreparation(kj::Promise<server::ThreadedWalLog::PreparedOpen> task)
        : ready(task.fork()) {}
    kj::ForkedPromise<server::ThreadedWalLog::PreparedOpen> ready;
  };
  struct SocketAdoption {
    explicit SocketAdoption(kj::Promise<void> promise): ready(promise.fork()) {}
    kj::ForkedPromise<void> ready;
  };
  std::map<uint64_t, std::weak_ptr<WalPreparation>> preparingWal;
  std::map<uint64_t, std::map<uint64_t, std::weak_ptr<DurableSessionScope>>> durableSessions;
  struct DurableRoot {
    kj::Own<Worker::Actor> actor;
    std::shared_ptr<DurableTreeContext> tree;
    DurableActorHooks *hooks;
    std::shared_ptr<server::DurableIdleLifecycle> idle;
    std::function<bool(bool)> facetsBusy;
    std::function<bool()> facetHasOrdinaryWebSocket;
    std::function<void(const kj::Exception *)> stopFacets;
    std::function<uint64_t()> rootOpenId;
    std::function<void(kj::StringPtr)> notifyReset;
    std::map<uint64_t, std::weak_ptr<DurableSessionScope>> sockets;
    bool resetting = false;
    bool closeRequested = false;
    bool idleClosing = false;
    bool cancelExpiredWork = false;
    bool admissionClosing = false;
    std::optional<server::DurableIdleLifecycle::Time> sqliteTrimmedForIdleSince;
    std::shared_ptr<class DurableNodeSockets> nodeSockets;
    kj::Function<bool()> hasLocalSockets;
    kj::Function<kj::Promise<SocketParkResult>()> parkSockets;
    kj::Maybe<kj::Promise<void>> parking;
    kj::Maybe<bool> parked;
    std::shared_ptr<SocketAdoption> adopting;
    bool socketsRestored = false;
    kj::Maybe<kj::Promise<void>> broken;
  };
  std::map<Worker::Actor *, uint64_t> durableActorTrees;
  std::map<uint64_t, DurableRoot> durableRoots;
  bool retiredActors = false;
  // A retired actor can still have a reference held by a settling facet or task.
  struct AdmissionWalClose {
    std::shared_ptr<DurableTreeContext> context;
    server::DurableAdmission::Time deadline;
  };
  std::map<uint64_t, AdmissionWalClose> admissionWalClosing;
  kj::TaskSet tasks{errors};

  ~ThreadState() noexcept(false) {
    // Cancel sessions while the roots and indexes are live. Dropping the roots can enqueue
    // drains from their facet sessions, so cancel those before destroying the task set.
    KJ_DEFER({
      durableRoots.clear();
      tasks.clear();
    });
    tasks.clear();
  }
};

Frankenvalue readNativeProps(ThreadState &state, rpc::Frankenvalue::Reader value);

using Json = capnp::JsonValue;
Json::Reader field(Json::Reader value, kj::StringPtr name) {
  if (value.isObject())
    for (auto member: value.getObject()) {
      if (member.getName() == name) return member.getValue();
    }
  return {};
}
kj::String text(Json::Reader value, kj::StringPtr name) {
  auto item = field(value, name);
  KJ_REQUIRE(item.isString(), "missing descriptor field", name);
  return kj::str(item.getString());
}
std::string str(Json::Reader value, kj::StringPtr name) {
  return text(value, name).cStr();
}
bool flag(Json::Reader value, kj::StringPtr name) {
  auto item = field(value, name);
  return item.isBoolean() && item.getBoolean();
}
kj::String encode(Json::Reader value) {
  return capnp::JsonCodec().encodeRaw(value);
}
struct JsonDoc {
  kj::String original;
  capnp::MallocMessageBuilder arena;
  explicit JsonDoc(kj::StringPtr input): original(kj::str(input)) {
    capnp::JsonCodec().decodeRaw(input.asArray(), arena.initRoot<Json>());
  }
  Json::Reader get() {
    return arena.getRoot<Json>();
  }
};

constexpr auto DATABASE_SQL_WALL_LIMIT = 30 * kj::SECONDS;

struct DurableRoute {
  server::DurableAdmission::Key key;
  server::DurableAdmission::Open open;
  std::string invocation;
  std::string uniqueKey, storageId;

  bool operator==(const DurableRoute &) const = default;
};

struct DurableIncoming {
  kj::AutoCloseFd socket;
  DurableRoute route;
  server::DurableAdmission::Ticket ticket;
  server::DurableAdmission::Time deadline;
};

uint64_t boundedDurableSetting(const char *name, uint64_t fallback, uint64_t maximum) {
  const auto value = configuredCount(name, fallback);
  KJ_REQUIRE(value > 0 && value <= maximum, "invalid durable test setting", name);
  return value;
}

uint64_t durableNumber(std::string_view encoded) {
  uint64_t number;
  auto [end, error] = std::from_chars(encoded.data(), encoded.data() + encoded.size(), number);
  KJ_REQUIRE(error == std::errc() && end == encoded.data() + encoded.size(),
      "invalid durable routing number");
  return number;
}

uint64_t durableNumber(Json::Reader value, kj::StringPtr name) {
  auto encoded = text(value, name);
  return durableNumber(std::string_view(encoded.begin(), encoded.size()));
}

DurableRoute readDurableRoute(Json::Reader value) {
  auto checkedText = [&](kj::StringPtr name, size_t maximum) {
    auto result = text(value, name);
    KJ_REQUIRE(
        result.size() > 0 && result.size() <= maximum, "invalid durable routing field", name);
    return std::string(result.begin(), result.size());
  };
  auto identifier = [&](kj::StringPtr name) {
    auto result = checkedText(name, 64);
    KJ_REQUIRE(result.size() == 64 &&
            std::all_of(result.begin(), result.end(),
                [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }),
        "invalid durable routing identifier", name);
    return result;
  };
  return {{identifier("namespaceId"), identifier("objectId")},
    {durableNumber(value, "generation"), durableNumber(value, "dataEpoch"),
      durableNumber(value, "shardEpoch"), durableNumber(value, "openId"),
      checkedText("definition", 256), checkedText("className", 255)},
    checkedText("invocationId", 255), checkedText("uniqueKey", 100), identifier("storageId")};
}

kj::String invocationIdentity(Slot &slot, Json::Reader descriptor) {
  return kj::str("\"workerId\":", encode(field(descriptor, "workerId")),
      ",\"versionId\":", encode(field(descriptor, "versionId")),
      ",\"invocationId\":", encode(field(descriptor, "invocationId")), ",\"isolate\":\"",
      slot.isolateLabel.c_str(), "\",");
}

CompatibilityFlags::Builder compileTenantFlags(
    Slot &slot, Json::Reader descriptor, Reporter &reporter, bool productPlatform = false) {
  // Compatibility flags are tenant validation, before package decoding.
  slot.loadStage = threaded::LoadStage::EVALUATION;
  auto flags = slot.flagsArena.initRoot<CompatibilityFlags>();
  capnp::MallocMessageBuilder flagMessage;
  auto given = field(descriptor, "compatibilityFlags");
  auto list =
      flagMessage.initRoot<capnp::List<capnp::Text>>(given.isArray() ? given.getArray().size() : 0);
  if (given.isArray()) {
    uint i = 0;
    for (auto item: given.getArray()) {
      KJ_REQUIRE(item.isString(), "invalid compatibility flag");
      if (item.getString() == "precise_timers")
        unsupported("precise_timers is disabled for tenants");
      list.set(i++, item.getString());
    }
  }
  // The product SQL class uses the same experimental SQL methods as the upstream runtime.
  const kj::StringPtr permittedPlatformFlags[] = {"experimental"};
  compileCompatibilityFlags(text(descriptor, "compatibilityDate"), list.asReader(), flags, reporter,
      false, CompatibilityDateValidation::CODE_VERSION,
      productPlatform ? kj::arrayPtr(permittedPlatformFlags, 1) : kj::ArrayPtr<const kj::StringPtr>());
  reporter.requireValid();
  if (flags.getPreciseTimers()) unsupported("precise_timers is disabled for tenants");
  return flags;
}

kj::Maybe<kj::String> sourceJsonFailure(kj::StringPtr input, IsolateLimits &limits) {
  // Run the same JSON decoder as a Worker, only after the protocol decoder
  // rejected the source response. Its temporary validation isolate belongs
  // to this load and uses its CPU, heap and process reservation.
  OomReportScope oom(limits.identity, limits.memoryMiB, limits.memory ? &limits.memory->heap : nullptr);
  auto *isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  FatalInvocation fatal(limits.durableEvents ? nullptr : isolate, limits.identity);
  limits.customizeIsolate(isolate);
  v8::HandleScope handles(isolate);
  auto context = v8::Context::New(isolate);
  v8::Context::Scope inContext(context);
  std::atomic<LimitCause> cause{LimitCause::NONE};
  CpuExecution execution(isolate, cause, limits.memoryState, limits.memory, 0, limits.cpuMs,
      limits.memoryMiB, limits.identity);
  v8::TryCatch caught(isolate);
  v8::Local<v8::String> text;
  v8::Local<v8::Value> value;
  kj::Maybe<kj::String> failure = kj::none;
  if (!v8::String::NewFromUtf8(isolate, input.begin(), v8::NewStringType::kNormal, input.size())
           .ToLocal(&text) ||
      !v8::JSON::Parse(context, text).ToLocal(&value)) {
    if (limits.memoryState->load() == LimitCause::MEMORY)
      failure = kj::str("MEMORY_LIMIT_EXCEEDED");
    else if (cause.load() == LimitCause::CPU)
      failure = kj::str("CPU_LIMIT_EXCEEDED");
    else if (cause.load() == LimitCause::PROCESS_MEMORY) {
      execution.finish();
      kj::throwRecoverableException(capacityError());
    } else {
      v8::String::Utf8Value description(isolate, caught.Exception());
      if (*description) failure = kj::str(kj::StringPtr(*description, description.length()));
    }
  }
  execution.finish();
  return failure;
}

// The existing node lifecycle wire layout, with one native writer. No KJ
// object is shared with the publisher or with the node's read-only view.
class LifecycleChannel {
 public:
  static constexpr uint32_t CAPACITY = 8 << 20;
  static constexpr size_t SIZE = 64 + 2 * (16 + size_t(CAPACITY));
  LifecycleChannel() {
    auto fd = configuredCount("COWORKERD_LIFECYCLE_FD", 0);
    if (fd == 0) return;
    struct stat facts;
    KJ_REQUIRE(fd > 2 && fstat(fd, &facts) == 0 && facts.st_size >= off_t(SIZE),
        "invalid lifecycle descriptor");
    auto mapping = mmap(nullptr, SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    KJ_REQUIRE(mapping != MAP_FAILED, "cannot map lifecycle channel");
    close(fd);
    base = static_cast<char *>(mapping);
    KJ_REQUIRE(*reinterpret_cast<uint32_t *>(base + 8) == 0x434c5743 &&
            *reinterpret_cast<uint32_t *>(base + 12) == CAPACITY,
        "invalid lifecycle channel header");
  }
  ~LifecycleChannel() {
    if (base) munmap(base, SIZE);
  }
  void publish(const std::string &json, uint64_t active, uint64_t background, uint64_t unknown) {
    if (!base) return;
    KJ_REQUIRE(json.size() <= CAPACITY, "lifecycle snapshot exceeds channel");
    std::lock_guard lock(writer);
    auto next = __atomic_load_n(reinterpret_cast<uint64_t *>(base), __ATOMIC_ACQUIRE) + 1;
    auto buffer = base + 64 + (next % 2) * (16 + size_t(CAPACITY));
    auto sequence = reinterpret_cast<uint64_t *>(buffer);
    __atomic_store_n(sequence, next * 2 + 1, __ATOMIC_SEQ_CST);
    memcpy(buffer + 16, json.data(), json.size());
    __atomic_store_n(
        reinterpret_cast<uint32_t *>(buffer + 8), uint32_t(json.size()), __ATOMIC_RELAXED);
    __atomic_store_n(sequence, next * 2 + 2, __ATOMIC_RELEASE);
    __atomic_store_n(reinterpret_cast<uint64_t *>(base), next, __ATOMIC_RELEASE);
    auto drainSequence = reinterpret_cast<uint64_t *>(base + 24);
    auto drain = __atomic_load_n(drainSequence, __ATOMIC_RELAXED);
    __atomic_store_n(drainSequence, drain + 1, __ATOMIC_SEQ_CST);
    __atomic_store_n(reinterpret_cast<uint64_t *>(base + 32), nowNs(), __ATOMIC_RELAXED);
    __atomic_store_n(reinterpret_cast<uint64_t *>(base + 40), active, __ATOMIC_RELAXED);
    __atomic_store_n(reinterpret_cast<uint64_t *>(base + 48), background, __ATOMIC_RELAXED);
    __atomic_store_n(reinterpret_cast<uint64_t *>(base + 56), unknown, __ATOMIC_RELAXED);
    __atomic_store_n(drainSequence, drain + 2, __ATOMIC_RELEASE);
  }

 private:
  char *base = nullptr;
  std::mutex writer;
};
std::string digest(kj::ArrayPtr<const kj::byte> value) {
  kj::byte result[SHA256_DIGEST_LENGTH];
  SHA256(value.begin(), value.size(), result);
  return kj::encodeHex(result).cStr();
}

bool productPlatformPlan(Json::Reader plan) {
  auto kind = field(plan, "kind");
  auto name = field(plan, "definitionName");
  return kind.isString() && kind.getString() == "platformDurable" &&
      name.isString() && name.getString() == "coworkerd:platform";
}

constexpr char PLATFORM_DATABASE_COPY_PREFIX[] = "platform-object-v1:";

bool shouldReleaseDurableCopy(uint32_t reservedTrees, bool hasResidentRoots,
    const std::string &definition, bool resettingOrExpired) {
  // The catalog mints tenant fingerprints with a hexadecimal definition hash first.
  // The platform copy key below is recomputed when the database route is validated.
  return reservedTrees == 0 && !hasResidentRoots &&
      (resettingOrExpired || definition.starts_with(PLATFORM_DATABASE_COPY_PREFIX));
}

std::string platformCopyKey(Json::Reader descriptor, Json::Reader plan,
    const server::DurableAdmission::Key *object = nullptr) {
  auto namedLimit = [&](kj::StringPtr name) -> uint32_t {
    auto value = field(descriptor, name);
    if (value.isNull()) return 0;
    KJ_REQUIRE(value.isNumber() && value.getNumber() >= 0 && value.getNumber() <= UINT32_MAX &&
        std::floor(value.getNumber()) == value.getNumber(), "invalid platform object limit", name);
    return uint32_t(value.getNumber());
  };
  auto identity = kj::str(text(plan, "tenant"), "\n", text(plan, "definitionName"), "\n",
      text(descriptor, "versionId"), "\n", text(descriptor, "definitionHash"), "\n",
      text(descriptor, "contentHash"), "\n", namedLimit("cpuLimitMs"), "\n",
      namedLimit("memoryLimitMiB"));
  if (object != nullptr) {
    auto exclusive = kj::str(identity, "\n", object->namespaceId.c_str(), "\n", object->objectId.c_str());
    return PLATFORM_DATABASE_COPY_PREFIX + digest(exclusive.asBytes());
  }
  return "platform-v1:" + digest(identity.asBytes());
}

bool platformDurablePlan(Json::Reader input, Json::Reader plan, Json::Reader selection,
    const DurableRoute &route) {
  const auto tenant = text(plan, "tenant");
  const auto database = field(field(input, "durableObject"), "database");
  const bool isDatabase = database.isString() && database.getString().size() > 0;
  if (isDatabase != (productPlatformPlan(plan) && route.open.className == "SqlDatabase")) return false;
  // The plan identifies the code package. The descriptor's Worker identifies the
  // events and limit records attributed to that package.
  const auto workerSuffix = isDatabase ? "/__databases"_kj : "/:platform"_kj;
  if (tenant.size() == 0 || text(input, "workerId") != kj::str(tenant, workerSuffix) ||
      text(input, "format") != "v2" || text(selection, "className") != route.open.className.c_str() ||
      platformCopyKey(input, plan, isDatabase ? &route.key : nullptr) != route.open.definition) return false;
  auto empty = [](Json::Reader value) {
    return value.isNull() || (value.isArray() && value.getArray().size() == 0);
  };
  return empty(field(input, "variables")) && empty(field(input, "secrets")) &&
      empty(field(input, "nativeNamespaces")) && empty(field(plan, "serviceBindings")) &&
      empty(field(plan, "durableObjects"));
}
std::string decode64(kj::StringPtr input) {
  KJ_REQUIRE(input.size() % 4 == 0, "invalid module encoding");
  std::string value(input.size() / 4 * 3, '\0');
  int size = EVP_DecodeBlock(reinterpret_cast<unsigned char *>(value.data()),
      reinterpret_cast<const unsigned char *>(input.begin()), input.size());
  KJ_REQUIRE(size >= 0, "invalid module encoding");
  if (input.size() && input.endsWith("=")) --size;
  if (input.size() > 1 && input.endsWith("==")) --size;
  value.resize(size);
  return value;
}

// Coworkerd's persisted namespace IDs use SHA-256(namespace bytes || "name" ||
// UTF-8 name). Changing this factory would select different durable data.
class ObjectIdFactory final: public ActorIdFactory {
 public:
  struct Id final: ActorId {
    Id(kj::String space, kj::String id, kj::Maybe<kj::String> name)
        : space(kj::mv(space)),
          id(kj::mv(id)),
          name(kj::mv(name)) {}
    kj::String space, id;
    kj::Maybe<kj::String> name;
    kj::String toString() const override {
      return kj::str(id);
    }
    kj::Maybe<kj::StringPtr> getName() const override {
      return name;
    }
    kj::Maybe<kj::StringPtr> getJurisdiction() const override {
      return kj::none;
    }
    bool equals(const ActorId &other) const override {
      auto &value = kj::downcast<const Id>(other);
      return id == value.id;
    }
    kj::Own<ActorId> clone() const override {
      return kj::heap<Id>(
          kj::str(space), kj::str(id), name.map([](auto &value) { return kj::str(value); }));
    }
  };
  explicit ObjectIdFactory(kj::StringPtr space): space(kj::str(space)) {}
  kj::String space;
  void requireAvailable() {
    JSG_REQUIRE(space.size() == 64, Error,
        "DO_NAMESPACE_UNAVAILABLE: the Durable Object namespace cannot "
        "be used");
  }
  kj::Own<ActorId> newUniqueId(kj::Maybe<kj::StringPtr> jurisdiction) override {
    requireAvailable();
    JSG_REQUIRE(
        jurisdiction == kj::none, TypeError, "Durable Object jurisdictions are not supported");
    kj::byte bytes[32];
    getEntropy(bytes);
    return kj::heap<Id>(kj::str(space), kj::encodeHex(kj::arrayPtr(bytes)), kj::none);
  }
  kj::Own<ActorId> idFromName(kj::String name) override {
    requireAvailable();
    auto prefix = kj::decodeHex(space);
    KJ_REQUIRE(!prefix.hadErrors && prefix.size() == 32);
    auto bytes = kj::heapArray<kj::byte>(32 + 4 + name.size());
    memcpy(bytes.begin(), prefix.begin(), 32);
    memcpy(bytes.begin() + 32, "name", 4);
    memcpy(bytes.begin() + 36, name.begin(), name.size());
    return kj::heap<Id>(kj::str(space), kj::str(digest(bytes).c_str()), kj::mv(name));
  }
  kj::Own<ActorId> idFromString(kj::String id) override {
    requireAvailable();
    bool valid = id.size() == 64;
    for (char c: id) valid = valid && ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
    JSG_REQUIRE(valid, TypeError, "Invalid Durable Object ID: 64 lowercase hexadecimal digits");
    return kj::heap<Id>(kj::str(space), kj::mv(id), kj::none);
  }
  bool matchesJurisdiction(const ActorId &) override {
    return true;
  }
  kj::Own<ActorIdFactory> cloneWithJurisdiction(kj::Maybe<kj::StringPtr> jurisdiction) override {
    JSG_REQUIRE(
        jurisdiction == kj::none, TypeError, "Durable Object jurisdictions are not supported");
    return kj::heap<ObjectIdFactory>(space);
  }
  kj::Maybe<kj::Own<ActorIdFactory>> cloneForSnapshot() const override {
    return kj::heap<ObjectIdFactory>(space);
  }
};

// Raw descriptors and atomic queue positions are the accept loop's only shared
// state.
class DescriptorQueue {
 public:
  explicit DescriptorQueue(size_t limit): entries(limit + 1) {}
  ~DescriptorQueue() {
    close();
  }
  // Called only after producer and consumer have stopped. Pending descriptors
  // are owned by this queue until pop() transfers them to a call loop.
  void close() {
    closed.store(true, std::memory_order_release);
    for (int fd; (fd = pop()) >= 0;) ::close(fd);
  }
  bool hasSpace() const {
    return !closed.load(std::memory_order_acquire) &&
        (tail.load(std::memory_order_relaxed) + 1) % entries.size() !=
        head.load(std::memory_order_acquire);
  }
  bool push(int fd) {
    if (closed.load(std::memory_order_acquire)) return false;
    auto position = tail.load(std::memory_order_relaxed);
    auto next = (position + 1) % entries.size();
    if (next == head.load(std::memory_order_acquire)) return false;
    entries[position] = fd;
    tail.store(next, std::memory_order_release);
    return true;
  }
  int pop() {
    auto position = head.load(std::memory_order_relaxed);
    if (position == tail.load(std::memory_order_acquire)) return -1;
    auto fd = entries[position];
    head.store((position + 1) % entries.size(), std::memory_order_release);
    return fd;
  }

 private:
  std::vector<int> entries;
  std::atomic<size_t> head{0}, tail{0};
  std::atomic<bool> closed{false};
};

struct ThreadedServer;
using BudgetLifetime = std::shared_ptr<void>;

struct DurableBudgetRelease {
  DurableBudgetRelease(ThreadedServer& server, uint64_t ticket): server(server), ticket(ticket) {}
  ~DurableBudgetRelease();

  ThreadedServer& server;
  uint64_t ticket;
};

// The server's maintenance entry: its pass runs inside the budget's maintenance
// mark, so that a reclaim the pass itself triggers never waits for that pass.
template <typename Server>
void runMaintenanceOf(Server &server) {
  server.resources->runMaintenance([&] { server.maintainPass(); });
}
struct InvocationState: kj::Refcounted {
  explicit InvocationState(std::shared_ptr<Slot> slot, std::shared_ptr<TenantPermit> tenant)
      : slot(std::move(slot)),
        tenant(std::move(tenant)) {
    this->slot->contexts.fetch_add(1);
    this->slot->lastUseNs.store(nowNs());
  }
  ~InvocationState() noexcept(false) {
    slot->lastUseNs.store(nowNs());
    slot->contexts.fetch_sub(1);
  }
  std::shared_ptr<Slot> slot;
  std::shared_ptr<TenantPermit> tenant;
  Measurement measurement;
};

class Invocation final: public kj::HttpService {
 public:
  Invocation(ThreadedServer &server, ThreadState &state, kj::String descriptor, Frankenvalue props,
      BudgetLifetime budgetLifetime,
      kj::Maybe<threaded::rpc::NativeLeaseControl::Client> lease)
      : server(server),
        state(state),
        descriptor(kj::mv(descriptor)),
        props(kj::mv(props)),
        budgetLifetime(kj::mv(budgetLifetime)),
        lease(kj::mv(lease)) {}
  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders &headers,
      kj::AsyncInputStream &body,
      Response &response) override;

 private:
  ThreadedServer &server;
  ThreadState &state;
  kj::String descriptor;
  Frankenvalue props;
  BudgetLifetime budgetLifetime;
  kj::Maybe<threaded::rpc::NativeLeaseControl::Client> lease;
  bool used = false;
};

class Dispatcher final: public rpc::EventDispatcher::Server {
 public:
  Dispatcher(ThreadedServer &server, ThreadState &state, kj::String descriptor, Frankenvalue props,
      BudgetLifetime budgetLifetime,
      kj::Maybe<threaded::rpc::NativeLeaseControl::Client> lease = kj::none)
      : server(server),
        state(state),
        descriptor(kj::mv(descriptor)),
        props(kj::mv(props)),
        budgetLifetime(kj::mv(budgetLifetime)),
        lease(kj::mv(lease)) {}
  kj::Promise<void> getHttpService(GetHttpServiceContext context) override {
    KJ_REQUIRE(!used, "NOT_SUPPORTED: one HTTP event per invocation");
    used = true;
    context.initResults().setHttp(state.http.kjToCapnp(
        kj::heap<Invocation>(server, state, kj::mv(descriptor), kj::mv(props), kj::mv(budgetLifetime), kj::mv(lease))));
    return kj::READY_NOW;
  }
  kj::Promise<void> jsRpcSession(JsRpcSessionContext context) override;

 private:
  ThreadedServer &server;
  ThreadState &state;
  kj::String descriptor;
  Frankenvalue props;
  BudgetLifetime budgetLifetime;
  kj::Maybe<threaded::rpc::NativeLeaseControl::Client> lease;
  bool used = false;
};

class Bootstrap final: public threaded::rpc::Bootstrap::Server {
 public:
  Bootstrap(ThreadedServer &server, ThreadState &state): server(server), state(state) {}
  kj::Promise<void> startEvent(StartEventContext context) override {
    KJ_REQUIRE(!used, "NOT_SUPPORTED: one startEvent per connection");
    used = true;
    auto params = context.getParams();
    KJ_REQUIRE(!params.getFromPersistentStub() && !params.getFromHandoff(),
        "NOT_SUPPORTED: persistent objects and handoff");
    context.initResults().setDispatcher(kj::heap<Dispatcher>(server, state,
        kj::str(params.getCfBlobJson()), Frankenvalue(),
        std::make_shared<capnp::Capability::Client>(params.getBudgetLifetime())));
    return kj::READY_NOW;
  }

  kj::Promise<void> startNativeEvent(StartNativeEventContext context) override {
    KJ_REQUIRE(!used, "one startEvent per connection");
    used = true;
    auto params = context.getParams();
    kj::Maybe<threaded::rpc::NativeLeaseControl::Client> lease;
    if (params.hasLease()) lease = params.getLease();
    context.getResults().setDispatcher(kj::heap<Dispatcher>(server, state,
        kj::str(params.getCfBlobJson()), readNativeProps(state, params.getCoworkerdProps()),
        std::make_shared<capnp::Capability::Client>(params.getBudgetLifetime()), kj::mv(lease)));
    return kj::READY_NOW;
  }

 private:
  ThreadedServer &server;
  ThreadState &state;
  bool used = false;
};

// An event that is still being prepared when its session ends is canceled with the session.
// Cap'n Proto lets the startEvent call itself run on (its interface does not allow
// cancellation), while the session's tree and home are released as soon as it has ended.
struct DurableSessionScope {
  kj::Canceler preparing;
  kj::Canceler transport;
  bool ended = false;
  int socketFd = -1;
  uint64_t nextSqlDisconnectCheck = 0;

  bool sqlClientDisconnected(uint64_t now) noexcept {
    if (ended) return true;
    if (now < nextSqlDisconnectCheck) return false;
    nextSqlDisconnectCheck = now + 1000000;
    pollfd pending{socketFd, POLLRDHUP, 0};
    return poll(&pending, 1, 0) > 0 &&
        (pending.revents & (POLLRDHUP | POLLHUP | POLLERR | POLLNVAL)) != 0;
  }
};

class DurableBootstrap final: public threaded::rpc::Bootstrap::Server {
 public:
  DurableBootstrap(ThreadedServer &server,
      ThreadState &state,
      DurableRoute route,
      server::DurableAdmission::Ticket ticket,
      kj::Own<kj::PromiseFulfiller<void>> accepted,
      std::shared_ptr<DurableSessionScope> scope)
      : server(server),
        state(state),
        route(std::move(route)),
        ticket(ticket),
        accepted(kj::mv(accepted)),
        scope(std::move(scope)) {}

  kj::Promise<void> startEvent(StartEventContext context) override;
  kj::Promise<void> startNativeEvent(StartNativeEventContext context) override;

 private:
  ThreadedServer &server;
  ThreadState &state;
  DurableRoute route;
  server::DurableAdmission::Ticket ticket;
  kj::Own<kj::PromiseFulfiller<void>> accepted;
  std::shared_ptr<DurableSessionScope> scope;
  bool used = false;

  kj::Promise<rpc::EventDispatcher::Client> prepare(kj::String descriptor, Frankenvalue props,
      BudgetLifetime budgetLifetime);
};

struct SubrequestBudget {
  SubrequestBudget(uint32_t limit, uint32_t tenantLimit): limit(limit), tenantLimit(tenantLimit) {}
  const uint32_t limit, tenantLimit;
  std::atomic<uint32_t> active{0};
  std::atomic<uint64_t> rejected{0};
  std::mutex mutex;
  std::map<std::string, uint32_t> tenants;
  uint64_t nextWaiter = 0;
  std::map<uint64_t, kj::Own<kj::CrossThreadPromiseFulfiller<void>>> waiters;
};

// One console line for the node's log post: at most LOG_MESSAGE_BYTES of the
// message, and when it was longer its size, so that the node marks the line
// truncated and counts it as it does for any line past its own limit (a cut
// here without the size reached the node as an ordinary line, v138).
constexpr size_t LOG_MESSAGE_BYTES = 4096;
kj::String consoleLogRow(kj::StringPtr level, kj::StringPtr message) {
  capnp::MallocMessageBuilder arena;
  auto value = arena.initRoot<Json>();
  const auto kept = kj::min(message.size(), LOG_MESSAGE_BYTES);
  value.setString(kj::str(message.first(kept)));
  if (kept == message.size())
    return kj::str("{\"level\":\"", level, "\",\"message\":", encode(value.asReader()), "}");
  return kj::str("{\"level\":\"", level, "\",\"message\":", encode(value.asReader()),
      ",\"messageBytes\":", message.size(), "}");
}

bool canEvict(const std::shared_ptr<Slot> &slot, Slot *exclude) {
  return slot.get() != exclude && slot->complete.load() && slot.use_count() == 1 &&
      slot->contexts.load() == 0;
}

struct ObjectRetirement {
  using Time = server::DurableAdmission::Time;
  uint64_t home;
  uint64_t tree;
  Time lastUse;
  Time deadline;
  bool awaitingWal = false;
  bool operator==(const ObjectRetirement&) const = default;
};

struct HomeRetirement {
  uint64_t home;
  uint64_t activity;
  server::DurableAdmission::Time deadline;
  bool operator==(const HomeRetirement&) const = default;
};

constexpr auto OBJECT_ADMISSION_WAL_WAIT = std::chrono::seconds(1);

constexpr auto DATABASE_ADMISSION_GRACE = std::chrono::milliseconds(100);

struct DatabaseRetirement {
  using Time = server::DurableAdmission::Time;
  enum class Outcome { PENDING, RELEASED, REFUSED, EXPIRED };
  uint64_t home;
  uint64_t tree;
  uint64_t lastUse;
  std::shared_ptr<DatabaseReservationReturn> reservationReturned;
  Time deadline;
  bool preferDatabase;
  std::optional<uint64_t> preparationCeiling;
  bool treePreferred = false;

  Outcome outcome(Time now, bool hasTree, bool closing) const {
    if (reservationReturned->releasedMemory.load() > 0) return Outcome::RELEASED;
    if (now >= deadline) return Outcome::EXPIRED;
    if (hasTree) return closing ? Outcome::PENDING : Outcome::REFUSED;
    return Outcome::PENDING;
  }
};

template <typename Retire>
kj::Promise<bool> registerDurablePreparation(
    ResourceReservation& reservation, bool platformDatabase, Retire retire) {
  if (reservation.registerPreparation()) co_return true;
  if (!platformDatabase) co_return false;
  // A cold load retries the reservation once after reclaiming a single copy.
  const auto replacement = co_await retire();
  co_return replacement && reservation.registerPreparation(replacement.get());
}

// The maintenance pass's view of the residents, which it formats into the
// lifecycle row outside the registry lock (tens of milliseconds with hundreds
// of residents, every 100 ms). Weak references: a strong one made every idle
// Worker look in use to canEvict() for that whole time, and a load past the
// resident limit was refused instead of evicting one.
template <typename Registry, typename Retired>
std::vector<std::weak_ptr<Slot>> residentSnapshot(const Registry &registry, const Retired &retired) {
  std::vector<std::weak_ptr<Slot>> snapshot;
  snapshot.reserve(registry.size() + retired.size());
  for (auto &[key, slot]: registry) snapshot.push_back(slot);
  for (auto &slot: retired) snapshot.push_back(slot);
  return snapshot;
}

struct ThreadedServer {
  struct Engine {
    kj::Own<v8::Platform> basePlatform = jsg::defaultPlatform(16);
    server::WorkerdPlatform platform{*basePlatform};
    jsg::V8System v8{platform, {}, basePlatform.get()};
  };
  const std::unique_ptr<Engine> engine;
  jsg::V8System& v8;
  const kj::MonotonicClock& isolateBusyClock;
  const std::string sourceAddress, identity;
  const std::unique_ptr<server::DurableStorage> durableStorage;
  kj::Own<server::ThreadedWalLog> durableLog;
  const server::DurablePlacementLimits durablePlacement;
  std::optional<server::DurableAdmission> durableAdmission;
  std::mutex durableRoutingMutex;
  using DurableWake = kj::Own<kj::CrossThreadPromiseFulfiller<void>>;
  std::map<uint64_t, std::map<uint64_t, DurableWake>> durableWaiters;
  // One retirement at a time, so concurrent cold loads can retry against the same release.
  // Both fields are guarded by durableRoutingMutex.
  std::optional<DatabaseRetirement> databaseVictim;
  // Guarded by durableRoutingMutex, like the per-home idle snapshots.
  std::optional<ObjectRetirement> objectVictim;
  std::optional<HomeRetirement> homeVictim;
  const char* lastHomeRefusal = nullptr;
  uint64_t homeRefusals = 0, homeRetirements = 0;
  uint64_t durableDescriptorBudget = 0;
  static constexpr auto HOME_RESERVATION = server::DurableAdmission::Resources{
    .threads = 1, .descriptors = 5,
  };
  static constexpr uint64_t HOME_NATIVE_BYTES = 256 << 10;
  using DatabaseReplacement = std::shared_ptr<DatabasePreparationReplacement>;
  std::vector<kj::Own<kj::CrossThreadPromiseFulfiller<DatabaseReplacement>>> databaseCapacityWaiters;
  std::atomic<uint64_t> durableHomeSessions{0};
  std::atomic<uint64_t> durableIdleRetirements{0}, durablePressureRetirements{0};
  std::atomic<uint64_t> objectSqliteConnections{0};
  std::atomic<uint64_t> durableAdmissionRetirements{0}, durableAdmissionWalTimeouts{0};
  std::atomic<uint64_t> durableActorResets{0}, durableIsolateResets{0};
  std::atomic<uint64_t> durableTreeResets{0};
  std::atomic<uint64_t> nextDurableActor{0};
  std::atomic<uint64_t> durablePendingCommits{0};
  std::atomic<bool> refuseDurableGroupForTest{false};
  const uint64_t durableCommitDelayMs;
  const std::string bindingAddress = getenv("COWORKERD_THREADED_BINDING_ADDRESS") == nullptr
      ? ""
      : getenv("COWORKERD_THREADED_BINDING_ADDRESS");
  const std::string egressAddress = getenv("COWORKERD_THREADED_EGRESS_ADDRESS") == nullptr
      ? ""
      : getenv("COWORKERD_THREADED_EGRESS_ADDRESS");
  const std::string egressToken = getenv("COWORKERD_THREADED_EGRESS_TOKEN") == nullptr
      ? ""
      : getenv("COWORKERD_THREADED_EGRESS_TOKEN");
  std::mutex registryMutex;
  std::map<std::string, std::shared_ptr<Slot>> registry;
  std::vector<std::shared_ptr<Slot>> retiredSlots;
  DescriptorQueue pending;
  const uint limit, maxDurableHomes;
  static constexpr uint CONTROL_THREADS = 8;
  static constexpr int MAINTAIN = -3, STOP = -4, STOPPED = -5;
  std::shared_ptr<ResourceBudget> resources;
  LifecycleChannel lifecycle;
  std::shared_ptr<TenantBudget> tenantBudget;
  std::atomic<uint64_t> nextInstance{0};
  struct Thread {
    const bool control;
    const bool maintenanceOnly;
    int wake;
    std::atomic<int> descriptor{-2};  // -2 initializes/runs; -1 is idle
    std::atomic<bool> invocation{false};
    std::atomic<uint64_t> idleSince{nowNs()};
    std::thread thread;
    struct DurableHome {
      std::string definition;
      std::string warmDefinition;
      uint32_t reservedTrees = 0;
      uint64_t memory = 0, heapBytes = 0, copyInstance = 0;
      bool admissionTurnDelayed = false;
      bool reclaiming = false;
      uint64_t activity = 0;
      server::DurableAdmission::Time lastUse{};
      std::shared_ptr<ResourceReservation> preparation;
      server::DurableHomeLoad load{std::chrono::seconds(4)};
      server::DurablePressure pressure;
      std::atomic<bool> walFull{false};
      std::vector<DurableIncoming> incoming;
      std::set<uint64_t> resident;
      std::map<uint64_t, server::DurableAdmission::Time> idleObjects;
      uint64_t databaseLastUse = 0;
      bool databaseCanRetire = false;
      std::weak_ptr<ResourceReservation> databaseReservation;
      server::DurableAdmission::Time databasePreferredUntil{};
    };
    std::optional<DurableHome> home;
    uint64_t homeId = 0;
    Thread(bool control, bool maintenanceOnly)
        : control(control),
          maintenanceOnly(maintenanceOnly),
          wake(eventfd(0, EFD_CLOEXEC)) {
      KJ_REQUIRE(wake >= 0);
    }
  };
  std::vector<std::unique_ptr<Thread>> threads;
  int managerWake = -1;
  const int parent;
  std::mutex budgetReleaseMutex;
  std::deque<uint64_t> budgetReleases;
  std::atomic<bool> releaseDurableMemoryRequested{false};
  std::atomic<bool> ready{false};
  std::atomic<uint32_t> liveThreads{0};
  std::atomic<uint64_t> growthFailures{0};
  std::atomic<uint> compilations{0};
  std::atomic<uint64_t> isolateCreationAttempts{0};
  std::atomic<uint> canceledLoads{0};
  SubrequestBudget subrequestBudget;
  std::atomic<uint64_t> acceptFailures{0};
  std::atomic<uint64_t> logAccepted{0}, logDroppedLines{0}, logDroppedBytes{0}, logFlushFailures{0};
  std::atomic<uint64_t> logPeakQueuedBytes{0};
  std::atomic<bool> delayedSourceForTest{false};
  std::thread manager;

  ThreadedServer(uint limit,
      std::string sourceAddress,
      std::string identity,
      int parent,
      kj::Own<const kj::Directory> durableDirectory = {},
      kj::Own<server::ThreadedWalLog> durableLog = {},
      jsg::V8System* existingV8 = nullptr,
      const kj::MonotonicClock& isolateBusyClock = kj::systemPreciseMonotonicClock(),
      std::optional<server::DurablePlacementLimits> placement = std::nullopt)
      : engine(existingV8 == nullptr ? std::make_unique<Engine>() : nullptr),
        v8(existingV8 == nullptr ? engine->v8 : *existingV8),
        isolateBusyClock(isolateBusyClock),
        sourceAddress(std::move(sourceAddress)),
        identity(std::move(identity)),
        durableStorage(durableDirectory
                ? std::make_unique<server::DurableStorage>(kj::mv(durableDirectory),
                    configuredCount("COWORKERD_DO_SQL_DATABASE_SPILL_BYTES", 1ull << 30),
                    configuredCount("COWORKERD_DO_SQL_PROCESS_SPILL_BYTES", 0))
                : nullptr),
        durableLog(kj::mv(durableLog)),
        durablePlacement(placement.value_or(server::DurablePlacementLimits{
          .treesPerCopy = this->durableStorage
              ? uint32_t(boundedDurableSetting("COWORKERD_DO_TEST_TREES_PER_COPY", 1024, 65536))
              : 0,
          .placementBytes = this->durableStorage
              ? configuredCount("COWORKERD_DO_TEST_PLACEMENT_BYTES", 64ull << 20)
              : 0,
          .busyThreshold = this->durableStorage
              ? std::chrono::milliseconds(
                    boundedDurableSetting("COWORKERD_DO_TEST_BUSY_MS", 3000, 4000))
              : std::chrono::milliseconds(0),
        })),
        durableCommitDelayMs(
            this->durableStorage && !this->durableLog
                ? configuredCount("COWORKERD_DO_TEST_COMMIT_DELAY_MS", 0) : 0),
        pending(this->durableStorage ? 0 : limit * 2),
        limit(limit),
        maxDurableHomes(this->durableStorage
            ? uint(boundedDurableSetting("COWORKERD_DO_MAX_HOMES", 16384, 16384))
            : 0),
        resources(std::make_shared<ResourceBudget>(
            configuredCount("COWORKERD_THREADED_MEMORY_BYTES", 2ull << 30) -
                (this->durableStorage
                        ? configuredCount("COWORKERD_THREADED_MEMORY_BYTES", 2ull << 30) / 8
                        : 0),
            configuredCount("COWORKERD_THREADED_RESIDENT_LIMIT", 1840),
            this->durableStorage ? 0 : limit,
            this->durableStorage ? ResourceBudget::ReclaimMode::NONE
                                 : ResourceBudget::ReclaimMode::BACKGROUND)),
        tenantBudget(std::make_shared<TenantBudget>(
            configuredCount("COWORKERD_THREADED_TENANT_IN_FLIGHT_LIMIT", 0))),
        parent(parent),
        subrequestBudget(limit, tenantBudget->limit) {
    KJ_REQUIRE(limit > 0 && limit <= 16384);
    if (this->durableStorage) {
      KJ_REQUIRE(durableCommitDelayMs <= 1000, "invalid durable test commit delay");
      const auto refuseGroup = configuredCount("COWORKERD_DO_TEST_REFUSE_GROUP", 0);
      KJ_REQUIRE(refuseGroup <= 1, "invalid durable test group refusal");
      refuseDurableGroupForTest.store(refuseGroup == 1);
      const auto colon = this->identity.rfind(':');
      KJ_REQUIRE(colon != std::string::npos, "invalid durable child identity");
      KJ_REQUIRE(durablePlacement.treesPerCopy != 0 && durablePlacement.placementBytes != 0 &&
              durablePlacement.busyThreshold > std::chrono::milliseconds(0) &&
              durablePlacement.busyThreshold <= std::chrono::seconds(4),
          "invalid durable copy placement thresholds");
      // Bound pre-execution admission independently of the existing ordinary queue.
      rlimit files;
      KJ_SYSCALL(getrlimit(RLIMIT_NOFILE, &files));
      const auto fdHeadroom =
          boundedDurableSetting("COWORKERD_DO_TEST_FD_HEADROOM", 256, 65536);
      KJ_REQUIRE(
          files.rlim_cur > fdHeadroom, "durable test descriptors leave no admission headroom");
      durableDescriptorBudget = files.rlim_cur - fdHeadroom;
      // Match the node's default process in-flight limit and allow 4 KiB of
      // declared bytes per waiting event on average.
      constexpr uint32_t defaultMailboxDepth = 512;
      constexpr uint64_t defaultMailboxBytes = uint64_t(defaultMailboxDepth) * 4096;
      durableAdmission.emplace(
          server::DurableAdmission::Limits{
            .capacity = {.threads = limit,
              .descriptors = durableDescriptorBudget,
              .memory = configuredCount("COWORKERD_THREADED_MEMORY_BYTES", 2ull << 30) / 8},
            .tree = {.descriptors = 4, .memory = 16 * 1024},
            .event = {.descriptors = 1, .memory = 4096},
            .residentTarget = uint32_t(configuredCount("COWORKERD_DO_RESIDENT_OBJECTS", 4096)),
            .mailboxDepth = uint32_t(boundedDurableSetting(
                "COWORKERD_DO_TEST_MAILBOX_EVENTS", defaultMailboxDepth, 65536)),
            .mailboxBytes = configuredCount("COWORKERD_DO_TEST_MAILBOX_BYTES", defaultMailboxBytes),
          },
          durableNumber(std::string_view(this->identity).substr(colon + 1)));
    }
  }

  void start() {
    managerWake = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    KJ_REQUIRE(managerWake >= 0);
    registerRoleFatalHandler(durableAdmission.has_value(), &v8::V8::SetFatalErrorHandler);
    // With the V8 sandbox: a process whose sandbox is not secure never loads a Worker (exit 78).
    server::requireSecureSandbox();
    resources->reclaimWake = managerWake;
    server::coworkerdServing();
    fprintf(stderr, "[threaded-start] {\"event\":\"steady-start\",\"threadLimit\":%u}\n", limit);
    manager = std::thread([this]() { manage(); });
  }

  std::shared_ptr<TenantPermit> admitTenant(Json::Reader descriptor) {
    auto worker = str(descriptor, "workerId");
    auto separator = worker.find('/');
    KJ_REQUIRE(separator != std::string::npos && separator > 0, "invalid trusted Worker tenant");
    return makeTenantPermit(tenantBudget, std::string_view(worker).substr(0, separator));
  }

  void maintain() {
    runMaintenanceOf(*this);
  }
  void maintainPass() {
    if (resources->reclaimRequested.exchange(false)) {
      bool noIdle = false;
      KJ_DEFER(resources->finishReclaim(noIdle));
      while (resources->needsReclaim()) {
        if (!evictOne()) {
          noIdle = true;
          break;
        }
      }
    }
    // Publish only after waking growers; a publisher must never wait on a
    // reservation whose owner is itself waiting for this maintenance pass.
    reapRetired();
    for (uint32_t i = 0; i < 8 && evictOne(nullptr, true); ++i) {}
    std::vector<std::weak_ptr<Slot>> snapshot;
    {
      std::lock_guard lock(registryMutex);
      snapshot = residentSnapshot(registry, retiredSlots);
    }
    auto quote = [](const std::string &value) {
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<Json>();
      root.setString(value.c_str());
      return std::string(encode(root.asReader()).cStr());
    };
    std::string json = "{\"ordinaryServer\":\"threaded\",\"residentWorkers\":[";
    uint64_t active = 0, background = 0, unknown = 0;
    bool first = true;
    for (auto &held: snapshot) {
      // Evicted since the snapshot: gone from the row as well.
      auto slot = held.lock();
      if (!slot) continue;
      auto complete = slot->complete.load();
      if (!complete) ++unknown;
      if (complete && slot->worker == kj::none) continue;
      auto contexts = std::max(slot->contexts.load(), complete ? 0u : 1u);
      auto js = slot->active.load();
      active += contexts;
      background += contexts && js == 0;
      json += first ? "" : ",";
      first = false;
      auto separator = slot->workerId.find('/');
      json += "{\"tenant\":" + quote(slot->workerId.substr(0, separator)) +
          ",\"workerId\":" + quote(slot->workerId) + ",\"versionId\":" + quote(slot->versionId) +
          ",\"planRevision\":" + quote(slot->planRevision) +
          ",\"isolate\":" + quote(slot->isolateLabel) +
          ",\"loadGeneration\":" + std::to_string(slot->instance) +
          ",\"instance\":" + std::to_string(slot->instance) +
          ",\"active\":" + std::to_string(contexts) +
          ",\"loading\":" + (complete ? "false" : "true") +
          ",\"backgroundActive\":" + (contexts && js == 0 ? "true" : "false") +
          ",\"nativeMemory\":" + slot->reservation->metrics();
      if (complete) {
        KJ_IF_SOME(busy, slot->busy) {
          const auto sample = busy->window.sample();
          json += ",\"isolateBusy\":{\"windowMs\":" +
              std::to_string(threaded::IsolateBusyWindow::WINDOW_NS / 1000000) +
              ",\"throughMonotonicMs\":" + std::to_string(sample.throughNs / 1000000) +
              ",\"busyNs\":" + std::to_string(sample.busyNs) +
              ",\"permille\":" + std::to_string(sample.permille()) + "}";
        }
      }
      json += "}";
    }
    json += "],\"createdIsolates\":" + std::to_string(compilations.load()) +
        ",\"resourceBudget\":{\"memoryBytes\":" + std::to_string(resources->memoryReserved.load()) +
        ",\"memoryMaximum\":" + std::to_string(resources->memoryMaximum) +
        ",\"admissionMaximum\":" + std::to_string(resources->admissionMaximum) +
        ",\"pressureTerminations\":" + std::to_string(resources->pressureTerminations.load()) +
        ",\"addressBytes\":" + std::to_string(resources->addressReserved.load()) +
        ",\"addressMaximum\":" + std::to_string(resources->addressMaximum) +
        ",\"residents\":" + std::to_string(resources->residents.load()) +
        ",\"residentMaximum\":" + std::to_string(resources->residentMaximum.load()) +
        ",\"configuredResidentMaximum\":" + std::to_string(resources->configuredResidentMaximum) +
        ",\"rejected\":" + std::to_string(resources->rejections.load()) +
        ",\"sandboxRefused\":" + std::to_string(sandboxRefusals.load()) +
        ",\"evicted\":" + std::to_string(resources->evictions.load()) +
        ",\"tenantRejected\":" + std::to_string(tenantBudget->rejected.load()) +
        ",\"limitRowsDropped\":" + std::to_string(limitRowsDropped.load()) + "}}";
    lifecycle.publish(json, active, background, unknown);
  }

  void reapRetired() {
    std::vector<std::shared_ptr<Slot>> released;
    {
      std::lock_guard lock(registryMutex);
      std::erase_if(retiredSlots, [&](auto &slot) {
        if (slot.use_count() != 1 || slot->contexts.load() != 0) return false;
        released.push_back(std::move(slot));
        return true;
      });
    }
    released.clear();
  }

  static uint32_t workerLimit(Json::Reader descriptor, kj::StringPtr name, const char *variable) {
    auto value = field(descriptor, name);
    if (value.isNumber() && value.getNumber() != 0) return uint32_t(value.getNumber());
    return uint32_t(configuredCount(variable, 0));
  }

  // Destruction runs on the current call thread, with its ThreadContext alive,
  // and never while holding the shared registry lock.
  bool evictOne(Slot *exclude = nullptr, bool expiredOnly = false) {
    std::shared_ptr<Slot> victim;
    {
      std::lock_guard lock(registryMutex);
      auto selected = registry.end();
      auto cutoff = configuredCount("COWORKERD_THREADED_IDLE_EVICT_MS", 300000) * 1000000;
      auto now = nowNs();
      for (auto at = registry.begin(); at != registry.end(); ++at) {
        auto &slot = at->second;
        if (!canEvict(slot, exclude)) continue;
        if (expiredOnly && (cutoff == 0 || now - kj::min(now, slot->lastUseNs.load()) < cutoff) &&
            !(slot->limits && slot->limits->memoryState->load() == LimitCause::MEMORY))
          continue;
        if (selected == registry.end() ||
            slot->lastUseNs.load() < selected->second->lastUseNs.load())
          selected = at;
      }
      if (selected == registry.end()) return false;
      victim = std::move(selected->second);
      registry.erase(selected);
    }
    resources->evictions.fetch_add(1);
    victim.reset();
    return true;
  }

  std::shared_ptr<ResourcePreparation> prepareSlot(const std::shared_ptr<Slot> &slot) {
    while (!slot->reservation->prepare()) {
      if (!evictOne(slot.get())) {
        resources->rejections.fetch_add(1);
        kj::throwRecoverableException(capacityError());
      }
    }
    return std::make_shared<ResourcePreparation>(slot->reservation);
  }

  kj::Promise<kj::String> confirmDecisionImpl(Json::Reader input,
      ThreadState &state,
      kj::StringPtr decision,
      kj::StringPtr reason,
      threaded::LoadStage loadStage) {
    // One descriptor per event loop is held aside for this private proof path.
    // Releasing it permits the receipt exchange even during process EMFILE.
    state.decisionSpare = nullptr;
    KJ_DEFER(state.decisionSpare = kj::AutoCloseFd(open("/dev/null", O_RDONLY | O_CLOEXEC)));
    auto receipt = field(input, "dispatchReceipt");
    KJ_REQUIRE(receipt.isString() && receipt.getString().size() == 32,
        "missing private dispatch decision receipt");
    capnp::MallocMessageBuilder arena;
    auto value = arena.initRoot<Json>();
    value.setString(reason);
    auto stage = threaded::loadStageInfo(loadStage);
    auto body = kj::str("{\"invocationId\":", encode(field(input, "invocationId")),
        ",\"receipt\":", encode(receipt), ",\"state\":\"", decision,
        "\",\"reason\":", encode(value.asReader()), ",\"stage\":\"",
        kj::heapString(stage.stage.data(), stage.stage.size()), "\",\"codeStage\":\"",
        kj::heapString(stage.codeStage.data(), stage.codeStage.size()), "\"}");
    auto address = co_await state.io.provider->getNetwork().parseAddress(sourceAddress.c_str());
    auto client = kj::newHttpClient(state.io.provider->getTimer(), *state.headers, *address);
    kj::HttpHeaders headers(*state.headers);
    headers.setPtr(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
    auto request =
        client->request(kj::HttpMethod::POST, "/v1/dispatch-state", headers, body.size());
    co_await request.body->write(body.asBytes());
    auto response = co_await request.response;
    KJ_REQUIRE(response.statusCode == 200, "private dispatch decision unavailable");
    auto report = co_await response.body->readAllText(8192);
    JsonDoc decoded(report);
    KJ_REQUIRE(flag(decoded.get(), "accepted"), "private dispatch decision refused");
    co_return encode(field(decoded.get(), "refusal"));
  }
  kj::Promise<kj::String> confirmDecision(Json::Reader input,
      ThreadState &state,
      kj::StringPtr decision,
      kj::StringPtr reason,
      threaded::LoadStage stage = threaded::LoadStage::LOAD) {
    return state.io.provider->getTimer().timeoutAfter(
        250 * kj::MILLISECONDS, confirmDecisionImpl(input, state, decision, reason, stage));
  }
  kj::Promise<kj::String> confirmCapacity(
      Json::Reader input, ThreadState &state, CapacityScope scope = CapacityScope::PROCESS) {
    return confirmDecision(input, state, "capacity-rejected",
        scope == CapacityScope::TENANT ? "tenant execution concurrency limit reached in process"
                                       : "process admission budget is full");
  }
  static void wake(int fd) {
    uint64_t one = 1;
    ssize_t n;
    do {
      n = write(fd, &one, sizeof(one));
    } while (n < 0 && errno == EINTR);
    KJ_REQUIRE(n == sizeof(one) || (n < 0 && errno == EAGAIN));
  }
  BudgetLifetime retainDurableBudget(uint64_t ticket) {
    if (ticket == 0) return {};
    KJ_REQUIRE(parent >= 0, "durable budget ticket requires the parent socket");
    return std::make_shared<DurableBudgetRelease>(*this, ticket);
  }
  void queueBudgetRelease(uint64_t ticket) {
    std::lock_guard lock(budgetReleaseMutex);
    const bool needsWake = budgetReleases.empty();
    budgetReleases.push_back(ticket);
    if (needsWake) wake(managerWake);
  }
  void requestDurableMemoryRelease() {
    if (!releaseDurableMemoryRequested.exchange(true)) wake(managerWake);
  }

  void releaseIdleDurablePages() {
    {
      std::lock_guard lock(durableRoutingMutex);
      if (durableAdmission->state().trees != 0) return;
      if (!releaseDurableMemoryRequested.exchange(false)) return;
    }
    using Allocator = tcmalloc::MallocExtension;
    const auto free = Allocator::GetNumericProperty("tcmalloc.pageheap_free_bytes").value_or(0);
    if (free == 0) return;

    // Honor TCMalloc's demand history during normal operation. The idle trim can return free
    // portions of huge pages even when the allocation peak was recent.
    const auto shortInterval = Allocator::GetSkipSubreleaseShortInterval();
    const auto longInterval = Allocator::GetSkipSubreleaseLongInterval();
    KJ_DEFER({
      Allocator::SetSkipSubreleaseShortInterval(shortInterval);
      Allocator::SetSkipSubreleaseLongInterval(longInterval);
    });
    Allocator::SetSkipSubreleaseShortInterval(absl::ZeroDuration());
    Allocator::SetSkipSubreleaseLongInterval(absl::ZeroDuration());
    Allocator::ReleaseMemoryToSystem(free);
  }

  // The durable pool manager drains the budget mailbox and returns free allocator pages.
  // Neither operation holds a home, isolate, or routing lock.
  void manageDurable() {
    if (parent < 0) return;
    constexpr uint64_t MIN_RELEASE_INTERVAL_NS = 1000000000;
    auto nextRelease = nowNs();
    size_t offset = 0;
    for (;;) {
      bool pending;
      bool retry = false;
      {
        std::lock_guard lock(budgetReleaseMutex);
        while (!budgetReleases.empty()) {
          const auto* bytes = reinterpret_cast<const char*>(&budgetReleases.front());
          const auto written = send(parent, bytes + offset, sizeof(uint64_t) - offset,
              MSG_DONTWAIT | MSG_NOSIGNAL);
          if (written < 0 && errno == EINTR) continue;
          if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
          if (written <= 0) {
            if (written < 0 && (errno == EPIPE || errno == ECONNRESET || errno == ENOTCONN)) return;
            retry = true;
            break;
          }
          offset += written;
          if (offset == sizeof(uint64_t)) {
            budgetReleases.pop_front();
            offset = 0;
          }
        }
        pending = !budgetReleases.empty();
      }
      if (nowNs() >= nextRelease && releaseDurableMemoryRequested.load()) {
        releaseIdleDurablePages();
        nextRelease = nowNs() + MIN_RELEASE_INTERVAL_NS;
      }
      // Keep a partially sent ticket at the front until the parent receives it.
      if (retry) {
        (void)poll(nullptr, 0, 10);
        continue;
      }
      pollfd waits[] = {{managerWake, POLLIN, 0}, {parent, short(pending ? POLLOUT : 0), 0}};
      int result;
      const int timeout = releaseDurableMemoryRequested.load() ? 1000 : -1;
      do {
        result = poll(waits, 2, timeout);
      } while (result < 0 && errno == EINTR);
      if (result >= 0 && (waits[1].revents & POLLHUP)) return;
      if (result < 0 || (waits[1].revents & (POLLERR | POLLNVAL))) (void)poll(nullptr, 0, 10);
      uint64_t count;
      while (read(managerWake, &count, sizeof(count)) > 0) {}
    }
  }
  bool enqueue(int fd) {
    if (!pending.push(fd)) return false;
    wake(managerWake);
    return true;
  }

  // Called with durableRoutingMutex held. The home still rechecks all the
  // prerequisites locally before it fences admission to the selected tree.
  bool requestDatabaseRetirement(server::DurableAdmission::Time now, bool needsPreparationBudget,
      std::optional<uint64_t> selectedHome = std::nullopt) {
    if (databaseVictim) {
      if (!needsPreparationBudget) databaseVictim->preferDatabase = true;
      if (needsPreparationBudget && !databaseVictim->preparationCeiling) {
        const auto reservation = threads[databaseVictim->home - 1]->home->databaseReservation.lock();
        if (!reservation) return false;
        databaseVictim->preparationCeiling = reservation->replacementCeiling();
        if (!databaseVictim->preparationCeiling) return false;
      }
      return true;
    }
    const auto deadline = now + std::chrono::seconds(1);
    std::shared_ptr<ResourceReservation> retiringReservation;
    for (const auto& thread: threads) {
      const auto& home = *thread->home;
      if ((selectedHome && *selectedHome != thread->homeId) || home.reclaiming ||
          (homeVictim && homeVictim->home == thread->homeId) || !home.databaseCanRetire || home.reservedTrees != 1 || home.resident.size() != 1 ||
          !home.warmDefinition.starts_with(PLATFORM_DATABASE_COPY_PREFIX) ||
          home.pressure.resetting()) {
        continue;
      }
      const auto tree = *home.resident.begin();
      if (!durableAdmission->isIdleTree(tree, thread->homeId) ||
          (databaseVictim && home.databaseLastUse >= databaseVictim->lastUse)) continue;
      auto reservation = home.databaseReservation.lock();
      if (!reservation) continue;
      const auto preparationCeiling =
          needsPreparationBudget ? reservation->replacementCeiling() : std::nullopt;
      if (needsPreparationBudget && !preparationCeiling) continue;
      databaseVictim =
          DatabaseRetirement{thread->homeId, tree, home.databaseLastUse, reservation->returned,
            deadline, !needsPreparationBudget, preparationCeiling};
      retiringReservation = std::move(reservation);
    }
    if (!databaseVictim) return false;
    // Both routing and cold-load retirement leave the returned preparation
    // credit for a database. Hold the reservation until the preference is set.
    resources->preferDatabasePreparation(deadline + DATABASE_ADMISSION_GRACE);
    wake(threads[databaseVictim->home - 1]->wake);
    return true;
  }

  // The routing lock serializes the completion pass and the accept loop's deadline.
  void finishDatabaseRetirement(DatabaseRetirement::Outcome outcome,
      server::DurableAdmission::Time now) {
    using Outcome = DatabaseRetirement::Outcome;
    if (outcome == Outcome::PENDING) return;
    auto& home = *threads[databaseVictim->home - 1]->home;
    if (outcome == Outcome::RELEASED && databaseVictim->preferDatabase) {
      // Leave time for the native preface retry, whose backoff is capped at 50 ms.
      home.databasePreferredUntil = now + DATABASE_ADMISSION_GRACE;
    }
    if (databaseVictim->treePreferred) {
      durableAdmission->limitDatabaseTreePreference(
          outcome == Outcome::RELEASED ? now + DATABASE_ADMISSION_GRACE : now);
    }
    resources->limitDatabasePreparationPreference(
        outcome == Outcome::RELEASED ? now + DATABASE_ADMISSION_GRACE : now);
    if (outcome == Outcome::EXPIRED) {
      home.databaseCanRetire = false;
      durableAdmission->recordOverload();
      KJ_LOG(WARNING, "idle database admission retirement timed out",
          databaseVictim->home, databaseVictim->tree);
    }
    DatabaseReplacement replacement;
    if (outcome == Outcome::RELEASED && databaseVictim->preparationCeiling) {
      const auto& returned = *databaseVictim->reservationReturned;
      // Recheck the actual return. The copy's charge may have changed after selection.
      const auto ceiling = returned.releasedMemory.load() < ResourceBudget::PREPARATION_MEMORY
          ? resources->admissionMaximum
          : kj::max(resources->admissionMaximum,
                kj::min(*databaseVictim->preparationCeiling, returned.memoryBeforeRelease));
      replacement = std::make_shared<DatabasePreparationReplacement>(ceiling);
    }
    databaseVictim.reset();
    for (const auto &waiter: databaseCapacityWaiters) {
      if (waiter->isWaiting()) waiter->fulfill(DatabaseReplacement(replacement));
    }
    databaseCapacityWaiters.clear();
  }

  std::vector<server::DurableAdmission::Ticket> expireDurableAdmissions(
      std::optional<server::DurableAdmission::Time> at = std::nullopt) {
    std::lock_guard lock(durableRoutingMutex);
    const auto now = at ? *at : server::DurableAdmission::Clock::now();
    if (databaseVictim && now >= databaseVictim->deadline) {
      finishDatabaseRetirement(databaseVictim->outcome(now, false, false), now);
    }
    if (homeVictim && now >= homeVictim->deadline) {
      threads[homeVictim->home - 1]->home->admissionTurnDelayed = true;
      homeVictim.reset();
    }
    if (objectVictim && now >= objectVictim->deadline) {
      if (objectVictim->awaitingWal) {
        // The actor has been dropped. Release the routing credit even if a
        // reference still holds a WAL registration or the home stops polling.
        durableAdmissionWalTimeouts.fetch_add(1);
        releaseDurableTree(objectVictim->tree, objectVictim->home, true);
      } else {
        // Do not choose the same home again until its loop shows progress.
        threads[objectVictim->home - 1]->home->admissionTurnDelayed = true;
        objectVictim.reset();
      }
    }
    return durableAdmission->expire(now);
  }

  kj::Promise<DatabaseReplacement> reclaimIdleDatabase(
      std::optional<server::DurableAdmission::Time> at = std::nullopt) {
    auto signal = kj::newPromiseAndCrossThreadFulfiller<DatabaseReplacement>();
    {
      std::lock_guard lock(durableRoutingMutex);
      const auto now = at ? *at : server::DurableAdmission::Clock::now();
      if (!requestDatabaseRetirement(now, true)) co_return DatabaseReplacement{};
      std::erase_if(databaseCapacityWaiters, [](auto &waiter) { return !waiter->isWaiting(); });
      databaseCapacityWaiters.push_back(kj::mv(signal.fulfiller));
    }
    co_return co_await signal.promise;
  }

  server::DurableCopyCandidate durableCopyCandidate(uint64_t id,
      server::DurableAdmission::Time now) const {
    const auto& home = *threads[id - 1]->home;
    return {id, home.definition, home.reservedTrees, home.memory, home.load.busyFor(now),
      home.pressure.accepting()};
  }

  // Both placement and victim selection run under durableRoutingMutex.
  std::optional<uint64_t> findDurableHome(const DurableRoute& route, bool database,
      server::DurableAdmission::Time now) const {
    std::vector<server::DurableCopyCandidate> candidates;
    uint64_t spare = 0;
    for (uint64_t id = 1; id <= threads.size(); ++id) {
      const auto& thread = *threads[id - 1];
      if (!thread.home || (databaseVictim && databaseVictim->home == id)) continue;
      const auto& home = *thread.home;
      if (home.reclaiming || home.walFull.load(std::memory_order_relaxed)) continue;
      if (!database && now < home.databasePreferredUntil) continue;
      // New-root eligibility is checked here, including on the empty-home path.
      // The empty copy owns its old isolate until the home releases it.
      if (home.reservedTrees == 0 && !home.pressure.resetting() &&
          (spare == 0 ||
              (home.definition.empty() && !threads[spare - 1]->home->definition.empty()))) {
        spare = id;
      }
      if (!home.definition.empty()) candidates.push_back(durableCopyCandidate(id, now));
    }
    if (auto warm = server::chooseDurableCopy(candidates, route.open.definition, durablePlacement)) {
      return warm;
    }
    return spare == 0 ? std::nullopt : std::optional(spare);
  }

  bool requestObjectRetirement(const DurableRoute& route, server::DurableAdmission::Time now,
      bool needsPlacement, bool matchingOnly = false) {
    if (objectVictim) return true;
    const auto deadline = now + std::chrono::seconds(1);
    bool selectedRunning = false;
    for (const auto& thread: threads) {
      const auto id = thread->homeId;
      const auto& home = *thread->home;
      if (home.reclaiming || (homeVictim && homeVictim->home == id) ||
          (matchingOnly && home.definition != route.open.definition) ||
          home.idleObjects.empty() || home.pressure.resetting() || home.admissionTurnDelayed ||
          now < home.databasePreferredUntil ||
          home.warmDefinition.starts_with(PLATFORM_DATABASE_COPY_PREFIX)) {
        continue;
      }
      if (needsPlacement && home.reservedTrees != 1) {
        // Evaluate placement after a root leaves. A full home can be a source of credit.
        auto candidate = durableCopyCandidate(id, now);
        --candidate.residentTrees;
        if (!server::chooseDurableCopy(
                std::span(&candidate, 1), route.open.definition, durablePlacement)) continue;
      }
      const bool running = home.load.hasRunningTurn();
      for (const auto& [tree, lastUse]: home.idleObjects) {
        if (objectVictim && std::tuple(running, lastUse, tree) >=
            std::tuple(selectedRunning, objectVictim->lastUse, objectVictim->tree)) continue;
        if (durableAdmission->isIdleTree(tree, id)) {
          objectVictim = ObjectRetirement{id, tree, lastUse, deadline};
          selectedRunning = running;
        }
      }
    }
    if (!objectVictim) return false;
    wake(threads[objectVictim->home - 1]->wake);
    return true;
  }

  // The routing lock is held. The home rechecks live actor state before it
  // fences the set, since the router only has each loop's idle snapshot.
  bool canReclaimHome(const Thread& thread) const {
    const auto& home = *thread.home;
    if (home.definition.empty() || home.reclaiming || home.pressure.resetting() ||
        home.admissionTurnDelayed || home.load.hasRunningTurn() ||
        home.reservedTrees != home.resident.size()) return false;
    const bool database = home.warmDefinition.starts_with(PLATFORM_DATABASE_COPY_PREFIX);
    if (database ? (home.reservedTrees != 0 && !home.databaseCanRetire)
                 : home.resident.size() != home.idleObjects.size()) return false;
    return std::ranges::all_of(home.resident, [&](uint64_t tree) {
      return durableAdmission->isIdleTree(tree, thread.homeId);
    });
  }

  bool requestHomeRetirement(server::DurableAdmission::Time now, bool database) {
    if (homeVictim || (database && databaseVictim)) return true;
    Thread* oldest = nullptr;
    for (const auto& thread: threads) {
      const auto id = thread->homeId;
      if ((databaseVictim && databaseVictim->home == id) ||
          (objectVictim && objectVictim->home == id) ||
          now < thread->home->databasePreferredUntil || !canReclaimHome(*thread)) continue;
      if (oldest == nullptr || thread->home->lastUse < oldest->home->lastUse) {
        oldest = thread.get();
      }
    }
    if (oldest == nullptr) return false;
    // Reuse the database replacement path when the oldest home is already a
    // database. It preserves the preference given to the retiring copy's credit.
    if (database && oldest->home->warmDefinition.starts_with(PLATFORM_DATABASE_COPY_PREFIX) &&
        requestDatabaseRetirement(now, false, oldest->homeId)) return true;
    homeVictim = HomeRetirement{oldest->homeId, oldest->home->activity,
      now + std::chrono::seconds(1)};
    wake(oldest->wake);
    return true;
  }

  bool fenceHome(const HomeRetirement& selected, server::DurableAdmission::Time now) {
    auto& thread = *threads[selected.home - 1];
    auto& home = *thread.home;
    if (homeVictim != selected || now >= selected.deadline ||
        selected.activity != home.activity || !canReclaimHome(thread)) return false;
    for (auto tree: home.resident) {
      KJ_ASSERT(durableAdmission->stopIdleTree(tree, selected.home));
    }
    home.reclaiming = true;
    return true;
  }

  // The accept loop holds placement and admission together before acknowledging a connection.
  bool reserveDurableHome(
      const DurableRoute &route, server::DurableAdmission::Ticket ticket, bool database,
      server::DurableAdmission::Time now, const char*& refusal) {
    auto assigned = durableAdmission->homeFor(ticket.tree);
    if (!assigned) {
      assigned = findDurableHome(route, database, now);
      const bool cold = !assigned ||
          (threads[*assigned - 1]->home->reservedTrees == 0 &&
              threads[*assigned - 1]->home->warmDefinition != route.open.definition);
      std::shared_ptr<ResourceReservation> preparation;
      if (maxDurableHomes != 0 && cold) {
        preparation = std::make_shared<ResourceReservation>(resources, 0, database);
        if (!preparation->registerPreparation(nullptr, &refusal)) return false;
      }
      if (!assigned) {
        if (maxDurableHomes == 0) return false;
        assigned = startDurableHome(refusal);
        if (!assigned) return false;
      }
      auto& home = *threads[*assigned - 1]->home;
      if (preparation) home.preparation = std::move(preparation);
      home.definition = route.open.definition;
      home.databasePreferredUntil = {};
      KJ_ASSERT(durableAdmission->publishHome(ticket.tree, *assigned));
      ++home.reservedTrees;
    }
    auto& home = *threads[*assigned - 1]->home;
    if (home.pressure.resetting() || home.reclaiming) return false;
    ++home.activity;
    return true;
  }

  server::DurableAdmission::Result queueDurable(
      const DurableRoute &route, uint64_t bytes, server::DurableAdmission::Time deadline,
      std::optional<server::DurableAdmission::Time> at = std::nullopt) {
    using Admission = server::DurableAdmission;
    if (!ready.load(std::memory_order_acquire)) return {Admission::Status::OVERLOADED};
    const bool database = route.open.className == "SqlDatabase" &&
        route.open.definition.starts_with(PLATFORM_DATABASE_COPY_PREFIX);
    std::lock_guard lock(durableRoutingMutex);
    const auto now = at ? *at : Admission::Clock::now();
    auto result = durableAdmission->queue(
        route.key, route.open, bytes, deadline, now, database, true);
    bool needsPlacement = false;
    const char* refusal = nullptr;
    if (result.ticket && !reserveDurableHome(route, *result.ticket, database, now, refusal)) {
      needsPlacement = result.needsHome;
      KJ_ASSERT(durableAdmission->refuseWaiting(*result.ticket, !needsPlacement));
      durableAdmission->releaseUnassigned(result.ticket->tree);
      result = {.status = Admission::Status::OVERLOADED, .newTreeCapacityRefused = result.needsHome};
    } else if (database && result.ticket && result.needsHome) {
      durableAdmission->claimDatabaseTree();
    }
    if (result.newTreeCapacityRefused) {
      // The pre-dispatch retry waits for idle retirement on its original lease.
      const bool lackingHome = needsPlacement || !findDurableHome(route, database, now);
      const bool retiringTree = database ? (!needsPlacement && requestDatabaseRetirement(now, false))
          : requestObjectRetirement(route, now, lackingHome, needsPlacement);
      if (retiringTree || (lackingHome && requestHomeRetirement(now, database))) {
        return {Admission::Status::RETIRING};
      }
      if (refusal != nullptr) {
        lastHomeRefusal = refusal;
        ++homeRefusals;
      }
      durableAdmission->recordOverload();
    }
    return result;
  }

  // The connection already owns a home reservation. Only that home reads RPC bytes.
  bool handoffDurable(int fd,
      DurableRoute &route,
      server::DurableAdmission::Ticket ticket,
      server::DurableAdmission::Time deadline) {
    if (!ready.load(std::memory_order_acquire)) return false;
    std::lock_guard lock(durableRoutingMutex);
    if (deadline <= server::DurableAdmission::Clock::now()) return false;
    auto assigned = durableAdmission->homeFor(ticket.tree);
    if (!assigned) return false;
    auto &thread = *threads[*assigned - 1];
    if (thread.home->pressure.resetting()) return false;
    thread.home->incoming.push_back({kj::AutoCloseFd(fd), std::move(route), ticket, deadline});
    wake(thread.wake);
    return true;
  }

  void publishDurableRoot(uint64_t homeId, uint64_t tree) {
    std::lock_guard lock(durableRoutingMutex);
    KJ_ASSERT(durableAdmission->homeFor(tree) == homeId);
    threads[homeId - 1]->home->resident.insert(tree);
  }

  void markDurableSocket(ThreadState& state, server::DurableAdmission::Ticket ticket,
      const std::shared_ptr<DurableSessionScope>& session) {
    auto& root = state.durableRoots.at(ticket.tree);
    std::lock_guard lock(durableRoutingMutex);
    KJ_REQUIRE(durableAdmission->markSocket(ticket, state.durableHome), "the durable tree is closing");
    KJ_ASSERT(root.sockets.emplace(ticket.event, session).second);
    root.idle->eventFinished(durableIdleNow());
  }

  bool finishDurable(server::DurableAdmission::Ticket ticket, bool databaseCanRetire = false,
      std::optional<server::DurableAdmission::Time> objectLastUse = std::nullopt) {
    std::lock_guard lock(durableRoutingMutex);
    durableAdmission->cancel(ticket);
    wakeDurableTree(ticket.tree);
    auto assigned = durableAdmission->homeFor(ticket.tree);
    if (!assigned) {
      durableAdmission->releaseUnassigned(ticket.tree);
      return false;
    }
    const auto finished = durableAdmission->complete(ticket, *assigned);
    auto &home = *threads[*assigned - 1]->home;
    if (home.resident.contains(ticket.tree)) {
      if (finished) {
        home.lastUse = kj::max(home.lastUse,
            objectLastUse.value_or(server::DurableAdmission::Clock::now()));
      }
      if (finished && home.warmDefinition.starts_with(PLATFORM_DATABASE_COPY_PREFIX)) {
        home.databaseLastUse = nowNs();
        home.databaseCanRetire = databaseCanRetire;
      } else if (finished) {
        if (objectLastUse) {
          home.idleObjects.insert_or_assign(ticket.tree, *objectLastUse);
        } else {
          home.idleObjects.erase(ticket.tree);
        }
      }
      return finished;
    }
    if (!durableAdmission->stopIdleTree(ticket.tree, *assigned))
      return finished;
    KJ_ASSERT(durableAdmission->releaseTree(ticket.tree, *assigned));
    if (--home.reservedTrees == 0) {
      home.preparation.reset();
      home.definition = home.warmDefinition;
    }
    return finished;
  }

  // durableRoutingMutex is held while checking the FIFO and installing or
  // waking a waiter. Fulfillment just posts to the home loop.
  void wakeDurableTree(uint64_t tree) {
    auto at = durableWaiters.find(tree);
    if (at == durableWaiters.end()) return;
    for (auto &[event, signal]: at->second) {
      if (signal->isWaiting()) signal->fulfill();
    }
    durableWaiters.erase(at);
  }

  kj::Promise<void> admitDurableEvent(ThreadState &state, const DurableRoute &route,
      server::DurableAdmission::Ticket ticket, bool platformDatabase) {
    using Admission = server::DurableAdmission;
    for (;;) {
      auto signal = kj::newPromiseAndCrossThreadFulfiller<void>();
      {
        std::lock_guard lock(durableRoutingMutex);
        auto first = durableAdmission->next(ticket.tree);
        if (!first.has_value() || durableAdmission->homeFor(ticket.tree) != state.durableHome) {
          kj::throwRecoverableException(jsg::tagCoworkerdRpcError(
              KJ_EXCEPTION(OVERLOADED, "durable object reset is draining"), "DO_UNAVAILABLE"));
        }
        // Let the previous database response leave its home before the next
        // synchronous query can block that home's event loop.
        if (first == ticket && !(platformDatabase && durableAdmission->hasActiveEvent(ticket.tree))) {
          const auto delivery = durableAdmission->begin(
              ticket, state.durableHome, route.key, route.open, Admission::Clock::now());
          if (delivery == Admission::Delivery::EXPIRED) {
            kj::throwRecoverableException(jsg::tagCoworkerdRpcError(
                KJ_EXCEPTION(OVERLOADED, "durable routing lease expired"), "LEASE_EXPIRED"));
          }
          KJ_REQUIRE(delivery == Admission::Delivery::ACCEPTED, "durable event identity changed");
          wakeDurableTree(ticket.tree);
          co_return;
        }
        durableWaiters[ticket.tree].emplace(ticket.event, kj::mv(signal.fulfiller));
      }
      KJ_DEFER({
        std::lock_guard lock(durableRoutingMutex);
        auto at = durableWaiters.find(ticket.tree);
        if (at != durableWaiters.end()) {
          at->second.erase(ticket.event);
          if (at->second.empty()) durableWaiters.erase(at);
        }
      });
      co_await signal.promise;
    }
  }

  kj::Promise<void> startDurableSession(ThreadState &state, DurableIncoming call) {
    const bool database = call.route.open.definition.starts_with(PLATFORM_DATABASE_COPY_PREFIX);
    KJ_DEFER({
      auto root = state.durableRoots.find(call.ticket.tree);
      const auto now = durableIdleNow();
      const bool canRetire = root != state.durableRoots.end() && canRetireAtAdmission(root->second);
      const auto objectLastUse = !database && canRetire ? std::optional(now) : std::nullopt;
      if (finishDurable(call.ticket, database && canRetire, objectLastUse)) {
        if (root != state.durableRoots.end() && !root->second.resetting) {
          root->second.sockets.erase(call.ticket.event);
          root->second.idle->eventFinished(now);
        }
      }
    });
    // Runs before the release above: an event this session was still preparing ends here,
    // and with it the waiter it has on a cold load.
    auto scope = std::make_shared<DurableSessionScope>();
    scope->socketFd = call.socket.get();
    auto &sessions = state.durableSessions[call.ticket.tree];
    KJ_ASSERT(sessions.emplace(call.ticket.event, scope).second);
    KJ_DEFER({
      sessions.erase(call.ticket.event);
      if (sessions.empty()) state.durableSessions.erase(call.ticket.tree);
      scope->ended = true;
      scope->preparing.cancel("durable session ended before its event was prepared");
    });
    const auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(
        call.deadline - server::DurableAdmission::Clock::now());
    if (remaining.count() <= 0) co_return;
    durableHomeSessions.fetch_add(1);
    KJ_DEFER(durableHomeSessions.fetch_sub(1));
    auto stream = state.io.lowLevelProvider->wrapSocketFd(kj::mv(call.socket));
    auto accepted = kj::newPromiseAndFulfiller<void>();
    capnp::TwoPartyServer session(kj::heap<DurableBootstrap>(
        *this, state, std::move(call.route), call.ticket, kj::mv(accepted.fulfiller), scope));
    auto disconnected = session.accept(*stream).fork();
    try {
      auto serving =
          state.io.provider->getTimer()
              .timeoutAfter(remaining.count() * kj::NANOSECONDS, kj::mv(accepted.promise))
              .exclusiveJoin(disconnected.addBranch())
              .then([&disconnected] { return disconnected.addBranch(); });
      // The membrane keeps streams and capabilities alive independently of
      // the timer for the unanswered open.
      co_await scope->transport.wrap(kj::mv(serving));
    } catch (const kj::Exception &error) {
      reportDurableFailureForTest("session", error);
    }
  }

  void beginDurableReset(ThreadState &state, uint64_t tree, const kj::Exception &reason) {
    auto root = state.durableRoots.find(tree);
    if (root == state.durableRoots.end() || root->second.resetting) return;
    root->second.resetting = true;
    durableTreeResets.fetch_add(1);
    {
      std::lock_guard lock(durableRoutingMutex);
      KJ_ASSERT(durableAdmission->beginReset(tree, state.durableHome));
      wakeDurableTree(tree);
    }
    auto code = reason.getDetail(jsg::COWORKERD_CODE_DETAIL_ID).map([](auto bytes) {
      return kj::str(bytes.asChars());
    });
    root->second.notifyReset(code.orDefault(kj::String()));
    if (code == kj::none || KJ_ASSERT_NONNULL(code) != "DO_CODE_UPDATED"_kj) return;
    // Give failed calls a chance to return the actor's typed exception before
    // closing capabilities that still hold the drained definition. Resource
    // resets leave the regular typed-error path in charge of their sessions.
    state.tasks.add(state.io.provider->getTimer()
                        .afterDelay(100 * kj::MILLISECONDS)
                        .then([&state, tree, reason = reason.clone()] {
      const auto at = state.durableSessions.find(tree);
      if (at == state.durableSessions.end()) return;
      std::vector<std::shared_ptr<DurableSessionScope>> sessions;
      for (const auto &[event, reference]: at->second) {
        if (auto session = reference.lock()) sessions.push_back(std::move(session));
      }
      for (const auto &session: sessions) session->transport.cancel(reason);
    }));
  }

  static bool canRetireAtAdmission(ThreadState::DurableRoot &root) {
    if (root.resetting || root.closeRequested || root.idleClosing || root.admissionClosing ||
        root.adopting ||
        root.parking != kj::none || root.parked != kj::none || root.idle->hasPending() ||
        !root.sockets.empty() || root.actor->getNonHibernatableWebSocketCount()->count > 0 ||
        root.facetHasOrdinaryWebSocket() || (root.nodeSockets && root.hasLocalSockets()) ||
        root.facetsBusy(true)) {
      return false;
    }
    KJ_IF_SOME(context, root.actor->getIoContext()) {
      if (context.hasCurrentIncomingRequest()) return false;
    }
    return true;
  }

  // The caller holds the routing lock across the last check and the stop.
  bool stopDurableIdleTree(uint64_t tree, uint64_t homeId, bool closeSocketSessions,
      const DatabaseRetirement* selected,
      std::optional<server::DurableAdmission::Time> at = std::nullopt) {
    if (selected &&
        (!databaseVictim || databaseVictim->tree != tree ||
            databaseVictim->deadline != selected->deadline ||
            (at ? *at : server::DurableAdmission::Clock::now()) >= selected->deadline ||
            threads[homeId - 1]->home->databaseLastUse != selected->lastUse ||
            !durableAdmission->isIdleTree(tree, homeId))) {
      return false;
    }
    return closeSocketSessions ? durableAdmission->stopIdleTreeWithSockets(tree, homeId)
                               : durableAdmission->stopIdleTree(tree, homeId);
  }

  // The home has just rechecked actor state. Under the routing lock, also rule out calls
  // queued since selection and candidates whose idle observation has changed.
  bool stopObjectAdmissionVictim(const ObjectRetirement& selected,
      server::DurableAdmission::Time now) {
    if (objectVictim != selected || now >= selected.deadline) return false;
    const auto& idle = threads[selected.home - 1]->home->idleObjects;
    const auto found = idle.find(selected.tree);
    return found != idle.end() && found->second == selected.lastUse &&
        durableAdmission->isIdleTree(selected.tree, selected.home) &&
        durableAdmission->stopIdleTree(selected.tree, selected.home);
  }

  // Called under the routing lock after dropping the actor. Ordinary admission
  // retirement waits for the home's WAL registrations up to its deadline.
  bool releaseDurableTree(uint64_t tree, uint64_t homeId, bool admissionClosing) {
    KJ_ASSERT(durableAdmission->releaseTree(tree, homeId));
    if (admissionClosing && databaseVictim && databaseVictim->tree == tree &&
        databaseVictim->preferDatabase) {
      // Keep the tree credit while the reservation is still returning. The
      // completion pass shortens this to the same retry window as the home.
      durableAdmission->preferDatabaseTree(databaseVictim->deadline + DATABASE_ADMISSION_GRACE);
      databaseVictim->treePreferred = true;
    }
    auto &home = *threads[homeId - 1]->home;
    home.resident.erase(tree);
    home.idleObjects.erase(tree);
    if (objectVictim && objectVictim->tree == tree) objectVictim.reset();
    if (admissionClosing) durableAdmissionRetirements.fetch_add(1);
    home.databaseCanRetire = false;
    home.databaseLastUse = 0;
    if (--home.reservedTrees == 0) home.definition = home.warmDefinition;
    return home.resident.empty();
  }

  void waitForAdmissionWal(ThreadState& state, uint64_t tree,
      std::shared_ptr<DurableTreeContext> context,
      server::DurableAdmission::Time now = server::DurableAdmission::Clock::now()) {
    const auto deadline = now + OBJECT_ADMISSION_WAL_WAIT;
    KJ_ASSERT(state.admissionWalClosing.emplace(
        tree, ThreadState::AdmissionWalClose{std::move(context), deadline}).second);
    std::lock_guard lock(durableRoutingMutex);
    if (objectVictim && objectVictim->tree == tree) {
      objectVictim->awaitingWal = true;
      objectVictim->deadline = deadline;
    }
  }

  void releaseClosedAdmissionTrees(ThreadState& state,
      server::DurableAdmission::Time now = server::DurableAdmission::Clock::now()) {
    for (auto it = state.admissionWalClosing.begin(); it != state.admissionWalClosing.end();) {
      const auto current = it++;
      const auto tree = current->first;
      const auto& close = current->second;
      const bool walPending = hasDurableWalRegistrations(*close.context);
      {
        std::lock_guard lock(durableRoutingMutex);
        // The accept loop can release the same tree at the deadline.
        if (durableAdmission->homeFor(tree) == state.durableHome) {
          if (walPending && now < close.deadline) continue;
          if (walPending) durableAdmissionWalTimeouts.fetch_add(1);
          // On normal close the home published walFull before returning routing credit.
          if (releaseDurableTree(tree, state.durableHome, true)) {
            state.durableSlot->lastUseNs.store(nowNs());
          }
        }
      }
      state.admissionWalClosing.erase(current);
    }
  }

  void retireIdleDurableRoots(
      ThreadState &state, server::DurableIdleLifecycle::Time now = durableIdleNow()) {
    const auto homeId = state.durableHome;
    const auto [victim, selectedObject, homeWake] = [&] {
      std::lock_guard lock(durableRoutingMutex);
      auto& thread = *threads[homeId - 1];
      auto& home = *thread.home;
      home.admissionTurnDelayed = false;
      if (homeVictim && homeVictim->home == homeId && !home.reclaiming) {
        const auto canRetire = [](auto& entry) { return canRetireAtAdmission(entry.second); };
        if (state.durableRoots.size() == home.resident.size() &&
            std::all_of(state.durableRoots.begin(), state.durableRoots.end(), canRetire) &&
            fenceHome(*homeVictim, server::DurableAdmission::Clock::now())) {
          for (auto& [_, root]: state.durableRoots) root.admissionClosing = true;
        } else {
          homeVictim.reset();
        }
      }
      return std::tuple(
          databaseVictim && databaseVictim->home == homeId ? databaseVictim : std::nullopt,
          objectVictim && objectVictim->home == homeId ? objectVictim : std::nullopt,
          thread.wake);
    }();
    KJ_DEFER(if (selectedObject && !selectedObject->awaitingWal) {
      const auto root = state.durableRoots.find(selectedObject->tree);
      if (root == state.durableRoots.end() || !root->second.admissionClosing) {
        std::lock_guard lock(durableRoutingMutex);
        if (objectVictim == selectedObject) objectVictim.reset();
      }
    });
    KJ_DEFER(if (victim) {
      const auto root = state.durableRoots.find(victim->tree);
      const bool hasTree = root != state.durableRoots.end();
      std::lock_guard lock(durableRoutingMutex);
      if (databaseVictim && databaseVictim->tree == victim->tree &&
          databaseVictim->deadline == victim->deadline) {
        const auto now = server::DurableAdmission::Clock::now();
        finishDatabaseRetirement(
            victim->outcome(now, hasTree, hasTree && root->second.admissionClosing), now);
      }
    });
    if (!state.durableSlot) {
      std::lock_guard lock(durableRoutingMutex);
      auto& home = *threads[homeId - 1]->home;
      if (home.reclaiming) {
        home.reclaiming = false;
        if (homeVictim && homeVictim->home == homeId) homeVictim.reset();
      }
      return;
    }
    auto &memory = *state.durableSlot->limits->memory;
    const auto accounted = [&] { return memory.heap.load() + memory.external.load(); };
    const auto placementLimit = kj::min(durablePlacement.placementBytes, memory.maximum * 3 / 4);
    const bool pressured = accounted() >= placementLimit;
    bool reclaimed = false;
    const bool database = state.durableSlot->planRevision.starts_with(PLATFORM_DATABASE_COPY_PREFIX);
    std::map<uint64_t, server::DurableAdmission::Time> idleObjects;
    for (auto it = state.durableRoots.begin(); it != state.durableRoots.end();) {
      auto current = it++;
      using Decision = server::DurableIdleLifecycle::Decision;
      auto& root = current->second;
      auto& actor = *root.actor;
      const auto tree = current->first;
      const bool canRetire = canRetireAtAdmission(root);
      const bool forDatabaseAdmission = victim && victim->tree == tree;
      const bool forObjectAdmission = selectedObject && selectedObject->tree == tree &&
          (root.admissionClosing || canRetire);
      const bool forAdmission = forDatabaseAdmission || forObjectAdmission || root.admissionClosing;
      if (forDatabaseAdmission && !root.admissionClosing && !canRetire) continue;
      if (root.adopting) continue;
      if (root.parking != kj::none && root.parked == kj::none) continue;
      KJ_IF_SOME(parked, root.parked) {
        root.parking = kj::none;
        if (!parked) {
          root.parked = kj::none;
          root.cancelExpiredWork = false;
          root.admissionClosing = false;
          if (!root.resetting && !root.closeRequested) {
            std::lock_guard lock(durableRoutingMutex);
            KJ_ASSERT(durableAdmission->resumeIdleTree(current->first, homeId));
            root.idle->eventFinished(now);
            continue;
          }
        }
      }
      const bool explicitClose = root.resetting || root.closeRequested || root.idleClosing;
      const bool ordinarySocket = !root.sockets.empty() ||
          actor.getNonHibernatableWebSocketCount()->count > 0 || root.facetHasOrdinaryWebSocket() ||
          (root.nodeSockets && root.hasLocalSockets());
      auto decision = Decision::RETIRE;
      if (!explicitClose) {
        if (root.parked == kj::none)
          decision = root.idle->check(now, ordinarySocket,
              pressured || forAdmission || root.admissionClosing);
        else if (root.cancelExpiredWork)
          decision = Decision::CANCEL_PENDING;
      }
      if (decision == Decision::WAIT) {
        const auto idleSince = root.idle->idleSince();
        if (root.sqliteTrimmedForIdleSince != idleSince &&
            root.idle->idleFor(now, std::chrono::seconds(1))) {
          trimIdleObjectSqlite(*root.tree);
          root.sqliteTrimmedForIdleSince = idleSince;
        }
        if (!database && canRetire) idleObjects.emplace(tree, root.idle->idleSince());
        continue;
      }
      const bool cancelPending = decision == Decision::CANCEL_PENDING;
      const bool closeSocketSessions = !root.sockets.empty() && !root.resetting && !root.idleClosing;
      if (root.resetting || root.idleClosing) root.stopFacets(nullptr);
      // An ordinary socket's session may itself own a facet. Once the
      // admission table has ruled out other active events, abort these
      // sessions after parking, then wait for the facets to close.
      if (!closeSocketSessions && !cancelPending && root.facetsBusy(!ordinarySocket)) continue;
      if (!cancelPending && !ordinarySocket) {
        KJ_IF_SOME(context, actor.getIoContext()) {
          if (context.hasCurrentIncomingRequest()) continue;
        }
      }
      {
        std::lock_guard lock(durableRoutingMutex);
        if (forObjectAdmission && !root.admissionClosing) {
          if (!stopObjectAdmissionVictim(*selectedObject, server::DurableAdmission::Clock::now())) {
            idleObjects.emplace(tree, root.idle->idleSince());
            continue;
          }
        } else {
          const auto* selected = forDatabaseAdmission && !root.admissionClosing ? &*victim : nullptr;
          if (!stopDurableIdleTree(tree, homeId, closeSocketSessions, selected)) continue;
        }
        if (forAdmission) root.admissionClosing = true;
      }
      // Park the whole object's node transports before canceling I/O or
      // closing standard sockets. The abort would break the actor and make
      // its attachments inaccessible to the parking RPC.
      if (!explicitClose && root.parked == kj::none && root.nodeSockets) {
        root.cancelExpiredWork = cancelPending;
        root.parking = root.parkSockets()
                           .then([this, &state, &root, tree, forAdmission, homeWake](
                                     SocketParkResult result) {
          if (result == SocketParkResult::UNKNOWN && !root.resetting) {
            auto reason =
                KJ_EXCEPTION(DISCONNECTED, "Durable Object could not confirm WebSocket parking.");
            beginDurableReset(state, tree, reason);
            root.actor->abort(kj::mv(reason));
          }
          root.parked = result == SocketParkResult::PARKED;
          if (forAdmission) wake(homeWake);
        }, [&root, homeWake, forAdmission](kj::Exception &&exception) {
          reportDurableFailureForTest("WebSocket parking", exception);
          root.parked = false;
          if (forAdmission) wake(homeWake);
        }).eagerlyEvaluate(nullptr);
        continue;
      }
      if (closeSocketSessions) {
        std::vector<std::shared_ptr<DurableSessionScope>> sockets;
        for (const auto& [event, reference]: root.sockets) {
          if (auto socket = reference.lock()) sockets.push_back(std::move(socket));
        }
        for (const auto& session: sockets) {
          session->transport.cancel("the durable socket's object was retired");
        }
      }
      if (cancelPending || closeSocketSessions || (!explicitClose && ordinarySocket)) {
        if (!explicitClose) root.idleClosing = true;
        root.stopFacets(nullptr);
        actor.abort(KJ_EXCEPTION(DISCONNECTED, "Durable Object retired after its idle deadline."));
        continue;
      }
      const bool admissionClosing = root.admissionClosing;
      const bool waitForWal = admissionClosing && !database;
      auto closingTree = waitForWal ? root.tree : nullptr;
      if (!admissionClosing && (!explicitClose || root.idleClosing)) {
        if (pressured && !root.idleClosing) {
          durablePressureRetirements.fetch_add(1);
          reclaimed = true;
        } else {
          durableIdleRetirements.fetch_add(1);
        }
      }
      if (current->second.resetting && memory.durableProcessPressure.load()) reclaimed = true;
      if (!explicitClose || root.idleClosing || admissionClosing) {
        state.retiredActors = true;
      }
      current->second.stopFacets(nullptr);
      actor.shutdown(0);
      state.durableRoots.erase(current);
      if (waitForWal) {
        waitForAdmissionWal(state, tree, std::move(closingTree));
      } else {
        std::lock_guard lock(durableRoutingMutex);
        if (releaseDurableTree(tree, homeId, admissionClosing))
          state.durableSlot->lastUseNs.store(nowNs());
      }
    }
    releaseClosedAdmissionTrees(state);
    const bool databaseCanRetire = database && state.durableRoots.size() == 1 &&
        canRetireAtAdmission(state.durableRoots.begin()->second);
    const bool closingForAdmission =
        std::ranges::any_of(state.durableRoots, [](const auto& root) {
      return root.second.admissionClosing;
    }) || !state.admissionWalClosing.empty();
    using Action = server::DurablePressure::Action;
    const auto samplePressure = [&](auto &home, bool onlyIfRelieved) {
      home.heapBytes = memory.heap.load();
      home.memory = home.heapBytes + memory.external.load();
      const bool rejectedGrowth =
          memory.cause->load() == LimitCause::MEMORY || memory.durableProcessPressure.load();
      const bool abovePlacement = home.memory >= placementLimit || rejectedGrowth;
      if (onlyIfRelieved && abovePlacement) return Action::WAIT;
      return home.pressure.sample(abovePlacement,
          home.memory >= memory.maximum - memory.maximum / 10 || rejectedGrowth, reclaimed);
    };
    Action pressureAction = Action::WAIT;
    bool releaseCopy;
    bool drained;
    {
      std::lock_guard lock(durableRoutingMutex);
      auto &home = *threads[homeId - 1]->home;
      home.databaseCanRetire = databaseCanRetire;
      home.idleObjects = std::move(idleObjects);
      for (const auto& [_, lastUse]: home.idleObjects) home.lastUse = kj::max(home.lastUse, lastUse);
      if (home.reclaiming && home.reservedTrees != 0 && !closingForAdmission) {
        // A failed socket handoff resumed its tree. That home can serve again.
        home.reclaiming = false;
        if (homeVictim && homeVictim->home == homeId) homeVictim.reset();
      }
      // A pressure reclamation can reopen placement or request another eviction. It cannot
      // start a reset, so a retained copy can be sampled after its collection.
      if (!reclaimed) pressureAction = samplePressure(home, false);
      const auto idleMs = configuredCount("COWORKERD_THREADED_IDLE_EVICT_MS", 300000);
      const auto lastUse = state.durableSlot->lastUseNs.load();
      // An empty database copy still charges the process budget. Release it when its
      // tree retires so a new database can open on an unused home.
      releaseCopy = shouldReleaseDurableCopy(home.reservedTrees, !state.durableRoots.empty(),
          state.durableSlot->planRevision,
          home.pressure.resetting() || (home.reclaiming && !closingForAdmission) ||
              (idleMs && nowNs() - lastUse >= idleMs * 1000000));
      // Reserved trees include starts still queued or preparing. The home owns the event loop,
      // so no new JavaScript can enter before this maintenance turn ends.
      drained = state.retiredActors && state.durableRoots.empty() &&
          state.durableActorTrees.empty() && state.admissionWalClosing.empty() &&
          home.reservedTrees == 0;
    }
    if (!releaseCopy && (reclaimed || drained)) {
      // Actor teardown outside the isolate lock defers JSG handle destruction.
      // Take the Worker lock to drop those roots before collecting their memory.
      auto &worker = *KJ_REQUIRE_NONNULL(state.durableSlot->worker);
      worker.runInLockScope(Worker::Lock::TakeSynchronously(kj::none), [&](Worker::Lock &lock) {
        lock.getIsolate()->LowMemoryNotification();
        memory.observe(true);
      });
      std::lock_guard lock(durableRoutingMutex);
      // Idle collection can reopen placement after relieving pressure. A still-pressured copy
      // keeps the decision already made for this turn.
      pressureAction = samplePressure(*threads[homeId - 1]->home, !reclaimed);
    }
    if (pressureAction == server::DurablePressure::Action::RESET) {
      for (auto &[tree, root]: state.durableRoots) {
        auto reason = limitException(LimitCause::MEMORY);
        beginDurableReset(state, tree, reason);
        root.actor->abort(reason);
      }
    }
    if (releaseCopy) {
      if (victim && !state.durableRoots.contains(victim->tree)) {
        if (const auto* directory = durableLifecycleTestDirectory()) {
          const auto path = kj::str(directory, "/hold-database-reservation");
          const auto fd = open(path.cStr(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
          if (fd >= 0) {
            close(fd);
            // Exercise the deadline while a real reservation outlives the copy.
            state.tasks.add(state.io.provider->getTimer().afterDelay(10 * kj::SECONDS).then(
                [hold = state.durableSlot->reservation]() mutable { hold.reset(); }));
          }
        }
      }
      state.durableSlot.reset();
      std::lock_guard lock(durableRoutingMutex);
      auto &home = *threads[homeId - 1]->home;
      home.memory = home.heapBytes = home.copyInstance = 0;
      home.warmDefinition.clear();
      home.databaseReservation.reset();
      if (home.reservedTrees == 0) home.definition.clear();
      if (home.reclaiming) {
        home.reclaiming = false;
        ++homeRetirements;
        if (homeVictim && homeVictim->home == homeId) homeVictim.reset();
      }
      if (home.pressure.resetting()) durableIsolateResets.fetch_add(1);
      home.pressure.closed();
    }
    if (drained || releaseCopy) {
      state.retiredActors = false;
      // Return the freed allocations in this thread's cache before returning process pages.
      tcmalloc::MallocExtension::MarkThreadIdle();
      requestDurableMemoryRelease();
    }
  }

  void trackDurableTurn(
      uint64_t homeId, bool entering, uint64_t heapBytes = 0, uint64_t externalBytes = 0) {
    std::lock_guard lock(durableRoutingMutex);
    auto &home = *threads[homeId - 1]->home;
    const auto now = server::DurableHomeLoad::Clock::now();
    if (entering) {
      ++home.activity;
      home.load.begin(now);
    } else {
      home.load.end(now);
      home.lastUse = kj::max(home.lastUse, now);
      home.heapBytes = heapBytes;
      home.memory = heapBytes + externalBytes;
    }
  }

  void reportDurable(bool publish, std::string *appendTo = nullptr) {
    std::lock_guard lock(durableRoutingMutex);
    const auto state = durableAdmission->state();
    size_t resident = 0, homes = 0, placementClosedHomes = 0, resettingHomes = 0;
    uint64_t maxHomeMemory = 0;
    std::string copies;
    if (ready.load(std::memory_order_acquire)) {
      for (const auto &thread: threads) {
        const auto &home = *thread->home;
        homes += !home.definition.empty();
        placementClosedHomes += !home.pressure.accepting();
        resettingHomes += home.pressure.resetting();
        maxHomeMemory = kj::max(maxHomeMemory, home.memory);
        resident += home.resident.size();
        if (home.copyInstance != 0) {
          if (!copies.empty()) copies += ',';
          copies += "{\"home\":" + std::to_string(thread->homeId) +
              ",\"instance\":" + std::to_string(home.copyInstance) +
              ",\"residentRoots\":" + std::to_string(home.resident.size()) +
              ",\"heapBytes\":" + std::to_string(home.heapBytes) +
              ",\"externalBytes\":" + std::to_string(home.memory - home.heapBytes) +
              ",\"resetting\":" + (home.pressure.resetting() ? "true" : "false") + "}";
        }
      }
    }
    const auto homeDescriptors = uint64_t(liveThreads.load()) * HOME_RESERVATION.descriptors;
    auto row = ",\"durableAdmission\":{\"trees\":" + std::to_string(state.trees) +
        ",\"residentRoots\":" + std::to_string(resident) + ",\"homes\":" + std::to_string(homes) +
        ",\"placementClosedHomes\":" + std::to_string(placementClosedHomes) +
        ",\"resettingHomes\":" + std::to_string(resettingHomes) +
        ",\"maxHomeMemoryBytes\":" + std::to_string(maxHomeMemory) +
        ",\"copies\":[" + copies + "]" +
        ",\"sqlSpillBytes\":" + std::to_string(durableStorage->getSqlSpillBudget().getUsed()) +
        ",\"sqlSpillMaximum\":" +
        std::to_string(durableStorage->getSqlSpillBudget().getCapacity()) +
        ",\"workerThreads\":" + std::to_string(liveThreads.load()) +
        ",\"maxHomes\":" + std::to_string(maxDurableHomes) +
        ",\"homeThreadBudget\":" + std::to_string(limit) +
        ",\"descriptorBudget\":" + std::to_string(durableDescriptorBudget) +
        ",\"homeReservedDescriptors\":" + std::to_string(homeDescriptors) +
        ",\"homeReservedBytes\":" + std::to_string(uint64_t(liveThreads.load()) * HOME_NATIVE_BYTES) +
        ",\"homeRetirements\":" + std::to_string(homeRetirements) +
        ",\"homeRefusals\":" + std::to_string(homeRefusals) +
        ",\"lastHomeRefusal\":" +
        (lastHomeRefusal == nullptr ? std::string("null") : "\"" + std::string(lastHomeRefusal) + "\"") +
        ",\"waiting\":" + std::to_string(state.waiting) +
        ",\"running\":" + std::to_string(state.active) +
        ",\"waitingBytes\":" + std::to_string(state.waitingBytes) +
        ",\"homeSessions\":" + std::to_string(durableHomeSessions.load()) +
        ",\"pendingCommits\":" + std::to_string(durablePendingCommits.load()) +
        ",\"unconfirmedCloseIdentities\":" +
        std::to_string(durableLog ? durableLog->unconfirmedCloseIdentities() : 0) +
        ",\"groupCreationRefusals\":" + std::to_string(sandboxRefusals.load()) +
        ",\"idleRetirements\":" + std::to_string(durableIdleRetirements.load()) +
        ",\"pressureRetirements\":" + std::to_string(durablePressureRetirements.load()) +
        ",\"admissionRetirements\":" + std::to_string(durableAdmissionRetirements.load()) +
        ",\"admissionWalTimeouts\":" + std::to_string(durableAdmissionWalTimeouts.load()) +
        ",\"actorResets\":" + std::to_string(durableActorResets.load()) +
        ",\"treeResets\":" + std::to_string(durableTreeResets.load()) +
        ",\"isolateResets\":" + std::to_string(durableIsolateResets.load()) +
        ",\"reservedDescriptors\":" + std::to_string(state.reserved.descriptors - homeDescriptors) +
        ",\"reservedBytes\":" + std::to_string(state.reserved.memory) +
        ",\"sqliteHeapReservedBytes\":" + std::to_string(resources->sqliteMemoryReserved.load()) +
        ",\"objectSqliteConnections\":" + std::to_string(objectSqliteConnections.load()) +
        ",\"copyMemoryReservedBytes\":" + std::to_string(resources->memoryReserved.load()) +
        ",\"copyMemoryMaximum\":" + std::to_string(resources->memoryMaximum) +
        ",\"copyMemoryAdmissionMaximum\":" + std::to_string(resources->admissionMaximum) +
        ",\"copyAddressReservedBytes\":" + std::to_string(resources->addressReserved.load()) +
        ",\"copyAddressMaximum\":" + std::to_string(resources->addressMaximum) +
        ",\"copyResidentMaximum\":" + std::to_string(resources->residentMaximum.load()) +
        ",\"overloaded\":" + std::to_string(state.overloads) +
        ",\"expired\":" + std::to_string(state.expired) + "}}";
    if (publish) {
      const std::string role = durableLog ? "threaded" : "threaded-test";
      lifecycle.publish("{\"durableServer\":\"" + role + "\"" + row, state.active,
          durableHomeSessions.load(), state.waiting);
    }
    if (appendTo) {
      appendTo->pop_back();
      *appendTo += row;
    }
  }

  // The router owns the growing home vector. A reserved loop can be reused
  // for another definition after its actor and isolate have left that loop.
  std::optional<uint64_t> startDurableHome(const char*& refusal) {
    if (threads.size() >= limit) {
      refusal = "home-threads";
      return std::nullopt;
    }
    if (threads.size() >= maxDurableHomes) {
      refusal = "home-ceiling";
      return std::nullopt;
    }
    if (!durableAdmission->reserveAuxiliary(HOME_RESERVATION)) {
      refusal = "home-descriptors";
      return std::nullopt;
    }
    if (!resources->change(0, HOME_NATIVE_BYTES, 0, 0, true)) {
      durableAdmission->releaseAuxiliary(HOME_RESERVATION);
      refusal = "home-memory";
      return std::nullopt;
    }
    bool started = false;
    KJ_DEFER(if (!started) {
      durableAdmission->releaseAuxiliary(HOME_RESERVATION);
      (void)resources->change(HOME_NATIVE_BYTES, 0, 0, 0);
      growthFailures.fetch_add(1);
      refusal = "home-loop-start";
    });
    try {
      auto item = std::make_unique<Thread>(false, false);
      kj::AutoCloseFd wakeFd(item->wake);
      item->home.emplace();
      item->homeId = threads.size() + 1;
      auto* pointer = item.get();
      threads.push_back(std::move(item));
      KJ_DEFER(if (!started) threads.pop_back());
      std::promise<bool> startup;
      auto initialized = startup.get_future();
      pointer->thread = std::thread([this, pointer, startup = std::move(startup)]() mutable {
        runThread(*pointer, &startup);
      });
      if (!initialized.get()) {
        pointer->thread.join();
        return std::nullopt;
      }
      started = true;
      (void)wakeFd.release();
      liveThreads.fetch_add(1);
      return pointer->homeId;
    } catch (...) {
      return std::nullopt;
    }
  }

  void manage() {
    pthread_setname_np(pthread_self(), "cwkd-pool");
    if (durableAdmission) {
      ready.store(true, std::memory_order_release);
      return manageDurable();
    }
    // The initial high watermark is the in-flight limit. Readiness waits for
    // all native loops, so a cold burst never makes the accept thread
    // initialize one.
    try {
      threads.reserve(limit + CONTROL_THREADS);
      for (uint i = 0; i < limit + CONTROL_THREADS; ++i) {
        auto item = std::make_unique<Thread>(i >= limit, i == limit + CONTROL_THREADS - 1);
        auto *pointer = item.get();
        liveThreads.fetch_add(1);
        item->thread = std::thread([this, pointer]() { runThread(*pointer); });
        threads.push_back(std::move(item));
      }
    } catch (...) {
      fprintf(stderr,
          "[threaded-start] "
          "{\"event\":\"startup-rejected\",\"resource\":\"call-"
          "thread-pool\"}\n");
      _exit(70);
    }
    for (auto &item: threads)
      while (item->descriptor.load(std::memory_order_acquire) != -1) usleep(1000);
    ready.store(true, std::memory_order_release);
    int held = -1;
    auto nextMaintenance = nowNs();
    auto idleMs = configuredCount("COWORKERD_THREADED_THREAD_IDLE_MS", 60000);
    for (;;) {
      for (auto &item: threads) {
        if (item->descriptor.load() == STOPPED && item->thread.joinable()) item->thread.join();
      }
      auto now = nowNs();
      if (now >= nextMaintenance) {
        nextMaintenance = now + 100000000;
        for (auto &item: threads) {
          if (!item->maintenanceOnly) continue;
          int idle = -1;
          if (item->descriptor.compare_exchange_strong(idle, MAINTAIN)) {
            wake(item->wake);
            break;
          }
        }
        auto remaining = liveThreads.load();
        if (idleMs != 0)
          for (auto &item: threads) {
            if (item->control || remaining <= std::min(limit, 32u) + CONTROL_THREADS ||
                now - kj::min(now, item->idleSince.load()) < idleMs * 1000000)
              continue;
            int idle = -1;
            if (item->descriptor.compare_exchange_strong(idle, STOP)) {
              --remaining;
              wake(item->wake);
            }
          }
      }
      if (held < 0) held = pending.pop();
      if (held >= 0) {
        bool assigned = false;
        for (auto &item: threads) {
          if (item->control) continue;
          int idle = -1;
          if (item->descriptor.compare_exchange_strong(idle, held)) {
            item->invocation.store(true);
            wake(item->wake);
            held = -1;
            assigned = true;
            break;
          }
        }
        if (assigned) continue;
        uint growing = 0;
        for (auto &item: threads) {
          if (item->control || item->descriptor.load() != STOPPED || growing == 16) continue;
          if (item->thread.joinable()) item->thread.join();
          item->descriptor.store(-2);
          liveThreads.fetch_add(1);
          try {
            item->thread = std::thread([this, pointer = item.get()] { runThread(*pointer); });
            ++growing;
          } catch (...) {
            liveThreads.fetch_sub(1);
            item->descriptor.store(STOPPED);
            growthFailures.fetch_add(1);
          }
        }
        // Dedicated control loops can decode a descriptor and confirm a refusal
        // even while every execution loop owns an invocation's background work.
        const bool warming = std::any_of(threads.begin(), threads.end(), [](const auto &item) {
          const auto descriptor = item->descriptor.load();
          return !item->control && (descriptor == -2 || descriptor == STOP);
        });
        if (growing == 0 && !warming)
          for (auto &item: threads) {
            if (!item->control || item->maintenanceOnly) continue;
            int idle = -1;
            if (item->descriptor.compare_exchange_strong(idle, held)) {
              item->invocation.store(true);
              wake(item->wake);
              held = -1;
              assigned = true;
              break;
            }
          }
        if (assigned) continue;
      }
      pollfd wait{managerWake, POLLIN, 0};
      int result;
      do {
        result = poll(&wait, 1, held >= 0 ? 1 : 100);
      } while (result < 0 && errno == EINTR);
      if (result < 0) {
        growthFailures.fetch_add(1);
        usleep(1000);
        continue;
      }
      uint64_t count;
      while (read(managerWake, &count, sizeof(count)) > 0) {}
    }
  }
  void runThread(Thread &thread, std::promise<bool>* startup = nullptr) {
    pthread_setname_np(pthread_self(), thread.home ? "cwkd-home" :
        thread.control ? "cwkd-refuse" : "cwkd-call");
    std::unique_ptr<ThreadState> storage;
    kj::Own<kj::AsyncInputStream> signal;
    try {
      storage = std::make_unique<ThreadState>();
      auto& state = *storage;
      state.rejectOnly = thread.control;
      state.durableHome = thread.homeId;
      if (thread.home) {
        KJ_REQUIRE(state.decisionSpare.get() >= 0, "could not reserve the home decision descriptor");
        signal = state.io.lowLevelProvider->wrapInputFd(thread.wake);
        attachDurableHomeWal(thread, state);
      }
    } catch (...) {
      if (startup != nullptr) {
        signal = nullptr;
        storage.reset();
        startup->set_value(false);
        return;
      }
      fprintf(stderr,
          "[threaded-start] "
          "{\"event\":\"startup-rejected\",\"resource\":\"call-"
          "event-loop\"}\n");
      if (!ready.load()) _exit(70);
      growthFailures.fetch_add(1);
      liveThreads.fetch_sub(1);
      thread.descriptor.store(STOPPED);
      wake(managerWake);
      return;
    }
    auto &state = *storage;
    thread.idleSince.store(nowNs());
    thread.descriptor.store(-1, std::memory_order_release);
    if (startup != nullptr) startup->set_value(true);
    else wake(managerWake);
    if (thread.home) {
      // The mailbox wait pumps this home's event loop and its idle maintenance.
      for (;;) {
        kj::byte notifications[sizeof(uint64_t)];
        signal->read(kj::arrayPtr(notifications))
            .exclusiveJoin(state.io.provider->getTimer().afterDelay(250 * kj::MILLISECONDS))
            .wait(state.io.waitScope);
        std::vector<DurableIncoming> incoming;
        {
          std::lock_guard lock(durableRoutingMutex);
          incoming.swap(thread.home->incoming);
        }
        for (auto &call: incoming) {
          state.tasks.add(startDurableSession(state, std::move(call)));
        }
        retireIdleDurableRoots(state);
      }
    }
    runOrdinaryThread(thread, state);
    storage.reset();
    liveThreads.fetch_sub(1);
    thread.descriptor.store(STOPPED);
    wake(managerWake);
  }

  void attachDurableHomeWal(Thread& thread, ThreadState& state) {
    if (durableLog) {
      state.wal = durableLog->attachHome(
          [this, &state](uint64_t openId, server::ThreadedWalLog::CloseReason why) {
        for (auto& [id, root]: state.durableRoots) {
          if (!root.rootOpenId || root.rootOpenId() != openId) continue;
          root.closeRequested = true;
          if (why == server::ThreadedWalLog::CloseReason::CODE_UPDATED) {
            auto reason = jsg::tagCoworkerdRpcError(
                KJ_EXCEPTION(DISCONNECTED,
                    "broken.codeUpdated; jsg.Error: Durable Object reset because its code was updated"),
                "DO_CODE_UPDATED"_kj);
            beginDurableReset(state, id, reason);
            root.stopFacets(&reason);
            root.actor->abort(kj::mv(reason));
          }
          break;
        }
      }, thread.home->walFull);
    }
  }

  void runOrdinaryThread(Thread& thread, ThreadState& state) {
    for (;;) {
      uint64_t count;
      ssize_t n;
      do {
        n = read(thread.wake, &count, sizeof(count));
      } while (n < 0 && errno == EINTR);
      KJ_REQUIRE(n == sizeof(count));
      auto fd = thread.descriptor.exchange(-2);
      if (fd == STOP) break;
      if (fd == MAINTAIN) {
        auto ignored = kj::runCatchingExceptions([&] { maintain(); });
        thread.descriptor.store(-1, std::memory_order_release);
        wake(managerWake);
        continue;
      }
      KJ_REQUIRE(fd >= 0);
      // The idle mailbox wait is outside KJ. Refresh its cached I/O time before
      // an already-buffered RPC can enter JavaScript without another OS poll.
      state.io.unixEventPort.poll();
      auto error = kj::runCatchingExceptions([&]() {
        auto stream = state.io.lowLevelProvider->wrapSocketFd(
            fd, kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP);
        capnp::TwoPartyServer session(kj::heap<Bootstrap>(*this, state));
        session.accept(*stream).wait(state.io.waitScope);
      });
      KJ_IF_SOME(exception, error) {
        // Never print the descriptor, variables, secret values, or request
        // body.
        if (exception.getType() != kj::Exception::Type::DISCONNECTED)
          KJ_LOG(INFO, "threaded invocation connection failed");
      }
      auto backgroundError = kj::runCatchingExceptions([&]() {
        settleBackground(state.io.provider->getTimer(), state.tasks).wait(state.io.waitScope);
      });
      if (backgroundError != kj::none) state.tasks.clear();
      thread.invocation.store(false);
      thread.idleSince.store(nowNs());
      thread.descriptor.store(-1, std::memory_order_release);
      wake(managerWake);
    }
  }

  kj::Promise<kj::String> fetchSource(Json::Reader descriptor, ThreadState &state, Slot &slot) {
    if (const char *delay = getenv("COWORKERD_THREADED_SOURCE_DELAY_FOR_TEST")) {
      const auto milliseconds = strtoul(delay, nullptr, 10);
      KJ_REQUIRE(milliseconds <= 10000, "invalid source test delay");
      if (!delayedSourceForTest.exchange(true))
        co_await state.io.provider->getTimer().afterDelay(milliseconds * kj::MILLISECONDS);
    }
    auto invocation = text(descriptor, "invocationId");
    auto forInvocation = field(descriptor, "sourceInvocationId");
    if (forInvocation.isString() && forInvocation.getString().size() > 0)
      invocation = kj::str(forInvocation.getString());
    auto url = kj::str("http://coworkerd-source/v1/source?workerId=",
        kj::encodeUriComponent(text(descriptor, "workerId")),
        "&versionId=", kj::encodeUriComponent(text(descriptor, "versionId")),
        "&contentHash=", text(descriptor, "contentHash"),
        "&definitionHash=", text(descriptor, "definitionHash"),
        "&invocationId=", kj::encodeUriComponent(invocation),
        "&processId=", kj::encodeUriComponent(text(descriptor, "processId")), "&processGeneration=",
        field(descriptor, "processGeneration").isString()
            ? kj::str(field(descriptor, "processGeneration").getString())
            : kj::str(field(descriptor, "processGeneration").getNumber()));
    auto address = co_await state.io.provider->getNetwork().parseAddress(sourceAddress.c_str());
    auto client = kj::newHttpClient(state.io.provider->getTimer(), *state.headers, *address);
    kj::HttpHeaders headers(*state.headers);
    auto request = client->request(kj::HttpMethod::GET, url, headers);
    auto response = co_await request.response;
    if (response.statusCode != 200) {
      kj::throwRecoverableException(kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__,
          kj::str("Trusted source fetch failed: ", response.statusCode)));
    }
    auto length =
        KJ_REQUIRE_NONNULL(response.body->tryGetLength(), "Trusted source omitted its length");
    while (!slot.reservation->retainSource(length)) {
      if (state.durableHome != 0 || !resources->reclaim(length * 5)) {
        if (slot.reservation->retainSource(length)) break;
        kj::throwRecoverableException(capacityError());
      }
    }
    auto content = co_await response.body->readAllText(256u << 20);
    slot.loadStage = threaded::LoadStage::DECODE;
    std::unique_ptr<JsonDoc> source;
    try {
      source = std::make_unique<JsonDoc>(content);
    } catch (kj::Exception &error) {
      auto group = KJ_UNWRAP_OR(server::tryNewIsolateGroup(), {
        noSandboxRoom(*resources);
        kj::throwFatalException(capacityError());
      });
      IsolateLimits limits(workerLimit(descriptor, "cpuLimitMs", "COWORKERD_CPU_LIMIT_MS"),
          workerLimit(descriptor, "memoryLimitMiB", "COWORKERD_MEMORY_LIMIT_MIB"),
          invocationIdentity(slot, descriptor), slot.reservation, kj::mv(group));
      limits.sandboxName = slot.isolateLabel + "/source-json";
      limits.durableEvents = bool(slot.resetDurableRoot);
      isolateCreationAttempts.fetch_add(1);
      KJ_IF_SOME(failure, sourceJsonFailure(content, limits)) {
        kj::throwRecoverableException(
            kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__, kj::mv(failure)));
      }
      kj::throwRecoverableException(kj::mv(error));
    }
    slot.loadStage = threaded::LoadStage::PACKAGE;
    KJ_REQUIRE(text(source->get(), "workerId") == text(descriptor, "workerId") &&
            text(source->get(), "versionId") == text(descriptor, "versionId") &&
            text(source->get(), "contentHash") == text(descriptor, "contentHash"),
        "Trusted source identity mismatch");
    co_return text(source->get(), "source");
  }

  kj::Own<Worker> compile(Slot &slot, Json::Reader descriptor, kj::StringPtr source) {
    slot.loadStage = threaded::LoadStage::PACKAGE;
    Reporter reporter;
    // A durable package reaches this compiler after DurableBootstrap has checked the trusted root plan.
    const bool productPlatform = slot.resetDurableRoot && productPlatformPlan(field(descriptor, "rootPlan"));
    auto flags = compileTenantFlags(slot, descriptor, reporter, productPlatform);
    auto config = slot.configArena.initRoot<server::config::Worker>();
    auto main = text(descriptor, "mainModule");
    if (field(descriptor, "format").isString() && text(descriptor, "format") == "v2") {
      slot.loadStage = threaded::LoadStage::PACKAGE;
      JsonDoc payload(source);
      auto manifestText = text(payload.get(), "manifest");
      KJ_REQUIRE(digest(manifestText.asBytes()) == str(descriptor, "contentHash"),
          "Trusted package manifest hash mismatch");
      JsonDoc manifest(manifestText);
      auto declarations = field(manifest.get(), "modules");
      KJ_REQUIRE(declarations.isArray(), "invalid module manifest");
      auto modules = config.initModules(declarations.getArray().size());
      uint next = 1;
      bool foundMain = false;
      for (auto declaration: declarations.getArray()) {
        auto name = text(declaration, "name");
        uint index = name == main ? 0 : next++;
        if (name == main) foundMain = true;
        KJ_REQUIRE(index < modules.size(), "missing main module");
        auto hash = text(declaration, "sha256");
        auto encoded = field(field(payload.get(), "modules"), hash);
        KJ_REQUIRE(encoded.isString(), "missing module bytes");
        auto bytes = decode64(encoded.getString());
        auto view = kj::arrayPtr(reinterpret_cast<const kj::byte *>(bytes.data()), bytes.size());
        KJ_REQUIRE(
            digest(view) == hash.cStr() && bytes.size() == field(declaration, "bytes").getNumber(),
            "Trusted module content hash mismatch");
        auto module = modules[index];
        module.setName(name);
        auto type = text(declaration, "type");
        auto content = kj::StringPtr(bytes.data(), bytes.size());
        if (type == "esm")
          module.setEsModule(content);
        else if (type == "cjs")
          module.setCommonJsModule(content);
        else if (type == "text")
          module.setText(content);
        else if (type == "json")
          module.setJson(content);
        else if (type == "data")
          module.setData(view);
        else if (type == "wasm")
          module.setWasm(view);
        else
          KJ_FAIL_REQUIRE("NOT_SUPPORTED: module type");
      }
      KJ_REQUIRE(foundMain, "missing main module");
    } else {
      KJ_REQUIRE(digest(source.asBytes()) == str(descriptor, "contentHash"),
          "Trusted source content hash mismatch");
      auto module = config.initModules(1)[0];
      module.setName(main);
      module.setEsModule(source);
    }
    auto identity = kj::str("\"workerId\":", encode(field(descriptor, "workerId")),
        ",\"versionId\":", encode(field(descriptor, "versionId")),
        ",\"invocationId\":", encode(field(descriptor, "invocationId")), ",\"isolate\":\"",
        slot.isolateLabel.c_str(), "\",");
    // With a V8 sandbox per isolate, an address space without room for another one refuses this
    // Worker (the node's per-process limit normally refuses first), never ends the process.
    // Inject the factory's empty result once, only for disposable durable storage, so the
    // real refusal cleanup/retry path can be exercised without exhausting host address space.
    auto group = KJ_UNWRAP_OR(slot.resetDurableRoot && refuseDurableGroupForTest.exchange(false)
            ? kj::Maybe<v8::IsolateGroup>(kj::none)
            : server::tryNewIsolateGroup(),
        {
          noSandboxRoom(*resources);
          kj::throwFatalException(capacityError());
        });
    auto limits = kj::refcounted<IsolateLimits>(
        workerLimit(descriptor, "cpuLimitMs", "COWORKERD_CPU_LIMIT_MS"),
        workerLimit(descriptor, "memoryLimitMiB", "COWORKERD_MEMORY_LIMIT_MIB"), kj::mv(identity),
        slot.reservation, group);
    limits->sandboxName = slot.isolateLabel;
    limits->durableEvents = bool(slot.resetDurableRoot);
    slot.limits = limits.get();
    slot.limitsOwner = kj::addRef(*limits);
    OomReportScope oom(
        limits->identity, limits->memoryMiB, limits->memory ? &limits->memory->heap : nullptr);
    isolateCreationAttempts.fetch_add(1);
    const api::pyodide::PythonConfig python{
      .packageDiskCacheRoot = kj::none, .createSnapshot = false, .createBaselineSnapshot = false};
    kj::Own<JsgIsolateObserver> jsgObserver;
    KJ_IF_SOME(busy, slot.busy) {
      jsgObserver = kj::atomicAddRef(*busy);
    } else {
      jsgObserver = kj::atomicRefcounted<JsgIsolateObserver>();
    }
    auto api = kj::heap<server::WorkerdApi>(v8, flags.asReader(),
        capnp::List<server::config::Extension>::Reader(), limits->getCreateParams(), group,
        kj::mv(jsgObserver), slot.cache, python);
    auto name = kj::str(text(descriptor, "workerId"), "@", text(descriptor, "versionId"));
    // Declared before the isolate so that a load that fails tears its Worker down in this scope.
    kj::Maybe<FatalInvocation> fatal;
    auto isolate =
        kj::atomicRefcounted<Worker::Isolate>(kj::mv(api), kj::atomicRefcounted<IsolateObserver>(),
            name, kj::mv(limits), Worker::Isolate::InspectorPolicy::DISALLOW,
            Worker::LoggingOptions(Worker::ConsoleMode::STDOUT));
    fatal.emplace(slot.limits->durableEvents ? nullptr : slot.limits->isolate, slot.limits->identity);
    slot.loadStage = threaded::LoadStage::EVALUATION;
    auto extracted = server::WorkerdApi::extractSource(name, config, flags.asReader(), reporter);
    reporter.requireValid();
    auto compiled = kj::atomicRefcounted<Worker::Script>(kj::mv(isolate), name, extracted,
        IsolateObserver::StartType::COLD, reporter, kj::none, SpanParent(nullptr),
        newWorkerFileSystem(kj::heap<FsMap>(), getTmpDirectoryImpl()), kj::none);
    reporter.requireValid();
    kj::Maybe<jsg::V8Ref<v8::Object>> exportsHandle;
    slot.loadStage = threaded::LoadStage::EVALUATION;
    auto worker =
        kj::atomicRefcounted<Worker>(kj::mv(compiled), kj::atomicRefcounted<WorkerObserver>(),
            [&](jsg::Lock &lock, const Worker::Api &api, v8::Local<v8::Object> target,
                v8::Local<v8::Object> ctxExports) {
      exportsHandle = lock.v8Ref(ctxExports);
      auto context = lock.v8Context();
      for (auto section: {"variables", "secrets"}) {
        auto values = field(descriptor, section);
        if (!values.isArray()) continue;
        for (auto variable: values.getArray()) {
          auto value = field(variable, "value");
          KJ_REQUIRE(value.isString(), "missing environment value");
          v8::Local<v8::Value> item = lock.str(value.getString());
          if (field(variable, "type").isString() && text(variable, "type") == "json") {
            slot.loadStage = threaded::LoadStage::DECODE;
            item = jsg::check(v8::JSON::Parse(context, lock.str(value.getString())));
            slot.loadStage = threaded::LoadStage::EVALUATION;
          }
          KJ_REQUIRE(
              jsg::check(target->DefineOwnProperty(context,
                  static_cast<v8::Local<v8::String>>(lock.str(text(variable, "name"))), item)),
              "invalid environment property");
        }
      }
      auto services = field(field(descriptor, "rootPlan"), "serviceBindings");
      auto objects = field(field(descriptor, "rootPlan"), "durableObjects");
      uint serviceCount = services.isArray() ? services.getArray().size() : 0;
      uint objectCount = objects.isArray() ? objects.getArray().size() : 0;
      capnp::MallocMessageBuilder globalsArena;
      auto globals = globalsArena.initRoot<capnp::List<server::Global>>(serviceCount + objectCount);
      for (uint i = 0; i < serviceCount; ++i) {
        auto global = globals[i];
        global.setName(text(field(services.getArray()[i], "metadata"), "name"));
        global.setFetcher(IoContext::SPECIAL_SUBREQUEST_CHANNEL_COUNT + i);
      }
      for (uint i = 0; i < objectCount; ++i) {
        auto metadata = field(objects.getArray()[i], "metadata");
        auto name = text(metadata, "binding");
        auto global = globals[serviceCount + i];
        global.setName(name);
        auto space = slot.resetDurableRoot
            ? metadata
            : field(field(field(descriptor, "rootSelection"), "durableObjectBindings"), name);
        auto database = field(metadata, "database");
        if (database.isString() && database.getString().size() != 0) {
          auto wrapped = global.initWrapped();
          wrapped.setModuleName("cloudflare-internal:d1-api");
          wrapped.setEntrypoint("default");
          auto fetcher = wrapped.initInnerBindings(1)[0];
          fetcher.setName("fetcher");
          fetcher.setFetcher(IoContext::SPECIAL_SUBREQUEST_CHANNEL_COUNT + serviceCount + i);
        } else {
          auto ns = global.initDurableActorNamespace();
          ns.setActorChannel(i);
          ns.setUniqueKey(text(space, "namespaceId"));
        }
      }
      static_cast<const server::WorkerdApi &>(api).compileGlobals(
          lock, globals.asReader(), target, [](kj::StringPtr key) -> kj::Own<ActorIdFactory> {
        return kj::heap<ObjectIdFactory>(key);
      });
    },
            IsolateObserver::StartType::COLD, SpanParent(nullptr),
            Worker::Lock::TakeSynchronously(kj::none), reporter);
    reporter.requireValid();
    uintptr_t isolateAddress = 0;
    worker->runInLockScope(Worker::Lock::TakeSynchronously(kj::none), [&](Worker::Lock &lock) {
#ifdef COWORKERD_FATAL_TEST_BUILD
      if (!slot.resetDurableRoot) injectSyntheticCrash(slot.workerId.c_str(), true);
#endif
      lock.validateHandlers(reporter);
      slot.loopbacks.assign(reporter.entrypoints.begin(), reporter.entrypoints.end());
      auto namespaces = field(descriptor, "nativeNamespaces");
      if (namespaces.isArray())
        for (auto value: namespaces.getArray()) {
          auto name = str(value, "className");
          if (reporter.actorClasses.contains(name)) slot.loopbackActors.push_back(std::move(name));
        }
      if (slot.resetDurableRoot) {
        for (const auto &name: reporter.actorClasses) {
          if (std::find(slot.loopbackActors.begin(), slot.loopbackActors.end(), name) ==
              slot.loopbackActors.end())
            slot.loopbackActors.push_back(name);
        }
      }
      uint base = IoContext::SPECIAL_SUBREQUEST_CHANNEL_COUNT;
      auto plan = field(descriptor, "rootPlan");
      for (auto name: {"serviceBindings", "durableObjects"}) {
        auto values = field(plan, name);
        if (values.isArray()) base += values.getArray().size();
      }
      JSG_WITHIN_CONTEXT_SCOPE(lock, lock.getContext(), [&](jsg::Lock &js) {
        capnp::MallocMessageBuilder arena;
        auto globals = arena.initRoot<capnp::List<server::Global>>(
            slot.loopbacks.size() + slot.loopbackActors.size());
        for (uint i = 0; i < slot.loopbacks.size(); ++i) {
          globals[i].setName(slot.loopbacks[i].c_str());
          globals[i].setLoopbackServiceStub(base + i);
        }
        auto objects = field(field(descriptor, "rootPlan"), "durableObjects");
        uint objectCount = objects.isArray() ? objects.getArray().size() : 0;
        for (uint i = 0; i < slot.loopbackActors.size(); ++i) {
          auto &name = slot.loopbackActors[i];
          auto global = globals[slot.loopbacks.size() + i];
          global.setName(name.c_str());
          auto ns = global.initLoopbackDurableActorNamespace();
          ns.setActorChannel(objectCount + i);
          ns.setClassChannel(i);
          if (namespaces.isArray()) {
            for (auto value: namespaces.getArray()) {
              if (text(value, "className") == name.c_str()) {
                ns.setUniqueKey(text(value, "namespaceId"));
              }
            }
          }
        }
        static_cast<const server::WorkerdApi &>(worker->getIsolate().getApi())
            .compileGlobals(js, globals.asReader(), KJ_REQUIRE_NONNULL(exportsHandle).getHandle(js),
                [](kj::StringPtr key) -> kj::Own<ActorIdFactory> {
          return kj::heap<ObjectIdFactory>(key);
        });
        exportsHandle = kj::none;
      });
      isolateAddress = reinterpret_cast<uintptr_t>(lock.getIsolate());
    });
    reporter.requireValid();
    slot.httpEntrypoints = std::move(reporter.httpEntrypoints);
    slot.entrypoints = std::move(reporter.entrypoints);
    slot.actorClasses = std::move(reporter.actorClasses);
    slot.creates.fetch_add(1);
    compilations.fetch_add(1);
    writeLimitRow("created", slot.limits->identity, 0, slot.limits->cpuMs, slot.limits->memoryMiB);
    writeLimitRow(
        "memory-created", slot.limits->identity, 0, slot.limits->cpuMs, slot.limits->memoryMiB);
    auto row = kj::str("[threaded-worker] {\"workerId\":", encode(field(descriptor, "workerId")),
        ",\"definitionHash\":", encode(field(descriptor, "definitionHash")),
        ",\"tid\":", syscall(SYS_gettid), ",\"isolate\":", (uint64_t)isolateAddress,
        ",\"compilations\":1}\n");
    (void)!write(STDERR_FILENO, row.begin(), row.size());
    return worker;
  }

  kj::Promise<std::shared_ptr<Slot>> loadDurableWorker(
      ThreadState &state, kj::String descriptor, std::string definition) {
    JsonDoc document(descriptor);
    auto input = document.get();
    const bool platformDatabase = definition.starts_with(PLATFORM_DATABASE_COPY_PREFIX);
    std::shared_ptr<ResourceReservation> reservation;
    {
      std::lock_guard lock(durableRoutingMutex);
      reservation = std::move(threads[state.durableHome - 1]->home->preparation);
    }
    const bool registered = bool(reservation);
    if (!registered) {
      reservation = std::make_shared<ResourceReservation>(
          resources, workerLimit(input, "memoryLimitMiB", "COWORKERD_MEMORY_LIMIT_MIB"), platformDatabase);
    }
    if (refuseCopyReservationForTest() ||
        (!registered && !(co_await registerDurablePreparation(
            *reservation, platformDatabase, [this] { return reclaimIdleDatabase(); })))) {
      auto error = capacityError();
      error.setDetail(DURABLE_COPY_RESERVATION_REFUSED, kj::heapArray<kj::byte>(0));
      kj::throwRecoverableException(kj::mv(error));
    }
    auto slot = std::make_shared<Slot>();
    slot->reservation = std::move(reservation);
    slot->instance = ++nextInstance;
    slot->workerId = str(input, "workerId");
    slot->versionId = str(input, "versionId");
    slot->planRevision = std::move(definition);
    slot->isolateLabel = "coworkerd-durable:" + std::to_string(slot->instance);
    slot->homeTurn = [this, homeId = state.durableHome, loaded = slot.get()](bool entering) {
      const auto &memory = *loaded->limits->memory;
      trackDurableTurn(homeId, entering, memory.heap.load(), memory.external.load());
    };
    slot->resetDurableRoot = [this, &state](Worker::Actor &actor) {
      auto at = state.durableActorTrees.find(&actor);
      if (at == state.durableActorTrees.end()) return;
      auto root = state.durableRoots.find(at->second);
      if (root == state.durableRoots.end() || root->second.resetting) return;
      durableActorResets.fetch_add(1);
      beginDurableReset(state, at->second, limitException(LimitCause::CPU));
      if (root->second.actor.get() != &actor) {
        // A root can be awaiting this facet. Unwind the facet turn before aborting its caller.
        state.tasks.add(kj::evalLater([owner = kj::addRef(*root->second.actor)]() mutable {
          owner->abort(limitException(LimitCause::CPU));
        }));
      }
    };
    auto preparation = std::make_shared<ResourcePreparation>(slot->reservation);
    auto source = co_await fetchSource(input, state, *slot);
    // End turn accounting before co_return moves the local slot into its result.
    {
      trackDurableTurn(state.durableHome, true);
      KJ_DEFER({
        const auto* memory = slot->limits ? slot->limits->memory.get() : nullptr;
        trackDurableTurn(state.durableHome, false,
            memory ? memory->heap.load() : 0, memory ? memory->external.load() : 0);
      });
      slot->worker = compile(*slot, input, source);
    }
    slot->complete = true;
    co_return slot;
  }

  kj::Promise<std::shared_ptr<Slot>> getDurableWorker(
      ThreadState &state, kj::StringPtr descriptor, const std::string &definition) {
    if (state.durableSlot && state.durableSlot->planRevision == definition) {
      co_return state.durableSlot;
    }
    KJ_REQUIRE(state.durableRoots.empty(), "durable home still owns a different isolate");
    KJ_REQUIRE(!state.durableLoad || state.durableLoad->definition == definition,
        "durable home is loading a different definition");
    state.durableSlot.reset();
    {
      std::lock_guard lock(durableRoutingMutex);
      auto &home = *threads[state.durableHome - 1]->home;
      home.warmDefinition.clear();
      home.memory = home.heapBytes = home.copyInstance = 0;
      home.databaseReservation.reset();
    }
    if (!state.durableLoad) {
      state.durableLoad.emplace(ThreadState::DurableLoad{
        .definition = definition,
        .promise = loadDurableWorker(state, kj::str(descriptor), definition).fork(),
      });
    }
    ++state.durableLoad->waiters;
    // Runs when this waiter returns, fails or is canceled. Dropping the load with its
    // last waiter cancels a load nobody waits for, before another definition can find it.
    KJ_DEFER({
      auto &load = *state.durableLoad;
      if (--load.waiters == 0) {
        if (!load.settled) canceledLoads.fetch_add(1);
        state.durableLoad.reset();
      }
    });
    std::shared_ptr<Slot> loaded;
    try {
      loaded = co_await state.durableLoad->promise.addBranch();
    } catch (...) {
      state.durableLoad->settled = true;
      throw;
    }
    state.durableLoad->settled = true;
    state.durableSlot = loaded;
    {
      std::lock_guard lock(durableRoutingMutex);
      auto &home = *threads[state.durableHome - 1]->home;
      home.warmDefinition = loaded->planRevision;
      home.copyInstance = loaded->instance;
      if (loaded->reservation->returned) home.databaseReservation = loaded->reservation;
    }
    co_return loaded;
  }

  kj::Promise<std::shared_ptr<Slot>> getWorker(
      Json::Reader descriptor, ThreadState &state, bool &coldLoaded) {
    auto plan = field(descriptor, "rootPlan");
    auto selection = field(descriptor, "rootSelection");
    if (!field(plan, "kind").isString() || text(plan, "kind") != "tenant" ||
        !field(plan, "namespaces").isArray() || field(plan, "namespaces").getArray().size() != 0 ||
        !field(selection, "kind").isString() || text(selection, "kind") != "plain" ||
        !flag(selection, "dispatchChecked")) {
      kj::throwRecoverableException(kj::Exception(kj::Exception::Type::FAILED, __FILE__, __LINE__,
          kj::str("Missing trusted ordinary worker plan")));
    }
    auto fingerprint = field(field(descriptor, "rootPlan"), "versionFingerprint");
    auto key = str(descriptor, "workerId") + "@" + str(descriptor, "versionId") + ":" +
        str(descriptor, "definitionHash") + ":" +
        (fingerprint.isString() ? std::string(fingerprint.getString().cStr())
                                : digest(kj::str(encode(field(descriptor, "variables")),
                                      encode(field(descriptor, "secrets")))
                                             .asBytes()));
    key += ":" +
        digest(
            encode(field(field(descriptor, "rootSelection"), "durableObjectBindings")).asBytes());
    key += ":" + digest(encode(field(descriptor, "nativeNamespaces")).asBytes());
    for (;;) {
      std::shared_ptr<Slot> slot;
      bool creator = false;
      for (;;) {
        std::shared_ptr<Slot> condemned;
        bool full = false;
        {
          std::lock_guard lock(registryMutex);
          auto at = registry.find(key);
          if (at != registry.end() && at->second->complete.load() && at->second->limits &&
              at->second->limits->memoryState->load() == LimitCause::MEMORY) {
            condemned = std::move(at->second);
            registry.erase(at);
            retiredSlots.push_back(condemned);
            at = registry.end();
          }
          if (at != registry.end())
            slot = at->second;
          else {
            auto reservation = std::make_shared<ResourceReservation>(
                resources, workerLimit(descriptor, "memoryLimitMiB", "COWORKERD_MEMORY_LIMIT_MIB"));
            if (!reservation->registerPreparation())
              full = true;
            else {
              slot = std::make_shared<Slot>();
              slot->reservation = std::move(reservation);
              slot->busy = kj::atomicRefcounted<BusyIsolateObserver>(isolateBusyClock);
              slot->instance = ++nextInstance;
              slot->workerId = str(descriptor, "workerId");
              slot->versionId = str(descriptor, "versionId");
              slot->planRevision = fingerprint.isString()
                  ? std::string(fingerprint.getString().cStr())
                  : str(descriptor, "definitionHash");
              slot->isolateLabel = "coworkerd-v1:" + slot->workerId + "@" + slot->versionId + "#" +
                  digest(kj::arrayPtr(reinterpret_cast<const kj::byte *>(key.data()), key.size()));
              registry.emplace(key, slot);
              creator = true;
            }
          }
        }
        if (condemned) {
          resources->evictions.fetch_add(1);
          condemned.reset();
          reapRetired();
          if (full) continue;
        }
        if (!full) break;
        if (!evictOne()) {
          resources->rejections.fetch_add(1);
          kj::throwRecoverableException(capacityError());
        }
      }
      if (creator) {
        auto preparation = std::make_shared<ResourcePreparation>(slot->reservation);
        auto trace = [&](const char *phase) {
          if (!traceForTest()) return;
          auto row =
              kj::str("[threaded-load] {\"workerId\":", encode(field(descriptor, "workerId")),
                  ",\"phase\":\"", phase, "\",\"atNs\":", nowNs(), "}\n");
          (void)!write(STDERR_FILENO, row.begin(), row.size());
        };
        auto remove = [this, &key, &slot] {
          std::lock_guard lock(registryMutex);
          auto found = registry.find(key);
          if (found != registry.end() && found->second == slot) registry.erase(found);
        };
        auto publish = [&slot] {
          std::vector<kj::Own<kj::CrossThreadPromiseFulfiller<void>>> waiters;
          {
            std::lock_guard lock(slot->mutex);
            slot->complete = true;
            waiters.swap(slot->waiters);
          }
          for (auto &waiter: waiters) waiter->fulfill();
        };
        auto cancel = kj::defer([&] {
          if (!slot->complete) {
            canceledLoads.fetch_add(1);
            slot->retryableLoad.store(true);
            remove();
            slot->error =
                KJ_EXCEPTION(DISCONNECTED, "Worker initialization canceled before publication");
            publish();
          }
        });
        bool fetched = false;
        try {
          trace("source-start");
          slot->loadStage = threaded::LoadStage::PACKAGE;
          auto source = co_await fetchSource(descriptor, state, *slot);
          trace("source-end");
          fetched = true;
          slot->worker = compile(*slot, descriptor, source);
          // All startup locks have been released. Clear their time before the slot is
          // published to request threads and the lifecycle sampler.
          KJ_ASSERT_NONNULL(slot->busy)->window.reset();
          trace("compiled");
        } catch (kj::Exception &error) {
          auto stage = kj::heapArray<kj::byte>(1);
          stage[0] = static_cast<kj::byte>(slot->loadStage);
          error.setDetail(LOAD_STAGE_DETAIL, kj::mv(stage));
          slot->error = kj::mv(error);
        }
        const bool pressure = slot->limits &&
            (slot->limits->startupCause.load() == LimitCause::PROCESS_MEMORY ||
                slot->limits->memory->pendingPressure.load());
        if (loadFailureRetryable(slot->error, fetched, pressure)) {
          slot->retryableLoad.store(true);
          remove();
        }
        publish();
      } else {
        auto signal = kj::newPromiseAndCrossThreadFulfiller<void>();
        bool wait;
        {
          std::lock_guard lock(slot->mutex);
          wait = !slot->complete;
          if (wait) slot->waiters.push_back(kj::mv(signal.fulfiller));
        }
        if (wait) co_await signal.promise;
      }
      KJ_IF_SOME(error, slot->error) {
        if (!creator && slot->retryableLoad.load()) continue;
        kj::throwRecoverableException(error.clone());
      }
      slot->lastUseNs.store(nowNs());
      coldLoaded = creator;
      co_return slot;
    }
  }
};

DurableBudgetRelease::~DurableBudgetRelease() {
  server.queueBudgetRelease(ticket);
}

struct DurableSocketStart {
  std::function<void()> opened;
  std::shared_ptr<server::DurableIdleLifecycle> idle;
  kj::String invocation;
  const kj::HttpHeaderTable& headers;
  std::shared_ptr<DurableTreeContext> tree;
};

struct StagedDurableUpgrade {
  uint facet;
  kj::Own<LegacyHibernationManagerImpl> manager;
};
void beginDurableUpgrade(DurableTreeContext &tree, kj::StringPtr invocation);
void finishDurableUpgrade(DurableTreeContext &tree, kj::StringPtr invocation);
kj::Maybe<StagedDurableUpgrade> findDurableUpgrade(
    DurableTreeContext &tree, kj::StringPtr token, kj::StringPtr invocation);

class DurableSocketStream final: public kj::WebSocket {
 public:
  DurableSocketStream(kj::Own<kj::WebSocket> inner, std::shared_ptr<server::DurableIdleLifecycle> idle)
      : inner(kj::mv(inner)), idle(std::move(idle)) {}

  kj::Promise<void> send(kj::ArrayPtr<const kj::byte> message) override {
    touch();
    return inner->send(message);
  }
  kj::Promise<void> send(kj::ArrayPtr<const char> message) override {
    touch();
    return inner->send(message);
  }
  kj::Promise<Message> receive(size_t maxSize) override {
    return inner->receive(maxSize).then([this](Message message) {
      touch();
      return kj::mv(message);
    });
  }
  kj::Promise<void> close(uint16_t code, kj::StringPtr reason) override { return inner->close(code, reason); }
  void disconnect() override { inner->disconnect(); }
  void abort() override { inner->abort(); }
  kj::Promise<void> whenAborted() override { return inner->whenAborted(); }
  uint64_t sentByteCount() override { return inner->sentByteCount(); }
  uint64_t receivedByteCount() override { return inner->receivedByteCount(); }
  kj::Maybe<kj::String> getPreferredExtensions(ExtensionsContext context) override {
    return inner->getPreferredExtensions(context);
  }

 private:
  void touch() {
    idle->eventFinished(server::DurableAdmission::Clock::now());
  }
  kj::Own<kj::WebSocket> inner;
  std::shared_ptr<server::DurableIdleLifecycle> idle;
};

class DurableHttp final: public kj::HttpService {
 public:
  DurableHttp(kj::Own<WorkerInterface> worker, kj::Maybe<DurableSocketStart> socket)
      : worker(kj::mv(worker)), socket(kj::mv(socket)) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders &headers,
      kj::AsyncInputStream &body,
      Response &response) override {
    try {
      if (headers.get(kj::HttpHeaderId::UPGRADE) != kj::none) {
        KJ_IF_SOME(start, socket) {
          beginDurableUpgrade(*start.tree, start.invocation);
          KJ_DEFER(finishDurableUpgrade(*start.tree, start.invocation));
          struct UpgradingResponse final: Response {
            UpgradingResponse(Response& inner, DurableSocketStart& start): inner(inner), start(start) {}
            kj::Own<kj::AsyncOutputStream> send(uint status, kj::StringPtr text,
                const kj::HttpHeaders& headers, kj::Maybe<uint64_t> size) override {
              return inner.send(status, text, headers, size);
            }
            kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders& headers) override {
              constexpr auto marker = LegacyHibernationManagerImpl::NODE_SOCKET_HEADER;
              kj::HttpHeaders outgoing(start.headers);
              kj::Maybe<kj::StringPtr> pending;
              headers.forEach([&](kj::StringPtr name, kj::StringPtr value) {
                if (strcasecmp(name.cStr(), marker.cStr()) == 0) {
                  pending = value;
                } else {
                  outgoing.addPtrPtr(name, value);
                }
              });
              KJ_IF_SOME(id, pending) {
                auto upgrade = KJ_REQUIRE_NONNULL(findDurableUpgrade(*start.tree, id, start.invocation),
                    "node-owned WebSocket is missing from the admitted tree");
                auto trusted = upgrade.facet == 0 ? kj::str(start.invocation)
                                                  : kj::str(start.invocation, ":", upgrade.facet);
                outgoing.addPtrPtr(marker, trusted);
                auto accepted = inner.acceptWebSocket(outgoing);
                // The binding channel acknowledges this transport separately
                // from HTTP-over-Cap'n-Proto's WebSocket pipe.
                attaching = upgrade.manager->activateNodeSocket(id, start.invocation)
                                .attach(kj::mv(upgrade.manager))
                                .eagerlyEvaluate(nullptr);
                return accepted;
              }
              auto opened = inner.acceptWebSocket(outgoing);
              start.opened();
              return kj::heap<DurableSocketStream>(kj::mv(opened), start.idle);
            }
            Response& inner;
            DurableSocketStart& start;
            kj::Maybe<kj::Promise<void>> attaching;
          } upgrading(response, start);
          co_await worker->request(method, url, headers, body, upgrading);
          KJ_IF_SOME(attachment, upgrading.attaching) {
            co_await kj::mv(attachment);
          }
          co_return;
        } else {
          unsupported("WebSockets in the durable test role");
        }
      }
      co_await worker->request(method, url, headers, body, response);
    } catch (const kj::Exception &error) {
      reportDurableFailureForTest("HTTP event", error);
      throw;
    }
  }

 private:
  kj::Own<WorkerInterface> worker;
  kj::Maybe<DurableSocketStart> socket;
};

struct DurableAlarmEvent {
  kj::Own<Worker::Actor> actor;
  kj::Date scheduledTime;
  uint32_t retryCount;
  bool clear;
};

// This service is selected by the trusted event descriptor. A Worker fetch
// to the same URL uses its regular entrypoint.
class DurableAlarmHttp final: public kj::HttpService {
 public:
  DurableAlarmHttp(
      kj::Own<WorkerInterface> worker, DurableAlarmEvent event, const kj::HttpHeaderTable &headers)
      : worker(kj::mv(worker)),
        event(kj::mv(event)),
        headers(headers) {}

  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders&,
      kj::AsyncInputStream&,
      Response& response) override {
    KJ_REQUIRE(
        method == kj::HttpMethod::POST && url == "http://coworkerd-node/__coworkerd/durable-alarm",
        "invalid alarm event");
    KJ_REQUIRE(worker.get() != nullptr, "one call per alarm event");
    auto entry = kj::mv(worker);
    auto& actor = *event.actor;
    auto& gate = actor.getOutputGate();
    auto &storage = KJ_REQUIRE_NONNULL(kj::tryDowncast<ActorSqlite>(
        KJ_REQUIRE_NONNULL(actor.getPersistent(), "an alarm requires persistent storage")));
    co_await gate.wait(nullptr);

    kj::StringPtr outcome;
    auto confirmedTime = storage.confirmedAlarmTime();
    if (event.clear) {
      co_await entry->abandonAlarm(event.scheduledTime);
      co_await gate.wait(nullptr);
      confirmedTime = storage.confirmedAlarmTime();
      if (confirmedTime == kj::none) {
        outcome = "cleared";
      } else {
        outcome = confirmedTime != event.scheduledTime ? "notDue" : "unavailable";
      }
    } else if (confirmedTime != event.scheduledTime) {
      outcome = "notDue";
    } else {
      WorkerInterface::AlarmResult result;
      try {
        result = co_await entry->runAlarm(event.scheduledTime, event.retryCount);
      } catch (const kj::Exception &error) {
        const auto description = error.getDescription();
        const auto tunneled = jsg::tunneledErrorType(description);
        const bool tenantFailure = error.getDetail(jsg::EXCEPTION_IS_USER_ERROR) != kj::none ||
            jsg::isExceptionFromInputGateBroken(description) ||
            (tunneled.isJsgError && !tunneled.isInternal &&
                (!tunneled.isDurableObjectReset ||
                    description.startsWith("broken.constructorFailed;"_kj)));
        if (!tenantFailure) throw;
        result.outcome = EventOutcome::EXCEPTION;
        result.retry = error.getDetail(jsg::EXCEPTION_DURABLE_OBJECT_ABORT_NO_RETRY) == kj::none;
      }
      if (result.outcome == EventOutcome::OK) {
        co_await gate.wait(nullptr);
        outcome = "fired";
      } else if (!result.retry && result.outcome == EventOutcome::CANCELED) {
        co_await gate.wait(nullptr);
        confirmedTime = storage.confirmedAlarmTime();
        outcome = confirmedTime != event.scheduledTime ? "notDue" : "unavailable";
      } else if (!result.retry) {
        outcome = "stop";
      } else if (result.retryCountsAgainstLimit) {
        outcome = "failed";
      } else {
        outcome = "unavailable";
      }
    }
    auto atUnixMs = (confirmedTime.orDefault(kj::UNIX_EPOCH) - kj::UNIX_EPOCH) / kj::MILLISECONDS;
    auto json = outcome == "notDue"
        ? kj::str("{\"outcome\":\"notDue\",\"atUnixMs\":", atUnixMs, "}")
        : kj::str("{\"outcome\":\"", outcome, "\"}");
    auto body = response.send(200, "OK", kj::HttpHeaders(headers), json.size());
    co_await body->write(json.asBytes());
  }

 private:
  kj::Own<WorkerInterface> worker;
  DurableAlarmEvent event;
  const kj::HttpHeaderTable& headers;
};

struct DurableSocketEvent {
  enum class Kind { TEXT, BINARY, CLOSE, ERROR };
  kj::Own<Worker::Actor> actor;
  kj::String socketId;
  uint64_t position;
  Kind kind;
  size_t bytes;
  uint16_t closeCode = 0;
  bool wasClean = false;
  kj::Maybe<kj::Date> lastAutoResponse;
};

DurableSocketEvent readDurableSocketEvent(Json::Reader descriptor, kj::Own<Worker::Actor> actor) {
  using Kind = DurableSocketEvent::Kind;
  auto name = text(descriptor, "kind");
  Kind kind;
  size_t limit = 32u << 20;
  if (name == "text") {
    kind = Kind::TEXT;
  } else if (name == "binary") {
    kind = Kind::BINARY;
  } else if (name == "close") {
    kind = Kind::CLOSE;
    limit = 123;  // A close control frame allows 125 bytes, including its two-byte status.
  } else {
    KJ_REQUIRE(name == "error", "invalid durable socket event kind");
    kind = Kind::ERROR;
    limit = 0;
  }
  const auto bytes = durableNumber(descriptor, "bytes");
  KJ_REQUIRE(bytes <= limit, "invalid durable socket event size");
  DurableSocketEvent event{kj::mv(actor), text(descriptor, "id"),
    durableNumber(descriptor, "frame"), kind, static_cast<size_t>(bytes)};
  if (kind == Kind::CLOSE) {
    const auto code = durableNumber(descriptor, "code");
    KJ_REQUIRE(code <= UINT16_MAX, "invalid durable socket close code");
    event.closeCode = static_cast<uint16_t>(code);
    event.wasClean = flag(descriptor, "wasClean");
  }
  if (field(descriptor, "autoResponseUnixMs").isString()) {
    const auto milliseconds = durableNumber(descriptor, "autoResponseUnixMs");
    KJ_REQUIRE(milliseconds <= INT64_MAX / 1000000);
    event.lastAutoResponse = kj::UNIX_EPOCH + milliseconds * kj::MILLISECONDS;
  }
  return event;
}

class DurableSocketHttp final: public kj::HttpService {
 public:
  DurableSocketHttp(kj::Own<WorkerInterface> worker, DurableSocketEvent event,
      const kj::HttpHeaderTable& headers)
      : worker(kj::mv(worker)), event(kj::mv(event)), headers(headers) {}

  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url,
      const kj::HttpHeaders&, kj::AsyncInputStream& body, Response& response) override {
    KJ_REQUIRE(method == kj::HttpMethod::POST &&
        url == "http://coworkerd-node/__coworkerd/durable-socket", "invalid socket event");
    KJ_REQUIRE(worker.get() != nullptr, "one call per socket event");
    KJ_REQUIRE(body.tryGetLength() == event.bytes, "invalid durable socket body length");
    auto entry = kj::mv(worker);
    kj::Maybe<api::HibernatableSocketParams> params;
    using Kind = DurableSocketEvent::Kind;
    if (event.kind == Kind::BINARY) {
      auto bytes = kj::heapArray<kj::byte>(event.bytes);
      if (bytes.size() > 0) co_await body.read(bytes.asPtr());
      params.emplace(kj::mv(bytes), kj::String());
    } else if (event.kind == Kind::ERROR) {
      params.emplace(KJ_EXCEPTION(FAILED, "WebSocket transport error"), kj::String());
    } else {
      auto text = kj::heapString(event.bytes);
      if (text.size() > 0) co_await body.read(text.asBytes());
      if (event.kind == Kind::TEXT) {
        params.emplace(kj::mv(text), kj::String());
      } else {
        params.emplace(event.closeCode, kj::mv(text), event.wasClean, kj::String());
      }
    }
    auto& manager = kj::downcast<LegacyHibernationManagerImpl>(
        KJ_REQUIRE_NONNULL(event.actor->getHibernationManager()));
    co_await manager.dispatchNodeSocket(*entry, kj::mv(event.socketId), event.position,
        event.lastAutoResponse, kj::mv(KJ_ASSERT_NONNULL(params)));
    co_await event.actor->getOutputGate().wait(nullptr);
    response.send(204, "No Content", kj::HttpHeaders(headers), uint64_t{0});
  }

 private:
  kj::Own<WorkerInterface> worker;
  DurableSocketEvent event;
  const kj::HttpHeaderTable& headers;
};

class ChannelDispatcher final: public rpc::EventDispatcher::Server {
 public:
  ChannelDispatcher(ThreadState &state,
      kj::Own<WorkerInterface> worker,
      std::shared_ptr<Slot> bindingSlot = {},
      kj::String bindingIdentity = nullptr,
      kj::Maybe<DurableAlarmEvent> alarm = kj::none,
      kj::Maybe<DurableSocketStart> socket = kj::none,
      kj::Maybe<DurableSocketEvent> socketEvent = kj::none)
      : state(state),
        worker(kj::mv(worker)),
        bindingSlot(std::move(bindingSlot)),
        bindingIdentity(kj::mv(bindingIdentity)),
        alarm(kj::mv(alarm)), socket(kj::mv(socket)), socketEvent(kj::mv(socketEvent)) {}
  kj::Promise<void> getHttpService(GetHttpServiceContext context) override {
    KJ_REQUIRE(worker.get() != nullptr, "one event per channel invocation");
    auto &http = state.http;
    KJ_IF_SOME(event, alarm) {
      context.getResults().setHttp(
          http.kjToCapnp(kj::heap<DurableAlarmHttp>(kj::mv(worker), kj::mv(event), *state.headers)));
    } else KJ_IF_SOME(event, socketEvent) {
      context.getResults().setHttp(
          http.kjToCapnp(kj::heap<DurableSocketHttp>(kj::mv(worker), kj::mv(event), *state.headers)));
    } else if (state.durableHome) {
      context.getResults().setHttp(http.kjToCapnp(kj::heap<DurableHttp>(kj::mv(worker), kj::mv(socket))));
    } else {
      context.getResults().setHttp(http.kjToCapnp(kj::mv(worker)));
    }
    return kj::READY_NOW;
  }
  kj::Promise<void> jsRpcSession(JsRpcSessionContext context) override;

 private:
  ThreadState &state;
  kj::Own<WorkerInterface> worker;
  // Only a node-dispatched invocation receives the platform binding handshake.
  std::shared_ptr<Slot> bindingSlot;
  kj::String bindingIdentity;
  kj::Maybe<DurableAlarmEvent> alarm;
  kj::Maybe<DurableSocketStart> socket;
  kj::Maybe<DurableSocketEvent> socketEvent;
};

class ChannelBootstrap final: public rpc::WorkerdBootstrap::Server {
 public:
  ChannelBootstrap(ThreadState &state, kj::Rc<IoChannelFactory::SubrequestChannel> channel)
      : state(state),
        channel(kj::mv(channel)) {}
  kj::Promise<void> startEvent(StartEventContext context) override {
    IoChannelFactory::SubrequestMetadata metadata;
    auto cf = context.getParams().getCfBlobJson();
    if (cf.size() != 0) metadata.cfBlobJson = kj::str(cf);
    context.getResults().setDispatcher(
        kj::heap<ChannelDispatcher>(state, channel->startRequest(kj::mv(metadata))));
    return kj::READY_NOW;
  }

 private:
  ThreadState &state;
  kj::Rc<IoChannelFactory::SubrequestChannel> channel;
};

class RemoteChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  RemoteChannel(ThreadState &state, rpc::WorkerdBootstrap::Client bootstrap)
      : state(state),
        bootstrap(kj::mv(bootstrap)) {}
  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata metadata) override {
    auto request = bootstrap.startEventRequest();
    KJ_IF_SOME(cf, metadata.cfBlobJson) {
      request.setCfBlobJson(cf);
    }
    return kj::heap<RpcWorkerInterface>(state.http, state.bytes,
        getUnsupportedFrankenvalueHandler(), request.send().getDispatcher());
  }
  void requireAllowsTransfer() override {}
  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage) override {
    unsupported("binding persistence");
  }

 private:
  ThreadState &state;
  rpc::WorkerdBootstrap::Client bootstrap;
};

Frankenvalue readNativeProps(ThreadState &state, rpc::Frankenvalue::Reader value) {
  kj::Vector<kj::Own<Frankenvalue::CapTableEntry>> capabilities;
  auto table = value.getCapTable().getAs<capnp::List<threaded::rpc::Channel>>();
  for (auto item: table) {
    KJ_REQUIRE(item.isService(), "unsupported native props channel kind");
    capabilities.add(kj::rc<RemoteChannel>(state, item.getService()).toOwn());
  }
  return Frankenvalue::fromCapnp(value, kj::mv(capabilities));
}

using PropsMessage = kj::Own<capnp::MallocMessageBuilder>;
PropsMessage writeNativeProps(ThreadState &state, Frankenvalue &props) {
  auto message = kj::heap<capnp::MallocMessageBuilder>();
  auto root = message->initRoot<rpc::Frankenvalue>();
  props.toCapnp(root);
  auto caps = props.getCapTable();
  auto table = root.getCapTable().initAs<capnp::List<threaded::rpc::Channel>>(caps.size());
  for (auto i: kj::indices(caps)) {
    auto &channel =
        KJ_REQUIRE_NONNULL(kj::tryDowncast<IoChannelFactory::SubrequestChannel>(*caps[i]),
            "native props only transfer resolved service or actor channels");
    table[i].setService(kj::heap<ChannelBootstrap>(state, channel.addRef()));
  }
  return message;
}

class NodeBindingDispatcher final: public rpc::EventDispatcher::Server {
 public:
  NodeBindingDispatcher(
      rpc::WorkerdBootstrap::Client bootstrap, kj::String envelope, PropsMessage props)
      : bootstrap(kj::mv(bootstrap)),
        envelope(kj::mv(envelope)),
        props(kj::mv(props)) {}
  rpc::EventDispatcher::Client startEvent(bool http) {
    if (props.get() != nullptr) {
      auto request = bootstrap.castAs<threaded::rpc::Bootstrap>().startNativeEventRequest();
      if (http) request.setCfBlobJson(envelope);
      request.setCoworkerdProps(props->getRoot<rpc::Frankenvalue>());
      return request.send().getDispatcher();
    }
    auto request = bootstrap.startEventRequest();
    if (http) request.setCfBlobJson(envelope);
    return request.send().getDispatcher();
  }
  kj::Promise<void> getHttpService(GetHttpServiceContext context) override {
    return context.tailCall(startEvent(true).getHttpServiceRequest());
  }
  kj::Promise<void> jsRpcSession(JsRpcSessionContext context) override {
    auto session = startEvent(false).jsRpcSessionRequest().send();
    auto bind = session.getTopLevel().callRequest();
    bind.setMethodName(envelope);
    auto bound = bind.send();
    auto results = context.getResults();
    results.setTopLevel(bound.getCallPipeline());
    results.setSession(session.getSession());
    return kj::READY_NOW;
  }

 private:
  rpc::WorkerdBootstrap::Client bootstrap;
  kj::String envelope;
  PropsMessage props;
};

struct BindingConnection {
  kj::Own<kj::NetworkAddress> address;
  kj::Own<kj::HttpClient> connector;
  kj::Own<kj::AsyncIoStream> stream;
  capnp::TwoPartyClient rpc;
  BindingConnection(kj::Own<kj::NetworkAddress> address,
      kj::Own<kj::HttpClient> connector,
      kj::Own<kj::AsyncIoStream> stream)
      : address(kj::mv(address)),
        connector(kj::mv(connector)),
        stream(kj::mv(stream)),
        rpc(*this->stream) {}
};

kj::Promise<kj::Own<WorkerInterface>> openBinding(
    ThreadState &state, kj::String addressText, kj::String envelope, PropsMessage props = {}) {
  KJ_REQUIRE(addressText.size() != 0, "missing node binding socket");
  auto address = co_await state.io.provider->getNetwork().parseAddress(addressText);
  auto connector = kj::newHttpClient(state.io.provider->getTimer(), *state.headers, *address);
  auto tunnel = connector->connect("coworkerd-server", kj::HttpHeaders(*state.headers), {});
  auto status = co_await tunnel.status;
  KJ_REQUIRE(status.statusCode == 200, "node binding CONNECT failed");
  auto connection =
      kj::heap<BindingConnection>(kj::mv(address), kj::mv(connector), kj::mv(tunnel.connection));
  rpc::EventDispatcher::Client dispatcher = kj::heap<NodeBindingDispatcher>(
      connection->rpc.bootstrap().castAs<rpc::WorkerdBootstrap>(), kj::mv(envelope), kj::mv(props));
  kj::Own<WorkerInterface> result = kj::heap<RpcWorkerInterface>(
      state.http, state.bytes, getUnsupportedFrankenvalueHandler(), kj::mv(dispatcher));
  co_return result.attach(kj::mv(connection));
}

struct DurableSocketSnapshot {
  uint facet;
  LegacyHibernationManagerImpl::SocketSnapshot socket;
};

using DurableFacetStarts = capnp::List<threaded::rpc::ThreadedSocketFacet>;
using DurableSocketAdoption = capnp::Response<threaded::rpc::ThreadedSocketControl::AdoptResults>;

class DurableNodeSockets final {
 public:
  DurableNodeSockets(ThreadState& state, kj::String address, DurableRoute route, uint64_t walOpen,
      std::shared_ptr<server::DurableIdleLifecycle> idle)
      : state(state), address(kj::mv(address)), route(std::move(route)), walOpen(walOpen),
        idle(std::move(idle)) {}

  auto beginWrite() {
    const auto position = idle->pendingStarted(durableIdleNow());
    return kj::defer(
        [idle = idle, position] { idle->pendingFinished(position, durableIdleNow()); });
  }

  kj::Promise<void> ready() {
    if (!connected) {
      connected =
          std::make_shared<ConnectAttempt>(clock().timeoutAfter(CONTROL_TIMEOUT, connect()));
    }
    auto attempt = connected;
    try {
      co_await attempt->ready.addBranch();
    } catch (...) {
      forget(attempt);
      throw;
    }
  }

  void writeOpen(threaded::rpc::ThreadedSocketOpen::Builder out) const {
    out.setNamespaceId(route.key.namespaceId.c_str());
    out.setObjectId(route.key.objectId.c_str());
    out.setInvocationId(route.invocation.c_str());
    out.setDataEpoch(route.open.dataEpoch);
    out.setShardEpoch(route.open.shardEpoch);
    out.setGeneration(route.open.generation);
    out.setWalOpenId(walOpen);
  }

  void writeAuthority(threaded::rpc::ThreadedSocketAuthority::Builder out,
      kj::StringPtr id, kj::StringPtr invocation) const {
    out.setSocketId(id);
    out.setInvocationId(invocation);
    out.setDataEpoch(route.open.dataEpoch);
    out.setShardEpoch(route.open.shardEpoch);
    out.setGeneration(route.open.generation);
  }

  kj::Promise<SocketParkResult> park(
      kj::Array<DurableSocketSnapshot> sockets, DurableFacetStarts::Reader facets) {
    if (sockets.size() == 0) co_return SocketParkResult::PARKED;
    co_await ready();
    const auto parkAttempt = ++lastParkAttempt;
    auto call = control.parkRequest();
    writeOpen(call.initOpen());
    call.setAttempt(parkAttempt);
    call.setFacets(facets);
    auto entries = call.initSockets(sockets.size());
    for (auto index: kj::indices(sockets)) {
      auto entry = entries[index];
      auto &incoming = sockets[index];
      auto &socket = incoming.socket;
      entry.setFacetId(incoming.facet);
      entry.initAuthority().setSocketId(socket.id);
      entry.setLastFrame(socket.lastFrame);
      auto tags = entry.initTags(socket.tags.size());
      for (auto tag: kj::indices(socket.tags)) tags.set(tag, socket.tags[tag]);
      KJ_IF_SOME(attachment, socket.attachment) {
        entry.setHasAttachment(true);
        entry.setAttachment(attachment);
      }
      entry.setClosedOutgoing(socket.closedOutgoing);
    }
    auto attempt = connected;
    try {
      co_return (co_await clock().timeoutAfter(CONTROL_TIMEOUT, call.send())).getAccepted()
          ? SocketParkResult::PARKED
          : SocketParkResult::REFUSED;
    } catch (const kj::Exception &error) {
      reportDurableFailureForTest("WebSocket parking reply", error);
      forget(attempt);
    }
    try {
      co_return (co_await clock().timeoutAfter(CONTROL_TIMEOUT, resolvePark(parkAttempt)))
          ? SocketParkResult::PARKED
          : SocketParkResult::REFUSED;
    } catch (const kj::Exception &error) {
      reportDurableFailureForTest("WebSocket parking resolution", error);
      co_return SocketParkResult::UNKNOWN;
    }
  }

  kj::Promise<kj::Maybe<DurableSocketAdoption>> adopt() {
    co_await ready();
    const auto adoption = ++lastAdoptAttempt;
    auto attempt = connected;
    kj::Maybe<DurableSocketAdoption> answer;
    try {
      answer = co_await clock().timeoutAfter(CONTROL_TIMEOUT, requestAdopt(adoption, false));
    } catch (const kj::Exception &error) {
      reportDurableFailureForTest("WebSocket adoption reply", error);
      forget(attempt);
    }
    if (answer == kj::none) {
      try {
        answer = co_await clock().timeoutAfter(CONTROL_TIMEOUT, requestAdopt(adoption, true));
      } catch (const kj::Exception &error) {
        reportDurableFailureForTest("WebSocket adoption resolution", error);
        co_return kj::none;
      }
    }
    auto &result = KJ_ASSERT_NONNULL(answer);
    KJ_REQUIRE(result.getAccepted(), "the node refused to restore the Durable Object's WebSockets");
    co_return kj::mv(answer);
  }

  kj::Promise<void> closeAborted(kj::Array<kj::String> ids) {
    auto pending = beginWrite();
    co_await ready();
    auto call = control.closeAbortedRequest();
    writeOpen(call.initOpen());
    auto socketIds = call.initSocketIds(ids.size());
    for (auto i: kj::indices(ids)) socketIds.set(i, ids[i]);
    co_await awaitReply(call.send(), clock().now() + CONTROL_TIMEOUT);
  }

  kj::Timer& clock() { return state.io.provider->getTimer(); }

  template <typename Result>
  kj::Promise<capnp::Response<Result>> awaitReply(
      capnp::RemotePromise<Result> reply, kj::Maybe<kj::TimePoint> deadline = kj::none) {
    auto attempt = connected;
    try {
      KJ_IF_SOME(at, deadline) {
        co_return co_await clock().timeoutAt(at, kj::mv(reply));
      }
      co_return co_await kj::mv(reply);
    } catch (...) {
      forget(attempt);
      throw;
    }
  }

 private:
  static constexpr auto CONTROL_TIMEOUT = 5 * kj::SECONDS;
  struct ConnectAttempt {
    explicit ConnectAttempt(kj::Promise<void> promise): ready(promise.fork()) {}
    kj::ForkedPromise<void> ready;
  };
  void forget(const std::shared_ptr<ConnectAttempt> &attempt) {
    if (connected != attempt) return;
    control = nullptr;
    connection = nullptr;
    connected.reset();
  }

  kj::Promise<bool> resolvePark(uint64_t attempt) {
    co_await ready();
    auto call = control.resolveParkRequest();
    writeOpen(call.initOpen());
    call.setAttempt(attempt);
    co_return (co_await call.send()).getAccepted();
  }

  kj::Promise<capnp::Response<threaded::rpc::ThreadedSocketControl::AdoptResults>> requestAdopt(
      uint64_t attempt, bool resolveOnly) {
    co_await ready();
    auto call = control.adoptRequest();
    writeOpen(call.initOpen());
    call.setAttempt(attempt);
    call.setResolveOnly(resolveOnly);
    co_return co_await awaitReply(call.send());
  }

  kj::Promise<void> connect() {
    KJ_REQUIRE(address.size() != 0, "missing node socket control address");
    auto parsed = co_await state.io.provider->getNetwork().parseAddress(address);
    auto connector = kj::newHttpClient(state.io.provider->getTimer(), *state.headers, *parsed);
    auto tunnel = connector->connect("coworkerd-server", kj::HttpHeaders(*state.headers), {});
    auto status = co_await tunnel.status;
    KJ_REQUIRE(status.statusCode == 200, "node socket control CONNECT failed");
    auto established =
        kj::heap<BindingConnection>(kj::mv(parsed), kj::mv(connector), kj::mv(tunnel.connection));
    auto bootstrap = established->rpc.bootstrap().castAs<threaded::rpc::Bootstrap>();
    control = (co_await bootstrap.socketControlRequest().send()).getControl();
    connection = kj::mv(established);
  }

  ThreadState& state;
  kj::String address;
  DurableRoute route;
  uint64_t walOpen;
  uint64_t lastParkAttempt = 0, lastAdoptAttempt = 0;
  std::shared_ptr<server::DurableIdleLifecycle> idle;
  kj::Own<BindingConnection> connection;
  std::shared_ptr<ConnectAttempt> connected;

 public:
  threaded::rpc::ThreadedSocketControl::Client control{nullptr};
};

class DurableNodeWebSocket final: public LegacyHibernationManagerImpl::NodeSocket {
 public:
  explicit DurableNodeWebSocket(std::shared_ptr<DurableNodeSockets> control)
      : control(std::move(control)),
        attaching(kj::newPromiseAndFulfiller<void>()),
        attached(attaching.promise.fork()),
        upgrading(kj::newPromiseAndFulfiller<void>()),
        upgraded(upgrading.promise.fork()) {}

  void useLocalTransport(kj::Own<kj::WebSocket> transport) override {
    local = kj::mv(transport);
  }
  void startUpgrade() override {
    pendingUpgrade = true;
    upgrading.fulfiller->fulfill();
  }
  void abandonUpgrade() override {
    if (!pendingUpgrade) startUpgrade();
    attaching.fulfiller->reject(KJ_EXCEPTION(DISCONNECTED, "WebSocket upgrade was abandoned"));
    if (local) local->abort();
  }
  kj::Promise<void> whenUpgrading() override {
    return upgraded.addBranch();
  }

  void attach(kj::StringPtr socketId, kj::StringPtr invocationId) override {
    KJ_REQUIRE(this->socketId.size() == 0);
    this->socketId = kj::str(socketId);
    invocation = kj::str(invocationId);
  }
  void activate() override { attaching.fulfiller->fulfill(); }
  kj::StringPtr id() const override { return socketId; }

  kj::Promise<void> configure(
      kj::Maybe<kj::StringPtr> request, kj::Maybe<kj::StringPtr> response) override {
    return setAutoResponse(request.map([](auto text) { return kj::str(text); }),
        response.map([](auto text) { return kj::str(text); }));
  }

  kj::Promise<void> send(kj::ArrayPtr<const kj::byte> message) override {
    if (local && !pendingUpgrade && co_await writeLocally(local->send(message))) co_return;
    auto pending = control->beginWrite();
    co_await attached.addBranch();
    co_await control->ready();
    auto call = control->control.sendRequest();
    control->writeAuthority(call.initAuthority(), socketId, invocation);
    call.initFrame().setBinary(message);
    co_await control->awaitReply(call.send());
    sent += message.size();
  }

  kj::Promise<void> send(kj::ArrayPtr<const char> message) override {
    if (local && !pendingUpgrade && co_await writeLocally(local->send(message))) co_return;
    auto pending = control->beginWrite();
    co_await attached.addBranch();
    co_await control->ready();
    auto call = control->control.sendRequest();
    control->writeAuthority(call.initAuthority(), socketId, invocation);
    call.initFrame().setText(kj::str(message));
    co_await control->awaitReply(call.send());
    sent += message.size();
  }

  kj::Promise<void> close(uint16_t code, kj::StringPtr reason) override {
    if (local && !pendingUpgrade && co_await writeLocally(local->close(code, reason))) co_return;
    auto pending = control->beginWrite();
    co_await attached.addBranch();
    co_await control->ready();
    auto call = control->control.sendRequest();
    control->writeAuthority(call.initAuthority(), socketId, invocation);
    auto frame = call.initFrame().initClose();
    frame.setCode(code);
    frame.setReason(reason);
    co_await control->awaitReply(call.send());
  }

  kj::Promise<Message> receive(size_t maxSize) override {
    if (local && !pendingUpgrade) return local->receive(maxSize);
    return KJ_EXCEPTION(FAILED, "receive on a node-owned WebSocket");
  }
  void disconnect() override {
    if (local && !pendingUpgrade) local->disconnect();
  }
  void abort() override {
    if (local && !pendingUpgrade) local->abort();
  }
  kj::Promise<void> whenAborted() override {
    if (!local || pendingUpgrade) return kj::NEVER_DONE;
    return local->whenAborted().exclusiveJoin(whenUpgrading()).then([this]() -> kj::Promise<void> {
      if (pendingUpgrade) return kj::NEVER_DONE;
      return kj::READY_NOW;
    });
  }
  uint64_t sentByteCount() override { return sent; }
  uint64_t receivedByteCount() override { return 0; }
  kj::Maybe<kj::String> getPreferredExtensions(ExtensionsContext) override { return kj::none; }

 private:
  kj::Promise<bool> writeLocally(kj::Promise<void> write) {
    return write.then([] {
      return true;
    }).exclusiveJoin(whenUpgrading().then([] { return false; }));
  }

  kj::Promise<void> setAutoResponse(kj::Maybe<kj::String> request, kj::Maybe<kj::String> response) {
    auto pending = control->beginWrite();
    co_await control->ready();
    auto& timer = control->clock();
    const auto deadline = timer.now() + 5 * kj::SECONDS;
    for (;;) {
      auto call = control->control.setAutoResponseRequest();
      control->writeAuthority(call.initAuthority(), socketId, invocation);
      KJ_IF_SOME(value, request) {
        call.setEnabled(true);
        call.setRequest(value);
        call.setResponse(KJ_ASSERT_NONNULL(response));
      }
      if ((co_await control->awaitReply(call.send(), deadline)).getAccepted()) co_return;
      KJ_REQUIRE(attaching.fulfiller->isWaiting() && timer.now() < deadline,
          "the node refused to configure the WebSocket transport");
      co_await timer.afterDelay(10 * kj::MILLISECONDS);
    }
  }

  std::shared_ptr<DurableNodeSockets> control;
  kj::PromiseFulfillerPair<void> attaching;
  kj::ForkedPromise<void> attached;
  kj::PromiseFulfillerPair<void> upgrading;
  kj::ForkedPromise<void> upgraded;
  kj::Own<kj::WebSocket> local;
  bool pendingUpgrade = false;
  kj::String socketId;
  kj::String invocation;
  uint64_t sent = 0;
};

class EgressWorker final: public WorkerInterface {
 public:
  EgressWorker(ThreadState &state, kj::Own<kj::NetworkAddress> address, kj::String identity)
      : state(state),
        address(kj::mv(address)),
        client(kj::newHttpClient(state.io.provider->getTimer(), *state.headers, *this->address)),
        service(kj::newHttpService(*client)),
        identity(kj::mv(identity)) {}
  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders &headers,
      kj::AsyncInputStream &body,
      Response &response) override {
    auto outgoing = headers.clone();
    outgoing.set(state.egressSource, identity);
    struct EgressResponse final: Response {
      EgressResponse(Response &inner, kj::HttpHeaderId errorId): inner(inner), errorId(errorId) {}
      kj::Own<kj::AsyncOutputStream> send(uint status,
          kj::StringPtr text,
          const kj::HttpHeaders &headers,
          kj::Maybe<uint64_t> size) override {
        KJ_IF_SOME(error, headers.get(errorId)) {
          kj::throwRecoverableException(KJ_EXCEPTION(FAILED, kj::str("jsg.Error: ", error)));
        }
        return inner.send(status, text, headers, size);
      }
      kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders &headers) override {
        return inner.acceptWebSocket(headers);
      }
      Response &inner;
      kj::HttpHeaderId errorId;
    } checked(response, state.egressError);
    co_await service->request(method, url, outgoing, body, checked);
  }
  kj::Promise<void> connect(kj::StringPtr,
      const kj::HttpHeaders &,
      kj::AsyncIoStream &,
      ConnectResponse &,
      kj::HttpConnectSettings) override {
    unsupported("outbound CONNECT");
  }
  kj::Promise<void> prewarm(kj::StringPtr) override {
    return kj::READY_NOW;
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date, kj::StringPtr) override {
    unsupported("scheduled outbound");
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date, uint32_t) override {
    unsupported("outbound alarm");
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return event->notSupported().attach(kj::mv(event));
  }

 private:
  ThreadState &state;
  kj::Own<kj::NetworkAddress> address;
  kj::Own<kj::HttpClient> client;
  kj::Own<kj::HttpService> service;
  kj::String identity;
};

kj::Promise<kj::Own<WorkerInterface>> openEgress(
    ThreadState &state, kj::String addressText, kj::String identity) {
  if (addressText.size() == 0) {
    kj::throwRecoverableException(KJ_EXCEPTION(FAILED, "jsg.Error: EGRESS_DENIED: egress-off"));
  }
  auto address = co_await state.io.provider->getNetwork().parseAddress(addressText);
  co_return kj::heap<EgressWorker>(state, kj::mv(address), kj::mv(identity));
}

// Hold the permit for the entire interface lifetime, including response bodies,
// RPC capabilities and cancellation. No connection is opened before admission.
struct SubrequestPermit {
  SubrequestPermit(SubrequestBudget &budget,
      std::shared_ptr<uint32_t> invocation,
      std::string tenant,
      bool tenantShare)
      : budget(budget),
        invocation(std::move(invocation)),
        // Without a per-tenant limit the share is the whole budget, which the
        // global count already bounds: keep no tenant and no per-tenant count.
        tenant(tenantShare && budget.tenantLimit != 0 ? std::move(tenant) : std::string()),
        tenantShare(tenantShare && budget.tenantLimit != 0) {}
  bool acquireLocked() {
    auto perTenant = budget.tenantLimit == 0
        ? budget.limit
        : kj::min(budget.limit, kj::max<uint32_t>(1, budget.tenantLimit - 1));
    auto found = budget.tenants.find(tenant);
    if (*invocation >= 32 || budget.active.load() >= budget.limit ||
        (tenantShare && found != budget.tenants.end() && found->second >= perTenant))
      return false;
    // Bindings retain the invocation and global connection limits, but never
    // consume the external-egress tenant share a recursive child may need.
    if (tenantShare) ++budget.tenants[tenant];
    ++*invocation;
    budget.active.fetch_add(1);
    acquired = true;
    return true;
  }
  kj::Promise<void> acquire() {
    bool counted = false;
    for (;;) {
      auto signal = kj::newPromiseAndCrossThreadFulfiller<void>();
      uint64_t id;
      {
        std::lock_guard lock(budget.mutex);
        if (acquireLocked()) co_return;
        if (!counted) {
          budget.rejected.fetch_add(1);
          counted = true;
        }
        id = ++budget.nextWaiter;
        budget.waiters.emplace(id, kj::mv(signal.fulfiller));
      }
      // Cancellation removes its waiter immediately. Registration and the
      // capacity check use the release mutex, so no release wakeup is lost.
      KJ_DEFER({
        std::lock_guard lock(budget.mutex);
        budget.waiters.erase(id);
      });
      co_await signal.promise;
    }
  }
  ~SubrequestPermit() {
    if (!acquired) return;
    decltype(budget.waiters) wake;
    {
      std::lock_guard lock(budget.mutex);
      --*invocation;
      budget.active.fetch_sub(1);
      if (tenantShare) {
        auto found = budget.tenants.find(tenant);
        KJ_ASSERT(found != budget.tenants.end() && found->second > 0);
        if (--found->second == 0) budget.tenants.erase(found);
      }
      wake.swap(budget.waiters);
    }
    for (auto &[id, waiter]: wake)
      if (waiter->isWaiting()) waiter->fulfill();
  }
  SubrequestBudget &budget;
  std::shared_ptr<uint32_t> invocation;
  std::string tenant;
  bool tenantShare, acquired = false;
};

class SubrequestWorker final: public WorkerInterface {
 public:
  SubrequestWorker(std::shared_ptr<SubrequestPermit> permit, kj::Own<WorkerInterface> target)
      : permit(std::move(permit)),
        target(kj::mv(target)) {}
  kj::Promise<void> request(kj::HttpMethod method,
      kj::StringPtr url,
      const kj::HttpHeaders &headers,
      kj::AsyncInputStream &body,
      Response &response) override {
    auto active = std::move(permit);
    return target->request(method, url, headers, body, response).attach(std::move(active));
  }
  kj::Promise<void> connect(kj::StringPtr host,
      const kj::HttpHeaders &headers,
      kj::AsyncIoStream &connection,
      ConnectResponse &response,
      kj::HttpConnectSettings settings) override {
    auto active = std::move(permit);
    return target->connect(host, headers, connection, response, kj::mv(settings))
        .attach(std::move(active));
  }
  kj::Promise<void> prewarm(kj::StringPtr url) override {
    auto active = std::move(permit);
    return target->prewarm(url).attach(std::move(active));
  }
  kj::Promise<ScheduledResult> runScheduled(kj::Date time, kj::StringPtr cron) override {
    auto active = std::move(permit);
    return target->runScheduled(time, cron).attach(std::move(active));
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date time, uint32_t retry) override {
    auto active = std::move(permit);
    return target->runAlarm(time, retry).attach(std::move(active));
  }
  kj::Promise<kj::Maybe<kj::Date>> abandonAlarm(kj::Date time) override {
    auto active = std::move(permit);
    return target->abandonAlarm(time).attach(std::move(active));
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    auto active = std::move(permit);
    return target->customEvent(kj::mv(event)).attach(std::move(active));
  }

 private:
  std::shared_ptr<SubrequestPermit> permit;
  kj::Own<WorkerInterface> target;
};

struct InvocationLogBuffer {
  kj::Vector<kj::String> lines;
  uint64_t queuedLogBytes = 0, inFlightLogLines = 0, inFlightLogBytes = 0;
  bool flushing = false;
  void resetAfterFlushFailure() {
    lines.clear();
    queuedLogBytes = inFlightLogLines = inFlightLogBytes = 0;
    flushing = false;
  }
};

class LocalActorClass final: public IoChannelFactory::ActorClassChannel {
 public:
  LocalActorClass(const std::shared_ptr<Slot> &slot, kj::StringPtr name, Frankenvalue props)
      : slot(slot),
        name(kj::str(name)),
        props(kj::mv(props)) {}

  void requireAllowsTransfer() override {
    unsupported("durable test actor class transfer");
  }
  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage) override {
    unsupported("durable test actor class persistence");
  }

  std::weak_ptr<Slot> slot;
  kj::String name;
  Frankenvalue props;
};

struct Channels final: IoChannelFactory, InvocationLogBuffer {
  Channels(
      ThreadState &state, ThreadedServer &server, std::shared_ptr<Slot> slot, kj::StringPtr input)
      : document(input),
        state(state),
        server(server),
        slot(std::move(slot)) {}
  JsonDoc document;
  ThreadState &state;
  ThreadedServer &server;
  std::shared_ptr<Slot> slot;
  std::weak_ptr<void> budgetLifetime;
  uint64_t sequence = 0;
  std::shared_ptr<uint32_t> subrequests = std::make_shared<uint32_t>(0);
  threaded::InvocationLogBudget logBudget;

  Json::Reader input() {
    return document.get();
  }
  Json::Reader services() {
    return field(field(input(), "rootPlan"), "serviceBindings");
  }
  Json::Reader objects() {
    return field(field(input(), "rootPlan"), "durableObjects");
  }
  uint count(Json::Reader value) {
    return value.isArray() ? value.getArray().size() : 0;
  }
  void abortIsolate(kj::StringPtr) override {
    KJ_FAIL_REQUIRE("isolate abort");
  }
  kj::String envelope(Json::Reader target,
      kj::StringPtr entrypoint,
      bool assets,
      kj::Maybe<kj::String> durable = kj::none) {
    auto value = kj::str("{");
    bool first = true;
    auto add = [&](kj::StringPtr name, kj::String encoded) {
      value = kj::str(value, first ? "" : ",", "\"", name, "\":", encoded);
      first = false;
    };
    for (auto name: {"workerId", "versionId", "contentHash", "definitionHash"}) {
      auto item = field(target, name);
      KJ_REQUIRE(
          item.isString() && item.getString().size() != 0, "invalid binding version identity");
      add(name, encode(item));
    }
    add("treeId", encode(field(input(), "treeId")));
    add("parentInvocationId", encode(field(input(), "invocationId")));
    auto next = flag(input(), "requiresChildSequence") && !assets ? ++sequence : 0;
    add("callSequence", kj::str(next));
    auto inherited = threaded::rawField(
        std::string_view(document.original.begin(), document.original.size()), "orderSequence");
    auto order = threaded::childOrderSequence(next != 0, inherited, next);
    add("orderSequence", kj::str(order.c_str()));
    add("sequenceMapSize", kj::str(next == 0 ? 0 : 1));
    auto deadline =
        threaded::rawPath(std::string_view(document.original.begin(), document.original.size()),
            {"lease", "expiresAtMonotonicMs"});
    add("expiresAtMonotonicMs", kj::heapString(deadline.data(), deadline.size()));
    capnp::MallocMessageBuilder arena;
    auto entry = arena.initRoot<Json>();
    entry.setString(entrypoint);
    add("entrypoint", encode(entry.asReader()));
    if (assets) add("assets", kj::str("true"));
    KJ_IF_SOME(object, durable) {
      add("durableObject", kj::mv(object));
    }
    return kj::str(value, "}");
  }
  struct ObjectRoute {
    kj::String space, className, database;
  };
  ObjectRoute objectRoute(uint channel) {
    auto all = objects();
    if (channel < count(all)) {
      auto metadata = field(all.getArray()[channel], "metadata");
      auto name = text(metadata, "binding");
      auto selected = state.durableHome
          ? metadata
          : field(field(field(input(), "rootSelection"), "durableObjectBindings"), name);
      return {
        text(selected, "namespaceId"), text(metadata, "className"), text(metadata, "database")};
    }
    auto index = channel - count(all);
    KJ_REQUIRE(index < slot->loopbackActors.size(), "invalid durable namespace channel");
    auto &name = slot->loopbackActors[index];
    auto spaces = field(input(), "nativeNamespaces");
    if (spaces.isArray())
      for (auto value: spaces.getArray()) {
        if (text(value, "className") == name.c_str())
          return {text(value, "namespaceId"), kj::str(name.c_str()), kj::str("")};
      }
    KJ_FAIL_REQUIRE("DO_NAMESPACE_UNAVAILABLE: the Durable Object namespace "
                    "cannot be used");
  }
  kj::String objectEnvelope(uint channel, kj::StringPtr objectId) {
    auto route = objectRoute(channel);
    KJ_REQUIRE(route.space.size() != 0,
        "jsg.Error: DO_NAMESPACE_UNAVAILABLE: the Durable Object "
        "namespace cannot be used");
    auto quoted = [](kj::StringPtr value) {
      capnp::MallocMessageBuilder arena;
      auto root = arena.initRoot<Json>();
      root.setString(value);
      return encode(root.asReader());
    };
    auto target = field(input(), "durableObject");
    const auto callerNamespace = target.isObject() ? text(target, "namespaceId") : kj::str("");
    const auto callerObject = target.isObject() ? text(target, "objectId") : kj::str("");
    auto object = kj::str("{\"namespaceId\":", quoted(route.space),
        ",\"className\":", quoted(route.className), ",\"objectId\":", quoted(objectId),
        ",\"callerNamespaceId\":", quoted(callerNamespace),
        ",\"callerObjectId\":", quoted(callerObject),
        ",\"database\":", quoted(route.database),
        "}");
    return envelope(input(), "", false, kj::mv(object));
  }
  kj::Promise<kj::Own<WorkerInterface>> queuedRequest(
      bool egress, kj::String descriptor, PropsMessage props = {}) {
    auto worker = str(input(), "workerId");
    auto permit = std::make_shared<SubrequestPermit>(
        server.subrequestBudget, subrequests, worker.substr(0, worker.find('/')), egress);
    co_await permit->acquire();
    auto target = egress
        ? co_await openEgress(state, kj::str(server.egressAddress.c_str()), kj::mv(descriptor))
        : co_await openBinding(
              state, kj::str(server.bindingAddress.c_str()), kj::mv(descriptor), kj::mv(props));
    co_return kj::heap<SubrequestWorker>(std::move(permit), kj::mv(target));
  }
  kj::Own<WorkerInterface> startSubrequest(uint channel, SubrequestMetadata metadata) override {
    return startRequest(channel, kj::mv(metadata), kj::none);
  }
  kj::Own<WorkerInterface> startRequest(
      uint channel, SubrequestMetadata metadata, kj::Maybe<Frankenvalue> props) {
    if (channel < IoContext::SPECIAL_SUBREQUEST_CHANNEL_COUNT) {
      auto identity =
          kj::str(server.egressToken.c_str(), " {\"workerId\":", encode(field(input(), "workerId")),
              ",\"versionId\":", encode(field(input(), "versionId")),
              ",\"invocationId\":", encode(field(input(), "invocationId")), "}");
      return newPromisedWorkerInterface(
          queuedRequest(true, kj::mv(identity)).attach(kj::addRef(*this)));
    }
    auto serviceCount = count(services());
    auto objectCount = count(objects());
    auto index = channel - IoContext::SPECIAL_SUBREQUEST_CHANNEL_COUNT;
    kj::String descriptor;
    if (index < serviceCount) {
      auto route = services().getArray()[index];
      auto name = text(field(route, "metadata"), "name");
      auto props = field(field(route, "target"), "props");
      bool assets = flag(props, "assets");
      auto target =
          assets ? input() : field(field(field(input(), "rootSelection"), "serviceVersions"), name);
      descriptor = envelope(target, text(props, "entrypoint"), assets);
    } else if (index < serviceCount + objectCount) {
      auto route = index - serviceCount;
      auto metadata = field(objects().getArray()[route], "metadata");
      auto selected = state.durableHome
          ? metadata
          : field(field(field(input(), "rootSelection"), "durableObjectBindings"),
                text(metadata, "binding"));
      descriptor = objectEnvelope(route, text(selected, "objectId"));
    } else {
      auto route = index - serviceCount - objectCount;
      KJ_REQUIRE(route < slot->loopbacks.size(), "invalid loopback channel");
      descriptor = envelope(input(), slot->loopbacks[route].c_str(), false);
    }
    PropsMessage serialized;
    KJ_IF_SOME(value, props) {
      serialized = writeNativeProps(state, value);
    }
    return newPromisedWorkerInterface(
        queuedRequest(false, kj::mv(descriptor), kj::mv(serialized)).attach(kj::addRef(*this)));
  }
  struct Bound final: SubrequestChannel {
    Bound(kj::Rc<Channels> owner,
        uint channel,
        kj::Maybe<kj::String> object,
        kj::Maybe<Frankenvalue> props = kj::none)
        : owner(kj::mv(owner)),
          channel(channel),
          object(kj::mv(object)),
          props(kj::mv(props)) {}
    kj::Own<WorkerInterface> startRequest(SubrequestMetadata metadata) override {
      auto current = owner.addRef();
      // Serialized facet props retain their binding selections. Calls from the same
      // durable Worker use the event's new lease, even if the facet stayed resident.
      KJ_IF_SOME(context, IoContext::tryCurrent()) {
        auto* active = dynamic_cast<Channels*>(&context.getIoChannelFactory());
        if (active != nullptr && active->state.durableHome != 0 && active->slot == owner->slot) {
          current = kj::addRef(*active);
        }
      }
      KJ_IF_SOME(id, object) {
        return newPromisedWorkerInterface(
            current->queuedRequest(false, current->objectEnvelope(channel, id)).attach(current.addRef()));
      }
      return current->startRequest(
          channel, kj::mv(metadata), props.map([](Frankenvalue &value) { return value.clone(); }));
    }
    void requireAllowsTransfer() override {}
    kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
        ChannelTokenUsage) override {
      unsupported("binding persistence");
    }
    kj::Rc<Channels> owner;
    uint channel;
    kj::Maybe<kj::String> object;
    kj::Maybe<Frankenvalue> props;
  };
  kj::Rc<SubrequestChannel> getSubrequestChannelResolved(
      uint channel, kj::Maybe<Frankenvalue> props, kj::Maybe<VersionRequest>, Persistent) override {
    auto base = IoContext::SPECIAL_SUBREQUEST_CHANNEL_COUNT + count(services()) + count(objects());
    KJ_REQUIRE(props == kj::none || channel >= base, "props require a loopback channel");
    return kj::rc<Bound>(kj::addRef(*this), channel, kj::none, kj::mv(props));
  }
  kj::Rc<ActorClassChannel> getActorClassResolved(
      uint channel, kj::Maybe<Frankenvalue> props, Persistent) override {
    if (state.durableHome != 0) {
      KJ_REQUIRE(channel < slot->loopbackActors.size(), "invalid local durable actor class");
      return kj::rc<LocalActorClass>(slot, slot->loopbackActors[channel].c_str(),
          kj::mv(props).orDefault([] { return Frankenvalue(); }));
    }
    unsupported("actor class transfer");
  }
  capnp::Capability::Client getCapability(uint) override {
    unsupported("capability");
  }
  kj::Own<CacheClient> getCache() override {
    return kj::heap<Cache>();
  }
  TimerChannel &getTimer() override {
    return state.timer;
  }
  kj::Promise<void> writeLogfwdr(
      uint, kj::FunctionParam<void(capnp::AnyPointer::Builder)>) override {
    unsupported("log forwarding binding");
  }
  kj::Rc<ActorChannel> getGlobalActor(uint channel,
      const ActorIdFactory::ActorId &id,
      kj::Maybe<kj::String>,
      ActorGetMode,
      bool,
      ActorRoutingMode,
      SpanParent,
      kj::Maybe<ActorVersion>,
      Persistent) override {
    auto &object = kj::downcast<const ObjectIdFactory::Id>(id);
    auto route = objectRoute(channel);
    JSG_REQUIRE(
        object.space == route.space, TypeError, "ID is not for this Durable Object namespace.");
    return kj::rc<Bound>(kj::addRef(*this), channel, id.toString());
  }
  kj::Rc<ActorChannel> getColoLocalActor(uint, kj::StringPtr, SpanParent) override {
    unsupported("local actor");
  }
  ~Channels() noexcept(false) {
    slot->reservation->releaseLogs(logCharge);
  }
  uint64_t logCharge = 0;
  void log(kj::StringPtr level, kj::StringPtr message) {
    auto row = consoleLogRow(level, message);
    if (!logBudget.accept(row.size())) {
      dropLog(1, row.size());
      return;
    }
    auto charge = row.size() * 3 + 256;
    if (!slot->reservation->retainLogs(charge)) {
      dropLog(1, row.size());
      return;
    }
    logCharge += charge;
    lines.add(kj::mv(row));
    queuedLogBytes += lines.back().size();
    server.logAccepted.fetch_add(1);
    auto peak = server.logPeakQueuedBytes.load();
    while (peak < queuedLogBytes &&
        !server.logPeakQueuedBytes.compare_exchange_weak(peak, queuedLogBytes)) {}
    if (!flushing) {
      flushing = true;
      IoContext::current().addWaitUntil(kj::evalLater(
          [self = kj::addRef(*this)]() mutable { return self->flushLogs().attach(kj::mv(self)); }));
    }
  }

  void dropLog(uint64_t count, uint64_t bytes) {
    server.logDroppedLines.fetch_add(count);
    server.logDroppedBytes.fetch_add(bytes);
  }
  kj::Promise<void> flushLogs() {
    bool success = false;
    KJ_DEFER({
      flushing = false;
      if (!success) {
        server.logFlushFailures.fetch_add(1);
        dropLog(lines.size() + inFlightLogLines, queuedLogBytes + inFlightLogBytes);
        resetAfterFlushFailure();
      }
    });
    while (lines.size() != 0) {
      auto batch = lines.releaseAsArray();
      inFlightLogLines = batch.size();
      inFlightLogBytes = queuedLogBytes;
      queuedLogBytes = 0;
      auto body = kj::str("{\"workerId\":", encode(field(input(), "workerId")),
          ",\"versionId\":", encode(field(input(), "versionId")),
          ",\"invocationId\":", encode(field(input(), "invocationId")), ",\"lines\":[",
          kj::strArray(batch, ","), "]}");
      auto address =
          co_await state.io.provider->getNetwork().parseAddress(server.sourceAddress.c_str());
      auto client = kj::newHttpClient(state.io.provider->getTimer(), *state.headers, *address);
      auto generation = field(input(), "processGeneration");
      auto url = kj::str("/v1/log?processId=", text(input(), "processId"), "&processGeneration=",
          generation.isString() ? kj::str(generation.getString()) : encode(generation));
      kj::HttpHeaders headers(*state.headers);
      headers.set(kj::HttpHeaderId::CONTENT_TYPE, "application/json");
      auto request = client->request(kj::HttpMethod::POST, url, headers, body.size());
      co_await request.body->write(body.asBytes());
      auto response = co_await request.response;
      KJ_REQUIRE(response.statusCode == 200, "tenant log flush failed");
      co_await response.body->readAllBytes(1u << 20);
      inFlightLogLines = inFlightLogBytes = 0;
    }
    success = true;
  }
};

class TenantTracer final: public BaseTracer {
 public:
  explicit TenantTracer(kj::Rc<Channels> channels): channels(kj::mv(channels)) {}
  void addLog(const tracing::InvocationSpanContext &,
      kj::Date,
      LogLevel level,
      kj::String message,
      tracing::LogErrorInfo) override {
    if (message.size() > threaded::InvocationLogBudget::MAX_BYTES) {
      channels->dropLog(1, message.size());
      return;
    }
    JsonDoc parsed(message);
    auto parts = parsed.get();
    auto joined = kj::str("");
    if (parts.isArray()) {
      bool first = true;
      for (auto item: parts.getArray()) {
        joined = kj::str(
            joined, first ? "" : " ", item.isString() ? kj::str(item.getString()) : encode(item));
        first = false;
      }
    } else
      joined = kj::mv(message);
    if (joined.startsWith("[tenant-entry] ")) return;
    kj::StringPtr name = "log";
    switch (level) {
      case LogLevel::DEBUG_:
        name = "debug";
        break;
      case LogLevel::INFO:
        name = "info";
        break;
      case LogLevel::WARN:
        name = "warn";
        break;
      case LogLevel::ERROR:
        name = "error";
        break;
      default:
        break;
    }
    channels->log(name, joined);
  }
  void addSpanOpen(tracing::SpanId, tracing::SpanId, kj::ConstString, kj::Date) override {}
  void addSpanClose(tracing::SpanEndData &&, kj::Maybe<kj::Date>) override {}
  void addSpanUpdate(tracing::SpanId, tracing::SpanUpdate &&) override {}
  void addSpanAttribute(
      const tracing::InvocationSpanContext &, kj::ConstString, tracing::Attribute::Value) override {
  }
  void addException(const tracing::InvocationSpanContext &,
      kj::Date,
      kj::String,
      kj::String,
      kj::Maybe<kj::String>) override {}
  void addSpanException(tracing::SpanId,
      kj::Date,
      kj::Maybe<tracing::Exception::Code>,
      kj::String,
      kj::String,
      kj::Maybe<kj::String>) override {}
  void addDiagnosticChannelEvent(
      const tracing::InvocationSpanContext &, kj::Date, kj::String, kj::Array<kj::byte>) override {}
  void setEventInfo(IoContext::IncomingRequest &, tracing::EventInfo &&) override {}
  void setReturn(kj::Maybe<kj::Date>, kj::Maybe<tracing::FetchResponseInfo>) override {}
  void setOutcome(EventOutcome, kj::Duration, kj::Duration) override {}
  void recordTimestamp(kj::Date value) override {
    completeTime = value;
  }
  void setJsRpcInfo(
      const tracing::InvocationSpanContext &, kj::Date, const kj::ConstString &) override {}
  void markUnused() override {}

 private:
  kj::Rc<Channels> channels;
};

struct PreparedInvocation {
  kj::Own<WorkerInterface> entry;
  kj::Own<InvocationState> invocation;
};

// The node's binding handshake returns a reference to the entrypoint before it
// forwards the application's first method. No tenant constructor or method runs
// for that handshake. The reference uses the library's normal RPC serializer.
class BindingTarget final: public rpc::JsRpcTarget::Server {
 public:
  BindingTarget(std::shared_ptr<Slot> slot, rpc::JsRpcTarget::Client target, kj::String identity)
      : slot(std::move(slot)),
        target(kj::mv(target)),
        identity(kj::mv(identity)) {}

  DispatchCallResult dispatchCall(uint64_t interfaceId,
      uint16_t methodId,
      capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) override {
    auto params = context.getParams();
    if (interfaceId == capnp::typeId<rpc::JsRpcTarget>() && methodId == 0) {
      auto call = params.getAs<rpc::JsRpcTarget::CallParams>();
      if (call.isMethodName() && call.getMethodName() == "coworkerd-bind") {
        return {bind(kj::mv(context)), false, true};
      }
    }
    auto request = target.typelessRequest(interfaceId, methodId, params.targetSize(), {});
    request.set(params);
    return {context.tailCall(kj::mv(request)), false, true};
  }

 private:
  kj::Promise<void> bind(capnp::CallContext<capnp::AnyPointer, capnp::AnyPointer> context) {
    KJ_REQUIRE(!bound, "one binding handshake per invocation");
    bound = true;
    struct External final: jsg::Serializer::ExternalHandler {
      void serializeFunction(
          jsg::Lock &, jsg::Serializer &serializer, v8::Local<v8::Function>) override {
        serializer.writeRawUint32(static_cast<uint>(rpc::SerializationTag::JS_RPC_STUB));
      }
    } external;
    auto results = context.getResults().initAs<rpc::JsRpcTarget::CallResults>();
    auto &worker = *KJ_REQUIRE_NONNULL(slot->worker);
    auto asyncLock = co_await worker.takeAsyncLockWithoutRequest(SpanParent(nullptr));
    OomReportScope oom(identity, slot->limits->memoryMiB,
        slot->limits->memory ? &slot->limits->memory->heap : nullptr);
    worker.runInLockScope(asyncLock, [&](Worker::Lock &lock) {
      FatalInvocation fatal(slot->resetDurableRoot ? nullptr : lock.getIsolate(), identity);
      JSG_WITHIN_CONTEXT_SCOPE(lock, lock.getContext(), [&](jsg::Lock &js) {
        auto placeholder = jsg::check(
            v8::Function::New(js.v8Context(), [](const v8::FunctionCallbackInfo<v8::Value> &) {}));
        jsg::Serializer serializer(js,
            {.version = 15,
              .omitHeader = false,
              .treatClassInstancesAsPlainObjects = false,
              .externalHandler = external});
        serializer.write(js, jsg::JsValue(placeholder));
        results.initResult().setV8Serialized(serializer.release().data);
      });
    });
    results.getResult().initExternals(1)[0].initRpcTarget().setCap(target);
    results.setCallPipeline(target);
    co_return;
  }

  std::shared_ptr<Slot> slot;
  rpc::JsRpcTarget::Client target;
  kj::String identity;
  bool bound = false;
};

void startBoundRpcSession(rpc::EventDispatcher::JsRpcSessionResults::Builder results,
    kj::Own<WorkerInterface> entry,
    std::shared_ptr<Slot> slot,
    kj::String identity) {
  auto event = kj::heap<api::JsRpcSessionCustomEvent>(
      api::JsRpcSessionCustomEvent::WORKER_RPC_EVENT_TYPE, kj::none);
  auto target = event->getCap();
  auto completed = entry->customEvent(kj::mv(event));
  results.setTopLevel(kj::heap<BindingTarget>(std::move(slot), kj::mv(target), kj::mv(identity)));
  results.setSession(
      completed.then([entry = kj::mv(entry)](auto) { return rpc::JsRpcSession::Client(nullptr); }));
}

kj::Promise<void> ChannelDispatcher::jsRpcSession(JsRpcSessionContext context) {
  KJ_REQUIRE(worker.get() != nullptr, "one event per channel invocation");
  KJ_REQUIRE(alarm == kj::none, "alarm event requires the alarm dispatcher");
  if (bindingSlot) {
    startBoundRpcSession(
        context.getResults(), kj::mv(worker), std::move(bindingSlot), kj::mv(bindingIdentity));
    return kj::READY_NOW;
  }
  return api::JsRpcSessionCustomEvent::receiveRpc(kj::mv(context), kj::mv(worker));
}

class HttpEntryObserver final: public RequestObserver {
 public:
  HttpEntryObserver(kj::String fields, int output): fields(kj::mv(fields)), output(output) {}
  void delivered() override {
    auto row = kj::str("[tenant-entry] {", fields,
        ",\"observedAtUnixMs\":", nowNs(CLOCK_REALTIME) / 1000000, "}\n");
    (void)!write(output, row.begin(), row.size());
  }

 private:
  kj::String fields;
  int output;
};

kj::Own<RequestObserver> httpEntryObserver(Json::Reader input,
    kj::StringPtr entrypoint,
    const kj::HttpHeaders &headers,
    kj::HttpHeaderId callIdHeader,
    int output = STDERR_FILENO) {
  // Placement probes ask for process-side delivery evidence. Ordinary requests
  // do not format or write this diagnostic. The node cleans child output with
  // the same known-secret dictionary as every other platform log stream.
  KJ_IF_SOME(callId, headers.get(callIdHeader)) {
    if (callId.size() > 128) return kj::refcounted<RequestObserver>();
    auto fields = kj::str("\"serverId\":", encode(field(input, "serverId")));
    for (auto name: {"processId", "processGeneration", "invocationId", "treeId"}) {
      auto value = field(input, name);
      fields = kj::str(fields, ",\"", name, "\":",
          kj::StringPtr(name) == "processGeneration" && value.isString()
              ? kj::str(value.getString())
              : encode(value));
    }
    capnp::MallocMessageBuilder arena;
    auto value = arena.initRoot<Json>();
    value.setString(kj::str(text(input, "workerId"), "@", text(input, "versionId")));
    fields = kj::str(fields, ",\"target\":", encode(value));
    value.setString(entrypoint.size() ? entrypoint : "default");
    fields = kj::str(fields, ",\"entrypoint\":", encode(value));
    value.setString(callId);
    fields = kj::str(fields, ",\"callId\":", encode(value));
    return kj::refcounted<HttpEntryObserver>(kj::mv(fields), output);
  }
  return kj::refcounted<RequestObserver>();
}

PreparedInvocation prepareInvocation(ThreadState &state,
    ThreadedServer &server,
    std::shared_ptr<Slot> slot,
    Json::Reader input,
    kj::StringPtr raw,
    BudgetLifetime budgetLifetime,
    Frankenvalue props,
    std::shared_ptr<ResourcePreparation> preparation,
    std::shared_ptr<TenantPermit> tenant,
    kj::Own<RequestObserver> observer = kj::refcounted<RequestObserver>(),
    kj::Maybe<kj::Own<Worker::Actor>> actor = kj::none,
    kj::Maybe<kj::StringPtr> localActorClass = kj::none,
    std::shared_ptr<SqlExecutionBudget> sqlDeadline = {}) {
  if (!preparation) preparation = server.prepareSlot(slot);
  auto invocation = kj::refcounted<InvocationState>(slot, std::move(tenant));
  invocation->measurement.preparation = std::move(preparation);
  auto worker = kj::atomicAddRef(*KJ_REQUIRE_NONNULL(slot->worker));
  auto cf = kj::str("{\"nodeId\":", encode(field(input, "serverId")),
      ",\"serverId\":", encode(field(input, "serverId")));
  for (auto name: {"processId", "workerId", "versionId", "contentHash", "invocationId", "treeId",
         "ingressServerId", "lease", "orderSequence", "depth"})
    if (kj::StringPtr(name) == "lease") {
      auto lease = threaded::rawPath(std::string_view(raw.begin(), raw.size()), {"lease"});
      cf = kj::str(cf, ",\"lease\":", kj::heapString(lease.data(), lease.size()));
    } else
      cf = kj::str(cf, ",\"", name, "\":", encode(field(input, name)));
  auto generation = field(input, "processGeneration");
  cf = kj::str(cf, ",\"processGeneration\":",
      generation.isString() ? kj::str(generation.getString()) : encode(generation), "}");
  auto entrypoint = threaded::normalizeEntrypoint(
      str(input, "format"), field(input, "entrypoint").isString() ? str(input, "entrypoint") : "");
  KJ_IF_SOME(name, localActorClass) {
    entrypoint.assign(name.begin(), name.size());
  }
  auto channels = kj::rc<Channels>(state, server, slot, raw);
  channels->budgetLifetime = budgetLifetime;
  auto tracer = kj::rc<TenantTracer>(channels.addRef());
  auto identity = kj::str("\"workerId\":", encode(field(input, "workerId")),
      ",\"versionId\":", encode(field(input, "versionId")),
      ",\"invocationId\":", encode(field(input, "invocationId")), ",\"isolate\":\"",
      slot->isolateLabel.c_str(), "\",");
  if (slot->resetDurableRoot) {
    auto event = std::make_shared<DurableEventCpu>(kj::str(identity), slot->limits->memoryMiB);
    event->sqlDeadline = std::move(sqlDeadline);
    observer = kj::refcounted<DurableEventObserver>(std::move(event));
  }
  observer = kj::mv(observer).attachToThisReference(std::move(budgetLifetime));
  auto limits =
      kj::heap<RequestLimits>(*slot, invocation->measurement, state.timer, kj::mv(identity));
  if (slot->resetDurableRoot) {
    // Actors retain their IoContext after the first entrypoint's reference (and its
    // ioContextDependency attachment) is gone. Keep the referenced measurement and
    // slot alive until the context destroys its limit enforcer.
    limits = kj::mv(limits).attach(kj::addRef(*invocation));
  }
  auto entry = newWorkerEntrypoint(state.context, kj::mv(worker),
      entrypoint.empty() || entrypoint == "default"
          ? kj::Maybe<kj::String>(kj::none)
          : kj::Maybe<kj::String>(kj::str(entrypoint.c_str())),
      kj::mv(props), kj::mv(actor), kj::mv(limits), kj::addRef(*invocation), kj::mv(channels),
      kj::mv(observer), state.tasks, true, kj::mv(tracer), kj::mv(cf), kj::none);
  return {kj::mv(entry), kj::mv(invocation)};
}

struct DurableTreeContext {
  struct SocketManager {
    Worker::Actor* actor;
    LegacyHibernationManagerImpl* manager;
  };

  ThreadState &state;
  ThreadedServer &server;
  std::weak_ptr<Slot> slot;
  uint64_t tree;
  kj::String uniqueKey, storageId, objectName;
  kj::Own<server::FacetTreeIndex> index;
  std::shared_ptr<server::DurableIdleLifecycle> idle;
  std::map<uint, server::DurableLogShipper*> shippers;
  std::map<uint, SqliteDatabase *> sqliteConnections;
  uint liveFacets = 0;
  std::shared_ptr<DurableNodeSockets> nodeSockets;
  std::map<uint, SocketManager> socketManagers;
  std::set<std::string, std::less<>> socketUpgradeInvocations;
  std::map<uint, PropsMessage> facetStarts;
  kj::Maybe<DurableSocketAdoption> adoption;
  std::map<uint, std::vector<threaded::rpc::ThreadedSocketSnapshot::Reader>> dormantSockets;

  void installSocketManager(Worker::Actor &actor, uint facet);
  void restoreSockets(DurableSocketAdoption response);
  kj::Promise<SocketParkResult> parkSockets();
  void forgetFacetSockets(uint facet);
  void abortFacetSockets(uint facet, kj::Vector<kj::String> &ids);
  void requireRestorableUpgrade(uint facet);

  const server::DurableStorage &storage() {
    return *server.durableStorage;
  }

  server::FacetTreeIndex &facetIndex() {
    if (index.get() == nullptr) index = storage().openFacetIndex(uniqueKey, storageId);
    return *index;
  }
};

void trimIdleObjectSqlite(DurableTreeContext &tree) {
  for (auto &[facet, connection]: tree.sqliteConnections) connection->trimIdleObjectSqlHeap();
}

bool hasDurableWalRegistrations(const DurableTreeContext& tree) {
  return !tree.shippers.empty();
}

void beginDurableUpgrade(DurableTreeContext &tree, kj::StringPtr invocation) {
  KJ_ASSERT(tree.socketUpgradeInvocations.emplace(invocation.begin(), invocation.end()).second);
}

void finishDurableUpgrade(DurableTreeContext &tree, kj::StringPtr invocation) {
  auto at = tree.socketUpgradeInvocations.find(std::string_view(invocation.begin(), invocation.size()));
  KJ_ASSERT(at != tree.socketUpgradeInvocations.end());
  tree.socketUpgradeInvocations.erase(at);
  for (const auto &[_, socket]: tree.socketManagers) {
    socket.manager->discardStagedNodeSockets(invocation);
  }
}

kj::Maybe<StagedDurableUpgrade> findDurableUpgrade(
    DurableTreeContext &tree, kj::StringPtr token, kj::StringPtr invocation) {
  for (const auto &[facet, socket]: tree.socketManagers) {
    if (socket.manager->hasStagedNodeSocket(token, invocation)) {
      return StagedDurableUpgrade{facet, kj::addRef(*socket.manager)};
    }
  }
  return kj::none;
}

kj::StringPtr currentUpgradeInvocation() {
  auto& channels = kj::downcast<Channels>(IoContext::current().getIoChannelFactory());
  return field(channels.input(), "invocationId").getString();
}

LegacyHibernationManagerImpl::SocketSnapshot readSocketSnapshot(
    threaded::rpc::ThreadedSocketSnapshot::Reader entry) {
  LegacyHibernationManagerImpl::SocketSnapshot snapshot{
    .id = kj::str(entry.getAuthority().getSocketId()),
    .tags = KJ_MAP(tag, entry.getTags()) { return kj::str(tag); },
    .lastFrame = entry.getLastFrame(),
    .closedOutgoing = entry.getClosedOutgoing(),
  };
  if (entry.getHasAttachment()) {
    auto attachment = entry.getAttachment();
    snapshot.attachment =
        kj::heapArray<kj::byte>(kj::arrayPtr(attachment.begin(), attachment.size()));
  }
  if (const auto time = entry.getAutoResponseUnixMs()) {
    KJ_REQUIRE(time <= INT64_MAX / 1000000);
    snapshot.lastAutoResponse = kj::UNIX_EPOCH + static_cast<int64_t>(time) * kj::MILLISECONDS;
  }
  return snapshot;
}

void DurableTreeContext::installSocketManager(Worker::Actor &actor, uint facet) {
  if (!nodeSockets) return;
  auto manager = kj::refcounted<LegacyHibernationManagerImpl>(actor.getLoopback(), 8,
      LegacyHibernationManagerImpl::SocketFactory(
          [sockets = nodeSockets] { return kj::heap<DurableNodeWebSocket>(sockets); }));
  // The manager's loopback owns this tree even when a request releases its actor.
  manager->setNodeUpgradeHooks([this, facet] {
    const auto invocation = currentUpgradeInvocation();
    JSG_REQUIRE(socketUpgradeInvocations.contains(std::string_view(invocation.begin(), invocation.size())),
        DOMInvalidStateError, "The Durable Object WebSocket must be returned from its upgrade request.");
    requireRestorableUpgrade(facet);
    return invocation;
  }, [this](kj::StringPtr pending) {
    return findDurableUpgrade(*this, pending, currentUpgradeInvocation()) != kj::none;
  });
  if (auto dormant = dormantSockets.find(facet); dormant != dormantSockets.end()) {
    const auto first = dormant->second.front();
    if (first.getAutoResponseEnabled()) {
      manager->setWebSocketAutoResponse(first.getAutoRequest(), first.getAutoResponse());
    }
    for (const auto socket: dormant->second) {
      manager->adoptNodeSocket(readSocketSnapshot(socket), socket.getAuthority().getInvocationId());
    }
    dormantSockets.erase(dormant);
    if (dormantSockets.empty()) adoption = kj::none;
  }
  KJ_ASSERT(socketManagers.emplace(facet, SocketManager{&actor, manager.get()}).second);
  actor.setHibernationManager(kj::mv(manager));
}

void DurableTreeContext::requireRestorableUpgrade(uint facet) {
  if (facet == 0) return;
  std::set<uint> counted;
  uint64_t total = 0;
  auto count = [&](uint id) {
    for (; id != 0 && counted.insert(id).second;) {
      auto at = facetStarts.find(id);
      JSG_REQUIRE(at != facetStarts.end(), DOMNotSupportedError,
          "The facet's startup options cannot be restored after WebSocket hibernation. "
          "A facet's options must be serializable and use at most 64 KiB.");
      auto start = at->second->getRoot<threaded::rpc::ThreadedSocketFacet>().asReader();
      total += start.totalSize().wordCount;
      JSG_REQUIRE(total <= threaded::rpc::THREADED_SOCKET_TREE_MAX_WORDS, DOMNotSupportedError,
          "The Durable Object tree's WebSocket facet startup options exceed 1 MiB.");
      id = start.getParent();
    }
  };
  count(facet);
  for (const auto &[id, socket]: socketManagers) {
    if (id != 0 && socket.manager->hasNodeUpgrades()) count(id);
  }
  for (const auto &[id, _]: dormantSockets) count(id);
}

void DurableTreeContext::restoreSockets(DurableSocketAdoption response) {
  for (const auto facet: response.getFacets()) {
    auto start = kj::heap<capnp::MallocMessageBuilder>();
    start->setRoot(facet);
    KJ_ASSERT(facetStarts.emplace(facet.getId(), kj::mv(start)).second);
  }
  for (const auto socket: response.getSockets()) {
    dormantSockets[socket.getFacetId()].push_back(socket);
  }
  if (!dormantSockets.empty()) adoption = kj::mv(response);
}

kj::Promise<SocketParkResult> DurableTreeContext::parkSockets() {
  kj::Vector<DurableSocketSnapshot> sockets;
  bool release = true;
  KJ_DEFER(if (release) {
    for (const auto &[_, socket]: socketManagers) socket.manager->setSocketParking(false);
  });
  for (const auto &[facet, socket]: socketManagers) {
    socket.manager->setSocketParking(true);
    for (auto &snapshot: socket.manager->snapshotNodeSockets()) {
      sockets.add(DurableSocketSnapshot{facet, kj::mv(snapshot)});
    }
  }
  for (const auto &[facet, entries]: dormantSockets) {
    for (const auto entry: entries) {
      sockets.add(DurableSocketSnapshot{facet, readSocketSnapshot(entry)});
    }
  }
  std::map<uint, threaded::rpc::ThreadedSocketFacet::Reader> starts;
  for (const auto &entry: sockets) {
    for (auto id = entry.facet; id != 0;) {
      auto start = facetStarts.at(id)->getRoot<threaded::rpc::ThreadedSocketFacet>().asReader();
      if (!starts.emplace(id, start).second) break;
      id = start.getParent();
    }
  }
  capnp::MallocMessageBuilder message;
  auto facets = message.initRoot<DurableFacetStarts>(starts.size());
  size_t index = 0;
  for (const auto &[_, start]: starts) facets.setWithCaveats(index++, start);
  const auto result = co_await nodeSockets->park(sockets.releaseAsArray(), facets.asReader());
  release = result == SocketParkResult::REFUSED;
  co_return result;
}

void DurableTreeContext::forgetFacetSockets(uint facet) {
  if (auto at = socketManagers.find(facet); at != socketManagers.end()) {
    at->second.manager->discardStagedNodeSockets();
    socketManagers.erase(at);
  }
  facetStarts.erase(facet);
  dormantSockets.erase(facet);
  if (dormantSockets.empty()) adoption = kj::none;
}

void DurableTreeContext::abortFacetSockets(uint facet, kj::Vector<kj::String> &ids) {
  facetIndex().forEachChild(
      facet, [&](uint child, kj::StringPtr) { abortFacetSockets(child, ids); });
  if (auto manager = socketManagers.find(facet); manager != socketManagers.end()) {
    for (auto &socket: manager->second.manager->snapshotNodeSockets()) ids.add(kj::mv(socket.id));
  }
  if (auto dormant = dormantSockets.find(facet); dormant != dormantSockets.end()) {
    for (const auto socket: dormant->second) ids.add(kj::str(socket.getAuthority().getSocketId()));
  }
  forgetFacetSockets(facet);
}

struct FacetPropChannel {
  uint channel;
  kj::Maybe<kj::StringPtr> object;
  kj::Maybe<Frankenvalue&> props;
};

bool writeFacetProps(Frankenvalue &value, rpc::Frankenvalue::Builder out, size_t &remaining,
    kj::FunctionParam<kj::Maybe<FacetPropChannel>(Frankenvalue::CapTableEntry&)> describe) {
  auto caps = value.getCapTable();
  const auto bytes = value.estimateSize();
  if (bytes > remaining) return false;
  remaining -= bytes;
  // Every channel adds data and pointer words even if it has no ID or own props.
  constexpr size_t CHANNEL_BYTES = 3 * sizeof(capnp::word);
  if (caps.size() > remaining / CHANNEL_BYTES) return false;
  remaining -= caps.size() * CHANNEL_BYTES;
  value.toCapnp(out);
  if (caps.size() == 0) return true;
  auto table =
      out.getCapTable().initAs<capnp::List<threaded::rpc::ThreadedSocketPropChannel>>(caps.size());
  for (auto i: kj::indices(caps)) {
    auto saved = describe(*caps[i]);
    if (saved == kj::none) return false;
    const auto& bound = KJ_ASSERT_NONNULL(saved);
    auto channel = table[i];
    channel.setChannel(bound.channel);
    KJ_IF_SOME(object, bound.object) {
      if (object.size() > remaining) return false;
      remaining -= object.size();
      channel.setHasObject(true);
      channel.setObjectId(object);
    }
    KJ_IF_SOME(props, bound.props) {
      if (!writeFacetProps(props, channel.initProps(), remaining, describe)) return false;
    }
  }
  return true;
}

Frankenvalue readFacetProps(rpc::Frankenvalue::Reader value,
    kj::FunctionParam<kj::Own<Frankenvalue::CapTableEntry>(uint, kj::Maybe<kj::String>,
        kj::Maybe<Frankenvalue>)> bind) {
  kj::Vector<kj::Own<Frankenvalue::CapTableEntry>> caps;
  for (auto channel:
      value.getCapTable().getAs<capnp::List<threaded::rpc::ThreadedSocketPropChannel>>()) {
    kj::Maybe<kj::String> object;
    if (channel.getHasObject()) object = kj::str(channel.getObjectId());
    kj::Maybe<Frankenvalue> props;
    if (channel.hasProps()) props = readFacetProps(channel.getProps(), bind);
    caps.add(bind(channel.getChannel(), kj::mv(object), kj::mv(props)));
  }
  return Frankenvalue::fromCapnp(value, kj::mv(caps));
}

class DurablePendingOperations final: public Worker::Actor::WaitUntilTaskTracker {
 public:
  explicit DurablePendingOperations(std::shared_ptr<server::DurableIdleLifecycle> idle)
      : idle(std::move(idle)) {}

  kj::Own<Worker::Actor::WaitUntilTaskHandle> registerTask() override {
    return kj::heap<PendingOperation>(idle);
  }
  kj::Own<void> registerPendingIo() override {
    return kj::heap<PendingOperation>(idle);
  }
  bool tracksSocketTransportSeparately() const override {
    return true;
  }

 private:
  class PendingOperation final: public Worker::Actor::WaitUntilTaskHandle {
   public:
    explicit PendingOperation(std::shared_ptr<server::DurableIdleLifecycle> idle)
        : idle(std::move(idle)),
          position(this->idle->pendingStarted(durableIdleNow())) {}
    ~PendingOperation() noexcept override {
      idle->pendingFinished(position, durableIdleNow());
    }

   private:
    std::shared_ptr<server::DurableIdleLifecycle> idle;
    server::DurableIdleLifecycle::Pending position;
  };
  std::shared_ptr<server::DurableIdleLifecycle> idle;
};

class DurableFacetReservation {
 public:
  explicit DurableFacetReservation(std::shared_ptr<DurableTreeContext> owner)
      : owner(std::move(owner)),
        tree(*this->owner) {
    std::lock_guard lock(tree.server.durableRoutingMutex);
    if (tree.liveFacets >= 64 || !tree.server.durableAdmission->reserveAuxiliary(cost)) {
      kj::throwRecoverableException(capacityError());
    }
    ++tree.liveFacets;
  }

  ~DurableFacetReservation() {
    std::lock_guard lock(tree.server.durableRoutingMutex);
    --tree.liveFacets;
    tree.server.durableAdmission->releaseAuxiliary(cost);
  }

  KJ_DISALLOW_COPY_AND_MOVE(DurableFacetReservation);

 private:
  static constexpr server::DurableAdmission::Resources cost{
    .descriptors = 3,
    .memory = 16 * 1024,
  };
  const std::shared_ptr<DurableTreeContext> owner;
  DurableTreeContext &tree;
};

class DurableActorObserver final: public ActorObserver {
 public:
  DurableActorObserver(std::shared_ptr<DurableTreeContext> tree, uint facet)
      : tree(std::move(tree)),
        id(++this->tree->server.nextDurableActor),
        tid(syscall(SYS_gettid)),
        facet(facet) {}
  ~DurableActorObserver() noexcept(false) {
    if (actor != nullptr) {
      record("destroyed");
      tree->state.durableActorTrees.erase(actor);
      auto at = tree->socketManagers.find(facet);
      if (at != tree->socketManagers.end() && at->second.actor == actor) {
        tree->socketManagers.erase(at);
      }
    }
  }

  void bind(Worker::Actor &value) {
    actor = &value;
    tree->state.durableActorTrees.emplace(actor, tree->tree);
    record("created");
  }

  void startRequest() override {
    record("delivered");
  }
  void constructorCompleted() override {
    record("executed");
  }

 private:
  void record(const char *phase) {
    const auto current = syscall(SYS_gettid);
    KJ_ASSERT(tid == current, "durable actor left its home thread");
    if (traceForTest()) {
      auto row = kj::str("[threaded-durable-actor] {\"actor\":", id, ",\"tree\":", tree->tree,
          ",\"home\":", tree->state.durableHome, ",\"facet\":", facet, ",\"tid\":", current,
          ",\"phase\":\"", phase, "\"}\n");
      (void)!write(STDERR_FILENO, row.begin(), row.size());
    }
  }

  std::shared_ptr<DurableTreeContext> tree;
  uint64_t id;
  long tid;
  uint facet;
  Worker::Actor *actor = nullptr;
};

struct DurableActorHandle {
  kj::Own<Worker::Actor> value;
  DurableActorHooks *hooks;
};

class DurableAlarmReporter final: public ActorSqlite::Hooks {
 public:
  explicit DurableAlarmReporter(server::DurableLogShipper& shipper, uint facet)
      : shipper(shipper), facet(facet) {}

  kj::Promise<void> scheduleRun(kj::Maybe<kj::Date>, kj::Promise<void>) override {
    JSG_FAIL_REQUIRE(Error, "Facets currently cannot set alarms.");
  }
  bool hasConfirmedAlarmScheduler() const override {
    return facet == 0;
  }
  void alarmConfirmed(kj::Maybe<kj::Date> time) override {
    shipper.reportAlarm(time);
  }

 private:
  server::DurableLogShipper& shipper;
  uint facet;
};

kj::Exception tagWalOpenError(kj::Exception error) {
  if (error.getDetail(server::ThreadedWalLog::CLOSE_UNCONFIRMED_DETAIL) != kj::none) {
    return jsg::tagCoworkerdRpcError(kj::mv(error), "DO_UNAVAILABLE");
  }
  return error;
}

kj::Promise<server::ThreadedWalLog::PreparedOpen> prepareDurableWal(server::ThreadedWalLog &log,
    server::DurableStorage::WalImage image,
    uint64_t tree,
    uint facet) {
  return log.prepareOpen(image.directory, kj::mv(image.path), kj::mv(image.storageKey), tree, facet)
      .catch_([](kj::Exception &&error) -> kj::Promise<server::ThreadedWalLog::PreparedOpen> {
    return tagWalOpenError(kj::mv(error));
  });
}

DurableActorHandle makeDurableActor(const std::shared_ptr<DurableTreeContext> &tree,
    Worker &worker,
    Worker::Actor::Id id,
    kj::StringPtr className,
    Frankenvalue props,
    uint facet,
    uint depth,
    std::shared_ptr<DurableFacetReservation> reservation = nullptr,
    uint parentFacet = 0,
    kj::StringPtr facetName = ""_kj,
    kj::Maybe<server::ThreadedWalLog::PreparedOpen> prepared = kj::none,
    bool platformDatabase = false,
    uint64_t sqlWalFrameLimitBytes = 0);

class LocalFacet final: public IoChannelFactory::ActorChannel {
 public:
  using StartInfo = Worker::Actor::FacetManager::StartInfo;

  LocalFacet(std::shared_ptr<DurableTreeContext> tree,
      uint facet,
      uint depth,
      uint parentFacet,
      kj::StringPtr name,
      kj::Function<kj::Promise<StartInfo>()> getStartInfo,
      std::shared_ptr<DurableFacetReservation> reservation)
      : tree(std::move(tree)),
        reservation(std::move(reservation)),
        facet(facet),
        depth(depth),
        parentFacet(parentFacet),
        name(kj::str(name)),
        getStartInfo(kj::mv(getStartInfo)) {}

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata) override;
  void requireAllowsTransfer() override {
    unsupported("durable facet transfer");
  }
  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage) override {
    unsupported("durable facet persistence");
  }

  bool busy(bool includeBackground = true);
  bool hasOrdinaryWebSocket();
  bool isStopped() const {
    return stopped;
  }
  uint getFacetId() const {
    return facet;
  }
  void stop(const kj::Exception &reason);
  kj::Promise<kj::Rc<LocalFacet>> openSocketFacet(uint id, kj::StringPtr descriptor);
  kj::Promise<std::pair<kj::Own<WorkerInterface>, kj::Own<Worker::Actor>>> openSocket(
      kj::String descriptor, BudgetLifetime budgetLifetime);

 private:
  struct Invocation {
    explicit Invocation(LocalFacet &owner): owner(kj::addRef(owner)) {
      ++owner.running;
    }
    ~Invocation() {
      --owner->running;
    }
    kj::Own<LocalFacet> owner;
  };

  kj::Promise<void> initialize(kj::String descriptor);
  kj::Promise<void> ensureStarted(kj::StringPtr descriptor);
  kj::Promise<kj::Own<WorkerInterface>> open(kj::String descriptor, kj::Own<Invocation> invocation,
      BudgetLifetime budgetLifetime);

  std::shared_ptr<DurableTreeContext> tree;
  std::shared_ptr<DurableFacetReservation> reservation;
  uint facet;
  uint depth;
  uint parentFacet;
  kj::String name;
  kj::Function<kj::Promise<StartInfo>()> getStartInfo;
  kj::Maybe<kj::ForkedPromise<void>> starting;
  kj::Maybe<DurableActorHandle> actor;
  kj::String className;
  uint running = 0;
  bool stopped = false;
};

class DurableActorHooks final: public Worker::Actor::Loopback,
                               public Worker::Actor::FacetManager,
                               public kj::Refcounted {
 public:
  DurableActorHooks(std::shared_ptr<DurableTreeContext> tree, uint facet, uint depth)
      : tree(std::move(tree)),
        facet(facet),
        depth(depth) {}

  kj::Own<WorkerInterface> getWorker(IoChannelFactory::SubrequestMetadata) override {
    unsupported("local hibernatable WebSocketPair events on the threaded Durable Object host");
  }
  kj::Own<Worker::Actor::Loopback> addRef() override {
    return kj::addRef(*this);
  }
  uint getDepth() const override {
    return depth;
  }

  kj::Rc<IoChannelFactory::ActorChannel> getFacet(
      kj::StringPtr name, kj::Function<kj::Promise<StartInfo>()> getStartInfo) override {
    return getLocalFacet(name, kj::mv(getStartInfo));
  }

  kj::Rc<LocalFacet> getLocalFacet(
      kj::StringPtr name, kj::Function<kj::Promise<StartInfo>()> getStartInfo) {
    const std::string key(name.begin(), name.size());
    auto at = children.find(key);
    if (at != children.end()) {
      if (!at->second->isStopped()) return at->second.addRef();
      if (at->second->busy()) kj::throwRecoverableException(capacityError());
      children.erase(at);
    }
    auto reservation = std::make_shared<DurableFacetReservation>(tree);
    const auto id = tree->facetIndex().getId(facet, name);
    auto child =
        kj::rc<LocalFacet>(tree, id, depth + 1, facet, name, kj::mv(getStartInfo), std::move(reservation));
    children.emplace(key, child.addRef());
    return child;
  }

  kj::Promise<kj::Rc<LocalFacet>> getSocketFacet(uint id, kj::StringPtr descriptor) {
    auto at = tree->facetStarts.find(id);
    KJ_REQUIRE(at != tree->facetStarts.end(), "the WebSocket's facet is no longer available");
    auto start = at->second->getRoot<threaded::rpc::ThreadedSocketFacet>();
    const auto parent = start.getParent();
    if (parent == facet) {
      co_return getLocalFacet(start.getName(), []() -> kj::Promise<StartInfo> {
        KJ_FAIL_REQUIRE("the WebSocket's facet startup options are no longer available");
      });
    }
    auto ancestor = co_await getSocketFacet(parent, descriptor);
    co_return co_await ancestor->openSocketFacet(id, descriptor);
  }

  void abortFacet(kj::StringPtr name, kj::Exception reason) override {
    if (tree->nodeSockets) {
      uint target = 0;
      auto at = children.find(std::string(name.begin(), name.size()));
      if (at != children.end()) {
        target = at->second->getFacetId();
      } else {
        for (auto &[id, stored]: tree->facetStarts) {
          auto start = stored->getRoot<threaded::rpc::ThreadedSocketFacet>();
          if (start.getParent() == facet && start.getName() == name) {
            target = id;
            break;
          }
        }
      }
      if (target != 0) {
        kj::Vector<kj::String> ids;
        tree->abortFacetSockets(target, ids);
        if (!ids.empty()) {
          auto sockets = tree->nodeSockets;
          tree->state.tasks.add(
              sockets->closeAborted(ids.releaseAsArray()).attach(std::move(sockets)));
        }
      }
    }
    stopFacet(name, reason);
  }

  void stopFacet(kj::StringPtr name, const kj::Exception &reason) {
    auto at = children.find(std::string(name.begin(), name.size()));
    if (at == children.end()) return;
    at->second->stop(reason);
    if (!at->second->busy()) children.erase(at);
  }

  void deleteFacet(kj::StringPtr name) override {
    requireIdle(name);
    stopFacet(name, KJ_EXCEPTION(DISCONNECTED, "Facet was deleted."));
    auto &index = tree->facetIndex();
    deleteStoredFacet(index, index.getId(facet, name));
  }

  void cloneFacet(kj::StringPtr source, kj::StringPtr destination) override {
    if (tree->state.wal) unsupported("cloning a persistent Durable Object facet");
    requireIdle(source);
    requireIdle(destination);
    abortFacet(destination, KJ_EXCEPTION(DISCONNECTED, "Facet was cloned over."));
    if (source == destination) return;
    auto &index = tree->facetIndex();
    const auto from = index.getId(facet, source);
    const auto to = index.getId(facet, destination);
    deleteStoredFacet(index, to);
    tree->storage().cloneFacet(tree->uniqueKey, tree->storageId, index, from, to);
  }

  bool busy(bool includeBackground = true) {
    const auto shipper = tree->shippers.find(facet);
    if (shipper != tree->shippers.end() && shipper->second->hasPendingWrites()) return true;
    for (auto &[name, child]: children) {
      if (child->busy(includeBackground)) return true;
    }
    return false;
  }

  bool hasOrdinaryWebSocket() {
    for (auto& [name, child]: children) {
      if (child->hasOrdinaryWebSocket()) return true;
    }
    return false;
  }

  void stop() {
    const auto reason = KJ_EXCEPTION(DISCONNECTED, "Durable test tree was closed.");
    stop(reason);
  }

  void stop(const kj::Exception &reason) {
    for (auto at = children.begin(); at != children.end();) {
      auto current = at++;
      current->second->stop(reason);
      if (!current->second->busy()) children.erase(current);
    }
  }

  void resetStorage(SqliteDatabase& database, server::DurableLogShipper& shipper) {
    for (auto& [name, child]: children) {
      KJ_REQUIRE(!child->busy(), "cannot reset storage with a live descendant facet");
    }
    stop();
    database.run("PRAGMA journal_mode=WAL;");
    shipper.afterReset(database);
    auto& index = tree->facetIndex();
    index.forEachChild(facet, [&](uint child, kj::StringPtr) { deleteStoredFacet(index, child); });
  }

 private:
  void deleteStoredFacet(server::FacetTreeIndex& index, uint target) {
    kj::Function<void(uint)> report = [&](uint deleted) {
      tree->forgetFacetSockets(deleted);
      auto shipper = tree->shippers.find(facet);
      if (shipper != tree->shippers.end()) shipper->second->reportFacetDeleted(deleted);
    };
    tree->storage().deleteFacet(tree->uniqueKey, tree->storageId, index, target, report);
  }

  void requireIdle(kj::StringPtr name) {
    auto at = children.find(std::string(name.begin(), name.size()));
    KJ_REQUIRE(at == children.end() || !at->second->busy(), "durable test facet is busy");
  }

  std::shared_ptr<DurableTreeContext> tree;
  uint facet;
  uint depth;
  std::map<std::string, kj::Rc<LocalFacet>> children;
};

bool LocalFacet::busy(bool includeBackground) {
  if (running != 0) return true;
  KJ_IF_SOME(opened, actor) {
    KJ_IF_SOME(context, opened.value->getIoContext()) {
      if (includeBackground && context.hasCurrentIncomingRequest()) return true;
    }
    return opened.hooks->busy(includeBackground);
  }
  return false;
}

bool LocalFacet::hasOrdinaryWebSocket() {
  if (stopped) return false;
  KJ_IF_SOME(opened, actor) {
    KJ_IF_SOME(manager, opened.value->getHibernationManager()) {
      if (tree->nodeSockets) {
        if (kj::downcast<LegacyHibernationManagerImpl>(manager).hasLocalSockets()) return true;
      } else if (manager.getWebSocketCount() != 0) {
        return true;
      }
    }
    return opened.value->getNonHibernatableWebSocketCount()->count > 0 ||
        opened.hooks->hasOrdinaryWebSocket();
  }
  return false;
}

void LocalFacet::stop(const kj::Exception &reason) {
  const auto wasStopped = std::exchange(stopped, true);
  KJ_IF_SOME(opened, actor) {
    opened.hooks->stop(reason);
    if (!wasStopped) opened.value->abort(reason.clone());
    if (!busy()) {
      opened.value->shutdown(0);
      actor = kj::none;
    }
  }
}

kj::Own<WorkerInterface> LocalFacet::startRequest(IoChannelFactory::SubrequestMetadata) {
  KJ_REQUIRE(!stopped, "durable test facet is closed");
  auto &channels =
      KJ_ASSERT_NONNULL(dynamic_cast<Channels *>(&IoContext::current().getIoChannelFactory()));
  KJ_REQUIRE(tree->slot.lock() == channels.slot, "facet request must stay on the original home");
  return newPromisedWorkerInterface(
      open(kj::str(channels.document.original), kj::heap<Invocation>(*this),
          channels.budgetLifetime.lock()));
}

kj::Promise<void> LocalFacet::initialize(kj::String descriptor) {
  auto slot = tree->slot.lock();
  KJ_REQUIRE(slot != nullptr, "durable test facet lost its isolate copy");
  // The startup callback is only used once. Its IoContext reentry handle counts as pending I/O.
  auto startFacet = kj::mv(getStartInfo);
  Worker::Actor::Id id;
  Frankenvalue props;
  PropsMessage saveStart;
  if (auto saved = tree->facetStarts.find(facet); saved != tree->facetStarts.end()) {
    const auto start = saved->second->getRoot<threaded::rpc::ThreadedSocketFacet>().asReader();
    KJ_REQUIRE(start.getParent() == parentFacet && start.getName() == name,
        "the restored WebSocket facet no longer matches its storage index");
    className = kj::str(start.getClassName());
    if (start.getNamespaceId().size() == 0) {
      id = kj::str(start.getActorId());
    } else {
      kj::Maybe<kj::String> actorName;
      if (start.getHasActorName()) actorName = kj::str(start.getActorName());
      id = kj::Own<ActorIdFactory::ActorId>(kj::heap<ObjectIdFactory::Id>(
          kj::str(start.getNamespaceId()), kj::str(start.getActorId()), kj::mv(actorName)));
    }
    auto channels = kj::rc<Channels>(tree->state, tree->server, slot, descriptor);
    props = readFacetProps(start.getProps(), [&](uint channel, kj::Maybe<kj::String> object,
                              kj::Maybe<Frankenvalue> props) {
      return kj::rc<Channels::Bound>(channels.addRef(), channel, kj::mv(object), kj::mv(props)).toOwn();
    });
  } else {
    auto info = co_await startFacet();
    co_await info.ensureAllResolved();
    auto *binding = dynamic_cast<LocalActorClass *>(info.actorClass.get());
    KJ_REQUIRE(!stopped && binding != nullptr && slot == binding->slot.lock(),
        "durable test facets require a class in the original isolate copy");
    className = kj::str(binding->name);
    id = kj::mv(info.id);
    props = binding->props.clone();
    constexpr size_t MAX_BYTES = threaded::rpc::THREADED_SOCKET_FACET_MAX_WORDS * sizeof(capnp::word);
    size_t identityBytes = name.size() + className.size();
    KJ_SWITCH_ONEOF(id) {
      KJ_CASE_ONEOF(string, kj::String) {
        identityBytes += string.size();
      }
      KJ_CASE_ONEOF(global, kj::Own<ActorIdFactory::ActorId>) {
        auto &object = kj::downcast<ObjectIdFactory::Id>(*global);
        identityBytes += object.space.size() + object.id.size();
        KJ_IF_SOME(value, object.name) { identityBytes += value.size(); }
      }
    }
    if (tree->nodeSockets && identityBytes <= MAX_BYTES) {
      saveStart = kj::heap<capnp::MallocMessageBuilder>();
      auto start = saveStart->initRoot<threaded::rpc::ThreadedSocketFacet>();
      start.setId(facet);
      start.setParent(parentFacet);
      start.setName(name);
      start.setClassName(className);
      KJ_SWITCH_ONEOF(id) {
        KJ_CASE_ONEOF(string, kj::String) {
          start.setActorId(string);
        }
        KJ_CASE_ONEOF(global, kj::Own<ActorIdFactory::ActorId>) {
          auto &object = kj::downcast<ObjectIdFactory::Id>(*global);
          start.setNamespaceId(object.space);
          start.setActorId(object.id);
          KJ_IF_SOME(value, object.name) {
            start.setHasActorName(true);
            start.setActorName(value);
          }
        }
      }
      auto remaining = MAX_BYTES - identityBytes;
      auto describe = [&](Frankenvalue::CapTableEntry& cap) -> kj::Maybe<FacetPropChannel> {
        auto* bound = dynamic_cast<Channels::Bound*>(&cap);
        if (bound == nullptr || bound->owner->slot != slot) return kj::none;
        return FacetPropChannel{bound->channel, bound->object.map([](auto& id) { return id.asPtr(); }),
          bound->props.map([](auto& props) -> Frankenvalue& { return props; })};
      };
      if (!writeFacetProps(props, start.initProps(), remaining, describe) ||
          start.asReader().totalSize().wordCount > threaded::rpc::THREADED_SOCKET_FACET_MAX_WORDS) {
        saveStart = nullptr;
      }
    }
  }
  kj::Maybe<server::ThreadedWalLog::PreparedOpen> prepared;
  if (tree->state.wal) {
    auto wal = tree->storage().walImage(tree->uniqueKey, tree->storageId, facet);
    prepared = co_await prepareDurableWal(*tree->server.durableLog, kj::mv(wal), tree->tree, facet);
    KJ_REQUIRE(!stopped, "durable facet closed during WAL preparation");
  }
  actor = makeDurableActor(tree, *KJ_REQUIRE_NONNULL(slot->worker), kj::mv(id), className,
      kj::mv(props), facet, depth, reservation, parentFacet, name, kj::mv(prepared));
  if (tree->state.wal) co_await tree->state.wal->waitForOpen(
      tree->shippers.at(facet)->getOpenId(), tree->state.io.provider->getTimer());
  KJ_REQUIRE(!stopped, "durable facet closed during startup");
  if (saveStart.get() != nullptr) tree->facetStarts.emplace(facet, kj::mv(saveStart));
  tree->installSocketManager(*KJ_ASSERT_NONNULL(actor).value, facet);
}

kj::Promise<void> LocalFacet::ensureStarted(kj::StringPtr descriptor) {
  if (actor == kj::none && starting == kj::none) starting = initialize(kj::str(descriptor)).fork();
  KJ_IF_SOME(load, starting) {
    co_await load.addBranch();
  }
  KJ_REQUIRE(!stopped, "durable test facet is closed");
}

kj::Promise<kj::Rc<LocalFacet>> LocalFacet::openSocketFacet(uint id, kj::StringPtr descriptor) {
  Invocation invocation(*this);
  co_await ensureStarted(descriptor);
  co_return co_await KJ_ASSERT_NONNULL(actor).hooks->getSocketFacet(id, descriptor);
}

kj::Promise<std::pair<kj::Own<WorkerInterface>, kj::Own<Worker::Actor>>> LocalFacet::openSocket(
    kj::String descriptor, BudgetLifetime budgetLifetime) {
  auto entry = co_await open(kj::mv(descriptor), kj::heap<Invocation>(*this), kj::mv(budgetLifetime));
  co_return std::make_pair(kj::mv(entry), kj::addRef(*KJ_ASSERT_NONNULL(actor).value));
}

kj::Promise<kj::Own<WorkerInterface>> LocalFacet::open(
    kj::String descriptor, kj::Own<Invocation> invocation, BudgetLifetime budgetLifetime) {
  co_await ensureStarted(descriptor);
  auto slot = tree->slot.lock();
  KJ_REQUIRE(slot != nullptr, "durable test facet lost its isolate copy");
  JsonDoc input(descriptor);
  auto prepared = prepareInvocation(tree->state, tree->server, std::move(slot), input.get(),
      descriptor, kj::mv(budgetLifetime), Frankenvalue(), nullptr, nullptr, kj::refcounted<RequestObserver>(),
      kj::addRef(*KJ_ASSERT_NONNULL(actor).value), className.asPtr());
  co_return kj::mv(prepared.entry).attach(kj::mv(invocation));
}

DurableActorHandle makeDurableActor(const std::shared_ptr<DurableTreeContext> &tree,
    Worker &worker,
    Worker::Actor::Id id,
    kj::StringPtr className,
    Frankenvalue props,
    uint facet,
    uint depth,
    std::shared_ptr<DurableFacetReservation> reservation,
    uint parentFacet,
    kj::StringPtr facetName,
    kj::Maybe<server::ThreadedWalLog::PreparedOpen> prepared,
    bool platformDatabase,
    uint64_t sqlWalFrameLimitBytes) {
  auto hooks = kj::refcounted<DurableActorHooks>(tree, facet, depth);
  auto *borrowedHooks = hooks.get();
  auto observer = kj::refcounted<DurableActorObserver>(tree, facet);
  auto *borrowedObserver = observer.get();
  if (reservation) observer = kj::mv(observer).attachToThisReference(std::move(reservation));
  auto makeCache = [tree, facet, parentFacet, name = kj::str(facetName),
                       prepared = kj::mv(prepared), borrowedHooks, platformDatabase,
                       sqlWalFrameLimitBytes](const ActorCache::SharedLru &, OutputGate &gate,
                       ActorCache::Hooks &,
                       SqliteObserver &observer) -> kj::Maybe<kj::Own<ActorCacheInterface>> {
    const auto localOpenStartedNs = tree->state.wal ? nowNs() : 0;
    const bool budgeted = !platformDatabase || sqlWalFrameLimitBytes != 0;
    const auto memoryLimit = budgeted
        ? boundedDurableSetting(platformDatabase ? "COWORKERD_DO_TEST_SQL_MEMORY_BYTES"
                                                 : "COWORKERD_DO_TEST_OBJECT_SQL_MEMORY_BYTES",
              SqliteDatabase::PLATFORM_SQL_HEAP_LIMIT_BYTES,
              SqliteDatabase::PLATFORM_SQL_HEAP_LIMIT_BYTES)
        : kj::maxValue;
    std::shared_ptr<SqliteProcessMemoryBudget> sqliteProcessBudget;
    if (budgeted) {
      auto slot = tree->slot.lock();
      KJ_REQUIRE(slot != nullptr, "durable storage opened without its isolate reservation");
      sqliteProcessBudget = slot->reservation;
    }
    KJ_ASSERT(tree->server.durableLog.get() == nullptr || prepared != kj::none,
        "persistent SQLite open without WAL preparation");
    server::DurableWalPosition position;
    KJ_IF_SOME(open, prepared) {
      try {
        position = open.positionForOpen();
      } catch (const kj::Exception &error) {
        kj::throwRecoverableException(tagWalOpenError(error.clone()));
      }
    }
    auto database = tree->storage().open(tree->uniqueKey, tree->storageId, observer, facet,
        sqlWalFrameLimitBytes, memoryLimit, std::move(sqliteProcessBudget));
    if (!platformDatabase) {
      KJ_REQUIRE(tree->sqliteConnections.emplace(facet, database.get()).second,
          "durable SQLite connection is already registered");
      tree->server.objectSqliteConnections.fetch_add(1);
      database = kj::mv(database).attach(kj::defer([tree, facet] {
        tree->sqliteConnections.erase(facet);
        tree->server.objectSqliteConnections.fetch_sub(1);
      }));
    }
    if (tree->state.wal) {
      kj::Maybe<kj::Function<void(SqliteDatabase::CloseOutcome)>> confirmClose;
      KJ_IF_SOME(open, prepared) confirmClose = open.trackClose();
      auto shipper = tree->storage().ship(tree->uniqueKey,
          {.objectName = kj::str(tree->objectName),
            .storageId = kj::str(tree->storageId),
            .facetId = facet,
            .parentFacetId = parentFacet,
            .facetName = kj::str(name)},
          *database, tree->state.io.provider->getTimer(), *tree->state.wal, position,
          localOpenStartedNs, kj::mv(confirmClose));
      auto* current = shipper.get();
      KJ_REQUIRE(tree->shippers.emplace(facet, current).second, "durable facet is already open");
      shipper = kj::mv(shipper).attach(kj::defer([tree, facet] { tree->shippers.erase(facet); }));
      database->afterReset([current, borrowedHooks](SqliteDatabase& reset) {
        borrowedHooks->resetStorage(reset, *current);
      });
      auto alarms = kj::heap<DurableAlarmReporter>(*current, facet);
      if (facet == 0) current->reportAlarm(server::readDurableAlarm(*database));
      auto& alarmHooks = *alarms;
      return kj::heap<ActorSqlite>(kj::mv(database), gate,
          [shipper = kj::mv(shipper), &server = tree->server](SpanParent) mutable {
        auto confirmation = shipper->onCommit();
        server.durablePendingCommits.fetch_add(1);
        return confirmation.attach(kj::defer([&server] { server.durablePendingCommits.fetch_sub(1); }));
      }, alarmHooks)
          .attach(kj::mv(alarms));
    }
    return kj::heap<ActorSqlite>(kj::mv(database), gate,
        [state = &tree->state, server = &tree->server](SpanParent) -> kj::Promise<void> {
      if (server->durableCommitDelayMs == 0) co_return;
      server->durablePendingCommits.fetch_add(1);
      KJ_DEFER(server->durablePendingCommits.fetch_sub(1));
      co_await state->io.provider->getTimer().afterDelay(
          server->durableCommitDelayMs * kj::MILLISECONDS);
    });
  };
  auto makeStorage = [](jsg::Lock &js, const Worker::Api &, ActorCacheInterface &cache) {
    return js.alloc<api::DurableObjectStorage>(js, IoContext::current().addObject(cache), true);
  };
  auto actor = kj::refcounted<Worker::Actor>(worker, kj::none, kj::mv(id), true, kj::mv(makeCache),
      className, kj::mv(props), kj::mv(makeStorage), kj::mv(hooks), tree->state.timer,
      kj::mv(observer), kj::none, kj::none, kj::none, jsg::Dict<kj::String>{}, *borrowedHooks,
      kj::none, kj::none, kj::heap<DurablePendingOperations>(tree->idle));
  if (platformDatabase && tree->server.durableLog) {
    // The output gate locks on the first write. Let both the SQL work and
    // the subsequent WAL confirmation use their separate time windows.
    actor->setOutputGateTimeout(
        DATABASE_SQL_WALL_LIMIT + tree->server.durableLog->confirmationTimeout() + kj::SECONDS);
  }
  borrowedObserver->bind(*actor);
  return {kj::mv(actor), borrowedHooks};
}

std::shared_ptr<ThreadState::WalPreparation> prepareRootWal(
    ThreadedServer &server, ThreadState &state, const DurableRoute &route, uint64_t tree) {
  auto& slot = state.preparingWal[tree];
  auto preparation = slot.lock();
  if (!preparation) {
    auto wal = server.durableStorage->walImage(route.uniqueKey.c_str(), route.storageId.c_str(), 0);
    preparation = std::make_shared<ThreadState::WalPreparation>(
        prepareDurableWal(*server.durableLog, kj::mv(wal), tree, 0));
    slot = preparation;
  }
  return preparation;
}

kj::Promise<rpc::EventDispatcher::Client> DurableBootstrap::prepare(
    kj::String descriptor, Frankenvalue props, BudgetLifetime budgetLifetime) {
  KJ_REQUIRE(!used, "one event per durable connection");
  used = true;
  KJ_REQUIRE(!server.durableLog || server.durableLog->ready(), "durable WAL channel unavailable");
  JsonDoc full(descriptor);
  auto input = full.get();
  auto selection = field(input, "rootSelection");
  auto plan = field(input, "rootPlan");
  const auto kind = text(plan, "kind");
  KJ_REQUIRE(readDurableRoute(field(input, "durableTestRoute")) == route &&
          flag(selection, "dispatchChecked") &&
          str(selection, "namespaceId") == route.key.namespaceId &&
          str(selection, "objectId") == route.key.objectId &&
          durableNumber(selection, "dataEpoch") == route.open.dataEpoch &&
          (kind == "tenant" || (kind == "platformDurable" &&
              platformDurablePlan(input, plan, selection, route))) &&
          str(plan, "versionFingerprint") == route.open.definition &&
          route.uniqueKey == "coworkerd-do-root-v1-" + route.key.namespaceId,
      "durable routing descriptor mismatch");
  const bool platformDatabase = kind == "platformDurable" && productPlatformPlan(plan) &&
      route.open.className == "SqlDatabase";
  const auto sqlWalFrameLimitBytes = platformDatabase && server.durableLog
      ? durableNumber(input, "sqlWalFrameLimitBytes") : 0;
  KJ_REQUIRE(!platformDatabase || !server.durableLog || sqlWalFrameLimitBytes > 0,
      "missing SQL WAL frame limit");
  std::shared_ptr<SqlExecutionBudget> sqlDeadline;
  kj::Maybe<kj::TimePoint> sqlExpires;
  if (field(input, "sqlDeadlineMonotonicNs").isString()) {
    KJ_REQUIRE(platformDatabase && !field(input, "durableAlarm").isObject(),
        "SQL deadline requires a platform database query");
    constexpr uint64_t SQL_LIMIT_NS = DATABASE_SQL_WALL_LIMIT / kj::NANOSECONDS;
    const auto expires = durableNumber(input, "sqlDeadlineMonotonicNs");
    const auto now = nowNs();
    KJ_REQUIRE(expires <= now + SQL_LIMIT_NS, "invalid SQL deadline");
    sqlExpires = kj::systemPreciseMonotonicClock().now() + (expires > now ? expires - now : 0) * kj::NANOSECONDS;
    sqlDeadline = std::make_shared<SqlExecutionBudget>(SQL_LIMIT_NS,
        [expires, weak = std::weak_ptr(scope)]() noexcept {
      const auto now = nowNs();
      if (const auto session = weak.lock(); !session || session->sqlClientDisconnected(now))
        return SqlExecutionBudget::Cause::DISCONNECTED;
      if (now >= expires) return SqlExecutionBudget::Cause::WALL;
      return SqlExecutionBudget::Cause::NONE;
    });
  }
  auto checkSqlDeadline = [&]() {
    if (sqlDeadline && sqlDeadline->checkDeadline()) {
      if (sqlDeadline->getCause() == SqlExecutionBudget::Cause::DISCONNECTED)
        kj::throwRecoverableException(KJ_EXCEPTION(DISCONNECTED, "SQL client disconnected before execution"));
      kj::throwRecoverableException(jsg::tagCoworkerdRpcError(
          KJ_EXCEPTION(OVERLOADED, sqlDeadline->describe()), "SQL_LIMIT_EXCEEDED"));
    }
  };
  checkSqlDeadline();
  auto admission = server.admitDurableEvent(state, route, ticket, platformDatabase);
  KJ_IF_SOME(expires, sqlExpires) {
    admission = admission.exclusiveJoin(state.io.provider->getTimer().atTime(expires).then([] {
      kj::throwRecoverableException(jsg::tagCoworkerdRpcError(
          KJ_EXCEPTION(OVERLOADED, "SQL_LIMIT_EXCEEDED: wall limit 30000ms exceeded"), "SQL_LIMIT_EXCEEDED"));
    }));
  }
  co_await admission;
  checkSqlDeadline();
  auto slot = co_await server.getDurableWorker(state, descriptor, route.open.definition);
  KJ_REQUIRE(slot->actorClasses.contains(route.open.className),
      "durable class missing from pinned Worker");
  auto found = state.durableRoots.find(ticket.tree);
  // Keep sharing the preparation until a home publishes the root. There can
  // be new arrivals between the scanner's completion and opening SQLite.
  std::shared_ptr<ThreadState::WalPreparation> walPreparation;
  KJ_DEFER(if (walPreparation.use_count() == 1) state.preparingWal.erase(ticket.tree));
  kj::Maybe<server::ThreadedWalLog::PreparedOpen> prepared;
  if (found == state.durableRoots.end() && state.wal) {
    walPreparation = prepareRootWal(server, state, route, ticket.tree);
    prepared = co_await walPreparation->ready.addBranch();
    found = state.durableRoots.find(ticket.tree);
  }
  if (found == state.durableRoots.end()) {
    auto actorId = ObjectIdFactory(kj::StringPtr(route.key.namespaceId.c_str()))
                       .idFromString(kj::str(route.key.objectId.c_str()));
    const auto jitter = std::hash<std::string>{}(route.key.objectId) % 71;
    auto idle = std::make_shared<server::DurableIdleLifecycle>(
        durableIdleNow(), std::chrono::seconds(70 + jitter));
    auto tree = std::make_shared<DurableTreeContext>(DurableTreeContext{
      state,
      server,
      slot,
      ticket.tree,
      kj::str(route.uniqueKey.c_str()),
      kj::str(route.storageId.c_str()),
      kj::str(route.key.namespaceId.c_str(), "/", route.key.objectId.c_str(), "/",
          route.open.dataEpoch),
      {},
      idle,
    });
    auto actor = makeDurableActor(tree, *KJ_REQUIRE_NONNULL(slot->worker),
        Worker::Actor::Id(kj::mv(actorId)), route.open.className.c_str(), Frankenvalue(), 0, 0,
        nullptr, 0, ""_kj, kj::mv(prepared), platformDatabase, sqlWalFrameLimitBytes);
    found = state.durableRoots
                .emplace(ticket.tree,
                    ThreadState::DurableRoot{
                      .actor = kj::mv(actor.value),
                      .tree = tree,
                      .hooks = actor.hooks,
                      .idle = std::move(idle),
                      .facetsBusy =
                          [hooks = actor.hooks](
                              bool includeBackground) { return hooks->busy(includeBackground); },
                      .facetHasOrdinaryWebSocket =
                          [hooks = actor.hooks] { return hooks->hasOrdinaryWebSocket(); },
                      .stopFacets =
                          [hooks = actor.hooks](const kj::Exception *reason) {
      if (reason == nullptr)
        hooks->stop();
      else
        hooks->stop(*reason);
    },
                      .rootOpenId = [tree]() -> uint64_t {
      const auto root = tree->shippers.find(0);
      return root == tree->shippers.end() ? 0 : root->second->getOpenId();
    },
                      .notifyReset =
                          [tree](kj::StringPtr code) {
      const auto root = tree->shippers.find(0);
      if (root != tree->shippers.end()) root->second->reportActorReset(code);
    },
                    })
                .first;
    server.publishDurableRoot(state.durableHome, ticket.tree);
    found->second.broken = found->second.actor->onBroken()
                               .then([]() {}, [tree](kj::Exception &&error) {
      const auto at = tree->state.durableRoots.find(tree->tree);
      if (at != tree->state.durableRoots.end() && !at->second.idleClosing) {
        tree->server.beginDurableReset(tree->state, tree->tree, error);
      }
    }).eagerlyEvaluate(nullptr);
  }
  if (state.wal) {
    co_await state.wal->waitForOpen(found->second.rootOpenId(), state.io.provider->getTimer());
    found = state.durableRoots.find(ticket.tree);
  }
  if (found == state.durableRoots.end() || found->second.resetting || found->second.closeRequested) {
    kj::throwRecoverableException(jsg::tagCoworkerdRpcError(
        KJ_EXCEPTION(OVERLOADED, "durable object reset is still draining"), "DO_UNAVAILABLE"));
  }
  if (state.wal) {
    auto& root = found->second;
    if (!root.nodeSockets) {
      auto sockets = std::make_shared<DurableNodeSockets>(state, kj::str(server.bindingAddress.c_str()),
          route, root.rootOpenId(), root.idle);
      root.tree->nodeSockets = sockets;
      root.parkSockets = [tree = root.tree] { return tree->parkSockets(); };
      root.hasLocalSockets = [tree = root.tree] {
        auto manager = tree->socketManagers.find(0);
        return manager != tree->socketManagers.end() && manager->second.manager->hasLocalSockets();
      };
      root.nodeSockets = std::move(sockets);
      root.socketsRestored = !flag(input, "hasNodeSockets");
      if (root.socketsRestored) root.tree->installSocketManager(*root.actor, 0);
    }
    if (!root.socketsRestored && !root.adopting) {
      const auto tree = ticket.tree;
      root.adopting = std::make_shared<ThreadState::SocketAdoption>(
          root.nodeSockets->adopt()
              .then([&state = state, &server = server, tree](
                        kj::Maybe<DurableSocketAdoption> restored) {
        auto current = state.durableRoots.find(tree);
        KJ_REQUIRE(
            current != state.durableRoots.end(), "the Durable Object WebSocket home has ended");
        KJ_IF_SOME(response, restored) {
          current->second.tree->restoreSockets(kj::mv(response));
          current->second.tree->installSocketManager(*current->second.actor, 0);
          current->second.socketsRestored = true;
        } else {
          auto reason =
              KJ_EXCEPTION(DISCONNECTED, "Durable Object could not confirm WebSocket adoption.");
          server.beginDurableReset(state, tree, reason);
          current->second.actor->abort(kj::mv(reason));
          KJ_FAIL_REQUIRE("the node could not confirm WebSocket adoption");
        }
      }).attach(std::shared_ptr(root.nodeSockets)));
      // A client can drop the first event while the node is adopting. Let the
      // home finish the handoff and settle the admission fence anyway.
      auto adopting = root.adopting;
      state.tasks.add(adopting->ready.addBranch()
                          .catch_([](kj::Exception &&error) {
        reportDurableFailureForTest("WebSocket adoption", error);
      }).then([&state = state, tree, adopting] {
        auto current = state.durableRoots.find(tree);
        if (current != state.durableRoots.end() && current->second.adopting == adopting)
          current->second.adopting.reset();
      }));
    }
    if (auto adopting = root.adopting) {
      try {
        co_await adopting->ready.addBranch();
      } catch (const kj::Exception &) {
        kj::throwRecoverableException(jsg::tagCoworkerdRpcError(
            KJ_EXCEPTION(OVERLOADED, "the node could not restore the durable object's WebSockets"),
            "DO_UNAVAILABLE"));
      }
      found = state.durableRoots.find(ticket.tree);
      if (found == state.durableRoots.end() || found->second.resetting ||
          found->second.closeRequested) {
        kj::throwRecoverableException(jsg::tagCoworkerdRpcError(
            KJ_EXCEPTION(OVERLOADED, "durable object reset is still draining"), "DO_UNAVAILABLE"));
      }
    }
  }
  auto &root = found->second;
  kj::Own<WorkerInterface> entry;
  kj::Maybe<DurableAlarmEvent> alarm;
  auto alarmDescriptor = field(input, "durableAlarm");
  if (alarmDescriptor.isObject()) {
    KJ_REQUIRE(state.wal.get() != nullptr, "persistent storage is required for an alarm");
    const auto time = durableNumber(alarmDescriptor, "scheduledTime");
    const auto retries = durableNumber(alarmDescriptor, "retryCount");
    KJ_REQUIRE(time > 0 && time <= INT64_MAX / 1000000 && retries <= 6,
        "invalid durable alarm descriptor");
    alarm = DurableAlarmEvent{kj::addRef(*found->second.actor),
      kj::UNIX_EPOCH + static_cast<int64_t>(time) * kj::MILLISECONDS,
      static_cast<uint32_t>(retries), flag(alarmDescriptor, "clear")};
  }
  kj::Maybe<DurableSocketEvent> socketEvent;
  auto socketDescriptor = field(input, "durableSocket");
  if (socketDescriptor.isObject()) {
    KJ_REQUIRE(state.wal.get() != nullptr, "persistent storage is required for a socket event");
    const auto facetId = durableNumber(socketDescriptor, "facet");
    KJ_REQUIRE(facetId <= UINT16_MAX, "invalid socket facet");
    auto socketActor = kj::addRef(*root.actor);
    if (facetId != 0) {
      auto facet = co_await root.hooks->getSocketFacet(facetId, descriptor);
      auto opened = co_await facet->openSocket(kj::str(descriptor), kj::mv(budgetLifetime));
      entry = kj::mv(opened.first);
      socketActor = kj::mv(opened.second);
    }
    socketEvent = readDurableSocketEvent(socketDescriptor, kj::mv(socketActor));
  }
  if (entry.get() == nullptr) {
    checkSqlDeadline();
    entry = prepareInvocation(state, server, slot, input, descriptor, kj::mv(budgetLifetime), kj::mv(props), nullptr,
        nullptr, kj::refcounted<RequestObserver>(), kj::addRef(*root.actor), kj::none, std::move(sqlDeadline))
                .entry;
  }
  auto identity = invocationIdentity(*slot, input);
  kj::Maybe<DurableSocketStart> socket;
  if (state.wal) {
    socket = DurableSocketStart{
      [server = &server, state = &state, ticket = ticket, scope = scope] {
      server->markDurableSocket(*state, ticket, scope);
    },
      found->second.idle,
      kj::str(route.invocation.c_str()),
      *state.headers,
      root.tree,
    };
  }
  co_return rpc::EventDispatcher::Client(kj::heap<ChannelDispatcher>(state, kj::mv(entry),
      std::move(slot), kj::mv(identity), kj::mv(alarm), kj::mv(socket), kj::mv(socketEvent)));
}

kj::Promise<void> DurableBootstrap::startEvent(StartEventContext context) {
  auto params = context.getParams();
  KJ_REQUIRE(!params.getFromPersistentStub() && !params.getFromHandoff(),
      "unsupported durable test event origin");
  try {
    KJ_REQUIRE(!scope->ended, "durable session ended before its event was prepared");
    const auto ticket = params.getBudgetTicket();
    auto dispatcher = co_await scope->preparing.wrap(prepare(kj::str(params.getCfBlobJson()),
        Frankenvalue(), server.retainDurableBudget(ticket)));
    auto result = context.getResults();
    result.setDispatcher(kj::mv(dispatcher));
    result.setBudgetAccepted(ticket != 0);
    if (accepted->isWaiting()) accepted->fulfill();
  } catch (const kj::Exception &error) {
    reportDurableFailureForTest("event preparation", error);
    throw;
  }
}

kj::Promise<void> DurableBootstrap::startNativeEvent(StartNativeEventContext context) {
  auto params = context.getParams();
  try {
    KJ_REQUIRE(!scope->ended, "durable session ended before its event was prepared");
    const auto ticket = params.getBudgetTicket();
    auto dispatcher = co_await scope->preparing.wrap(prepare(
        kj::str(params.getCfBlobJson()), readNativeProps(state, params.getCoworkerdProps()),
        server.retainDurableBudget(ticket)));
    auto result = context.getResults();
    result.setDispatcher(kj::mv(dispatcher));
    result.setBudgetAccepted(ticket != 0);
    if (accepted->isWaiting()) accepted->fulfill();
  } catch (const kj::Exception &error) {
    reportDurableFailureForTest("native event preparation", error);
    throw;
  }
}

kj::Promise<void> Dispatcher::jsRpcSession(JsRpcSessionContext context) {
  KJ_REQUIRE(!used, "one event per invocation");
  used = true;
  JsonDoc document(descriptor);
  auto input = document.get();
  KJ_REQUIRE(!flag(input, "trial"), "a trial load cannot start an RPC session");
  bool coldLoaded = false;
  std::shared_ptr<Slot> slot;
  std::shared_ptr<ResourcePreparation> preparation;
  std::shared_ptr<TenantPermit> tenant;
  bool capacity = false;
  bool tenantCapacity = false;
  kj::Maybe<kj::Exception> loadFailure;
  try {
    if (state.rejectOnly) kj::throwRecoverableException(capacityError());
    tenant = server.admitTenant(input);
    slot = co_await server.getWorker(input, state, coldLoaded);
    preparation = server.prepareSlot(slot);
  } catch (const kj::Exception &error) {
    if (error.getDetail(CAPACITY_DETAIL) != kj::none) {
      capacity = true;
      const auto detail = KJ_REQUIRE_NONNULL(error.getDetail(CAPACITY_DETAIL));
      tenantCapacity = detail.size() == 1 && detail[0] == 1;
    } else
      loadFailure = error.clone();
  }
  if (capacity) {
    const auto scope = tenantCapacity ? CapacityScope::TENANT : CapacityScope::PROCESS;
    co_await server.confirmCapacity(input, state, scope);
    kj::throwRecoverableException(capacityError(scope));
  }
  KJ_IF_SOME(error, loadFailure) {
    auto stage = threaded::LoadStage::LOAD;
    KJ_IF_SOME(detail, error.getDetail(LOAD_STAGE_DETAIL)) {
      if (detail.size() == 1) stage = static_cast<threaded::LoadStage>(detail[0]);
    }
    co_await server.confirmDecision(input, state, "load-rejected", error.getDescription(), stage);
    kj::throwRecoverableException(error.clone());
  }
  auto prepared = prepareInvocation(state, server, slot, input, descriptor, kj::mv(budgetLifetime), kj::mv(props),
      std::move(preparation), std::move(tenant));
  watchRequestLease(prepared.invocation->measurement, kj::mv(lease));
  auto identity = invocationIdentity(*slot, input);
  startBoundRpcSession(context.getResults(),
      kj::mv(prepared.entry).attach(kj::mv(prepared.invocation)), std::move(slot),
      kj::mv(identity));
}

// Response::send() can run inside the same JS turn that allocated a large
// object, before the turn's final limit check. Check before committing headers.
class LimitedResponse final: public kj::HttpService::Response {
 public:
  LimitedResponse(kj::HttpService::Response &inner, InvocationState &invocation)
      : inner(inner),
        invocation(invocation) {}
  kj::Own<kj::AsyncOutputStream> send(uint status,
      kj::StringPtr text,
      const kj::HttpHeaders &headers,
      kj::Maybe<uint64_t> length) override {
    check();
    return inner.send(status, text, headers, length);
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders &headers) override {
    check();
    return inner.acceptWebSocket(headers);
  }

 private:
  void check() {
    auto &limits = *invocation.slot->limits;
    if (v8::Isolate::TryGetCurrent() == limits.memory->isolate) limits.memory->observe();
    auto cause = limits.memoryState->load() == LimitCause::MEMORY
        ? LimitCause::MEMORY
        : invocation.measurement.limitCause.load();
    if (cause != LimitCause::NONE) kj::throwRecoverableException(limitException(cause));
  }
  kj::HttpService::Response &inner;
  InvocationState &invocation;
};

kj::Promise<void> Invocation::request(kj::HttpMethod method,
    kj::StringPtr url,
    const kj::HttpHeaders &headers,
    kj::AsyncInputStream &body,
    Response &response) {
  KJ_REQUIRE(!used, "NOT_SUPPORTED: one request per invocation");
  used = true;
  JsonDoc document(descriptor);
  auto input = document.get();
  const bool trial = flag(input, "trial");
  std::shared_ptr<Slot> slot;
  kj::Maybe<kj::String> loadError;
  auto loadStage = threaded::LoadStage::LOAD;
  bool coldLoaded = false;
  bool capacity = false;
  bool tenantCapacity = false;
  std::shared_ptr<ResourcePreparation> preparation;
  std::shared_ptr<TenantPermit> tenant;
  try {
    if (state.rejectOnly) kj::throwRecoverableException(capacityError());
    tenant = server.admitTenant(input);
    slot = co_await server.getWorker(input, state, coldLoaded);
    if (!trial) preparation = server.prepareSlot(slot);
  } catch (const kj::Exception &error) {
    KJ_IF_SOME(detail, error.getDetail(CAPACITY_DETAIL)) {
      capacity = true;
      tenantCapacity = detail.size() == 1 && detail[0] == 1;
    }
    loadError = kj::str(error.getDescription());
    KJ_IF_SOME(stage, error.getDetail(LOAD_STAGE_DETAIL)) {
      if (stage.size() == 1 && stage[0] <= static_cast<kj::byte>(threaded::LoadStage::EVALUATION))
        loadStage = static_cast<threaded::LoadStage>(stage[0]);
    }
  }
  if (capacity) {
    auto report = trial
        ? kj::str("{\"ok\":false,\"capacityRejected\":true,\"message\":\"",
              tenantCapacity ? "tenant execution concurrency limit reached in process"
                             : "process admission budget is full",
              "\"}")
        : co_await server.confirmCapacity(
              input, state, tenantCapacity ? CapacityScope::TENANT : CapacityScope::PROCESS);
    auto output = response.send(trial ? 200 : 503, trial ? "OK" : "Service Unavailable",
        kj::HttpHeaders(*state.headers), report.size());
    co_await output->write(report.asBytes());
    co_return;
  }
  KJ_IF_SOME(message, loadError) {
    capnp::MallocMessageBuilder arena;
    auto value = arena.initRoot<Json>();
    value.setString(message);
    auto info = threaded::loadStageInfo(loadStage);
    auto report = kj::str("{\"ok\":false,\"stage\":\"",
        kj::heapString(info.stage.data(), info.stage.size()), "\",\"codeStage\":\"",
        kj::heapString(info.codeStage.data(), info.codeStage.size()), "\",\"name\":\"",
        kj::heapString(info.name.data(), info.name.size()), "\",\"message\":", encode(value), "}");
    if (!trial)
      report = co_await server.confirmDecision(input, state, "load-rejected", message, loadStage);
    auto output = response.send(trial ? 200 : 503, trial ? "OK" : "Service Unavailable",
        kj::HttpHeaders(*state.headers), report.size());
    co_await output->write(report.asBytes());
    co_return;
  }
  auto entrypoint = threaded::normalizeEntrypoint(
      str(input, "format"), field(input, "entrypoint").isString() ? str(input, "entrypoint") : "");
  if (trial) {
    auto names = kj::str("[");
    bool first = true;
    for (auto &name: slot->httpEntrypoints) {
      if (name == "default") continue;
      capnp::MallocMessageBuilder arena;
      auto value = arena.initRoot<Json>();
      value.setString(name.c_str());
      names = kj::str(names, first ? "" : ",", encode(value));
      first = false;
    }
    names = kj::str(names, "]");
    auto classes = kj::str("[");
    auto declared = field(input, "durableObjectClasses");
    if (declared.isArray()) {
      bool first = true;
      for (auto name: declared.getArray()) {
        bool usable = name.isString() && slot->actorClasses.contains(name.getString().cStr());
        classes = kj::str(classes, first ? "" : ",", "{\"name\":", encode(name),
            ",\"usable\":", usable ? "true" : "false", ",\"reason\":",
            usable ? "null" : "\"class is missing or is not a Durable Object class\"", "}");
        first = false;
      }
    }
    classes = kj::str(classes, "]");
    auto report = kj::str("{\"ok\":true,\"nativeHttpValidated\":",
        slot->httpEntrypoints.contains(entrypoint) ? "true" : "false",
        ",\"durableObjectClasses\":", classes,
        ",\"rootBridgeChecked\":false,\"coldLoad\":", coldLoaded ? "true" : "false",
        ",\"defaultKind\":\"native\",\"namedEntrypoints\":", names,
        ",\"hasDefault\":", slot->entrypoints.contains("default") ? "true" : "false",
        ",\"entrypointUsable\":", slot->entrypoints.contains(entrypoint) ? "true" : "false", "}");
    auto output = response.send(200, "OK", kj::HttpHeaders(*state.headers), report.size());
    co_await output->write(report.asBytes());
    co_return;
  }
  auto prepared = prepareInvocation(state, server, slot, input, descriptor, kj::mv(budgetLifetime), kj::mv(props),
      std::move(preparation), std::move(tenant),
      httpEntryObserver(input, entrypoint.c_str(), headers, state.placementCallId));
  auto &invocation = prepared.invocation;
  auto &entry = prepared.entry;
  watchRequestLease(invocation->measurement, kj::mv(lease));
  kj::Maybe<kj::Exception> failure;
  LimitedResponse checkedResponse(response, *invocation);
  // workerd's request() returns once the response body is sent; a client that
  // goes away cancels this coroutine. Either ends the response for the drain
  // limit (RequestLimits::limitDrain).
  auto responseEnd = kj::newPromiseAndFulfiller<void>();
  invocation->measurement.responseEnded = responseEnd.promise.fork();
  KJ_DEFER(if (responseEnd.fulfiller->isWaiting()) responseEnd.fulfiller->fulfill());
  try {
    co_await entry->request(method, url, headers, body, checkedResponse);
  } catch (const kj::Exception &error) {
    failure = error.clone();
  }
  responseEnd.fulfiller->fulfill();
  KJ_IF_SOME(error, failure) {
    // The library converts native refusals to JS errors. Preserve the refusal
    // recorded by our own channels, without interpreting application messages.
    KJ_IF_SOME(feature, invocation->measurement.unsupportedFeature) {
      unsupported(feature);
    }
    kj::throwRecoverableException(kj::mv(error));
  }
  // Return the HTTP result at body completion, so a chunked writer can send
  // EOF and ingress can start the shared drain. The incoming request retains
  // its budget capability while background work runs. The connection keeps
  // pumping the call thread and its teardown still has a bounded backstop.
  if (invocation->measurement.requestLeaseEnded == kj::none) {
    co_await settleBackground(state.io.provider->getTimer(), state.tasks);
  }
}

void acceptLoop(ThreadedServer &server, int listener, int parent) {
  using Admission = workerd::server::DurableAdmission;
  int epoll = epoll_create1(EPOLL_CLOEXEC);
  KJ_REQUIRE(epoll >= 0);
  auto add = [&](int fd) {
    epoll_event event{};
    event.events = EPOLLIN | EPOLLRDHUP;
    event.data.fd = fd;
    int result;
    do {
      result = epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &event);
    } while (result < 0 && errno == EINTR);
    return result == 0;
  };
  fcntl(listener, F_SETFL, fcntl(listener, F_GETFL) | O_NONBLOCK);
  bool listening = add(listener);
  uint64_t retryAt = 0;
  if (parent >= 0) {
    KJ_REQUIRE(add(parent), "could not monitor parent during startup");
  }
  auto pauseListener = [&]() {
    server.acceptFailures.fetch_add(1);
    if (listening) epoll_ctl(epoll, EPOLL_CTL_DEL, listener, nullptr);
    listening = false;
    retryAt = nowNs() + 25000000;
  };
  struct Connection {
    std::string input, output;
    size_t sent = 0;
    std::optional<Admission::Ticket> durableCredit;
    std::optional<DurableRoute> durableRoute;
    Admission::Time durableDeadline{};
  };
  std::map<int, Connection> connections;
  std::map<uint64_t, int> durableWaiting;
  uint64_t nextDurableReport = 0;
  auto closeConnection = [&](auto found) {
    const int fd = found->first;
    if (auto credit = found->second.durableCredit) {
      durableWaiting.erase(credit->event);
      server.finishDurable(*credit);
    }
    epoll_ctl(epoll, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    connections.erase(found);
  };
  for (;;) {
    if (!listening && nowNs() >= retryAt) {
      listening = add(listener);
      if (!listening) pauseListener();
    }
    epoll_event events[256];
    int count = epoll_wait(epoll, events, 256, listening && !server.durableAdmission ? -1 : 25);
    if (count < 0) {
      if (errno != EINTR) {
        server.acceptFailures.fetch_add(1);
        usleep(25000);
      }
      continue;
    }
    for (int i = 0; i < count; ++i) {
      auto fd = events[i].data.fd;
      if (fd == parent) {
        char bytes[64];
        if (read(parent, bytes, sizeof(bytes)) == 0) _exit(0);
        continue;
      }
      if (fd == listener) {
        for (;;) {
          auto accepted = accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
          if (accepted < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
          if (accepted < 0 && errno == EINTR) continue;
          if (accepted < 0) {
            // A transient descriptor/socket/epoll shortage must never discard
            // invocations already executing on the other call threads.
            pauseListener();
            break;
          }
          if (!add(accepted)) {
            ::close(accepted);
            pauseListener();
            break;
          }
          connections.emplace(accepted, Connection{});
        }
        continue;
      }
      auto found = connections.find(fd);
      if (found == connections.end()) continue;
      auto &connection = found->second;
      bool close = false;
      if (connection.durableCredit) {
        // Before the application dispatch handshake, a disconnect releases its
        // waiting credit. No request body belongs to the routing preface.
        char discarded;
        const auto size = recv(fd, &discarded, 1, MSG_DONTWAIT | MSG_PEEK);
        if (size > 0 &&
            server.handoffDurable(fd, *connection.durableRoute, *connection.durableCredit,
                connection.durableDeadline)) {
          epoll_ctl(epoll, EPOLL_CTL_DEL, fd, nullptr);
          durableWaiting.erase(connection.durableCredit->event);
          connections.erase(found);
          continue;
        }
        if (size >= 0 || (events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))) {
          closeConnection(found);
        }
        continue;
      }
      if (connection.input.empty() && connection.output.empty()) {
        char prefix;
        auto size = recv(fd, &prefix, 1, MSG_PEEK | MSG_DONTWAIT);
        if (size == 1 && prefix == 'T') {
          (void)!recv(fd, &prefix, 1, MSG_DONTWAIT);
          epoll_ctl(epoll, EPOLL_CTL_DEL, fd, nullptr);
          connections.erase(found);
          char admission = !server.durableStorage && server.pending.hasSpace() ? 'A' : 'R';
          auto sent = send(fd, &admission, 1, MSG_DONTWAIT | MSG_NOSIGNAL);
          if (sent != 1 || admission == 'R')
            ::close(fd);
          else if (!server.enqueue(fd))
            ::close(fd);
          continue;
        }
        if (size == 0) close = true;
      }
      if (connection.output.empty() && !close) {
        char bytes[2048];
        auto size = recv(fd, bytes, sizeof(bytes), MSG_DONTWAIT);
        if (size > 0)
          connection.input.append(bytes, size);
        else if (size == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
          close = true;
        if (connection.input.size() > 4096) close = true;
        if (!close && server.durableAdmission && connection.input.starts_with('D') &&
            connection.input.ends_with('\n')) {
          char receipt = 'S';
          auto error = kj::runCatchingExceptions([&] {
            JsonDoc frame(kj::StringPtr(connection.input.c_str() + 1, connection.input.size() - 1));
            auto input = frame.get();
            auto route = readDurableRoute(input);
            KJ_REQUIRE(!server.durableLog || server.durableLog->ready(), "durable WAL channel unavailable");
            const auto deadlineNs = durableNumber(input, "expiresMonotonicNs");
            KJ_REQUIRE(deadlineNs <= uint64_t(std::numeric_limits<int64_t>::max()),
                "invalid durable routing deadline");
            auto deadline = Admission::Time(std::chrono::duration_cast<Admission::Clock::duration>(
                std::chrono::nanoseconds(deadlineNs)));
            auto result = server.queueDurable(route, durableNumber(input, "queuedBytes"), deadline);
            if (result.ticket) {
              receipt = 'A';
              connection.durableCredit = *result.ticket;
              connection.durableRoute = std::move(route);
              connection.durableDeadline = deadline;
              durableWaiting.emplace(result.ticket->event, fd);
            } else if (result.status == Admission::Status::OVERLOADED) {
              receipt = 'R';
            } else if (result.status == Admission::Status::EXPIRED) {
              receipt = 'E';
            } else if (result.status == Admission::Status::RETIRING) {
              receipt = 'I';
            }
          });
          if (error != kj::none || send(fd, &receipt, 1, MSG_DONTWAIT | MSG_NOSIGNAL) != 1 ||
              receipt != 'A') {
            closeConnection(found);
          }
          continue;
        }
        if (connection.input.find("\r\n\r\n") != std::string::npos) {
          auto body = server.identity;
          if (connection.input.starts_with("GET /__coworkerd/threaded ")) {
            uint busy = 0;
            if (!server.durableAdmission && server.ready.load(std::memory_order_acquire))
              for (auto &thread: server.threads)
                if (!thread->control && thread->invocation.load()) ++busy;
            body = "{\"threadLimit\":" + std::to_string(server.limit) +
                ",\"busyThreads\":" + std::to_string(busy) +
                ",\"idleConnections\":" + std::to_string(connections.size() - 1) +
                ",\"compilations\":" + std::to_string(server.compilations.load()) +
                ",\"isolateCreationAttempts\":" +
                std::to_string(server.isolateCreationAttempts.load()) +
                ",\"canceledLoads\":" + std::to_string(server.canceledLoads.load()) +
                ",\"liveThreads\":" + std::to_string(server.liveThreads.load()) +
                ",\"controlThreads\":" +
                std::to_string(server.durableAdmission ? 0 : server.CONTROL_THREADS) +
                ",\"threadGrowthFailures\":" + std::to_string(server.growthFailures.load()) +
                ",\"acceptFailures\":" + std::to_string(server.acceptFailures.load()) +
                ",\"subrequestsActive\":" + std::to_string(server.subrequestBudget.active.load()) +
                ",\"subrequestsRejected\":" +
                std::to_string(server.subrequestBudget.rejected.load()) +
                ",\"logAccepted\":" + std::to_string(server.logAccepted.load()) +
                ",\"logDroppedLines\":" + std::to_string(server.logDroppedLines.load()) +
                ",\"logDroppedBytes\":" + std::to_string(server.logDroppedBytes.load()) +
                ",\"logFlushFailures\":" + std::to_string(server.logFlushFailures.load()) +
                ",\"logPeakQueuedBytes\":" + std::to_string(server.logPeakQueuedBytes.load()) + "}";
            if (server.durableAdmission) server.reportDurable(false, &body);
          }
          const bool walReady = !server.durableLog || server.durableLog->ready();
          auto status = server.ready.load(std::memory_order_acquire) && walReady
              ? "200 OK" : "503 Starting";
          if (!connection.input.starts_with("GET /__coworkerd/probe ") &&
              !connection.input.starts_with("GET /__coworkerd/threaded ")) {
            status = "501 Not Implemented";
            body = "{\"code\":\"NOT_SUPPORTED\",\"message\":\"threaded "
                   "lifecycle controls require S1c\"}";
          }
          connection.output = "HTTP/1.1 " + std::string(status) +
              "\r\nX-Coworkerd-Threaded-Protocol: 3" +
              (server.durableLog
                      ? "\r\nX-Coworkerd-Threaded-Role: durable\r\nX-Coworkerd-WAL-Protocol: 1"
                      : server.durableStorage ? "\r\nX-Coworkerd-Threaded-Role: durable-test" : "") +
              "\r\nContent-Length: " + std::to_string(body.size()) +
              "\r\nConnection: close\r\n\r\n" + body;
        }
      }
      if (!connection.output.empty() && !close) {
        auto size = send(fd, connection.output.data() + connection.sent,
            connection.output.size() - connection.sent, MSG_DONTWAIT | MSG_NOSIGNAL);
        if (size > 0)
          connection.sent += size;
        else if (size == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
          close = true;
        if (connection.sent == connection.output.size())
          close = true;
        else {
          epoll_event event{};
          event.events = EPOLLOUT | EPOLLRDHUP;
          event.data.fd = fd;
          epoll_ctl(epoll, EPOLL_CTL_MOD, fd, &event);
        }
      }
      if (close || (events[i].events & EPOLLRDHUP)) {
        closeConnection(found);
      }
    }
    if (server.durableAdmission) {
      const auto expired = server.expireDurableAdmissions();
      for (const auto &ticket: expired) {
        auto socket = durableWaiting.find(ticket.event);
        if (socket != durableWaiting.end()) closeConnection(connections.find(socket->second));
      }
      if (nowNs() >= nextDurableReport) {
        server.reportDurable(true);
        nextDurableReport = nowNs() + 100000000;
      }
    }
  }
}
}  // namespace
}  // namespace workerd

int main(int argc, char **argv) {
  using namespace workerd;
  if (argc == 2 && strcmp(argv[1], "--sandbox-facts") == 0) {
    // The node's startup probe of this build: one [coworkerd-sandbox] row on stderr, with V8's mode
    // and this build's sandbox layout ("mode":"off" without the sandbox); exit 78 with the problem
    // when the sandbox would not be secure on this machine.
    auto basePlatform = jsg::defaultPlatform(0);
    server::WorkerdPlatform platform{*basePlatform};
    jsg::V8System v8{platform, {}, basePlatform.get()};
    server::requireSecureSandbox();
    // How many isolates' sandboxes fit in this machine's address space (the node's plan).
    if (v8::IsolateGroup::CanCreateNewGroups())
      fprintf(stderr, "[coworkerd-sandbox] {\"capacity\":true,\"groups\":%zu}\n", server::measureGroupCapacity());
    return 0;
  }
  const bool disposable = argc == 6 && strcmp(argv[5], "--durable-test-storage") == 0;
  const bool persistent = argc == 6 && strcmp(argv[5], "--durable-storage") == 0;
  if (argc != 5 && !disposable && !persistent) {
    fprintf(stderr,
        "usage: coworkerd-threaded LISTEN_FD SOURCE_ADDRESS "
        "IDENTITY INFLIGHT [--durable-test-storage|--durable-storage]\n");
    return 64;
  }
  char *end = nullptr;
  auto limit = strtoul(argv[4], &end, 10);
  if (!*argv[4] || !end || *end || limit == 0 || limit > 16384) return 64;
  if (disposable && getenv("COWORKERD_DO_WAL_FD") != nullptr) {
    fprintf(stderr, "durable test storage refuses a persistent durable log channel\n");
    return 78;
  }
  // Raise the durable soft limit only for startup and one home. Further homes
  // take descriptor credits from the inherited soft limit as they are created.
  if (!prepareDescriptors(disposable || persistent ? 1 : limit)) return 78;
  auto event = limitEventFd();
  if (event >= 0) {
    auto flags = fcntl(event, F_GETFL);
    if (flags < 0 || fcntl(event, F_SETFL, flags | O_NONBLOCK) != 0) {
      fprintf(stderr,
          "[threaded-start] "
          "{\"event\":\"startup-rejected\",\"resource\":\"event-channel\"}\n");
      return 78;
    }
  }
  try {
    auto wasmMiB = configuredCount("COWORKERD_THREADED_WASM_MAX_MIB", 128);
    KJ_REQUIRE(wasmMiB <= 65536, "invalid WebAssembly memory configuration");
    if (wasmMiB != 0) {
      auto flags = std::string("--wasm-max-mem-pages=") +
          std::to_string(std::min<uint64_t>(65536, wasmMiB * 16)) +
          " --enforce-global-heap-limit --maximum-global-heap-limit-factor=1";
      v8::V8::SetFlagsFromString(flags.c_str());
    }
  } catch (...) {
    fprintf(stderr,
        "[threaded-start] "
        "{\"event\":\"startup-rejected\",\"resource\":\"v8-configuration\"}\n");
    return 78;
  }
  signal(SIGPIPE, SIG_IGN);
  util::Autogate::initAutogateNamesForTest({}, util::IgnoreAllAutogatesEnv(true));
  setenv("COWORKERD_THREADED", "1", 1);
  const int listener = strtol(argv[1], nullptr, 10);
  const char *parentEnv = getenv("COWORKERD_PARENT_FD");
  const int parent = parentEnv == nullptr ? -1 : strtol(parentEnv, nullptr, 10);
  ThreadedServer *server;
  try {
    kj::Own<const kj::Directory> directory;
    kj::Own<server::ThreadedWalLog> wal;
    if (persistent) {
      installSqliteCustomAllocator();
      auto path = getenv("COWORKERD_DO_STORAGE_DIR");
      KJ_REQUIRE(path != nullptr && path[0] == '/', "persistent durable storage requires a volume");
      auto filesystem = kj::newDiskFilesystem();
      directory = filesystem->getRoot().openSubdir(filesystem->getCurrentPath().evalNative(path),
          kj::WriteMode::MODIFY);
      const auto descriptor = configuredCount("COWORKERD_DO_WAL_FD", 0);
      KJ_REQUIRE(descriptor > STDERR_FILENO && descriptor <= INT_MAX,
          "persistent durable storage requires an inherited WAL socket");
      const auto fd = static_cast<int>(descriptor);
      KJ_SYSCALL(fcntl(fd, F_SETFD, FD_CLOEXEC));
      const std::string_view identity(argv[3]);
      auto colon = identity.rfind(':');
      KJ_REQUIRE(colon != std::string_view::npos, "invalid durable child identity");
      const auto generation = durableNumber(identity.substr(colon + 1));
      const auto confirm = boundedDurableSetting("COWORKERD_DO_CONFIRM_TIMEOUT_MS", 5000, 29000);
      wal = kj::heap<server::ThreadedWalLog>(fd, generation, confirm * kj::MILLISECONDS);
    } else if (disposable) {
      directory = newDurableTestDirectory(listener);
    }
    server = new ThreadedServer(limit, argv[2], argv[3], parent, kj::mv(directory), kj::mv(wal));
    server->start();
  } catch (...) {
    fprintf(stderr,
        "[threaded-start] "
        "{\"event\":\"startup-rejected\",\"resource\":\"server-"
        "initialization\"}\n");
    return 78;
  }
  acceptLoop(*server, listener, parent);
  return 0;
}
