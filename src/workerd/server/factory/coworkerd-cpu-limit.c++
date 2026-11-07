#include "coworkerd-cpu-limit.h"
#include <workerd/util/autogate.h>
#include <csignal>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <limits>
#include <v8-isolate.h>

namespace workerd::server {
namespace {
void wakeLimitWatchdog() { LimitWatchdog::start().wake(); }
}

LimitWatchdog& LimitWatchdog::start() {
  // Destroyed at exit, which stops and joins the thread.
  static LimitWatchdog watchdog;
  return watchdog;
}
LimitWatchdog::LimitWatchdog(): thread([this]() { run(); }) {
  setLimitStateObserver(&wakeLimitWatchdog);
}
LimitWatchdog::~LimitWatchdog() noexcept(false) {
  setLimitStateObserver(nullptr);
  state.lockExclusive()->stopping = true;
}
void LimitWatchdog::add(Entry& entry, kj::Maybe<kj::TimePoint> due) {
  auto lock = state.lockExclusive();
  lock->entries.add(&entry);
  KJ_IF_SOME(time, due) {
    KJ_IF_SOME(planned, lock->sleepUntil) {
      if (time < planned) lock->woken = true;
    } else {
      lock->woken = true;
    }
  }
}
void LimitWatchdog::wake() { state.lockExclusive()->woken = true; }
void LimitWatchdog::run() {
  // Process-directed signals go to other threads.
  sigset_t signals;
  sigfillset(&signals);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);
#if __APPLE__
  pthread_setname_np("limit-watchdog");
#else
  pthread_setname_np(pthread_self(), "limit-watchdog");
#endif
  auto lock = state.lockExclusive();
  while (!lock->stopping) {
    lock->woken = false;
    auto now = LimitWatchdog::now();
    kj::Maybe<kj::TimePoint> next;
    for (auto* entry: lock->entries) {
      KJ_IF_SOME(due, entry->watch(now)) {
        KJ_IF_SOME(earliest, next) {
          if (due < earliest) next = due;
        } else {
          next = due;
        }
      }
    }
    auto ready = [](const State& s) { return s.stopping || s.woken; };
    KJ_IF_SOME(due, next) {
      // A long sleep can end late by an amount that grows with its length (timer coalescing, as
      // on macOS hosts and their virtual machines), so a due time is approached in halving steps
      // and only the last one, at most 1 ms, sleeps until it.
      auto from = LimitWatchdog::now();
      auto remaining = due - from;
      if (remaining > 1 * kj::MILLISECONDS) remaining = remaining / 2;
      if (remaining < 0 * kj::NANOSECONDS) remaining = 0 * kj::NANOSECONDS;
      lock->sleepUntil = from + remaining;
      lock.wait(ready, remaining);
    } else {
      lock->sleepUntil = kj::none;
      lock.wait(ready);
    }
  }
}
MemoryStartupIdentityScope::MemoryStartupIdentityScope(kj::StringPtr identity)
    : previous(memoryStartupIdentitySlot()) { memoryStartupIdentitySlot() = identity; }
MemoryStartupIdentityScope::~MemoryStartupIdentityScope() { memoryStartupIdentitySlot() = previous; }

kj::Arc<jsg::AsyncContextFrame::StorageKey> memoryExecutionKey() {
  static const auto key = kj::arc<jsg::AsyncContextFrame::StorageKey>();
  return key.addRef();
}
kj::String memoryExecutionIdentity(jsg::Lock& lock) {
  KJ_IF_SOME(frame, jsg::AsyncContextFrame::current(lock)) {
    KJ_IF_SOME(value, frame.get(*memoryExecutionKey())) {
      return lock.toString(value.getHandle(lock));
    }
  }
  return kj::str("");
}
uint32_t cpuLimitFromEnvironment() {
  const char* text = std::getenv("COWORKERD_CPU_LIMIT_MS");
  if (text == nullptr) return 0;
  KJ_REQUIRE(*text != '\0', "COWORKERD_CPU_LIMIT_MS must be an unsigned 32-bit integer");
  uint64_t value = 0;
  for (const char* p = text; *p; ++p) {
    KJ_REQUIRE(*p >= '0' && *p <= '9', "COWORKERD_CPU_LIMIT_MS must be an unsigned 32-bit integer");
    value = value * 10 + (*p - '0');
    KJ_REQUIRE(value <= std::numeric_limits<uint32_t>::max(), "COWORKERD_CPU_LIMIT_MS exceeds uint32");
  }
  return static_cast<uint32_t>(value);
}
int coworkerdPid() {
  static const int pid = getpid();
  return pid;
}
int cpuEventsDescriptor() {
  static const int descriptor = []() {
    const char* text = std::getenv("COWORKERD_EVENT_FD");
    if (text == nullptr) return -1;
    KJ_REQUIRE(*text, "COWORKERD_EVENT_FD must be a descriptor number");
    int value = 0;
    for (const char* p = text; *p; ++p) {
      KJ_REQUIRE(*p >= '0' && *p <= '9' && value < 1000000,
          "COWORKERD_EVENT_FD must be a descriptor number");
      value = value * 10 + (*p - '0');
    }
    KJ_REQUIRE(value > STDERR_FILENO, "invalid event descriptor");
    int flags = fcntl(value, F_GETFD);
    KJ_REQUIRE(flags >= 0, "COWORKERD_EVENT_FD is not an open descriptor");
    KJ_REQUIRE(fcntl(value, F_SETFD, flags | FD_CLOEXEC) == 0, "cannot protect event descriptor");
    return value;
  }();
  return descriptor;
}
void writeLimitEvent(kj::StringPtr row) {
  auto fd = cpuEventsDescriptor();
  if (fd < 0) return;
  // One unbuffered write per complete row; EINTR before any bytes is retried.
  ssize_t written;
  do { written = write(fd, row.cStr(), row.size()); } while (written < 0 && errno == EINTR);
}
namespace {
class InheritedMapping {
 public:
  InheritedMapping(const char* variable, size_t expectedSize, int protection, bool exactSize = false) {
    const char* text = std::getenv(variable);
    if (!text) return;
    KJ_REQUIRE(*text, "descriptor setting must be a number", variable);
    int fd = 0;
    for (auto* p = text; *p; ++p) {
      KJ_REQUIRE(*p >= '0' && *p <= '9' && fd < 1000000,
          "descriptor setting must be a number", variable);
      fd = fd * 10 + (*p - '0');
    }
    KJ_REQUIRE(fd > STDERR_FILENO, "invalid inherited descriptor", variable);
    int flags = fcntl(fd, F_GETFD);
    KJ_REQUIRE(flags >= 0 && fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0,
        "invalid inherited descriptor", variable);
    auto closeFd = kj::defer([fd]() { close(fd); });
    struct stat info;
    // POSIX shared memory may be rounded up to the host page size.
    KJ_REQUIRE(fstat(fd, &info) == 0 &&
        (exactSize ? info.st_size == expectedSize : info.st_size >= expectedSize),
        "invalid inherited shared memory", variable);
    auto mapping = mmap(nullptr, expectedSize, protection, MAP_SHARED, fd, 0);
    KJ_REQUIRE(mapping != MAP_FAILED, "cannot map inherited shared memory", variable);
    base = mapping;
    size = expectedSize;
  }
  ~InheritedMapping() { if (base) munmap(base, size); }
  void* base = nullptr;
 private:
  size_t size = 0;
};
}  // namespace

void* coworkerdLifecycleMapping() {
  static InheritedMapping lifecycle("COWORKERD_LIFECYCLE_FD",
      64 + 2 * (16 + size_t(8 << 20)), PROT_READ | PROT_WRITE);
  if (lifecycle.base) {
    auto* header = static_cast<const uint32_t*>(lifecycle.base);
    KJ_REQUIRE(header[2] == 0x434c5743 && header[3] == (8 << 20), "invalid lifecycle channel");
  }
  return lifecycle.base;
}

const uint32_t* coworkerdMemoryBandMapping() {
  static InheritedMapping band("COWORKERD_MEMORY_BAND_FD", 4096, PROT_READ, true);
  return static_cast<const uint32_t*>(band.base);
}

void initializeCoworkerdChannels() {
  coworkerdPid();
  cpuEventsDescriptor();
  coworkerdLifecycleMapping();
  coworkerdMemoryBandMapping();
}

void recordPlatformFacts(kj::StringPtr name, bool dynamic) {
  auto line = kj::str("[coworkerd-platform] {\"isolate\":", escapeCpuJsonString(name),
      ",\"dynamic\":", dynamic ? "true" : "false", ",\"canCreateNewIsolateGroups\":",
      v8::IsolateGroup::CanCreateNewGroups() ? "true" : "false", ",\"startupSnapshotAutogate\":",
      util::Autogate::isEnabled(util::AutogateKey::STARTUP_SNAPSHOT) ? "true" : "false", "}\n");
  ssize_t written;
  do { written = write(STDERR_FILENO, line.cStr(), line.size()); } while (written < 0 && errno == EINTR);
}
kj::String escapeCpuJsonString(kj::StringPtr text) {
  static const char HEXDIGITS[] = "0123456789abcdef";
  kj::Vector<char> escaped(text.size() + 1);

  for (char c: text) {
    switch (c) {
      case '"':
        escaped.addAll("\\\""_kj);
        break;
      case '\\':
        escaped.addAll("\\\\"_kj);
        break;
      case '\b':
        escaped.addAll("\\b"_kj);
        break;
      case '\f':
        escaped.addAll("\\f"_kj);
        break;
      case '\n':
        escaped.addAll("\\n"_kj);
        break;
      case '\r':
        escaped.addAll("\\r"_kj);
        break;
      case '\t':
        escaped.addAll("\\t"_kj);
        break;
      default:
        if (static_cast<uint8_t>(c) < 0x20) {
          escaped.addAll("\\u00"_kj);
          uint8_t c2 = c;
          escaped.add(HEXDIGITS[c2 / 16]);
          escaped.add(HEXDIGITS[c2 % 16]);
        } else {
          escaped.add(c);
        }
        break;
    }
  }

  return kj::str("\"", escaped.releaseAsArray(), "\"");
}

namespace {
// Deferred body proxying outlives IoContext; revoke it through the complete request promise.
class CpuWorkerInterface final: public WorkerInterface {
 public:
  CpuWorkerInterface(kj::Own<WorkerInterface> inner, kj::Own<CpuIsolateLimitEnforcer> limits)
      : inner(kj::mv(inner)), limits(kj::mv(limits)) {}
  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url,
      const kj::HttpHeaders& headers, kj::AsyncInputStream& body,
      kj::HttpService::Response& response) override {
    limits->requireNotExceeded();
    return inner->request(method, url, headers, body, response).exclusiveJoin(limits->onExceeded());
  }
  kj::Promise<void> connect(kj::StringPtr host, const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection, ConnectResponse& response,
      kj::HttpConnectSettings settings) override {
    return inner->connect(host, headers, connection, response, kj::mv(settings));
  }
  kj::Promise<void> prewarm(kj::StringPtr url) override { return inner->prewarm(url); }
  kj::Promise<ScheduledResult> runScheduled(kj::Date time, kj::StringPtr cron) override {
    return inner->runScheduled(time, cron);
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date time, uint32_t retry) override {
    return inner->runAlarm(time, retry);
  }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return inner->customEvent(kj::mv(event));
  }
 private:
  kj::Own<WorkerInterface> inner;
  kj::Own<CpuIsolateLimitEnforcer> limits;
};



class RequestLimits final: public LimitEnforcer {
   public:
    RequestLimits(kj::Own<LimitEnforcer> fallback, CpuIsolateLimitEnforcer& limits,
        kj::String identity, kj::Function<void()> abortIsolate, bool isActor)
        : fallback(kj::mv(fallback)), limits(kj::addRef(limits)),
          abortIsolate(kj::mv(abortIsolate)), identity(kj::mv(identity)), isActor(isActor) {}
    // The request context ends here, after its waitUntil work, whether or not it ran JavaScript. An actor's context
    // ends with the actor: its row has the CPU of all its events.
    ~RequestLimits() noexcept(false) { limits->completed(identity, toppedUp + cpuUsed, gcExcluded); }
    struct IdentityScope {
      jsg::AsyncContextFrame::StorageScope frame;
      MemoryStartupIdentityScope startup;
      kj::Own<void> limit;
      IdentityScope(jsg::Lock& lock, RequestLimits& owner)
          : frame(lock, memoryExecutionKey(),
                lock.v8Ref<v8::Value>(jsg::v8Str(lock.v8Isolate, owner.identity))),
            startup(owner.identity),
            limit(owner.limits->enterRequestJs(owner.cpuUsed, owner.identity,
                [&owner]() { if (owner.limits->publish()) owner.abortIsolate(); }, &owner.gcExcluded)) {}
    };
    kj::Own<void> enterJs(jsg::Lock& lock, IoContext&) override {
      if (limits->publish()) abortIsolate();
      limits->requireNotExceeded();
      if (limits->memoryIdentityWanted()) return kj::heap<IdentityScope>(lock, *this);
      return limits->enterRequestJs(cpuUsed, identity, [this]() { if (limits->publish()) abortIsolate(); }, &gcExcluded);
    }
    // Each new event a request receives starts with its CPU topped up: a message of a WebSocket it accepted, a call on
    // an RPC target it handed out, and -- upstream's contract, which tops up actors only -- an actor's request (an
    // actor's requests share one IoContext, and this enforcer, for the actor's whole life). A connection's messages are
    // thus limited one by one, as requests are, instead of together; what each charged stays in the completed row, and
    // while the request lasts its total so far goes on the event pipe at most once per PROGRESS_INTERVAL, so that the
    // node meters a long connection as it runs.
    void topUpActor() override {
      if (isActor) idleWorkerActorEvent(limits->activity());
      toppedUp += cpuUsed;
      cpuUsed = 0;
      const auto now = LimitWatchdog::now();
      if (toppedUp + gcExcluded > reported && now - lastProgress >= PROGRESS_INTERVAL) {
        lastProgress = now;
        reported = toppedUp + gcExcluded;
        limits->progress(identity, toppedUp, gcExcluded);
      }
    }
    void newSubrequest(bool inHouse) override { fallback->newSubrequest(inHouse); }
    void newKvRequest(KvOpType type) override { fallback->newKvRequest(type); }
    void newAnalyticsEngineRequest() override { fallback->newAnalyticsEngineRequest(); }
    kj::Maybe<kj::Own<void>> tryReserveActorCallReplayMemory(size_t bytes) override { return fallback->tryReserveActorCallReplayMemory(bytes); }
    kj::Promise<void> limitDrain() override { return fallback->limitDrain(); }
    kj::Promise<void> limitScheduled() override { return fallback->limitScheduled(); }
    kj::Duration getAlarmLimit() override { return fallback->getAlarmLimit(); }
    size_t getBufferingLimit() override { return fallback->getBufferingLimit(); }
    kj::Maybe<EventOutcome> getLimitsExceeded() override { return limits->outcome(); }
    kj::Promise<void> onLimitsExceeded() override { return limits->onExceeded(); }
    void setCpuLimitNearlyExceededCallback(kj::Function<void()>) override {}
    void requireLimitsNotExceeded() override {
      if (limits->publish()) abortIsolate();
      limits->requireNotExceeded();
    }
    void reportMetrics(RequestObserver&) override {}
    kj::Duration consumeTimeElapsedForPeriodicLogging() override { return 0 * kj::SECONDS; }
    size_t getSqliteMemoryUsage() const override { return 0; }
   private:
    kj::Own<LimitEnforcer> fallback;
    kj::Own<CpuIsolateLimitEnforcer> limits;
    kj::Function<void()> abortIsolate;
    kj::String identity;
    const bool isActor;
    // What the limit charged since the last top-up, before it, and the GC CPU it did not charge.
    uint64_t cpuUsed = 0, toppedUp = 0, gcExcluded = 0;
    // The last progress row: when, and the CPU it reported.
    static constexpr kj::Duration PROGRESS_INTERVAL = 1 * kj::SECONDS;
    kj::TimePoint lastProgress = kj::origin<kj::TimePoint>();
    uint64_t reported = 0;
  };


}  // namespace

kj::Own<LimitEnforcer> wrapCpuRequestLimits(kj::Own<LimitEnforcer> fallback,
    CpuIsolateLimitEnforcer& limits, kj::Maybe<kj::String>& metadata,
    kj::Function<void()> abortIsolate, bool isActor) {
  if (!limits.isEnabled() && !limits.memoryIdentityWanted() && !limits.reportsCompletion() &&
      !limits.activity()) return fallback;
  return kj::heap<RequestLimits>(kj::mv(fallback), limits, limits.identity(metadata),
      kj::mv(abortIsolate), isActor);
}
kj::Own<WorkerInterface> wrapCpuWorkerInterface(kj::Own<WorkerInterface> entry,
    CpuIsolateLimitEnforcer& limits) {
  if (!limits.isEnabled()) return entry;
  return kj::heap<CpuWorkerInterface>(kj::mv(entry), kj::addRef(limits));
}
}  // namespace workerd::server
