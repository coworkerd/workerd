#pragma once

#include <workerd/io/actor-cache.h>
#include <workerd/io/limit-enforcer.h>
#include <workerd/io/worker-interface.h>
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/function.h>
#include <kj/mutex.h>
#include <kj/thread.h>
#include <kj/time.h>
#include <kj/vector.h>
#include <atomic>
#include <cstdio>
#include <pthread.h>
#include <unistd.h>
#if __APPLE__
#include <mach/mach.h>
#endif

namespace workerd::server {
// Creation identity is distinct from per-request origin. The factory slot is empty
// until the trusted loader transports this record across the existing bridge.
struct CoworkerdWorkerIdentity {
  kj::String tenant;
  kj::String version;
  uint64_t generation = 0;
};
uint32_t cpuLimitFromEnvironment();
kj::String cpuStateFromEnvironment();
int cpuEventsDescriptor();
void writeLimitEvent(kj::StringPtr row);
kj::String escapeCpuJsonString(kj::StringPtr text);
void recordPlatformFacts(kj::StringPtr name, bool dynamic);

class LimitWatchdog {
 public:
  class Entry {
   public:
    // Called on the watchdog thread with the registry locked. Returns when the entry next needs a
    // call; none if only a wake-up can make it need one.
    virtual kj::Maybe<kj::TimePoint> watch(kj::TimePoint now) = 0;

   protected:
    ~Entry() = default;
  };
  static LimitWatchdog& start();
  static kj::TimePoint now() { return kj::systemPreciseMonotonicClock().now(); }
  void add(Entry& entry, kj::Maybe<kj::TimePoint> due);
  // `underLock` runs after removal with the registry still locked.
  template <typename Func>
  void remove(Entry& entry, Func&& underLock) {
    auto lock = state.lockExclusive();
    auto& entries = lock->entries;
    for (size_t i = 0; i < entries.size(); ++i) {
      if (entries[i] == &entry) {
        entries[i] = entries.back();
        entries.removeLast();
        break;
      }
    }
    underLock();
  }
  // Some limit state left NONE: entries may need a check before their due time.
  void wake();
  ~LimitWatchdog() noexcept(false);

 private:
  struct State {
    kj::Vector<Entry*> entries;
    kj::Maybe<kj::TimePoint> sleepUntil;  // none: until woken
    bool woken = false;
    bool stopping = false;
  };
  kj::MutexGuarded<State> state;
  kj::Thread thread;
  LimitWatchdog();
  void run();
};

// CPU accounting stays on the isolate thread; only termination and file writes cross threads.
class CpuIsolateLimitEnforcer final: public IsolateLimitEnforcer {
 public:
  CpuIsolateLimitEnforcer(kj::Own<IsolateLimitEnforcer> fallback, kj::StringPtr name, bool dynamic)
      : fallback(kj::mv(fallback)), name(kj::str(name)),
        cpuNs(uint64_t(cpuLimitFromEnvironment()) * (dynamic ? 1000000 : 0)),
        state(cpuStateFromEnvironment()) {
    if (isEnabled()) { cpuEventsDescriptor(); watchdog = &LimitWatchdog::start(); }
    recordPlatformFacts(name, dynamic);
    auto paf = kj::newPromiseAndFulfiller<void>();
    failure = paf.promise.fork();
    fulfiller = kj::mv(paf.fulfiller);
    if (cpuNs) record("created", "", 0);
  }

  v8::Isolate::CreateParams getCreateParams() override { return fallback->getCreateParams(); }
  ActorCacheSharedLruOptions getActorCacheLruOptions() override { return fallback->getActorCacheLruOptions(); }
  kj::Own<void> enterStartupPython(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override { return fallback->enterStartupPython(lock, result); }
  kj::Own<void> enterDynamicImportJs(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override { return fallback->enterDynamicImportJs(lock, result); }
  kj::Own<void> enterLoggingJs(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override { return fallback->enterLoggingJs(lock, result); }
  kj::Own<void> enterInspectorJs(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override { return fallback->enterInspectorJs(lock, result); }
  void completedRequest(kj::StringPtr id) const override { fallback->completedRequest(id); }
  bool exitJs(jsg::Lock& lock) const override { return fallback->exitJs(lock); }
  void reportMetrics(IsolateObserver& observer) const override { fallback->reportMetrics(observer); }
  kj::Maybe<size_t> checkPbkdfIterations(jsg::Lock& lock, size_t n) const override { return fallback->checkPbkdfIterations(lock, n); }
  kj::Maybe<uint64_t> checkScryptCost(jsg::Lock& lock, uint32_t n, uint32_t r, uint32_t p) const override { return fallback->checkScryptCost(lock, n, r, p); }
  size_t getBlobSizeLimit() const override { return fallback->getBlobSizeLimit(); }
  bool hasExcessivelyExceededHeapLimit() const override { return fallback->hasExcessivelyExceededHeapLimit(); }
  const TrackedWasmInstanceList& getTrackedWasmInstances() const override { return fallback->getTrackedWasmInstances(); }
  void markPerfEvent(kj::LiteralStringConst name) const override { fallback->markPerfEvent(name); }
  void customizeIsolate(v8::Isolate* value) override { isolate = value; }
  bool isEnabled() const { return cpuNs != 0; }
  kj::Maybe<EventOutcome> outcome() {
    return exceeded.load() ? kj::Maybe<EventOutcome>(EventOutcome::EXCEEDED_CPU) : kj::none;
  }
  kj::Exception exception() {
    return KJ_EXCEPTION(OVERLOADED, "jsg.Error: CPU_LIMIT_EXCEEDED");
  }
  void requireNotExceeded() {
    if (exceeded.load()) kj::throwFatalException(exception());
  }
  kj::Promise<void> onExceeded() { return failure.addBranch(); }
  bool publish() {
    if (!exceeded.load() || published) return false;
    published = true;
    fulfiller->reject(exception());
    return true;
  }

  // Metadata is copied before entering V8. Neither a tenant log nor an HTTP header is attribution.
  kj::String identity(kj::Maybe<kj::String>& metadata) {
    kj::String worker, version, invocation;
    KJ_IF_SOME(blob, metadata) {
      capnp::MallocMessageBuilder arena;
      auto value = arena.initRoot<capnp::JsonValue>();
      capnp::JsonCodec().decodeRaw(blob, value);
      if (value.isObject()) for (auto field: value.getObject()) {
        if (!field.getValue().isString()) continue;
        auto text = field.getValue().getString();
        if (field.getName() == "workerId") worker = kj::str(text);
        if (field.getName() == "versionId") version = kj::str(text);
        if (field.getName() == "invocationId") invocation = kj::str(text);
      }
    }
    return kj::str("\"workerId\":", escapeCpuJsonString(worker), ",\"versionId\":",
        escapeCpuJsonString(version), ",\"invocationId\":", escapeCpuJsonString(invocation), ',');
  }

  kj::String report(kj::StringPtr event, kj::StringPtr identity, uint64_t used) {
    return kj::str("{\"event\":", escapeCpuJsonString(event), ",\"isolate\":", escapeCpuJsonString(name),
        ',', identity, "\"cpuUsedNs\":", used, ",\"cpuLimitMs\":", cpuNs / 1000000,
        ",\"pid\":", getpid(), "}\n");
  }
  void record(kj::StringPtr event, kj::StringPtr identity, uint64_t used) {
    auto row = report(event, identity, used);
    // All workerd processes append to one log: emit the prefix and JSON in one syscall.
    auto log = kj::str("[cpu-limit] ", row);
    ssize_t written;
    do { written = write(STDERR_FILENO, log.cStr(), log.size()); } while (written < 0 && errno == EINTR);
    writeLimitEvent(row);
  }
  void culprit(kj::StringPtr identity, uint64_t used) {
    if (state.size() == 0) return;
    auto row = report("culprit", identity, used);
    auto path = kj::str(state, ".tmp");
    if (auto* file = fopen(path.cStr(), "w")) {
      auto written = fwrite(row.cStr(), 1, row.size(), file);
      auto closed = fclose(file);
      if (written == row.size() && closed == 0) rename(path.cStr(), state.cStr());
      else unlink(path.cStr());
    }
  }

  struct CpuClock {
#if __APPLE__
    mach_port_t thread = mach_thread_self();
    ~CpuClock() { mach_port_deallocate(mach_task_self(), thread); }
    uint64_t now() const {
      thread_basic_info_data_t info;
      mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
      KJ_REQUIRE(thread_info(thread, THREAD_BASIC_INFO,
          reinterpret_cast<thread_info_t>(&info), &count) == KERN_SUCCESS);
      return (uint64_t(info.user_time.seconds) + info.system_time.seconds) * 1000000000
          + (uint64_t(info.user_time.microseconds) + info.system_time.microseconds) * 1000;
    }
#else
    clockid_t clock;
    CpuClock() { KJ_REQUIRE(pthread_getcpuclockid(pthread_self(), &clock) == 0); }
    uint64_t now() const {
      timespec time;
      KJ_REQUIRE(clock_gettime(clock, &time) == 0);
      return uint64_t(time.tv_sec) * 1000000000 + time.tv_nsec;
    }
#endif
  };

  struct Scope final: public LimitWatchdog::Entry {
    static constexpr uint64_t MIN_RECHECK_NS = 100000;
    CpuIsolateLimitEnforcer& owner;
    uint64_t& used;
    kj::String identity;
    kj::Function<void()> onExit;
    CpuClock clock;
    uint64_t started = clock.now();
    bool terminated = false;
    bool finished = false;
    bool reported = false;
    kj::TimePoint graceEnd = kj::origin<kj::TimePoint>();

    Scope(CpuIsolateLimitEnforcer& owner, uint64_t& used, kj::StringPtr identity,
        kj::Function<void()> onExit)
        : owner(owner), used(used), identity(kj::str(identity)), onExit(kj::mv(onExit)) {
      owner.watchdog->add(*this, firstCheck());
    }
    kj::Maybe<kj::TimePoint> firstCheck() const {
      auto now = LimitWatchdog::now();
      if (owner.exceeded.load()) return now;
      return now + static_cast<int64_t>(used < owner.cpuNs ? owner.cpuNs - used : 0) * kj::NANOSECONDS;
    }
    kj::Maybe<kj::TimePoint> watch(kj::TimePoint now) override {
      if (finished) return kj::none;
      if (terminated) {
        if (now < graceEnd) return graceEnd;
        auto consumed = clock.now() - started + used;
        if (consumed >= owner.cpuNs) {
          owner.culprit(identity, consumed);
          reported = true;
          finished = true;
          return kj::none;
        }
        terminated = false;
        return now + 1 * kj::MILLISECONDS;
      }
      auto consumed = clock.now() - started + used;
      if (consumed >= owner.cpuNs || owner.exceeded.load()) {
        owner.exceeded.store(true);
        owner.isolate->TerminateExecution();
        terminated = true;
        graceEnd = now + 100 * kj::MILLISECONDS;
        return graceEnd;
      }
      return now + static_cast<int64_t>(kj::max(owner.cpuNs - consumed, MIN_RECHECK_NS)) * kj::NANOSECONDS;
    }
    ~Scope() noexcept(false) {
      owner.watchdog->remove(*this, [this]() { used += clock.now() - started; });
      if (used >= owner.cpuNs && !owner.exceeded.exchange(true)) owner.watchdog->wake();
      if (reported && owner.state.size()) unlink(owner.state.cStr());
      if (owner.exceeded.load()) owner.record("terminated", identity, used);
      onExit();
    }
  };
  kj::Own<void> enterRequestJs(uint64_t& used, kj::StringPtr identity, kj::Function<void()> onExit) {
    requireNotExceeded();
    if (!cpuNs) return {};
    return kj::heap<Scope>(*this, used, identity, kj::mv(onExit));
  }
  kj::Own<void> enterStartupJs(jsg::Lock&, kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    if (!cpuNs) return {};
    auto& self = const_cast<CpuIsolateLimitEnforcer&>(*this);
    return kj::heap<Scope>(self, self.startupUsed, "", [&self, &result]() {
      if (self.exceeded.load()) { self.publish(); result = self.exception(); }
      else result = static_cast<int64_t>(self.startupUsed) * kj::NANOSECONDS;
    });
  }
 private:
  kj::Own<IsolateLimitEnforcer> fallback;
  kj::String name;
  uint64_t cpuNs;
  kj::String state;
  v8::Isolate* isolate = nullptr;
  uint64_t startupUsed = 0;
  LimitWatchdog* watchdog = nullptr;
  std::atomic<bool> exceeded{false};
  bool published = false;
  kj::ForkedPromise<void> failure = nullptr;
  kj::Own<kj::PromiseFulfiller<void>> fulfiller;
};

kj::Own<LimitEnforcer> wrapCpuRequestLimits(kj::Own<LimitEnforcer> fallback,
    CpuIsolateLimitEnforcer& limits, kj::Maybe<kj::String>& metadata,
    kj::Function<void()> abortIsolate);
kj::Own<WorkerInterface> wrapCpuWorkerInterface(kj::Own<WorkerInterface> entry,
    CpuIsolateLimitEnforcer& limits);
}  // namespace workerd::server
