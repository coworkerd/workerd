#pragma once

#include <workerd/jsg/coworkerd-rpc-code.h>
#include "coworkerd-gc.h"
#include "coworkerd-idle-eviction.h"
#include <workerd/server/coworkerd-memory-limit.h>

#include <workerd/io/actor-cache.h>
#include <workerd/io/limit-enforcer.h>
#include <workerd/io/worker-interface.h>
#include <workerd/jsg/async-context.h>
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
uint32_t cpuLimitFromEnvironment();
// The limits one Worker names (Coworkerd v49), as the loader key the node's runtime bridge chose carries them:
// the isolate name's last segment `limits=<cpuMs>:<memoryMiB>` (either may be 0: the process's value, which
// COWORKERD_CPU_LIMIT_MS and COWORKERD_MEMORY_LIMIT_MIB give, applies to it instead). Both are zero for any
// other name, so an isolate whose Worker names no limit behaves as before. An isolate's heap constraint is
// fixed when it is created, and a Worker's limits are part of the key it is loaded under, so a Worker whose
// limits change is loaded as another isolate rather than kept at the old ones.
struct CoworkerdNamedLimits {
  uint32_t cpuMs = 0, memoryMiB = 0;
};
CoworkerdNamedLimits coworkerdNamedLimits(kj::StringPtr isolateName);
kj::String escapeCpuJsonString(kj::StringPtr text);
void recordPlatformFacts(kj::StringPtr name, bool dynamic);
kj::Arc<jsg::AsyncContextFrame::StorageKey> memoryExecutionKey();
kj::String memoryExecutionIdentity(jsg::Lock& lock);
class MemoryStartupIdentityScope {
 public:
  explicit MemoryStartupIdentityScope(kj::StringPtr identity);
  ~MemoryStartupIdentityScope();
 private:
  kj::StringPtr previous;
};
kj::String captureMemoryStartupIdentity();

// The process's one limit watchdog thread, started by the first enabled limit enforcer. A JS
// entry registers for as long as it runs. The thread sleeps toward the earliest time a registered
// entry needs a check: when its CPU deadline can first arrive (thread CPU time never advances
// faster than the monotonic clock), when a termination grace period ends, or when a limit state
// leaves NONE elsewhere and wakes it. Registering and unregistering make no system call unless
// the new entry needs a check before the thread's planned wake-up.
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

// CPU accounting stays on the isolate thread; only termination and event writes cross threads.
class CpuIsolateLimitEnforcer final: public IsolateLimitEnforcer {
 public:
  CpuIsolateLimitEnforcer(kj::Own<IsolateLimitEnforcer> fallback, kj::StringPtr name, bool dynamic,
      IdleWorkerRef idleWorker = nullptr, kj::StringPtr startupIdentity = "")
      : fallback(kj::mv(fallback)), name(kj::str(name)), named(coworkerdNamedLimits(name)),
        cpuNs(uint64_t(named.cpuMs ? named.cpuMs : cpuLimitFromEnvironment()) * (dynamic ? 1000000 : 0)),
        dynamic(dynamic), idleWorker(kj::mv(idleWorker)),
        startupIdentity(kj::str(startupIdentity)) {
    if (dynamic) {
      auto limit = named.memoryMiB ? named.memoryMiB : memoryLimitFromEnvironment();
      if (limit) memory = std::make_shared<IsolateMemoryLimit>(limit, exceeded);
    }
    // The isolate's collections (coworkerd-gc.h): what its CPU limit excludes, its long pauses, its idle collection.
    if (gcSettings().observes()) {
      gc = std::make_shared<IsolateGc>(name, dynamic, cpuNs);
      idleWorkerSetGc(this->idleWorker, gc);
    }
    if (isEnabled()) watchdog = &LimitWatchdog::start();
    recordPlatformFacts(name, dynamic);
    auto paf = kj::newPromiseAndFulfiller<void>();
    failure = paf.promise.fork();
    fulfiller = kj::mv(paf.fulfiller);
    if (cpuNs) record("created", "", 0);
    if (memory) record("memory-created", "", 0);
  }

  v8::Isolate::CreateParams getCreateParams() override {
    auto params = fallback->getCreateParams();
    if (memory) {
      params.constraints.ConfigureDefaultsFromHeapSize(0, memory->limitBytes);
      params.array_buffer_allocator_shared = memory;
    } else if (!dynamic && gcSettings().staticHeapBytes) {
      // The runtime: a heap sized for its working set rather than from the machine, so its collections stay small
      // (its young generation follows from the maximum).
      params.constraints.ConfigureDefaultsFromHeapSize(0, gcSettings().staticHeapBytes);
    }
    return params;
  }
  ActorCacheSharedLruOptions getActorCacheLruOptions() override { return fallback->getActorCacheLruOptions(); }
  kj::Own<void> enterStartupPython(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    if (memory) return enterAuxiliaryJs(lock, result);
    return fallback->enterStartupPython(lock, result);
  }
  kj::Own<void> enterDynamicImportJs(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    if (memory) return enterAuxiliaryJs(lock, result);
    return fallback->enterDynamicImportJs(lock, result);
  }
  kj::Own<void> enterLoggingJs(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    if (memory) return enterAuxiliaryJs(lock, result);
    return fallback->enterLoggingJs(lock, result);
  }
  kj::Own<void> enterInspectorJs(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    if (memory) return enterAuxiliaryJs(lock, result);
    return fallback->enterInspectorJs(lock, result);
  }
  void completedRequest(kj::StringPtr id) const override { fallback->completedRequest(id); }
  bool exitJs(jsg::Lock& lock) const override { return fallback->exitJs(lock); }
  void reportMetrics(IsolateObserver& observer) const override { fallback->reportMetrics(observer); }
  kj::Maybe<size_t> checkPbkdfIterations(jsg::Lock& lock, size_t n) const override { return fallback->checkPbkdfIterations(lock, n); }
  kj::Maybe<uint64_t> checkScryptCost(jsg::Lock& lock, uint32_t n, uint32_t r, uint32_t p) const override { return fallback->checkScryptCost(lock, n, r, p); }
  size_t getBlobSizeLimit() const override { return fallback->getBlobSizeLimit(); }
  bool hasExcessivelyExceededHeapLimit() const override { return fallback->hasExcessivelyExceededHeapLimit(); }
  const TrackedWasmInstanceList& getTrackedWasmInstances() const override { return fallback->getTrackedWasmInstances(); }
  void markPerfEvent(kj::LiteralStringConst name) const override { fallback->markPerfEvent(name); }
  void customizeIsolate(v8::Isolate* value) override {
    isolate = value;
    if (memory) memory->customize(value);
    if (gc) gc->install(value);
  }
  bool isEnabled() const { return cpuNs != 0 || memory != nullptr; }
  // Whether a request of this isolate carries the memory execution identity (v33's attribution of a fatal
  // OOM): this isolate has a memory limit -- its Worker's own (v49) or the process's -- or the process has
  // one while this isolate has none, as before v49, so that every isolate of a process with a memory limit
  // keeps reporting who was executing.
  bool memoryIdentityWanted() const { return memory != nullptr || memoryLimitFromEnvironment() != 0; }
  // The limits in force here, whichever named them: what the state endpoint shows as applied.
  CoworkerdNamedLimits applied() const {
    return {static_cast<uint32_t>(cpuNs / 1000000), memory ? static_cast<uint32_t>(memory->limitBytes >> 20) : 0};
  }
  // A dynamic Worker's request contexts report their end on the event pipe, limits or not.
  bool reportsCompletion() const { return dynamic && cpuEventsDescriptor() >= 0; }
  const IdleWorkerRef& activity() const { return idleWorker; }
  kj::Maybe<EventOutcome> outcome() {
    if (exceeded->load() == IsolateLimitCause::NONE) return kj::none;
    return exceeded->load() == IsolateLimitCause::MEMORY ? EventOutcome::EXCEEDED_MEMORY : EventOutcome::EXCEEDED_CPU;
  }
  kj::Exception exception() {
    return exceeded->load() == IsolateLimitCause::MEMORY
        ? jsg::tagCoworkerdRpcError(KJ_EXCEPTION(OVERLOADED, "jsg.Error: MEMORY_LIMIT_EXCEEDED"),
              "MEMORY_LIMIT_EXCEEDED")
        : jsg::tagCoworkerdRpcError(KJ_EXCEPTION(OVERLOADED, "jsg.Error: CPU_LIMIT_EXCEEDED"),
              "CPU_LIMIT_EXCEEDED");
  }
  void requireNotExceeded() {
    if (exceeded->load() != IsolateLimitCause::NONE) kj::throwFatalException(exception());
  }
  kj::Promise<void> onExceeded() { return failure.addBranch(); }
  bool publish() {
    if (exceeded->load() == IsolateLimitCause::NONE || published) return false;
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

  // `gcExcluded`: the GC CPU the limit did not charge (coworkerd-gc.h), in the row only when there was some.
  kj::String report(kj::StringPtr event, kj::StringPtr identity, uint64_t used, uint64_t gcExcluded = 0) {
    return kj::str("{\"event\":", escapeCpuJsonString(event), ",\"isolate\":", escapeCpuJsonString(name),
        ',', identity, "\"cpuUsedNs\":", used, ",\"cpuLimitMs\":", cpuNs / 1000000,
        ",\"pid\":", coworkerdPid(), memory ? memory->metrics() : kj::str(""),
        gcExcluded ? kj::str(",\"gcExcludedNs\":", gcExcluded) : kj::str(""), "}\n");
  }
  void record(kj::StringPtr event, kj::StringPtr identity, uint64_t used, uint64_t gcExcluded = 0) {
    auto row = report(event, identity, used, gcExcluded);
    if (isEnabled()) {
      // All workerd processes append to one log: emit the prefix and JSON in one syscall.
      auto log = kj::str("[cpu-limit] ", row);
      ssize_t written;
      do { written = write(STDERR_FILENO, log.cStr(), log.size()); } while (written < 0 && errno == EINTR);
    }
    writeLimitEvent(row);
  }
  // The end of a dynamic Worker's request context (after its waitUntil work), whether or not it
  // ever ran JavaScript; without limits only the event pipe gets the row. `used` is what the limit charged, and
  // `gcExcluded` the GC CPU it did not: the request's CPU is their sum.
  void completed(kj::StringPtr identity, uint64_t used, uint64_t gcExcluded) {
    if (dynamic) record("completed", identity, used, gcExcluded);
  }
  // A request's CPU so far while it lasts -- a WebSocket's events top it up, and its context ends only with the
  // connection --, in the completed row's terms: on the event pipe only, where the node meters it, never in the log.
  void progress(kj::StringPtr identity, uint64_t used, uint64_t gcExcluded) {
    if (dynamic) writeLimitEvent(report("cpu-progress", identity, used, gcExcluded));
  }
  // Written while a termination has not taken effect; a later terminated row for the same
  // invocation withdraws it.
  void culprit(kj::StringPtr identity, uint64_t used) { writeLimitEvent(report("culprit", identity, used)); }

  using CpuClock = ThreadCpuClock;

  // One JS entry, registered with the watchdog while it runs. Its watch state is touched only by
  // the watchdog thread with the registry locked, and read here after unregistering.
  struct Scope final: public LimitWatchdog::Entry {
    // Below the remaining budget, the watchdog rechecks at this interval rather than sooner.
    static constexpr uint64_t MIN_RECHECK_NS = 100000;
    CpuIsolateLimitEnforcer& owner;
    uint64_t& used;
    kj::String identity;
    kj::Function<void()> onExit;
    // The request's excluded GC CPU (the gcExcludedNs of its rows), or null; and this entry inside the isolate's
    // collections, which tells it what to exclude.
    uint64_t* gcExcluded;
    IsolateGc::Entry gcEntry;
    CpuClock clock;
    uint64_t started = clock.now();
    bool terminated = false;
    bool finished = false;
    kj::TimePoint graceEnd = kj::origin<kj::TimePoint>();
    bool reported = false;
    kj::String oomReport;
    MemoryExecution previousMemory;

    Scope(CpuIsolateLimitEnforcer& owner, uint64_t& used, kj::StringPtr identity,
        kj::Function<void()> onExit, uint64_t* gcExcluded = nullptr)
        : owner(owner), used(used), identity(kj::str(identity)), onExit(kj::mv(onExit)), gcExcluded(gcExcluded),
          gcEntry(owner.gc.get(), this->identity) {
      if (owner.memory) {
        oomReport = owner.report("memory-culprit", this->identity, 0);
        previousMemory = owner.memory->enter(oomReport);
      }
      // Last: from here on the watchdog thread may call watch().
      owner.watchdog->add(*this, firstCheck());
    }
    kj::Maybe<kj::TimePoint> firstCheck() const {
      auto now = LimitWatchdog::now();
      if (owner.exceeded->load() != IsolateLimitCause::NONE) return now;
      if (!owner.cpuNs) return kj::none;
      return now + static_cast<int64_t>(used < owner.cpuNs ? owner.cpuNs - used : 0) * kj::NANOSECONDS;
    }
    // One check of this entry: terminate once the CPU budget is spent or the limit state left
    // NONE; when the grace period ends, report the culprit if the termination has not taken effect.
    kj::Maybe<kj::TimePoint> watch(kj::TimePoint now) override {
      if (finished) return kj::none;
      if (terminated) {
        if (now < graceEnd) return graceEnd;
        auto finalCpu = clock.now();
        auto finalConsumed = finalCpu - started - gcEntry.excluded(started, finalCpu) + used;
        if (owner.cpuNs && finalConsumed >= owner.cpuNs) {
          owner.culprit(identity, finalConsumed);
          reported = true;
        }
        if (!owner.cpuNs || reported) {
          finished = true;
          return kj::none;
        }
        terminated = false;
        return now + 1 * kj::MILLISECONDS;
      }
      // The CPU of a major GC that ran inside this entry is not charged, as far as its Worker's budget goes.
      auto cpuNow = clock.now();
      auto consumed = cpuNow - started - gcEntry.excluded(started, cpuNow) + used;
      if ((owner.cpuNs && consumed >= owner.cpuNs) ||
          owner.exceeded->load() != IsolateLimitCause::NONE) {
        if (owner.cpuNs && consumed >= owner.cpuNs) {
          auto expected = IsolateLimitCause::NONE;
          owner.exceeded->compare_exchange_strong(expected, IsolateLimitCause::CPU);
        }
        owner.isolate->TerminateExecution();
        terminated = true;
        // 100 ms exceeds observed interrupt jitter, and leaves ample time before 3 s kill.
        graceEnd = now + 100 * kj::MILLISECONDS;
        return graceEnd;
      }
      if (!owner.cpuNs) return kj::none;
      return now + static_cast<int64_t>(kj::max(owner.cpuNs - consumed, MIN_RECHECK_NS)) * kj::NANOSECONDS;
    }
    ~Scope() noexcept(false) {
      owner.watchdog->remove(*this, [this]() {
        auto cpuNow = clock.now();
        auto excluded = gcEntry.excluded(started, cpuNow);
        used += cpuNow - started - excluded;
        if (gcExcluded) *gcExcluded += excluded;
      });
      if (owner.gc) owner.gc->measureOld();
      if (owner.cpuNs && used >= owner.cpuNs) {
        auto expected = IsolateLimitCause::NONE;
        if (owner.exceeded->compare_exchange_strong(expected, IsolateLimitCause::CPU)) {
          owner.watchdog->wake();
        }
      }
      if (owner.memory) owner.memory->leave(previousMemory);
      if (owner.exceeded->load() != IsolateLimitCause::NONE) {
        owner.record(owner.exceeded->load() == IsolateLimitCause::MEMORY ? "memory-terminated" : "terminated", identity, used);
      }
      onExit();
    }
  };
  struct AuxiliaryScope {
    uint64_t used = 0;
    Scope scope;
    AuxiliaryScope(CpuIsolateLimitEnforcer& owner, kj::StringPtr identity,
        kj::OneOf<kj::Exception, kj::Duration>& result)
        : scope(owner, used, identity, [&owner, &result, this]() {
      if (owner.exceeded->load() != IsolateLimitCause::NONE) result = owner.exception();
      else result = static_cast<int64_t>(used) * kj::NANOSECONDS;
    }) {}
  };
  kj::Own<void> enterAuxiliaryJs(jsg::Lock& lock,
      kj::OneOf<kj::Exception, kj::Duration>& result) const {
    auto& self = const_cast<CpuIsolateLimitEnforcer&>(*this);
    return kj::heap<AuxiliaryScope>(self, memoryExecutionIdentity(lock), result);
  }
  kj::Own<void> enterRequestJs(uint64_t& used, kj::StringPtr identity, kj::Function<void()> onExit,
      uint64_t* gcExcluded = nullptr) {
    requireNotExceeded();
    if (!isEnabled()) return {};
    return kj::heap<Scope>(*this, used, identity, kj::mv(onExit), gcExcluded);
  }
  struct StartupScope {
    jsg::AsyncContextFrame::StorageScope frame;
    kj::Own<void> limit;
    StartupScope(jsg::Lock& lock, CpuIsolateLimitEnforcer& owner,
        kj::OneOf<kj::Exception, kj::Duration>& result)
        : frame(lock, memoryExecutionKey(),
              lock.v8Ref<v8::Value>(jsg::v8Str(lock.v8Isolate, owner.startupIdentity))),
          limit(kj::heap<Scope>(owner, owner.startupUsed, owner.startupIdentity, [&owner, &result]() {
            if (owner.exceeded->load() != IsolateLimitCause::NONE) {
              owner.publish(); result = owner.exception();
            } else result = static_cast<int64_t>(owner.startupUsed) * kj::NANOSECONDS;
          })) {}
  };
  kj::Own<void> enterStartupJs(jsg::Lock& lock, kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    if (!isEnabled()) return {};
    auto& self = const_cast<CpuIsolateLimitEnforcer&>(*this);
    if (memory) return kj::heap<StartupScope>(lock, self, result);
    return kj::heap<Scope>(self, self.startupUsed, "", [&self, &result]() {
      if (self.exceeded->load() != IsolateLimitCause::NONE) { self.publish(); result = self.exception(); }
      else result = static_cast<int64_t>(self.startupUsed) * kj::NANOSECONDS;
    });
  }
 private:
  kj::Own<IsolateLimitEnforcer> fallback;
  kj::String name;
  CoworkerdNamedLimits named;
  uint64_t cpuNs;
  bool dynamic;
  IdleWorkerRef idleWorker;
  kj::String startupIdentity;
  v8::Isolate* isolate = nullptr;
  uint64_t startupUsed = 0;
  LimitWatchdog* watchdog = nullptr;
  IsolateLimitState exceeded = std::make_shared<std::atomic<IsolateLimitCause>>(IsolateLimitCause::NONE);
  std::shared_ptr<IsolateMemoryLimit> memory;
  std::shared_ptr<IsolateGc> gc;
  bool published = false;
  kj::ForkedPromise<void> failure = nullptr;
  kj::Own<kj::PromiseFulfiller<void>> fulfiller;
};

// `actor`: the request is an actor's (its IoContext, and this enforcer, serve the actor's later events too).
kj::Own<LimitEnforcer> wrapCpuRequestLimits(kj::Own<LimitEnforcer> fallback,
    CpuIsolateLimitEnforcer& limits, kj::Maybe<kj::String>& metadata,
    kj::Function<void()> abortIsolate, bool actor);
kj::Own<WorkerInterface> wrapCpuWorkerInterface(kj::Own<WorkerInterface> entry,
    CpuIsolateLimitEnforcer& limits);
}  // namespace workerd::server
