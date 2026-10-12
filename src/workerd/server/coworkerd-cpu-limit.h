#pragma once

#include <workerd/io/actor-cache.h>
#include <workerd/io/limit-enforcer.h>
#include <workerd/io/worker-interface.h>
#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/function.h>
#include <kj/mutex.h>
#include <kj/thread.h>
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
kj::String cpuStateFromEnvironment();
kj::String escapeCpuJsonString(kj::StringPtr text);
void recordPlatformFacts(kj::StringPtr name, bool dynamic);

// CPU accounting stays on the isolate thread; only termination and file writes cross threads.
class CpuIsolateLimitEnforcer final: public IsolateLimitEnforcer {
 public:
  CpuIsolateLimitEnforcer(kj::Own<IsolateLimitEnforcer> fallback, kj::StringPtr name, bool dynamic)
      : fallback(kj::mv(fallback)), name(kj::str(name)),
        cpuNs(uint64_t(cpuLimitFromEnvironment()) * (dynamic ? 1000000 : 0)),
        state(cpuStateFromEnvironment()) {
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
    if (state.size() == 0) return;
    auto path = kj::str(state, ".events");
    if (auto* file = fopen(path.cStr(), "a")) {
      fwrite(row.cStr(), 1, row.size(), file);
      fclose(file);
    }
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

  struct Scope {
    CpuIsolateLimitEnforcer& owner;
    uint64_t& used;
    kj::String identity;
    kj::Function<void()> onExit;
    CpuClock clock;
    uint64_t started = clock.now();
    kj::MutexGuarded<bool> stopped{false};
    kj::Maybe<kj::Thread> watchdog;
    bool reported = false;

    Scope(CpuIsolateLimitEnforcer& owner, uint64_t& used, kj::StringPtr identity,
        kj::Function<void()> onExit)
        : owner(owner), used(used), identity(kj::str(identity)), onExit(kj::mv(onExit)) {
      watchdog.emplace([this]() {
        auto lock = stopped.lockExclusive();
        while (!*lock) {
          auto consumed = clock.now() - started + this->used;
          if (consumed >= this->owner.cpuNs) {
            this->owner.exceeded.store(true);
            this->owner.isolate->TerminateExecution();
            // 100 ms exceeds observed interrupt jitter, and leaves ample time before 3 s kill.
            lock.wait([](bool stopped) { return stopped; }, 100 * kj::MILLISECONDS);
            if (!*lock) {
              this->owner.culprit(this->identity, clock.now() - started + this->used);
              reported = true;
            }
            break;
          }
          lock.wait([](bool stopped) { return stopped; }, 1 * kj::MILLISECONDS);
        }
      });
    }
    ~Scope() noexcept(false) {
      { auto lock = stopped.lockExclusive(); *lock = true; }
      watchdog = kj::none;
      used += clock.now() - started;
      if (used >= owner.cpuNs) owner.exceeded.store(true);
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
