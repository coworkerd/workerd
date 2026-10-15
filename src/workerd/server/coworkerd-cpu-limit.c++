#include "coworkerd-cpu-limit.h"
#include <workerd/util/autogate.h>
#include <csignal>
#include <cstdlib>
#include <fcntl.h>
#include <limits>
#include <v8-isolate.h>

namespace workerd::server {
namespace {
thread_local kj::StringPtr startupExecutionIdentity;
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
    : previous(startupExecutionIdentity) { startupExecutionIdentity = identity; }
MemoryStartupIdentityScope::~MemoryStartupIdentityScope() { startupExecutionIdentity = previous; }
kj::String captureMemoryStartupIdentity() { return kj::str(startupExecutionIdentity); }

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
        kj::String identity, kj::Function<void()> abortIsolate)
        : fallback(kj::mv(fallback)), limits(kj::addRef(limits)),
          abortIsolate(kj::mv(abortIsolate)), identity(kj::mv(identity)) {}
    // The request context ends here, after its waitUntil work, whether or not it ran JavaScript.
    ~RequestLimits() noexcept(false) { limits->completed(identity, cpuUsed); }
    struct IdentityScope {
      jsg::AsyncContextFrame::StorageScope frame;
      MemoryStartupIdentityScope startup;
      kj::Own<void> limit;
      IdentityScope(jsg::Lock& lock, RequestLimits& owner)
          : frame(lock, memoryExecutionKey(),
                lock.v8Ref<v8::Value>(jsg::v8Str(lock.v8Isolate, owner.identity))),
            startup(owner.identity),
            limit(owner.limits->enterRequestJs(owner.cpuUsed, owner.identity,
                [&owner]() { if (owner.limits->publish()) owner.abortIsolate(); })) {}
    };
    kj::Own<void> enterJs(jsg::Lock& lock, IoContext&) override {
      if (limits->publish()) abortIsolate();
      limits->requireNotExceeded();
      if (memoryLimitFromEnvironment()) return kj::heap<IdentityScope>(lock, *this);
      return limits->enterRequestJs(cpuUsed, identity, [this]() { if (limits->publish()) abortIsolate(); });
    }
    void topUpActor() override {}
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
    uint64_t cpuUsed = 0;
  };


}  // namespace

kj::Own<LimitEnforcer> wrapCpuRequestLimits(kj::Own<LimitEnforcer> fallback,
    CpuIsolateLimitEnforcer& limits, kj::Maybe<kj::String>& metadata,
    kj::Function<void()> abortIsolate) {
  if (!limits.isEnabled() && !memoryLimitFromEnvironment() && !limits.reportsCompletion()) return fallback;
  return kj::heap<RequestLimits>(kj::mv(fallback), limits, limits.identity(metadata), kj::mv(abortIsolate));
}
kj::Own<WorkerInterface> wrapCpuWorkerInterface(kj::Own<WorkerInterface> entry,
    CpuIsolateLimitEnforcer& limits) {
  if (!limits.isEnabled()) return entry;
  return kj::heap<CpuWorkerInterface>(kj::mv(entry), kj::addRef(limits));
}
}  // namespace workerd::server
