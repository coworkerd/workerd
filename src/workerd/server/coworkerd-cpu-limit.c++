#include "coworkerd-cpu-limit.h"
#include <workerd/util/autogate.h>
#include <cstdlib>
#include <fcntl.h>
#include <limits>
#include <v8-isolate.h>

namespace workerd::server {
namespace {
thread_local kj::StringPtr startupExecutionIdentity;
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
kj::String cpuStateFromEnvironment() {
  const char* value = std::getenv("COWORKERD_CPU_STATE");
  if (value == nullptr) return kj::str("");
  KJ_REQUIRE(*value != '\0', "COWORKERD_CPU_STATE must be a non-empty path");
  return kj::str(value);
}
int cpuEventsDescriptor() {
  // Opened once per process, when the first enabled limit enforcer is created.
  static const int descriptor = []() {
    auto state = cpuStateFromEnvironment();
    if (state.size() == 0) return -1;
    auto path = kj::str(state, ".events");
    int fd;
    do { fd = open(path.cStr(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600); } while (fd < 0 && errno == EINTR);
    return fd;
  }();
  return descriptor;
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
    ~RequestLimits() noexcept(false) {
      if (cpuUsed) limits->record("completed", identity, cpuUsed);
    }
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
  if (!limits.isEnabled() && !memoryLimitFromEnvironment()) return fallback;
  return kj::heap<RequestLimits>(kj::mv(fallback), limits, limits.identity(metadata), kj::mv(abortIsolate));
}
kj::Own<WorkerInterface> wrapCpuWorkerInterface(kj::Own<WorkerInterface> entry,
    CpuIsolateLimitEnforcer& limits) {
  if (!limits.isEnabled()) return entry;
  return kj::heap<CpuWorkerInterface>(kj::mv(entry), kj::addRef(limits));
}
}  // namespace workerd::server
