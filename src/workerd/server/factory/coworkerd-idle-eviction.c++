#include "coworkerd-idle-eviction.h"
#include "coworkerd-cpu-limit.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <list>
#include <limits>
#include <string>
#include <unistd.h>

namespace workerd::server {
namespace {
double monotonicMs() {
  return std::chrono::duration<double, std::milli>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}
uint64_t cpuNs() {
  timespec t;
  KJ_REQUIRE(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t) == 0);
  return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}
uint32_t setting(const char* name, uint32_t fallback) {
  auto* text = std::getenv(name);
  if (!text) return fallback;
  KJ_REQUIRE(*text, "idle eviction setting must be an unsigned integer", name);
  uint64_t value = 0;
  for (auto* p = text; *p; ++p) {
    KJ_REQUIRE(*p >= '0' && *p <= '9', "invalid idle eviction setting", name);
    value = value * 10 + (*p - '0');
    KJ_REQUIRE(value <= std::numeric_limits<uint32_t>::max(), "idle eviction setting overflow", name);
  }
  return value;
}
void emit(kj::StringPtr row) {
  auto line = kj::str("[coworkerd-lifecycle] ", row, '\n');
  ssize_t n;
  do { n = write(STDERR_FILENO, line.cStr(), line.size()); } while (n < 0 && errno == EINTR);
}
// The lifecycle snapshot channel: shared memory from the Server (COWORKERD_LIFECYCLE_FD) holding
// the latest snapshot this process published. Layout, shared with the Server:
//   [0, 8)    publications completed; the latest snapshot is in buffer (publications % 2)
//   [8, 12)   magic 0x434c5743 ("CWLC")
//   [12, 16)  capacity of each buffer's data in bytes
//   buffer i at 64 + i * (16 + capacity): sequence (8; odd while being written), length (4),
//             4 unused, data
// One writer, this process; a reader copies the published buffer and keeps the copy only if its
// sequence did not change meanwhile. A writer killed mid-write leaves the published buffer intact.
class LifecycleChannel {
 public:
  static constexpr uint32_t MAGIC = 0x434c5743;
  LifecycleChannel(): base(static_cast<char*>(coworkerdLifecycleMapping())) {
    if (base) capacity = *reinterpret_cast<uint32_t*>(base + 12);
  }
  explicit operator bool() const { return base != nullptr; }
  // False when the snapshot does not fit; the previous one stays published.
  bool publish(kj::StringPtr row) {
    auto lock = mutex.lockExclusive();
    if (row.size() > capacity) return false;
    auto* published = reinterpret_cast<uint64_t*>(base);
    auto next = __atomic_load_n(published, __ATOMIC_RELAXED) + 1;
    auto* buffer = base + 64 + (next % 2) * (16 + uint64_t(capacity));
    auto* sequence = reinterpret_cast<uint64_t*>(buffer);
    auto start = __atomic_load_n(sequence, __ATOMIC_RELAXED);
    __atomic_store_n(sequence, start + 1, __ATOMIC_RELAXED);
    __atomic_thread_fence(__ATOMIC_RELEASE);
    memcpy(buffer + 16, row.begin(), row.size());
    __atomic_store_n(reinterpret_cast<uint32_t*>(buffer + 8), uint32_t(row.size()), __ATOMIC_RELAXED);
    __atomic_store_n(sequence, start + 2, __ATOMIC_RELEASE);
    __atomic_store_n(published, next, __ATOMIC_RELEASE);
    return true;
  }
 private:
  kj::MutexGuarded<bool> mutex;
  char* base = nullptr;
  uint32_t capacity = 0;
};
// The inherited descriptors are mapped once per process and closed.
LifecycleChannel& lifecycleChannel() {
  static LifecycleChannel channel;
  return channel;
}

// Borrow the fd6 mapping established before the Rust server starts.
const uint32_t* inheritedBand() { return coworkerdMemoryBandMapping(); }

}  // namespace

struct IdleWorker {
  std::weak_ptr<IdleReaper> owner;
  kj::String name;
  kj::Maybe<kj::Function<void()>> remove;
  uint64_t id;
  uint32_t active = 0;
  bool ready = false, removed = false, created = false, destroyed = false, eligible = true;
  double lastEnd = monotonicMs(), loadStart = lastEnd, teardownStart = 0;
  uint64_t loadCpu = cpuNs();
  kj::String reason = kj::str("");
  IdleWorker(std::shared_ptr<IdleReaper> owner, kj::StringPtr name,
      kj::Function<void()> remove, uint64_t id)
      : owner(owner), name(kj::str(name)), remove(kj::mv(remove)), id(id) {}
};

class IdleReaper {
 public:
  explicit IdleReaper(kj::Timer& timer)
      : timer(timer), threshold(setting("COWORKERD_IDLE_EVICT_MS", 0)),
        interval(setting("COWORKERD_IDLE_DESTROY_INTERVAL_MS", 10)) {
    KJ_REQUIRE(interval > 0, "COWORKERD_IDLE_DESTROY_INTERVAL_MS must be positive");
    publishedBand = inheritedBand();
    publish();
    if (threshold || publishedBand) task = loop().eagerlyEvaluate(nullptr);
  }
  kj::Timer& timer;
  uint32_t threshold, interval;
  uint64_t nextId = 1, created = 0, destroyed = 0, evicted = 0, failed = 0, pending = 0;
  uint64_t watermarkEvicted = 0, bandReads = 0, bandReadNs = 0;
  const uint32_t* publishedBand = nullptr;
  uint32_t band = 0;
  double lastPublish = 0;
  uint32_t effectiveThreshold() const { return band == 2 ? 1000 : band == 1 ? 5000 : threshold; }
  double lastDestroy = 0;
  std::list<std::weak_ptr<IdleWorker>> workers;
  kj::Promise<void> task = nullptr;

  void event(const IdleWorker& worker, kj::StringPtr event, double elapsed = 0, uint64_t cpu = 0) {
    emit(kj::str("{\"event\":", escapeCpuJsonString(event), ",\"isolate\":",
        escapeCpuJsonString(worker.name), ",\"instance\":", worker.id,
        ",\"pid\":", coworkerdPid(), ",\"monotonicMs\":", monotonicMs(),
        ",\"active\":", worker.active, ",\"lastEndMs\":", worker.lastEnd,
        ",\"reason\":", escapeCpuJsonString(worker.reason), ",\"elapsedMs\":", elapsed,
        ",\"cpuUsedNs\":", cpu, ",\"idleEvictMs\":", threshold,
        ",\"memoryBand\":" , band, ",\"effectiveIdleEvictMs\":" , effectiveThreshold(),
        ",\"idleDestroyIntervalMs\":", interval, "}"));
  }
  void publish() {
    auto& channel = lifecycleChannel();
    if (!channel) return;
    kj::Vector<kj::String> entries;
    for (auto it = workers.begin(); it != workers.end();) {
      if (auto worker = it->lock()) {
        if (worker->created && !worker->destroyed) entries.add(kj::str(
            "{\"isolate\":", escapeCpuJsonString(worker->name), ",\"instance\":", worker->id, "}"));
        ++it;
      } else {
        it = workers.erase(it);
      }
    }
    auto row = kj::str("{\"pid\":", coworkerdPid(), ",\"residentIsolates\":", created - destroyed,
        ",\"createdIsolates\":", created, ",\"destroyedIsolates\":", destroyed,
        ",\"idleEvictions\":", evicted, ",\"watermarkEvictions\":", watermarkEvicted,
        ",\"memoryBand\":", band, ",\"bandReadCount\":", bandReads, ",\"bandReadNs\":", bandReadNs, ",\"loadFailureRemovals\":", failed,
        ",\"idleEvictMs\":", threshold, ",\"idleDestroyIntervalMs\":", interval,
        ",\"residentWorkers\":[", kj::strArray(entries, ","), "]}");
    if (!channel.publish(row) && !overflowReported) {
      overflowReported = true;
      emit(kj::str("{\"event\":\"snapshot-too-large\",\"pid\":", coworkerdPid(), ",\"bytes\":", row.size(), "}"));
    }
  }
  bool overflowReported = false;
 private:
  kj::Promise<void> loop() {
    while (true) {
      co_await timer.afterDelay(std::min(interval, 100u) * kj::MILLISECONDS);
      if (publishedBand) {
        auto start = std::chrono::steady_clock::now();
        band = std::min(__atomic_load_n(publishedBand, __ATOMIC_ACQUIRE), 2u);
        bandReadNs += std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start).count();
        ++bandReads;
      }
      // At most once a second for the band.
      if (monotonicMs() - lastPublish >= 1000 &&
          publishedBand) {
        publish();
        lastPublish = monotonicMs();
      }
      auto minimum = effectiveThreshold();
      if (!minimum || pending || monotonicMs() - lastDestroy < interval) continue;
      IdleWorkerRef oldest;
      auto now = monotonicMs();
      for (auto it = workers.begin(); it != workers.end();) {
        auto worker = it->lock();
        if (!worker) { it = workers.erase(it); continue; }
        ++it;
        if (!worker->eligible || !worker->ready || !worker->created || worker->removed || worker->active ||
            now - worker->lastEnd <= minimum) continue;
        if (!oldest || worker->lastEnd < oldest->lastEnd) oldest = kj::mv(worker);
      }
      if (oldest) {
        // Activity and removal are decided synchronously on the same event loop.
        pending = oldest->id;
        KJ_IF_SOME(cb, oldest->remove) {
          auto remove = kj::mv(cb);
          oldest->remove = kj::none;
          idleWorkerRemoved(oldest, threshold && now - oldest->lastEnd > threshold ? "idle" : "watermark");
          remove();
        }
      }
    }
  }
};

std::shared_ptr<IdleReaper> idleReaper(kj::Timer& timer) {
  return std::make_shared<IdleReaper>(timer);
}
IdleWorkerRef registerIdleWorker(std::shared_ptr<IdleReaper> reaper,
    kj::StringPtr name, kj::Function<void()> remove, bool eligible) {
  auto worker = std::make_shared<IdleWorker>(reaper, name, kj::mv(remove), reaper->nextId++);
  reaper->workers.push_back(worker);
  worker->eligible = eligible;
  return worker;
}
void idleWorkerReady(const IdleWorkerRef& worker) {
  if (worker) { worker->ready = true; worker->lastEnd = monotonicMs(); }
}
void idleWorkerRemoved(const IdleWorkerRef& worker, kj::StringPtr reason) {
  if (!worker || worker->removed) return;
  worker->removed = true; worker->reason = kj::str(reason);
  if (auto owner = worker->owner.lock()) {
    if (reason == "idle") ++owner->evicted;
    if (reason == "watermark") ++owner->watermarkEvicted;
    if (reason == "load-failed") ++owner->failed;
    owner->event(*worker, "removed"); owner->publish();
  }
}
void idleWorkerFailed(const IdleWorkerRef& worker) {
  if (!worker || worker->ready || worker->removed) return;
  if (auto owner = worker->owner.lock()) owner->event(*worker, "load-failed",
      monotonicMs() - worker->loadStart, cpuNs() - worker->loadCpu);
  KJ_IF_SOME(cb, worker->remove) {
    auto remove = kj::mv(cb); worker->remove = kj::none;
    idleWorkerRemoved(worker, "load-failed"); remove();
  }
}
void detachIdleWorker(const IdleWorkerRef& worker) { if (worker) worker->remove = kj::none; }

namespace {
struct Activity {
  IdleWorkerRef worker;
  explicit Activity(IdleWorkerRef worker): worker(kj::mv(worker)) { ++this->worker->active; }
  ~Activity() { KJ_ASSERT(worker->active); --worker->active; worker->lastEnd = monotonicMs(); }
};
class IdleObserver final: public IsolateObserver {
 public:
  IdleObserver(kj::StringPtr name, bool dynamic, IdleWorkerRef worker,
      kj::Own<IsolateObserver> inner)
      : name(kj::str(name)), dynamic(dynamic), worker(kj::mv(worker)), inner(kj::mv(inner)) {}
  kj::Maybe<AsyncTraceConfig> getAsyncTraceConfig() const override {
    return inner->getAsyncTraceConfig();
  }
  void addAsyncTraceSinks(AsyncTraceSinks& sinks, kj::StringPtr name,
      kj::Maybe<kj::StringPtr> actorId) const override {
    inner->addAsyncTraceSinks(sinks, name, actorId);
  }
  void created() override {
    inner->created();
    if (!worker) return;
    worker->created = true;
    if (auto owner = worker->owner.lock()) { ++owner->created; owner->event(*worker, "created"); owner->publish(); }
  }
  void teardownStarted() override {
    inner->teardownStarted();
    if (worker) worker->teardownStart = monotonicMs();
  }
  void teardownLockAcquired() override { inner->teardownLockAcquired(); }
  void teardownFinished() override {
    inner->teardownFinished();
    auto elapsed = worker ? monotonicMs() - worker->teardownStart : 0;
    if (worker) {
      worker->destroyed = true;
      if (auto owner = worker->owner.lock()) {
        ++owner->destroyed;
        if (owner->pending == worker->id) { owner->pending = 0; owner->lastDestroy = monotonicMs(); }
        owner->event(*worker, "destroyed", elapsed); owner->publish();
      }
    }
    auto line = kj::str("[coworkerd-platform-destroyed] {\"isolate\":", escapeCpuJsonString(name),
        ",\"dynamic\":", dynamic ? "true" : "false", ",\"elapsedMs\":", elapsed, "}\n");
    ssize_t n;
    do { n = write(STDERR_FILENO, line.cStr(), line.size()); } while (n < 0 && errno == EINTR);
  }
 private:
  kj::String name;
  bool dynamic;
  IdleWorkerRef worker;
  kj::Own<IsolateObserver> inner;
};
class IdleInterface final: public WorkerInterface {
 public:
  IdleInterface(kj::Own<WorkerInterface> inner, IdleWorkerRef worker)
      : inner(kj::mv(inner)), worker(kj::mv(worker)) {}
  kj::Promise<void> request(kj::HttpMethod method, kj::StringPtr url,
      const kj::HttpHeaders& headers, kj::AsyncInputStream& body, Response& response) override {
    auto activity = idleActivity(worker);
    return inner->request(method, url, headers, body, response).attach(kj::mv(activity));
  }
  kj::Promise<void> connect(kj::StringPtr host, const kj::HttpHeaders& headers,
      kj::AsyncIoStream& connection, ConnectResponse& response, kj::HttpConnectSettings settings) override {
    auto activity = idleActivity(worker);
    return inner->connect(host, headers, connection, response, settings).attach(kj::mv(activity));
  }
  kj::Promise<void> prewarm(kj::StringPtr url) override { return inner->prewarm(url).attach(idleActivity(worker)); }
  kj::Promise<ScheduledResult> runScheduled(kj::Date time, kj::StringPtr cron) override {
    return inner->runScheduled(time, cron).attach(idleActivity(worker));
  }
  kj::Promise<AlarmResult> runAlarm(kj::Date time, uint32_t count) override {
    return inner->runAlarm(time, count).attach(idleActivity(worker));
  }
  kj::Promise<kj::Maybe<kj::Date>> abandonAlarm(kj::Date time) override {
    return inner->abandonAlarm(time).attach(idleActivity(worker));
  }
  kj::Promise<bool> test() override { return inner->test().attach(idleActivity(worker)); }
  kj::Promise<CustomEvent::Result> customEvent(kj::Own<CustomEvent> event) override {
    return inner->customEvent(kj::mv(event)).attach(idleActivity(worker));
  }
 private:
  kj::Own<WorkerInterface> inner;
  IdleWorkerRef worker;
};
}  // namespace
kj::Own<void> idleActivity(const IdleWorkerRef& worker) {
  if (!worker) return {};
  return kj::heap<Activity>(worker);
}
kj::Own<IsolateObserver> idleIsolateObserver(kj::StringPtr name, bool dynamic, IdleWorkerRef worker,
    kj::Own<IsolateObserver> inner) {
  return kj::atomicRefcounted<IdleObserver>(name, dynamic, kj::mv(worker), kj::mv(inner));
}
kj::Own<WorkerInterface> wrapIdleWorkerInterface(kj::Own<WorkerInterface> inner, const IdleWorkerRef& worker) {
  if (!worker) return inner;
  return kj::heap<IdleInterface>(kj::mv(inner), worker);
}
}  // namespace workerd::server
