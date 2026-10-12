#include "coworkerd-idle-eviction.h"
#include "coworkerd-cpu-limit.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <list>
#include <limits>
#include <string>

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
  const char* state = std::getenv("COWORKERD_CPU_STATE");
  if (state && *state) {
    auto path = kj::str(state, ".lifecycle.events");
    if (auto* f = fopen(path.cStr(), "a")) {
      fwrite(row.cStr(), 1, row.size(), f); fputc('\n', f); fclose(f);
    }
  }
}
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
    publish();
    if (threshold) task = loop().eagerlyEvaluate(nullptr);
  }
  kj::Timer& timer;
  uint32_t threshold, interval;
  uint64_t nextId = 1, created = 0, destroyed = 0, evicted = 0, failed = 0, pending = 0;
  double lastDestroy = 0;
  std::list<std::weak_ptr<IdleWorker>> workers;
  kj::Promise<void> task = nullptr;

  void event(const IdleWorker& worker, kj::StringPtr event, double elapsed = 0, uint64_t cpu = 0) {
    emit(kj::str("{\"event\":", escapeCpuJsonString(event), ",\"isolate\":",
        escapeCpuJsonString(worker.name), ",\"instance\":", worker.id,
        ",\"pid\":", getpid(), ",\"monotonicMs\":", monotonicMs(),
        ",\"active\":", worker.active, ",\"lastEndMs\":", worker.lastEnd,
        ",\"reason\":", escapeCpuJsonString(worker.reason), ",\"elapsedMs\":", elapsed,
        ",\"cpuUsedNs\":", cpu, ",\"idleEvictMs\":", threshold,
        ",\"idleDestroyIntervalMs\":", interval, "}"));
  }
  void publish() {
    const char* state = std::getenv("COWORKERD_CPU_STATE");
    if (!state || !*state) return;
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
    auto row = kj::str("{\"pid\":", getpid(), ",\"residentIsolates\":", created - destroyed,
        ",\"createdIsolates\":", created, ",\"destroyedIsolates\":", destroyed,
        ",\"idleEvictions\":", evicted, ",\"loadFailureRemovals\":", failed,
        ",\"idleEvictMs\":", threshold, ",\"idleDestroyIntervalMs\":", interval,
        ",\"residentWorkers\":[", kj::strArray(entries, ","), "]}");
    auto path = kj::str(state, ".lifecycle"), temporary = kj::str(path, ".tmp");
    if (auto* f = fopen(temporary.cStr(), "w")) {
      auto written = fwrite(row.cStr(), 1, row.size(), f);
      if (fclose(f) == 0 && written == row.size()) rename(temporary.cStr(), path.cStr());
      else unlink(temporary.cStr());
    }
  }
 private:
  kj::Promise<void> loop() {
    while (true) {
      co_await timer.afterDelay(std::min(interval, 100u) * kj::MILLISECONDS);
      if (pending || monotonicMs() - lastDestroy < interval) continue;
      for (auto it = workers.begin(); it != workers.end();) {
        auto worker = it->lock();
        if (!worker) { it = workers.erase(it); continue; }
        ++it;
        if (!worker->eligible || !worker->ready || !worker->created || worker->removed || worker->active ||
            monotonicMs() - worker->lastEnd <= threshold) continue;
        // The event-loop decision and map removal are synchronous. A new request either
        // acquired activity first, or sees a fresh named stub after this removal.
        pending = worker->id;
        KJ_IF_SOME(cb, worker->remove) {
          auto remove = kj::mv(cb);
          worker->remove = kj::none;
          idleWorkerRemoved(worker, "idle");
          remove();
        }
        break;
      }
    }
  }
};

std::shared_ptr<IdleReaper> idleReaper(kj::Timer& timer) {
  static std::weak_ptr<IdleReaper> current;
  auto reaper = current.lock();
  if (!reaper) { reaper = std::make_shared<IdleReaper>(timer); current = reaper; }
  return reaper;
}
IdleWorkerRef registerIdleWorker(std::shared_ptr<IdleReaper> reaper,
    kj::StringPtr name, kj::Function<void()> remove, bool eligible) {
  auto worker = std::make_shared<IdleWorker>(reaper, name, kj::mv(remove), reaper->nextId++);
  reaper->workers.push_back(worker);
  worker->eligible = eligible;
  return worker;
}
void idleWorkerReady(const IdleWorkerRef& worker) { worker->ready = true; worker->lastEnd = monotonicMs(); }
void idleWorkerRemoved(const IdleWorkerRef& worker, kj::StringPtr reason) {
  if (!worker || worker->removed) return;
  worker->removed = true; worker->reason = kj::str(reason);
  if (auto owner = worker->owner.lock()) {
    if (reason == "idle") ++owner->evicted;
    if (reason == "load-failed") ++owner->failed;
    owner->event(*worker, "removed"); owner->publish();
  }
}
void idleWorkerFailed(const IdleWorkerRef& worker) {
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
  IdleObserver(kj::StringPtr name, bool dynamic, IdleWorkerRef worker)
      : name(kj::str(name)), dynamic(dynamic), worker(kj::mv(worker)) {}
  void created() override {
    if (!worker) return;
    worker->created = true;
    if (auto owner = worker->owner.lock()) { ++owner->created; owner->event(*worker, "created"); owner->publish(); }
  }
  void teardownStarted() override { if (worker) worker->teardownStart = monotonicMs(); }
  void teardownFinished() override {
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
kj::Own<IsolateObserver> idleIsolateObserver(kj::StringPtr name, bool dynamic, IdleWorkerRef worker) {
  return kj::atomicRefcounted<IdleObserver>(name, dynamic, kj::mv(worker));
}
kj::Own<WorkerInterface> wrapIdleWorkerInterface(kj::Own<WorkerInterface> inner, const IdleWorkerRef& worker) {
  if (!worker) return inner;
  return kj::heap<IdleInterface>(kj::mv(inner), worker);
}
}  // namespace workerd::server
