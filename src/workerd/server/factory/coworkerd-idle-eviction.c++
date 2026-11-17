#include "coworkerd-idle-eviction.h"
#include "coworkerd-cpu-limit.h"
#include "coworkerd-gc.h"
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
//   [16, 24)  reader's drain request counter
//   [24, 32)  drain reply sequence (request * 2; odd while writing)
//   [32, 64)  observed monotonic ns, active count, background count, unknown busy count
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
  uint64_t drainRequest() const {
    return base ? __atomic_load_n(reinterpret_cast<const uint64_t*>(base + 16), __ATOMIC_ACQUIRE) : 0;
  }
  void publishDrain(uint64_t request, uint64_t observed, uint64_t active, uint64_t background, uint64_t unknown) {
    auto* sequence = reinterpret_cast<uint64_t*>(base + 24);
    __atomic_store_n(sequence, request * 2 - 1, __ATOMIC_RELAXED);
    __atomic_thread_fence(__ATOMIC_RELEASE);
    __atomic_store_n(reinterpret_cast<uint64_t*>(base + 32), observed, __ATOMIC_RELAXED);
    __atomic_store_n(reinterpret_cast<uint64_t*>(base + 40), active, __ATOMIC_RELAXED);
    __atomic_store_n(reinterpret_cast<uint64_t*>(base + 48), background, __ATOMIC_RELAXED);
    __atomic_store_n(reinterpret_cast<uint64_t*>(base + 56), unknown, __ATOMIC_RELAXED);
    __atomic_store_n(sequence, request * 2, __ATOMIC_RELEASE);
  }
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
  bool retireWhenIdle = false;
  kj::Maybe<kj::Function<bool()>> backgroundDone;
  IdleWorkerLabels labels;
  double lastEnd = monotonicMs(), loadStart = lastEnd, teardownStart = 0;
  uint64_t loadCpu = cpuNs();
  kj::String reason = kj::str("");
  // Its collections and its collector (fix B); the actor events it received and their last time.
  std::shared_ptr<IsolateGc> gc;
  kj::Maybe<IdleCollector> collect;
  uint64_t actorEvents = 0, idleCollections = 0;
  double lastActorEvent = 0;
  IdleWorker(std::shared_ptr<IdleReaper> owner, kj::StringPtr name,
      kj::Function<void()> remove, uint64_t id)
      : owner(owner), name(kj::str(name)), remove(kj::mv(remove)), id(id) {}
};

kj::String labelFields(const IdleWorkerLabels& labels) {
  return kj::str(",\"tenant\":", escapeCpuJsonString(labels.tenant),
      ",\"workerId\":", escapeCpuJsonString(labels.workerId),
      ",\"versionId\":", escapeCpuJsonString(labels.versionId),
      ",\"planRevision\":", escapeCpuJsonString(labels.planRevision),
      ",\"loadGeneration\":", labels.loadGeneration);
}

class IdleReaper {
 public:
  explicit IdleReaper(kj::Timer& timer)
      : timer(timer), threshold(setting("COWORKERD_IDLE_EVICT_MS", 0)),
        interval(setting("COWORKERD_IDLE_DESTROY_INTERVAL_MS", 10)) {
    KJ_REQUIRE(interval > 0, "COWORKERD_IDLE_DESTROY_INTERVAL_MS must be positive");
    publishedBand = inheritedBand();
    publish();
    if (threshold || publishedBand || gcSettings().observes() || lifecycleChannel()) startLoop();
    if (gcSettings().idleCollectMs) collectTask = collectLoop().eagerlyEvaluate(nullptr);
  }
  kj::Timer& timer;
  bool loopStarted = false, retirementLoopStarted = false, observesBackground = false, durableLifecycle = false;
  void startLoop() {
    if (!loopStarted) {loopStarted = true; task = loop().eagerlyEvaluate(nullptr);}
  }
  void startRetirementLoop() {
    if (!retirementLoopStarted) {
      retirementLoopStarted = true;
      retirementTask = retirementLoop().eagerlyEvaluate(nullptr);
    }
  }
  uint32_t threshold, interval;
  uint64_t nextId = 1, created = 0, destroyed = 0, evicted = 0, failed = 0, pending = 0, retirementPending = 0;
  uint64_t watermarkEvicted = 0, bandReads = 0, bandReadNs = 0;
  const uint32_t* publishedBand = nullptr;
  uint32_t band = 0;
  double lastPublish = 0;
  uint32_t effectiveThreshold() const { return band == 2 ? 1000 : band == 1 ? 5000 : threshold; }
  double lastDestroy = 0;
  std::list<std::weak_ptr<IdleWorker>> workers;
  std::list<std::weak_ptr<IdleWorker>> retiringWorkers;
  kj::Promise<void> task = nullptr, collectTask = nullptr, retirementTask = nullptr;
  uint64_t publishedGc = 0;
  uint64_t publicationCpuNs = 0, publications = 0, drainCpuNs = 0, drainPublications = 0, lastDrainRequest = 0;

  void publishDrain() {
    auto& channel = lifecycleChannel();
    const auto request = channel.drainRequest();
    if (!request || request == lastDrainRequest) return;
    const auto start = cpuNs();
    uint64_t active = 0, background = 0, unknown = 0;
    for (const auto& entry: workers) if (auto worker = entry.lock()) {
      if (!worker->created || worker->destroyed) continue;
      bool busy = false;
      KJ_IF_SOME(done, worker->backgroundDone) { busy = !done(); }
      active += worker->active; background += busy;
      const auto& labels = worker->labels;
      if ((!labels.tenant.size() || !labels.workerId.size() || !labels.versionId.size() || !labels.planRevision.size()) &&
          (worker->active || busy)) ++unknown;
    }
    const auto observed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    channel.publishDrain(request, observed, active, background, unknown);
    lastDrainRequest = request; ++drainPublications; drainCpuNs += cpuNs() - start;
  }

  void event(const IdleWorker& worker, kj::StringPtr event, double elapsed = 0, uint64_t cpu = 0) {
    emit(kj::str("{\"event\":", escapeCpuJsonString(event), ",\"isolate\":",
        escapeCpuJsonString(worker.name), ",\"instance\":", worker.id,
        labelFields(worker.labels),
        ",\"pid\":", coworkerdPid(), ",\"monotonicMs\":", monotonicMs(),
        ",\"active\":", worker.active, ",\"lastEndMs\":", worker.lastEnd,
        ",\"reason\":", escapeCpuJsonString(worker.reason), ",\"elapsedMs\":", elapsed,
        ",\"cpuUsedNs\":", cpu, ",\"idleEvictMs\":", threshold,
        ",\"memoryBand\":" , band, ",\"effectiveIdleEvictMs\":" , effectiveThreshold(),
        ",\"idleDestroyIntervalMs\":", interval, ",\"actorEvents\":", worker.actorEvents,
        ",\"idleCollections\":", worker.idleCollections, "}"));
  }
  void publish() {
    auto& channel = lifecycleChannel();
    if (!channel) return;
    const auto now = monotonicMs();
    if (durableLifecycle && publications && now - lastPublish < 1000) return;
    const auto start = cpuNs();
    kj::Vector<kj::String> entries;
    bool anyActive = false, anyBackground = false, unknownBusy = false;
    for (auto it = workers.begin(); it != workers.end();) {
      if (auto worker = it->lock()) {
        bool backgroundActive = false;
        KJ_IF_SOME(done, worker->backgroundDone) { backgroundActive = !done(); }
        if (worker->created && !worker->destroyed) {
          anyActive |= worker->active > 0;
          anyBackground |= backgroundActive;
          const auto& labels = worker->labels;
          const bool tagged = labels.tenant.size() && labels.workerId.size() && labels.versionId.size() &&
              labels.planRevision.size() && worker->id;
          unknownBusy |= !tagged && (worker->active || backgroundActive);
        }
        if (worker->created && !worker->destroyed) entries.add(kj::str(
            "{\"isolate\":", escapeCpuJsonString(worker->name), ",\"instance\":", worker->id,
            labelFields(worker->labels), ",\"active\":", worker->active,
            ",\"backgroundActive\":", backgroundActive ? "true" : "false", "}"));
        ++it;
      } else {
        it = workers.erase(it);
      }
    }
    const auto observed = monotonicMs();
    // The small prefix and completion trailer share the full snapshot's atomic publication.
    // A durable generation needs this aggregate, independently of its historical call count.
    auto row = kj::str("{\"drain\":{\"observedAtMonotonicMs\":", observed,
        ",\"residentCount\":", entries.size(), ",\"active\":", anyActive ? "true" : "false",
        ",\"backgroundActive\":", anyBackground ? "true" : "false",
        ",\"unknownBusy\":", unknownBusy ? "true" : "false", "},\"pid\":", coworkerdPid(),
        ",\"observedAtMonotonicMs\":", observed, ",\"residentIsolates\":", created - destroyed,
        ",\"createdIsolates\":", created, ",\"destroyedIsolates\":", destroyed,
        ",\"idleEvictions\":", evicted, ",\"watermarkEvictions\":", watermarkEvicted,
        ",\"memoryBand\":", band, ",\"bandReadCount\":", bandReads, ",\"bandReadNs\":", bandReadNs, ",\"loadFailureRemovals\":", failed,
        ",\"idleEvictMs\":", threshold, ",\"idleDestroyIntervalMs\":", interval,
        ",\"publicationCpuNs\":", publicationCpuNs, ",\"publications\":", publications,
        ",\"drainCpuNs\":", drainCpuNs, ",\"drainPublications\":", drainPublications,
        gcSettings().observes() ? processGcJson() : kj::str(""),
        ",\"residentWorkers\":[", kj::strArray(entries, ","), "],\"drainSnapshotComplete\":true}");
    if (gcSettings().observes()) publishedGc = processGcVersion();
    if (!channel.publish(row) && !overflowReported) {
      overflowReported = true;
      emit(kj::str("{\"event\":\"snapshot-too-large\",\"pid\":", coworkerdPid(), ",\"bytes\":", row.size(), "}"));
    }
    ++publications; publicationCpuNs += cpuNs() - start; lastPublish = monotonicMs();
  }
  bool overflowReported = false;
 private:
  // The isolate fix B collects next: idle for the threshold, not hosting actors, its old generation grown the most since
  // its last full collection and by at least the minimum. `responsiveSince` is when the event loop last came out of a stall:
  // idle time counts from there too. `deferred` says a candidate was due by its own idle time but the loop has not been
  // free that long.
  IdleWorkerRef idleCandidate(double now, double responsiveSince, bool& deferred) {
    const auto& settings = gcSettings();
    IdleWorkerRef best;
    uint64_t bestGrowth = 0;
    for (auto& entry: workers) {
      auto worker = entry.lock();
      if (!worker || !worker->gc || worker->collect == kj::none || !worker->ready || !worker->created ||
          worker->removed || worker->destroyed || worker->active || worker->actorEvents ||
          now - worker->lastEnd < settings.idleCollectMs) continue;
      auto growth = worker->gc->oldGrowth();
      if (growth && growth >= settings.idleCollectMinBytes && growth > bestGrowth) { best = worker; bestGrowth = growth; }
    }
    deferred = best && now - responsiveSince < settings.idleCollectMs;
    return deferred ? IdleWorkerRef() : best;
  }
  // Fix B (coworkerd-gc.h): one isolate at a time, collected between its requests. Idle is not only a matter of the
  // isolate's clock: while the event loop is busy -- running another isolate's request, or paused by a collection -- the
  // requests that arrive meanwhile wait unread, and the isolate looks idle because its own next request is among them; a
  // collection then would lengthen the wait rather than use idle time. The loop's own ticks tell: one that comes more
  // than STALL_MS late ends a stall, and the isolate must also have been idle that long since then. Before collecting, what
  // is already queued on the event loop runs first (until it would sleep, waiting no longer than the idle threshold), and
  // the isolate must still be idle when its lock is taken.
  static constexpr double STALL_MS = 10;
  kj::Promise<void> collectLoop() {
    const auto& settings = gcSettings();
    const auto tick = std::clamp(interval, 10u, 100u);
    double responsiveSince = monotonicMs();
    while (true) {
      const auto expected = monotonicMs() + tick;
      co_await timer.afterDelay(tick * kj::MILLISECONDS);
      const auto now = monotonicMs();
      if (now - expected > STALL_MS) responsiveSince = now;
      bool deferred = false;
      auto candidate = idleCandidate(now, responsiveSince, deferred);
      if (deferred) countIdleCollection(IdleCollection::BUSY, true);
      if (!candidate) continue;
      bool quiet = co_await kj::yieldUntilWouldSleep().then([]() { return true; }).exclusiveJoin(
          timer.afterDelay(settings.idleCollectMs * kj::MILLISECONDS).then([]() { return false; }));
      auto lastEnd = candidate->lastEnd;
      auto stillIdle = [candidate, lastEnd]() {
        return !candidate->active && candidate->lastEnd == lastEnd && !candidate->removed && !candidate->destroyed;
      };
      if (!quiet || !stillIdle()) {
        countIdleCollection(IdleCollection::BUSY, true);
        continue;
      }
      kj::Maybe<kj::Promise<IdleCollection>> started;
      KJ_IF_SOME(collect, candidate->collect) { started = collect(candidate->gc, kj::mv(stillIdle)); }
      auto outcome = IdleCollection::GONE;
      KJ_IF_SOME(promise, started) {
        try {
          outcome = co_await kj::mv(promise);
        } catch (kj::Exception& error) {
          emit(kj::str("{\"event\":\"idle-collection-failed\",\"isolate\":", escapeCpuJsonString(candidate->name),
              ",\"pid\":", coworkerdPid(), ",\"reason\":", escapeCpuJsonString(error.getDescription()), "}"));
        }
      }
      if (outcome == IdleCollection::COLLECTED) ++candidate->idleCollections;
      countIdleCollection(outcome, false);
    }
  }
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
      publishDrain();
      // Actor activity never causes a full publication. Even an empty registry
      // answers drain requests and publishes its observation once per second.
      if (monotonicMs() - lastPublish >= 1000 &&
          (lifecycleChannel() || publishedBand || observesBackground ||
              (gcSettings().observes() && processGcVersion() != publishedGc))) {
        publish();
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
            worker->retireWhenIdle || now - worker->lastEnd <= minimum) continue;
        if (!oldest || worker->lastEnd < oldest->lastEnd) oldest = kj::mv(worker);
      }
      if (oldest) {
        // Activity and removal are decided synchronously on the same event loop.
        pending = oldest->id;
        KJ_IF_SOME(cb, oldest->remove) {
          auto remove = kj::mv(cb);
          oldest->remove = kj::none;
          idleWorkerRemoved(oldest,
              threshold && now - oldest->lastEnd > threshold ? "idle" : "watermark");
          remove();
        }
      }
    }
  }
  kj::Promise<void> retirementLoop() {
    while (!retiringWorkers.empty()) {
      co_await timer.afterDelay(std::min(interval, 100u) * kj::MILLISECONDS);
      if (retirementPending || monotonicMs() - lastDestroy < interval) continue;
      IdleWorkerRef oldest;
      // A pending supersession never turns on a scan of unrelated resident instances.
      for (auto it = retiringWorkers.begin(); it != retiringWorkers.end();) {
        auto worker = it->lock();
        if (!worker || worker->removed || worker->destroyed || worker->remove == kj::none) {
          it = retiringWorkers.erase(it);
          continue;
        }
        ++it;
        if (!worker->eligible || !worker->ready || !worker->created || worker->active) continue;
        KJ_IF_SOME(done, worker->backgroundDone) { if (!done()) continue; }
        if (!oldest || worker->lastEnd < oldest->lastEnd) oldest = kj::mv(worker);
      }
      if (oldest) {
        retirementPending = oldest->id;
        KJ_IF_SOME(cb, oldest->remove) {
          auto remove = kj::mv(cb);
          oldest->remove = kj::none;
          idleWorkerRemoved(oldest, "superseded");
          remove();
        }
      }
    }
    retirementLoopStarted = false;
  }
};

std::shared_ptr<IdleReaper> idleReaper(kj::Timer& timer) {
  return std::make_shared<IdleReaper>(timer);
}
void idleReaperRootLifecycle(const std::shared_ptr<IdleReaper>& reaper, bool durable) {
  reaper->durableLifecycle = durable;
  reaper->startLoop();
}
IdleWorkerRef registerIdleWorker(std::shared_ptr<IdleReaper> reaper,
    kj::StringPtr name, kj::Function<void()> remove, bool eligible, IdleWorkerLabels labels) {
  auto worker = std::make_shared<IdleWorker>(reaper, name, kj::mv(remove), reaper->nextId++);
  reaper->workers.push_back(worker);
  worker->eligible = eligible;
  worker->labels = std::move(labels);
  return worker;
}
void idleWorkerObserveBackground(const IdleWorkerRef& worker, kj::Function<bool()> backgroundDone) {
  if (!worker || worker->removed || worker->backgroundDone != kj::none) return;
  worker->backgroundDone = kj::mv(backgroundDone);
  if (auto owner = worker->owner.lock()) {
    owner->observesBackground = true;
    owner->startLoop();
  }
}
void idleWorkerRetireWhenIdle(const IdleWorkerRef& worker, kj::Function<bool()> backgroundDone) {
  if (!worker || worker->removed || worker->retireWhenIdle) return;
  worker->retireWhenIdle = true;
  worker->backgroundDone = kj::mv(backgroundDone);
  if (auto owner = worker->owner.lock()) {
    owner->retiringWorkers.push_back(worker);
    owner->startRetirementLoop();
  }
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
void detachIdleWorker(const IdleWorkerRef& worker) {
  if (worker) { worker->remove = kj::none; worker->collect = kj::none; worker->backgroundDone = kj::none; }
}
void idleWorkerSetGc(const IdleWorkerRef& worker, std::shared_ptr<IsolateGc> gc) { if (worker) worker->gc = kj::mv(gc); }
void idleWorkerSetCollector(const IdleWorkerRef& worker, IdleCollector collect) {
  if (worker) worker->collect = kj::mv(collect);
}
void idleWorkerActorEvent(const IdleWorkerRef& worker) {
  if (!worker) return;
  ++worker->actorEvents;
  // An actor's events never pass through the isolate's entry (activity()), so its last one counts as its last use: the
  // reaper does not evict an isolate whose actors are still receiving events.
  worker->lastActorEvent = worker->lastEnd = monotonicMs();
}

namespace {
struct Activity {
  IdleWorkerRef worker;
  explicit Activity(IdleWorkerRef worker): worker(kj::mv(worker)) { ++this->worker->active; }
  ~Activity() {
    KJ_ASSERT(worker->active); --worker->active; worker->lastEnd = monotonicMs();
  }
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
        if (owner->retirementPending == worker->id) { owner->retirementPending = 0; owner->lastDestroy = monotonicMs(); }
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
