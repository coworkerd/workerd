#include "coworkerd-gc.h"
#include "coworkerd-cpu-limit.h"

#include <workerd/io/worker.h>
#include <kj/debug.h>
#include <kj/mutex.h>
#include <cppgc/heap-statistics.h>
#include <v8-cppgc.h>
#include <v8-statistics.h>
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <map>
#include <string>
#include <unistd.h>

namespace workerd::server {
namespace {

uint64_t setting(const char* name, bool* given = nullptr) {
  const char* text = std::getenv(name);
  if (given) *given = text != nullptr;
  if (text == nullptr) return 0;
  KJ_REQUIRE(*text != '\0', "GC setting must be an unsigned 32-bit integer", name);
  uint64_t value = 0;
  for (const char* p = text; *p; ++p) {
    KJ_REQUIRE(*p >= '0' && *p <= '9', "GC setting must be an unsigned 32-bit integer", name);
    value = value * 10 + uint64_t(*p - '0');
    KJ_REQUIRE(value <= std::numeric_limits<uint32_t>::max(), "GC setting exceeds uint32", name);
  }
  return value;
}

GcSettings readSettings() {
  GcSettings s;
  s.budgetNs = setting("COWORKERD_GC_BUDGET_MS") * 1000000;
  s.refillNsPerSecond = setting("COWORKERD_GC_REFILL_MS_PER_S") * 1000000;
  s.longPauseNs = setting("COWORKERD_GC_LONG_PAUSE_MS") * 1000000;
  s.idleCollectMs = static_cast<uint32_t>(setting("COWORKERD_IDLE_GC_MS"));
  bool minGiven = false;
  auto minMiB = setting("COWORKERD_IDLE_GC_MIN_MIB", &minGiven);
  if (minGiven) s.idleCollectMinBytes = minMiB << 20;
  s.staticHeapBytes = setting("COWORKERD_STATIC_HEAP_MIB") << 20;
  return s;
}

uint64_t monotonicNs() {
  timespec t;
  KJ_REQUIRE(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
  return uint64_t(t.tv_sec) * 1000000000 + uint64_t(t.tv_nsec);
}
uint64_t unixMs() {
  timespec t;
  KJ_REQUIRE(clock_gettime(CLOCK_REALTIME, &t) == 0);
  return uint64_t(t.tv_sec) * 1000 + uint64_t(t.tv_nsec) / 1000000;
}

constexpr uint32_t MAJOR = v8::kGCTypeMarkSweepCompact | v8::kGCTypeIncrementalMarking;
constexpr uint32_t EXCLUDABLE = MAJOR | v8::kGCTypeProcessWeakCallbacks;

const char* typeName(v8::GCType type) {
  if (type & v8::kGCTypeMarkSweepCompact) return "mark-compact";
  if (type & v8::kGCTypeIncrementalMarking) return "incremental-marking";
  if (type & v8::kGCTypeProcessWeakCallbacks) return "weak-callbacks";
  if (type & v8::kGCTypeMinorMarkSweep) return "minor-mark-sweep";
  return "scavenge";
}

// The Worker an isolate belongs to: its name before the version. A dynamic isolate is named
// "<namespace>:<workerId>@<versionId>#<content>#<definition>[#limits=...]" (the node's runtime bridge); any other
// name is its own Worker.
std::string workerKey(kj::StringPtr name) {
  std::string text(name.cStr());
  auto hash = text.find('#');
  if (hash != std::string::npos) text.resize(hash);
  auto at = text.rfind('@');
  if (at != std::string::npos) text.resize(at);
  return text;
}

// What a static isolate (the runtime) holds once a mark-compact is done: its working set.
struct StaticHeap {
  uint64_t majors = 0, liveAfterMajorBytes = 0, peakLiveAfterMajorBytes = 0, heapLimitBytes = 0;
};

// This process's collections. Counted on the isolates' thread (the GC callbacks) and read by the idle reaper on the
// event loop; locked anyway, so that nothing depends on there being one such thread.
struct ProcessGc {
  uint64_t version = 0;
  uint64_t majors = 0, minors = 0, weakCallbacks = 0;
  uint64_t idleCollections = 0, idleSkippedBusy = 0, idleSkippedQueued = 0, idleSkippedGone = 0;
  uint64_t longPauses = 0;
  uint64_t longestPauseNs = 0;
  std::string longestPauseIsolate, longestPauseType;
  uint64_t firstMajorLongestNs = 0;
  std::string firstMajorLongestIsolate;
  uint64_t excludedNs = 0, chargedMajorNs = 0;
  std::map<std::string, StaticHeap> statics;
};
kj::MutexGuarded<ProcessGc>& processGc() {
  static kj::MutexGuarded<ProcessGc> value;
  return value;
}

void writeRow(kj::StringPtr row) {
  // As the CPU limit's rows: the log line with its prefix in one write, then the event pipe.
  auto line = kj::str("[cpu-limit] ", row);
  for (size_t done = 0; done < line.size();) {
    auto n = write(STDERR_FILENO, line.cStr() + done, line.size() - done);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;
    done += size_t(n);
  }
  writeLimitEvent(row);
}

}  // namespace

const GcSettings& gcSettings() {
  static const GcSettings settings = readSettings();
  return settings;
}

#if __APPLE__
uint64_t ThreadCpuClock::now() const {
  thread_basic_info_data_t info;
  mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
  KJ_REQUIRE(thread_info(thread, THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&info), &count) == KERN_SUCCESS);
  return (uint64_t(info.user_time.seconds) + info.system_time.seconds) * 1000000000 +
      (uint64_t(info.user_time.microseconds) + info.system_time.microseconds) * 1000;
}
#else
ThreadCpuClock::ThreadCpuClock() { KJ_REQUIRE(pthread_getcpuclockid(pthread_self(), &clock) == 0); }
uint64_t ThreadCpuClock::now() const {
  timespec time;
  KJ_REQUIRE(clock_gettime(clock, &time) == 0);
  return uint64_t(time.tv_sec) * 1000000000 + time.tv_nsec;
}
#endif

// A Worker's budget of excludable GC CPU in this process: a token bucket of the burst, refilled by wall time. Used on
// the isolates' thread only.
class GcBudget {
 public:
  explicit GcBudget(uint64_t now): tokens(double(gcSettings().budgetNs)), updated(now) {}
  // The tokens now: refilled by the wall time since the last touch, at most the burst.
  uint64_t refill(uint64_t now) {
    const auto& s = gcSettings();
    if (now > updated) {
      tokens = std::min(double(s.budgetNs), tokens + double(now - updated) / 1e9 * double(s.refillNsPerSecond));
      updated = now;
    }
    return uint64_t(tokens);
  }
  // Spends what a collection of `cpu` may exclude; returns that much.
  uint64_t spend(uint64_t cpu) {
    auto excluded = std::min(cpu, uint64_t(tokens));
    tokens -= double(excluded);
    return excluded;
  }
  bool full(uint64_t now) { return refill(now) >= gcSettings().budgetNs; }

 private:
  double tokens;
  uint64_t updated;
};

namespace {
// Every Worker's budget in this process, kept while an isolate of the Worker holds it and until it is full again (from
// then on it is the same as a new one), so that a new isolate of the Worker goes on with the old one's.
std::shared_ptr<GcBudget> budgetFor(kj::StringPtr isolateName) {
  static kj::MutexGuarded<std::map<std::string, std::shared_ptr<GcBudget>>> budgets;
  auto now = monotonicNs();
  auto lock = budgets.lockExclusive();
  std::erase_if(*lock, [now](auto& entry) { return entry.second.use_count() == 1 && entry.second->full(now); });
  auto key = workerKey(isolateName);
  auto found = lock->find(key);
  if (found != lock->end()) return found->second;
  auto made = std::make_shared<GcBudget>(now);
  lock->emplace(kj::mv(key), made);
  return made;
}
}  // namespace

IsolateGc::IsolateGc(kj::StringPtr name, bool dynamic, uint64_t cpuLimitNs)
    : name(kj::str(name)), dynamic(dynamic), cpuLimitNs(cpuLimitNs) {
  if (dynamic && cpuLimitNs && gcSettings().excludes()) budget = budgetFor(name);
}
IsolateGc::~IsolateGc() noexcept(false) {}

void IsolateGc::install(v8::Isolate* value) {
  isolate = value;
  clock = kj::heap<ThreadCpuClock>();
  value->AddGCPrologueCallback(&IsolateGc::prologue, this, v8::kGCTypeAll);
  value->AddGCEpilogueCallback(&IsolateGc::epilogue, this, v8::kGCTypeAll);
  if (dynamic && gcSettings().idleCollectMs) oldUsed = oldAfterMajor = oldGenerationBytes();
}

void IsolateGc::prologue(v8::Isolate*, v8::GCType type, v8::GCCallbackFlags, void* data) {
  static_cast<IsolateGc*>(data)->began(type);
}
void IsolateGc::epilogue(v8::Isolate*, v8::GCType type, v8::GCCallbackFlags, void* data) {
  static_cast<IsolateGc*>(data)->ended(type);
}

void IsolateGc::began(v8::GCType value) {
  // A collection inside another one (the weak callbacks of a mark-compact) is part of it.
  if (depth++ > 0) return;
  type = value;
  startCpu = KJ_ASSERT_NONNULL(clock)->now();
  startMono = monotonicNs();
  startedInside = inside;
  startedExcludable = false;
  if (startedInside && budget && (type & EXCLUDABLE)) {
    excludableBudgetNs.store(budget->refill(startMono), std::memory_order_release);
    excludableStartCpu.store(startCpu ? startCpu : 1, std::memory_order_release);
    startedExcludable = true;
  }
}

void IsolateGc::ended(v8::GCType) {
  if (depth == 0 || --depth > 0) return;
  auto cpuNow = KJ_ASSERT_NONNULL(clock)->now();
  const uint64_t cpu = cpuNow > startCpu ? cpuNow - startCpu : 0;
  const uint64_t pause = monotonicNs() - startMono;
  uint64_t excluded = 0, left = 0;
  if (startedExcludable) {
    excluded = budget->spend(cpu);
    left = budget->refill(startMono);
    excludedNs.fetch_add(excluded, std::memory_order_acq_rel);
    excludableStartCpu.store(0, std::memory_order_release);
  }
  const bool major = (type & MAJOR) != 0;
  if (major) ++majors;
  const auto& settings = gcSettings();
  if (dynamic && settings.idleCollectMs) {
    oldUsed = oldGenerationBytes();
    if (major) oldAfterMajor = oldUsed;
  }
  uint64_t live = 0, limit = 0;
  if (!dynamic && major) {
    v8::HeapStatistics stats;
    isolate->GetHeapStatistics(&stats);
    live = stats.used_heap_size();
    limit = stats.heap_size_limit();
    if (auto* cpp = isolate->GetCppHeap()) {
      live += cpp->CollectStatistics(cppgc::HeapStatistics::DetailLevel::kBrief).used_size_bytes;
    }
  }
  const bool longPause = settings.longPauseNs && pause >= settings.longPauseNs;
  {
    auto lock = processGc().lockExclusive();
    auto& p = *lock;
    ++p.version;
    if (major) ++p.majors;
    else if (type & v8::kGCTypeProcessWeakCallbacks) ++p.weakCallbacks;
    else ++p.minors;
    p.excludedNs += excluded;
    if (startedInside && (type & EXCLUDABLE)) p.chargedMajorNs += cpu - excluded;
    if (longPause) ++p.longPauses;
    if (pause > p.longestPauseNs) {
      p.longestPauseNs = pause;
      p.longestPauseIsolate = name.cStr();
      p.longestPauseType = typeName(type);
    }
    if (major && majors == 1 && pause > p.firstMajorLongestNs) {
      p.firstMajorLongestNs = pause;
      p.firstMajorLongestIsolate = name.cStr();
    }
    if (!dynamic && major) {
      auto& heap = p.statics[name.cStr()];
      ++heap.majors;
      heap.liveAfterMajorBytes = live;
      heap.peakLiveAfterMajorBytes = std::max(heap.peakLiveAfterMajorBytes, live);
      heap.heapLimitBytes = limit;
    }
  }
  if (longPause) {
    writeRow(kj::str("{\"event\":\"gc-pause\",\"isolate\":", escapeCpuJsonString(name), ',',
        startedInside ? identity : kj::StringPtr(""), "\"cpuUsedNs\":", cpu, ",\"cpuLimitMs\":", cpuLimitNs / 1000000,
        ",\"pid\":", coworkerdPid(), ",\"gcType\":\"", typeName(type), "\",\"pauseNs\":", pause,
        ",\"gcExcludedNs\":", excluded, ",\"gcBudgetNs\":", left, ",\"inRequest\":", startedInside ? "true" : "false",
        ",\"idleCollection\":", idle ? "true" : "false", ",\"majorOrdinal\":", major ? majors : 0,
        ",\"observedAtUnixMs\":", unixMs(), "}\n"));
  }
}

// The old generation of the unified heap: V8's spaces but the young ones, and cppgc's objects (the jsg wrappers' C++
// side, which a full collection of the V8 heap collects too and whose growth alone can trigger one).
size_t IsolateGc::oldGenerationBytes() const {
  size_t total = 0;
  auto spaces = isolate->NumberOfHeapSpaces();
  for (size_t i = 0; i < spaces; ++i) {
    v8::HeapSpaceStatistics space;
    if (!isolate->GetHeapSpaceStatistics(&space, i)) continue;
    kj::StringPtr spaceName = space.space_name();
    if (spaceName == "new_space" || spaceName == "new_large_object_space") continue;
    total += space.space_used_size();
  }
  if (auto* cpp = isolate->GetCppHeap()) {
    total += cpp->CollectStatistics(cppgc::HeapStatistics::DetailLevel::kBrief).used_size_bytes;
  }
  return total;
}

void IsolateGc::measureOld() {
  // At most every few milliseconds: a request's entries come in bursts, and the collections measure it anyway.
  if (!dynamic || !isolate || !gcSettings().idleCollectMs) return;
  auto now = monotonicNs();
  if (now - lastMeasured < 5000000) return;
  lastMeasured = now;
  oldUsed = oldGenerationBytes();
}

void IsolateGc::collectIdle(v8::Isolate* value) {
  idle = true;
  KJ_DEFER(idle = false);
  value->MemoryPressureNotification(v8::MemoryPressureLevel::kCritical);
}

IsolateGc::Entry::Entry(IsolateGc* gc, kj::StringPtr identity): gc(gc) {
  if (!gc) return;
  base = gc->excludedNs.load(std::memory_order_acquire);
  previousIdentity = gc->identity;
  previousInside = gc->inside;
  gc->identity = identity;
  gc->inside = true;
}
IsolateGc::Entry::~Entry() noexcept(false) {
  if (!gc) return;
  gc->identity = previousIdentity;
  gc->inside = previousInside;
}
uint64_t IsolateGc::Entry::excluded(uint64_t entryStartCpu, uint64_t cpuNow) const {
  if (!gc || !gc->budget) return 0;
  auto inGc = gc->excludableStartCpu.load(std::memory_order_acquire);
  auto inGcBudget = gc->excludableBudgetNs.load(std::memory_order_acquire);
  uint64_t total = gc->excludedNs.load(std::memory_order_acquire) - base;
  if (inGc && cpuNow > inGc) total += std::min(cpuNow - inGc, inGcBudget);
  return std::min(total, cpuNow > entryStartCpu ? cpuNow - entryStartCpu : uint64_t(0));
}

kj::Promise<IdleCollection> collectIdleIsolate(
    kj::Own<const Worker> worker, std::shared_ptr<IsolateGc> gc, kj::Function<bool()> stillIdle) {
  auto asyncLock = co_await worker->takeAsyncLockWithoutRequest(nullptr);
  if (!stillIdle()) co_return IdleCollection::BUSY;
  worker->runInLockScope(asyncLock, [&](Worker::Lock& lock) { gc->collectIdle(lock.getIsolate()); });
  co_return IdleCollection::COLLECTED;
}

void countIdleCollection(IdleCollection outcome, bool skippedQueued) {
  auto lock = processGc().lockExclusive();
  ++lock->version;
  if (skippedQueued) ++lock->idleSkippedQueued;
  else if (outcome == IdleCollection::COLLECTED) ++lock->idleCollections;
  else if (outcome == IdleCollection::BUSY) ++lock->idleSkippedBusy;
  else ++lock->idleSkippedGone;
}

uint64_t processGcVersion() { return processGc().lockShared()->version; }

kj::String processGcJson() {
  const auto& s = gcSettings();
  auto lock = processGc().lockShared();
  const auto& p = *lock;
  kj::Vector<kj::String> statics;
  for (const auto& [isolateName, heap]: p.statics) {
    statics.add(kj::str("{\"isolate\":", escapeCpuJsonString(isolateName.c_str()), ",\"majors\":", heap.majors,
        ",\"liveAfterMajorBytes\":", heap.liveAfterMajorBytes, ",\"peakLiveAfterMajorBytes\":",
        heap.peakLiveAfterMajorBytes, ",\"heapLimitBytes\":", heap.heapLimitBytes, "}"));
  }
  return kj::str(",\"gc\":{\"majorCollections\":", p.majors, ",\"minorCollections\":", p.minors,
      ",\"weakCallbacks\":", p.weakCallbacks, ",\"idleCollections\":", p.idleCollections,
      ",\"idleSkippedBusy\":", p.idleSkippedBusy, ",\"idleSkippedQueued\":", p.idleSkippedQueued,
      ",\"idleSkippedGone\":", p.idleSkippedGone, ",\"longPauses\":", p.longPauses,
      ",\"longPauseMs\":", s.longPauseNs / 1000000, ",\"longestPauseNs\":", p.longestPauseNs,
      ",\"longestPauseIsolate\":", escapeCpuJsonString(p.longestPauseIsolate.c_str()),
      ",\"longestPauseType\":", escapeCpuJsonString(p.longestPauseType.c_str()),
      ",\"firstMajorLongestNs\":", p.firstMajorLongestNs,
      ",\"firstMajorLongestIsolate\":", escapeCpuJsonString(p.firstMajorLongestIsolate.c_str()),
      ",\"excludedNs\":", p.excludedNs, ",\"chargedMajorNs\":", p.chargedMajorNs,
      ",\"budgetMs\":", s.budgetNs / 1000000, ",\"refillMsPerSecond\":", s.refillNsPerSecond / 1000000,
      ",\"idleCollectMs\":", s.idleCollectMs, ",\"idleCollectMinBytes\":", s.idleCollectMinBytes,
      ",\"staticHeapBytes\":", s.staticHeapBytes, ",\"static\":[", kj::strArray(statics, ","), "]}");
}

}  // namespace workerd::server
