#pragma once
// Coworkerd: V8's garbage collection under the CPU limit. A major GC -- one atomic mark-compact of the V8 heap and
// cppgc together, most of it the destruction of dead jsg wrappers -- runs inside the request whose allocation
// triggered it, and the CPU limit used to charge all of it to that request: the request was terminated, and its
// isolate with it, although its own JavaScript had used a few milliseconds. Here, each part enabled by the
// environment the node gives workerd (read once per process; without it nothing changes):
//
//   COWORKERD_GC_BUDGET_MS, COWORKERD_GC_REFILL_MS_PER_S
//       The thread CPU of a major GC that runs inside a JS entry is not charged to the entry, up to a token bucket
//       per Worker of that many milliseconds, refilled at that rate of wall time; beyond it the GC is charged as
//       before. The bucket is the Worker's (the isolate name before its version), not the isolate's, so an isolate
//       the limit revoked does not come back with a full one. 0: every GC is charged (as before).
//   COWORKERD_GC_LONG_PAUSE_MS
//       A collection whose pause reaches this writes one row, event gc-pause, where the CPU limit writes its rows
//       (the log's [cpu-limit] lines and the limit event pipe): which isolate, how long, in whose request, what kind.
//   COWORKERD_IDLE_GC_MS, COWORKERD_IDLE_GC_MIN_MIB
//       The idle reaper collects a dynamic isolate between requests once it has been idle that long and its old
//       generation grew that much since its last mark-compact (coworkerd-idle-eviction.c++).
//   COWORKERD_STATIC_HEAP_MIB
//       The heap constraint of the static isolates (the runtime), which V8 otherwise sizes from the machine.
#include <kj/async.h>
#include <kj/function.h>
#include <kj/string.h>
#include <v8-callbacks.h>
#include <v8-isolate.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <pthread.h>
#include <time.h>
#if __APPLE__
#include <mach/mach.h>
#endif

namespace workerd {
class Worker;
}

namespace workerd::server {

struct GcSettings {
  uint64_t budgetNs = 0, refillNsPerSecond = 0;
  uint64_t longPauseNs = 0;
  uint32_t idleCollectMs = 0;
  uint64_t idleCollectMinBytes = 4ull << 20;
  uint64_t staticHeapBytes = 0;
  bool excludes() const { return budgetNs != 0; }
  // Whether the isolates need the GC callbacks at all.
  bool observes() const { return excludes() || longPauseNs != 0 || idleCollectMs != 0; }
};
const GcSettings& gcSettings();

// A thread's CPU time, created on that thread and readable from any other (the limit watchdog reads the isolate
// thread's).
struct ThreadCpuClock {
#if __APPLE__
  mach_port_t thread = mach_thread_self();
  ThreadCpuClock() = default;
  ThreadCpuClock(const ThreadCpuClock&) = delete;
  ~ThreadCpuClock() { mach_port_deallocate(mach_task_self(), thread); }
  uint64_t now() const;
#else
  clockid_t clock;
  ThreadCpuClock();
  ThreadCpuClock(const ThreadCpuClock&) = delete;
  uint64_t now() const;
#endif
};

// A Worker's GC budget in this process (coworkerd-gc.c++).
class GcBudget;

// What one idle collection came to (coworkerd-idle-eviction.c++ counts them).
enum class IdleCollection { COLLECTED, BUSY, GONE };

// One isolate's collections: its GC callbacks, its CPU limit's entries and the idle reaper on the isolate's thread,
// and the limit watchdog on its own thread (the atomics). Owned by the isolate's limit enforcer, which outlives the
// V8 isolate (Worker::Isolate destroys its API, and with it the V8 isolate, before its limit enforcer), so the
// callbacks are never removed: they end with the isolate.
class IsolateGc {
 public:
  IsolateGc(kj::StringPtr name, bool dynamic, uint64_t cpuLimitNs);
  ~IsolateGc() noexcept(false);
  IsolateGc(const IsolateGc&) = delete;

  // On the isolate's thread, once, before any JavaScript: the callbacks, and the clock the exclusion reads.
  void install(v8::Isolate* isolate);

  // A JS entry of this isolate (the CPU limit's scope) for as long as it runs: the collections that happen meanwhile
  // run inside it, in the request `identity` names (the CPU limit's identity fragment; empty outside a request).
  class Entry {
   public:
    Entry(IsolateGc* gc, kj::StringPtr identity);
    ~Entry() noexcept(false);
    Entry(const Entry&) = delete;
    // The excluded GC CPU since this entry began: collections finished meanwhile and the one in progress (at most what
    // its Worker's budget held when it began), never more than the entry's own CPU. Read on the watchdog thread with
    // its registry locked, and on the isolate's thread when the entry ends. The in-progress start is read before the
    // total, so a collection that ends in between may be counted twice (a late termination), never missed (an early one).
    uint64_t excluded(uint64_t entryStartCpu, uint64_t cpuNow) const;

   private:
    IsolateGc* gc;
    uint64_t base = 0;
    kj::StringPtr previousIdentity;
    bool previousInside = false;
  };

  // The idle reaper's view: whether the old generation grew enough since the last mark-compact to be worth a
  // collection, and by how much.
  uint64_t oldGrowth() const { return oldUsed > oldAfterMajor ? oldUsed - oldAfterMajor : 0; }
  uint64_t majorCollections() const { return majors; }
  // A full collection of this isolate between requests (fix B): with the isolate locked on its own thread, V8 collects
  // synchronously -- a full GC that reduces the footprint, then another while that keeps freeing much.
  void collectIdle(v8::Isolate* isolate);
  // The isolate's old generation, measured again (at the end of a JS entry: what an allocation in old space added).
  void measureOld();

  kj::StringPtr getName() const { return name; }
  bool isDynamic() const { return dynamic; }

 private:
  static void prologue(v8::Isolate* isolate, v8::GCType type, v8::GCCallbackFlags flags, void* data);
  static void epilogue(v8::Isolate* isolate, v8::GCType type, v8::GCCallbackFlags flags, void* data);
  void began(v8::GCType type);
  void ended(v8::GCType type);
  size_t oldGenerationBytes() const;

  const kj::String name;
  const bool dynamic;
  const uint64_t cpuLimitNs;
  std::shared_ptr<GcBudget> budget;  // dynamic isolates while the exclusion is on
  v8::Isolate* isolate = nullptr;
  kj::Maybe<kj::Own<ThreadCpuClock>> clock;

  // The isolate's thread only.
  uint32_t depth = 0;
  v8::GCType type = v8::kGCTypeScavenge;
  uint64_t startCpu = 0, startMono = 0;
  bool startedInside = false, startedExcludable = false, idle = false;
  bool inside = false;
  kj::StringPtr identity;
  uint64_t majors = 0;
  // The old generation of the unified heap (V8's old spaces and cppgc's objects): at the last measurement, and right
  // after the last full collection.
  uint64_t oldUsed = 0, oldAfterMajor = 0, lastMeasured = 0;

  // Shared with the limit watchdog.
  std::atomic<uint64_t> excludableStartCpu{0};  // nonzero while an excludable collection runs: its start
  std::atomic<uint64_t> excludableBudgetNs{0};  // what its Worker's budget held when it began
  std::atomic<uint64_t> excludedNs{0};          // excluded GC CPU since this isolate began
};

// Fix B's collection, from the idle reaper: the Worker's async lock, then -- unless `stillIdle` says a request came
// meanwhile -- one collectIdle() under it.
kj::Promise<IdleCollection> collectIdleIsolate(
    kj::Own<const Worker> worker, std::shared_ptr<IsolateGc> gc, kj::Function<bool()> stillIdle);

// This process's collections as one JSON object member, ,"gc":{...}, for the lifecycle snapshot (the node's state
// endpoint shows it per process); and a number that changes whenever it does.
kj::String processGcJson();
uint64_t processGcVersion();
// The idle reaper's outcomes, counted with the rest.
void countIdleCollection(IdleCollection outcome, bool skippedQueued);

}  // namespace workerd::server
