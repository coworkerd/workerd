// Test the production implementation, including its private ownership types.
// The executable's main is renamed; KJ supplies this target's test main.
#define main coworkerdThreadedExecutableMain
#include "threaded-server.c++"
#undef main

#include <spawn.h>
#include <sys/wait.h>

#include <kj/test.h>
#include <kj/timer.h>

#include <barrier>
#include <future>

extern char** environ;

namespace workerd {
namespace {

struct TestPlatform {
  kj::Own<v8::Platform> base = jsg::defaultPlatform(2);
  server::WorkerdPlatform platform{*base};
  jsg::V8System system{platform, {}, base.get()};
};
void initializeV8() {
  static auto* platform = new TestPlatform;
  (void)platform;
}
// A group of its own for a test isolate, as the server creates one per Worker.
v8::IsolateGroup newGroup() {
  initializeV8();
  return KJ_ASSERT_NONNULL(server::tryNewIsolateGroup());
}
// customizeIsolate() runs with the isolate entered, as Worker::Isolate and sourceJsonFailure do.
void customizeEntered(IsolateLimits& limits, v8::Isolate* isolate) {
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  limits.customizeIsolate(isolate);
}

KJ_TEST("descriptor queue: empty, full, FIFO, SPSC handoff and close") {
  DescriptorQueue queue(2);
  KJ_EXPECT(queue.pop() == -1 && queue.hasSpace());
  int a = open("/dev/null", O_RDONLY | O_CLOEXEC);
  int b = open("/dev/null", O_RDONLY | O_CLOEXEC);
  KJ_ASSERT(a >= 0 && b >= 0);
  KJ_EXPECT(queue.push(a) && queue.push(b));
  KJ_EXPECT(!queue.hasSpace() && !queue.push(1000000));
  KJ_EXPECT(queue.pop() == a);
  ::close(a);
  queue.close();
  KJ_EXPECT(fcntl(b, F_GETFD) == -1 && errno == EBADF);
  KJ_EXPECT(queue.pop() == -1 && !queue.hasSpace() && !queue.push(1000000));

  DescriptorQueue concurrent(8);
  std::atomic<bool> done{false};
  std::thread producer([&] {
    for (unsigned i = 0; i < 2000; ++i) {
      int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
      while (!concurrent.push(fd)) std::this_thread::yield();
    }
    done.store(true, std::memory_order_release);
  });
  unsigned received = 0;
  while (!done.load(std::memory_order_acquire) || received < 2000) {
    int fd = concurrent.pop();
    if (fd < 0) {
      std::this_thread::yield();
      continue;
    }
    KJ_EXPECT(fcntl(fd, F_GETFD) >= 0);
    ::close(fd);
    ++received;
  }
  producer.join();
  KJ_EXPECT(received == 2000 && concurrent.pop() == -1);
}

KJ_TEST("actual reservation arithmetic, both watermarks and RAII release") {
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(2ull << 30, 10, 512);
  KJ_EXPECT(budget->memoryMaximum - budget->admissionMaximum == (2ull << 30) / 10);
  auto smaller = std::make_shared<ResourceBudget>(2ull << 30, 10, 16);
  KJ_EXPECT(smaller->memoryMaximum - smaller->admissionMaximum == (16ull << 20));
  const auto baseline = budget->memoryReserved.load();
  {
    auto reservation = std::make_shared<ResourceReservation>(budget, 128);
    KJ_ASSERT(reservation->registerPreparation());
    auto seed = budget->memoryReserved.load();
    KJ_EXPECT(seed > baseline && budget->residents.load() == 1);
    KJ_EXPECT(reservation->retainSource(100));
    KJ_EXPECT(budget->memoryReserved.load() == seed + 500);
    KJ_EXPECT(reservation->allocateExternal(4096));
    KJ_EXPECT(budget->memoryReserved.load() == seed + 500 + 4096);
    KJ_EXPECT(!reservation->allocateExternal(2ull << 30));
    KJ_EXPECT(budget->memoryReserved.load() == seed + 500 + 4096);
    reservation->freeExternal(4096);
    KJ_EXPECT(budget->memoryReserved.load() == seed + 500);
    {
      ResourcePreparation preparation(reservation);
      KJ_EXPECT(reservation->preparing == 1);
    }
    KJ_EXPECT(reservation->preparing == 0);
  }
  KJ_EXPECT(budget->memoryReserved.load() == baseline && budget->residents.load() == 0);
  auto memory = budget->memoryReserved.load();
  KJ_EXPECT(!budget->change(0, budget->admissionMaximum, 0, 0, true));
  KJ_EXPECT(budget->memoryReserved.load() == memory);
  KJ_EXPECT(!budget->change(0, budget->memoryMaximum, 0, 0, false, true));
  KJ_EXPECT(!budget->withinMaximum() && budget->reclaimRequested.load());
  KJ_EXPECT(budget->change(budget->memoryMaximum, 0, 0, 0, false, true));
  KJ_EXPECT(budget->memoryReserved.load() == baseline);
}

KJ_TEST("eviction excludes loading, referenced and open-context Workers") {
  auto slot = std::make_shared<Slot>();
  KJ_EXPECT(!canEvict(slot, nullptr));
  slot->complete.store(true);
  KJ_EXPECT(canEvict(slot, nullptr) && !canEvict(slot, slot.get()));
  {
    auto reference = slot;
    KJ_EXPECT(!canEvict(slot, nullptr));
  }
  slot->contexts.store(1);
  KJ_EXPECT(!canEvict(slot, nullptr));
  slot->contexts.store(0);
  KJ_EXPECT(canEvict(slot, nullptr));
}

KJ_TEST("tenant admission releases counts on errors and cancellation") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  auto budget = std::make_shared<TenantBudget>(1);
  {
    TenantPermit active(budget, "a");
    TenantPermit borrowed(budget, "a");
    KJ_EXPECT(budget->active["a"] == 2);
    {
      TenantPermit other(budget, "b");
      KJ_EXPECT(budget->active.size() == 2);
      KJ_EXPECT_THROW_RECOVERABLE_MESSAGE(
          "tenant execution concurrency limit reached in process", TenantPermit(budget, "a"));
      KJ_EXPECT_THROW_RECOVERABLE_MESSAGE(
          "tenant execution concurrency limit reached in process", TenantPermit(budget, "b"));
      KJ_EXPECT(budget->active["a"] == 2 && budget->active["b"] == 1);
    }
    KJ_EXPECT(budget->active.size() == 1);
    TenantPermit borrowedAgain(budget, "a");
    KJ_EXPECT(budget->active["a"] == 3);
  }
  KJ_EXPECT(budget->active.empty());
  auto pair = kj::newPromiseAndFulfiller<void>();
  auto canceled = pair.promise.attach(std::make_shared<TenantPermit>(budget, "a"));
  canceled = nullptr;
  KJ_EXPECT(budget->active.empty());
  auto disabled = std::make_shared<TenantBudget>(0);
  auto a = makeTenantPermit(disabled, "a");
  auto b = makeTenantPermit(disabled, "b");
  auto a2 = makeTenantPermit(disabled, "a");
  KJ_EXPECT(!a && !b && !a2 && disabled->active.empty());
  const auto refusal = capacityError(CapacityScope::TENANT);
  const auto detail = KJ_REQUIRE_NONNULL(refusal.getDetail(CAPACITY_DETAIL));
  KJ_EXPECT(detail.size() == 1 && detail[0] == 1);
}

KJ_TEST("subrequest quotas, recursive bindings, release wakeup and cancellation") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  SubrequestBudget budget(512, 16);
  auto parent = std::make_shared<uint32_t>(0);
  std::vector<std::unique_ptr<SubrequestPermit>> bindings;
  for (unsigned i = 0; i < 15; ++i) {
    auto permit = std::make_unique<SubrequestPermit>(budget, parent, "a", false);
    permit->acquire().wait(waitScope);
    bindings.push_back(std::move(permit));
  }
  auto child = std::make_shared<uint32_t>(0);
  SubrequestPermit childEgress(budget, child, "a", true);
  auto childReady = childEgress.acquire();
  KJ_EXPECT(childReady.poll(waitScope));
  childReady.wait(waitScope);
  KJ_EXPECT(budget.tenants["a"] == 1 && *parent == 15 && *child == 1);

  SubrequestBudget small(1, 16);
  auto invocation = std::make_shared<uint32_t>(0);
  auto active = std::make_unique<SubrequestPermit>(small, invocation, "a", true);
  active->acquire().wait(waitScope);
  SubrequestPermit queued(small, invocation, "a", true);
  auto pending = queued.acquire();
  KJ_EXPECT(!pending.poll(waitScope) && small.waiters.size() == 1);
  active.reset();
  pending.wait(waitScope);
  KJ_EXPECT(small.active.load() == 1 && small.waiters.empty());
  SubrequestPermit canceled(small, invocation, "a", true);
  auto abandoned = canceled.acquire();
  KJ_EXPECT(!abandoned.poll(waitScope) && small.waiters.size() == 1);
  abandoned = nullptr;
  KJ_EXPECT(small.waiters.empty());

  SubrequestBudget perCall(512, 128);
  auto one = std::make_shared<uint32_t>(0);
  std::vector<std::unique_ptr<SubrequestPermit>> held;
  for (unsigned i = 0; i < 32; ++i) {
    auto permit = std::make_unique<SubrequestPermit>(perCall, one, "a", false);
    permit->acquire().wait(waitScope);
    held.push_back(std::move(permit));
  }
  SubrequestPermit extra(perCall, one, "a", false);
  auto waiting = extra.acquire();
  KJ_EXPECT(!waiting.poll(waitScope));
  held.pop_back();
  waiting.wait(waitScope);
  KJ_EXPECT(*one == 32);
}

KJ_TEST("log quotas and a failed flush reset pending and in-flight buffers") {
  threaded::InvocationLogBudget bytes, lines;
  KJ_EXPECT(bytes.accept(threaded::InvocationLogBudget::MAX_BYTES));
  KJ_EXPECT(!bytes.accept(1) && !bytes.accept(UINT64_MAX));
  for (unsigned i = 0; i < 1023; ++i) KJ_ASSERT(lines.accept(0));
  KJ_EXPECT(!lines.accept(0));
  InvocationLogBuffer buffer;
  buffer.lines.add(kj::str("pending"));
  buffer.queuedLogBytes = 7;
  buffer.inFlightLogBytes = 11;
  buffer.inFlightLogLines = 1;
  buffer.flushing = true;
  buffer.resetAfterFlushFailure();
  KJ_EXPECT(buffer.lines.size() == 0 && buffer.queuedLogBytes == 0 &&
      buffer.inFlightLogBytes == 0 && buffer.inFlightLogLines == 0 && !buffer.flushing);
  buffer.lines.add(kj::str("recovered"));
  KJ_EXPECT(buffer.lines.size() == 1);
}

KJ_TEST("CPU remaining budget, previous CPU, nested attribution and late termination") {
  initializeV8();
  IsolateLimits limits(100, 32, kj::str("\"workerId\":\"a/cpu\","), nullptr, newGroup());
  auto* isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  v8::HandleScope handles(isolate);
  auto context = v8::Context::New(isolate);
  v8::Context::Scope inContext(context);
  auto healthySource = v8::String::NewFromUtf8Literal(isolate, "6 * 7");
  auto healthyScript = v8::Script::Compile(context, healthySource).ToLocalChecked();
  std::atomic<LimitCause> cause{LimitCause::NONE}, previous{LimitCause::NONE};
  limits.memory->pressure();
  KJ_EXPECT(limits.memory->pendingPressure.load() && !isolate->IsExecutionTerminating());
  {
    CpuExecution deferred(
        isolate, cause, limits.memoryState, limits.memory, 0, 100, 32, limits.identity);
    KJ_EXPECT(cause.load() == LimitCause::PROCESS_MEMORY && !limits.memory->pendingPressure.load());
    deferred.finish();
  }
  KJ_EXPECT(limits.memory->currentCause == nullptr && !isolate->IsExecutionTerminating());
  cause.store(LimitCause::NONE);
  limits.memory->currentCause = &previous;
  CpuExecution execution(
      isolate, cause, limits.memoryState, limits.memory, 90000000, 100, 32, limits.identity);
  KJ_EXPECT(execution.entry.allowance == 10000000 && execution.entry.previousCpu == 90000000);
  KJ_EXPECT(limits.memory->currentCause == &cause);
  cause.store(LimitCause::CPU);
  isolate->TerminateExecution();
  v8::TryCatch caught(isolate);
  // Consume the termination in a real JS entry. IsExecutionTerminating() alone
  // cannot distinguish an unprocessed interrupt from a successfully canceled one.
  auto stopped = healthyScript->Run(context);
  KJ_EXPECT(stopped.IsEmpty() && caught.HasTerminated());
  execution.finish();
  KJ_EXPECT(!isolate->IsExecutionTerminating() && limits.memory->currentCause == &previous);
  caught.Reset();
  auto healed = healthyScript->Run(context);
  KJ_EXPECT(!healed.IsEmpty());
  if (!healed.IsEmpty()) KJ_EXPECT(healed.ToLocalChecked()->Int32Value(context).FromJust() == 42);
  // Cleanup also makes a deliberately broken cancellation safe for later tests.
  isolate->CancelTerminateExecution();
  caught.Reset();
  execution.finish();
  KJ_EXPECT(limits.memory->currentCause == &previous);
  {
    // Isolate the CPU scope's cancellation from heap-statistic API calls.
    std::atomic<LimitCause> cpuCause{LimitCause::NONE};
    CpuExecution cpuOnly(
        isolate, cpuCause, limits.memoryState, nullptr, 0, 100, 32, limits.identity);
    cpuCause.store(LimitCause::CPU);
    isolate->TerminateExecution();
    auto terminated = healthyScript->Run(context);
    KJ_EXPECT(terminated.IsEmpty() && caught.HasTerminated());
    cpuOnly.finish();
    caught.Reset();
    auto nextTurn = healthyScript->Run(context);
    KJ_EXPECT(!nextTurn.IsEmpty());
    if (!nextTurn.IsEmpty())
      KJ_EXPECT(nextTurn.ToLocalChecked()->Int32Value(context).FromJust() == 42);
    isolate->CancelTerminateExecution();
    caught.Reset();
  }
}

void allocationComponent(kj::StringPtr code) {
  initializeV8();
  IsolateLimits neighborLimits(1000, 32, kj::str("\"workerId\":\"a/neighbor\","), nullptr, newGroup());
  auto* neighbor = v8::Isolate::New(neighborLimits.group, neighborLimits.getCreateParams());
  KJ_DEFER(neighbor->Dispose());
  customizeEntered(neighborLimits, neighbor);
  auto checkNeighbor = [&] {
    v8::Locker lock(neighbor);
    v8::Isolate::Scope scope(neighbor);
    v8::HandleScope handles(neighbor);
    auto context = v8::Context::New(neighbor);
    v8::Context::Scope entered(context);
    auto source = v8::String::NewFromUtf8Literal(neighbor, "6 * 7");
    auto script = v8::Script::Compile(context, source).ToLocalChecked();
    KJ_EXPECT(script->Run(context).ToLocalChecked()->Int32Value(context).FromJust() == 42);
  };
  checkNeighbor();
  auto budget = std::make_shared<ResourceBudget>(1ull << 30, 4, 1);
  auto reservation = std::make_shared<ResourceReservation>(budget, 32);
  KJ_ASSERT(reservation->registerPreparation());
  IsolateLimits limits(1000, 32, kj::str("\"workerId\":\"a/heap\","), reservation, newGroup());
  auto params = limits.getCreateParams();
  KJ_EXPECT(params.constraints.max_old_generation_size_in_bytes() > (1ull << 30));
  auto* isolate = v8::Isolate::New(limits.group, params);
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  v8::HandleScope handles(isolate);
  auto context = v8::Context::New(isolate);
  v8::Context::Scope inContext(context);
  std::atomic<LimitCause> cause{LimitCause::NONE};
  CpuExecution execution(
      isolate, cause, limits.memoryState, limits.memory, 0, 1000, 32, limits.identity);
  v8::TryCatch caught(isolate);
  auto source =
      v8::String::NewFromUtf8(isolate, code.begin(), v8::NewStringType::kNormal, code.size())
          .ToLocalChecked();
  auto script = v8::Script::Compile(context, source).ToLocalChecked();
  auto result = script->Run(context);
  caught.Reset();
  execution.finish();
  KJ_EXPECT(limits.memoryState->load() == LimitCause::MEMORY);
  KJ_EXPECT(!isolate->IsExecutionTerminating());
  auto metrics = reservation->metrics();
  KJ_EXPECT(reservation->heapPhysical > 0);
  KJ_EXPECT(threaded::rawField(metrics, "heapPhysicalBytes") ==
      std::to_string(reservation->heapPhysical));
  checkNeighbor();
}

KJ_TEST("a Worker's ArrayBuffers lie in its own isolate group's sandbox") {
  // MemoryBudget allocates in the group it was given: with a V8 sandbox per group, V8 ends the
  // process on the first backing store outside the isolate's own sandbox.
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(1ull << 30, 4, 1);
  auto reservation = std::make_shared<ResourceReservation>(budget, 32);
  KJ_ASSERT(reservation->registerPreparation());
  auto other = newGroup();
  IsolateLimits limits(1000, 32, kj::str("\"workerId\":\"a/buffers\","), reservation, newGroup());
  auto* isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  auto store = v8::ArrayBuffer::NewBackingStore(isolate, 1 << 20);
  KJ_EXPECT(limits.group.SandboxContains(store->Data()));
  KJ_EXPECT(!other.SandboxContains(store->Data()) || !v8::IsolateGroup::CanCreateNewGroups());
  // A buffer above the Worker's limit is refused before anything is reserved.
  KJ_EXPECT(limits.memory->MaxAllocationSize() == (32ull << 20));
}

KJ_TEST("single large heap allocation preserves the process") {
  allocationComponent("globalThis.retained = new Array(12*1024*1024).fill(7)");
}
KJ_TEST("continuous heap growth preserves the process") {
  allocationComponent(
      "globalThis.retained=[]; while(true) retained.push(new Array(50000).fill(7))");
}

// One V8 heap object of about 1 GiB (a flattened two-byte string of the
// maximum length) with almost nothing else live, and the dictionary-to-array
// growth of a 128 Mi-element array (the first reproduction's shape; one alone
// peaks at about 1.75 GiB and survives).
constexpr kj::StringPtr NEAR_GIB_STRING =
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;
constexpr kj::StringPtr NEAR_GIB_ARRAY =
    "globalThis.retained = new Array(128*1024*1024-8192).fill(1.5)"_kj;
// About 126 MiB live in small arrays (old-space pages, with their
// fragmentation), just under the 128 MiB limit, then one maximum string.
constexpr kj::StringPtr LIVE126_THEN_STRING =
    "globalThis.kept = []; for (let i = 0; i < 126 * 128; ++i) kept.push(new Array(1024).fill(1.5));"
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;
// The same with large regular objects, which pack worst into V8's 256 KiB
// pages: double arrays of 88,016 bytes (two per page, about a third wasted) and
// of 130,416 bytes (one per page, about half wasted), about 126 MiB of them.
constexpr kj::StringPtr LIVE126_86K_THEN_STRING =
    "globalThis.kept = []; for (let i = 0; i < 1501; ++i) kept.push(new Array(11000).fill(1.5));"
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;
// A 500 MB one-byte string first: V8 takes the first object of the young
// large-object space without checking its capacity or collecting, so the
// tenant check (after a collection) does not see it; then one maximum string.
// Before the near-heap-limit callback this ended the process (exit 74,
// raw/s27-x500).
constexpr kj::StringPtr YOUNG500_THEN_STRING =
    "const a = ' '.repeat(5e8); a.indexOf('z'); globalThis.kept = a;"
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;
constexpr kj::StringPtr LIVE126_128K_THEN_STRING =
    "globalThis.kept = []; for (let i = 0; i < 1013; ++i) kept.push(new Array(16300).fill(1.5));"
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;

// `count` isolates allocate at the same moment while a neighbor isolate keeps
// answering. Every heap of this build shares one pointer cage.
// `raised`: whether each isolate's ceiling must have been raised by the
// near-heap-limit callback (-1: not checked). The fragmentation reserve alone
// must let one maximum object finish after up to 126 MiB live, so those
// shapes never need the callback; only an object V8 placed without a
// collection (the 500 MB young one) does. Without this check the callback
// would hide a missing reserve.
void simultaneousAllocations(size_t count, kj::StringPtr code, int raised = -1) {
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(16ull << 30, 8, count + 1);
  std::vector<kj::Own<IsolateLimits>> limits;
  std::vector<v8::Isolate*> isolates;
  std::vector<uint64_t> peaks(count);
  std::vector<char> refused(count);
  for (size_t i = 0; i < count; ++i) {
    auto reservation = std::make_shared<ResourceReservation>(budget, 128);
    KJ_ASSERT(reservation->registerPreparation());
    limits.push_back(kj::refcounted<IsolateLimits>(
        10000, 128, kj::str("\"workerId\":\"a/large-", i, "\","), reservation, newGroup()));
    isolates.push_back(v8::Isolate::New(limits[i]->group, limits[i]->getCreateParams()));
    customizeEntered(*limits[i], isolates[i]);
  }
  KJ_DEFER(for (auto* isolate : isolates) isolate->Dispose());
  // No resident is idle during this test. Wake growers just as a maintenance
  // pass with no eviction candidate does, without holding their reservation lock.
  std::atomic<bool> finished{false};
  std::thread maintenance([&] {
    while (!finished.load()) {
      if (budget->reclaimRequested.exchange(false)) budget->finishReclaim(true);
      std::this_thread::yield();
    }
  });
  KJ_DEFER(finished.store(true); maintenance.join());
  // The neighbor is an ordinary resident: its own reservation in the same
  // budget and the default 128 MiB limit.
  auto neighborReservation = std::make_shared<ResourceReservation>(budget, 128);
  KJ_ASSERT(neighborReservation->registerPreparation());
  IsolateLimits neighborLimits(
      10000, 128, kj::str("\"workerId\":\"a/neighbor\","), neighborReservation, newGroup());
  auto* neighbor = v8::Isolate::New(neighborLimits.group, neighborLimits.getCreateParams());
  KJ_DEFER(neighbor->Dispose());
  customizeEntered(neighborLimits, neighbor);
  auto runNeighbor = [&](kj::StringPtr code, int32_t expected) {
    v8::Locker lock(neighbor);
    v8::Isolate::Scope scope(neighbor);
    v8::HandleScope handles(neighbor);
    auto context = v8::Context::New(neighbor);
    v8::Context::Scope entered(context);
    std::atomic<LimitCause> cause{LimitCause::NONE};
    CpuExecution execution(neighbor, cause, neighborLimits.memoryState, neighborLimits.memory, 0,
        10000, 128, neighborLimits.identity);
    v8::TryCatch caught(neighbor);
    auto source =
        v8::String::NewFromUtf8(neighbor, code.begin(), v8::NewStringType::kNormal, code.size())
            .ToLocalChecked();
    auto result = v8::Script::Compile(context, source).ToLocalChecked()->Run(context);
    bool answered =
        !result.IsEmpty() && result.ToLocalChecked()->Int32Value(context).FromJust() == expected;
    caught.Reset();
    execution.finish();
    KJ_EXPECT(answered && cause.load() == LimitCause::NONE &&
            neighborLimits.memoryState->load() == LimitCause::NONE,
        code);
  };
  auto checkNeighbor = [&] { runNeighbor("6 * 7"_kj, 42); };
  checkNeighbor();
  const auto participants = static_cast<std::ptrdiff_t>(count + 1);
  std::barrier start(participants), completed(participants), release(participants);
  std::vector<std::thread> workers;
  for (size_t i = 0; i < count; ++i)
    workers.emplace_back([&, i] {
      auto* isolate = isolates[i];
      v8::Locker lock(isolate);
      v8::Isolate::Scope scope(isolate);
      v8::HandleScope handles(isolate);
      auto context = v8::Context::New(isolate);
      v8::Context::Scope entered(context);
      auto source =
          v8::String::NewFromUtf8(isolate, code.begin(), v8::NewStringType::kNormal, code.size())
              .ToLocalChecked();
      auto script = v8::Script::Compile(context, source).ToLocalChecked();
      start.arrive_and_wait();
      std::atomic<LimitCause> cause{LimitCause::NONE};
      CpuExecution execution(isolate, cause, limits[i]->memoryState, limits[i]->memory, 0, 10000,
          128, limits[i]->identity);
      v8::TryCatch caught(isolate);
      auto result = script->Run(context);
      completed.arrive_and_wait();
      release.arrive_and_wait();
      caught.Reset();
      execution.finish();
      peaks[i] = limits[i]->memory->peakUsedBytes.load();
      refused[i] = limits[i]->memoryState->load() == LimitCause::MEMORY ||
          cause.load() == LimitCause::PROCESS_MEMORY;
    });
  start.arrive_and_wait();
  checkNeighbor();
  completed.arrive_and_wait();
  // While the large objects are alive the neighbor still allocates 32 MiB.
  checkNeighbor();
  runNeighbor("globalThis.kept = new Array(4 * 1024 * 1024).fill(1.5); kept.length"_kj,
      4 * 1024 * 1024);
  release.arrive_and_wait();
  for (auto& worker: workers) worker.join();
  for (size_t i = 0; i < count; ++i) {
    KJ_EXPECT(refused[i] && peaks[i] >= (128ull << 20), i, peaks[i]);
    const int ceilingRaised = limits[i]->memory->ceilingRaised.load();
    if (raised >= 0) KJ_EXPECT(ceilingRaised == raised, i, ceilingRaised, raised);
    fprintf(stdout, "CONCURRENT_HEAP worker=%zu peak=%llu separateCages=%d ceilingRaised=%d\n", i,
        static_cast<unsigned long long>(peaks[i]), v8::IsolateGroup::CanCreateNewGroups(), ceilingRaised);
  }
  checkNeighbor();
}

// Scenarios that can end the process run in a fresh copy of this binary,
// selected by the parent through the environment; otherwise this is a no-op.
constexpr uint ALLOCATION_CHILD_LINE = __LINE__ + 1;
KJ_TEST("child body: simultaneous near-GiB heap allocations, selected by a parent test") {
  auto selected = getenv("COWORKERD_THREADED_ALLOCATION_CHILD");
  if (selected == nullptr) return;
  kj::StringPtr spec(selected);
  auto colon = KJ_REQUIRE_NONNULL(spec.findFirst(':'), "expected <count>:<string|array|live126|live86k|live128k|young500>");
  auto count = kj::str(spec.first(colon)).parseAs<uint>();
  auto kind = spec.slice(colon + 1);
  KJ_REQUIRE(count > 0 && count <= 8 &&
      (kind == "string" || kind == "array" || kind == "live126" || kind == "live86k" || kind == "live128k" ||
          kind == "young500"));
  simultaneousAllocations(count,
      kind == "array"          ? NEAR_GIB_ARRAY
          : kind == "live126"  ? LIVE126_THEN_STRING
          : kind == "live86k"  ? LIVE126_86K_THEN_STRING
          : kind == "live128k" ? LIVE126_128K_THEN_STRING
          : kind == "young500" ? YOUNG500_THEN_STRING
                               : NEAR_GIB_STRING,
      count == 1 ? int(kind == "young500") : -1);
  fprintf(stdout, "ALLOCATION_CHILD survived count=%u kind=%s\n", count, kind.cStr());
  fflush(stdout);
}

struct ChildResult {
  int status;
  std::string events;  // the rows the child wrote to its limit event descriptor
};
ChildResult runAllocationChild(kj::StringPtr spec) {
  auto filter = kj::str("--filter=threaded-server-test.c++:", ALLOCATION_CHILD_LINE);
  auto selector = kj::str("COWORKERD_THREADED_ALLOCATION_CHILD=", spec);
  // The child's limit event descriptor, as the node passes one to the server.
  int events[2];
  KJ_REQUIRE(pipe2(events, O_CLOEXEC) == 0);
  int childEvents = fcntl(events[1], F_DUPFD, 100);
  KJ_REQUIRE(childEvents >= 0);
  auto eventFd = kj::str("COWORKERD_EVENT_FD=", childEvents);
  kj::Vector<char*> env;
  for (char** at = environ; *at != nullptr; ++at)
    if (!kj::StringPtr(*at).startsWith("COWORKERD_EVENT_FD=")) env.add(*at);
  env.add(selector.begin());
  env.add(eventFd.begin());
  env.add(nullptr);
  char program[] = "/proc/self/exe";
  char* argv[] = {program, filter.begin(), nullptr};
  fflush(stdout);
  fflush(stderr);
  pid_t pid;
  KJ_REQUIRE(posix_spawn(&pid, program, nullptr, nullptr, argv, env.begin()) == 0);
  close(childEvents);
  close(events[1]);
  // A child that neither finishes nor exits within a minute is a failure of
  // its own kind (reported as killed by SIGKILL), never a silent pass.
  int status = 0;
  for (unsigned waited = 0;; ++waited) {
    auto done = waitpid(pid, &status, WNOHANG);
    if (done < 0 && errno == EINTR) continue;
    KJ_REQUIRE(done == pid || done == 0);
    if (done == pid) break;
    if (waited == 600) kill(pid, SIGKILL);
    usleep(100000);
  }
  std::string rows;
  char buffer[4096];
  for (ssize_t n; (n = read(events[0], buffer, sizeof(buffer))) != 0;) {
    if (n < 0 && errno == EINTR) continue;
    KJ_REQUIRE(n > 0);
    rows.append(buffer, n);
  }
  close(events[0]);
  fprintf(stdout, "ALLOCATION_CHILD spec=%s exited=%d code=%d signal=%d\n", spec.cStr(),
      WIFEXITED(status), WIFEXITED(status) ? WEXITSTATUS(status) : -1,
      WIFSIGNALED(status) ? WTERMSIG(status) : 0);
  fprintf(stdout, "ALLOCATION_CHILD_EVENTS %zu bytes\n%s", rows.size(), rows.c_str());
  fflush(stdout);
  return {status, kj::mv(rows)};
}

KJ_TEST("126 MiB live then one near-GiB object preserves the process and a neighboring Worker") {
  // Without the fragmentation reserve this child ended with V8's fatal OOM
  // (exit 74) at 1150.5 MiB live in 1154.8 MiB of pages.
  auto child = runAllocationChild("1:live126");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

KJ_TEST("126 MiB live in 86 KiB objects then one near-GiB object preserves the process") {
  // With a reserve of a quarter of the limit this child ended with V8's fatal
  // OOM (exit 74) at 1150.1 MB live in 1212.0 MB of pages.
  auto child = runAllocationChild("1:live86k");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

KJ_TEST("126 MiB live in objects near 128 KiB then one near-GiB object preserves the process") {
  auto child = runAllocationChild("1:live128k");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

KJ_TEST("a 500 MB young object then one near-GiB object preserves the process") {
  auto child = runAllocationChild("1:young500");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

KJ_TEST("one near-GiB heap object preserves the process and a neighboring Worker") {
  auto child = runAllocationChild("1:string");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

// Four isolates each finish an object of about 1 GiB at the same moment while a neighbor keeps
// answering. With a pointer cage (and a V8 sandbox) per isolate (--config=coworkerd-sandbox), each
// heap has its own 4 GiB, so the process survives. With one shared 4 GiB cage this ended the
// process with V8's fatal OOM (exit 74; the known failure until S3, v137 §6.24), which a build
// without per-isolate cages still reproduces: that branch keeps the old expectation.
KJ_TEST("four simultaneous near-GiB heap objects preserve the process and a neighboring Worker") {
  auto child = runAllocationChild("4:string");
  if (v8::IsolateGroup::CanCreateNewGroups()) {
    KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
    KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
    return;
  }
  bool reproduced = WIFEXITED(child.status) && WEXITSTATUS(child.status) == 74;
  fprintf(stdout, "KNOWN_FAILURE four-simultaneous-near-GiB reproduced=%d\n", reproduced);
  fflush(stdout);
  KJ_EXPECT(reproduced, "the shared-cage failure changed; update its status", child.status);
  // The OOM row names the execution whose allocation failed and carries its
  // isolate's heap: far below that Worker's 128 MiB limit, because the other
  // isolates filled the shared cage. The node must not blame it.
  if (reproduced) {
    auto at = child.events.find("\"event\":\"memory-culprit\"");
    KJ_ASSERT(child.events.find("\"event\":\"memory-oom-undetermined\"") != std::string::npos);
    KJ_ASSERT(at != std::string::npos, "the OOM thread was executing a Worker");
    auto line = child.events.substr(at, child.events.find('\n', at) - at);
    auto field = line.find("\"oomIsolateHeapBytes\":");
    KJ_ASSERT(field != std::string::npos, line.c_str());
    KJ_EXPECT(line.find("\"oomIsolateHeapReported\":true") != std::string::npos, line.c_str());
    auto heap = strtoull(line.c_str() + field + strlen("\"oomIsolateHeapBytes\":"), nullptr, 10);
    KJ_EXPECT(heap > 0 && heap < (128ull << 20), heap, line.c_str());
  }
}

KJ_TEST("deadline decimal spelling and native refusal mappings") {
  for (auto literal: {"123456789.12345679", "123456789.12345678", "9007199254740991",
         "1.2345678901234567e+12", "1e+09", "0.00100", "0"}) {
    auto descriptor = kj::str(
        "{\"decoy\":[{\"lease\":\"x\\\"}y\"}],\"lease\":{\"expiresAtMonotonicMs\":", literal, "}}");
    auto raw = threaded::rawPath(descriptor.cStr(), {"lease", "expiresAtMonotonicMs"});
    KJ_EXPECT(raw == literal);
    auto forwarded = kj::str("{\"deadline\":", kj::heapString(raw.data(), raw.size()), "}");
    KJ_EXPECT(threaded::rawField(forwarded.cStr(), "deadline") == literal);
  }
  for (auto item: {std::pair{LimitCause::CPU, "CPU_LIMIT_EXCEEDED"},
         std::pair{LimitCause::MEMORY, "MEMORY_LIMIT_EXCEEDED"},
         std::pair{LimitCause::PROCESS_MEMORY, "PROCESS_MEMORY_PRESSURE"}}) {
    auto error = limitException(item.first);
    auto detail = KJ_REQUIRE_NONNULL(error.getDetail(jsg::COWORKERD_CODE_DETAIL_ID));
    KJ_EXPECT(detail.size() == strlen(item.second) &&
        memcmp(detail.begin(), item.second, detail.size()) == 0);
    fprintf(stdout, "NATIVE_ERROR_CODE %.*s\n", int(detail.size()),
        reinterpret_cast<const char*>(detail.begin()));
  }
  auto capacity = capacityError();
  KJ_EXPECT(capacity.getDetail(CAPACITY_DETAIL) != kj::none);
  KJ_EXPECT(KJ_EXCEPTION(FAILED, "PROCESS_MEMORY_PRESSURE").getDetail(CAPACITY_DETAIL) == kj::none);
  using R = threaded::Refusal;
  struct Expected {
    R reason;
    unsigned status;
    std::string_view code;
  };
  for (auto row:
      {Expected{R::UNSUPPORTED, 501, "NOT_SUPPORTED"}, Expected{R::CPU, 503, "CPU_LIMIT_EXCEEDED"},
        Expected{R::MEMORY, 503, "MEMORY_LIMIT_EXCEEDED"},
        Expected{R::OVERLOADED, 503, "OVERLOADED"},
        Expected{R::DRAINING, 503, "PROCESS_DRAINING"}}) {
    auto info = threaded::refusalInfo(row.reason);
    KJ_EXPECT(info.status == row.status && info.code == row.code);
    fprintf(stdout, "NATIVE_ERROR_STATUS %u %.*s\n", info.status, int(info.code.size()),
        info.code.data());
  }
  fflush(stdout);
}

KJ_TEST("legacy named HTTP entrypoints select the global handler") {
  KJ_EXPECT(threaded::normalizeEntrypoint("v1", "Named") == "default");
  KJ_EXPECT(threaded::normalizeEntrypoint("v1", "") == "default");
  KJ_EXPECT(threaded::normalizeEntrypoint("v2", "") == "default");
  KJ_EXPECT(threaded::normalizeEntrypoint("v2", "Named") == "Named");
}

KJ_TEST("bridge and decode failures retain their distinct trial stages") {
  using L = threaded::LoadStage;
  auto bridge = threaded::loadStageInfo(L::LOAD);
  auto package = threaded::loadStageInfo(L::PACKAGE);
  auto decode = threaded::loadStageInfo(L::DECODE);
  auto evaluation = threaded::loadStageInfo(L::EVALUATION);
  KJ_EXPECT(bridge.stage == "bridge" && bridge.category == "LOAD_FAILED");
  KJ_EXPECT(package.stage == "code" && package.codeStage == "package" && package.name == "Error");
  KJ_EXPECT(decode.stage == "code" && decode.codeStage == "decode" && decode.name == "SyntaxError");
  KJ_EXPECT(evaluation.stage == "evaluation" && evaluation.category == "TOP_LEVEL_THROW");
}

KJ_TEST("the naive ordering counterexample inherits the parent order") {
  KJ_EXPECT(threaded::childOrderSequence(false, "9", 1) == "0");
  KJ_EXPECT(threaded::childOrderSequence(true, "0", 7) == "7");
  KJ_EXPECT(threaded::childOrderSequence(true, "\"0\"", 7) == "7");
  KJ_EXPECT(threaded::childOrderSequence(true, "9", 1) == "9");
  KJ_EXPECT(threaded::childOrderSequence(true, "\"9\"", 1) == "\"9\"");
}

KJ_TEST("descriptor preflight raises a low soft limit and rejects a low hard limit") {
  for (bool hardLow: {false, true}) {
    auto pid = fork();
    KJ_REQUIRE(pid >= 0);
    if (pid == 0) {
      struct rlimit limits;
      if (getrlimit(RLIMIT_NOFILE, &limits) != 0) _exit(2);
      limits.rlim_cur = hardLow ? 128 : 1024;
      if (hardLow) limits.rlim_max = 128;
      if (setrlimit(RLIMIT_NOFILE, &limits) != 0) _exit(3);
      auto accepted = prepareDescriptors(512);
      if (getrlimit(RLIMIT_NOFILE, &limits) != 0) _exit(4);
      _exit(hardLow ? (accepted ? 5 : 0) : (accepted && limits.rlim_cur >= 4352 ? 0 : 6));
    }
    int status;
    KJ_REQUIRE(waitpid(pid, &status, 0) == pid);
    KJ_EXPECT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }
}

KJ_TEST("validation reports retain the original JavaScript exception") {
  Reporter reporter;
  reporter.addError(kj::str("Uncaught SyntaxError: Unexpected token '='\nat index.js:1:6"));
  reporter.addError(kj::str("a later validation error"));
  bool rejected = false;
  try {
    reporter.requireValid();
  } catch (const kj::Exception& error) {
    rejected = true;
    KJ_EXPECT(
        error.getDescription() == "Uncaught SyntaxError: Unexpected token '='\nat index.js:1:6");
  }
  KJ_EXPECT(rejected);
}

KJ_TEST("source JSON failures preserve the actual Worker decoder exception") {
  initializeV8();
  IsolateLimits limits(100, 32, kj::str("\"workerId\":\"a/source\","), nullptr, newGroup());
  bool failed = false;
  KJ_IF_SOME(error, sourceJsonFailure("not JSON", limits)) {
    failed = true;
    KJ_EXPECT(error == "SyntaxError: Unexpected token 'o', \"not JSON\" is not valid JSON");
  }
  KJ_EXPECT(failed);
  IsolateLimits valid(100, 32, kj::str("\"workerId\":\"a/source\","), nullptr, newGroup());
  KJ_EXPECT(sourceJsonFailure("{}", valid) == kj::none);
}

KJ_TEST("compatibility refusals are tenant validation before package decoding") {
  Slot slot;
  Reporter reporter;
  JsonDoc invalid(kj::str(
      "{\"compatibilityDate\":\"2026-11-01\",\"compatibilityFlags\":[\"no_such_threaded_test_flag\"]}"));
  bool rejected = false;
  try {
    compileTenantFlags(slot, invalid.get(), reporter);
  } catch (const kj::Exception&) {
    rejected = true;
  }
  KJ_EXPECT(rejected && slot.loadStage == threaded::LoadStage::EVALUATION);
  Slot validSlot;
  Reporter validReporter;
  JsonDoc valid(kj::str("{\"compatibilityDate\":\"2026-11-01\",\"compatibilityFlags\":[]}"));
  KJ_EXPECT(!compileTenantFlags(validSlot, valid.get(), validReporter).getPreciseTimers());
}

KJ_TEST("waitUntil drain outlives child-call authority and ends 30 seconds after the response") {
  initializeV8();
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  // The timer's cached time lags the monotonic clock by the last JavaScript
  // turn; the deadline must come from the clock, not from the cached time.
  auto lagging = kj::systemPreciseMonotonicClock().now() - 5 * kj::SECONDS;
  kj::TimerImpl clock(lagging);
  Timer timer(clock);
  IsolateLimits isolateLimits(100, 32, kj::str("\"workerId\":\"a/background\","), nullptr, newGroup());
  Slot slot;
  slot.limits = &isolateLimits;
  Measurement measurement;
  RequestLimits limits(slot, measurement, timer, kj::str("\"workerId\":\"a/background\","));
  KJ_EXPECT(WAIT_UNTIL_LIMIT == 30 * kj::SECONDS);
  // limitDrain() is called when the response has ended. A 2 s child-call
  // lease and later waitUntil work (the 2.4 s legacy trigger) stay inside it.
  auto before = kj::systemPreciseMonotonicClock().now();
  auto drain = limits.limitDrain();
  auto after = kj::systemPreciseMonotonicClock().now();
  clock.advanceTo(before + 2400 * kj::MILLISECONDS);
  KJ_EXPECT(!drain.poll(waitScope));
  // A delay from the cached time would have ended here, 5 s early.
  clock.advanceTo(lagging + WAIT_UNTIL_LIMIT);
  KJ_EXPECT(!drain.poll(waitScope), "the deadline must not lose the last JavaScript turn");
  clock.advanceTo(before + WAIT_UNTIL_LIMIT - 1 * kj::MILLISECONDS);
  KJ_EXPECT(!drain.poll(waitScope));
  clock.advanceTo(after + WAIT_UNTIL_LIMIT);
  bool ended = drain.poll(waitScope);
  KJ_EXPECT(ended, "waitUntil work must end at the drain limit");
  if (ended) drain.wait(waitScope);
}

KJ_TEST("a call thread cancels background work that outlives the drain limit") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  kj::TimerImpl clock(kj::origin<kj::TimePoint>());
  Errors errors;
  kj::TaskSet tasks(errors);
  // Work that ends in time is awaited, not canceled.
  auto finished = kj::newPromiseAndFulfiller<void>();
  tasks.add(kj::mv(finished.promise));
  auto settled = settleBackground(clock, tasks);
  clock.advanceTo(clock.now() + WAIT_UNTIL_LIMIT);
  KJ_EXPECT(!settled.poll(waitScope));
  finished.fulfiller->fulfill();
  bool awaited = settled.poll(waitScope);
  KJ_EXPECT(awaited);
  if (awaited) settled.wait(waitScope);
  KJ_EXPECT(tasks.isEmpty());
  // A task that never ends (a timer loop the drain failed to cancel) holds
  // the thread only until the backstop, then it is canceled.
  bool canceled = false;
  tasks.add(kj::Promise<void>(kj::NEVER_DONE).attach(kj::defer([&canceled] { canceled = true; })));
  auto start = clock.now();
  auto backstop = settleBackground(clock, tasks);
  clock.advanceTo(start + BACKGROUND_BACKSTOP - 1 * kj::MILLISECONDS);
  KJ_EXPECT(!backstop.poll(waitScope) && !canceled);
  clock.advanceTo(start + BACKGROUND_BACKSTOP);
  bool returned = backstop.poll(waitScope);
  KJ_EXPECT(returned, "the call thread must return at the backstop");
  if (returned) backstop.wait(waitScope);
  KJ_EXPECT(canceled && tasks.isEmpty());
  KJ_EXPECT(BACKGROUND_BACKSTOP > WAIT_UNTIL_LIMIT);
}

KJ_TEST("a reclaim requested during maintenance never waits for that pass") {
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(2ull << 30, 4, 1);
  std::promise<bool> result;
  auto future = result.get_future();
  // An eviction inside the pass can reach reserve()/observe(). The pass entry
  // point the server's maintain() uses sets the flag itself.
  std::thread maintenance([&] {
    KJ_EXPECT(!ResourceBudget::maintaining);
    budget->runMaintenance([&] {
      KJ_EXPECT(ResourceBudget::maintaining, "the maintenance pass must mark its own thread");
      result.set_value(budget->reclaim(1));
    });
    KJ_EXPECT(!ResourceBudget::maintaining);
  });
  bool returned = future.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  KJ_EXPECT(returned, "the maintenance thread waited for its own reclaim pass");
  if (!returned) budget->finishReclaim(true);
  maintenance.join();
  KJ_EXPECT(!future.get() && budget->reclaimRequested.load());
  // The server's own entry (ThreadedServer::maintain()) marks the pass it runs.
  struct StandIn {
    std::shared_ptr<ResourceBudget> resources;
    bool marked = false;
    void maintainPass() { marked = ResourceBudget::maintaining; }
  } standIn{budget};
  runMaintenanceOf(standIn);
  KJ_EXPECT(standIn.marked, "the server's maintenance entry must mark its own pass");
  KJ_EXPECT(!ResourceBudget::maintaining);
  // Any other thread still waits for a completed pass.
  std::atomic<bool> stop{false};
  std::thread pass([&] {
    while (!stop.load()) {
      if (budget->reclaimRequested.exchange(false)) budget->finishReclaim(false);
      std::this_thread::yield();
    }
  });
  KJ_EXPECT(budget->reclaim(1));
  stop.store(true);
  pass.join();
}

KJ_TEST("HTTP entry evidence comes from delivered requests, not canceled probes") {
  int fds[2];
  KJ_ASSERT(pipe2(fds, O_NONBLOCK | O_CLOEXEC) == 0);
  KJ_DEFER(close(fds[0]); close(fds[1]));
  kj::HttpHeaderTable::Builder builder;
  auto callId = builder.add("x-placement-call-id");
  auto table = builder.build();
  kj::HttpHeaders headers(*table);
  JsonDoc input(kj::str("{\"serverId\":\"s2\",\"processId\":\"p1\","
                        "\"processGeneration\":\"3\",\"workerId\":\"caller\",\"versionId\":\"v1\","
                        "\"invocationId\":\"i7\",\"treeId\":\"t1\"}"));
  char buffer[2048];
  httpEntryObserver(input.get(), "", headers, callId, fds[1])->delivered();
  KJ_EXPECT(read(fds[0], buffer, sizeof(buffer)) == -1 && errno == EAGAIN);
  headers.setPtr(callId, "probe-7");
  { auto canceled = httpEntryObserver(input.get(), "", headers, callId, fds[1]); }
  KJ_EXPECT(read(fds[0], buffer, sizeof(buffer)) == -1 && errno == EAGAIN);
  httpEntryObserver(input.get(), "", headers, callId, fds[1])->delivered();
  auto count = read(fds[0], buffer, sizeof(buffer));
  KJ_ASSERT(count > 0);
  auto stored = kj::str(kj::arrayPtr(buffer, count));
  auto line = stored.asPtr();
  KJ_ASSERT(line.startsWith("[tenant-entry] ") && line.endsWith("\n"));
  JsonDoc row(kj::str(line.slice(strlen("[tenant-entry] "), line.size() - 1)));
  KJ_EXPECT(text(row.get(), "serverId") == "s2");
  KJ_EXPECT(text(row.get(), "processId") == "p1");
  KJ_EXPECT(field(row.get(), "processGeneration").getNumber() == 3);
  KJ_EXPECT(text(row.get(), "invocationId") == "i7");
  KJ_EXPECT(text(row.get(), "target") == "caller@v1");
  KJ_EXPECT(text(row.get(), "entrypoint") == "default");
  KJ_EXPECT(text(row.get(), "callId") == "probe-7");
}

}  // namespace
}  // namespace workerd
