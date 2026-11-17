// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include <workerd/api/memory-cache.h>
#include <workerd/io/actor-cache.h>
#include <workerd/io/compatibility-date.h>
#include <workerd/io/io-channels.h>
#include <workerd/io/io-thread-context.h>
#include <workerd/io/limit-enforcer.h>
#include <workerd/io/observer.h>
#include <workerd/io/tracked-wasm-instance.h>
#include <workerd/io/worker-entrypoint.h>
#include <workerd/io/worker-interface.h>
#include <workerd/jsg/setup.h>
#include <workerd/server/workerd-api.h>
#include <workerd/util/autogate.h>
#include <workerd/util/entropy.h>
#include <workerd/util/stream-utils.h>

#include <capnp/message.h>
#include <kj/async-io.h>
#include <kj/compat/http.h>
#include <kj/debug.h>
#include <kj/mutex.h>
#include <kj/thread.h>
#include <kj/vector.h>

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace workerd {
namespace {

uint64_t nowNs(clockid_t clock = CLOCK_MONOTONIC) {
  timespec t;
  KJ_SYSCALL(clock_gettime(clock, &t));
  return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}

struct Entropy final: kj::EntropySource {
  void generate(kj::ArrayPtr<kj::byte> bytes) override { getEntropy(bytes); }
};

struct Timer final: TimerChannel {
  explicit Timer(kj::Timer& timer): timer(timer) {}
  void syncTime() override {}
  kj::Date now(kj::Maybe<kj::Date>) override { return kj::systemPreciseCalendarClock().now(); }
  kj::Promise<void> atTime(kj::Date when) override {
    auto delay = when - now(kj::none);
    return delay > 0 * kj::NANOSECONDS ? timer.afterDelay(delay) : kj::Promise<void>(kj::READY_NOW);
  }
  kj::Promise<void> afterLimitTimeout(kj::Duration delay) override { return timer.afterDelay(delay); }
  kj::Timer& timer;
};

struct Cache final: CacheClient {
  kj::Own<kj::HttpClient> getDefault(SubrequestMetadata) override { KJ_UNIMPLEMENTED("cache"); }
  kj::Own<kj::HttpClient> getNamespace(kj::StringPtr, SubrequestMetadata) override {
    KJ_UNIMPLEMENTED("cache namespace");
  }
};

struct Channels final: IoChannelFactory {
  explicit Channels(TimerChannel& timer): timer(timer) {}
  void abortIsolate(kj::StringPtr) override { KJ_FAIL_REQUIRE("isolate abort"); }
  kj::Own<WorkerInterface> startSubrequest(uint, SubrequestMetadata) override {
    KJ_UNIMPLEMENTED("outbound");
  }
  kj::Rc<SubrequestChannel> getSubrequestChannelResolved(uint, kj::Maybe<Frankenvalue>,
      kj::Maybe<VersionRequest>, Persistent) override { KJ_UNIMPLEMENTED("outbound channel"); }
  kj::Rc<ActorClassChannel> getActorClassResolved(uint, kj::Maybe<Frankenvalue>, Persistent) override {
    KJ_UNIMPLEMENTED("actor class");
  }
  capnp::Capability::Client getCapability(uint) override { KJ_UNIMPLEMENTED("capability"); }
  kj::Own<CacheClient> getCache() override { return kj::heap<Cache>(); }
  TimerChannel& getTimer() override { return timer; }
  kj::Promise<void> writeLogfwdr(uint, kj::FunctionParam<void(capnp::AnyPointer::Builder)>) override {
    KJ_UNIMPLEMENTED("log forwarding");
  }
  kj::Rc<ActorChannel> getGlobalActor(uint, const ActorIdFactory::ActorId&,
      kj::Maybe<kj::String>, ActorGetMode, bool, ActorRoutingMode, SpanParent,
      kj::Maybe<ActorVersion>, Persistent) override { KJ_UNIMPLEMENTED("actor"); }
  kj::Rc<ActorChannel> getColoLocalActor(uint, kj::StringPtr, SpanParent) override {
    KJ_UNIMPLEMENTED("local actor");
  }
  TimerChannel& timer;
};

struct IsolateLimits final: IsolateLimitEnforcer {
  v8::Isolate::CreateParams getCreateParams() override { return {}; }
  void customizeIsolate(v8::Isolate*) override {}
  ActorCacheSharedLruOptions getActorCacheLruOptions() override { return {}; }
  kj::Own<void> enterStartupJs(jsg::Lock&, kj::OneOf<kj::Exception, kj::Duration>&) const override {
    return {};
  }
  kj::Own<void> enterStartupPython(jsg::Lock& js,
      kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    return enterStartupJs(js, result);
  }
  kj::Own<void> enterDynamicImportJs(jsg::Lock& js,
      kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    return enterStartupJs(js, result);
  }
  kj::Own<void> enterLoggingJs(jsg::Lock& js,
      kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    return enterStartupJs(js, result);
  }
  kj::Own<void> enterInspectorJs(jsg::Lock& js,
      kj::OneOf<kj::Exception, kj::Duration>& result) const override {
    return enterStartupJs(js, result);
  }
  void completedRequest(kj::StringPtr) const override {}
  bool exitJs(jsg::Lock&) const override { return false; }
  void reportMetrics(IsolateObserver&) const override {}
  bool hasExcessivelyExceededHeapLimit() const override { return false; }
  const TrackedWasmInstanceList& getTrackedWasmInstances() const override { return instances; }
  TrackedWasmInstanceList instances;
};

struct Reporter final: Worker::ValidationErrorReporter {
  void addError(kj::String error) override { KJ_FAIL_REQUIRE("invalid worker", error); }
  void addEntrypoint(kj::Maybe<kj::StringPtr>, kj::Array<kj::String>) override {}
  void addActorClass(kj::StringPtr) override {}
  void addWorkflowClass(kj::StringPtr, kj::Array<kj::String>) override {}
};

struct Slot {
  capnp::MallocMessageBuilder configArena;
  capnp::MallocMessageBuilder flagsArena;
  api::MemoryCacheProvider cache;
  kj::MutexGuarded<kj::Maybe<kj::Own<Worker>>> worker;
  std::atomic<uint> active{0}, maxActive{0}, creates{0};
};

struct Entry {
  uint64_t startNs, endNs, cpuNs;
  uintptr_t isolate;
};

struct Measurement {
  kj::Vector<Entry> entries;
  uint64_t totalCpu = 0;
};

struct RequestLimits final: LimitEnforcer {
  RequestLimits(Slot& slot, Measurement& measurement): slot(slot), measurement(measurement) {}
  struct Scope {
    Scope(Slot& slot, Measurement& measurement, v8::Isolate* isolate)
        : slot(slot), measurement(measurement), isolate(isolate), start(nowNs()),
          cpu(nowNs(CLOCK_THREAD_CPUTIME_ID)) {
      auto active = slot.active.fetch_add(1) + 1;
      KJ_REQUIRE(active == 1, "overlapping JS entries");
      slot.maxActive.store(active);
    }
    ~Scope() noexcept(false) {
      auto used = nowNs(CLOCK_THREAD_CPUTIME_ID) - cpu;
      measurement.totalCpu += used;
      measurement.entries.add(Entry{start, nowNs(), used, reinterpret_cast<uintptr_t>(isolate)});
      slot.active.fetch_sub(1);
    }
    Slot& slot;
    Measurement& measurement;
    v8::Isolate* isolate;
    uint64_t start, cpu;
  };
  kj::Own<void> enterJs(jsg::Lock& js, IoContext&) override {
    return kj::heap<Scope>(slot, measurement, js.v8Isolate);
  }
  void topUpActor() override {}
  void newSubrequest(bool) override { KJ_UNIMPLEMENTED("subrequest"); }
  void newKvRequest(KvOpType) override { KJ_UNIMPLEMENTED("kv"); }
  void newAnalyticsEngineRequest() override { KJ_UNIMPLEMENTED("analytics"); }
  kj::Promise<void> limitDrain() override { return kj::NEVER_DONE; }
  kj::Promise<void> limitScheduled() override { return kj::NEVER_DONE; }
  kj::Duration getAlarmLimit() override { return 0 * kj::SECONDS; }
  size_t getBufferingLimit() override { return 1 << 20; }
  kj::Maybe<EventOutcome> getLimitsExceeded() override { return kj::none; }
  kj::Promise<void> onLimitsExceeded() override { return kj::NEVER_DONE; }
  void setCpuLimitNearlyExceededCallback(kj::Function<void()>) override {}
  void requireLimitsNotExceeded() override {}
  void reportMetrics(RequestObserver&) override {}
  kj::Duration consumeTimeElapsedForPeriodicLogging() override { return 0 * kj::SECONDS; }
  size_t getSqliteMemoryUsage() const override { return 0; }
  Slot& slot;
  Measurement& measurement;
};

struct Output final: kj::AsyncOutputStream, kj::Refcounted {
  kj::Vector<kj::byte> bytes;
  kj::Promise<void> write(kj::ArrayPtr<const kj::byte> data) override {
    bytes.addAll(data);
    return kj::READY_NOW;
  }
  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const kj::byte>> parts) override {
    for (auto part: parts) bytes.addAll(part);
    return kj::READY_NOW;
  }
  kj::Promise<void> whenWriteDisconnected() override { return kj::NEVER_DONE; }
};

struct Response final: kj::HttpService::Response {
  uint status = 0;
  kj::Own<Output> body = kj::refcounted<Output>();
  kj::Own<kj::AsyncOutputStream> send(uint code, kj::StringPtr,
      const kj::HttpHeaders&, kj::Maybe<uint64_t>) override {
    status = code;
    return kj::addRef(*body);
  }
  kj::Own<kj::WebSocket> acceptWebSocket(const kj::HttpHeaders&) override {
    KJ_UNIMPLEMENTED("websocket");
  }
};

struct Errors final: kj::TaskSet::ErrorHandler {
  void taskFailed(kj::Exception&& error) override { KJ_FAIL_REQUIRE("background task", error); }
};

struct ThreadState {
  kj::AsyncIoContext io = kj::setupAsyncIo();
  Entropy entropy;
  capnp::ByteStreamFactory bytes;
  kj::HttpHeaderTable::Builder headerBuilder;
  ThreadContext::HeaderIdBundle ids{headerBuilder};
  capnp::HttpOverCapnpFactory http{bytes, capnp::HttpOverCapnpFactory::HeaderIdBundle(headerBuilder),
    capnp::HttpOverCapnpFactory::LEVEL_2};
  kj::Own<kj::HttpHeaderTable> headers = headerBuilder.build();
  ThreadContext context{io.provider->getTimer(), entropy, ids, http, bytes};
  Timer timer{io.provider->getTimer()};
  Errors errors;
  kj::TaskSet tasks{errors};
};

static constexpr kj::StringPtr script = R"JS(
const id = '__ID__';
function burn(n) {
  let x = 1;
  while (n > 0) {
    const block = Math.min(n, 1000000);
    for (let i = 0; i < block; ++i) x = Math.imul(x ^ i, 1664525) + 1013904223 | 0;
    n -= block;
  }
  globalThis.sink = x;
  return x;
}
export default { async fetch(request) {
  const u = new URL(request.url);
  const n = Number(u.searchParams.get('n') || 0);
  const value = burn(n);
  const delay = Number(u.searchParams.get('wait') || 0);
  if (delay) await new Promise(resolve => setTimeout(resolve, delay));
  return Response.json({worker: id, value, n, token: u.searchParams.get('token')});
}};
)JS"_kj;

struct Server {
  enum class SourceKind { BENCHMARK, MINIMAL };
  explicit Server(uint count, SourceKind sourceKind = SourceKind::BENCHMARK)
      : slots(kj::heapArray<Slot>(count)), sourceKind(sourceKind) {}
  jsg::V8System v8;
  kj::Array<Slot> slots;
  SourceKind sourceKind;
  kj::MutexGuarded<kj::Vector<int>> pending;
  kj::Vector<kj::Own<kj::Thread>> threads;
  std::atomic<uint> live{0}, busy{0}, maxBusy{0};
  std::atomic<uint64_t> sequence{0};
  kj::MutexGuarded<uint> logMutex;

  kj::Own<Worker> getWorker(uint index) {
    auto& slot = slots[index];
    auto cell = slot.worker.lockExclusive();
    KJ_IF_SOME(worker, *cell) { return kj::atomicAddRef(*worker); }
    auto name = kj::str("w", index);
    kj::StringPtr sourceTemplate = sourceKind == SourceKind::MINIMAL
        ? "const id='__ID__'; export default {fetch(){return new Response(id)}};"_kj
        : script;
    auto marker = KJ_REQUIRE_NONNULL(sourceTemplate.findFirst('_'));
    auto source = kj::str(sourceTemplate.slice(0, marker), name,
        sourceTemplate.slice(marker + strlen("__ID__")));
    auto config = slot.configArena.initRoot<server::config::Worker>();
    auto modules = config.initModules(1);
    modules[0].setName("main");
    modules[0].setEsModule(source);
    Reporter reporter;
    auto flags = slot.flagsArena.initRoot<CompatibilityFlags>();
    compileCompatibilityFlags("2026-11-01", capnp::List<capnp::Text>::Reader(), flags,
        reporter, false, CompatibilityDateValidation::CODE_VERSION, {});
    auto limits = kj::refcounted<IsolateLimits>();
    auto group = jsg::newIsolateGroup();
    const api::pyodide::PythonConfig python{
      .packageDiskCacheRoot = kj::none, .createSnapshot = false, .createBaselineSnapshot = false};
    auto api = kj::heap<server::WorkerdApi>(v8, flags.asReader(),
        capnp::List<server::config::Extension>::Reader(), limits->getCreateParams(), group,
        kj::atomicRefcounted<JsgIsolateObserver>(), slot.cache, python);
    auto isolate = kj::atomicRefcounted<Worker::Isolate>(kj::mv(api),
        kj::atomicRefcounted<IsolateObserver>(), name, kj::mv(limits),
        Worker::Isolate::InspectorPolicy::DISALLOW);
    auto compiled = kj::atomicRefcounted<Worker::Script>(kj::mv(isolate), name,
        server::WorkerdApi::extractSource("main", config, flags.asReader(), reporter),
        IsolateObserver::StartType::COLD, kj::none, kj::none, SpanParent(nullptr),
        newWorkerFileSystem(kj::heap<FsMap>(), getTmpDirectoryImpl()), kj::none);
    auto worker = kj::atomicRefcounted<Worker>(kj::mv(compiled),
        kj::atomicRefcounted<WorkerObserver>(),
        [](jsg::Lock&, const Worker::Api&, v8::Local<v8::Object>, v8::Local<v8::Object>) {},
        IsolateObserver::StartType::COLD, SpanParent(nullptr),
        Worker::Lock::TakeSynchronously(kj::none), reporter);
    worker->runInLockScope(Worker::Lock::TakeSynchronously(kj::none),
        [&](Worker::Lock& lock) { lock.validateHandlers(reporter); });
    slot.creates.fetch_add(1);
    *cell = kj::atomicAddRef(*worker);
    return worker;
  }

  void enqueue(int fd) {
    auto queue = pending.lockExclusive();
    queue->add(fd);
    if (queue->size() + busy.load() > live.load()) {
      live.fetch_add(1);
      threads.add(kj::heap<kj::Thread>([this]() { threadMain(); }));
    }
  }

  void threadMain() {
    ThreadState state;
    for (;;) {
      int fd;
      {
        auto queue = pending.lockExclusive();
        queue.wait([](auto& q) { return !q.empty(); });
        fd = queue->back();
        queue->removeLast();
        busy.fetch_add(1);
      }
      KJ_DEFER(busy.fetch_sub(1));
      KJ_DEFER(close(fd));
      auto error = kj::runCatchingExceptions([&]() { serve(fd, state); });
      KJ_IF_SOME(e, error) {
        auto row = kj::str("{\"type\":\"request-error\",\"tid\":", syscall(SYS_gettid),
            ",\"description\":\"request failed; see diagnostic\"}\n");
        auto lock = logMutex.lockExclusive();
        fwrite(row.cStr(), 1, row.size(), stderr);
        KJ_LOG(ERROR, e);
        fflush(stderr);
      }
    }
  }

  void serve(int fd, ThreadState& state) {
    auto started = nowNs();
    kj::Vector<char> request;
    while (request.size() < 8192) {
      char part[2048];
      ssize_t n;
      KJ_SYSCALL(n = read(fd, part, sizeof(part)));
      KJ_REQUIRE(n > 0, "closed before request headers");
      request.addAll(kj::arrayPtr(part, n));
      request.add('\0');
      bool complete = strstr(request.begin(), "\r\n\r\n") != nullptr;
      request.removeLast();
      if (complete) break;
    }
    request.add('\0');
    auto line = kj::StringPtr(request.begin());
    KJ_REQUIRE(line.startsWith("GET "), "only GET supported");
    auto rest = line.slice(4);
    auto end = KJ_REQUIRE_NONNULL(rest.findFirst(' '));
    auto path = kj::str(rest.slice(0, end));
    if (path == "/ready") {
      send(fd, 200, "{\"ready\":true}"_kj.asArray(), 0, 0);
      return;
    }
    KJ_REQUIRE(path.startsWith("/w/"), "expected /w/index");
    char* after;
    auto index = strtoul(path.cStr() + 3, &after, 10);
    KJ_REQUIRE(index < slots.size() && (*after == '?' || *after == '\0'), "worker index");
    auto worker = getWorker(index);
    Measurement measurement;
    Response response;
    {
      auto entry = newWorkerEntrypoint(state.context, kj::mv(worker), kj::none, Frankenvalue(),
          kj::none, kj::heap<RequestLimits>(slots[index], measurement), kj::Own<void>(),
          kj::rc<Channels>(state.timer), kj::refcounted<RequestObserver>(), state.tasks, false,
          kj::none, kj::none, kj::none);
      kj::HttpHeaders headers(*state.headers);
      auto body = newMemoryInputStream(""_kj);
      entry->request(kj::HttpMethod::GET, kj::str("http://fixture", path), headers, *body, response)
          .wait(state.io.waitScope);
    }
    state.tasks.onEmpty().wait(state.io.waitScope);
    send(fd, response.status, response.body->bytes.asPtr().asChars(), measurement.totalCpu,
        syscall(SYS_gettid));
    auto lock = logMutex.lockExclusive();
    fprintf(stderr, "{\"type\":\"request\",\"sequence\":%llu,\"worker\":%lu,\"tid\":%ld,"
                    "\"startNs\":%llu,\"endNs\":%llu,\"cpuNs\":%llu,\"status\":%u,"
                    "\"creates\":%u,\"entries\":[",
        (unsigned long long)sequence.fetch_add(1), index, syscall(SYS_gettid),
        (unsigned long long)started, (unsigned long long)nowNs(),
        (unsigned long long)measurement.totalCpu, response.status, slots[index].creates.load());
    bool first = true;
    for (auto& e: measurement.entries) {
      fprintf(stderr, "%s{\"isolate\":%llu,\"startNs\":%llu,\"endNs\":%llu,\"cpuNs\":%llu}",
          first ? "" : ",", (unsigned long long)e.isolate, (unsigned long long)e.startNs,
          (unsigned long long)e.endNs, (unsigned long long)e.cpuNs);
      first = false;
    }
    fputs("]}\n", stderr);
    fflush(stderr);
  }

  static void send(int fd, uint status, kj::ArrayPtr<const char> body, uint64_t cpu, long tid) {
    auto head = kj::str("HTTP/1.1 ", status, " Result\r\nConnection: close\r\nContent-Type: application/json"
        "\r\nContent-Length: ", body.size(), "\r\nX-Thread-Cpu-Ns: ", cpu,
        "\r\nX-Thread-Id: ", tid, "\r\n\r\n");
    kj::ArrayPtr<const char> parts[] = {head.asArray(), body};
    for (auto part: parts) {
      while (part.size() != 0) {
        ssize_t n;
        KJ_SYSCALL(n = ::send(fd, part.begin(), part.size(), MSG_NOSIGNAL));
        KJ_REQUIRE(n > 0, "response write");
        part = part.slice(n);
      }
    }
  }
};

void lockBench(uint turns) {
  ThreadState state;
  Server server(1);
  auto worker = server.getWorker(0);
  auto before = nowNs();
  uint64_t isolate = 0;
  for (uint i = 0; i < turns * 2; ++i) {
    worker->runInLockScope(Worker::Lock::TakeSynchronously(kj::none), [&](Worker::Lock& lock) {
      isolate = reinterpret_cast<uintptr_t>(lock.getIsolate());
    });
  }
  auto sameThread = nowNs() - before;
  kj::MutexGuarded<uint> turn(0);
  uint64_t lockNs[2] = {}, cpuNs[2] = {}, tids[2] = {}, isolates[2] = {};
  before = nowNs();
  {
    auto run = [&](uint index) {
      ThreadState ownState;
      auto ownWorker = kj::atomicAddRef(*worker);
      tids[index] = syscall(SYS_gettid);
      auto cpuStart = nowNs(CLOCK_THREAD_CPUTIME_ID);
      for (uint i = 0; i < turns; ++i) {
        auto current = turn.lockExclusive();
        current.wait([index](uint value) { return value == index; });
        auto start = nowNs();
        ownWorker->runInLockScope(Worker::Lock::TakeSynchronously(kj::none),
            [&](Worker::Lock& lock) {
          isolates[index] = reinterpret_cast<uintptr_t>(lock.getIsolate());
        });
        lockNs[index] += nowNs() - start;
        *current = 1 - index;
      }
      cpuNs[index] = nowNs(CLOCK_THREAD_CPUTIME_ID) - cpuStart;
    };
    kj::Thread a([&]() { run(0); });
    kj::Thread b([&]() { run(1); });
  }
  auto crossThread = nowNs() - before;
  printf("{\"turnsPerThread\":%u,\"isolate\":%llu,\"sameThreadNs\":%llu,"
         "\"crossThreadNs\":%llu,\"lockNs\":[%llu,%llu],\"cpuNs\":[%llu,%llu],"
         "\"tids\":[%llu,%llu],\"isolates\":[%llu,%llu]}\n", turns,
      (unsigned long long)isolate, (unsigned long long)sameThread, (unsigned long long)crossThread,
      (unsigned long long)lockNs[0], (unsigned long long)lockNs[1],
      (unsigned long long)cpuNs[0], (unsigned long long)cpuNs[1],
      (unsigned long long)tids[0], (unsigned long long)tids[1],
      (unsigned long long)isolates[0], (unsigned long long)isolates[1]);
}

void capacity(uint maximum) {
  ThreadState state;
  Server server(maximum, Server::SourceKind::MINIMAL);
  auto defaultGroup = v8::IsolateGroup::GetDefault();
  kj::Maybe<v8::IsolateGroup> previous;
  auto started = nowNs();
  KJ_REQUIRE(v8::IsolateGroup::CanCreateNewGroups(), "capacity requires multiple cages");
  printf("{\"type\":\"capacity-start\",\"pid\":%d,\"canCreateNewGroups\":true,\"maximum\":%u}\n",
      getpid(), maximum);
  fflush(stdout);
  for (uint index = 0; index < maximum; ++index) {
    uintptr_t address = 0;
    auto failure = kj::runCatchingExceptions([&]() {
      auto worker = server.getWorker(index);
      worker->runInLockScope(Worker::Lock::TakeSynchronously(kj::none), [&](Worker::Lock& lock) {
        auto group = lock.getIsolate()->GetGroup();
        KJ_REQUIRE(group != defaultGroup, "worker retained default group");
        KJ_IF_SOME(old, previous) {
          KJ_REQUIRE(group != old, "workers share an isolate group");
        }
        previous = kj::mv(group);
        address = reinterpret_cast<uintptr_t>(lock.getIsolate());
      });
    });
    KJ_IF_SOME(error, failure) {
      KJ_LOG(ERROR, "worker creation rejected", index, error);
      auto existing = server.getWorker(0);
      existing->runInLockScope(Worker::Lock::TakeSynchronously(kj::none), [](Worker::Lock&) {});
      printf("{\"type\":\"capacity-create-failed\",\"successfulWorkers\":%u,"
             "\"recoverableCaught\":true,\"survivedExistingWorkerLock\":true}\n", index);
      fflush(stdout);
      return;
    }
    printf("{\"type\":\"capacity-created\",\"successfulWorkers\":%u,\"isolate\":%llu,"
           "\"differentFromDefaultAndPrevious\":true,\"elapsedNs\":%llu}\n",
        index + 1, (unsigned long long)address, (unsigned long long)(nowNs() - started));
    fflush(stdout);
  }
  printf("{\"type\":\"capacity-bound-reached\",\"successfulWorkers\":%u}\n", maximum);
  fflush(stdout);
}

}  // namespace
}  // namespace workerd

int main(int argc, char** argv) {
  using namespace workerd;
  KJ_REQUIRE(argc == 3, "usage: multi-thread-experiment PORT WORKERS");
  util::Autogate::initAutogateNamesForTest({}, util::IgnoreAllAutogatesEnv(true));
  if (kj::StringPtr(argv[1]) == "lockbench") {
    lockBench(strtoul(argv[2], nullptr, 10));
    return 0;
  }
  if (kj::StringPtr(argv[1]) == "capacity") {
    capacity(strtoul(argv[2], nullptr, 10));
    return 0;
  }
  Server server(strtoul(argv[2], nullptr, 10));
  auto fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  KJ_REQUIRE(fd >= 0);
  int one = 1;
  KJ_SYSCALL(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(strtoul(argv[1], nullptr, 10));
  KJ_SYSCALL(bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)));
  KJ_SYSCALL(listen(fd, 2048));
  fprintf(stderr, "{\"type\":\"listening\",\"pid\":%d,\"port\":%u,\"workers\":%s}\n",
      getpid(), ntohs(address.sin_port), argv[2]);
  for (;;) {
    int accepted;
    KJ_SYSCALL(accepted = accept4(fd, nullptr, nullptr, SOCK_CLOEXEC));
    KJ_SYSCALL(setsockopt(accepted, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)));
    server.enqueue(accepted);
  }
}
