#include "coworkerd-memory-limit.h"

#include <kj/debug.h>
#include <v8-statistics.h>
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace workerd::server {
namespace {
thread_local MemoryExecution executingMemory;
char unknownOomReport[256] = {};
std::atomic<bool> oomReportReady{false};
std::atomic<void (*)()> limitStateObserver{nullptr};

void raiseLimitState(const IsolateLimitState& state) {
  auto expected = IsolateLimitCause::NONE;
  if (state->compare_exchange_strong(expected, IsolateLimitCause::MEMORY)) {
    if (auto observer = limitStateObserver.load()) observer();
  }
}

void raisePeak(std::atomic<size_t>& peak, size_t value) {
  auto old = peak.load();
  while (old < value && !peak.compare_exchange_weak(old, value)) {}
}

}  // namespace

int coworkerdPid() {
  static const int pid = getpid();
  return pid;
}
int cpuEventsDescriptor() {
  static const int descriptor = []() {
    const char* text = std::getenv("COWORKERD_EVENT_FD");
    if (text == nullptr) return -1;
    KJ_REQUIRE(*text != '\0', "COWORKERD_EVENT_FD must be a descriptor number");
    int value = 0;
    for (const char* p = text; *p; ++p) {
      KJ_REQUIRE(*p >= '0' && *p <= '9' && value < 1000000, "COWORKERD_EVENT_FD must be a descriptor number");
      value = value * 10 + (*p - '0');
    }
    KJ_REQUIRE(value > STDERR_FILENO && fcntl(value, F_GETFD) >= 0, "COWORKERD_EVENT_FD is not an open descriptor");
    // Kept from any process this one might start.
    fcntl(value, F_SETFD, FD_CLOEXEC);
    return value;
  }();
  return descriptor;
}
void writeLimitEvent(kj::StringPtr row) {
  auto fd = cpuEventsDescriptor();
  // A row of at most PIPE_BUF bytes is written whole by this one call or not at all.
  for (size_t done = 0; fd >= 0 && done < row.size();) {
    auto n = write(fd, row.cStr() + done, row.size() - done);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) break;
    done += n;
  }
}

uint32_t memoryLimitFromEnvironment() {
  auto* text = std::getenv("COWORKERD_MEMORY_LIMIT_MIB");
  if (text == nullptr) return 0;
  uint64_t value = 0;
  KJ_REQUIRE(*text, "memory limit must be an unsigned 32-bit integer");
  for (auto* p = text; *p; ++p) {
    KJ_REQUIRE(*p >= '0' && *p <= '9', "memory limit must be an unsigned 32-bit integer");
    value = value * 10 + (*p - '0');
    KJ_REQUIRE(value <= UINT32_MAX, "memory limit exceeds uint32");
  }
  return value;
}
void setLimitStateObserver(void (*observer)()) { limitStateObserver.store(observer); }

IsolateMemoryLimit::IsolateMemoryLimit(uint32_t limitMiB, IsolateLimitState state)
    : limitBytes(uint64_t(limitMiB) << 20), state(kj::mv(state)), inner(v8::ArrayBuffer::Allocator::NewDefaultAllocator()) {
  // Everything the fatal path needs is prepared here: the descriptor and the bare OOM row.
  if (cpuEventsDescriptor() < 0) return;
  if (!oomReportReady.load(std::memory_order_acquire)) {
    snprintf(unknownOomReport, sizeof(unknownOomReport), "{\"event\":\"memory-oom-undetermined\",\"pid\":%d}\n", coworkerdPid());
    oomReportReady.store(true, std::memory_order_release);
  }
}
IsolateMemoryLimit::~IsolateMemoryLimit() noexcept { delete inner; }

bool IsolateMemoryLimit::charge(size_t bytes) {
  auto used = ownedBytes.load();
  for (;;) {
    auto base = heapBytes.load() + otherBytes.load();
    if (bytes > limitBytes || base > limitBytes - bytes || used > limitBytes - bytes - base) {
      raisePeak(deniedBytes, bytes);
      raiseLimitState(state);
      return false;
    }
    if (ownedBytes.compare_exchange_weak(used, used + bytes)) {
      raisePeak(peakOwnedBytes, used + bytes);
      raisePeak(peakUsedBytes, base + used + bytes);
      return true;
    }
  }
}
void IsolateMemoryLimit::release(size_t bytes) { ownedBytes.fetch_sub(bytes); }
void* IsolateMemoryLimit::allocate(size_t length, Initialization initialization) {
  if (!charge(length)) return nullptr;
  auto* result = initialization == Initialization::ZERO ? inner->Allocate(length) : inner->AllocateUninitialized(length);
  if (!result) release(length);
  else bufferBytes.fetch_add(length);
  return result;
}
void* IsolateMemoryLimit::Allocate(size_t length) { return allocate(length, Initialization::ZERO); }
void* IsolateMemoryLimit::AllocateUninitialized(size_t length) { return allocate(length, Initialization::UNINITIALIZED); }
void IsolateMemoryLimit::Free(void* address, size_t length) { inner->Free(address, length); bufferBytes.fetch_sub(length); release(length); }

void IsolateMemoryLimit::customize(v8::Isolate* value) {
  isolate = value;
  value->AddNearHeapLimitCallback(&nearHeap, this);
  value->AddGCEpilogueCallback(&gc, this);
  value->SetOOMErrorHandler(&oom);
}
MemoryExecution IsolateMemoryLimit::enter(kj::StringPtr report) {
  auto previous = executingMemory;
  executingMemory = {this, report};
  observe();
  return previous;
}
void IsolateMemoryLimit::leave(MemoryExecution previous) { observe(); executingMemory = previous; }
void IsolateMemoryLimit::observe() {
  v8::HeapStatistics stats;
  isolate->GetHeapStatistics(&stats);
  auto owned = ownedBytes.load();
  auto other = stats.external_memory() > owned ? stats.external_memory() - owned : 0;
  heapBytes.store(stats.used_heap_size());
  otherBytes.store(other);
  auto total = stats.used_heap_size() + other + owned;
  raisePeak(peakUsedBytes, total);
  if (total >= limitBytes) terminate();
}
void IsolateMemoryLimit::terminate() {
  raiseLimitState(state);
  isolate->TerminateExecution();
}
size_t IsolateMemoryLimit::nearHeap(void* data, size_t current, size_t initial) {
  auto& self = *static_cast<IsolateMemoryLimit*>(data);
  ++self.nearCalls;
  self.observe();
  self.terminate();
  auto ceiling = initial + HEADROOM;
  if (current < ceiling) self.grantedHeadroomBytes.store(ceiling - initial);
  self.logSample("near-heap");
  return std::max(current, ceiling);
}
void IsolateMemoryLimit::gc(v8::Isolate*, v8::GCType, v8::GCCallbackFlags, void* data) {
  auto& self = *static_cast<IsolateMemoryLimit*>(data);
  self.observe();
  if (self.state->load() == IsolateLimitCause::MEMORY) self.logSample("gc-limit");
}
void IsolateMemoryLimit::logSample(const char* event) const {
  char row[2048];
  auto length = snprintf(row, sizeof(row),
      "[memory-limit-sample] {\"event\":\"%s\",\"pid\":%d,\"memoryLimitMiB\":%zu,\"heapUsedBytes\":%zu,"
      "\"externalOwnedBytes\":%zu,\"arrayBufferBytes\":%zu,\"peakUsedBytes\":%zu,"
      "\"headroomBytes\":%zu,\"nearHeapCalls\":%zu}\n", event, coworkerdPid(), limitBytes >> 20,
      heapBytes.load(), ownedBytes.load(), bufferBytes.load(), peakUsedBytes.load(),
      grantedHeadroomBytes.load(), nearCalls.load());
  if (length > 0 && static_cast<size_t>(length) < sizeof(row)) {
    ssize_t written;
    do { written = write(STDERR_FILENO, row, length); } while (written < 0 && errno == EINTR);
  }
}
void IsolateMemoryLimit::oom(const char*, const v8::OOMDetails&) {
  // Only writes of rows prepared before entering JS, on the descriptor inherited at startup: first
  // the bare OOM row, which tells an OOM apart even if nothing else is known, then the identity of
  // the execution that hit it.
  if (!oomReportReady.load(std::memory_order_acquire)) return;
  writeLimitEvent(unknownOomReport);
  if (executingMemory.owner != nullptr && executingMemory.report.size()) writeLimitEvent(executingMemory.report);
  // V8 invokes the original process-wide jsg OOM callback after this returns.
}
kj::String IsolateMemoryLimit::metrics() const {
  return kj::str(",\"memoryLimitMiB\":", limitBytes >> 20, ",\"heapUsedBytes\":", heapBytes.load(),
      ",\"externalOwnedBytes\":", ownedBytes.load(), ",\"arrayBufferBytes\":", bufferBytes.load(),
      ",\"peakUsedBytes\":", peakUsedBytes.load(),
      ",\"deniedAllocationBytes\":", deniedBytes.load(), ",\"headroomBytes\":", grantedHeadroomBytes.load(),
      ",\"nearHeapCalls\":", nearCalls.load());
}
}  // namespace workerd::server
