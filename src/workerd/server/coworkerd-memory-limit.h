#pragma once

#include <kj/memory.h>
#include <kj/string.h>
#include <v8-array-buffer.h>
#include <v8-isolate.h>
#include <atomic>
#include <memory>

namespace workerd::server {

enum class IsolateLimitCause { NONE, CPU, MEMORY };
using IsolateLimitState = std::shared_ptr<std::atomic<IsolateLimitCause>>;
uint32_t memoryLimitFromEnvironment();
// This process's pid, read once: event rows carry it on every request.
int coworkerdPid();
// The write end of the Server's limit event pipe, inherited at startup and named by
// COWORKERD_EVENT_FD; -1 without one. Read once per process.
int cpuEventsDescriptor();
// Writes one event row (ending in a newline) to the event pipe in one unbuffered write(): the row
// is in the pipe when this returns, so a later SIGKILL cannot lose it, and a row of at most
// PIPE_BUF bytes never interleaves with another thread's row.
void writeLimitEvent(kj::StringPtr row);
// Called after a memory limit moves a limit state from NONE, so that the limit watchdog
// terminates the isolate without waiting for a CPU deadline. Set by the watchdog; may be null.
void setLimitStateObserver(void (*observer)());

struct MemoryExecution {
  class IsolateMemoryLimit* owner = nullptr;
  kj::StringPtr report;
};

// V8 retains shared allocator ownership until the last backing store is freed.
class IsolateMemoryLimit final: public v8::ArrayBuffer::Allocator, public std::enable_shared_from_this<IsolateMemoryLimit> {
 public:
  static constexpr size_t HEADROOM = 8ull << 20;
  IsolateMemoryLimit(uint32_t limitMiB, IsolateLimitState state);
  ~IsolateMemoryLimit() noexcept override;
  void* Allocate(size_t length) override;
  void* AllocateUninitialized(size_t length) override;
  void Free(void* address, size_t length) override;
  void customize(v8::Isolate* isolate);
  MemoryExecution enter(kj::StringPtr report);
  void leave(MemoryExecution previous);
  void observe();
  bool charge(size_t bytes);
  void release(size_t bytes);
  kj::String metrics() const;
  const size_t limitBytes;
  std::atomic<size_t> ownedBytes{0}, bufferBytes{0}, peakOwnedBytes{0};
  std::atomic<size_t> heapBytes{0}, otherBytes{0}, peakUsedBytes{0}, deniedBytes{0};
  std::atomic<size_t> nearCalls{0}, grantedHeadroomBytes{0};

 private:
  enum class Initialization { ZERO, UNINITIALIZED };
  void* allocate(size_t length, Initialization initialization);
  void terminate();
  void logSample(const char* event) const;
  static size_t nearHeap(void* data, size_t current, size_t initial);
  static void gc(v8::Isolate*, v8::GCType, v8::GCCallbackFlags, void* data);
  static void oom(const char*, const v8::OOMDetails&);
  IsolateLimitState state;
  v8::ArrayBuffer::Allocator* inner;
  v8::Isolate* isolate = nullptr;
};

}  // namespace workerd::server
