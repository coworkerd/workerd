#pragma once

#include "coworkerd-channels.h"
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
// Called after a memory limit moves a limit state from NONE, so that the limit watchdog
// terminates the isolate without waiting for a CPU deadline. Set by the watchdog; may be null.
void setLimitStateObserver(void (*observer)());

struct MemoryExecution {
  class IsolateMemoryLimit* owner = nullptr;
  kj::StringPtr report;
};

// V8 retains shared allocator ownership until the last backing store is freed.
// `group` is the isolate's group: with the V8 sandbox and one sandbox per group, backing stores
// must lie in that group's sandbox (V8 checks it), so the allocator allocates there and keeps the
// group, and with it the sandbox, alive.
class IsolateMemoryLimit final: public v8::ArrayBuffer::Allocator, public std::enable_shared_from_this<IsolateMemoryLimit> {
 public:
  static constexpr size_t HEADROOM = 8ull << 20;
  IsolateMemoryLimit(uint32_t limitMiB, IsolateLimitState state, const v8::IsolateGroup& group);
  ~IsolateMemoryLimit() noexcept override;
  void* Allocate(size_t length) override;
  void* AllocateUninitialized(size_t length) override;
  void Free(void* address, size_t length) override;
  // No single buffer, and no resizable buffer's reservation, beyond the limit: V8 refuses those
  // before reserving anything (otherwise its default is 32 GiB, and a resizable ArrayBuffer could
  // reserve the whole buffer half of an 8 GiB sandbox).
  size_t MaxAllocationSize() const override;
  v8::PageAllocator* GetPageAllocator() override;
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
  v8::IsolateGroup group;
  v8::ArrayBuffer::Allocator* inner;
  v8::Isolate* isolate = nullptr;
};

}  // namespace workerd::server
