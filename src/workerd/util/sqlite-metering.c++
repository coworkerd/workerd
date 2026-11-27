// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "sqlite-metering.h"

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

#include <kj/debug.h>

#if defined(__linux__) || defined(__GLIBC__)
#include <malloc.h>
#define SQLITE_METERING_MALLOC_USABLE_SIZE(p) malloc_usable_size(p)
#elifdef __APPLE__
#include <malloc/malloc.h>
#define SQLITE_METERING_MALLOC_USABLE_SIZE(p) malloc_size(p)
#elifdef _WIN32
#include <malloc.h>
#define SQLITE_METERING_MALLOC_USABLE_SIZE(p) _msize(p)
#else
#error "Platform not supported"
#endif

namespace {

inline size_t usableSize(void* ptr) {
  if (ptr == nullptr) return 0;
  return SQLITE_METERING_MALLOC_USABLE_SIZE(ptr);
}

}  // namespace

namespace workerd {

thread_local SqliteMemoryScope* SqliteMemoryScope::threadLocalScope = nullptr;

void* sqliteMemMalloc(int size) {
  if (size <= 0) return nullptr;

  auto* scope = SqliteMemoryScope::threadLocalScope;
  const auto requested = static_cast<size_t>(size);
  if (scope == nullptr) {
    KJ_LOG(ERROR, "sqliteMemMalloc did not find a valid scope.");
    return malloc(requested);
  }

  // Reserve the request before allocating, then reconcile the allocator's usable size.
  if (!scope->resize(0, requested)) return nullptr;
  auto* ptr = malloc(requested);
  if (scope->resize(requested, usableSize(ptr))) return ptr;
  free(ptr);
  scope->resize(requested, 0);
  return nullptr;
}

void sqliteMemFree(void* ptr) {
  if (ptr == nullptr) return;
  auto* scope = SqliteMemoryScope::threadLocalScope;
  auto bytes = usableSize(ptr);
  free(ptr);
  if (scope != nullptr) {
    scope->resize(bytes, 0);
  } else {
    KJ_LOG(ERROR, "sqliteMemFree did not find a valid scope.");
  }
}

void* sqliteMemRealloc(void* ptr, int newSize) {
  if (newSize <= 0) {
    sqliteMemFree(ptr);
    return nullptr;
  }
  if (ptr == nullptr) return sqliteMemMalloc(newSize);

  auto* scope = SqliteMemoryScope::threadLocalScope;
  const auto requested = static_cast<size_t>(newSize);
  if (scope == nullptr) {
    KJ_LOG(ERROR, "sqliteMemRealloc did not find a valid scope.");
    return realloc(ptr, requested);
  }

  const auto oldActual = usableSize(ptr);
  // Modest shrinks can keep the same buffer. It remains charged at its real
  // usable size. Substantial shrinks still return the released capacity.
  if (scope->objectReservation != nullptr && requested <= oldActual && requested > oldActual / 2) {
    return ptr;
  }
  const auto reserved = kj::max(oldActual, requested);
  if (!scope->resize(oldActual, reserved)) return nullptr;
  const bool canRefuse =
      scope->maxMemoryBytes != kj::maxValue ||
      (scope->processBudget != nullptr && !scope->observeProcess);
  auto* newPtr = canRefuse ? malloc(requested) : realloc(ptr, requested);
  if (newPtr == nullptr) {
    scope->resize(reserved, oldActual);
    return nullptr;
  }

  const auto actual = usableSize(newPtr);
  if (canRefuse) {
    // With a limit, the original block remains live until its replacement fits.
    if (actual > reserved && !scope->resize(reserved, actual)) {
      free(newPtr);
      scope->resize(reserved, oldActual);
      return nullptr;
    }
    memcpy(newPtr, ptr, kj::min(oldActual, requested));
    free(ptr);
    if (actual < reserved) scope->resize(reserved, actual);
  } else {
    scope->resize(reserved, actual);
  }
  return newPtr;
}

namespace {

int sqliteMemSize(void* ptr) {
  return static_cast<int>(usableSize(ptr));
}

// sqliteMemRoundup is used by xRoundup as a performance optimization to tell SQLite how the memory
// allocator rounds up allocation requests. This implementation rounds up to the next multiple of 8
// bytes because we assume that most allocators round up to the next 8-bytes. TCMalloc rounds up to
// the next 8 bytes for small size classes.
int sqliteMemRoundup(int n) {
  // Round up to the next multiple of 8 as a conservative estimate.
  return (n + 7) & ~7;
}

int sqliteMemInit(void* /*pAppData*/) {
  return SQLITE_OK;
}

void sqliteMemShutdown(void* /*pAppData*/) {}

static const sqlite3_mem_methods kSqliteMemMethods = {
  sqliteMemMalloc,
  sqliteMemFree,
  sqliteMemRealloc,
  sqliteMemSize,
  sqliteMemRoundup,
  sqliteMemInit,
  sqliteMemShutdown,
  /*pAppData=*/nullptr,
};

}  // namespace

bool SqliteMemoryScope::resize(size_t previous, size_t replacement) noexcept {
  if (previous == replacement) return true;
  if (previous > memoryBytes) {
    KJ_LOG(ERROR,
        replacement == 0 ? "sqliteMemFree would have triggered a memoryBytes underflow."
                         : "sqliteMemRealloc would have triggered a memoryBytes underflow.");
    previous = memoryBytes;
  }
  const auto retained = memoryBytes - previous;
  if (replacement > size_t(kj::maxValue) - retained ||
      (replacement > previous && retained + replacement > maxMemoryBytes)) {
    limitRefusal = LimitRefusal::DATABASE;
    return false;
  }
  const auto next = retained + replacement;
  if (processBudget != nullptr) {
    auto oldCharge = SqliteProcessMemoryBudget::chargeFor(memoryBytes);
    auto newCharge = SqliteProcessMemoryBudget::chargeFor(next);
    if (objectReservation != nullptr) {
      oldCharge = objectReservation->bytes;
      // While busy, retain one step across small drops below a boundary.
      const auto target = ObjectSqlHeapReservation::chargeFor(next);
      newCharge = target != 0 && target < oldCharge &&
              oldCharge - target <= ObjectSqlHeapReservation::STEP_BYTES
          ? oldCharge
          : target;
    }
    if (oldCharge != newCharge &&
        !processBudget->resizeSqliteMemory(oldCharge, newCharge, observeProcess)) {
      limitRefusal = LimitRefusal::PROCESS;
      return false;
    }
    if (objectReservation != nullptr) objectReservation->bytes = newCharge;
  }
  memoryBytes = next;
  return true;
}

void SqliteMemoryScope::clearLimitRefusal() noexcept {
  if (threadLocalScope != nullptr) threadLocalScope->limitRefusal = LimitRefusal::NONE;
}

SqliteMemoryScope::LimitRefusal SqliteMemoryScope::getLimitRefusal() noexcept {
  return threadLocalScope == nullptr ? LimitRefusal::NONE : threadLocalScope->limitRefusal;
}

SqliteMemoryScope::SqliteMemoryScope(size_t& memoryBytes,
    size_t maxMemoryBytes,
    SqliteProcessMemoryBudget* processBudget,
    bool observeProcess,
    ObjectSqlHeapReservation* objectReservation)
    : memoryBytes(memoryBytes),
      maxMemoryBytes(maxMemoryBytes),
      processBudget(processBudget),
      observeProcess(observeProcess),
      objectReservation(objectReservation) {
  if (threadLocalScope == nullptr) {
    threadLocalScope = this;
  }
}

SqliteMemoryScope::~SqliteMemoryScope() noexcept(false) {
  if (threadLocalScope == this) {
    threadLocalScope = nullptr;
  }
}

void installSqliteCustomAllocator() {
  // sqlite3_config() must be called before sqlite3_initialize(), which is itself invoked
  // implicitly by the first sqlite3_vfs_register() or sqlite3_open_v2() call. We rely on callers
  // to invoke this before any of those.
  static bool installed KJ_UNUSED = []() {
    int rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &kSqliteMemMethods);
    KJ_ASSERT(rc == SQLITE_OK, "sqlite3_config(SQLITE_CONFIG_MALLOC) failed", rc);

    // Hack: Explicitly initialize SQLite now with a SqliteMemoryScope to avoid cases that
    // implicitly call sqlite3_initialize() without an active memory scope. sqlite3_initialize() is
    // idempotent per-process (https://sqlite.org/c3ref/initialize.html).
    static size_t initMemoryBytes = 0;
    SqliteMemoryScope scope(initMemoryBytes, kj::maxValue);
    sqlite3_initialize();
    return true;
  }();
}

size_t getSqliteProcessMemoryUsed() {
  return static_cast<size_t>(sqlite3_memory_used());
}

}  // namespace workerd
