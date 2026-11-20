#include "coworkerd-sandbox.h"

#include <kj/debug.h>
#include <v8-initialization.h>
#include <v8-internal.h>
#include <v8-isolate.h>
#include <v8-platform.h>

#include <kj/vector.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_map>
#include <version>

#ifdef V8_ENABLE_SANDBOX
// V8 requires libc++ hardening with its sandbox (its GN build asserts it); --config=coworkerd-sandbox
// sets the fast mode for every target compilation unit.
#ifndef _LIBCPP_HARDENING_MODE
#error "the V8 sandbox build needs libc++ hardening (--config=coworkerd-sandbox)"
#else
static_assert(_LIBCPP_HARDENING_MODE == _LIBCPP_HARDENING_MODE_FAST,
    "the V8 sandbox build needs libc++ hardening in its fast mode (--config=coworkerd-sandbox)");
#endif
#endif

namespace workerd::server {

namespace {

void writeStderr(kj::StringPtr line) {
  size_t done = 0;
  while (done < line.size()) {
    ssize_t written = write(STDERR_FILENO, line.begin() + done, line.size() - done);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) return;
    done += written;
  }
}

// A JSON string: the text quoted, with quotes, backslashes and control characters escaped.
[[maybe_unused]] kj::String jsonString(kj::StringPtr text) {
  static const char HEX[] = "0123456789abcdef";
  kj::Vector<char> out(text.size() + 3);
  out.add('"');
  for (char c: text) {
    if (c == '"' || c == '\\') {
      out.add('\\');
      out.add(c);
    } else if (static_cast<unsigned char>(c) < 0x20) {
      out.addAll(kj::StringPtr("\\u00"));
      out.add(HEX[(c >> 4) & 0xf]);
      out.add(HEX[c & 0xf]);
    } else {
      out.add(c);
    }
  }
  out.add('"');
  out.add('\0');
  return kj::String(out.releaseAsArray());
}

#ifdef V8_ENABLE_SANDBOX
using v8::internal::kAdditionalTrailingGuardRegionSize;
using v8::internal::kRequiresTypedArrayAccessMasks;
using v8::internal::kSandboxGuardRegionSize;
using v8::internal::kSandboxSize;

// The address space V8 reserves for one sandbox in this build: the front guard region, the
// sandbox, the back guard region and any additional trailing guard region (sandbox.cc).
constexpr size_t CONFIGURED_ADDRESS_SPACE =
    kSandboxSize + 2 * kSandboxGuardRegionSize + kAdditionalTrailingGuardRegionSize;

// The sandbox of the calling thread: the process-wide one on the thread that initialized V8, an
// isolate's own while that isolate is entered.
SandboxFacts currentFacts() {
  return {
    .mode = static_cast<int>(v8::V8::GetSandboxMode()),
    .sandboxBytes = v8::V8::GetSandboxSizeInBytes(),
    .addressSpaceBytes = v8::V8::GetSandboxAddressSpace()->size(),
    .groupPerIsolate = v8::IsolateGroup::CanCreateNewGroups(),
    .zeroSegmentHeld = zeroSegmentHeld(),
  };
}

// The refusal, also on the process's event channel when it has one (COWORKERD_EVENT_FD), so that
// the node reports why a process exited with 78 instead of restarting it.
void writeRefusalEvent(kj::StringPtr problem) {
  auto* text = getenv("COWORKERD_EVENT_FD");
  if (text == nullptr) return;
  int fd = atoi(text);
  if (fd < 0) return;
  auto row = kj::str("{\"event\":\"sandbox-refused\",\"problem\":", jsonString(problem),
      ",\"pid\":", getpid(), "}\n");
  if (row.size() > 4096) return;
  ssize_t written;
  do {
    written = write(fd, row.begin(), row.size());
  } while (written < 0 && errno == EINTR);
}

// One [coworkerd-sandbox] row: `subject` names what was checked (a JSON member), then V8's facts,
// this build's layout and the problem, if any.
void writeRow(kj::StringPtr subject, const SandboxFacts& facts, kj::Maybe<kj::StringPtr> problem) {
  auto problemMember = problem.map([](kj::StringPtr text) {
    return kj::str(",\"problem\":", jsonString(text));
  }).orDefault(kj::String());
  writeStderr(kj::str("[coworkerd-sandbox] {", subject, ",\"mode\":\"", sandboxModeName(facts.mode),
      "\",\"sandboxBytes\":", facts.sandboxBytes, ",\"reservationBytes\":", facts.addressSpaceBytes,
      ",\"guardBytes\":", kSandboxGuardRegionSize,
      ",\"trailingGuardBytes\":", kSandboxGuardRegionSize + kAdditionalTrailingGuardRegionSize,
      ",\"typedArrayAccessMasks\":", kRequiresTypedArrayAccessMasks ? "true" : "false",
      ",\"groupPerIsolate\":", facts.groupPerIsolate ? "true" : "false",
      ",\"zeroSegmentHeld\":", facts.zeroSegmentHeld ? "true" : "false", problemMember, "}\n"));
}
#endif

}  // namespace

kj::StringPtr sandboxModeName(int mode) {
#ifdef V8_ENABLE_SANDBOX
  switch (static_cast<v8::V8::SandboxMode>(mode)) {
    case v8::V8::SandboxMode::kSecure:
      return "secure"_kj;
    case v8::V8::SandboxMode::kInsecureFullReservationSmiAccessible:
      return "smi-accessible"_kj;
    case v8::V8::SandboxMode::kInsecurePartialReservationSmiInaccessible:
      return "partially-reserved"_kj;
    case v8::V8::SandboxMode::kInsecurePartialReservationSmiAccessible:
      return "partially-reserved-smi-accessible"_kj;
    default:
      return "insecure"_kj;
  }
#else
  (void)mode;
  return "off"_kj;
#endif
}

kj::Maybe<kj::String> sandboxProblem(const SandboxFacts& facts) {
#ifdef V8_ENABLE_SANDBOX
  if (!facts.groupPerIsolate) {
    return kj::str("this build has one pointer-compression cage, and with it one sandbox, for every "
                   "isolate; build with --config=coworkerd-sandbox");
  }
  if (facts.sandboxBytes != kSandboxSize) {
    return kj::str("V8's sandbox is ", facts.sandboxBytes, " bytes, not the ", kSandboxSize,
        " this build was configured with: part of the build lacks the sandbox defines");
  }
  switch (static_cast<v8::V8::SandboxMode>(facts.mode)) {
    case v8::V8::SandboxMode::kSecure:
      break;
    case v8::V8::SandboxMode::kInsecureFullReservationSmiAccessible:
      return kj::str("the first 4 GiB of the address space are not reserved (something is mapped "
                     "there, or vm.mmap_min_addr is above 64 KiB), so V8's sandbox is not secure "
                     "(smi-accessible)");
    case v8::V8::SandboxMode::kInsecurePartialReservationSmiInaccessible:
    case v8::V8::SandboxMode::kInsecurePartialReservationSmiAccessible:
      return kj::str("V8 could not reserve its sandbox with guard regions, so the sandbox is not "
                     "secure (",
          sandboxModeName(facts.mode), ")");
    default:
      return kj::str("V8 reports its sandbox as insecure (mode ", facts.mode, ")");
  }
  if (!facts.zeroSegmentHeld) {
    return kj::str("the platform reports the first 4 GiB and 1 MiB of the address space as reserved, "
                   "but addresses there can still be mapped, so V8's secure mode does not hold");
  }
  if (facts.addressSpaceBytes != CONFIGURED_ADDRESS_SPACE) {
    return kj::str("V8's sandbox reserves ", facts.addressSpaceBytes,
        " bytes with its guard regions, not the ", CONFIGURED_ADDRESS_SPACE,
        " this build was configured with: part of the build lacks the sandbox defines");
  }
  if (!facts.ownGroup) {
    return kj::str("the isolate is in the default isolate group, whose sandbox the process itself "
                   "uses; every isolate needs a group of its own");
  }
#else
  (void)facts;
#endif
  return kj::none;
}

void requireSecureSandbox() {
#ifdef V8_ENABLE_SANDBOX
  auto facts = currentFacts();
  auto problem = sandboxProblem(facts);
  writeRow("\"process\":true"_kj, facts, problem.map([](kj::String& text) -> kj::StringPtr {
    return text;
  }));
  KJ_IF_SOME(text, problem) {
    writeStderr(kj::str("workerd: refusing to start: ", text, "\n"));
    writeRefusalEvent(text);
    _exit(SANDBOX_REFUSED_EXIT);
  }
#else
  writeStderr(kj::str("[coworkerd-sandbox] {\"process\":true,\"mode\":\"off\",\"groupPerIsolate\":",
      v8::IsolateGroup::CanCreateNewGroups() ? "true" : "false", "}\n"));
#endif
}

namespace {

// A first-fit-by-size allocator over one pool, reserved inside the group's sandbox when the group
// is created. Blocks are multiples of 128 bytes (as V8's default allocator rounds them); freed
// blocks merge with free neighbors, and whole pages that become free go back to the system.
class SandboxPoolAllocator final: public v8::Allocator {
 public:
  SandboxPoolAllocator(v8::VirtualAddressSpace& space, uintptr_t base, size_t size)
      : space(space),
        base(base),
        size(size),
        page(space.page_size()) {
    insertFree(base, size);
  }
  ~SandboxPoolAllocator() override {
    // Destroyed by the sandbox's teardown while its address space is still intact.
    space.FreePages(base, size);
  }

  void* Allocate(size_t length) override {
    auto* data = AllocateUninitialized(length);
    if (data != nullptr) memset(data, 0, length);
    return data;
  }
  void* AllocateUninitialized(size_t length) override {
    const size_t rounded = (std::max<size_t>(length, 1) + GRANULE - 1) / GRANULE * GRANULE;
    if (rounded < length) return nullptr;  // overflow
    std::lock_guard lock(mutex);
    auto fit = bySize.lower_bound(rounded);
    if (fit == bySize.end()) return nullptr;
    auto [blockSize, address] = *fit;
    bySize.erase(fit);
    byAddress.erase(address);
    if (blockSize > rounded) insertFree(address + rounded, blockSize - rounded);
    used.emplace(address, rounded);
    return reinterpret_cast<void*>(address);
  }
  void* AllocateUninitializedOrCrash(size_t length) override {
    auto* data = AllocateUninitialized(length);
    if (data == nullptr) {
      writeStderr(kj::str("[coworkerd-sandbox] {\"event\":\"pool-exhausted\",\"bytes\":", length, "}\n"));
      abort();
    }
    return data;
  }
  void Free(void* data) override {
    if (data == nullptr) return;
    auto address = reinterpret_cast<uintptr_t>(data);
    std::lock_guard lock(mutex);
    auto block = used.find(address);
    if (block == used.end()) {
      writeStderr("[coworkerd-sandbox] {\"event\":\"pool-free-unknown\"}\n"_kj);
      abort();
    }
    size_t length = block->second;
    used.erase(block);
    uintptr_t start = address, end = address + length;
    auto next = byAddress.find(end);
    if (next != byAddress.end()) {
      end += next->second;
      eraseFree(next);
    }
    auto previous = byAddress.lower_bound(start);
    if (previous != byAddress.begin()) {
      --previous;
      if (previous->first + previous->second == start) {
        start = previous->first;
        eraseFree(previous);
      }
    }
    insertFree(start, end - start);
    // The pages that this free made entirely free; the rest of the merged block was released when
    // it became free.
    uintptr_t from = std::max(roundUp(start), roundDown(address));
    uintptr_t to = std::min(roundDown(end), roundUp(address + length));
    if (from < to) (void)space.DiscardSystemPages(from, to - from);
  }

 private:
  static constexpr size_t GRANULE = 128;
  v8::VirtualAddressSpace& space;
  const uintptr_t base;
  const size_t size, page;
  std::mutex mutex;
  std::map<uintptr_t, size_t> byAddress;  // free blocks
  std::multimap<size_t, uintptr_t> bySize;
  std::unordered_map<uintptr_t, size_t> used;

  uintptr_t roundUp(uintptr_t value) const { return (value + page - 1) / page * page; }
  uintptr_t roundDown(uintptr_t value) const { return value / page * page; }
  void insertFree(uintptr_t address, size_t length) {
    byAddress.emplace(address, length);
    bySize.emplace(length, address);
  }
  void eraseFree(std::map<uintptr_t, size_t>::iterator at) {
    auto range = bySize.equal_range(at->second);
    for (auto i = range.first; i != range.second; ++i) {
      if (i->second == at->first) {
        bySize.erase(i);
        break;
      }
    }
    byAddress.erase(at);
  }
};

#ifdef __linux__
bool addressTaken(uintptr_t address) {
  void* got = mmap(reinterpret_cast<void*>(address), 4096, PROT_NONE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_FIXED_NOREPLACE, -1, 0);
  if (got == MAP_FAILED) return errno == EEXIST;
  // Mapped (or, before Linux 4.17, mapped elsewhere): nothing held the address.
  munmap(got, 4096);
  return false;
}
#endif

}  // namespace

std::shared_ptr<v8::Allocator> newSandboxPoolAllocator(v8::VirtualAddressSpace& space) {
  for (size_t size = SANDBOX_POOL_BYTES; size >= (size_t(256) << 20); size /= 2) {
    auto base = space.AllocatePages(v8::VirtualAddressSpace::kNoHint, size, size_t(1) << 20,
        v8::PagePermissions::kReadWrite);
    if (base != 0) return std::make_shared<SandboxPoolAllocator>(space, base, size);
  }
  return nullptr;
}

bool zeroSegmentHeld() {
#ifdef __linux__
  return addressTaken(uintptr_t(1) << 30) && addressTaken((uintptr_t(4) << 30) + (512 << 10));
#else
  return true;
#endif
}

kj::Maybe<v8::IsolateGroup> tryNewIsolateGroup() {
  if (!v8::IsolateGroup::CanCreateNewGroups()) return v8::IsolateGroup::GetDefault();
  auto group = v8::IsolateGroup::TryCreate();
  if (!group.has_value()) return kj::none;
#ifdef V8_ENABLE_SANDBOX
  // Before any isolate exists in the group, so the pool is the first thing in the buffer half.
  auto allocator = newSandboxPoolAllocator(*group->GetSandboxAddressSpace());
  if (allocator == nullptr) return kj::none;
  group->SetInSandboxAllocator(kj::mv(allocator));
#endif
  return kj::mv(*group);
}

void requireSecureIsolateSandbox(v8::Isolate* isolate, kj::StringPtr name) {
#ifdef V8_ENABLE_SANDBOX
  // V8 answers for the calling thread's sandbox, so the isolate must be the one entered here
  // (without any, V8's current sandbox is null).
  KJ_REQUIRE(v8::Isolate::GetCurrent() == isolate, "the isolate is not entered on this thread", name);
  KJ_REQUIRE(isolate->GetGroup().GetSandboxAddressSpace() == v8::V8::GetSandboxAddressSpace(),
      "the isolate's sandbox is not the current one", name);
  auto facts = currentFacts();
  facts.ownGroup = !v8::IsolateGroup::CanCreateNewGroups() ||
      isolate->GetGroup() != v8::IsolateGroup::GetDefault();
  auto problem = sandboxProblem(facts);
  writeRow(kj::str("\"isolate\":", jsonString(name)), facts,
      problem.map([](kj::String& text) -> kj::StringPtr { return text; }));
  KJ_IF_SOME(text, problem) {
    KJ_FAIL_REQUIRE("the V8 sandbox of this isolate is not usable", name, text);
  }
#else
  (void)isolate;
  (void)name;
#endif
}

}  // namespace workerd::server
