#pragma once

#include <kj/common.h>
#include <kj/string.h>
#include <v8-isolate.h>
#include <v8-platform.h>
#include <cstddef>
#include <memory>

namespace workerd::server {

// The V8 sandbox as Coworkerd builds it (--config=coworkerd-sandbox in .bazelrc): one 8 GiB
// sandbox per isolate group, with V8's guard regions. V8 classifies a sandbox as secure when its
// address space is fully reserved and the first 4 GiB of the process's address space and a page
// can never be mapped (WorkerdPlatform reserves them before V8 starts). Without V8_ENABLE_SANDBOX
// nothing here checks or writes anything.

// What V8 reports for one sandbox.
struct SandboxFacts {
  int mode = 0;                  // v8::V8::SandboxMode
  size_t sandboxBytes = 0;       // v8::V8::GetSandboxSizeInBytes()
  size_t addressSpaceBytes = 0;  // the sandbox's address space, its guard regions included
  bool groupPerIsolate = false;  // v8::IsolateGroup::CanCreateNewGroups()
  // Addresses inside the zero segment (1 GiB and 4 GiB + 512 KiB) cannot be mapped: probed, not
  // taken from what the platform reports to V8.
  bool zeroSegmentHeld = true;
  // An isolate's check: the isolate is not in the default group (which the process itself uses).
  bool ownGroup = true;
};

// Why `facts` do not describe a secure sandbox laid out as this build configured it, or none.
kj::Maybe<kj::String> sandboxProblem(const SandboxFacts& facts);

// V8's name-like spelling of a v8::V8::SandboxMode: "secure", "smi-accessible",
// "partially-reserved", "partially-reserved-smi-accessible", or "insecure"; "off" without the
// sandbox.
kj::StringPtr sandboxModeName(int mode);

// Checks the process-wide sandbox once V8 is initialized, on the thread that initialized it, and
// writes one [coworkerd-sandbox] row to stderr (`"mode":"off"` in a build without the sandbox).
// Unless the sandbox is secure and laid out as this build configured it, the row carries the
// problem and the process exits with status SANDBOX_REFUSED_EXIT before it loads any Worker.
void requireSecureSandbox();
constexpr int SANDBOX_REFUSED_EXIT = 78;

// The same for the sandbox of `isolate`, which the caller holds locked and entered, before the
// isolate runs any code; the row names the isolate. Throws instead of exiting, so that only this
// Worker is not created.
void requireSecureIsolateSandbox(v8::Isolate* isolate, kj::StringPtr name);

// A new isolate group for one isolate, or none when the address space has no room left for its
// sandbox and the reservations around it (V8's IsolateGroup::TryCreate(), fork patch
// patches/v8/0057): the caller refuses that isolate instead of losing the process. The group gets
// newSandboxPoolAllocator() as its in-sandbox allocator before any isolate exists. The default
// group in a build with one shared pointer-compression cage.
kj::Maybe<v8::IsolateGroup> tryNewIsolateGroup();

// The in-sandbox allocator of a group's ArrayBuffer backing stores. V8's default reserves its pool
// on the first allocation, trying 8, 4, 2 and 1 GiB, and ends the process when none fits: in an
// 8 GiB sandbox only the second half's 4 GiB hold buffers, and a resizable ArrayBuffer or a Wasm
// memory reserved there before that first allocation can leave less than 1 GiB. This one reserves
// its pool at once (the largest of 2 GiB, 1 GiB, ... 256 MiB that fits in `space`) and returns
// null from Allocate() instead of ending the process when the pool is full. None when no pool fits.
std::shared_ptr<v8::Allocator> newSandboxPoolAllocator(v8::VirtualAddressSpace& space);
constexpr size_t SANDBOX_POOL_BYTES = size_t(2) << 30;

// How many isolate groups this process can still create: creates them until tryNewIsolateGroup()
// refuses, then releases them. V8 places each sandbox at a random address, so the address space
// holds about three quarters of what its size divided by one reservation suggests (random
// sequential packing; 720 on a 47-bit user address space with 136 GiB reservations, cwkd-p40).
// 0 in a build with one shared pointer-compression cage.
size_t measureGroupCapacity(size_t limit = 65536);

// Whether the zero segment is really held: MAP_FIXED_NOREPLACE at 1 GiB and at 4 GiB + 512 KiB
// fails with EEXIST. True where it cannot be probed (not Linux).
bool zeroSegmentHeld();

}  // namespace workerd::server
