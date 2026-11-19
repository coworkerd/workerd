// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "v8-platform-impl.h"

#include <workerd/io/io-util.h>

#if defined(V8_ENABLE_SANDBOX) && defined(__linux__)
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#elif defined(V8_ENABLE_SANDBOX) && defined(__APPLE__)
#include <mach-o/dyld.h>
#include <mach-o/loader.h>
#include <mach/mach.h>
#include <mach/mach_vm.h>

#include <cstdint>
#include <cstring>
#endif

namespace workerd::server {

namespace {

#if defined(V8_ENABLE_SANDBOX) && (defined(__linux__) || defined(__APPLE__))
// The range V8 needs unmappable: the 4 GiB a Smi can address, plus padding for the fields of an
// object at such an address. V8 15.8 asks for 4 GiB plus the largest JS or Wasm object field offset
// (kMinZeroSegmentSize, src/sandbox/sandbox-zero-segment.h; a Wasm struct of 10,000 fields makes it
// about 160 KB) and asserts that this stays within 4097 MiB, so 4 GiB + 1 MiB always suffices.
constexpr uintptr_t kZeroSegmentEnd = (uintptr_t(4) << 30) + (uintptr_t(1) << 20);
#endif

size_t reserveZeroSegment() {
#if defined(V8_ENABLE_SANDBOX) && defined(__linux__)
  // Nothing can be mapped below vm.mmap_min_addr (mmap fails with EPERM, or EACCES under an LSM).
  // Reserve the rest of [0, kZeroSegmentEnd), inaccessible and without commit, for the life of the
  // process. Its start is found by trying page by page up to 64 KiB, the largest usual
  // vm.mmap_min_addr, so that this needs no /proc (a confined process may have none). If anything
  // already occupies part of the range, nothing is reserved and V8 reports the sandbox as insecure
  // (smi-accessible).
  uintptr_t page = sysconf(_SC_PAGESIZE);
  for (uintptr_t start = page; start <= 65536 && start < kZeroSegmentEnd; start += page) {
    void* reserved = mmap(reinterpret_cast<void*>(start), kZeroSegmentEnd - start, PROT_NONE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_FIXED_NOREPLACE, -1, 0);
    if (reserved == reinterpret_cast<void*>(start)) return kZeroSegmentEnd;
    // Kernels before 4.17 take MAP_FIXED_NOREPLACE as a hint and may map elsewhere.
    if (reserved != MAP_FAILED) {
      munmap(reserved, kZeroSegmentEnd - start);
      return 0;
    }
    if (errno != EPERM && errno != EACCES) return 0;
  }
  return 0;
#elif defined(V8_ENABLE_SANDBOX) && defined(__APPLE__)
  // For a PIE executable the kernel extends __PAGEZERO by the ASLR slide and puts the whole range
  // below the VM map's minimum address, where nothing can ever be mapped; there is nothing to
  // reserve. Confirm it for the page below the end before reporting it.
  auto header = reinterpret_cast<const mach_header_64*>(_dyld_get_image_header(0));
  if (header == nullptr || header->magic != MH_MAGIC_64) return 0;
  auto command = reinterpret_cast<const load_command*>(header + 1);
  for (uint32_t i = 0; i < header->ncmds; i++) {
    if (command->cmd == LC_SEGMENT_64) {
      auto segment = reinterpret_cast<const segment_command_64*>(command);
      if (strncmp(segment->segname, SEG_PAGEZERO, sizeof(segment->segname)) == 0) {
        if (segment->vmaddr != 0 || segment->maxprot != VM_PROT_NONE) return 0;
        uintptr_t end = segment->vmsize + _dyld_get_image_vmaddr_slide(0);
        if (end < kZeroSegmentEnd) return 0;
        mach_vm_address_t probe = (end - 1) & ~uintptr_t(vm_page_size - 1);
        kern_return_t result = mach_vm_allocate(mach_task_self(), &probe, vm_page_size, VM_FLAGS_FIXED);
        if (result == KERN_SUCCESS) mach_vm_deallocate(mach_task_self(), probe, vm_page_size);
        return result == KERN_INVALID_ADDRESS ? end : 0;
      }
    }
    command = reinterpret_cast<const load_command*>(
        reinterpret_cast<const char*>(command) + command->cmdsize);
  }
  return 0;
#else
  return 0;
#endif
}

}  // namespace

WorkerdPlatform::WorkerdPlatform(v8::Platform& inner)
    : inner(inner),
      zeroSegmentSize(reserveZeroSegment()) {}

double WorkerdPlatform::CurrentClockTimeMillis() noexcept {
  return dateNow();
}

}  // namespace workerd::server
