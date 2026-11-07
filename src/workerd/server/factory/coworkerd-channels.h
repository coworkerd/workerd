#pragma once
#include <kj/string.h>
#include <cstdint>
namespace workerd::server {
int coworkerdPid();
int cpuEventsDescriptor();
void writeLimitEvent(kj::StringPtr row);
void initializeCoworkerdChannels();
// Borrow the mapping initialized before the Rust server starts; never reopen fd5.
void* coworkerdLifecycleMapping();
const uint32_t* coworkerdMemoryBandMapping();
}  // namespace workerd::server
