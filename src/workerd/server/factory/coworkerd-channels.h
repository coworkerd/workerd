#pragma once
#include <kj/string.h>
namespace workerd::server {
int coworkerdPid();
int cpuEventsDescriptor();
void writeLimitEvent(kj::StringPtr row);
void initializeCoworkerdChannels();
// Borrow the mapping initialized before the Rust server starts; never reopen fd5.
void* coworkerdLifecycleMapping();
}  // namespace workerd::server
