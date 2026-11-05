#pragma once
#include <kj/string.h>
namespace workerd::server {
int coworkerdPid();
int cpuEventsDescriptor();
void writeLimitEvent(kj::StringPtr row);
void initializeCoworkerdChannels();
}  // namespace workerd::server
