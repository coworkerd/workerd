#pragma once

namespace workerd::server {
// Coworkerd v49: the moment this process starts serving. Called at the start of every request; the first
// call -- the node's first readiness probe, before anything has been answered -- makes the process
// non-dumpable and installs the steady-state system call filter, as the node's environment asks. Nothing
// happens without that environment, and nothing happens outside Linux.
void coworkerdServing();
}  // namespace workerd::server
