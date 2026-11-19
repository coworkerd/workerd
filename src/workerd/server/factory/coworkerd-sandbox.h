#pragma once

#include <kj/common.h>
#include <kj/string.h>
#include <cstddef>

namespace v8 {
class Isolate;
}

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
};

// Why `facts` do not describe a secure sandbox laid out as this build configured it, or none.
kj::Maybe<kj::String> sandboxProblem(const SandboxFacts& facts);

// V8's name-like spelling of a v8::V8::SandboxMode: "secure", "smi-accessible",
// "partially-reserved", "partially-reserved-smi-accessible", or "insecure"; "off" without the
// sandbox.
kj::StringPtr sandboxModeName(int mode);

// Checks the process-wide sandbox once V8 is initialized, on the thread that initialized it, and
// writes one [coworkerd-sandbox] row to stderr. Unless the sandbox is secure and laid out as this
// build configured it, the row carries the problem and the process exits with status
// SANDBOX_REFUSED_EXIT before it loads any Worker.
void requireSecureSandbox();
constexpr int SANDBOX_REFUSED_EXIT = 78;

// The same for the sandbox of `isolate`, which the caller holds locked and entered, before the
// isolate runs any code; the row names the isolate. Throws instead of exiting, so that only this
// Worker is not created.
void requireSecureIsolateSandbox(v8::Isolate* isolate, kj::StringPtr name);

}  // namespace workerd::server
