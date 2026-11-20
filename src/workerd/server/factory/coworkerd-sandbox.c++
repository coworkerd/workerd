#include "coworkerd-sandbox.h"

#include <kj/debug.h>
#include <v8-initialization.h>
#include <v8-internal.h>
#include <v8-isolate.h>
#include <v8-platform.h>

#include <kj/vector.h>

#include <cerrno>
#include <unistd.h>

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
  };
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
      ",\"groupPerIsolate\":", facts.groupPerIsolate ? "true" : "false", problemMember, "}\n"));
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
  if (facts.addressSpaceBytes != CONFIGURED_ADDRESS_SPACE) {
    return kj::str("V8's sandbox reserves ", facts.addressSpaceBytes,
        " bytes with its guard regions, not the ", CONFIGURED_ADDRESS_SPACE,
        " this build was configured with: part of the build lacks the sandbox defines");
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
    _exit(SANDBOX_REFUSED_EXIT);
  }
#else
  writeStderr(kj::str("[coworkerd-sandbox] {\"process\":true,\"mode\":\"off\",\"groupPerIsolate\":",
      v8::IsolateGroup::CanCreateNewGroups() ? "true" : "false", "}\n"));
#endif
}

kj::Maybe<v8::IsolateGroup> tryNewIsolateGroup() {
  if (!v8::IsolateGroup::CanCreateNewGroups()) return v8::IsolateGroup::GetDefault();
  auto group = v8::IsolateGroup::TryCreate();
  if (!group.has_value()) return kj::none;
  return kj::mv(*group);
}

void requireSecureIsolateSandbox(v8::Isolate* isolate, kj::StringPtr name) {
#ifdef V8_ENABLE_SANDBOX
  // V8 answers for the calling thread's sandbox; it must be this isolate's.
  KJ_REQUIRE(isolate->GetGroup().GetSandboxAddressSpace() == v8::V8::GetSandboxAddressSpace(),
      "the isolate's sandbox is not the current one; is the isolate entered?", name);
  auto facts = currentFacts();
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
