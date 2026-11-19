// The V8 sandbox in Coworkerd's build (--config=coworkerd-sandbox): its layout, V8's secure mode
// for the process and for every isolate group, and the two places where the embedder must place
// bytes in the right group's sandbox.
#include "coworkerd-memory-limit.h"
#include "coworkerd-sandbox.h"

#include <workerd/jsg/sandbox.h>
#include <workerd/jsg/setup.h>
#include <workerd/server/v8-platform-impl.h>

#include <kj/test.h>
#include <kj/vector.h>
#include <v8-array-buffer.h>
#include <v8-initialization.h>
#include <v8-internal.h>
#include <v8-isolate.h>
#include <v8-locker.h>

#include <cstring>

namespace workerd::server {
namespace {

// As the workerd binary does: the platform reserves the zero segment before V8 starts.
kj::Own<v8::Platform> innerPlatform = jsg::defaultPlatform(0);
WorkerdPlatform platform(*innerPlatform);
jsg::V8System v8System(platform, nullptr, innerPlatform.get());

#ifdef V8_ENABLE_SANDBOX
constexpr size_t GiB = size_t(1) << 30;

bool mentions(const kj::Maybe<kj::String>& problem, const char* words) {
  KJ_IF_SOME(text, problem) {
    return strstr(text.cStr(), words) != nullptr;
  }
  return false;
}

// An isolate in `group`, with `allocator` or else V8's allocator for that group.
struct GroupIsolate {
  explicit GroupIsolate(
      v8::IsolateGroup groupParam, std::shared_ptr<v8::ArrayBuffer::Allocator> allocator = nullptr)
      : group(kj::mv(groupParam)) {
    v8::Isolate::CreateParams params;
    params.array_buffer_allocator_shared = allocator != nullptr
        ? kj::mv(allocator)
        : std::shared_ptr<v8::ArrayBuffer::Allocator>(
              v8::ArrayBuffer::Allocator::NewDefaultAllocator(group));
    isolate = v8::Isolate::New(group, params);
  }
  ~GroupIsolate() {
    isolate->Dispose();
  }
  KJ_DISALLOW_COPY_AND_MOVE(GroupIsolate);

  v8::IsolateGroup group;
  v8::Isolate* isolate;
};

// First: V8 answers for the calling thread's sandbox, which is the process-wide one only until
// this thread has entered and left an isolate.
KJ_TEST("the process-wide sandbox is secure and laid out as configured") {
  KJ_EXPECT(v8::internal::kSandboxSize == 8 * GiB);
  KJ_EXPECT(v8::internal::kSandboxGuardRegionSize == 64 * GiB);
  KJ_EXPECT(v8::internal::kAdditionalTrailingGuardRegionSize == 0);
  KJ_EXPECT(v8::internal::kRequiresTypedArrayAccessMasks);
  KJ_EXPECT(v8::IsolateGroup::CanCreateNewGroups());

  KJ_EXPECT(v8::V8::GetSandboxMode() == v8::V8::SandboxMode::kSecure,
      sandboxModeName(static_cast<int>(v8::V8::GetSandboxMode())));
  KJ_EXPECT(v8::V8::GetSandboxSizeInBytes() == 8 * GiB);
  KJ_EXPECT(v8::V8::GetSandboxAddressSpace()->size() == 136 * GiB);
  SandboxFacts facts{
    .mode = static_cast<int>(v8::V8::GetSandboxMode()),
    .sandboxBytes = v8::V8::GetSandboxSizeInBytes(),
    .addressSpaceBytes = v8::V8::GetSandboxAddressSpace()->size(),
    .groupPerIsolate = v8::IsolateGroup::CanCreateNewGroups(),
  };
  KJ_EXPECT(sandboxProblem(facts) == kj::none);
}

KJ_TEST("every isolate group has its own secure sandbox") {
  constexpr size_t COUNT = 3;
  kj::Vector<kj::Own<GroupIsolate>> isolates;
  kj::Vector<uintptr_t> bases;
  bases.add(v8::IsolateGroup::GetDefault().GetSandboxAddressSpace()->base());
  for (size_t i = 0; i < COUNT; i++) {
    auto& entry = *isolates.add(kj::heap<GroupIsolate>(jsg::newIsolateGroup()));
    v8::Locker locker(entry.isolate);
    v8::Isolate::Scope scope(entry.isolate);
    auto* space = entry.group.GetSandboxAddressSpace();
    KJ_EXPECT(v8::V8::GetSandboxAddressSpace() == space);
    KJ_EXPECT(v8::V8::GetSandboxMode() == v8::V8::SandboxMode::kSecure,
        sandboxModeName(static_cast<int>(v8::V8::GetSandboxMode())));
    KJ_EXPECT(space->size() == 136 * GiB);
    requireSecureIsolateSandbox(entry.isolate, "every-group-test"_kj);
    bases.add(space->base());
  }
  // The default group's and the three new ones: four reservations that do not overlap.
  for (size_t i = 0; i < bases.size(); i++) {
    for (size_t j = i + 1; j < bases.size(); j++) {
      KJ_EXPECT(bases[i] + 136 * GiB <= bases[j] || bases[j] + 136 * GiB <= bases[i], i, j);
    }
  }
}

KJ_TEST("a memory-limited isolate allocates its ArrayBuffers in its own sandbox") {
  auto group = jsg::newIsolateGroup();
  auto other = jsg::newIsolateGroup();
  auto state = std::make_shared<std::atomic<IsolateLimitCause>>(IsolateLimitCause::NONE);
  auto limit = std::make_shared<IsolateMemoryLimit>(64, state, group);
  GroupIsolate entry(group, limit);
  v8::Locker locker(entry.isolate);
  v8::Isolate::Scope scope(entry.isolate);
  // A backing store through the isolate's allocator; V8 checks that it lies in the isolate's sandbox.
  auto store = v8::ArrayBuffer::NewBackingStore(entry.isolate, 1 << 20);
  KJ_EXPECT(group.SandboxContains(store->Data()));
  KJ_EXPECT(!other.SandboxContains(store->Data()));
  KJ_EXPECT(limit->bufferBytes.load() >= size_t(1) << 20);
}

KJ_TEST("a byte view of an ArrayBuffer decodes to the same bytes in its isolate's sandbox") {
  GroupIsolate entry(jsg::newIsolateGroup());
  v8::Locker locker(entry.isolate);
  v8::Isolate::Scope scope(entry.isolate);
  auto store = v8::ArrayBuffer::NewBackingStore(entry.isolate, 4096);
  auto bytes = kj::arrayPtr(static_cast<const kj::byte*>(store->Data()), 4096);
  jsg::SandboxedBytes view(entry.isolate, bytes);
  auto decoded = view.get(entry.isolate);
  KJ_EXPECT(decoded.begin() == bytes.begin());
  KJ_EXPECT(decoded.size() == bytes.size());
}

KJ_TEST("sandboxProblem names what is wrong with a sandbox") {
  SandboxFacts good{
    .mode = static_cast<int>(v8::V8::SandboxMode::kSecure),
    .sandboxBytes = 8 * GiB,
    .addressSpaceBytes = 136 * GiB,
    .groupPerIsolate = true,
  };
  KJ_EXPECT(sandboxProblem(good) == kj::none);

  auto smiAccessible = good;
  smiAccessible.mode = static_cast<int>(v8::V8::SandboxMode::kInsecureFullReservationSmiAccessible);
  KJ_EXPECT(mentions(sandboxProblem(smiAccessible), "first 4 GiB"));

  auto partial = good;
  partial.mode = static_cast<int>(v8::V8::SandboxMode::kInsecurePartialReservationSmiInaccessible);
  KJ_EXPECT(mentions(sandboxProblem(partial), "guard regions"));

  auto sharedCage = good;
  sharedCage.groupPerIsolate = false;
  KJ_EXPECT(mentions(sandboxProblem(sharedCage), "one pointer-compression cage"));

  auto defaultSize = good;
  defaultSize.sandboxBytes = 1024 * GiB;
  KJ_EXPECT(mentions(sandboxProblem(defaultSize), "sandbox defines"));

  auto defaultTrailingGuard = good;
  defaultTrailingGuard.addressSpaceBytes = 360 * GiB;
  KJ_EXPECT(mentions(sandboxProblem(defaultTrailingGuard), "reserves"));
}
#else
KJ_TEST("without the sandbox nothing is checked") {
  KJ_EXPECT(sandboxModeName(0) == "off"_kj);
  KJ_EXPECT(sandboxProblem(SandboxFacts{}) == kj::none);
}
#endif

}  // namespace
}  // namespace workerd::server
