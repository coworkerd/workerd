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
#include <v8-context.h>
#include <v8-local-handle.h>
#include <v8-locker.h>
#include <v8-primitive.h>
#include <v8-script.h>

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <spawn.h>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

// Selected by a parent test: before any static initializer runs (the platform reserves the zero
// segment in one), take a page at 1 GiB, as a preloaded library or an early mapping might.
__attribute__((constructor(101))) static void occupyLowForChild() {
  if (getenv("COWORKERD_SANDBOX_CHILD_OCCUPY_LOW") == nullptr) return;
  (void)mmap(reinterpret_cast<void*>(uintptr_t(1) << 30), 4096, PROT_NONE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED_NOREPLACE, -1, 0);
}

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
KJ_TEST("the zero segment is really held, not only reported") {
  // Probed with MAP_FIXED_NOREPLACE at 1 GiB and 4 GiB + 512 KiB, independently of the number the
  // platform reports to V8.
  KJ_EXPECT(zeroSegmentHeld());
  SandboxFacts reportedOnly{
    .mode = static_cast<int>(v8::V8::SandboxMode::kSecure),
    .sandboxBytes = 8 * GiB,
    .addressSpaceBytes = 136 * GiB,
    .groupPerIsolate = true,
    .zeroSegmentHeld = false,
  };
  KJ_EXPECT(mentions(sandboxProblem(reportedOnly), "can still be mapped"));
}

KJ_TEST("an isolate of the default group, or one not entered, is refused") {
  GroupIsolate entry(v8::IsolateGroup::GetDefault());
  // Without the isolate entered V8's current sandbox is null: a refusal, not a crash.
  KJ_EXPECT_THROW_MESSAGE("not entered", requireSecureIsolateSandbox(entry.isolate, "default"_kj));
  v8::Locker locker(entry.isolate);
  v8::Isolate::Scope scope(entry.isolate);
  KJ_EXPECT_THROW_MESSAGE("default isolate group", requireSecureIsolateSandbox(entry.isolate, "default"_kj));
}

KJ_TEST("a resizable ArrayBuffer reserved first leaves plain ArrayBuffers their pool") {
  // The buffer half of the 8 GiB sandbox is 4 GiB. With V8's default in-sandbox allocator, whose
  // pool is reserved on the first plain allocation, a 3.5 GiB resizable buffer reserved first
  // leaves no 1 GiB for it and V8 ends the process. The group's own pool exists before any isolate.
  auto group = KJ_ASSERT_NONNULL(tryNewIsolateGroup());
  GroupIsolate entry(group);
  v8::Locker locker(entry.isolate);
  v8::Isolate::Scope scope(entry.isolate);
  v8::HandleScope handles(entry.isolate);
  auto context = v8::Context::New(entry.isolate);
  v8::Context::Scope inContext(context);
  auto code = "let reserved; try { new ArrayBuffer(0, {maxByteLength: 3.5 * 2 ** 30}); reserved = 'reserved'; }"
              " catch (e) { reserved = e.name; }"
              " const plain = new ArrayBuffer(1 << 20); new Uint8Array(plain)[0] = 1;"
              " reserved + ':' + plain.byteLength"_kj;
  auto source = v8::String::NewFromUtf8(entry.isolate, code.begin(), v8::NewStringType::kNormal, code.size())
                    .ToLocalChecked();
  auto result = v8::Script::Compile(context, source).ToLocalChecked()->Run(context).ToLocalChecked();
  v8::String::Utf8Value text(entry.isolate, result);
  KJ_EXPECT(kj::StringPtr(*text) == "RangeError:1048576"_kj, *text);
}

KJ_TEST("the sandbox pool refuses when full, zeroes, and reuses what was freed") {
  auto group = KJ_ASSERT_NONNULL(tryNewIsolateGroup());
  // A second pool in the same group's buffer half (its own pool holds 2 GiB of it).
  auto pool = newSandboxPoolAllocator(*group.GetSandboxAddressSpace());
  KJ_ASSERT(pool != nullptr);
  constexpr size_t BLOCK = size_t(64) << 20;
  kj::Vector<void*> blocks;
  while (blocks.size() < 64) {
    auto* block = pool->AllocateUninitialized(BLOCK);
    if (block == nullptr) break;
    KJ_EXPECT(group.SandboxContains(block));
    blocks.add(block);
  }
  KJ_EXPECT(blocks.size() >= 4 && blocks.size() < 64, blocks.size());
  KJ_EXPECT(pool->Allocate(BLOCK) == nullptr);
  // A small block reused after it was dirtied comes back zeroed.
  auto* small = static_cast<char*>(pool->Allocate(1000));
  KJ_ASSERT(small == nullptr);  // full
  pool->Free(blocks.back());
  blocks.removeLast();
  small = static_cast<char*>(pool->Allocate(1000));
  KJ_ASSERT(small != nullptr);
  memset(small, 0xab, 1000);
  pool->Free(small);
  small = static_cast<char*>(pool->Allocate(1000));
  KJ_ASSERT(small != nullptr);
  KJ_EXPECT(small[0] == 0 && small[999] == 0);
  pool->Free(small);
  // Freed neighbors merge: the whole pool is one block again.
  const size_t total = (blocks.size() + 1) * BLOCK;
  for (auto* block: blocks) pool->Free(block);
  auto* whole = static_cast<char*>(pool->AllocateUninitialized(total));
  KJ_EXPECT(whole != nullptr);
  pool->Free(whole);
}

KJ_TEST("a backing store that outlives its isolate and group keeps their sandbox") {
  std::shared_ptr<v8::BackingStore> store;
  {
    auto group = KJ_ASSERT_NONNULL(tryNewIsolateGroup());
    GroupIsolate entry(group, jsg::newGroupArrayBufferAllocator(group));
    v8::Locker locker(entry.isolate);
    v8::Isolate::Scope scope(entry.isolate);
    store = v8::ArrayBuffer::NewBackingStore(entry.isolate, 1 << 20);
    memset(store->Data(), 7, 1 << 20);
  }
  // The isolate is gone and so are this test's references to the group: only the allocator V8
  // keeps for the store holds it, and with it the sandbox the bytes are in.
  KJ_EXPECT(static_cast<unsigned char*>(store->Data())[12345] == 7);
  store.reset();
}

struct ChildResult {
  int status;
  std::string output, events;
};
constexpr uint SANDBOX_CHILD_LINE = __LINE__ + 1;
KJ_TEST("child body: the process-wide check, selected by a parent test") {
  if (getenv("COWORKERD_SANDBOX_CHILD") == nullptr) return;
  requireSecureSandbox();
  _exit(0);
}
ChildResult runSandboxChild(bool occupyLow) {
  int output[2], events[2];
  KJ_REQUIRE(pipe2(output, O_CLOEXEC) == 0 && pipe2(events, O_CLOEXEC) == 0);
  int childEvents = fcntl(events[1], F_DUPFD, 100);
  KJ_REQUIRE(childEvents >= 0);
  auto filter = kj::str("--filter=coworkerd-sandbox-test.c++:", SANDBOX_CHILD_LINE);
  auto eventFd = kj::str("COWORKERD_EVENT_FD=", childEvents);
  kj::Vector<char*> env;
  for (char** at = environ; *at != nullptr; ++at)
    if (!kj::StringPtr(*at).startsWith("COWORKERD_")) env.add(*at);
  env.add(const_cast<char*>("COWORKERD_SANDBOX_CHILD=1"));
  if (occupyLow) env.add(const_cast<char*>("COWORKERD_SANDBOX_CHILD_OCCUPY_LOW=1"));
  env.add(eventFd.begin());
  env.add(nullptr);
  posix_spawn_file_actions_t actions;
  posix_spawn_file_actions_init(&actions);
  posix_spawn_file_actions_adddup2(&actions, output[1], STDERR_FILENO);
  char program[] = "/proc/self/exe";
  char* argv[] = {program, filter.begin(), nullptr};
  pid_t pid;
  KJ_REQUIRE(posix_spawn(&pid, program, &actions, nullptr, argv, env.begin()) == 0);
  posix_spawn_file_actions_destroy(&actions);
  close(output[1]);
  close(events[1]);
  close(childEvents);
  auto drain = [](int fd) {
    std::string text;
    char buffer[4096];
    for (ssize_t n; (n = read(fd, buffer, sizeof(buffer))) != 0;) {
      if (n < 0 && errno == EINTR) continue;
      if (n < 0) break;
      text.append(buffer, n);
    }
    close(fd);
    return text;
  };
  ChildResult result;
  result.output = drain(output[0]);
  result.events = drain(events[0]);
  while (waitpid(pid, &result.status, 0) < 0 && errno == EINTR) {}
  return result;
}

KJ_TEST("a process whose first 4 GiB are taken refuses to start with 78 and says why") {
  auto refused = runSandboxChild(true);
  KJ_EXPECT(WIFEXITED(refused.status) && WEXITSTATUS(refused.status) == SANDBOX_REFUSED_EXIT,
      refused.status, refused.output.c_str());
  KJ_EXPECT(refused.output.find("\"mode\":\"smi-accessible\"") != std::string::npos, refused.output.c_str());
  KJ_EXPECT(refused.output.find("workerd: refusing to start: the first 4 GiB") != std::string::npos,
      refused.output.c_str());
  KJ_EXPECT(refused.events.find("\"event\":\"sandbox-refused\"") != std::string::npos, refused.events.c_str());
  // The same process without the page starts.
  auto started = runSandboxChild(false);
  KJ_EXPECT(WIFEXITED(started.status) && WEXITSTATUS(started.status) == 0, started.status, started.output.c_str());
  KJ_EXPECT(started.output.find("\"mode\":\"secure\"") != std::string::npos, started.output.c_str());
}

// Last: it uses up the address space for a moment.
KJ_TEST("isolate groups run out as a refusal, not as the end of the process") {
  // Every group reserves its sandbox with the guard regions (136 GiB) and, outside it, its
  // trusted range: a 47-bit user address space (128 TiB) holds fewer than a thousand. Past that,
  // tryNewIsolateGroup() returns none (V8's IsolateGroup::TryCreate()) and the process goes on.
  auto created = measureGroupCapacity(4096);
  fprintf(stdout, "SANDBOX_GROUPS created=%zu\n", created);
  fflush(stdout);
  KJ_EXPECT(created > 100 && created < 4096, created);
  // Released groups give their address space back.
  KJ_EXPECT(tryNewIsolateGroup() != kj::none);
  KJ_EXPECT(measureGroupCapacity(4096) + 50 > created);
}
#else
KJ_TEST("without the sandbox nothing is checked") {
  KJ_EXPECT(sandboxModeName(0) == "off"_kj);
  KJ_EXPECT(sandboxProblem(SandboxFacts{}) == kj::none);
}
#endif

}  // namespace
}  // namespace workerd::server
