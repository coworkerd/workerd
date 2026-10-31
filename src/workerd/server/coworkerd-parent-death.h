#pragma once

#include <kj/async-io.h>
#include <kj/debug.h>
#include <cerrno>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>

namespace workerd::server {
class CoworkerdParentWatch {
 public:
  explicit CoworkerdParentWatch(kj::Own<kj::AsyncInputStream> input): input(kj::mv(input)) {
    task = watch().eagerlyEvaluate([](kj::Exception&&) { parentGone("error"); });
  }
 private:
  kj::Own<kj::AsyncInputStream> input;
  kj::Promise<void> task = nullptr;
  char bytes[64];
  [[noreturn]] static void parentGone(const char* reason) {
    char row[192];
    int n = snprintf(row, sizeof(row),
        "[coworkerd-lifecycle] {\"event\":\"parent-gone\",\"pid\":%d,\"reason\":\"%s\"}\n",
        getpid(), reason);
    ssize_t written;
    do { written = write(STDERR_FILENO, row, n); } while (written < 0 && errno == EINTR);
    _exit(0);
  }
  kj::Promise<void> watch() {
    while (co_await input->tryRead(bytes, 1, sizeof(bytes)) != 0) {}
    parentGone("eof");
  }
};

inline void followCoworkerdParent(kj::LowLevelAsyncIoProvider& provider) {
  const char* text = std::getenv("COWORKERD_PARENT_FD");
  if (text == nullptr) return;
  char* end;
  long fd = std::strtol(text, &end, 10);
  KJ_REQUIRE(*text && !*end && fd > STDERR_FILENO && fd <= 1000000,
      "COWORKERD_PARENT_FD must be an inherited descriptor");
  // Process lifetime; retaining the promise keeps the asynchronous read registered on the main loop.
  static auto* watch = new CoworkerdParentWatch(provider.wrapSocketFd(
      static_cast<kj::LowLevelAsyncIoProvider::Fd>(fd),
      kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP));
  (void)watch;
}
}  // namespace workerd::server
