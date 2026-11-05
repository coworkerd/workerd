#include "coworkerd-steady.h"

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <unistd.h>
#if __linux__
#include <fcntl.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <pthread.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <thread>
#include <vector>
#endif

namespace workerd::server {
namespace {

// Every row this file writes is built in a fixed buffer and written with one write(2): after the steady-state
// filter is on, the only calls left to this process are the ones that table holds, and nothing here may
// depend on an allocator reaching for more memory.
void writeRow(const char* row, size_t length) {
  for (size_t done = 0; done < length;) {
    const auto written = write(STDERR_FILENO, row + done, length - done);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) break;
    done += static_cast<size_t>(written);
  }
}

#if __linux__
constexpr size_t MAX_CALLS = 512;

// The steady-state allowlist as the node hands it over: the file scripts/syscall-allowlist.py generated for
// these bytes (coworkerd-syscalls-steady.txt), read through the one path the launcher bound for it. Only the
// numbers are read here; the `# name` table and the digest are the node's business, and it reports them.
// Read before the filter goes on, so openat/read/close are still the startup table's.
size_t readTable(const char* path, int numbers[MAX_CALLS], int& failure) {
  const int fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) { failure = errno; return 0; }
  size_t count = 0, held = 0;
  bool comment = false, digits = false;
  int value = 0;
  char buffer[8192];
  for (;;) {
    const auto n = read(fd, buffer, sizeof buffer);
    if (n < 0 && errno == EINTR) continue;
    if (n < 0) { failure = errno; close(fd); return 0; }
    if (n == 0) break;
    held += static_cast<size_t>(n);
    // A line that does not begin with a digit is a comment, as in the file the launcher reads; a line that does
    // begin with one is that call, and whatever follows the number on it (the generator writes `21\t# epoll_ctl`)
    // is a comment too.
    for (ssize_t i = 0; i < n; ++i) {
      const char c = buffer[i];
      if (c == '\n') {
        if (digits && count < MAX_CALLS) numbers[count++] = value;
        comment = digits = false;
        value = 0;
        continue;
      }
      if (comment) continue;
      if (c >= '0' && c <= '9') { digits = true; value = value * 10 + (c - '0'); continue; }
      if (digits) {  // the number ended; the rest of the line says what it is called
        if (count < MAX_CALLS) numbers[count++] = value;
        digits = false;
        value = 0;
      }
      if (c != ' ' && c != '\t') comment = true;
    }
    if (held > (1u << 20)) { failure = EFBIG; close(fd); return 0; }
  }
  if (digits && count < MAX_CALLS) numbers[count++] = value;
  close(fd);
  return count;
}

// The launcher's filter layout (launcher/launcher.c, spike 21 PROTOCOL 4.4), so that both tables are
// installed by the same program shape: the architecture is checked, the x32 range is rejected, each allowed
// call is one equality test, and everything else takes `action`. Every jump is forward -- the kernel refuses
// a filter with a backward one -- so the refusal is repeated rather than jumped back to.
size_t buildFilter(const int* numbers, size_t count, __u32 action, struct sock_filter* program) {
#if defined(__x86_64__)
  const __u32 arch = AUDIT_ARCH_X86_64;
#elif defined(__aarch64__)
  const __u32 arch = AUDIT_ARCH_AARCH64;
#else
#error "the steady-state system call filter needs this architecture's audit arch"
#endif
  size_t at = 0;
  program[at++] = (struct sock_filter)BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, arch));
  program[at++] = (struct sock_filter)BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, arch, 1, 0);
  program[at++] = (struct sock_filter)BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS);
  program[at++] = (struct sock_filter)BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr));
#if defined(__x86_64__)
  // An x32 call carries bit 30; reject the whole range rather than letting it alias an allowed number.
  program[at++] = (struct sock_filter)BPF_JUMP(BPF_JMP | BPF_JGE | BPF_K, 0x40000000, 1, 0);
  program[at++] = (struct sock_filter)BPF_STMT(BPF_JMP | BPF_JA | BPF_K, 1);
  program[at++] = (struct sock_filter)BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS);
#endif
  for (size_t i = 0; i < count; ++i)
    program[at++] = (struct sock_filter)BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, static_cast<__u32>(numbers[i]),
        static_cast<__u8>(count - i), 0);
  program[at++] = (struct sock_filter)BPF_STMT(BPF_RET | BPF_K, action);
  program[at++] = (struct sock_filter)BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
  return at;
}

// The observe mode's supervisor, inside this process. The launcher's own observe mode hands its listener to
// the node, which still has a socket to it; this filter is installed long after that socket is closed, so
// the process that narrowed itself answers its own notifications: every call outside the steady table is let
// through (SECCOMP_USER_NOTIF_FLAG_CONTINUE), so behaviour is as it is without the filter, and the first
// notification of each call number writes a row into the node's workerd.log.
//
// Two things keep this from waiting on itself. The thread exists before the filter is installed -- the
// steady table of a serving process holds no `clone` -- and waits for the listener on a condition variable
// (futex, which it does hold). After that it makes only ioctl and write calls, both in the table, and never
// allocates: a call of its own outside the table would be sent to this thread, which is inside it. The
// receive blocks, as the kernel's receive always does, so it needs no poll -- which the table does not hold
// either -- and the thread ends with the process. This is also why `enforce`, with no supervisor at all, is
// the mode for production.
class SteadySupervisor final {
 public:
  void start() { std::thread([this] { loop(); }).detach(); }
  void supervise(int fd) {
    {
      std::lock_guard<std::mutex> lock(mutex);
      listener = fd;
    }
    ready.notify_one();
  }

 private:
  std::mutex mutex;
  std::condition_variable ready;
  int listener = -1;
  bool reported[MAX_CALLS] = {};
  uint64_t notifications = 0;

  void loop() {
    // Process-directed signals go to other threads (the limit watchdog does the same).
    sigset_t signals;
    sigfillset(&signals);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);
    pthread_setname_np(pthread_self(), "coworkerd-steady");
    int fd = -1;
    {
      std::unique_lock<std::mutex> lock(mutex);
      ready.wait(lock, [this] { return listener >= 0; });
      fd = listener;
    }
    for (;;) {
      struct seccomp_notif request;
      memset(&request, 0, sizeof request);
      if (ioctl(fd, SECCOMP_IOCTL_NOTIF_RECV, &request) != 0) {
        if (errno == EINTR || errno == ENOENT) continue;  // ENOENT: that thread was signalled meanwhile
        return;                                          // the filter is gone with this process
      }
      struct seccomp_notif_resp response;
      memset(&response, 0, sizeof response);
      response.id = request.id;
      response.flags = SECCOMP_USER_NOTIF_FLAG_CONTINUE;
      const bool answered = ioctl(fd, SECCOMP_IOCTL_NOTIF_SEND, &response) == 0 || errno == ENOENT;
      ++notifications;
      const auto number = static_cast<size_t>(request.data.nr);
      if (request.data.nr >= 0 && number < MAX_CALLS && !reported[number]) {
        reported[number] = true;
        char row[256];
        const auto length = snprintf(row, sizeof row,
            "[coworkerd-seccomp] {\"event\":\"steady-observed\",\"call\":%d,\"pid\":%u,\"answered\":%s,"
            "\"notifications\":%llu}\n", static_cast<int>(request.data.nr), request.pid,
            answered ? "true" : "false", static_cast<unsigned long long>(notifications));
        if (length > 0 && static_cast<size_t>(length) < sizeof row) writeRow(row, static_cast<size_t>(length));
      }
    }
  }
};

// Everything this process does once it serves: the dumpable bit the launcher cleared before it executed us
// (the kernel sets it again for a readable, same-owner binary -- which is why v44's state endpoint reported
// `nonDumpable` false and why this is done from here), and the steady-state filter.
void narrow() {
  const char* modeText = std::getenv("COWORKERD_SECCOMP_STEADY_MODE");
  const char* path = std::getenv("COWORKERD_SECCOMP_STEADY_FILE");
  const char* dumpableText = std::getenv("COWORKERD_NON_DUMPABLE");
  const char* mode = modeText == nullptr || *modeText == '\0' ? "off" : modeText;
  const bool observe = strcmp(mode, "observe") == 0, enforce = strcmp(mode, "enforce") == 0;
  const bool wantNonDumpable = dumpableText != nullptr && *dumpableText == '1';
  const int dumpable = wantNonDumpable ? prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) : 0;
  const int dumpableErrno = dumpable == 0 ? 0 : errno;
  if (!wantNonDumpable && !observe && !enforce) return;  // nothing asked of this process
  int numbers[MAX_CALLS];
  size_t count = 0;
  int failure = 0;
  bool installed = false;
  size_t instructions = 0;
  if ((observe || enforce) && path != nullptr && *path != '\0') {
    count = readTable(path, numbers, failure);
    if (count == 0 && failure == 0) failure = EINVAL;  // a table without a call is a configuration error
  } else if (observe || enforce) {
    failure = ENOENT;
  }
  if (count > 0) {
    std::vector<struct sock_filter> program(count + 8);
    instructions = buildFilter(numbers, count, observe ? SECCOMP_RET_USER_NOTIF : SECCOMP_RET_KILL_PROCESS,
        program.data());
    struct sock_fprog filter = {static_cast<unsigned short>(instructions), program.data()};
    // no_new_privs is seccomp(2)'s precondition for an unprivileged process. The launcher set it and it
    // survives the execve; setting it again keeps this independent of how the process was started.
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
      failure = errno;
    } else if (observe) {
      // The supervisor exists before the filter does: from the moment it is installed, a call outside the
      // table waits for an answer, and only this thread can give one.
      auto* supervisor = new SteadySupervisor();
      supervisor->start();
      const long fd = syscall(SYS_seccomp, SECCOMP_SET_MODE_FILTER, SECCOMP_FILTER_FLAG_NEW_LISTENER, &filter);
      if (fd < 0) failure = errno;
      else { supervisor->supervise(static_cast<int>(fd)); installed = true; }
    } else if (syscall(SYS_seccomp, SECCOMP_SET_MODE_FILTER, SECCOMP_FILTER_FLAG_TSYNC, &filter) != 0) {
      failure = errno;
    } else {
      installed = true;
    }
  }
  // Test only: a call this process no longer has (v49's acceptance 3.1). The startup table holds the calls only
  // the startup needs -- execve above all, since the launcher has to execve this program -- and the steady table
  // does not; with the narrow filter in force, making one of them must end the process. Honoured in `enforce`
  // alone: in `observe` the call would be let through, and an execve of this process is not something to let
  // through. COWORKERD_SECCOMP_STEADY_PROBE_FOR_TEST=execve|seccomp.
  if (installed && enforce) {
    const char* probe = std::getenv("COWORKERD_SECCOMP_STEADY_PROBE_FOR_TEST");
    if (probe != nullptr && strcmp(probe, "execve") == 0) {
      char row[160];
      const auto length = snprintf(row, sizeof row,
          "[coworkerd-seccomp] {\"event\":\"steady-probe\",\"call\":\"execve\",\"pid\":%d}\n", getpid());
      if (length > 0) writeRow(row, static_cast<size_t>(length));
      char* const argv[] = {const_cast<char*>("/proc/self/exe"), nullptr};
      char* const envp[] = {nullptr};
      execve("/proc/self/exe", argv, envp);
    } else if (probe != nullptr && strcmp(probe, "seccomp") == 0) {
      char row[160];
      const auto length = snprintf(row, sizeof row,
          "[coworkerd-seccomp] {\"event\":\"steady-probe\",\"call\":\"seccomp\",\"pid\":%d}\n", getpid());
      if (length > 0) writeRow(row, static_cast<size_t>(length));
      syscall(SYS_seccomp, SECCOMP_SET_MODE_STRICT, 0, nullptr);
    }
  }
  // One row either way, through this process's standard error into the node's workerd.log: what it narrowed
  // to, whether it is non-dumpable, and the reason when it is neither. The node reports what /proc shows of
  // the process itself (`Seccomp_filters`, the dumpable owner of its environ), not this row.
  char row[512];
  const auto length = snprintf(row, sizeof row,
      "[coworkerd-seccomp] {\"event\":\"steady\",\"mode\":\"%s\",\"installed\":%s,\"calls\":%zu,"
      "\"instructions\":%zu,\"errno\":%d,\"nonDumpable\":%s,\"dumpableErrno\":%d,\"pid\":%d}\n",
      mode, installed ? "true" : "false", count, instructions, failure,
      wantNonDumpable && dumpable == 0 ? "true" : "false", dumpableErrno, getpid());
  if (length > 0 && static_cast<size_t>(length) < sizeof row) writeRow(row, static_cast<size_t>(length));
}
#endif

}  // namespace

void coworkerdServing() {
#if __linux__
  static std::atomic<bool> done{false};
  bool expected = false;
  if (done.compare_exchange_strong(expected, true)) narrow();
#endif
}

}  // namespace workerd::server
