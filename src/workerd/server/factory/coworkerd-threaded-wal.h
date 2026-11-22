#pragma once

#include "coworkerd-wal-shipper.h"

namespace workerd::server {

// Owns the inherited node connection and its I/O thread for one child generation.
// Each home creates and destroys its own channel handle on its event loop.
class ThreadedWalLog {
 public:
  ThreadedWalLog(int fd, uint64_t generation, kj::Duration confirmationTimeout);
  ~ThreadedWalLog() noexcept(false);
  KJ_DISALLOW_COPY_AND_MOVE(ThreadedWalLog);

  bool ready() const;
  kj::Own<DurableWalChannel> attachHome(kj::Function<void(uint64_t)> requestClose);
  kj::Promise<DurableWalPosition> prepareOpen(const kj::Directory& directory, kj::Path path);

 private:
  struct Impl;
  kj::Own<Impl> impl;
};

}  // namespace workerd::server
