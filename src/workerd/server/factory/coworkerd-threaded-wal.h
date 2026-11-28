#pragma once

#include "coworkerd-wal-shipper.h"

#include <atomic>
#include <memory>

namespace workerd::server {

// Owns the inherited node connection and its I/O thread for one child generation.
// Each home creates and destroys its own channel handle on its event loop.
class ThreadedWalLog {
  struct OpenReservation;

 public:
  ThreadedWalLog(int fd, uint64_t generation, kj::Duration confirmationTimeout);
  ~ThreadedWalLog() noexcept(false);
  KJ_DISALLOW_COPY_AND_MOVE(ThreadedWalLog);

  bool ready() const;
  kj::Duration confirmationTimeout() const;
  uint64_t unconfirmedCloseIdentities() const;
  static constexpr kj::Exception::DetailTypeId CLOSE_UNCONFIRMED_DETAIL = 0x997a5d73b0e11893ull;
  static constexpr kj::Exception::DetailTypeId HOME_CAPACITY_DETAIL = 0xb81762c5823219dfull;
  enum class CloseReason { DRAIN, CODE_UPDATED };
  class HomeChannel: public DurableWalChannel {
   public:
    // The owner thread must keep pumping while a shipper holds this channel.
    virtual bool hasRegistrations() const = 0;
  };
  // If supplied, the routing flag outlives the Home and is updated on its owning thread.
  kj::Own<HomeChannel> attachHome(kj::Function<void(uint64_t, CloseReason)> requestClose,
      kj::Maybe<std::atomic<bool>&> walFull = kj::none);

  class PreparedOpen {
   public:
    // Check that this coalesced preparation has not already been used.
    DurableWalPosition positionForOpen() const;

    // Bind a successful SQLite open to this reservation. Only a confirmed
    // SQLite close can then release it for a later preparation.
    kj::Function<void(SqliteDatabase::CloseOutcome)> trackClose() const;

   private:
    friend class ThreadedWalLog;
    PreparedOpen(DurableWalPosition position, std::shared_ptr<OpenReservation> reservation)
        : position(position),
          reservation(std::move(reservation)) {}

    DurableWalPosition position;
    std::shared_ptr<OpenReservation> reservation;
  };

  // storageKey is the root database path on the shared volume. Paths for
  // different facets of the same tree may be prepared independently.
  kj::Promise<PreparedOpen> prepareOpen(const kj::Directory& directory,
      kj::Path path,
      kj::String storageKey,
      uint64_t tree,
      uint facet);

 private:
  struct Impl;
  kj::Own<Impl> impl;
};

}  // namespace workerd::server
