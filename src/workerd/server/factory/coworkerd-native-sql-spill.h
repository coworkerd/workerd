// Copyright (c) 2026 Coworkerd.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/util/sqlite.h>

#include <kj/debug.h>
#include <kj/filesystem.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>

namespace workerd::server {

// One spill budget for all database homes in a durable process.
class NativeSqlSpillPool {
 public:
  explicit NativeSqlSpillPool(uint64_t maximum): maximum(maximum) {}

  bool acquire(uint64_t bytes) noexcept {
    auto current = used.load();
    do {
      if (bytes > maximum - current) return false;
    } while (!used.compare_exchange_weak(current, current + bytes));
    return true;
  }

  void release(uint64_t bytes) noexcept { used.fetch_sub(bytes); }
  uint64_t size() const noexcept { return used.load(); }
  const uint64_t maximum;

 private:
  std::atomic<uint64_t> used{0};
};

// The WAL has a committed prefix (or can reuse frames from an earlier
// generation). Only the write extent of the current transaction is charged.
// Temporary files stay charged for their full lifetime.
class NativeSqlSpill final: public SqliteDatabase::VfsOptions::SpillTracker {
 public:
  NativeSqlSpill(NativeSqlSpillPool& pool, uint64_t maximum, const kj::Directory& directory,
      kj::Path walPath)
      : pool(pool), maximum(maximum), directory(directory), walPath(kj::mv(walPath)) {}

  ~NativeSqlSpill() { pool.release(walBytes + temporaryBytes); }

  void limitTransactionFrames(uint64_t bytes, uint32_t pageSize) {
    KJ_ASSERT(!active && pageSize > 0);
    transactionFrames = bytes / (24 + pageSize);
    frameSize = 24 + pageSize;
  }

  void begin() {
    KJ_ASSERT(!active && walBytes == 0);
    active = true;
    exceeded = false;
    transactionTooLarge = false;
    walStatFailed = false;
  }

  bool writeWal(uint64_t offset, uint64_t length) noexcept override {
    if (!active || length == 0) return true;
    if (!walSizeKnown) {
      try {
        KJ_IF_SOME(file, directory.tryOpenFile(walPath)) { originalWalSize = file->stat().size; }
        walSizeKnown = true;
      } catch (const kj::Exception&) {
        walStatFailed = true;
        return false;
      }
    }
    constexpr uint64_t HEADER_BYTES = 32;
    const auto end = offset + length;
    const auto header = offset < HEADER_BYTES ? std::max(headerBytes, std::min(HEADER_BYTES, end)) : headerBytes;
    const auto start = end > HEADER_BYTES ? std::min(firstFrame, std::max(offset, HEADER_BYTES)) : firstFrame;
    const auto finish = end > HEADER_BYTES ? std::max(lastFrame, end) : lastFrame;
    const auto frames = finish > start ? finish - start : 0;
    // SQLite writes a frame's header and page separately. Reject an oversized
    // frame before its commit marker can overwrite any part of a reused WAL.
    if (frameSize != 0 && frames != 0 &&
        (finish - HEADER_BYTES - 1) / frameSize - (start - HEADER_BYTES) / frameSize >= transactionFrames) {
      transactionTooLarge = true;
      return false;
    }
    const auto growth = end > originalWalSize ? end - originalWalSize : 0;
    const auto updated = std::max(walBytes, std::max(header + frames, growth));
    if (!reserve(updated - walBytes)) return false;
    walBytes = updated;
    firstFrame = start;
    lastFrame = finish;
    headerBytes = header;
    return true;
  }

  bool resizeTemporary(uint64_t before, uint64_t after) noexcept override {
    if (after > before) {
      if (!reserve(after - before)) return false;
      temporaryBytes += after - before;
      peakTemporaryBytes = std::max(peakTemporaryBytes, temporaryBytes);
    } else {
      temporaryBytes -= before - after;
      pool.release(before - after);
    }
    return true;
  }

  void end(bool committed) {
    if (!active) return;
    if (!committed && walBytes != 0) {
      // SQLite has already released the write transaction, so its WAL index
      // ignores the abandoned tail. Preserve the entire pre-transaction file.
      KJ_IF_SOME(file, directory.tryOpenFile(walPath, kj::WriteMode::MODIFY)) {
        if (file->stat().size > originalWalSize) file->truncate(originalWalSize);
      }
    }
    pool.release(walBytes);
    active = false;
    walSizeKnown = false;
    originalWalSize = 0;
    firstFrame = std::numeric_limits<uint64_t>::max();
    lastFrame = headerBytes = walBytes = 0;
  }

  bool wasRefused() const noexcept { return exceeded; }
  bool exceededTransactionLimit() const noexcept { return transactionTooLarge; }
  bool failedToMeasureWal() const noexcept { return walStatFailed; }
  uint64_t temporaryHighWater() const noexcept { return peakTemporaryBytes; }

 private:
  NativeSqlSpillPool& pool;
  const uint64_t maximum;
  const kj::Directory& directory;
  kj::Path walPath;
  uint64_t firstFrame = std::numeric_limits<uint64_t>::max(), lastFrame = 0, headerBytes = 0;
  uint64_t originalWalSize = 0, walBytes = 0, temporaryBytes = 0;
  uint64_t peakTemporaryBytes = 0;
  uint64_t transactionFrames = 0;
  uint32_t frameSize = 0;
  bool active = false, walSizeKnown = false, exceeded = false, walStatFailed = false;
  bool transactionTooLarge = false;

  bool reserve(uint64_t bytes) noexcept {
    if (bytes == 0) return true;
    if (bytes <= maximum - walBytes - temporaryBytes && pool.acquire(bytes)) return true;
    exceeded = true;
    return false;
  }
};

}  // namespace workerd::server
