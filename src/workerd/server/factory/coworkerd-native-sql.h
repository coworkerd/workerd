// Copyright (c) 2026 Coworkerd contributors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/util/sqlite.h>

#include <chrono>
#include <string>

namespace workerd::server {

class NativeSqlSpill;

inline constexpr auto NATIVE_SQL_LIMIT = std::chrono::seconds(30);

enum class NativeSqlResults { OBJECTS, COLUMNS };

struct NativeSqlReply {
  uint status;
  std::string body;
};

// The caller exclusively owns the database for the duration of the request
// and must confirm any committed WAL before sending the reply, including errors.
NativeSqlReply runNativeSql(SqliteDatabase& db,
    kj::StringPtr body,
    bool query,
    NativeSqlResults format,
    std::chrono::steady_clock::time_point deadline,
    int connectionFd = -1,
    NativeSqlSpill* spill = nullptr,
    SqliteMemoryBudget* requestMemory = nullptr);

std::string nativeSqlFailure(kj::StringPtr code, kj::StringPtr reason);
NativeSqlReply nativeSqlTimeout();

}  // namespace workerd::server
