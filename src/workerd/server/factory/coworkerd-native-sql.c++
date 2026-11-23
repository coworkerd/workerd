// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-native-sql.h"

#include <poll.h>
#include <sqlite3.h>
#include <strings.h>
#include <sys/socket.h>

#include <capnp/compat/json.h>
#include <capnp/message.h>
#include <kj/debug.h>
#include <kj/encoding.h>
#include <kj/vector.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <limits>
#include <string_view>
#include <vector>

namespace workerd::server {
namespace {

using Json = capnp::JsonValue;
using Clock = std::chrono::steady_clock;
using Value = SqliteDatabase::Query::ValuePtr;
constexpr size_t RESULT_LIMIT = 7340032;
constexpr kj::StringPtr RESULT_TOO_LARGE =
    "D1_RESULT_TOO_LARGE: query response exceeds 7340032 bytes; use LIMIT to fetch results in smaller batches"_kj;

struct SqlError {
  kj::String message;
};
struct ResultTooLarge {};
struct Interrupted {};

class Deadline {
 public:
  Deadline(SqliteDatabase& database, Clock::time_point end, int fd)
      : database(database),
        end(end),
        fd(fd) {
    sqlite3_progress_handler(database, 1000,
        [](void* self) { return static_cast<Deadline*>(self)->stopped() ? 1 : 0; }, this);
  }

  ~Deadline() {
    disable();
  }

  void disable() {
    sqlite3_progress_handler(database, 0, nullptr, nullptr);
  }

  bool stopped() {
    if (expired || disconnected) return true;
    const auto now = Clock::now();
    expired = now >= end;
    if (!expired && fd >= 0 && now >= nextPoll) {
      nextPoll = now + std::chrono::milliseconds(1);
      pollfd socket{fd, POLLRDHUP | POLLIN, 0};
      if (poll(&socket, 1, 0) == 1) {
        disconnected = socket.revents & (POLLRDHUP | POLLHUP | POLLERR | POLLNVAL);
        if (!disconnected && (socket.revents & POLLIN)) {
          char byte;
          disconnected = recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT) == 0;
        }
      }
    }
    return expired || disconnected;
  }

  void check() {
    if (stopped()) throw Interrupted{};
  }

  bool wasInterrupted() const {
    return expired || disconnected;
  }
  bool clientGone() const {
    return disconnected;
  }

 private:
  SqliteDatabase& database;
  Clock::time_point end;
  int fd;
  Clock::time_point nextPoll = Clock::time_point::min();
  bool expired = false;
  bool disconnected = false;
};

class Regulator final: public SqliteDatabase::Regulator {
 public:
  bool isAllowedName(kj::StringPtr name) const override {
    return name.size() < 4 || strncasecmp(name.begin(), "_cf_", 4) != 0;
  }
  bool isAllowedTrigger(kj::StringPtr) const override {
    return true;
  }
  bool shouldAddQueryStats() const override {
    return true;
  }

  // The SQLite authorizer saves KJ exceptions for propagation outside the C callback.
  bool allowTransactions() const override {
    throw KJ_EXCEPTION(FAILED,
        "To execute a transaction, please use the state.storage.transaction() or "
        "state.storage.transactionSync() APIs instead of the SQL BEGIN TRANSACTION or SAVEPOINT "
        "statements. The JavaScript API is safer because it will automatically roll back on "
        "exceptions, and because it interacts correctly with Durable Objects' automatic atomic "
        "write coalescing.");
  }

  void onError(kj::Maybe<int>, kj::StringPtr message) const override {
    throw SqlError{kj::str(message)};
  }
};

constexpr Regulator REGULATOR;

unsigned hexCodeUnit(kj::ArrayPtr<const char> input) {
  if (input.size() < 4) return 0;
  unsigned value = 0;
  auto parsed = std::from_chars(input.begin(), input.begin() + 4, value, 16);
  return parsed.ec == std::errc() && parsed.ptr == input.begin() + 4 ? value : 0;
}

kj::Array<char> normalizeJson(kj::ArrayPtr<const char> input, Deadline& deadline) {
  if (input.size() >= 3 && std::string_view(input.begin(), 3) == "\xef\xbb\xbf") {
    input = input.slice(3);
  }
  kj::Vector<char> result(input.size());
  bool quoted = false;
  for (size_t i = 0; i < input.size(); ++i) {
    if ((i & 1023) < 12) deadline.check();
    char c = input[i];
    if (c == '"') quoted = !quoted;
    if (quoted && c == '\\' && i + 1 < input.size()) {
      char escape = input[++i];
      if (escape == 'u') {
        unsigned code = hexCodeUnit(input.slice(i + 1));
        if (code >= 0xd800 && code <= 0xdfff) {
          char16_t units[]{static_cast<char16_t>(code), 0};
          i += 4;
          if (code <= 0xdbff && i + 6 < input.size() && input[i + 1] == '\\' &&
              input[i + 2] == 'u') {
            unsigned next = hexCodeUnit(input.slice(i + 3));
            if (next >= 0xdc00 && next <= 0xdfff) {
              units[1] = static_cast<char16_t>(next);
              i += 6;
            }
          }
          if (units[1] == 0) units[0] = 0xfffd;
          result.addAll(kj::decodeUtf16(kj::arrayPtr(units, units[1] == 0 ? 1 : 2)));
          continue;
        }
      }
      result.add(c);
      result.add(escape);
      continue;
    }
    KJ_REQUIRE(!quoted || static_cast<unsigned char>(c) >= 0x20, "invalid JSON string");
    KJ_REQUIRE(
        quoted || c != '.' || (i + 1 < input.size() && input[i + 1] >= '0' && input[i + 1] <= '9'),
        "invalid JSON fraction");
    result.add(c);
  }
  return result.releaseAsArray();
}

Json::Reader field(Json::Reader object, kj::StringPtr name) {
  Json::Reader value;
  if (object.isObject()) {
    for (auto member: object.getObject()) {
      if (member.getName() == name) value = member.getValue();
    }
  }
  return value;
}

bool hasField(Json::Reader object, kj::StringPtr name) {
  if (object.isObject()) {
    for (auto member: object.getObject()) {
      if (member.getName() == name) return true;
    }
  }
  return false;
}

// JSON.stringify prints the shortest representation of a JS number in fixed
// notation for exponents in [-6, 20], and scientific notation otherwise.
std::string number(double value) {
  if (!std::isfinite(value)) return "null";
  if (value == 0) return "0";
  char buffer[64];
  auto result =
      std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::scientific);
  KJ_ASSERT(result.ec == std::errc());
  std::string_view representation(buffer, result.ptr - buffer);
  const bool negative = representation.front() == '-';
  if (negative) representation.remove_prefix(1);
  const auto e = representation.find('e');
  int exponent = 0;
  auto parsed = std::from_chars(
      representation.data() + e + 2, representation.data() + representation.size(), exponent);
  KJ_ASSERT(parsed.ec == std::errc());
  if (representation[e + 1] == '-') exponent = -exponent;
  std::string out = negative ? "-" : "";
  if (exponent < -6 || exponent >= 21) {
    out += representation.substr(0, e);
    out += exponent < 0 ? "e-" : "e+";
    out += std::to_string(std::abs(exponent));
    return out;
  }
  std::string digits(representation.substr(0, e));
  if (digits.size() > 1) digits.erase(1, 1);
  const auto point = exponent + 1;
  if (point <= 0) {
    out += "0.";
    out.append(-point, '0');
    out += digits;
  } else if (size_t(point) >= digits.size()) {
    out += digits;
    out.append(size_t(point) - digits.size(), '0');
  } else {
    digits.insert(size_t(point), 1, '.');
    out += digits;
  }
  return out;
}

class Writer {
 public:
  explicit Writer(size_t limit = RESULT_LIMIT, Deadline* deadline = nullptr)
      : limit(limit),
        deadline(deadline) {}

  void append(std::string_view value) {
    if (value.size() > limit - bytes.size()) throw ResultTooLarge{};
    bytes += value;
  }

  void tick() {
    if (deadline != nullptr) deadline->check();
  }

  void string(kj::StringPtr text) {
    append("\"");
    constexpr char HEX[] = "0123456789abcdef";
    for (size_t i = 0; i < text.size();) {
      if ((i & 1023) < 4) tick();
      const auto value = static_cast<unsigned char>(text[i]);
      switch (value) {
        case '"':
          append("\\\"");
          break;
        case '\\':
          append("\\\\");
          break;
        case '\b':
          append("\\b");
          break;
        case '\f':
          append("\\f");
          break;
        case '\n':
          append("\\n");
          break;
        case '\r':
          append("\\r");
          break;
        case '\t':
          append("\\t");
          break;
        default:
          if (value < 0x20) {
            char escaped[]{'\\', 'u', '0', '0', HEX[value >> 4], HEX[value & 15]};
            append({escaped, sizeof(escaped)});
          } else if (value < 0x80) {
            append({text.begin() + i, 1});
          } else {
            // Match decoding SQLite text into a JS string, including replacement
            // of malformed UTF-8. Consume the valid prefix of a truncated sequence.
            const size_t length = value >= 0xc2 && value <= 0xdf ? 2
                : value >= 0xe0 && value <= 0xef                 ? 3
                : value >= 0xf0 && value <= 0xf4                 ? 4
                                                                 : 0;
            size_t n = 1;
            while (n < length && i + n < text.size()) {
              const auto next = static_cast<unsigned char>(text[i + n]);
              const int low = n == 1 && value == 0xe0 ? 0xa0
                  : n == 1 && value == 0xf0           ? 0x90
                                                      : 0x80;
              const int high = n == 1 && value == 0xed ? 0x9f
                  : n == 1 && value == 0xf4            ? 0x8f
                                                       : 0xbf;
              if (next < low || next > high) break;
              ++n;
            }
            if (n == length)
              append({text.begin() + i, n});
            else
              append("\xef\xbf\xbd");
            i += n;
            continue;
          }
      }
      ++i;
    }
    append("\"");
  }

  std::string bytes;

 private:
  size_t limit;
  Deadline* deadline;
};

void value(Writer& out, Value cell) {
  KJ_SWITCH_ONEOF(cell) {
    KJ_CASE_ONEOF(blob, kj::ArrayPtr<const kj::byte>) {
      out.append("[");
      for (size_t i = 0; i < blob.size(); ++i) {
        if ((i & 1023) == 0) out.tick();
        if (i != 0) out.append(",");
        char digits[3];
        auto end = std::to_chars(digits, digits + sizeof(digits), unsigned(blob[i])).ptr;
        out.append({digits, size_t(end - digits)});
      }
      out.append("]");
    }
    KJ_CASE_ONEOF(text, kj::StringPtr) {
      out.string(text);
    }
    KJ_CASE_ONEOF(integer, int64_t) {
      out.append(number(static_cast<double>(integer)));
    }
    KJ_CASE_ONEOF(real, double) {
      out.append(number(real));
    }
    KJ_CASE_ONEOF(null, decltype(nullptr)) {
      out.append("null");
    }
  }
}

kj::String typeText(Json::Reader item) {
  if (item.isString()) return kj::str(item.getString());
  if (item.isBoolean()) return kj::str(item.getBoolean() ? "true" : "false");
  if (item.isNumber()) {
    const auto n = item.getNumber();
    if (std::isinf(n)) return kj::str(n < 0 ? "-Infinity" : "Infinity");
    return kj::str(number(n).c_str());
  }
  if (item.isNull()) return kj::str("null");
  if (item.isArray()) {
    kj::Vector<kj::String> parts;
    for (auto nested: item.getArray()) parts.add(nested.isNull() ? kj::str("") : typeText(nested));
    return kj::strArray(parts.asPtr(), ",");
  }
  return kj::str("[object Object]");
}

[[noreturn]] void typeError(Json::Reader item) {
  const auto kind = item.isString() ? "string"
      : item.isBoolean()            ? "boolean"
      : item.isNumber()             ? "number"
                                    : "object";
  throw SqlError{
    kj::str("D1_TYPE_ERROR: Type '", kind, "' not supported for value '", typeText(item), "'")};
}

uint32_t arrayIndex(kj::StringPtr name) {
  uint32_t index;
  constexpr auto NO_INDEX = std::numeric_limits<uint32_t>::max();
  if (name.size() == 0 || (name.size() > 1 && name[0] == '0')) return NO_INDEX;
  auto result = std::from_chars(name.begin(), name.end(), index);
  return result.ec == std::errc() && result.ptr == name.end() ? index : NO_INDEX;
}

struct Column {
  kj::StringPtr name;
  uint position;
  uint32_t arrayIndex;
};

std::vector<Column> columns(SqliteDatabase::Query& cursor, NativeSqlResults format) {
  std::vector<Column> result;
  for (uint i = 0; i < cursor.columnCount(); ++i) {
    auto name = cursor.getColumnName(i);
    if (format == NativeSqlResults::OBJECTS) {
      auto existing = std::find_if(
          result.begin(), result.end(), [&](const auto& column) { return column.name == name; });
      if (existing != result.end()) {
        existing->position = i;
        continue;
      }
    }
    result.push_back({name, i, arrayIndex(name)});
  }
  if (format == NativeSqlResults::OBJECTS) {
    std::stable_sort(result.begin(), result.end(),
        [](const auto& a, const auto& b) { return a.arrayIndex < b.arrayIndex; });
  }
  return result;
}

struct Counters {
  int64_t changes, lastRow, size;
};

Counters counters(SqliteDatabase& db) {
  auto result = db.run("SELECT total_changes(), last_insert_rowid(), "
                       "((SELECT page_count FROM pragma_page_count) - "
                       " (SELECT freelist_count FROM pragma_freelist_count)) * "
                       "(SELECT page_size FROM pragma_page_size)");
  return {result.getInt64(0), result.getInt64(1), result.getInt64(2)};
}

void meta(Writer& out, Counters before, Counters after, uint64_t rowsRead, uint64_t rowsWritten) {
  out.append(",\"meta\":{\"duration\":0,\"changes\":");
  out.append(number(double(after.changes) - double(before.changes)));
  out.append(",\"last_row_id\":");
  out.append(number(double(after.lastRow)));
  out.append(",\"changed_db\":");
  out.append(double(after.changes) != double(before.changes) ||
              double(after.lastRow) != double(before.lastRow) || after.size != before.size
          ? "true"
          : "false");
  out.append(",\"size_after\":");
  out.append(number(double(after.size)));
  out.append(",\"rows_read\":");
  out.append(number(double(rowsRead)));
  out.append(",\"rows_written\":");
  out.append(number(double(rowsWritten)));
  out.append("}}");
}

void encodeRows(Writer& out, SqliteDatabase::Query& cursor, NativeSqlResults format) {
  auto names = columns(cursor, format);
  const bool tabular = format == NativeSqlResults::COLUMNS;
  if (tabular) {
    out.append("{\"columns\":[");
    for (size_t i = 0; i < names.size(); ++i) {
      if (i != 0) out.append(",");
      out.string(names[i].name);
    }
    out.append("],\"rows\":");
  }
  out.append("[");
  bool first = true;
  while (!cursor.isDone()) {
    out.tick();
    if (!first) out.append(",");
    first = false;
    out.append(tabular ? "[" : "{");
    for (size_t i = 0; i < names.size(); ++i) {
      if (i != 0) out.append(",");
      if (!tabular) {
        out.string(names[i].name);
        out.append(":");
      }
      value(out, cursor.getValue(names[i].position));
    }
    out.append(tabular ? "]" : "}");
    cursor.nextRow();
  }
  out.append(tabular ? "]}" : "]");
}

void statement(SqliteDatabase& db, Json::Reader entry, Writer& out, NativeSqlResults format) {
  if (hasField(entry, "ingest"_kj)) {
    throw SqlError{kj::str("ingest is only accepted as a standalone /query object")};
  }
  auto sql = field(entry, "sql"_kj);
  if (!sql.isString()) throw SqlError{kj::str("a statement names its sql")};
  auto params = field(entry, "params"_kj);
  kj::Vector<Value> bindings;
  kj::Vector<kj::Array<kj::byte>> blobs;
  if (hasField(entry, "params"_kj)) {
    if (!params.isArray()) typeError(params);
    for (auto param: params.getArray()) {
      out.tick();
      switch (param.which()) {
        case Json::ARRAY: {
          auto source = param.getArray();
          // Keep a pointer to allocated storage even for a zero-byte blob.
          auto bytes = kj::heapArray<kj::byte>(kj::max(size_t(1), size_t(source.size())));
          size_t i = 0;
          for (auto byte: source) {
            if ((i & 1023) == 0) out.tick();
            if (!byte.isNumber()) typeError(param);
            const auto n = byte.getNumber();
            if (!(n >= 0 && n <= 255 && std::trunc(n) == n)) typeError(param);
            bytes[i++] = static_cast<kj::byte>(n);
          }
          bindings.add(kj::arrayPtr(static_cast<const kj::byte*>(bytes.begin()), i));
          blobs.add(kj::mv(bytes));
          break;
        }
        case Json::STRING:
          bindings.add(kj::StringPtr(param.getString()));
          break;
        case Json::NUMBER:
          bindings.add(param.getNumber());
          break;
        case Json::BOOLEAN:
          bindings.add(param.getBoolean() ? 1.0 : 0.0);
          break;
        case Json::NULL_:
          bindings.add(nullptr);
          break;
        default:
          typeError(param);
      }
    }
  }
  const auto before = counters(db);
  uint64_t rowsRead, rowsWritten;
  bool overflow = false;
  {
    auto cursor = db.run({.regulator = REGULATOR}, sql.getString(), bindings.asPtr().asConst());
    try {
      out.append("{\"success\":true,\"results\":");
      encodeRows(out, cursor, format);
    } catch (const ResultTooLarge&) {
      // A statement with RETURNING can still encounter SQLite errors. Drain its
      // cursor before reporting an encoding failure, as the JS path does.
      while (!cursor.isDone()) {
        out.tick();
        cursor.nextRow();
      }
      overflow = true;
    }
    rowsRead = cursor.getRowsRead();
    rowsWritten = cursor.getRowsWritten();
  }
  if (overflow) throw ResultTooLarge{};
  meta(out, before, counters(db), rowsRead, rowsWritten);
}

void ingest(SqliteDatabase& db, Json::Reader entry, Writer& out) {
  auto sql = field(entry, "ingest"_kj);
  if (!sql.isString() || hasField(entry, "sql"_kj) || hasField(entry, "params"_kj)) {
    throw SqlError{kj::str(
        "ingest must be a string, without sql or params, sent as a standalone /query object")};
  }
  const auto before = counters(db);
  auto result = db.ingestSql(REGULATOR, sql.getString());
  out.append("{\"success\":true,\"results\":[],\"ingest\":{\"remainder\":");
  out.string(result.remainder);
  out.append(",\"rowsRead\":");
  out.append(std::to_string(result.rowsRead));
  out.append(",\"rowsWritten\":");
  out.append(std::to_string(result.rowsWritten));
  out.append(",\"statementCount\":");
  out.append(std::to_string(result.statementCount));
  out.append("}");
  meta(out, before, counters(db), result.rowsRead, result.rowsWritten);
}

void failure(Writer& out, kj::StringPtr reason) {
  out.append("{\"success\":false,\"error\":");
  out.string(reason);
  out.append("}");
}

NativeSqlReply failed(kj::StringPtr reason, bool batch) {
  Writer out;
  if (batch) out.append("[");
  try {
    failure(out, reason);
    if (batch) out.append("]");
  } catch (const ResultTooLarge&) {
    return failed(RESULT_TOO_LARGE, batch);
  }
  return {200, std::move(out.bytes)};
}

// SQLite can already have rolled back an interrupted write by the time its
// cursor is destroyed. Roll back only a live transaction, with cancellation
// disabled so it can reach a clean boundary.
void rollback(SqliteDatabase& db, Deadline& deadline) {
  deadline.disable();
  if (sqlite3_get_autocommit(db) == 0) db.run("ROLLBACK");
}

}  // namespace

std::string nativeSqlFailure(kj::StringPtr code, kj::StringPtr reason) {
  Writer out;
  out.append("{\"code\":");
  out.string(code);
  out.append(",\"reason\":");
  out.string(reason);
  out.append(",\"error\":");
  // The D1 adapter reads the error string and may discard the other fields.
  out.string(kj::str(code, ": ", reason));
  out.append("}");
  return std::move(out.bytes);
}

NativeSqlReply runNativeSql(SqliteDatabase& db,
    kj::StringPtr body,
    bool query,
    NativeSqlResults format,
    Clock::time_point end,
    int connectionFd) {
  Deadline deadline(db, end, connectionFd);
  capnp::MallocMessageBuilder parsed;
  auto root = parsed.initRoot<Json>();
  bool batch = false;
  try {
    try {
      capnp::JsonCodec codec;
      auto normalized = normalizeJson(body.asArray(), deadline);
      codec.decodeRaw(normalized.asPtr(), root);
      batch = root.isArray();
    } catch (const kj::Exception&) {
      deadline.check();
      return {400, "{\"error\":\"the body of a statement is JSON\"}"};
    }
    auto input = root.asReader();
    Writer out(RESULT_LIMIT, &deadline);
    deadline.check();
    if (query) {
      db.run("BEGIN");
      if (batch) {
        out.append("[");
        bool first = true;
        for (auto entry: input.getArray()) {
          deadline.check();
          if (!first) out.append(",");
          first = false;
          statement(db, entry, out, format);
        }
        out.append("]");
      } else if (hasField(input, "ingest"_kj)) {
        ingest(db, input, out);
      } else {
        statement(db, input, out, format);
      }
      deadline.check();
      db.run("COMMIT");
    } else {
      auto execute = [&](Json::Reader entry, bool separator) {
        deadline.check();
        db.run("BEGIN");
        auto before = out.bytes.size();
        bool success = false;
        kj::String reason;
        try {
          if (separator) out.append(",");
          statement(db, entry, out, format);
          success = true;
        } catch (const SqlError& error) {
          reason = kj::str(error.message);
        } catch (const ResultTooLarge&) {
          reason = kj::str(RESULT_TOO_LARGE);
        } catch (const kj::Exception& error) {
          if (deadline.wasInterrupted() || db.observedCriticalError()) throw;
          reason = kj::str(error.getDescription());
        }
        // Preserve the prefix on an ordinary SQL or result error, including
        // earlier SQL statements within this entry. Timeout cancels this entry.
        deadline.check();
        db.run("COMMIT");
        if (!success) {
          out.bytes.resize(before);
          if (separator) out.append(",");
          failure(out, reason);
        }
        return success;
      };
      if (batch) {
        out.append("[");
        bool first = true;
        for (auto entry: input.getArray()) {
          if (!execute(entry, !first)) break;
          first = false;
        }
        out.append("]");
      } else {
        execute(input, false);
      }
    }
    return {200, std::move(out.bytes)};
  } catch (const Interrupted&) {
    rollback(db, deadline);
  } catch (const ResultTooLarge&) {
    rollback(db, deadline);
    return failed(RESULT_TOO_LARGE, batch);
  } catch (const SqlError& error) {
    rollback(db, deadline);
    if (!deadline.wasInterrupted()) return failed(error.message, batch);
  } catch (const kj::Exception& error) {
    rollback(db, deadline);
    if (!deadline.wasInterrupted()) return failed(error.getDescription(), batch);
  }
  if (deadline.clientGone()) return {0, {}};
  return {503,
    nativeSqlFailure("SQL_LIMIT_EXCEEDED"_kj, "SQL execution exceeded the 30-second deadline"_kj)};
}

}  // namespace workerd::server
