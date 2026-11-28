#pragma once

#include <array>
#include <cctype>
#include <cstdint>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <string_view>

namespace workerd::threaded {

// A call loop owns its mailbox, event port and invocation channels. Leave room
// for probes, the platform and the node's prepared connections as well.
inline constexpr uint64_t descriptorBudget(uint32_t callThreads) {
  return uint64_t(callThreads) * 8 + 256;
}

inline constexpr uint32_t ORDINARY_MAX_IN_FLIGHT = 1u << 20;
inline constexpr uint64_t ORDINARY_FD_HEADROOM = 256;
inline constexpr uint64_t ORDINARY_FDS_PER_CALL = 8;
inline constexpr char ORDINARY_ADMISSION_PREFIX = 'O';
enum class OrdinaryLane { LEGACY, ROOT, NESTED };

enum class OrdinaryResource : char { MEMORY = 'M', DESCRIPTORS = 'F', THREADS = 'T', CEILING = 'C' };

struct OrdinaryRefusal {
  OrdinaryResource resource;
  uint64_t current = 0;
  uint64_t budget = 0;

  // Extended refusal on the private socket. The complete record is the proof
  // that workerd refused the call before giving it to Cap'n Proto.
  std::array<char, 18> encode() const {
    std::array<char, 18> reply{'R', static_cast<char>(resource)};
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
      const auto shift = (sizeof(uint64_t) - i - 1) * 8;
      reply[2 + i] = static_cast<char>(current >> shift);
      reply[10 + i] = static_cast<char>(budget >> shift);
    }
    return reply;
  }
};

// The existing trace budget is 256 KiB per event. A separate line bound also
// covers empty console calls, whose native objects still occupy memory.
struct InvocationLogBudget {
  static constexpr uint64_t MAX_BYTES = 256 * 1024, MAX_LINES = 1023;
  uint64_t bytes = 0, lines = 0;
  bool accept(uint64_t length) {
    if (lines == MAX_LINES || length > MAX_BYTES - bytes) return false;
    bytes += length;
    ++lines;
    return true;
  }
};

// A descriptor has already passed JSON decoding. These views retain the exact
// wire spelling of values that must be forwarded without another float conversion.
inline std::string_view trim(std::string_view value) {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())))
    value.remove_prefix(1);
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back())))
    value.remove_suffix(1);
  return value;
}
inline size_t jsonValueEnd(std::string_view value, size_t start) {
  bool quoted = false, escaped = false;
  unsigned depth = 0;
  for (size_t i = start; i < value.size(); ++i) {
    char c = value[i];
    if (quoted) {
      if (escaped)
        escaped = false;
      else if (c == '\\')
        escaped = true;
      else if (c == '"') {
        quoted = false;
        if (depth == 0) return i + 1;
      }
    } else if (c == '"')
      quoted = true;
    else if (c == '{' || c == '[')
      ++depth;
    else if (c == '}' || c == ']') {
      if (depth == 0) return i;
      if (--depth == 0) return i + 1;
    } else if (c == ',' && depth == 0)
      return i;
  }
  return value.size();
}
inline std::string_view rawField(std::string_view object, std::string_view name) {
  object = trim(object);
  if (object.empty() || object.front() != '{') throw std::invalid_argument("expected JSON object");
  size_t at = 1;
  while (at < object.size()) {
    while (at < object.size() && std::isspace(static_cast<unsigned char>(object[at]))) ++at;
    if (at == object.size()) break;
    if (object[at] == '}') break;
    if (object[at] != '"') throw std::invalid_argument("expected JSON field");
    auto keyEnd = jsonValueEnd(object, at);
    auto key = object.substr(at + 1, keyEnd - at - 2);
    at = keyEnd;
    while (at < object.size() && std::isspace(static_cast<unsigned char>(object[at]))) ++at;
    if (at == object.size() || object[at++] != ':')
      throw std::invalid_argument("expected JSON colon");
    while (at < object.size() && std::isspace(static_cast<unsigned char>(object[at]))) ++at;
    auto end = jsonValueEnd(object, at);
    if (key == name) return trim(object.substr(at, end - at));
    at = end;
    while (at < object.size() && std::isspace(static_cast<unsigned char>(object[at]))) ++at;
    if (at < object.size() && object[at] == ',')
      ++at;
    else
      break;
  }
  throw std::invalid_argument("missing descriptor field");
}
inline std::string_view rawPath(
    std::string_view value, std::initializer_list<std::string_view> path) {
  for (auto name: path) value = rawField(value, name);
  return value;
}

inline std::string normalizeEntrypoint(std::string_view format, std::string_view entrypoint) {
  // A legacy service-worker script has only its global fetch handler. Named
  // HTTP bindings historically select that handler as well.
  return format == "v1" || entrypoint.empty() ? "default" : std::string(entrypoint);
}

inline std::string childOrderSequence(
    bool required, std::string_view inherited, uint64_t generated) {
  if (!required) return "0";
  return inherited.empty() || inherited == "0" || inherited == "\"0\"" ? std::to_string(generated)
                                                                       : std::string(inherited);
}

enum class LoadStage { LOAD, PACKAGE, DECODE, EVALUATION };
struct LoadStageInfo {
  std::string_view stage, codeStage, category, name;
};
inline constexpr LoadStageInfo loadStageInfo(LoadStage stage) {
  switch (stage) {
    case LoadStage::LOAD:
      return {"bridge", "", "LOAD_FAILED", "Error"};
    case LoadStage::PACKAGE:
      return {"code", "package", "LOAD_FAILED", "Error"};
    case LoadStage::DECODE:
      return {"code", "decode", "LOAD_FAILED", "SyntaxError"};
    case LoadStage::EVALUATION:
      return {"evaluation", "", "TOP_LEVEL_THROW", "Error"};
  }
  throw std::invalid_argument("unknown load stage");
}

enum class Refusal { UNSUPPORTED, CPU, MEMORY, OVERLOADED, DRAINING };
struct RefusalInfo {
  unsigned status;
  std::string_view code, message;
};
inline constexpr RefusalInfo refusalInfo(Refusal reason) {
  switch (reason) {
    case Refusal::UNSUPPORTED:
      return {501, "NOT_SUPPORTED", "operation is not supported"};
    case Refusal::CPU:
      return {503, "CPU_LIMIT_EXCEEDED", "CPU_LIMIT_EXCEEDED"};
    case Refusal::MEMORY:
      return {503, "MEMORY_LIMIT_EXCEEDED", "MEMORY_LIMIT_EXCEEDED"};
    case Refusal::OVERLOADED:
      return {503, "OVERLOADED", "process admission budget is full"};
    case Refusal::DRAINING:
      return {503, "PROCESS_DRAINING", "process is draining"};
  }
  throw std::invalid_argument("unknown platform refusal");
}
}  // namespace workerd::threaded
