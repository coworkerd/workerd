#pragma once

#include <cctype>
#include <initializer_list>
#include <stdexcept>
#include <string_view>

namespace workerd::threaded {

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

enum class LoadStage { LOAD, PACKAGE, DECODE, EVALUATION };
struct LoadStageInfo {
  std::string_view stage, codeStage, category;
};
inline constexpr LoadStageInfo loadStageInfo(LoadStage stage) {
  switch (stage) {
    case LoadStage::LOAD:
      return {"load", "", "LOAD_FAILED"};
    case LoadStage::PACKAGE:
      return {"code", "package", "LOAD_FAILED"};
    case LoadStage::DECODE:
      return {"code", "decode", "LOAD_FAILED"};
    case LoadStage::EVALUATION:
      return {"evaluation", "", "TOP_LEVEL_THROW"};
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
