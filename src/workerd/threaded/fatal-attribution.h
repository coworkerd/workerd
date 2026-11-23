#pragma once

#include <kj/string.h>

#include <cstdio>

namespace workerd::threaded {

// Only the top scope can identify the isolate executing on this thread.
class FatalInvocation {
 public:
  FatalInvocation(const void* isolate, kj::StringPtr identity)
      : isolate(isolate),
        identity(identity),
        previous(current) {
    current = this;
  }
  ~FatalInvocation() {
    current = previous;
  }
  KJ_DISALLOW_COPY_AND_MOVE(FatalInvocation);

  static const FatalInvocation* forIsolate(const void* isolate) {
    return current != nullptr && isolate != nullptr && current->isolate == isolate ? current
                                                                                   : nullptr;
  }

  kj::StringPtr getIdentity() const {
    return identity;
  }

 private:
  const void* isolate;
  kj::StringPtr identity;
  const FatalInvocation* previous;
  inline static thread_local const FatalInvocation* current = nullptr;
};

enum class FatalHandlerRole { ORDINARY_OR_QUARANTINE, DURABLE };
using FatalHandler = void (*)(const char* location, int line, const char* message);

template <typename Registrar>
void registerProcessFatalHandler(
    FatalHandlerRole role, Registrar registerHandler, FatalHandler handler) {
  if (role == FatalHandlerRole::ORDINARY_OR_QUARANTINE) registerHandler(handler);
}

inline constexpr size_t FATAL_LOCATION_MAX_BYTES = 256;
inline constexpr size_t FATAL_MESSAGE_MAX_BYTES = 768;
inline constexpr size_t FATAL_DIAGNOSTIC_ROW_BYTES = 2304;

// Encode fixed-size byte prefixes for the JSON wire format. The node decodes
// and redacts them before logging. The crash callback allocates no heap memory.
template <size_t capacity>
void encodeDiagnosticPrefix(char (&out)[capacity], const char* text) {
  static constexpr char hex[] = "0123456789abcdef";
  size_t written = 0;
  if (text != nullptr) {
    for (size_t i = 0; i < (capacity - 1) / 2 && text[i] != '\0'; ++i) {
      const auto byte = static_cast<unsigned char>(text[i]);
      out[written++] = hex[byte >> 4];
      out[written++] = hex[byte & 15];
    }
  }
  out[written] = '\0';
}

inline size_t formatFatalDiagnostic(char (&row)[FATAL_DIAGNOSTIC_ROW_BYTES],
    int pid,
    const char* location,
    int line,
    const char* message) {
  if (location == nullptr && message == nullptr) return 0;
  char where[2 * FATAL_LOCATION_MAX_BYTES + 1];
  char what[2 * FATAL_MESSAGE_MAX_BYTES + 1];
  encodeDiagnosticPrefix(where, location);
  encodeDiagnosticPrefix(what, message);
  const auto size = snprintf(row, sizeof(row),
      "{\"event\":\"v8-fatal-diagnostic\",\"pid\":%d,\"fatalLocationHex\":\"%s\","
      "\"fatalLine\":%d,\"fatalMessageHex\":\"%s\"}\n",
      pid, where, line, what);
  return size > 0 && size_t(size) < sizeof(row) ? size_t(size) : 0;
}

}  // namespace workerd::threaded
