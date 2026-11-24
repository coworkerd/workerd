#include "fatal-attribution.h"

#include <kj/test.h>

namespace workerd::threaded {
namespace {

KJ_TEST("fatal diagnostics preserve short text and cap both byte prefixes") {
  char row[FATAL_DIAGNOSTIC_ROW_BYTES];
  KJ_EXPECT(formatFatalDiagnostic(row, 1, nullptr, 0, nullptr) == 0);
  KJ_EXPECT(formatFatalDiagnostic(row, 1, "x", 42, "a\"\n") > 0);
  KJ_EXPECT(kj::StringPtr(row) ==
      "{\"event\":\"v8-fatal-diagnostic\",\"pid\":1,\"fatalLocationHex\":\"78\","
      "\"fatalLine\":42,\"fatalMessageHex\":\"61220a\"}\n");

  auto location = kj::str(kj::repeat('f', 600), '!');
  auto message = kj::str(kj::repeat('w', 1600), '!');
  const auto size = formatFatalDiagnostic(row, 123, location.cStr(), -2147483647, message.cStr());
  const auto expected =
      kj::str("{\"event\":\"v8-fatal-diagnostic\",\"pid\":123,\"fatalLocationHex\":\"",
          kj::repeat('6', 2 * FATAL_LOCATION_MAX_BYTES),
          "\",\"fatalLine\":-2147483647,\"fatalMessageHex\":\"",
          kj::repeat('7', 2 * FATAL_MESSAGE_MAX_BYTES), "\"}\n");
  KJ_EXPECT(size == expected.size());
  KJ_EXPECT(size < FATAL_DIAGNOSTIC_ROW_BYTES);
  KJ_EXPECT(kj::StringPtr(row) == expected);
}

KJ_TEST("successful calls enter and restore nested invocation identities") {
  struct IsolateToken {};
  const IsolateToken first, second;
  auto identity = [](const IsolateToken* current) -> kj::StringPtr {
    if (const auto* scope = FatalInvocation::forIsolate(current)) return scope->getIdentity();
    return "";
  };
  auto call = [&](const IsolateToken& entered, kj::StringPtr invocation, auto run) {
    const FatalInvocation scope(&entered, invocation);
    return run();
  };

  KJ_EXPECT(identity(&first) == "");
  auto result = call(first, "outer", [&] {
    KJ_EXPECT(identity(&first) == "outer");
    KJ_EXPECT(identity(&second) == "");
    KJ_EXPECT(identity(nullptr) == "");
    const auto sameIsolate = call(first, "inner", [&] {
      KJ_EXPECT(identity(&first) == "inner");
      return 40;
    });
    KJ_EXPECT(identity(&first) == "outer");
    const auto differentIsolate = call(second, "other", [&] {
      KJ_EXPECT(identity(&second) == "other");
      KJ_EXPECT(identity(&first) == "");
      return 2;
    });
    KJ_EXPECT(identity(&first) == "outer");
    KJ_EXPECT(identity(&second) == "");
    return sameIsolate + differentIsolate;
  });
  KJ_EXPECT(result == 42);
  KJ_EXPECT(identity(&first) == "");
  KJ_EXPECT(identity(&second) == "");
}

KJ_TEST("the process registration seam forwards the handler only for ordinary and quarantine") {
  struct MockRegistrar {
    FatalHandler callback = nullptr;
    unsigned registrations = 0;

    void accept(FatalHandler value) {
      ++registrations;
      callback = value;
    }
  } mock;
  auto registerHandler = [&](FatalHandler callback) { mock.accept(callback); };
  auto record = +[](const char* location, int line, const char* message) {
    char row[FATAL_DIAGNOSTIC_ROW_BYTES];
    KJ_EXPECT(formatFatalDiagnostic(row, 2, location, line, message) > 0);
    KJ_EXPECT(kj::StringPtr(row).contains("647269766572"_kj));
  };

  registerProcessFatalHandler(FatalHandlerRole::DURABLE, registerHandler, record);
  KJ_EXPECT(mock.registrations == 0 && mock.callback == nullptr);
  registerProcessFatalHandler(FatalHandlerRole::ORDINARY_OR_QUARANTINE, registerHandler, record);
  KJ_REQUIRE(mock.registrations == 1 && mock.callback == record);
  mock.callback("driver", 10, "ordinary message");
}

}  // namespace
}  // namespace workerd::threaded
