#include "fatal-attribution.h"

#include <workerd/jsg/setup.h>

#include <kj/test.h>

namespace workerd::threaded {
namespace {

KJ_TEST("successful V8 calls restore their invocation identity across isolate entry") {
  jsg::V8System v8;
  v8::Isolate::CreateParams params;
  params.array_buffer_allocator_shared.reset(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
  auto* first = v8::Isolate::New(params);
  KJ_DEFER(first->Dispose());
  auto* second = v8::Isolate::New(params);
  KJ_DEFER(second->Dispose());

  auto current = [] { return FatalInvocation::forIsolate(v8::Isolate::TryGetCurrent()); };
  auto compute = [](v8::Isolate* isolate, const char* text) {
    const auto context = isolate->GetCurrentContext();
    v8::Local<v8::String> source;
    KJ_REQUIRE(v8::String::NewFromUtf8(isolate, text).ToLocal(&source));
    v8::Local<v8::Script> script;
    KJ_REQUIRE(v8::Script::Compile(context, source).ToLocal(&script));
    v8::Local<v8::Value> value;
    KJ_REQUIRE(script->Run(context).ToLocal(&value));
    int32_t result;
    KJ_REQUIRE(value->Int32Value(context).To(&result));
    return result;
  };

  KJ_EXPECT(current() == nullptr);
  {
    v8::Locker locker(first);
    v8::Isolate::Scope entered(first);
    v8::HandleScope handles(first);
    v8::Context::Scope context(v8::Context::New(first));
    FatalInvocation outer(first, "outer");
    KJ_EXPECT(current() == &outer);
    KJ_EXPECT(compute(first, "6 * 7") == 42);
    {
      FatalInvocation nested(first, "nested");
      KJ_EXPECT(current() == &nested);
      KJ_EXPECT(compute(first, "19 + 23") == 42);
    }
    KJ_EXPECT(current() == &outer);
    {
      v8::Locker otherLocker(second);
      v8::Isolate::Scope otherEntered(second);
      KJ_REQUIRE(v8::Isolate::TryGetCurrent() == second);
      KJ_EXPECT(current() == nullptr);
      v8::HandleScope otherHandles(second);
      v8::Context::Scope otherContext(v8::Context::New(second));
      FatalInvocation other(second, "other");
      KJ_EXPECT(current() == &other);
      KJ_EXPECT(compute(second, "84 / 2") == 42);
    }
    KJ_EXPECT(current() == &outer);
    KJ_EXPECT(compute(first, "43 - 1") == 42);
  }
  KJ_EXPECT(current() == nullptr);
}

}  // namespace
}  // namespace workerd::threaded
