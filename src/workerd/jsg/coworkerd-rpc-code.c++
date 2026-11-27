#include "coworkerd-rpc-code.h"
#include "jsg.h"

namespace workerd::jsg {
void coworkerdSetRpcErrorCode(v8::Isolate* isolate, v8::Local<v8::Value> value,
    const kj::Exception& exception) {
  if (!value->IsObject()) return;
  auto object = value.As<v8::Object>();
  auto context = isolate->GetCurrentContext();
  if (exception.getDetail(COWORKERD_SQL_RETRY_DETAIL_ID) != kj::none) {
    check(object->Set(context, v8StrIntern(isolate, "dispatched"_kj), v8::False(isolate)));
    check(object->Set(context, v8StrIntern(isolate, "retryable"_kj), v8::True(isolate)));
  }
  KJ_IF_SOME(code, exception.getDetail(COWORKERD_CODE_DETAIL_ID)) {
    auto text = kj::heapString(code.asChars());
    check(object->Set(context, v8StrIntern(isolate, "code"_kj), v8Str(isolate, text)));
  }
}
}  // namespace workerd::jsg
