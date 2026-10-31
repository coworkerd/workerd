#include "coworkerd-rpc-code.h"
#include "jsg.h"

namespace workerd::jsg {
void coworkerdSetRpcErrorCode(v8::Isolate* isolate, v8::Local<v8::Value> value,
    const kj::Exception& exception) {
  if (!value->IsObject()) return;
  KJ_IF_SOME(code, exception.getDetail(COWORKERD_CODE_DETAIL_ID)) {
    auto text = kj::heapString(code.asChars());
    check(value.As<v8::Object>()->Set(isolate->GetCurrentContext(),
        v8StrIntern(isolate, "code"_kj), v8Str(isolate, text)));
  }
}
}  // namespace workerd::jsg
