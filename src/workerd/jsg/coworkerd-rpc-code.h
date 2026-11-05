#pragma once

#include <kj/exception.h>
#include <kj/string.h>

namespace workerd::jsg {
// Assigned by the v76 RPC error-detail contract. Keep in sync with the node's
// common/rpc-error.h. The value is a platform code's bytes, without a trailing NUL.
inline constexpr kj::Exception::DetailTypeId COWORKERD_CODE_DETAIL_ID = 0xc6f4a09d8b3e2175ull;

inline kj::Exception tagCoworkerdRpcError(kj::Exception error, kj::StringPtr code) {
  error.setDetail(COWORKERD_CODE_DETAIL_ID, kj::heapArray(code.asBytes()));
  return error;
}
}  // namespace workerd::jsg
