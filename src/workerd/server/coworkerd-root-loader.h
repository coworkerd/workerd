// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <workerd/api/http.h>

namespace workerd::server {

// Only the platform runtime receives this capability. The selected service is fixed by config.
class CoworkerdRootLoader final: public api::Fetcher {
 public:
  CoworkerdRootLoader(uint channel, kj::String service)
      : Fetcher(channel, RequiresHostAndProtocol::YES), rootChannel(channel), service(kj::mv(service)) {}

  jsg::Promise<jsg::Ref<api::Fetcher>> getRoot(jsg::Lock& js, kj::String selectionJson);
  jsg::Ref<api::Fetcher> getPlain(jsg::Lock& js, kj::String selectionJson);

  JSG_RESOURCE_TYPE(CoworkerdRootLoader) {
    JSG_INHERIT(api::Fetcher);
    JSG_METHOD(getRoot);
    JSG_METHOD(getPlain);
  }

 private:
  uint rootChannel;
  kj::String service;
};

}  // namespace workerd::server
