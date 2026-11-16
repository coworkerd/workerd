// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "coworkerd-root-loader.h"

#include <workerd/io/frankenvalue.h>
#include <workerd/io/io-context.h>
#include <workerd/io/worker-interface.h>

namespace workerd::server {
namespace {

// A process-local selected root, using the existing bootstrap capability protocol.
class RootBootstrapChannel final: public IoChannelFactory::SubrequestChannel {
 public:
  RootBootstrapChannel(rpc::WorkerdBootstrap::Client bootstrap,
      capnp::HttpOverCapnpFactory& httpFactory,
      capnp::ByteStreamFactory& byteFactory)
      : bootstrap(kj::mv(bootstrap)), httpFactory(httpFactory), byteFactory(byteFactory) {}

  kj::Own<WorkerInterface> startRequest(IoChannelFactory::SubrequestMetadata metadata) override {
    auto request = bootstrap.startEventRequest();
    KJ_IF_SOME(cf, metadata.cfBlobJson) {
      request.setCfBlobJson(cf);
    }
    return kj::heap<RpcWorkerInterface>(httpFactory, byteFactory,
        getUnsupportedFrankenvalueHandler(), request.send().getDispatcher());
  }

  void requireAllowsTransfer() override {
    JSG_FAIL_REQUIRE(Error, "Private root capabilities cannot be transferred");
  }

  kj::OneOf<kj::Array<byte>, kj::Promise<kj::Array<byte>>> getTokenMaybeSync(
      IoChannelFactory::ChannelTokenUsage usage) override {
    JSG_FAIL_REQUIRE(Error, "Private root capabilities cannot be transferred");
  }

 private:
  rpc::WorkerdBootstrap::Client bootstrap;
  capnp::HttpOverCapnpFactory& httpFactory;
  capnp::ByteStreamFactory& byteFactory;
};

}  // namespace

jsg::Ref<api::Fetcher> CoworkerdRootLoader::getPlain(
    jsg::Lock& js, kj::String selectionJson) {
  auto& context = IoContext::current();
  auto channel = context.getIoChannelFactory().getSubrequestChannel(rootChannel,
      Frankenvalue::fromJson(kj::str("{\"plainSelection\":", selectionJson, "}")));
  return js.alloc<api::Fetcher>(
      context.addObject(kj::mv(channel)), api::Fetcher::RequiresHostAndProtocol::NO);
}

jsg::Promise<jsg::Ref<api::Fetcher>> CoworkerdRootLoader::getRoot(
    jsg::Lock& js, kj::String selectionJson) {
  auto& context = IoContext::current();
  auto request = context.getIoChannelFactory().getWorkerdDebugPort().getEntrypointRequest();
  request.setService(service);
  Frankenvalue::fromJson(kj::mv(selectionJson)).toCapnp(request.initProps());
  // Pin a validated plan before exposing a Fetcher; the object is selected when its event starts.
  return context.awaitIo(js, request.send(),
      [](jsg::Lock& js, capnp::Response<rpc::WorkerdDebugPort::GetEntrypointResults> response)
          -> jsg::Ref<api::Fetcher> {
    auto& context = IoContext::current();
    kj::Rc<IoChannelFactory::SubrequestChannel> channel = kj::rc<RootBootstrapChannel>(
        response.getEntrypoint(), context.getHttpOverCapnpFactory(), context.getByteStreamFactory());
    return js.alloc<api::Fetcher>(
        context.addObject(kj::mv(channel)), api::Fetcher::RequiresHostAndProtocol::NO);
  });
}

}  // namespace workerd::server
