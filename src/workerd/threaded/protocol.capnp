@0xd1a2c8be79534f61;
using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("workerd::threaded::rpc");
using Worker = import "/workerd/io/worker-interface.capnp";

# The native loopback's resolved channels. The node copies the enclosing props
# without decoding JavaScript values or taking ownership of their implementation.
struct Channel {
  union {
    service @0 :Worker.WorkerdBootstrap;
    actorClass @1 :Text;
  }
}

using Props = import "/workerd/io/frankenvalue.capnp".Frankenvalue;
interface Bootstrap @0xa0c731b2df279451 extends(Worker.WorkerdBootstrap) {
  startNativeEvent @0 (cfBlobJson :Text, coworkerdProps :Props)
      -> (dispatcher :Worker.EventDispatcher);
}
