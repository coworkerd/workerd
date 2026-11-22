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

# Private node transport for hibernatable sockets. The socket ID names the
# original upgrade. The authority changes when the object opens on a new home.
struct ThreadedSocketAuthority @0xab062bc88f9558c5 {
  socketId @0 :Text;
  invocationId @1 :Text;
  dataEpoch @2 :UInt64;
  shardEpoch @3 :UInt64;
  generation @4 :UInt64;
}

struct ThreadedSocketFrame @0xba48661ff14acb08 {
  union {
    text @0 :Text;
    binary @1 :Data;
    close :group {
      code @2 :UInt16;
      reason @3 :Text;
    }
  }
}

struct ThreadedSocketSnapshot @0x9e4cd226c6cf0c04 {
  authority @0 :ThreadedSocketAuthority;
  lastFrame @1 :UInt64;
  tags @2 :List(Text);
  hasAttachment @3 :Bool;
  attachment @4 :Data;
  autoResponseUnixMs @5 :UInt64;
  closedOutgoing @6 :Bool;
  autoResponseEnabled @7 :Bool;
  autoRequest @8 :Text;
  autoResponse @9 :Text;
}

struct ThreadedSocketOpen @0x807618c69c531b73 {
  namespaceId @0 :Text;
  objectId @1 :Text;
  invocationId @2 :Text;
  dataEpoch @3 :UInt64;
  shardEpoch @4 :UInt64;
  generation @5 :UInt64;
  walOpenId @6 :UInt64;
}

interface ThreadedSocketControl @0xdc432f018090bcc4 {
  send @0 (authority :ThreadedSocketAuthority, frame :ThreadedSocketFrame) -> () $Cxx.allowCancellation;
  setAutoResponse @1 (authority :ThreadedSocketAuthority, enabled :Bool, request :Text, response :Text) -> (accepted :Bool);
  park @2 (open :ThreadedSocketOpen, sockets :List(ThreadedSocketSnapshot)) -> (accepted :Bool);
  adopt @3 (open :ThreadedSocketOpen) -> (sockets :List(ThreadedSocketSnapshot));
}

interface Bootstrap @0xa0c731b2df279451 extends(Worker.WorkerdBootstrap) {
  startNativeEvent @0 (cfBlobJson :Text, coworkerdProps :Props)
      -> (dispatcher :Worker.EventDispatcher);
  socketControl @1 () -> (control :ThreadedSocketControl);
}
