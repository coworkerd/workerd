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
  facetId @10 :UInt32;
}

const threadedSocketFacetMaxWords :UInt32 = 8192;  # 64 KiB
const threadedSocketTreeMaxWords :UInt32 = 131072;  # 1 MiB

struct ThreadedSocketFacet @0xdcd0f9f40ff655bf {
  id @0 :UInt32;
  parent @1 :UInt32;
  name @2 :Text;
  className @3 :Text;
  actorId @4 :Text;
  namespaceId @5 :Text;  # Empty for a string actor ID.
  hasActorName @6 :Bool;
  actorName @7 :Text;
  props @8 :Props;
}

# Resolved local Fetchers in a facet's props. Rebind them to the new event
# instead of exporting capabilities that depend on the retired home.
struct ThreadedSocketPropChannel {
  channel @0 :UInt32;
  hasObject @1 :Bool;
  objectId @2 :Text;
  props @3 :Props;
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
  park @2 (open :ThreadedSocketOpen, sockets :List(ThreadedSocketSnapshot), attempt :UInt64,
      facets :List(ThreadedSocketFacet))
      -> (accepted :Bool) $Cxx.allowCancellation;
  adopt @3 (open :ThreadedSocketOpen, attempt :UInt64, resolveOnly :Bool)
      -> (sockets :List(ThreadedSocketSnapshot), accepted :Bool, facets :List(ThreadedSocketFacet))
      $Cxx.allowCancellation;
  resolvePark @4 (open :ThreadedSocketOpen, attempt :UInt64) -> (accepted :Bool);
  closeAborted @5 (open :ThreadedSocketOpen, socketIds :List(Text)) -> () $Cxx.allowCancellation;
}

interface Bootstrap @0xa0c731b2df279451 extends(Worker.WorkerdBootstrap) {
  startNativeEvent @0 (cfBlobJson :Text, coworkerdProps :Props)
      -> (dispatcher :Worker.EventDispatcher);
  socketControl @1 () -> (control :ThreadedSocketControl);
  startLeasedEvent @2 (cfBlobJson :Text, coworkerdProps :Props, lease :NativeLeaseControl)
      -> (dispatcher :Worker.EventDispatcher);
}

interface NativeLeaseControl @0xbbb54ed58665162a $Cxx.allowCancellation {
  whenEnded @0 () -> ();
}
