#pragma once

#include <workerd/io/observer.h>
#include <workerd/io/worker-interface.h>
#include <kj/async.h>
#include <kj/function.h>
#include <kj/timer.h>
#include <memory>

namespace workerd::server {
class IdleReaper;
struct IdleWorker;
using IdleWorkerRef = std::shared_ptr<IdleWorker>;

std::shared_ptr<IdleReaper> idleReaper(kj::Timer& timer);
IdleWorkerRef registerIdleWorker(std::shared_ptr<IdleReaper> reaper,
    kj::StringPtr name, kj::Function<void()> remove, bool eligible = true);
void idleWorkerReady(const IdleWorkerRef& worker);
void idleWorkerFailed(const IdleWorkerRef& worker);
void idleWorkerRemoved(const IdleWorkerRef& worker, kj::StringPtr reason);
void detachIdleWorker(const IdleWorkerRef& worker);
// The isolate's collections (coworkerd-gc.h), and how to collect it between requests (fix B): the collector returns
// none once the isolate's service is gone. The reaper collects with it only while the environment asks for idle
// collection.
class IsolateGc;
enum class IdleCollection;
using IdleCollector = kj::Function<kj::Maybe<kj::Promise<IdleCollection>>(
    std::shared_ptr<IsolateGc> gc, kj::Function<bool()> stillIdle)>;
void idleWorkerSetGc(const IdleWorkerRef& worker, std::shared_ptr<IsolateGc> gc);
void idleWorkerSetCollector(const IdleWorkerRef& worker, IdleCollector collect);
// A new event delivered to an actor of this isolate (the CPU limit's topUpActor()): the reaper sees an actor's events
// here, which never pass through the isolate's entry the way a request does -- each counts as the isolate's last use, so
// it is not evicted as idle while its actors receive events. An isolate that hosts actors is not collected between
// requests: that decision belongs with the Durable Objects' process group.
void idleWorkerActorEvent(const IdleWorkerRef& worker);
kj::Own<void> idleActivity(const IdleWorkerRef& worker);
kj::Own<IsolateObserver> idleIsolateObserver(kj::StringPtr name,
    bool dynamic, IdleWorkerRef worker);
kj::Own<WorkerInterface> wrapIdleWorkerInterface(
    kj::Own<WorkerInterface> inner, const IdleWorkerRef& worker);
}  // namespace workerd::server
