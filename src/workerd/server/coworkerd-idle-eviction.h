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
kj::Own<void> idleActivity(const IdleWorkerRef& worker);
kj::Own<IsolateObserver> idleIsolateObserver(kj::StringPtr name,
    bool dynamic, IdleWorkerRef worker);
kj::Own<WorkerInterface> wrapIdleWorkerInterface(
    kj::Own<WorkerInterface> inner, const IdleWorkerRef& worker);
}  // namespace workerd::server
