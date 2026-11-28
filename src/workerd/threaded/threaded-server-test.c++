// Test the production implementation, including its private ownership types.
// The executable's main is renamed; KJ supplies this target's test main.
#define main coworkerdThreadedExecutableMain
#include "threaded-server.c++"
#undef main

#include <workerd/server/factory/coworkerd-wal-wire.h>

#include <spawn.h>
#include <sys/wait.h>

#include <kj/encoding.h>
#include <kj/test.h>
#include <kj/timer.h>

#include <barrier>
#include <future>

extern char** environ;

namespace workerd {
namespace {

struct TestPlatform {
  kj::Own<v8::Platform> base = jsg::defaultPlatform(2);
  server::WorkerdPlatform platform{*base};
  jsg::V8System system{platform, {}, base.get()};
};
jsg::V8System& initializeV8() {
  static auto* platform = new TestPlatform;
  return platform->system;
}
// A group of its own for a test isolate, as the server creates one per Worker.
v8::IsolateGroup newGroup() {
  initializeV8();
  return KJ_ASSERT_NONNULL(server::tryNewIsolateGroup());
}
// customizeIsolate() runs with the isolate entered, as Worker::Isolate and sourceJsonFailure do.
void customizeEntered(IsolateLimits& limits, v8::Isolate* isolate) {
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  limits.customizeIsolate(isolate);
}

KJ_TEST("a stateless isolate starts idle after loading and counts its next HTTP request") {
  struct BusyClock final: kj::MonotonicClock {
    kj::TimePoint now() const override {
      const auto reading = kj::origin<kj::TimePoint>() + time;
      time += advanceAfterRead;
      advanceAfterRead = 0 * kj::MILLISECONDS;
      return reading;
    }

    mutable kj::Duration time = 0 * kj::MILLISECONDS;
    mutable kj::Duration advanceAfterRead = 2850 * kj::MILLISECONDS;
  } clock;

  constexpr auto source = "export default { fetch() { return new Response('served'); } };"_kj;
  const auto hash = digest(source.asBytes());
  auto descriptor = kj::str(R"({
    "workerId":"test/busy","versionId":"v1","format":"v1","definitionHash":"definition",
    "invocationId":"request","processId":"process","processGeneration":1,"lease":null,
    "mainModule":"index.js","compatibilityDate":"2026-11-01","contentHash":")",
      hash.c_str(), R"(","rootPlan":{"kind":"tenant","namespaces":[]},
    "rootSelection":{"kind":"plain","dispatchChecked":true}
  })");
  JsonDoc input(descriptor);
  auto sourceReply = kj::str(R"({"workerId":"test/busy","versionId":"v1","contentHash":")",
      hash.c_str(), R"(","source":")", source, R"("})");
  ThreadState state;
  struct SourceService final: kj::HttpService {
    SourceService(const kj::HttpHeaderTable& headers, kj::StringPtr reply)
        : headers(headers),
          reply(reply) {}

    kj::Promise<void> request(kj::HttpMethod method,
        kj::StringPtr url,
        const kj::HttpHeaders&,
        kj::AsyncInputStream&,
        Response& response) override {
      KJ_EXPECT(method == kj::HttpMethod::GET && url.contains("/v1/source?"));
      auto output = response.send(200, "OK", kj::HttpHeaders(headers), reply.size());
      co_await output->write(reply.asBytes());
    }

    const kj::HttpHeaderTable& headers;
    kj::StringPtr reply;
  } sourceService(*state.headers, sourceReply);

  auto& waitScope = state.io.waitScope;
  auto listener =
      state.io.provider->getNetwork().parseAddress("127.0.0.1", 0).wait(waitScope)->listen();
  kj::HttpServer sourceServer(state.io.provider->getTimer(), *state.headers, sourceService);
  auto listening = sourceServer.listenHttp(*listener).eagerlyEvaluate(nullptr);
  ThreadedServer server{
    4, kj::str("127.0.0.1:", listener->getPort()).cStr(), "", -1, {}, {}, &initializeV8(), clock};

  bool coldLoaded = false;
  auto slot = server.getWorker(input.get(), state, coldLoaded).wait(waitScope);
  KJ_REQUIRE(coldLoaded && slot->complete && slot->worker != kj::none);
  KJ_EXPECT(clock.time == 2850 * kj::MILLISECONDS);
  KJ_EXPECT(slot->contexts.load() == 0);
  auto& window = KJ_ASSERT_NONNULL(slot->busy)->window;
  const auto ready = window.sample();
  KJ_EXPECT(ready.throughNs == 2800000000);
  KJ_EXPECT(ready.busyNs == 0);
  KJ_EXPECT(ready.permille() == 0);

  // The first lock taken while serving the request lasts 200 ms on the controlled clock.
  clock.time = 2875 * kj::MILLISECONDS;
  clock.advanceAfterRead = 200 * kj::MILLISECONDS;
  Invocation invocation(server, state, kj::mv(descriptor), Frankenvalue(), {});
  auto client = kj::newHttpClient(invocation);
  auto response =
      client->request(kj::HttpMethod::GET, "http://test/busy", kj::HttpHeaders(*state.headers))
          .response.wait(waitScope);
  KJ_EXPECT(response.statusCode == 200);
  KJ_EXPECT(response.body->readAllText().wait(waitScope) == "served");
  KJ_EXPECT(server.compilations.load() == 1);
  KJ_EXPECT(clock.time == 3075 * kj::MILLISECONDS);
  clock.time = 3100 * kj::MILLISECONDS;
  const auto served = window.sample();
  KJ_EXPECT(served.busyNs == 200000000);
  KJ_EXPECT(served.permille() == 66);
}

KJ_TEST("platform durable plans pin the class and copy to the tenant") {
  JsonDoc input(kj::str(R"({"workerId":"alpha/:platform","format":"v2","versionId":"build-a",
      "definitionHash":"definition","contentHash":"package","cpuLimitMs":200,"memoryLimitMiB":64})"));
  JsonDoc otherTenant(kj::str(R"({"workerId":"beta/:platform","format":"v2","versionId":"build-a",
      "definitionHash":"definition","contentHash":"package","cpuLimitMs":200,"memoryLimitMiB":64})"));
  JsonDoc databaseWorker(kj::str(R"({"workerId":"alpha/__databases","format":"v2","versionId":"build-a",
      "definitionHash":"definition","contentHash":"package","cpuLimitMs":200,"memoryLimitMiB":64})"));
  JsonDoc bound(kj::str(R"({"workerId":"alpha/:platform","format":"v2","versionId":"build-a",
      "definitionHash":"definition","contentHash":"package","cpuLimitMs":200,"memoryLimitMiB":64,
      "variables":[{"name":"TENANT_SECRET","value":"fixture"}]})"));
  JsonDoc alpha(kj::str(R"({"tenant":"alpha","definitionName":"coworkerd:platform-test"})"));
  JsonDoc beta(kj::str(R"({"tenant":"beta","definitionName":"coworkerd:platform-test"})"));
  JsonDoc selection(kj::str(R"({"className":"Store"})"));
  JsonDoc otherClass(kj::str(R"({"className":"Other"})"));
  DurableRoute route{};
  route.open.className = "Store";
  route.open.definition = platformCopyKey(input.get(), alpha.get());
  KJ_EXPECT(platformDurablePlan(input.get(), alpha.get(), selection.get(), route));
  KJ_EXPECT(!platformDurablePlan(databaseWorker.get(), alpha.get(), selection.get(), route));
  KJ_EXPECT(platformCopyKey(otherTenant.get(), beta.get()) != route.open.definition);
  KJ_EXPECT(!platformDurablePlan(otherTenant.get(), beta.get(), selection.get(), route));
  KJ_EXPECT(!platformDurablePlan(input.get(), beta.get(), selection.get(), route));
  KJ_EXPECT(!platformDurablePlan(input.get(), alpha.get(), otherClass.get(), route));
  KJ_EXPECT(!platformDurablePlan(bound.get(), alpha.get(), selection.get(), route));
  route.open.definition = "wrong pin";
  KJ_EXPECT(!platformDurablePlan(input.get(), alpha.get(), selection.get(), route));
}

KJ_TEST("platform databases use the database Worker and a copy pinned to the specific object") {
  JsonDoc input(kj::str(R"({"workerId":"alpha/__databases","format":"v2","versionId":"build-a",
      "definitionHash":"definition","contentHash":"package","cpuLimitMs":200,"memoryLimitMiB":64,
      "durableObject":{"database":"first"}})"));
  JsonDoc platformWorker(kj::str(R"({"workerId":"alpha/:platform","format":"v2","versionId":"build-a",
      "definitionHash":"definition","contentHash":"package","cpuLimitMs":200,"memoryLimitMiB":64,
      "durableObject":{"database":"first"}})"));
  JsonDoc plan(kj::str(R"({"kind":"platformDurable","definitionName":"coworkerd:platform","tenant":"alpha"})"));
  JsonDoc otherTenant(kj::str(R"({"kind":"platformDurable","definitionName":"coworkerd:platform","tenant":"beta"})"));
  JsonDoc fixture(kj::str(R"({"kind":"platformDurable","definitionName":"coworkerd:platform-test","tenant":"alpha"})"));
  JsonDoc selection(kj::str(R"({"className":"SqlDatabase"})"));
  DurableRoute route{};
  route.key = {"namespace-one", "object-one"};
  route.open.className = "SqlDatabase";
  route.open.definition = platformCopyKey(input.get(), plan.get());
  KJ_EXPECT(!platformDurablePlan(input.get(), plan.get(), selection.get(), route));
  route.open.definition = platformCopyKey(input.get(), plan.get(), &route.key);
  KJ_EXPECT(platformDurablePlan(input.get(), plan.get(), selection.get(), route));
  KJ_EXPECT(!platformDurablePlan(platformWorker.get(), plan.get(), selection.get(), route));
  route.open.definition = platformCopyKey(input.get(), otherTenant.get(), &route.key);
  KJ_EXPECT(!platformDurablePlan(input.get(), otherTenant.get(), selection.get(), route));
  route.open.definition = platformCopyKey(input.get(), fixture.get(), &route.key);
  KJ_EXPECT(!platformDurablePlan(input.get(), fixture.get(), selection.get(), route));
  route.open.definition = platformCopyKey(input.get(), plan.get(), &route.key);
  route.key.namespaceId = "namespace-two";
  KJ_EXPECT(!platformDurablePlan(input.get(), plan.get(), selection.get(), route));
  route.key = {"namespace-one", "object-two"};
  KJ_EXPECT(!platformDurablePlan(input.get(), plan.get(), selection.get(), route));
}

KJ_TEST("an exclusive database copy keeps its isolate while the home has a reservation") {
  const auto database = PLATFORM_DATABASE_COPY_PREFIX + std::string(64, 'a');
  const auto tenant = std::string(64, 'a') + ":" + std::string(64, 'b');
  KJ_EXPECT(!shouldReleaseDurableCopy(1, false, database, false));
  KJ_EXPECT(!shouldReleaseDurableCopy(0, true, database, false));
  KJ_EXPECT(shouldReleaseDurableCopy(0, false, database, false));
  KJ_EXPECT(!shouldReleaseDurableCopy(0, false, tenant, false));
  KJ_EXPECT(shouldReleaseDurableCopy(0, false, tenant, true));
}

KJ_TEST("above the admission ceiling a database replaces only a sufficient charge within the hard budget") {
  constexpr auto MiB = ResourceBudget::MiB;
  const auto budget = std::make_shared<ResourceBudget>(128 * MiB, 10, 4,
      ResourceBudget::ReclaimMode::NONE);
  auto idleDatabase = std::make_shared<ResourceReservation>(budget, 64, true);
  ResourceReservation ordinary(budget, 64);
  ResourceReservation otherTenant(budget, 64);
  ResourceReservation incoming(budget, 64, true);
  KJ_REQUIRE(idleDatabase->registerPreparation());
  KJ_REQUIRE(ordinary.registerPreparation());
  const auto aboveAdmission = budget->admissionMaximum + 1;
  KJ_REQUIRE(ordinary.allocateExternal(aboveAdmission - budget->memoryReserved.load()));
  KJ_EXPECT(!incoming.registerPreparation());
  KJ_EXPECT(!budget->replacementCeiling(
      ResourceBudget::PREPARATION_MEMORY - 1, ResourceBudget::PREPARATION_HEAP));
  const auto ceiling = idleDatabase->replacementCeiling();
  KJ_REQUIRE(ceiling == aboveAdmission);
  DatabasePreparationReplacement replacement{*ceiling};
  idleDatabase.reset();
  KJ_EXPECT(!otherTenant.registerPreparation(&replacement));
  KJ_EXPECT(!incoming.registerPreparation());
  KJ_REQUIRE(incoming.registerPreparation(&replacement));
  KJ_EXPECT(budget->memoryReserved.load() == aboveAdmission);
  KJ_EXPECT(budget->residents.load() == 2);

  // A measured increase can take total reserved memory above the hard ceiling.
  KJ_EXPECT(!budget->change(0, budget->memoryMaximum + 1 - aboveAdmission, 0, 0, false, true));
  KJ_EXPECT(!incoming.replacementCeiling());
  KJ_EXPECT(budget->memoryReserved.load() == budget->memoryMaximum + 1);
}

KJ_TEST("database retirement waits for the final reservation holder only until its deadline") {
  using Outcome = DatabaseRetirement::Outcome;
  constexpr auto MiB = ResourceBudget::MiB;
  const auto budget = std::make_shared<ResourceBudget>(128 * MiB, 1, 4,
      ResourceBudget::ReclaimMode::NONE);
  auto idleDatabase = std::make_shared<ResourceReservation>(budget, 64, true);
  KJ_REQUIRE(idleDatabase->registerPreparation());
  KJ_EXPECT(idleDatabase->replacementCeiling().has_value());
  const auto now = server::DurableAdmission::Clock::now();
  const auto deadline = now + std::chrono::seconds(1);
  const DatabaseRetirement victim{1, 1, 0, idleDatabase->returned, deadline, false};
  KJ_EXPECT(victim.outcome(now, true, false) == Outcome::REFUSED);
  KJ_EXPECT(victim.outcome(now, true, true) == Outcome::PENDING);
  auto lastHolder = idleDatabase;
  idleDatabase.reset();
  KJ_EXPECT(victim.outcome(now, false, false) == Outcome::PENDING);
  KJ_EXPECT(victim.outcome(deadline, false, false) == Outcome::EXPIRED);
  KJ_EXPECT(budget->residents.load() == 1);
  lastHolder.reset();
  KJ_EXPECT(victim.outcome(now, false, false) == Outcome::RELEASED);
  KJ_EXPECT(victim.outcome(deadline, false, false) == Outcome::RELEASED);
  KJ_EXPECT(budget->residents.load() == 0);
  KJ_EXPECT(budget->memoryReserved.load() == budget->baselineMemory);
}

KJ_TEST("a database preparation retires only one copy when competing work claims its budget") {
  constexpr auto MiB = ResourceBudget::MiB;
  const auto budget = std::make_shared<ResourceBudget>(128 * MiB, 2, 4,
      ResourceBudget::ReclaimMode::NONE);
  auto first = std::make_shared<ResourceReservation>(budget, 64, true);
  auto second = std::make_shared<ResourceReservation>(budget, 64, true);
  KJ_REQUIRE(first->registerPreparation());
  KJ_REQUIRE(second->registerPreparation());
  ResourceReservation competitor(budget, 64);
  ResourceReservation incoming(budget, 64, true);
  unsigned retirements = 0;
  auto retire = [&]() -> kj::Promise<std::shared_ptr<DatabasePreparationReplacement>> {
    if (++retirements == 1) {
      first.reset();
      KJ_REQUIRE(competitor.registerPreparation());
    } else {
      second.reset();
    }
    return std::make_shared<DatabasePreparationReplacement>(budget->admissionMaximum);
  };
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  KJ_EXPECT(!registerDurablePreparation(incoming, true, retire).wait(waitScope));
  KJ_EXPECT(retirements == 1);
  KJ_EXPECT(budget->residents.load() == 2);
  KJ_EXPECT(!incoming.registered);
}

KJ_TEST("one returned copy admits at most one replacement above the admission ceiling") {
  constexpr auto MiB = ResourceBudget::MiB;
  const auto budget = std::make_shared<ResourceBudget>(128 * MiB, 10, 4,
      ResourceBudget::ReclaimMode::NONE);
  auto victim = std::make_shared<ResourceReservation>(budget, 64, true);
  ResourceReservation live(budget, 64);
  ResourceReservation first(budget, 64, true);
  ResourceReservation second(budget, 64, true);
  KJ_REQUIRE(victim->registerPreparation());
  KJ_REQUIRE(live.registerPreparation());
  KJ_REQUIRE(live.allocateExternal(budget->admissionMaximum - budget->memoryReserved.load() +
      ResourceBudget::PREPARATION_MEMORY));
  KJ_REQUIRE(victim->allocateExternal(ResourceBudget::PREPARATION_MEMORY));
  const auto ceiling = victim->replacementCeiling();
  KJ_REQUIRE(ceiling.has_value());
  DatabasePreparationReplacement replacement{*ceiling};
  victim.reset();
  KJ_REQUIRE(first.registerPreparation(&replacement));
  KJ_EXPECT(!second.registerPreparation(&replacement));
  KJ_EXPECT(budget->memoryReserved.load() == budget->admissionMaximum +
      ResourceBudget::PREPARATION_MEMORY);
}

KJ_TEST("a returned preparation credit is available to the waiting database first") {
  constexpr auto MiB = ResourceBudget::MiB;
  const auto budget = std::make_shared<ResourceBudget>(128 * MiB, 10, 4,
      ResourceBudget::ReclaimMode::NONE);
  auto victim = std::make_shared<ResourceReservation>(budget, 64, true);
  ResourceReservation live(budget, 64);
  ResourceReservation competitor(budget, 64);
  ResourceReservation incoming(budget, 64, true);
  KJ_REQUIRE(victim->registerPreparation());
  KJ_REQUIRE(live.registerPreparation());
  KJ_REQUIRE(live.allocateExternal(budget->admissionMaximum - budget->memoryReserved.load()));
  budget->preferDatabasePreparation(server::DurableAdmission::Time::max());
  victim.reset();
  KJ_EXPECT(!competitor.registerPreparation());
  KJ_EXPECT(incoming.registerPreparation());
  KJ_EXPECT(budget->memoryReserved.load() == budget->admissionMaximum);
  live.freeExternal(ResourceBudget::PREPARATION_MEMORY);
  budget->preferDatabasePreparation(server::DurableAdmission::Clock::now());
  KJ_EXPECT(competitor.registerPreparation());
  KJ_EXPECT(budget->databasePreparationPreferredUntil == server::DurableAdmission::Time{});
}

KJ_TEST("a database replacement counts heap headroom lent after the victim was selected") {
  constexpr auto preparation = ResourceBudget::PREPARATION_MEMORY;
  const auto budget = std::make_shared<ResourceBudget>(128 * ResourceBudget::MiB, 2, 4,
      ResourceBudget::ReclaimMode::NONE);
  auto victim = std::make_shared<ResourceReservation>(budget, 64, true);
  ResourceReservation live(budget, 64);
  ResourceReservation incoming(budget, 64, true);
  KJ_REQUIRE(victim->registerPreparation());
  KJ_REQUIRE(live.registerPreparation());
  const auto beforeRelease = budget->memoryMaximum - 1;
  KJ_REQUIRE(live.allocateExternal(beforeRelease - budget->memoryReserved.load()));
  const auto ceiling = victim->replacementCeiling();
  KJ_REQUIRE(ceiling == beforeRelease);

  // Simulate two bytes lent to a heap before admission retries.
  budget->lent = 2;
  KJ_EXPECT(!victim->replacementCeiling());
  victim.reset();
  DatabasePreparationReplacement replacement{*ceiling};
  KJ_EXPECT(!incoming.registerPreparation(&replacement));
  KJ_EXPECT(!replacement.used);
  KJ_EXPECT(budget->memoryReserved.load() == beforeRelease - preparation);
  budget->lent = 1;
  KJ_REQUIRE(incoming.registerPreparation(&replacement));
  KJ_EXPECT(budget->memoryReserved.load() == beforeRelease);
  KJ_EXPECT(budget->memoryReserved.load() + budget->lent == budget->memoryMaximum);
}

KJ_TEST("lent heap leaves the ordinary admission ceiling available to a database replacement") {
  const auto budget = std::make_shared<ResourceBudget>(128 * ResourceBudget::MiB, 2, 4,
      ResourceBudget::ReclaimMode::NONE);
  auto victim = std::make_shared<ResourceReservation>(budget, 64, true);
  ResourceReservation live(budget, 64);
  KJ_REQUIRE(victim->registerPreparation());
  KJ_REQUIRE(live.registerPreparation());
  KJ_REQUIRE(live.allocateExternal(budget->admissionMaximum - budget->memoryReserved.load()));

  // Simulate the emergency headroom from an isolate's first near-heap-limit raise.
  budget->lent = MAX_SINGLE_HEAP_OBJECT;
  KJ_EXPECT(victim->replacementCeiling() == budget->admissionMaximum);
  victim.reset();
  {
    ResourceReservation ordinary(budget, 64, true);
    KJ_REQUIRE(ordinary.registerPreparation());
    KJ_EXPECT(budget->memoryReserved.load() == budget->admissionMaximum);
  }
  DatabasePreparationReplacement replacement{budget->admissionMaximum};
  ResourceReservation incoming(budget, 64, true);
  KJ_EXPECT(incoming.registerPreparation(&replacement));
  KJ_EXPECT(budget->memoryReserved.load() == budget->admissionMaximum);
}

// Exercise the server's routing and accounting without starting homes or creating actors.
struct DatabaseAdmissionFixture {
  using Admission = server::DurableAdmission;
  static constexpr auto PREPARATION = ResourceBudget::PREPARATION_MEMORY;
  // Advance the routing clock explicitly while the budget's real clock remains before the
  // preference deadline. No test waits for a real deadline or allocates the charged bytes.
  const Admission::Time now = Admission::Clock::now() + std::chrono::hours(1);
  const Admission::Time lease = now + std::chrono::seconds(5);
  static constexpr auto RETIREMENT_WAIT = std::chrono::seconds(1);
  ThreadedServer server{3, "", "", -1, {}, {}, &initializeV8()};
  const std::shared_ptr<ResourceBudget> budget = std::make_shared<ResourceBudget>(
      128 * ResourceBudget::MiB, 2, 3, ResourceBudget::ReclaimMode::NONE);
  const DurableRoute victim = route("victim", true);
  const DurableRoute incoming = route("incoming", true);
  const DurableRoute neighbor = route("neighbor", false);
  const DurableRoute competitor = route("competitor", false);
  std::shared_ptr<ResourceReservation> held = std::make_shared<ResourceReservation>(budget, 64, true);
  uint64_t victimTree = 0;

  DatabaseAdmissionFixture() {
    server.resources = budget;
    server.durableAdmission.emplace(Admission::Limits{
      .capacity = {.threads = 3, .descriptors = 20, .memory = 256},
      .tree = {.threads = 1, .descriptors = 3, .memory = 16},
      .event = {.descriptors = 1, .memory = 4},
      .residentTarget = 2,
      .mailboxDepth = 3,
      .mailboxBytes = 64,
    }, 1);
    for (auto id: {1u, 2u, 3u}) {
      auto thread = std::make_unique<ThreadedServer::Thread>(false, false);
      thread->homeId = id;
      thread->home.emplace();
      server.threads.push_back(std::move(thread));
    }
    server.ready.store(true);
    KJ_REQUIRE(held->registerPreparation());
    victimTree = makeResident(victim);
    KJ_REQUIRE(server.durableAdmission->homeFor(victimTree) == 1u);
    home().databaseCanRetire = true;
    home().databaseLastUse = 1;
    home().databaseReservation = held;
  }

  ~DatabaseAdmissionFixture() {
    for (auto& thread: server.threads) close(thread->wake);
  }

  static DurableRoute route(const char* object, bool database) {
    return {{database ? "database" : "tenant", object},
      {1, 1, 1, 1, database ? PLATFORM_DATABASE_COPY_PREFIX + std::string(object) : object,
        database ? "SqlDatabase" : "Counter"},
      {}, {}, {}};
  }

  ThreadedServer::Thread::DurableHome& home() {
    return *server.threads.front()->home;
  }

  Admission::Result queue(const DurableRoute& route, Admission::Time at) {
    return server.queueDurable(route, 0, lease, at);
  }

  uint64_t makeResident(const DurableRoute& route) {
    auto ticket = queue(route, now).ticket;
    KJ_REQUIRE(ticket.has_value());
    auto& table = *server.durableAdmission;
    const auto homeId = table.homeFor(ticket->tree);
    KJ_REQUIRE(homeId.has_value());
    KJ_REQUIRE(table.begin(*ticket, *homeId, route.key, route.open, now) ==
        Admission::Delivery::ACCEPTED);
    KJ_REQUIRE(table.complete(*ticket, *homeId));
    auto& home = *server.threads[*homeId - 1]->home;
    home.resident.insert(ticket->tree);
    home.warmDefinition = route.open.definition;
    return ticket->tree;
  }

  void closeVictim(const DatabaseRetirement& selected) {
    std::lock_guard lock(server.durableRoutingMutex);
    KJ_REQUIRE(server.stopDurableIdleTree(victimTree, 1, false, &selected, now));
    KJ_REQUIRE(server.releaseDurableTree(victimTree, 1, true));
    // Mirror the home dropping its empty copy while the test keeps the final charge alive.
    home().warmDefinition.clear();
    home().definition.clear();
    home().databaseReservation.reset();
  }
};

KJ_TEST("durable placement skips a full WAL Home for fresh trees and keeps existing routes") {
  DatabaseAdmissionFixture test;
  auto& table = *test.server.durableAdmission;
  for (auto& thread: test.server.threads) thread->home->walFull = true;

  auto existing = test.queue(test.victim, test.now).ticket;
  KJ_REQUIRE(existing.has_value());
  KJ_EXPECT(table.homeFor(existing->tree) == 1u);

  auto refused = test.queue(test.neighbor, test.now);
  KJ_EXPECT(refused.status == DatabaseAdmissionFixture::Admission::Status::OVERLOADED);
  KJ_EXPECT(!refused.ticket.has_value());

  test.server.threads[1]->home->walFull = false;
  auto replacement = test.queue(test.neighbor, test.now).ticket;
  KJ_REQUIRE(replacement.has_value());
  KJ_EXPECT(table.homeFor(replacement->tree) == 2u);
}

KJ_TEST("accept expiry releases every waiter and limits replacement to the actual pre-release total") {
  DatabaseAdmissionFixture test;
  auto& server = test.server;
  auto& table = *server.durableAdmission;
  ResourceReservation live(test.budget, 64);
  KJ_REQUIRE(live.registerPreparation());
  const auto selectedTotal = test.budget->admissionMaximum + DatabaseAdmissionFixture::PREPARATION;
  KJ_REQUIRE(live.allocateExternal(selectedTotal - test.budget->memoryReserved.load()));

  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  auto first = server.reclaimIdleDatabase(test.now);
  auto second = server.reclaimIdleDatabase(test.now);
  KJ_REQUIRE(server.databaseVictim.has_value());
  auto selected = *server.databaseVictim;
  KJ_REQUIRE(selected.preparationCeiling == selectedTotal);
  KJ_EXPECT(test.budget->databasePreparationPreferredUntil == selected.deadline + DATABASE_ADMISSION_GRACE);
  test.closeVictim(selected);
  KJ_EXPECT(server.expireDurableAdmissions(selected.deadline - std::chrono::milliseconds(1)).empty());
  KJ_EXPECT(!first.poll(waitScope));

  // The home has not observed completion. A competing reservation shrank before the final
  // holder died, so the replacement must use the smaller total recorded at release.
  live.freeExternal(1);
  test.held.reset();
  KJ_EXPECT(server.expireDurableAdmissions(selected.deadline).empty());
  KJ_EXPECT(!server.databaseVictim);
  KJ_EXPECT(table.state().overloads == 0);
  KJ_REQUIRE(first.poll(waitScope));
  KJ_REQUIRE(second.poll(waitScope));
  auto replacement = first.wait(waitScope);
  KJ_REQUIRE(replacement != nullptr);
  KJ_EXPECT(second.wait(waitScope) == replacement);
  KJ_EXPECT(replacement->ceiling == selectedTotal - 1);
  ResourceReservation incoming(test.budget, 64, true);
  KJ_REQUIRE(live.allocateExternal(1));
  KJ_EXPECT(!incoming.registerPreparation(&*replacement));
  live.freeExternal(1);
  KJ_EXPECT(incoming.registerPreparation(&*replacement));
  KJ_EXPECT(test.budget->memoryReserved.load() == selectedTotal - 1);
}

KJ_TEST("accept expiry refuses every budget waiter while the charge is still held") {
  DatabaseAdmissionFixture test;
  auto& server = test.server;
  auto& table = *server.durableAdmission;
  auto neighbor = server.queueDurable(
      test.neighbor, 0, test.now + DatabaseAdmissionFixture::RETIREMENT_WAIT, test.now).ticket;
  KJ_REQUIRE(neighbor.has_value());
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  auto first = server.reclaimIdleDatabase(test.now);
  auto second = server.reclaimIdleDatabase(test.now);
  KJ_REQUIRE(server.databaseVictim.has_value());
  const auto deadline = server.databaseVictim->deadline;
  KJ_EXPECT(deadline == test.now + DatabaseAdmissionFixture::RETIREMENT_WAIT);
  KJ_EXPECT_LOG(WARNING, "idle database admission retirement timed out");
  const auto expired = server.expireDurableAdmissions(deadline);
  KJ_REQUIRE(expired.size() == 1);
  KJ_EXPECT(expired.front() == *neighbor);
  KJ_REQUIRE(first.poll(waitScope));
  KJ_REQUIRE(second.poll(waitScope));
  KJ_EXPECT(!first.wait(waitScope));
  KJ_EXPECT(!second.wait(waitScope));
  KJ_EXPECT(!server.databaseVictim);
  KJ_EXPECT(!test.home().databaseCanRetire);
  KJ_EXPECT(table.state().overloads == 1);
  KJ_EXPECT(table.state().expired == 1);
  KJ_EXPECT(test.budget->databasePreparationPreferredUntil == deadline);
}

KJ_TEST("the server rechecks the deadline and activity before stopping the selected tree") {
  using Admission = DatabaseAdmissionFixture::Admission;
  DatabaseAdmissionFixture test;
  auto& server = test.server;
  auto& table = *server.durableAdmission;
  test.makeResident(test.neighbor);
  KJ_REQUIRE(test.queue(test.incoming, test.now).status == Admission::Status::RETIRING);
  KJ_REQUIRE(server.databaseVictim.has_value());
  const auto selected = *server.databaseVictim;
  auto waiting = test.queue(test.victim, test.now).ticket;
  KJ_REQUIRE(waiting.has_value());
  std::lock_guard lock(server.durableRoutingMutex);
  auto stop = [&](Admission::Time at) {
    return server.stopDurableIdleTree(test.victimTree, 1, false, &selected, at);
  };
  KJ_EXPECT(!stop(test.now));
  KJ_REQUIRE(table.cancel(*waiting) == Admission::Cancellation::RELEASED);
  KJ_EXPECT(!stop(selected.deadline));
  KJ_EXPECT(table.isIdleTree(test.victimTree, 1));
  ++test.home().databaseLastUse;
  const auto beforeDeadline = selected.deadline - std::chrono::milliseconds(1);
  KJ_EXPECT(!stop(beforeDeadline));
  test.home().databaseLastUse = selected.lastUse;
  KJ_EXPECT(stop(beforeDeadline));
}

KJ_TEST("queueDurable holds the returned tree and preparation credits until a database claims them") {
  using Admission = DatabaseAdmissionFixture::Admission;
  using Status = Admission::Status;
  DatabaseAdmissionFixture test;
  auto& server = test.server;
  auto& table = *server.durableAdmission;
  test.makeResident(test.neighbor);
  ResourceReservation live(test.budget, 64);
  KJ_REQUIRE(live.registerPreparation());
  KJ_REQUIRE(test.queue(test.incoming, test.now).status == Status::RETIRING);
  KJ_REQUIRE(server.databaseVictim.has_value());
  const auto selected = *server.databaseVictim;
  KJ_EXPECT(test.budget->databasePreparationPreferredUntil == selected.deadline + DATABASE_ADMISSION_GRACE);
  test.closeVictim(selected);
  KJ_REQUIRE(server.databaseVictim->treePreferred);
  test.held.reset();
  const auto returnedAt = test.now + std::chrono::milliseconds(200);
  {
    std::lock_guard lock(server.durableRoutingMutex);
    server.finishDatabaseRetirement(selected.outcome(returnedAt, false, false), returnedAt);
  }
  KJ_EXPECT(test.budget->databasePreparationPreferredUntil == returnedAt + DATABASE_ADMISSION_GRACE);
  KJ_EXPECT(test.home().databasePreferredUntil == returnedAt + DATABASE_ADMISSION_GRACE);
  KJ_EXPECT(test.queue(test.neighbor, returnedAt).status == Status::QUEUED);
  // There is a spare home, but the only free tree credit is reserved for the database.
  KJ_EXPECT(test.queue(test.competitor, returnedAt).status == Status::OVERLOADED);
  ResourceReservation ordinary(test.budget, 64);
  KJ_EXPECT(!ordinary.registerPreparation());
  const auto claimed = test.queue(test.incoming, returnedAt);
  KJ_REQUIRE(claimed.ticket.has_value());
  KJ_EXPECT(table.homeFor(claimed.ticket->tree) == 1u);
  auto database = std::make_shared<ResourceReservation>(test.budget, 64, true);
  KJ_EXPECT(database->registerPreparation());
  KJ_EXPECT(test.budget->databasePreparationPreferredUntil == Admission::Time{});

  // If queueDurable consumed the tree preference, the ordinary caller can use the tree after
  // this database closes, even though the preference's time has not elapsed.
  {
    std::lock_guard lock(server.durableRoutingMutex);
    KJ_REQUIRE(table.cancel(*claimed.ticket) == Admission::Cancellation::RELEASED);
    KJ_REQUIRE(server.stopDurableIdleTree(claimed.ticket->tree, 1, false, nullptr));
    KJ_REQUIRE(server.releaseDurableTree(claimed.ticket->tree, 1, false));
  }
  database.reset();
  KJ_EXPECT(test.queue(test.competitor, returnedAt).status == Status::QUEUED);
  KJ_EXPECT(ordinary.registerPreparation());
}

KJ_TEST("SQLite allocations share the copy reservation with V8 and return only their net change") {
  constexpr auto MiB = ResourceBudget::MiB;
  const auto budget =
      std::make_shared<ResourceBudget>(96 * MiB, 4, 2, ResourceBudget::ReclaimMode::NONE);
  ResourceReservation copy(budget, 64, true);
  KJ_REQUIRE(copy.registerPreparation());
  const auto baseline = budget->memoryReserved.load();
  size_t bytes = 0;
  SqliteMemoryScope scope(bytes, 4 * MiB, &copy);
  auto* block = sqliteMemMalloc(4096);
  KJ_REQUIRE(block != nullptr);
  KJ_DEFER(sqliteMemFree(block));
  const auto original = bytes;
  KJ_EXPECT(original >= 4096);
  KJ_EXPECT(budget->memoryReserved.load() == baseline + MiB);

  // Saturate the process with the copy's independently accounted ArrayBuffers.
  const auto buffers = budget->memoryMaximum - budget->memoryReserved.load();
  KJ_REQUIRE(copy.allocateExternal(buffers));
  KJ_EXPECT(sqliteMemRealloc(block, MiB + 4096) == nullptr);
  KJ_EXPECT(SqliteMemoryScope::getLimitRefusal() == SqliteMemoryScope::LimitRefusal::PROCESS);
  KJ_EXPECT(bytes == original);
  KJ_EXPECT(budget->sqliteMemoryReserved.load() == MiB);
  KJ_EXPECT(budget->memoryReserved.load() == budget->memoryMaximum);
  copy.freeExternal(buffers);
  SqliteMemoryScope::clearLimitRefusal();
  KJ_EXPECT(sqliteMemRealloc(block, 4 * MiB + 4096) == nullptr);
  KJ_EXPECT(SqliteMemoryScope::getLimitRefusal() == SqliteMemoryScope::LimitRefusal::DATABASE);
  KJ_EXPECT(bytes == original);

  auto* grown = sqliteMemRealloc(block, MiB + 4096);
  KJ_REQUIRE(grown != nullptr);
  block = grown;
  const auto growth = bytes;
  KJ_EXPECT(growth >= MiB + 4096);
  KJ_EXPECT(budget->memoryReserved.load() == baseline + 2 * MiB);
  KJ_EXPECT(budget->sqliteMemoryReserved.load() == 2 * MiB);

  auto* shrunk = sqliteMemRealloc(block, 1024);
  KJ_REQUIRE(shrunk != nullptr);
  block = shrunk;
  KJ_EXPECT(bytes >= 1024 && bytes < original);
  KJ_EXPECT(budget->memoryReserved.load() == baseline + MiB);
  KJ_EXPECT(budget->sqliteMemoryReserved.load() == MiB);
  sqliteMemFree(block);
  block = nullptr;
  KJ_EXPECT(bytes == 0);
  KJ_EXPECT(budget->sqliteMemoryReserved.load() == 0);
  KJ_EXPECT(budget->memoryReserved.load() == baseline);
}

KJ_TEST("SQLite heap held by multiple databases meets the process ceiling and returns on close") {
  constexpr auto MiB = ResourceBudget::MiB;
  constexpr int queryBytes = int(MiB) + 192 * 1024;
  const auto budget =
      std::make_shared<ResourceBudget>(96 * MiB, 4, 2, ResourceBudget::ReclaimMode::NONE);
  auto firstCopy = std::make_shared<ResourceReservation>(budget, 64, true);
  auto secondCopy = std::make_shared<ResourceReservation>(budget, 64, true);
  ResourceReservation competitor(budget, 64);
  KJ_REQUIRE(firstCopy->registerPreparation());
  KJ_REQUIRE(secondCopy->registerPreparation());
  KJ_REQUIRE(competitor.registerPreparation());

  installSqliteCustomAllocator();
  auto dir = kj::newInMemoryDirectory(kj::nullClock());
  SqliteDatabase::WalTransactionLimit firstLimit(MiB), secondLimit(MiB);
  SqliteDatabase::Vfs firstVfs(*dir, {.walTransactionLimit = &firstLimit});
  SqliteDatabase::Vfs secondVfs(*dir, {.walTransactionLimit = &secondLimit});
  const auto mode = kj::WriteMode::CREATE | kj::WriteMode::MODIFY;
  auto openFirst = [&] {
    return kj::heap<SqliteDatabase>(firstVfs, kj::Path({"one.db"}), mode, 4 * MiB, kj::maxValue,
        SqliteObserver::DEFAULT, kj::none, firstCopy);
  };
  const auto beforeOpen = budget->memoryReserved.load();
  auto first = openFirst();
  auto second = kj::heap<SqliteDatabase>(secondVfs, kj::Path({"two.db"}), mode, 4 * MiB,
      kj::maxValue, SqliteObserver::DEFAULT, kj::none, secondCopy);
  auto verify = [&] {
    const auto firstCharge = SqliteProcessMemoryBudget::chargeFor(first->getSqliteMemoryBytes());
    const auto secondCharge = SqliteProcessMemoryBudget::chargeFor(second->getSqliteMemoryBytes());
    KJ_EXPECT(firstCopy->sqliteMemoryCharge() == firstCharge);
    KJ_EXPECT(secondCopy->sqliteMemoryCharge() == secondCharge);
    KJ_EXPECT(budget->sqliteMemoryReserved.load() == firstCharge + secondCharge);
    return firstCharge + secondCharge;
  };
  auto idleBytes = verify();
  KJ_REQUIRE(idleBytes == 2 * MiB);
  KJ_EXPECT(budget->memoryReserved.load() == beforeOpen + idleBytes);
  const auto buffers = budget->memoryMaximum - budget->memoryReserved.load() - MiB;
  KJ_REQUIRE(competitor.allocateExternal(buffers));

  firstLimit.setPageSize(4096);
  secondLimit.setPageSize(4096);
  {
    auto held = first->run("SELECT randomblob(?)", queryBytes);
    KJ_REQUIRE(held.getBlob(0).size() == queryBytes);
    const auto holdingBytes = verify();
    KJ_EXPECT(holdingBytes == 3 * MiB);
    KJ_EXPECT(budget->memoryReserved.load() == beforeOpen + holdingBytes + buffers);
    {
      // Trusted work outside the transaction still acquires and returns the step.
      auto maintenance = second->run("SELECT randomblob(?)", queryBytes);
      KJ_REQUIRE(maintenance.getBlob(0).size() == queryBytes);
      KJ_EXPECT(verify() == holdingBytes + MiB);
      KJ_EXPECT(budget->memoryReserved.load() == budget->memoryMaximum + MiB);
    }
    KJ_EXPECT(verify() == holdingBytes);
    secondLimit.begin();
    second->run("SAVEPOINT pending");
    KJ_EXPECT_THROW_MESSAGE("out of memory", second->run("SELECT randomblob(?)", queryBytes));
    KJ_EXPECT(secondLimit.wasProcessMemoryRefused() && !secondLimit.wasMemoryRefused());
    second->rollbackWalTransaction();
    secondLimit.end(*second, /*rolledBack=*/true);
    KJ_EXPECT(!second->observedCriticalError());
    KJ_EXPECT(held.getBlob(0).size() == queryBytes);
    KJ_EXPECT(budget->withinMaximum());
  }
  KJ_EXPECT(verify() == idleBytes);
  secondLimit.begin();
  {
    auto admitted = second->run("SELECT randomblob(?)", queryBytes);
    KJ_EXPECT(admitted.getBlob(0).size() == queryBytes);
  }
  secondLimit.end(*second);

  // The copy can outlive its connection. Its SQLite bytes must be available
  // before the copy returns its own reservation to the process.
  first->close();
  KJ_EXPECT(firstCopy->sqliteMemoryCharge() == 0);
  KJ_EXPECT(verify() == MiB);
  const auto openHeadroom = budget->memoryMaximum - budget->memoryReserved.load();
  KJ_REQUIRE(competitor.allocateExternal(openHeadroom));
  first = openFirst();
  KJ_EXPECT(budget->memoryReserved.load() == budget->memoryMaximum + MiB);
  first->reset();
  KJ_EXPECT(budget->memoryReserved.load() == budget->memoryMaximum + MiB);
  KJ_EXPECT(first->run("SELECT 1").getInt(0) == 1);
  KJ_EXPECT(verify() == 2 * MiB);
  competitor.freeExternal(openHeadroom);
  first = nullptr;
  second = nullptr;
  KJ_EXPECT(budget->sqliteMemoryReserved.load() == 0);
  KJ_EXPECT(budget->memoryReserved.load() == beforeOpen + buffers);
  competitor.freeExternal(buffers);
  KJ_EXPECT(budget->memoryReserved.load() == beforeOpen);
}

KJ_TEST("a copy logs SQLite charge drift and returns its recorded memory once") {
  constexpr auto MiB = ResourceBudget::MiB;
  auto budget = std::make_shared<ResourceBudget>(96 * MiB, 4, 2, ResourceBudget::ReclaimMode::NONE);
  const auto baseline = budget->memoryReserved.load();
  {
    KJ_EXPECT_LOG(ERROR, "SQLite heap charge remained when releasing a copy reservation");
    ResourceReservation copy(budget, 64, true);
    KJ_REQUIRE(copy.registerPreparation());
    KJ_REQUIRE(copy.resizeSqliteMemory(0, MiB, true));
    KJ_EXPECT_LOG(ERROR, "SQLite heap change exceeded the copy's recorded charge");
    KJ_REQUIRE(copy.resizeSqliteMemory(2 * MiB, 0, true));
    KJ_EXPECT(budget->sqliteMemoryReserved.load() == 0);
    KJ_REQUIRE(copy.resizeSqliteMemory(0, MiB, true));
  }
  KJ_EXPECT(budget->sqliteMemoryReserved.load() == 0);
  KJ_EXPECT(budget->memoryReserved.load() == baseline);
}

KJ_TEST("fatal record identifies only the invocation on the current V8 thread") {
  int fds[2];
  KJ_SYSCALL(pipe(fds));
  KJ_DEFER(close(fds[0]));
  KJ_DEFER(close(fds[1]));
  v8::Isolate::CreateParams params;
  params.array_buffer_allocator_shared.reset(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
  auto* isolate = v8::Isolate::New(newGroup(), params);
  KJ_DEFER(isolate->Dispose());
  auto* otherIsolate = v8::Isolate::New(newGroup(), params);
  KJ_DEFER(otherIsolate->Dispose());
  auto readRecord = [&] {
    char row[PIPE_BUF];
    const auto count = read(fds[0], row, sizeof(row));
    KJ_REQUIRE(count > 0);
    return kj::heapString(row, count);
  };
  auto checkUnknown = [&] {
    KJ_EXPECT(
        readRecord() == kj::str("{\"event\":\"v8-fatal-undetermined\",\"pid\":", getpid(), "}\n"));
  };
  const kj::StringPtr identity =
      "\"workerId\":\"tenant/w\",\"versionId\":\"v1\","
      "\"invocationId\":\"call\",\"isolate\":\"coworkerd-v1:tenant/w@v1#5\",";
  FatalInvocation invocation(isolate, identity);
  writeFatalRecord(fds[1]);
  checkUnknown();  // A scope alone does not prove the isolate is current.
  {
    v8::Locker locker(isolate);
    v8::Isolate::Scope active(isolate);
    std::thread other([&] { writeFatalRecord(fds[1]); });
    other.join();
    checkUnknown();
    auto checkIdentity = [&](kj::StringPtr expected) {
      writeFatalRecord(fds[1]);
      KJ_EXPECT(readRecord() ==
          kj::str("{\"event\":\"v8-fatal\",\"pid\":", getpid(), ",",
              expected.slice(0, expected.size() - 1), "}\n"));
    };
    checkIdentity(identity);
    {
      const kj::StringPtr nestedIdentity = "\"invocationId\":\"nested\",";
      FatalInvocation nested(isolate, nestedIdentity);
      checkIdentity(nestedIdentity);
    }
    checkIdentity(identity);  // The outer scope is current again.
    {
      // Another isolate entered on this thread, with this scope still the top one.
      v8::Locker otherLocker(otherIsolate);
      v8::Isolate::Scope otherEntered(otherIsolate);
      KJ_REQUIRE(v8::Isolate::TryGetCurrent() == otherIsolate);
      writeFatalRecord(fds[1]);
      checkUnknown();
    }
    checkIdentity(identity);
  }
}

KJ_TEST("an entry outside any scope or live call takes the Worker's load identity") {
  // Startup, logging and dynamic import entries ask scopedExecutionIdentity() whom a fatal error
  // belongs to. With no enclosing scope of the isolate and no I/O context (a legacy dynamic import
  // continues outside its call's), the isolate still runs only its own Worker's code.
  IsolateLimits limits(100, 32,
      kj::str("\"workerId\":\"tenant/w\",\"versionId\":\"v1\",\"invocationId\":\"load\","), nullptr,
      newGroup());
  v8::Isolate::CreateParams params;
  params.array_buffer_allocator_shared.reset(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
  auto* isolate = v8::Isolate::New(newGroup(), params);
  KJ_DEFER(isolate->Dispose());
  auto* otherIsolate = v8::Isolate::New(newGroup(), params);
  KJ_DEFER(otherIsolate->Dispose());
  limits.isolate = isolate;  // as customizeIsolate() records it
  KJ_REQUIRE(!IoContext::hasCurrent());
  KJ_EXPECT(KJ_ASSERT_NONNULL(scopedExecutionIdentity(limits, isolate)) == limits.identity);
  {
    // An enclosing scope of this isolate (the load or the call) takes precedence.
    FatalInvocation call(isolate, "\"invocationId\":\"call\",");
    KJ_EXPECT(KJ_ASSERT_NONNULL(scopedExecutionIdentity(limits, isolate)) == "\"invocationId\":\"call\",");
    // An entry on an isolate that is not this Worker's own is named by nobody.
    KJ_EXPECT(scopedExecutionIdentity(limits, otherIsolate) == kj::none);
  }
  limits.durableEvents = true;
  KJ_EXPECT(scopedExecutionIdentity(limits, isolate) == kj::none);
}

KJ_TEST("another Worker's call never lends its identity to this Worker's entry") {
  IsolateLimits limits(100, 32, kj::str("\"invocationId\":\"load\","), nullptr, newGroup());
  IsolateLimits other(100, 32, kj::str("\"invocationId\":\"other-load\","), nullptr, newGroup());
  auto* isolate = reinterpret_cast<v8::Isolate*>(&limits);  // only compared, never entered
  limits.isolate = isolate;
  const kj::StringPtr call = "\"invocationId\":\"call\",";
  // The current I/O context's call runs this Worker: its identity.
  KJ_EXPECT(KJ_ASSERT_NONNULL(entryFatalIdentity(limits, isolate, kj::none, &limits, call)) == call);
  // It runs another Worker: this entry keeps its own Worker's load identity.
  KJ_EXPECT(KJ_ASSERT_NONNULL(entryFatalIdentity(limits, isolate, kj::none, &other, call)) ==
      limits.identity);
  // No I/O context.
  KJ_EXPECT(KJ_ASSERT_NONNULL(entryFatalIdentity(limits, isolate, kj::none, nullptr, ""_kj)) ==
      limits.identity);
  // An enclosing scope of this isolate comes first.
  KJ_EXPECT(KJ_ASSERT_NONNULL(entryFatalIdentity(
                limits, isolate, kj::StringPtr("\"invocationId\":\"outer\","), &limits, call)) ==
      "\"invocationId\":\"outer\",");
}

// A minimal jsg isolate, for the server's own startup entry on a real jsg::Lock.
struct StartupEntryGlobal: public jsg::Object, public jsg::ContextGlobal {
  JSG_RESOURCE_TYPE(StartupEntryGlobal) {}
};
JSG_DECLARE_ISOLATE_TYPE(StartupEntryIsolate, StartupEntryGlobal);

KJ_TEST("a startup entry opens the fatal scope of its Worker and closes it") {
  auto& system = initializeV8();
  IsolateLimits limits(100, 32,
      kj::str("\"workerId\":\"tenant/w\",\"versionId\":\"v1\",\"invocationId\":\"load\","), nullptr,
      newGroup());
  StartupEntryIsolate jsgIsolate(system, newGroup(), nullptr, kj::heap<jsg::IsolateObserver>());
  jsgIsolate.runInLockScope([&](StartupEntryIsolate::Lock& lock) {
    limits.customizeIsolate(lock.v8Isolate);
    KJ_EXPECT(FatalInvocation::forIsolate(lock.v8Isolate) == nullptr);
    kj::OneOf<kj::Exception, kj::Duration> result;
    {
      auto entry = limits.enterStartupJs(lock, result);
      const auto* scope = FatalInvocation::forIsolate(lock.v8Isolate);
      KJ_ASSERT(scope != nullptr, "the startup entry pushed no fatal scope");
      KJ_EXPECT(scope->getIdentity() == limits.identity);
    }
    KJ_EXPECT(FatalInvocation::forIsolate(lock.v8Isolate) == nullptr);
    // A durable role's entry pushes none.
    limits.durableEvents = true;
    {
      auto entry = limits.enterLoggingJs(lock, result);
      KJ_EXPECT(FatalInvocation::forIsolate(lock.v8Isolate) == nullptr);
    }
  });
}

KJ_TEST("descriptor queue: empty, full, FIFO, SPSC handoff and close") {
  DescriptorQueue queue(2);
  KJ_EXPECT(queue.pop() == -1 && queue.hasSpace());
  int a = open("/dev/null", O_RDONLY | O_CLOEXEC);
  int b = open("/dev/null", O_RDONLY | O_CLOEXEC);
  KJ_ASSERT(a >= 0 && b >= 0);
  KJ_EXPECT(queue.push(a) && queue.push(b));
  KJ_EXPECT(!queue.hasSpace() && !queue.push(1000000));
  KJ_EXPECT(queue.pop() == a);
  ::close(a);
  queue.close();
  KJ_EXPECT(fcntl(b, F_GETFD) == -1 && errno == EBADF);
  KJ_EXPECT(queue.pop() == -1 && !queue.hasSpace() && !queue.push(1000000));

  DescriptorQueue concurrent(8);
  std::atomic<bool> done{false};
  std::thread producer([&] {
    for (unsigned i = 0; i < 2000; ++i) {
      int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
      while (!concurrent.push(fd)) std::this_thread::yield();
    }
    done.store(true, std::memory_order_release);
  });
  unsigned received = 0;
  while (!done.load(std::memory_order_acquire) || received < 2000) {
    int fd = concurrent.pop();
    if (fd < 0) {
      std::this_thread::yield();
      continue;
    }
    KJ_EXPECT(fcntl(fd, F_GETFD) >= 0);
    ::close(fd);
    ++received;
  }
  producer.join();
  KJ_EXPECT(received == 2000 && concurrent.pop() == -1);
}

KJ_TEST("actual reservation arithmetic, both watermarks and RAII release") {
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(2ull << 30, 10, 512);
  KJ_EXPECT(budget->memoryMaximum - budget->admissionMaximum == (2ull << 30) / 10);
  auto smaller = std::make_shared<ResourceBudget>(2ull << 30, 10, 16);
  KJ_EXPECT(smaller->memoryMaximum - smaller->admissionMaximum == (16ull << 20));
  const auto baseline = budget->memoryReserved.load();
  {
    auto reservation = std::make_shared<ResourceReservation>(budget, 128);
    KJ_ASSERT(reservation->registerPreparation());
    auto seed = budget->memoryReserved.load();
    KJ_EXPECT(seed > baseline && budget->residents.load() == 1);
    KJ_EXPECT(reservation->retainSource(100));
    KJ_EXPECT(budget->memoryReserved.load() == seed + 500);
    KJ_EXPECT(reservation->allocateExternal(4096));
    KJ_EXPECT(budget->memoryReserved.load() == seed + 500 + 4096);
    KJ_EXPECT(!reservation->allocateExternal(2ull << 30));
    KJ_EXPECT(budget->memoryReserved.load() == seed + 500 + 4096);
    reservation->freeExternal(4096);
    KJ_EXPECT(budget->memoryReserved.load() == seed + 500);
    {
      ResourcePreparation preparation(reservation);
      KJ_EXPECT(reservation->preparing == 1);
    }
    KJ_EXPECT(reservation->preparing == 0);
  }
  KJ_EXPECT(budget->memoryReserved.load() == baseline && budget->residents.load() == 0);
  auto memory = budget->memoryReserved.load();
  KJ_EXPECT(!budget->change(0, budget->admissionMaximum, 0, 0, true));
  KJ_EXPECT(budget->memoryReserved.load() == memory);
  KJ_EXPECT(!budget->change(0, budget->memoryMaximum, 0, 0, false, true));
  KJ_EXPECT(!budget->withinMaximum() && budget->reclaimRequested.load());
  KJ_EXPECT(budget->change(budget->memoryMaximum, 0, 0, 0, false, true));
  KJ_EXPECT(budget->memoryReserved.load() == baseline);
}

KJ_TEST("eviction excludes loading, referenced and open-context Workers") {
  auto slot = std::make_shared<Slot>();
  KJ_EXPECT(!canEvict(slot, nullptr));
  slot->complete.store(true);
  KJ_EXPECT(canEvict(slot, nullptr) && !canEvict(slot, slot.get()));
  {
    auto reference = slot;
    KJ_EXPECT(!canEvict(slot, nullptr));
  }
  slot->contexts.store(1);
  KJ_EXPECT(!canEvict(slot, nullptr));
  slot->contexts.store(0);
  KJ_EXPECT(canEvict(slot, nullptr));
}

KJ_TEST("tenant admission releases counts on errors and cancellation") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  auto budget = std::make_shared<TenantBudget>(1);
  {
    TenantPermit active(budget, "a");
    TenantPermit borrowed(budget, "a");
    KJ_EXPECT(budget->active["a"] == 2);
    {
      TenantPermit other(budget, "b");
      KJ_EXPECT(budget->active.size() == 2);
      KJ_EXPECT_THROW_RECOVERABLE_MESSAGE(
          "tenant execution concurrency limit reached in process", TenantPermit(budget, "a"));
      KJ_EXPECT_THROW_RECOVERABLE_MESSAGE(
          "tenant execution concurrency limit reached in process", TenantPermit(budget, "b"));
      KJ_EXPECT(budget->active["a"] == 2 && budget->active["b"] == 1);
    }
    KJ_EXPECT(budget->active.size() == 1);
    TenantPermit borrowedAgain(budget, "a");
    KJ_EXPECT(budget->active["a"] == 3);
  }
  KJ_EXPECT(budget->active.empty());
  auto pair = kj::newPromiseAndFulfiller<void>();
  auto canceled = pair.promise.attach(std::make_shared<TenantPermit>(budget, "a"));
  canceled = nullptr;
  KJ_EXPECT(budget->active.empty());
  auto disabled = std::make_shared<TenantBudget>(0);
  auto a = makeTenantPermit(disabled, "a");
  auto b = makeTenantPermit(disabled, "b");
  auto a2 = makeTenantPermit(disabled, "a");
  KJ_EXPECT(!a && !b && !a2 && disabled->active.empty());
  const auto refusal = capacityError(CapacityScope::TENANT);
  const auto detail = KJ_REQUIRE_NONNULL(refusal.getDetail(CAPACITY_DETAIL));
  KJ_EXPECT(detail.size() == 1 && detail[0] == 1);
}

// A console line past 4,096 bytes reaches the node cut, with its size, so that
// the node marks it truncated and counts it (b50 log-quota-isolation).
KJ_TEST("a console line past 4,096 bytes carries its size to the node") {
  auto longLine = consoleLogRow("log", kj::str(kj::repeat('x', 5000)));
  KJ_EXPECT(longLine.endsWith(",\"messageBytes\":5000}"), longLine.slice(longLine.size() - 40));
  KJ_EXPECT(longLine.size() == kj::str("{\"level\":\"log\",\"message\":\"\",\"messageBytes\":5000}").size() + 4096);
  auto shortLine = consoleLogRow("warn", "hello");
  KJ_EXPECT(shortLine == "{\"level\":\"warn\",\"message\":\"hello\"}", shortLine);
}

// The maintenance pass formats every resident into its lifecycle row outside
// the registry lock; holding them meanwhile must not make an idle Worker look
// in use, or a load past the resident limit is refused instead of evicting
// one (r50's fill cells: about four loads in ten refused).
KJ_TEST("the maintenance pass's snapshot leaves an idle resident evictable") {
  std::map<std::string, std::shared_ptr<Slot>> registry;
  auto idle = std::make_shared<Slot>();
  idle->complete.store(true);
  registry.emplace("a", idle);
  std::vector<std::shared_ptr<Slot>> retired{std::make_shared<Slot>()};
  idle.reset();
  auto snapshot = residentSnapshot(registry, retired);
  KJ_EXPECT(snapshot.size() == 2);
  KJ_EXPECT(canEvict(registry.at("a"), nullptr), "a snapshot must not make an idle Worker look in use");
  registry.clear();
  KJ_EXPECT(snapshot[0].expired(), "an evicted resident leaves the snapshot");
}

KJ_TEST("subrequest quotas, recursive bindings, release wakeup and cancellation") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  SubrequestBudget budget(512, 16);
  auto parent = std::make_shared<uint32_t>(0);
  std::vector<std::unique_ptr<SubrequestPermit>> bindings;
  for (unsigned i = 0; i < 15; ++i) {
    auto permit = std::make_unique<SubrequestPermit>(budget, parent, "a", false);
    permit->acquire().wait(waitScope);
    bindings.push_back(std::move(permit));
  }
  auto child = std::make_shared<uint32_t>(0);
  SubrequestPermit childEgress(budget, child, "a", true);
  auto childReady = childEgress.acquire();
  KJ_EXPECT(childReady.poll(waitScope));
  childReady.wait(waitScope);
  KJ_EXPECT(budget.tenants["a"] == 1 && *parent == 15 && *child == 1);

  SubrequestBudget small(1, 16);
  auto invocation = std::make_shared<uint32_t>(0);
  auto active = std::make_unique<SubrequestPermit>(small, invocation, "a", true);
  active->acquire().wait(waitScope);
  SubrequestPermit queued(small, invocation, "a", true);
  auto pending = queued.acquire();
  KJ_EXPECT(!pending.poll(waitScope) && small.waiters.size() == 1);
  active.reset();
  pending.wait(waitScope);
  KJ_EXPECT(small.active.load() == 1 && small.waiters.empty());
  SubrequestPermit canceled(small, invocation, "a", true);
  auto abandoned = canceled.acquire();
  KJ_EXPECT(!abandoned.poll(waitScope) && small.waiters.size() == 1);
  abandoned = nullptr;
  KJ_EXPECT(small.waiters.empty());

  // Without a per-tenant limit no per-tenant count is kept (v138 section 1).
  SubrequestBudget unlimited(512, 0);
  auto calls = std::make_shared<uint32_t>(0);
  SubrequestPermit egress(unlimited, calls, "a", true);
  egress.acquire().wait(waitScope);
  KJ_EXPECT(unlimited.tenants.empty() && unlimited.active.load() == 1 && egress.tenant.empty());

  SubrequestBudget perCall(512, 128);
  auto one = std::make_shared<uint32_t>(0);
  std::vector<std::unique_ptr<SubrequestPermit>> held;
  for (unsigned i = 0; i < 32; ++i) {
    auto permit = std::make_unique<SubrequestPermit>(perCall, one, "a", false);
    permit->acquire().wait(waitScope);
    held.push_back(std::move(permit));
  }
  SubrequestPermit extra(perCall, one, "a", false);
  auto waiting = extra.acquire();
  KJ_EXPECT(!waiting.poll(waitScope));
  held.pop_back();
  waiting.wait(waitScope);
  KJ_EXPECT(*one == 32);
}

KJ_TEST("log quotas and a failed flush reset pending and in-flight buffers") {
  threaded::InvocationLogBudget bytes, lines;
  KJ_EXPECT(bytes.accept(threaded::InvocationLogBudget::MAX_BYTES));
  KJ_EXPECT(!bytes.accept(1) && !bytes.accept(UINT64_MAX));
  for (unsigned i = 0; i < 1023; ++i) KJ_ASSERT(lines.accept(0));
  KJ_EXPECT(!lines.accept(0));
  InvocationLogBuffer buffer;
  buffer.lines.add(kj::str("pending"));
  buffer.queuedLogBytes = 7;
  buffer.inFlightLogBytes = 11;
  buffer.inFlightLogLines = 1;
  buffer.flushing = true;
  buffer.resetAfterFlushFailure();
  KJ_EXPECT(buffer.lines.size() == 0 && buffer.queuedLogBytes == 0 &&
      buffer.inFlightLogBytes == 0 && buffer.inFlightLogLines == 0 && !buffer.flushing);
  buffer.lines.add(kj::str("recovered"));
  KJ_EXPECT(buffer.lines.size() == 1);
}

// A call's JavaScript entries point at the OOM report its RequestLimits
// formatted once (the row, the pid and the identity), instead of formatting
// it again at every entry; the watchdog entry points at the owner's identity.
KJ_TEST("JavaScript entries reuse the call's OOM report and identity") {
  initializeV8();
  IsolateLimits limits(100, 32, kj::str("\"workerId\":\"a/oom-report\","), nullptr, newGroup());
  auto* isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  v8::HandleScope handles(isolate);
  auto identity = kj::str("\"workerId\":\"a/oom-report\",\"invocationId\":\"i1\",");
  auto report = oomReportText(identity, 32);
  KJ_EXPECT(report == kj::str("{\"event\":\"memory-culprit\",\"pid\":", getpid(), ",", identity,
      "\"memoryLimitMiB\":32}\n"));
  for (int turn = 0; turn < 2; ++turn) {
    std::atomic<LimitCause> cause{LimitCause::NONE};
    CpuExecution execution(
        isolate, cause, limits.memoryState, limits.memory, 0, 100, 32, identity, &report);
    KJ_EXPECT(currentOomReport == report.cStr());
    KJ_EXPECT(execution.entry.identity.begin() == identity.begin());
    execution.finish();
  }
  KJ_EXPECT(currentOomReport == nullptr);
}

KJ_TEST("CPU remaining budget, previous CPU, nested attribution and late termination") {
  initializeV8();
  IsolateLimits limits(100, 32, kj::str("\"workerId\":\"a/cpu\","), nullptr, newGroup());
  auto* isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  v8::HandleScope handles(isolate);
  auto context = v8::Context::New(isolate);
  v8::Context::Scope inContext(context);
  auto healthySource = v8::String::NewFromUtf8Literal(isolate, "6 * 7");
  auto healthyScript = v8::Script::Compile(context, healthySource).ToLocalChecked();
  std::atomic<LimitCause> cause{LimitCause::NONE}, previous{LimitCause::NONE};
  limits.memory->pressure();
  KJ_EXPECT(limits.memory->pendingPressure.load() && !isolate->IsExecutionTerminating());
  {
    CpuExecution deferred(
        isolate, cause, limits.memoryState, limits.memory, 0, 100, 32, limits.identity);
    KJ_EXPECT(cause.load() == LimitCause::PROCESS_MEMORY && !limits.memory->pendingPressure.load());
    deferred.finish();
  }
  KJ_EXPECT(limits.memory->currentCause == nullptr && !isolate->IsExecutionTerminating());
  cause.store(LimitCause::NONE);
  limits.memory->currentCause = &previous;
  CpuExecution execution(
      isolate, cause, limits.memoryState, limits.memory, 90000000, 100, 32, limits.identity);
  KJ_EXPECT(execution.entry.allowance == 10000000 && execution.entry.previousCpu == 90000000);
  KJ_EXPECT(limits.memory->currentCause == &cause);
  cause.store(LimitCause::CPU);
  isolate->TerminateExecution();
  v8::TryCatch caught(isolate);
  // Consume the termination in a real JS entry. IsExecutionTerminating() alone
  // cannot distinguish an unprocessed interrupt from a successfully canceled one.
  auto stopped = healthyScript->Run(context);
  KJ_EXPECT(stopped.IsEmpty() && caught.HasTerminated());
  execution.finish();
  KJ_EXPECT(!isolate->IsExecutionTerminating() && limits.memory->currentCause == &previous);
  caught.Reset();
  auto healed = healthyScript->Run(context);
  KJ_EXPECT(!healed.IsEmpty());
  if (!healed.IsEmpty()) KJ_EXPECT(healed.ToLocalChecked()->Int32Value(context).FromJust() == 42);
  // Cleanup also makes a deliberately broken cancellation safe for later tests.
  isolate->CancelTerminateExecution();
  caught.Reset();
  execution.finish();
  KJ_EXPECT(limits.memory->currentCause == &previous);
  {
    // Isolate the CPU scope's cancellation from heap-statistic API calls.
    std::atomic<LimitCause> cpuCause{LimitCause::NONE};
    CpuExecution cpuOnly(
        isolate, cpuCause, limits.memoryState, nullptr, 0, 100, 32, limits.identity);
    cpuCause.store(LimitCause::CPU);
    isolate->TerminateExecution();
    auto terminated = healthyScript->Run(context);
    KJ_EXPECT(terminated.IsEmpty() && caught.HasTerminated());
    cpuOnly.finish();
    caught.Reset();
    auto nextTurn = healthyScript->Run(context);
    KJ_EXPECT(!nextTurn.IsEmpty());
    if (!nextTurn.IsEmpty())
      KJ_EXPECT(nextTurn.ToLocalChecked()->Int32Value(context).FromJust() == 42);
    isolate->CancelTerminateExecution();
    caught.Reset();
  }
}

void allocationComponent(kj::StringPtr code) {
  initializeV8();
  IsolateLimits neighborLimits(1000, 32, kj::str("\"workerId\":\"a/neighbor\","), nullptr, newGroup());
  auto* neighbor = v8::Isolate::New(neighborLimits.group, neighborLimits.getCreateParams());
  KJ_DEFER(neighbor->Dispose());
  customizeEntered(neighborLimits, neighbor);
  auto checkNeighbor = [&] {
    v8::Locker lock(neighbor);
    v8::Isolate::Scope scope(neighbor);
    v8::HandleScope handles(neighbor);
    auto context = v8::Context::New(neighbor);
    v8::Context::Scope entered(context);
    auto source = v8::String::NewFromUtf8Literal(neighbor, "6 * 7");
    auto script = v8::Script::Compile(context, source).ToLocalChecked();
    KJ_EXPECT(script->Run(context).ToLocalChecked()->Int32Value(context).FromJust() == 42);
  };
  checkNeighbor();
  auto budget = std::make_shared<ResourceBudget>(1ull << 30, 4, 1);
  auto reservation = std::make_shared<ResourceReservation>(budget, 32);
  KJ_ASSERT(reservation->registerPreparation());
  IsolateLimits limits(1000, 32, kj::str("\"workerId\":\"a/heap\","), reservation, newGroup());
  auto params = limits.getCreateParams();
  KJ_EXPECT(params.constraints.max_old_generation_size_in_bytes() > (1ull << 30));
  auto* isolate = v8::Isolate::New(limits.group, params);
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  v8::HandleScope handles(isolate);
  auto context = v8::Context::New(isolate);
  v8::Context::Scope inContext(context);
  std::atomic<LimitCause> cause{LimitCause::NONE};
  CpuExecution execution(
      isolate, cause, limits.memoryState, limits.memory, 0, 1000, 32, limits.identity);
  v8::TryCatch caught(isolate);
  auto source =
      v8::String::NewFromUtf8(isolate, code.begin(), v8::NewStringType::kNormal, code.size())
          .ToLocalChecked();
  auto script = v8::Script::Compile(context, source).ToLocalChecked();
  auto result = script->Run(context);
  caught.Reset();
  execution.finish();
  KJ_EXPECT(limits.memoryState->load() == LimitCause::MEMORY);
  KJ_EXPECT(!isolate->IsExecutionTerminating());
  auto metrics = reservation->metrics();
  KJ_EXPECT(reservation->heapPhysical > 0);
  KJ_EXPECT(threaded::rawField(metrics, "heapPhysicalBytes") ==
      std::to_string(reservation->heapPhysical));
  checkNeighbor();
}

KJ_TEST("a Worker's ArrayBuffers lie in its own isolate group's sandbox") {
  // MemoryBudget allocates in the group it was given: with a V8 sandbox per group, V8 ends the
  // process on the first backing store outside the isolate's own sandbox.
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(1ull << 30, 4, 1);
  auto reservation = std::make_shared<ResourceReservation>(budget, 32);
  KJ_ASSERT(reservation->registerPreparation());
  auto other = newGroup();
  IsolateLimits limits(1000, 32, kj::str("\"workerId\":\"a/buffers\","), reservation, newGroup());
  auto* isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  auto store = v8::ArrayBuffer::NewBackingStore(isolate, 1 << 20);
  KJ_EXPECT(limits.group.SandboxContains(store->Data()));
  KJ_EXPECT(!other.SandboxContains(store->Data()) || !v8::IsolateGroup::CanCreateNewGroups());
  // V8 refuses a buffer above the group's pool before reserving anything; the Worker's own limit
  // (32 MiB here) is not V8's maximum (r50 §6.5 B2: the next test).
  if (v8::IsolateGroup::CanCreateNewGroups()) {
    KJ_EXPECT(limits.memory->MaxAllocationSize() == server::SANDBOX_POOL_BYTES);
  } else {
    KJ_EXPECT(limits.memory->MaxAllocationSize() > (32ull << 20));
  }
}

KJ_TEST("one ArrayBuffer above the memory limit ends the execution as MEMORY_LIMIT_EXCEEDED") {
  // With or without the V8 sandbox: the buffer reaches the budget, which marks MEMORY and ends
  // the execution, never a RangeError the Worker could catch (r50 §6.5 B2).
  allocationComponent(
      "try { globalThis.retained = new ArrayBuffer(64 * 1024 * 1024); } catch (e) { globalThis.caught = e; }");
}

KJ_TEST("a refused isolate group is retried, not kept as the Worker's failure") {
  // compile() throws capacityError() when tryNewIsolateGroup() refuses (no room in the address
  // space for another V8 sandbox). Kept in the slot, every request for that Worker answered 503
  // in this process until the slot idled out (r50 §6.5 B1).
  kj::Maybe<kj::Exception> refused = capacityError();
  KJ_EXPECT(loadFailureRetryable(refused, true, false));
  KJ_EXPECT(isCapacityError(KJ_ASSERT_NONNULL(refused)));
  // A tenant's own failure stays the Worker's failure, unless the source was never fetched.
  kj::Maybe<kj::Exception> script = KJ_EXCEPTION(FAILED, "jsg.Error: the Worker's script threw");
  KJ_EXPECT(!loadFailureRetryable(script, true, false));
  KJ_EXPECT(loadFailureRetryable(script, false, false));
  kj::Maybe<kj::Exception> canceled = KJ_EXCEPTION(DISCONNECTED, "canceled before publication");
  KJ_EXPECT(loadFailureRetryable(canceled, true, false));
  // Memory pressure at startup becomes a capacity refusal, even of a load that succeeded.
  kj::Maybe<kj::Exception> loaded;
  KJ_EXPECT(!loadFailureRetryable(loaded, true, false) && loaded == kj::none);
  KJ_EXPECT(loadFailureRetryable(loaded, true, true));
  KJ_EXPECT(isCapacityError(KJ_ASSERT_NONNULL(loaded)));
}

KJ_TEST("a refused isolate group lowers the process's resident limit to its residents") {
  // r50 §6.5 B3: the next Worker then evicts an idle one instead of being refused again.
  auto budget = std::make_shared<ResourceBudget>(1ull << 30, 1840, 1);
  std::vector<std::shared_ptr<ResourceReservation>> residents;
  for (int i = 0; i < 4; i++) {
    residents.push_back(std::make_shared<ResourceReservation>(budget, 32));
    KJ_ASSERT(residents.back()->registerPreparation());
  }
  // The fourth one's group was refused: three residents besides it.
  noSandboxRoom(*budget);
  KJ_EXPECT(budget->residentMaximum.load() == 3 && budget->configuredResidentMaximum == 1840,
      budget->residentMaximum.load());
  residents.pop_back();
  auto next = std::make_shared<ResourceReservation>(budget, 32);
  KJ_EXPECT(!next->registerPreparation());
  // Lowered again to fewer residents, never raised back.
  residents.pop_back();
  noSandboxRoom(*budget);
  KJ_EXPECT(budget->residentMaximum.load() == 1, budget->residentMaximum.load());
  for (int i = 0; i < 2; i++) {
    residents.push_back(std::make_shared<ResourceReservation>(budget, 32));
    (void)residents.back()->registerPreparation();
  }
  noSandboxRoom(*budget);
  KJ_EXPECT(budget->residentMaximum.load() == 1, budget->residentMaximum.load());
}

KJ_TEST("single large heap allocation preserves the process") {
  allocationComponent("globalThis.retained = new Array(12*1024*1024).fill(7)");
}
KJ_TEST("continuous heap growth preserves the process") {
  allocationComponent(
      "globalThis.retained=[]; while(true) retained.push(new Array(50000).fill(7))");
}

// One V8 heap object of about 1 GiB (a flattened two-byte string of the
// maximum length) with almost nothing else live, and the dictionary-to-array
// growth of a 128 Mi-element array (the first reproduction's shape; one alone
// peaks at about 1.75 GiB and survives).
constexpr kj::StringPtr NEAR_GIB_STRING =
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;
constexpr kj::StringPtr NEAR_GIB_ARRAY =
    "globalThis.retained = new Array(128*1024*1024-8192).fill(1.5)"_kj;
// About 126 MiB live in small arrays (old-space pages, with their
// fragmentation), just under the 128 MiB limit, then one maximum string.
constexpr kj::StringPtr LIVE126_THEN_STRING =
    "globalThis.kept = []; for (let i = 0; i < 126 * 128; ++i) kept.push(new Array(1024).fill(1.5));"
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;
// The same with large regular objects, which pack worst into V8's 256 KiB
// pages: double arrays of 88,016 bytes (two per page, about a third wasted) and
// of 130,416 bytes (one per page, about half wasted), about 126 MiB of them.
constexpr kj::StringPtr LIVE126_86K_THEN_STRING =
    "globalThis.kept = []; for (let i = 0; i < 1501; ++i) kept.push(new Array(11000).fill(1.5));"
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;
// A 500 MB one-byte string first: V8 takes the first object of the young
// large-object space without checking its capacity or collecting, so the
// tenant check (after a collection) does not see it; then one maximum string.
// Before the near-heap-limit callback this ended the process (exit 74,
// raw/s27-x500).
constexpr kj::StringPtr YOUNG500_THEN_STRING =
    "const a = ' '.repeat(5e8); a.indexOf('z'); globalThis.kept = a;"
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;
constexpr kj::StringPtr LIVE126_128K_THEN_STRING =
    "globalThis.kept = []; for (let i = 0; i < 1013; ++i) kept.push(new Array(16300).fill(1.5));"
    "globalThis.retained = '\\u0100'.repeat(2**29 - 24); globalThis.retained.indexOf('z')"_kj;

// Long operations that check no interrupt while they allocate: after the
// near-heap-limit callback has raised the ceiling and requested the
// termination, they keep allocating until they meet the raised ceiling (v137
// §6.28-§6.29, v138 §4).
// JSON.parse of 120 million empty objects (about 3.4 GB) from a 360 MB text.
constexpr kj::StringPtr JSON_OBJECTS =
    "globalThis.retained = JSON.parse('[' + '{},'.repeat(1.2e8) + '{}]'); retained.length"_kj;
// JSON.parse of one number more than the largest array: no V8 allocation
// while parsing, then a backing store V8 cannot allocate.
constexpr kj::StringPtr JSON_NUMBERS =
    "globalThis.retained = JSON.parse('[' + '1,'.repeat(2**27) + '1]'); retained.length"_kj;
// Straight-line code: no loop and no JavaScript call, so no interrupt check,
// six flattened 500 MB strings one after another (the shape of the §6.29
// verifier).
constexpr kj::StringPtr STRAIGHT_LINE =
    "const a = ' '.repeat(5e8); a.indexOf('z'); const b = a + '1'; b.indexOf('z');"
    "const c = b + '2'; c.indexOf('z'); const d = c + '3'; d.indexOf('z');"
    "const e = d + '4'; e.indexOf('z'); const f = e + '5'; f.indexOf('z');"
    "globalThis.kept = [a, b, c, d, e, f]; kept.length"_kj;
// A structured clone (ValueSerializer and ValueDeserializer, as
// jsg::structuredClone) of four maximum two-byte strings: the serializer
// flattens each, 4 GiB in all, more than the shared cage leaves.
constexpr kj::StringPtr CLONE_STRINGS =
    "globalThis.retained = clone([0, 1, 2, 3].map(i => String.fromCharCode(256 + i).repeat(2**29 - 24)));"
    "retained.length"_kj;
// replaceAll with a string replacement: a new ConsString per match and no
// JavaScript call, 250 million matches (§8.2 of the v138 task).
constexpr kj::StringPtr REPLACE_ALL =
    "const s = 'a'.repeat(2.5e8); s.indexOf('z'); globalThis.retained = s.replaceAll('a', 'b'); retained.length"_kj;
// String.prototype.split into 120 million three-character strings (about
// 1.9 GB) and their array.
constexpr kj::StringPtr SPLIT_PARTS =
    "globalThis.retained = 'abc,'.repeat(1.2e8).split(','); retained.length"_kj;
// Splits into one part more than the largest array, by a separator and into
// characters.
constexpr kj::StringPtr SPLIT_TOO_MANY =
    "globalThis.retained = 'a,'.repeat(2**27).split(','); retained.length"_kj;
constexpr kj::StringPtr SPLIT_CHARACTERS =
    "const s = ' '.repeat(2**27 + 1); s.indexOf('z'); globalThis.retained = s.split(''); retained.length"_kj;
// One element past the largest array after the numbers' fast path (the first
// element it hands on was not counted), and past half of it with a reviver
// (the parse nodes and snapshots take twice the array's length); one property
// past the largest JSON object (2^24, by duplicate keys: no allocation).
constexpr kj::StringPtr JSON_MIXED =
    "globalThis.retained = JSON.parse('[' + '1,'.repeat(2**27) + '\"x\"]'); retained.length"_kj;
constexpr kj::StringPtr JSON_REVIVER =
    "globalThis.retained = JSON.parse('[' + '1,'.repeat(2**26) + '1]', (k, v) => v); retained.length"_kj;
constexpr kj::StringPtr JSON_PROPERTIES =
    "globalThis.retained = JSON.parse('{' + '\"a\":1,'.repeat(2**24) + '\"a\":1}'); 1"_kj;
// The same object with a reviver takes the iterative parser and its own check. Both must throw the RangeError:
// without the limit the repeated key collapses into one property and the parse completes.
constexpr kj::StringPtr JSON_PROPERTIES_REVIVER =
    "globalThis.retained = JSON.parse('{' + '\"a\":1,'.repeat(2**24) + '\"a\":1}', (k, v) => v); 1"_kj;
// The verifier's sweep of V8 builtins (v138 §8.4), the shapes that ended the
// process on cwkd-p40: a sort of an array-like object or a string sized its
// work array by the length; a global replace with a function collected every
// match before the first call; a global atom match allocated one array for
// every match it counted.
constexpr kj::StringPtr SORT_STRING =
    "Array.prototype.sort.call('x'.repeat(2**28)); 1"_kj;
// The work array of such a sort grows as it collects (v138 §8.5): past 89,989,624 elements its next
// growth went beyond the largest FixedArray and ended the process for a tenant whose memory limit
// lets it get there; it stops at the largest now, and one element more throws a RangeError.
constexpr kj::StringPtr SORT_STRING_90M = "Array.prototype.sort.call('x'.repeat(9e7)); 1"_kj;
constexpr kj::StringPtr TO_SORTED_STRING_90M =
    "globalThis.retained = Array.prototype.toSorted.call('x'.repeat(9e7)); retained.length"_kj;
constexpr kj::StringPtr SORT_STRING_PAST_LARGEST =
    "Array.prototype.sort.call('x'.repeat(2**27 + 1)); 1"_kj;
constexpr kj::StringPtr REPLACE_FUNCTION =
    "globalThis.retained = 'x'.repeat(2**27 + 1).replace(/x/g, () => ''); retained.length"_kj;
constexpr kj::StringPtr MATCH_ATOM =
    "globalThis.retained = 'x'.repeat(2**27 + 1).match(/x/g); retained.length"_kj;
// Loops that allocate nothing while they run: their interrupt checks are what
// lets a short CPU limit end the call (each takes about a second unchecked).
// The numbers' fast path: long decimals, about 2 s of parsing on cwkd-p40 after about 0.15 s of flattening the text.
// Its CPU limit (500 ms, below) expires inside the number loop, so only that loop's check can end the call in time;
// with the 50 ms of the others the limit expired while the text was flattened and the first value's check ended it.
constexpr kj::StringPtr CPU_JSON_NUMBERS =
    "globalThis.retained = JSON.parse('[' + '1.2345678901234567890123e10,'.repeat(2**23) + '1]'); retained.length"_kj;
constexpr kj::StringPtr CPU_JSON_SLOW =
    "globalThis.retained = JSON.parse('[' + '\"a\",'.repeat(2**24) + '\"a\"]'); retained.length"_kj;
constexpr kj::StringPtr CPU_JSON_REVIVER =
    "globalThis.retained = JSON.parse('[' + '\"a\",'.repeat(2**24) + '\"a\"]', (k, v) => v); retained.length"_kj;
// An array-like object's length alone: a work array of that length ended the
// process at once; now the loop over its indices takes interrupts.
constexpr kj::StringPtr CPU_SORT_LENGTH = "Array.prototype.sort.call({length: 2**30}); 1"_kj;
constexpr kj::StringPtr CPU_TO_SORTED_LENGTH =
    "Array.prototype.toSorted.call({length: 2**28}).length"_kj;
// (split's check every 1,024 parts is in the loop that creates them, after
// the search for the separators, which checks none: it is tested by the parts
// outgrowing a 2 GiB budget, "split" above.)

// structuredClone for the plain test contexts: V8's serializer and
// deserializer with the default delegate, as jsg::structuredClone uses them.
void cloneForTest(const v8::FunctionCallbackInfo<v8::Value>& info) {
  auto* isolate = info.GetIsolate();
  auto context = isolate->GetCurrentContext();
  v8::ValueSerializer serializer(isolate);
  serializer.WriteHeader();
  if (serializer.WriteValue(context, info[0]).IsNothing()) return;
  auto released = serializer.Release();
  KJ_DEFER(free(released.first));
  v8::ValueDeserializer deserializer(isolate, released.first, released.second);
  v8::Local<v8::Value> result;
  if (deserializer.ReadHeader(context).FromMaybe(false) &&
      deserializer.ReadValue(context).ToLocal(&result))
    info.GetReturnValue().Set(result);
}

// What a child's allocating call must end with: the memory limit, or the
// memory limit or a RangeError (an array V8 cannot allocate), or anything
// (an exploration run).
enum class Outcome { MEMORY, MEMORY_OR_RANGE, RANGE, CPU, ANY };
// A child's limits: the process budget, each allocating isolate's memory and
// CPU limits, and (CPU outcome) how long its call may run at most.
struct ChildLimits {
  uint64_t budgetBytes = 16ull << 30;
  uint32_t memoryMiB = 128, cpuMs = 10000;
  double maxSeconds = 0;
  // When set, the largest heap the isolate may reach: a limit that took effect late (an interrupt check missing)
  // still ends the call at the memory limit, only far above it.
  uint64_t maxPeakBytes = 0;
};

// `count` isolates allocate at the same moment while a neighbor isolate keeps
// answering. Every heap of this build shares one pointer cage.
// `raised`: whether each isolate's ceiling must have been raised by the
// near-heap-limit callback (-1: not checked). The fragmentation reserve alone
// must let one maximum object finish after up to 126 MiB live, so those
// shapes never need the callback; only an object V8 placed without a
// collection (the 500 MB young one) does. Without this check the callback
// would hide a missing reserve.
void simultaneousAllocations(size_t count, kj::StringPtr code, int raised = -1,
    Outcome outcome = Outcome::MEMORY, ChildLimits childLimits = {}) {
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(childLimits.budgetBytes, 8, count + 1);
  std::vector<double> seconds(count);
  std::vector<char> cpuEnded(count);
  std::vector<kj::Own<IsolateLimits>> limits;
  std::vector<v8::Isolate*> isolates;
  std::vector<uint64_t> peaks(count);
  std::vector<char> refused(count), rangeError(count);
  for (size_t i = 0; i < count; ++i) {
    auto reservation = std::make_shared<ResourceReservation>(budget, childLimits.memoryMiB);
    KJ_ASSERT(reservation->registerPreparation());
    limits.push_back(kj::refcounted<IsolateLimits>(childLimits.cpuMs, childLimits.memoryMiB,
        kj::str("\"workerId\":\"a/large-", i, "\","), reservation, newGroup()));
    isolates.push_back(v8::Isolate::New(limits[i]->group, limits[i]->getCreateParams()));
    customizeEntered(*limits[i], isolates[i]);
  }
  KJ_DEFER(for (auto* isolate : isolates) isolate->Dispose());
  // No resident is idle during this test. Wake growers just as a maintenance
  // pass with no eviction candidate does, without holding their reservation lock.
  std::atomic<bool> finished{false};
  std::thread maintenance([&] {
    while (!finished.load()) {
      if (budget->reclaimRequested.exchange(false)) budget->finishReclaim(true);
      std::this_thread::yield();
    }
  });
  KJ_DEFER(finished.store(true); maintenance.join());
  // The neighbor is an ordinary resident: its own reservation in the same
  // budget and the default 128 MiB limit.
  auto neighborReservation = std::make_shared<ResourceReservation>(budget, 128);
  KJ_ASSERT(neighborReservation->registerPreparation());
  IsolateLimits neighborLimits(
      10000, 128, kj::str("\"workerId\":\"a/neighbor\","), neighborReservation, newGroup());
  auto* neighbor = v8::Isolate::New(neighborLimits.group, neighborLimits.getCreateParams());
  KJ_DEFER(neighbor->Dispose());
  customizeEntered(neighborLimits, neighbor);
  auto runNeighbor = [&](kj::StringPtr code, int32_t expected) {
    v8::Locker lock(neighbor);
    v8::Isolate::Scope scope(neighbor);
    v8::HandleScope handles(neighbor);
    auto context = v8::Context::New(neighbor);
    v8::Context::Scope entered(context);
    std::atomic<LimitCause> cause{LimitCause::NONE};
    CpuExecution execution(neighbor, cause, neighborLimits.memoryState, neighborLimits.memory, 0,
        10000, 128, neighborLimits.identity);
    v8::TryCatch caught(neighbor);
    auto source =
        v8::String::NewFromUtf8(neighbor, code.begin(), v8::NewStringType::kNormal, code.size())
            .ToLocalChecked();
    auto result = v8::Script::Compile(context, source).ToLocalChecked()->Run(context);
    bool answered =
        !result.IsEmpty() && result.ToLocalChecked()->Int32Value(context).FromJust() == expected;
    caught.Reset();
    execution.finish();
    KJ_EXPECT(answered && cause.load() == LimitCause::NONE &&
            neighborLimits.memoryState->load() == LimitCause::NONE,
        code);
  };
  auto checkNeighbor = [&] { runNeighbor("6 * 7"_kj, 42); };
  checkNeighbor();
  const auto participants = static_cast<std::ptrdiff_t>(count + 1);
  std::barrier start(participants), completed(participants), release(participants);
  std::vector<std::thread> workers;
  for (size_t i = 0; i < count; ++i)
    workers.emplace_back([&, i] {
      auto* isolate = isolates[i];
      v8::Locker lock(isolate);
      v8::Isolate::Scope scope(isolate);
      v8::HandleScope handles(isolate);
      auto context = v8::Context::New(isolate);
      v8::Context::Scope entered(context);
      KJ_ASSERT(context->Global()
              ->Set(context, v8::String::NewFromUtf8Literal(isolate, "clone"),
                  v8::Function::New(context, cloneForTest).ToLocalChecked())
              .FromJust());
      auto source =
          v8::String::NewFromUtf8(isolate, code.begin(), v8::NewStringType::kNormal, code.size())
              .ToLocalChecked();
      auto script = v8::Script::Compile(context, source).ToLocalChecked();
      start.arrive_and_wait();
      std::atomic<LimitCause> cause{LimitCause::NONE};
      CpuExecution execution(isolate, cause, limits[i]->memoryState, limits[i]->memory, 0,
          childLimits.cpuMs, childLimits.memoryMiB, limits[i]->identity);
      v8::TryCatch caught(isolate);
      const auto began = std::chrono::steady_clock::now();
      auto result = script->Run(context);
      seconds[i] = std::chrono::duration<double>(std::chrono::steady_clock::now() - began).count();
      cpuEnded[i] = cause.load() == LimitCause::CPU;
      std::string thrown;
      if (result.IsEmpty() && caught.HasCaught() && !caught.HasTerminated()) {
        v8::String::Utf8Value text(isolate, caught.Exception());
        if (*text != nullptr) thrown = *text;
      }
      rangeError[i] = thrown.starts_with("RangeError");
      fprintf(stdout, "ALLOCATION_RESULT worker=%zu completed=%d terminated=%d cpu=%d seconds=%.3f thrown=\"%s\"\n", i,
          !result.IsEmpty(), caught.HasTerminated(), int(cpuEnded[i]), seconds[i], thrown.c_str());
      fflush(stdout);
      completed.arrive_and_wait();
      release.arrive_and_wait();
      caught.Reset();
      execution.finish();
      peaks[i] = limits[i]->memory->peakUsedBytes.load();
      refused[i] = limits[i]->memoryState->load() == LimitCause::MEMORY ||
          cause.load() == LimitCause::PROCESS_MEMORY;
    });
  start.arrive_and_wait();
  checkNeighbor();
  completed.arrive_and_wait();
  // While the large objects are alive the neighbor still allocates 32 MiB.
  checkNeighbor();
  runNeighbor("globalThis.kept = new Array(4 * 1024 * 1024).fill(1.5); kept.length"_kj,
      4 * 1024 * 1024);
  release.arrive_and_wait();
  for (auto& worker: workers) worker.join();
  for (size_t i = 0; i < count; ++i) {
    const bool limited = refused[i] && peaks[i] >= (128ull << 20);
    if (outcome == Outcome::MEMORY) KJ_EXPECT(limited, i, peaks[i]);
    if (outcome == Outcome::MEMORY_OR_RANGE)
      KJ_EXPECT(limited || rangeError[i], i, peaks[i], int(refused[i]));
    if (outcome == Outcome::RANGE) KJ_EXPECT(rangeError[i], i, peaks[i]);
    if (childLimits.maxPeakBytes != 0) KJ_EXPECT(peaks[i] <= childLimits.maxPeakBytes, i, peaks[i]);
    if (outcome == Outcome::CPU)
      KJ_EXPECT(cpuEnded[i] && seconds[i] <= childLimits.maxSeconds, i, seconds[i], int(cpuEnded[i]));
    const int ceilingRaised = limits[i]->memory->ceilingRaised.load();
    if (raised >= 0) KJ_EXPECT(ceilingRaised == raised, i, ceilingRaised, raised);
    fprintf(stdout, "CONCURRENT_HEAP worker=%zu peak=%llu separateCages=%d ceilingRaised=%d\n", i,
        static_cast<unsigned long long>(peaks[i]), v8::IsolateGroup::CanCreateNewGroups(), ceilingRaised);
  }
  checkNeighbor();
}

// Scenarios that can end the process run in a fresh copy of this binary,
// selected by the parent through the environment; otherwise this is a no-op.
constexpr uint ALLOCATION_CHILD_LINE = __LINE__ + 1;
KJ_TEST("child body: simultaneous near-GiB heap allocations, selected by a parent test") {
  auto selected = getenv("COWORKERD_THREADED_ALLOCATION_CHILD");
  if (selected == nullptr) return;
  kj::StringPtr spec(selected);
  auto colon = KJ_REQUIRE_NONNULL(spec.findFirst(':'), "expected <count>:<kind>");
  auto count = kj::str(spec.first(colon)).parseAs<uint>();
  auto kind = spec.slice(colon + 1);
  struct Shape {
    kj::StringPtr name, code;
    int raised;
    Outcome outcome;
    ChildLimits limits = {};
  };
  // The CPU shapes: a 50 ms limit and a 1 GiB heap (no memory limit gets in
  // the way); the call must end within 0.4 s.
  const ChildLimits cpu{.memoryMiB = 1024, .cpuMs = 50, .maxSeconds = 0.4};
  // `raised` only for one isolate: -1 is not checked.
  const Shape shapes[] = {
    {"string"_kj, NEAR_GIB_STRING, 0, Outcome::MEMORY},
    {"array"_kj, NEAR_GIB_ARRAY, 0, Outcome::MEMORY},
    {"live126"_kj, LIVE126_THEN_STRING, 0, Outcome::MEMORY},
    {"live86k"_kj, LIVE126_86K_THEN_STRING, 0, Outcome::MEMORY},
    {"live128k"_kj, LIVE126_128K_THEN_STRING, 0, Outcome::MEMORY},
    {"young500"_kj, YOUNG500_THEN_STRING, 1, Outcome::MEMORY},
    {"json"_kj, JSON_OBJECTS, -1, Outcome::MEMORY},
    {"json-numbers"_kj, JSON_NUMBERS, -1, Outcome::MEMORY_OR_RANGE},
    {"straight"_kj, STRAIGHT_LINE, -1, Outcome::MEMORY},
    {"clone"_kj, CLONE_STRINGS, -1, Outcome::MEMORY},
    // In a 2 GiB process budget: unchecked, the parts outgrow every raise.
    {"split"_kj, SPLIT_PARTS, -1, Outcome::MEMORY, {.budgetBytes = 2ull << 30}},
    {"split-many"_kj, SPLIT_TOO_MANY, -1, Outcome::MEMORY_OR_RANGE},
    {"split-chars"_kj, SPLIT_CHARACTERS, -1, Outcome::MEMORY_OR_RANGE},
    {"replace-all"_kj, REPLACE_ALL, -1, Outcome::MEMORY},
    {"json-mixed"_kj, JSON_MIXED, -1, Outcome::MEMORY_OR_RANGE},
    {"json-reviver"_kj, JSON_REVIVER, -1, Outcome::MEMORY_OR_RANGE},
    {"json-properties"_kj, JSON_PROPERTIES, -1, Outcome::RANGE},
    {"json-properties-reviver"_kj, JSON_PROPERTIES_REVIVER, -1, Outcome::RANGE},
    {"cpu-json-numbers"_kj, CPU_JSON_NUMBERS, -1, Outcome::CPU, {.memoryMiB = 1024, .cpuMs = 500, .maxSeconds = 1.2}},
    {"cpu-json-slow"_kj, CPU_JSON_SLOW, -1, Outcome::CPU, cpu},
    {"cpu-json-reviver"_kj, CPU_JSON_REVIVER, -1, Outcome::CPU, cpu},
    {"sort-string"_kj, SORT_STRING, -1, Outcome::MEMORY},
    // The matches' collection takes interrupts: it ends near the limit (about 135 MB), not after the builder's
    // last doubling past 1 GB.
    {"replace-function"_kj, REPLACE_FUNCTION, -1, Outcome::MEMORY, {.maxPeakBytes = 512ull << 20}},
    // About 8.5 s of CPU to collect 2^27 matches: a CPU limit well above it, so that only the RangeError ends it
    // (a busy machine took it past the default 10 s once).
    {"replace-function-high"_kj, REPLACE_FUNCTION, -1, Outcome::RANGE, {.memoryMiB = 1024, .cpuMs = 60000}},
    {"match-atom"_kj, MATCH_ATOM, -1, Outcome::MEMORY_OR_RANGE},
    {"sort-string-90m"_kj, SORT_STRING_90M, -1, Outcome::ANY, {.memoryMiB = 1024, .cpuMs = 60000}},
    {"to-sorted-string-90m"_kj, TO_SORTED_STRING_90M, -1, Outcome::ANY, {.memoryMiB = 1024, .cpuMs = 60000}},
    {"sort-string-past-largest"_kj, SORT_STRING_PAST_LARGEST, -1, Outcome::RANGE, {.memoryMiB = 2048, .cpuMs = 60000}},
    {"cpu-sort-length"_kj, CPU_SORT_LENGTH, -1, Outcome::CPU, cpu},
    {"cpu-to-sorted-length"_kj, CPU_TO_SORTED_LENGTH, -1, Outcome::CPU, cpu},
  };
  KJ_REQUIRE(count > 0 && count <= 8);
  if (kind == "code") {
    // Exploration of other shapes (not a test): the code comes from the
    // environment, the outcome is only printed.
    const char* code = getenv("COWORKERD_THREADED_ALLOCATION_CODE");
    KJ_REQUIRE(code != nullptr, "COWORKERD_THREADED_ALLOCATION_CODE is not set");
    ChildLimits explored;
    auto number = [](const char* name, uint64_t fallback) {
      const char* value = getenv(name);
      return value ? std::strtoull(value, nullptr, 10) : fallback;
    };
    explored.budgetBytes = number("COWORKERD_THREADED_ALLOCATION_BUDGET_BYTES", explored.budgetBytes);
    explored.memoryMiB = uint32_t(number("COWORKERD_THREADED_ALLOCATION_MEMORY_MIB", explored.memoryMiB));
    explored.cpuMs = uint32_t(number("COWORKERD_THREADED_ALLOCATION_CPU_MS", explored.cpuMs));
    simultaneousAllocations(count, kj::StringPtr(code), -1, Outcome::ANY, explored);
  } else {
    const Shape* found = nullptr;
    for (auto& shape: shapes)
      if (shape.name == kind) found = &shape;
    KJ_REQUIRE(found != nullptr, "unknown allocation shape", kind);
    simultaneousAllocations(count, found->code, count == 1 ? found->raised : -1, found->outcome, found->limits);
  }
  fprintf(stdout, "ALLOCATION_CHILD survived count=%u kind=%s\n", count, kind.cStr());
  fflush(stdout);
}

struct ChildResult {
  int status;
  std::string events;  // the rows the child wrote to its limit event descriptor
};
ChildResult runTestChild(uint line, kj::String selector) {
  auto filter = kj::str("--filter=threaded-server-test.c++:", line);
  // The child's limit event descriptor, as the node passes one to the server.
  int events[2];
  KJ_REQUIRE(pipe2(events, O_CLOEXEC) == 0);
  int childEvents = fcntl(events[1], F_DUPFD, 100);
  KJ_REQUIRE(childEvents >= 0);
  auto eventFd = kj::str("COWORKERD_EVENT_FD=", childEvents);
  kj::Vector<char*> env;
  for (char** at = environ; *at != nullptr; ++at)
    if (!kj::StringPtr(*at).startsWith("COWORKERD_EVENT_FD=")) env.add(*at);
  env.add(selector.begin());
  env.add(eventFd.begin());
  env.add(nullptr);
  char program[] = "/proc/self/exe";
  char* argv[] = {program, filter.begin(), nullptr};
  fflush(stdout);
  fflush(stderr);
  pid_t pid;
  KJ_REQUIRE(posix_spawn(&pid, program, nullptr, nullptr, argv, env.begin()) == 0);
  close(childEvents);
  close(events[1]);
  // A child that neither finishes nor exits within a minute is a failure of
  // its own kind (reported as killed by SIGKILL), never a silent pass.
  int status = 0;
  for (unsigned waited = 0;; ++waited) {
    auto done = waitpid(pid, &status, WNOHANG);
    if (done < 0 && errno == EINTR) continue;
    KJ_REQUIRE(done == pid || done == 0);
    if (done == pid) break;
    if (waited == 600) kill(pid, SIGKILL);
    usleep(100000);
  }
  std::string rows;
  char buffer[4096];
  for (ssize_t n; (n = read(events[0], buffer, sizeof(buffer))) != 0;) {
    if (n < 0 && errno == EINTR) continue;
    KJ_REQUIRE(n > 0);
    rows.append(buffer, n);
  }
  close(events[0]);
  return {status, kj::mv(rows)};
}

ChildResult runAllocationChild(kj::StringPtr spec) {
  auto [status, rows] =
      runTestChild(ALLOCATION_CHILD_LINE, kj::str("COWORKERD_THREADED_ALLOCATION_CHILD=", spec));
  fprintf(stdout, "ALLOCATION_CHILD spec=%s exited=%d code=%d signal=%d\n", spec.cStr(),
      WIFEXITED(status), WIFEXITED(status) ? WEXITSTATUS(status) : -1,
      WIFSIGNALED(status) ? WTERMSIG(status) : 0);
  fprintf(stdout, "ALLOCATION_CHILD_EVENTS %zu bytes\n%s", rows.size(), rows.c_str());
  fflush(stdout);
  return {status, kj::mv(rows)};
}

// The process-wide callback the role registers, captured by a registrar standing in for
// v8::V8::SetFatalErrorHandler: no public V8 call reaches the process-wide callback.
threaded::FatalHandler registeredProcessFatal = nullptr;
unsigned processFatalRegistrations = 0;
void captureProcessFatal(threaded::FatalHandler callback) {
  ++processFatalRegistrations;
  registeredProcessFatal = callback;
}

// A child that ends through one of the server's V8 fatal callbacks: "isolate", V8's own API
// check on an isolate set up by customizeIsolate(); "process", the callback the ordinary role
// registers process-wide, called as V8 calls it.
constexpr uint FATAL_CHILD_LINE = __LINE__ + 1;
KJ_TEST("child body: V8 fatal callbacks, selected by a parent test") {
  auto selected = getenv("COWORKERD_THREADED_FATAL_CHILD");
  if (selected == nullptr) return;
  const kj::StringPtr kind = selected;
  IsolateLimits limits(100, 32,
      kj::str("\"workerId\":\"a/fatal-fixture\",\"versionId\":\"v1\",\"invocationId\":\"native-check\","),
      nullptr, newGroup());
  auto* isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  limits.customizeIsolate(isolate);
  FatalInvocation active(isolate, limits.identity);
  if (kind == "isolate") {
    // An intentional native API precondition failure. It executes no JavaScript.
    (void)v8::MaybeLocal<v8::Value>().ToLocalChecked();
  } else if (kind == "process") {
    registerRoleFatalHandler(false, &captureProcessFatal);
    KJ_REQUIRE(registeredProcessFatal != nullptr, "the ordinary role registered no callback");
    registeredProcessFatal("driver.cc", 42, "process-wide check");
  }
  KJ_FAIL_REQUIRE("the fatal callback returned", kind);
}

struct FatalChild {
  int status;
  kj::String record, diagnostic;
};
FatalChild runFatalChild(kj::StringPtr kind) {
  auto [status, rows] = runTestChild(FATAL_CHILD_LINE, kj::str("COWORKERD_THREADED_FATAL_CHILD=", kind));
  fprintf(stdout, "FATAL_CHILD kind=%s exited=%d code=%d signal=%d\n%s", kind.cStr(),
      WIFEXITED(status), WIFEXITED(status) ? WEXITSTATUS(status) : -1,
      WIFSIGNALED(status) ? WTERMSIG(status) : 0, rows.c_str());
  fflush(stdout);
  const auto newline = rows.find('\n');
  if (newline == std::string::npos) return {status, kj::heapString(rows.c_str()), kj::str()};
  return {status, kj::heapString(rows.c_str(), newline + 1), kj::heapString(rows.c_str() + newline + 1)};
}
kj::String fatalChildRecord(kj::StringPtr row) {
  int pid = 0;
  KJ_REQUIRE(sscanf(row.cStr(), "{\"event\":\"v8-fatal\",\"pid\":%d,", &pid) == 1 && pid > 0, row);
  return kj::str("{\"event\":\"v8-fatal\",\"pid\":", pid,
      ",\"workerId\":\"a/fatal-fixture\",\"versionId\":\"v1\",\"invocationId\":\"native-check\"}\n");
}
kj::String hexOf(kj::StringPtr text) {
  return kj::encodeHex(text.asBytes());
}

KJ_TEST("the isolate's registered V8 fatal callback records the invocation and V8's reason") {
  auto child = runFatalChild("isolate");
  KJ_REQUIRE(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 75, child.status);
  KJ_EXPECT(child.record == fatalChildRecord(child.record), child.record);
  KJ_EXPECT(child.diagnostic.endsWith(kj::str(",\"fatalLocationHex\":\"", hexOf("v8::ToLocalChecked"),
                "\",\"fatalLine\":0,\"fatalMessageHex\":\"", hexOf("Empty MaybeLocal"), "\"}\n")),
      child.diagnostic);
}

KJ_TEST("the ordinary role registers the process-wide V8 fatal callback, durable roles do not") {
  registeredProcessFatal = nullptr;
  processFatalRegistrations = 0;
  registerRoleFatalHandler(true, &captureProcessFatal);
  KJ_EXPECT(processFatalRegistrations == 0 && registeredProcessFatal == nullptr);
  auto child = runFatalChild("process");
  KJ_REQUIRE(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 75, child.status);
  KJ_EXPECT(child.record == fatalChildRecord(child.record), child.record);
  KJ_EXPECT(child.diagnostic.endsWith(kj::str(",\"fatalLocationHex\":\"", hexOf("driver.cc"),
                "\",\"fatalLine\":42,\"fatalMessageHex\":\"", hexOf("process-wide check"), "\"}\n")),
      child.diagnostic);
}

KJ_TEST("126 MiB live then one near-GiB object preserves the process and a neighboring Worker") {
  // Without the fragmentation reserve this child ended with V8's fatal OOM
  // (exit 74) at 1150.5 MiB live in 1154.8 MiB of pages.
  auto child = runAllocationChild("1:live126");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

KJ_TEST("126 MiB live in 86 KiB objects then one near-GiB object preserves the process") {
  // With a reserve of a quarter of the limit this child ended with V8's fatal
  // OOM (exit 74) at 1150.1 MB live in 1212.0 MB of pages.
  auto child = runAllocationChild("1:live86k");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

KJ_TEST("126 MiB live in objects near 128 KiB then one near-GiB object preserves the process") {
  auto child = runAllocationChild("1:live128k");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

KJ_TEST("a 500 MB young object then one near-GiB object preserves the process") {
  auto child = runAllocationChild("1:young500");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

// The near-heap-limit callback's ceilings (MemoryBudget::raise), against a
// real isolate with 64 MiB live in a 2 GiB process budget: the first is one
// maximum object above the heap in use even when the heap is already past the
// old ceiling (young large objects are promoted without a check); a later one,
// while the termination is pending, reaches at most what the process budget has
// left; with nothing left the ceiling stays and V8's OOM path follows.
KJ_TEST("the raised heap ceiling counts from the heap in use and later stays within the process budget") {
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(2ull << 30, 4, 2);
  auto reservation = std::make_shared<ResourceReservation>(budget, 128);
  KJ_ASSERT(reservation->registerPreparation());
  IsolateLimits limits(10000, 128, kj::str("\"workerId\":\"a/raise\","), reservation, newGroup());
  auto* isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  v8::HandleScope handles(isolate);
  auto context = v8::Context::New(isolate);
  v8::Context::Scope inContext(context);
  auto source = v8::String::NewFromUtf8Literal(isolate, "globalThis.kept = new Array(8 * 1024 * 1024).fill(1.5); 1");
  KJ_ASSERT(!v8::Script::Compile(context, source).ToLocalChecked()->Run(context).IsEmpty());
  isolate->LowMemoryNotification();  // the reservation now holds this heap (observe() after the collection)
  v8::HeapStatistics stats;
  isolate->GetHeapStatistics(&stats);
  const uint64_t used = stats.total_heap_size();
  KJ_ASSERT(used >= (64ull << 20) && used < (256ull << 20), used);
  auto& memory = *limits.memory;
  const auto first = memory.raise(used / 2);
  KJ_EXPECT(first == used + MAX_SINGLE_HEAP_OBJECT, first, used);
  const auto left = budget->memoryMaximum - budget->memoryReserved.load();
  KJ_ASSERT(left > MAX_SINGLE_HEAP_OBJECT && left < 2 * MAX_SINGLE_HEAP_OBJECT, left);
  const auto second = memory.raise(first);
  KJ_EXPECT(second == used + left, second, used, left);
  const auto third = memory.raise(second);
  KJ_EXPECT(third == second, third, second);
  KJ_EXPECT(memory.ceilingRaised.load() == 2);
  KJ_EXPECT(budget->lent == left);
  // A second isolate of the same process, terminated at its ceiling too: its
  // first raise is one maximum object as always, but what the first isolate
  // was lent is not left for its later one, until that call's execution ends.
  auto otherReservation = std::make_shared<ResourceReservation>(budget, 128);
  KJ_ASSERT(otherReservation->registerPreparation());
  IsolateLimits otherLimits(
      10000, 128, kj::str("\"workerId\":\"a/raise-other\","), otherReservation, newGroup());
  auto* other = v8::Isolate::New(otherLimits.group, otherLimits.getCreateParams());
  KJ_DEFER(other->Dispose());
  customizeEntered(otherLimits, other);
  {
    v8::Locker otherLocker(other);
    v8::Isolate::Scope otherEntered(other);
    v8::HeapStatistics otherStats;
    other->GetHeapStatistics(&otherStats);
    const uint64_t otherUsed = otherStats.total_heap_size();
    auto& otherMemory = *otherLimits.memory;
    const auto otherFirst = otherMemory.raise(otherUsed / 2);
    KJ_EXPECT(otherFirst == otherUsed + MAX_SINGLE_HEAP_OBJECT, otherFirst, otherUsed);
    KJ_EXPECT(otherMemory.raise(otherFirst) == otherFirst, "the first isolate's lent headroom is not left");
    memory.returnLent();
    KJ_EXPECT(budget->lent == MAX_SINGLE_HEAP_OBJECT);
    KJ_EXPECT(otherMemory.raise(otherFirst) > otherFirst, "returned headroom is left again");
    otherMemory.returnLent();
    KJ_EXPECT(budget->lent == 0);
  }
  isolate->CancelTerminateExecution();
}

// With a V8 sandbox per isolate group each isolate's heap has a pointer cage of its own: a raise
// reaches at most that cage less what V8 keeps in it, however much the process budget has left
// (v138 §8.1's interface with r50's S3a). A build with one shared cage bounds it by that cage's
// remainder over all isolates instead (the test above), and skips this one.
KJ_TEST("with a sandbox per isolate, a raised ceiling stays within the isolate's own cage") {
  initializeV8();
  if (!v8::IsolateGroup::CanCreateNewGroups()) return;
  auto budget = std::make_shared<ResourceBudget>(64ull << 30, 4, 2);
  auto reservation = std::make_shared<ResourceReservation>(budget, 128);
  KJ_ASSERT(reservation->registerPreparation());
  IsolateLimits limits(10000, 128, kj::str("\"workerId\":\"a/raise-own-cage\","), reservation, newGroup());
  auto* isolate = v8::Isolate::New(limits.group, limits.getCreateParams());
  KJ_DEFER(isolate->Dispose());
  customizeEntered(limits, isolate);
  v8::Locker locker(isolate);
  v8::Isolate::Scope entered(isolate);
  auto& memory = *limits.memory;
  const auto first = memory.raise(OWN_CAGE_HEAP - (256ull << 20));
  KJ_EXPECT(first == OWN_CAGE_HEAP, first);
  KJ_EXPECT(memory.raise(first) == first, "the isolate's own cage has nothing left");
  memory.returnLent();
  KJ_EXPECT(budget->lent == 0);
}

// Each long operation below ended the process before v138 (exit 74 at the
// raised ceiling, or V8's fatal invalid size): the child must survive, its
// call end with the memory limit (or, where noted, a RangeError), and the
// neighbor keep answering and allocating.
void expectChildSurvives(kj::StringPtr spec) {
  auto child = runAllocationChild(spec);
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, spec, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos, spec);
}
KJ_TEST("JSON.parse of 120 million objects ends at the memory limit and preserves the process") {
  expectChildSurvives("1:json");
}
KJ_TEST("JSON.parse of an array past the largest one ends with a RangeError or the limit") {
  expectChildSurvives("1:json-numbers");
}
KJ_TEST("straight-line code making 500 MB strings one after another preserves the process") {
  expectChildSurvives("1:straight");
}
KJ_TEST("a structured clone of four maximum strings preserves the process") {
  expectChildSurvives("1:clone");
}
KJ_TEST("replaceAll of 250 million matches ends at the memory limit and preserves the process") {
  expectChildSurvives("1:replace-all");
}
KJ_TEST("splitting into 120 million strings ends at the memory limit and preserves the process") {
  expectChildSurvives("1:split");
}
KJ_TEST("splits past the largest array end with a RangeError or the limit") {
  expectChildSurvives("1:split-many");
  expectChildSurvives("1:split-chars");
}
KJ_TEST("JSON.parse one element past the largest array, with a reviver, or past the largest object") {
  expectChildSurvives("1:json-mixed");
  expectChildSurvives("1:json-reviver");
  expectChildSurvives("1:json-properties");
  expectChildSurvives("1:json-properties-reviver");
}
KJ_TEST("a short CPU limit ends JSON.parse loops that allocate nothing") {
  expectChildSurvives("1:cpu-json-numbers");
  expectChildSurvives("1:cpu-json-slow");
  expectChildSurvives("1:cpu-json-reviver");
}
KJ_TEST("sort of an array-like object or a string, global replace with a function and global match") {
  expectChildSurvives("1:sort-string");
  expectChildSurvives("1:replace-function");
  expectChildSurvives("1:replace-function-high");
  expectChildSurvives("1:match-atom");
  expectChildSurvives("1:sort-string-90m");
  expectChildSurvives("1:to-sorted-string-90m");
  expectChildSurvives("1:sort-string-past-largest");
  expectChildSurvives("1:cpu-sort-length");
  expectChildSurvives("1:cpu-to-sorted-length");
}

KJ_TEST("one near-GiB heap object preserves the process and a neighboring Worker") {
  auto child = runAllocationChild("1:string");
  KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
  KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
}

// Four isolates each finish an object of about 1 GiB at the same moment while a neighbor keeps
// answering. With a pointer cage (and a V8 sandbox) per isolate (--config=coworkerd-sandbox), each
// heap has its own 4 GiB, so the process survives. With one shared 4 GiB cage this ended the
// process with V8's fatal OOM (exit 74; the known failure until S3, v137 §6.24), which a build
// without per-isolate cages still reproduces: that branch keeps the old expectation.
KJ_TEST("four simultaneous near-GiB heap objects preserve the process and a neighboring Worker") {
  auto child = runAllocationChild("4:string");
  if (v8::IsolateGroup::CanCreateNewGroups()) {
    KJ_EXPECT(WIFEXITED(child.status) && WEXITSTATUS(child.status) == 0, child.status);
    KJ_EXPECT(child.events.find("memory-oom") == std::string::npos);
    return;
  }
  bool reproduced = WIFEXITED(child.status) && WEXITSTATUS(child.status) == 74;
  fprintf(stdout, "KNOWN_FAILURE four-simultaneous-near-GiB reproduced=%d\n", reproduced);
  fflush(stdout);
  KJ_EXPECT(reproduced, "the shared-cage failure changed; update its status", child.status);
  // The OOM row names the execution whose allocation failed and carries its
  // isolate's heap: far below that Worker's 128 MiB limit, because the other
  // isolates filled the shared cage. The node must not blame it.
  if (reproduced) {
    auto at = child.events.find("\"event\":\"memory-culprit\"");
    KJ_ASSERT(child.events.find("\"event\":\"memory-oom-undetermined\"") != std::string::npos);
    KJ_ASSERT(at != std::string::npos, "the OOM thread was executing a Worker");
    auto line = child.events.substr(at, child.events.find('\n', at) - at);
    auto field = line.find("\"oomIsolateHeapBytes\":");
    KJ_ASSERT(field != std::string::npos, line.c_str());
    KJ_EXPECT(line.find("\"oomIsolateHeapReported\":true") != std::string::npos, line.c_str());
    auto heap = strtoull(line.c_str() + field + strlen("\"oomIsolateHeapBytes\":"), nullptr, 10);
    KJ_EXPECT(heap > 0 && heap < (128ull << 20), heap, line.c_str());
  }
}

KJ_TEST("facet WebSocket startup props round-trip nested binding channels and object identity") {
  struct Binding final: Frankenvalue::CapTableEntry, kj::Refcounted {
    Binding(uint channel, kj::Maybe<kj::String> object, kj::Maybe<Frankenvalue> props)
        : channel(channel), object(kj::mv(object)), props(kj::mv(props)) {}
    kj::Own<CapTableEntry> clone() override {
      return kj::addRef(*this);
    }
    uint channel;
    kj::Maybe<kj::String> object;
    kj::Maybe<Frankenvalue> props;
  };
  auto bind = [](uint channel, kj::Maybe<kj::String> object,
                  kj::Maybe<Frankenvalue> props) -> kj::Own<Frankenvalue::CapTableEntry> {
    return kj::refcounted<Binding>(channel, kj::mv(object), kj::mv(props));
  };
  auto describe = [](Frankenvalue::CapTableEntry& entry) -> kj::Maybe<FacetPropChannel> {
    auto& binding = kj::downcast<Binding>(entry);
    return FacetPropChannel{binding.channel,
      binding.object.map([](auto& id) { return id.asPtr(); }),
      binding.props.map([](auto& props) -> Frankenvalue& { return props; })};
  };
  Frankenvalue serviceProps;
  serviceProps.setProperty(kj::str("nested"), Frankenvalue::fromCapability(42,
      bind(15, kj::str("original-object"), kj::none)));
  Frankenvalue input;
  input.setProperty(kj::str("service"), Frankenvalue::fromCapability(42,
      bind(12, kj::none, kj::mv(serviceProps))));

  capnp::MallocMessageBuilder message;
  auto outbound = message.initRoot<rpc::Frankenvalue>();
  size_t remaining = 64 * 1024;
  KJ_REQUIRE(writeFacetProps(input, outbound, remaining, describe));
  auto restored = readFacetProps(outbound.asReader(), bind);
  auto bindings = restored.getCapTable();
  KJ_ASSERT(bindings.size() == 1);
  auto& service = kj::downcast<Binding>(*bindings[0]);
  KJ_EXPECT(service.channel == 12 && service.object == kj::none);
  auto nested = KJ_REQUIRE_NONNULL(service.props).getCapTable();
  KJ_ASSERT(nested.size() == 1);
  auto& object = kj::downcast<Binding>(*nested[0]);
  KJ_EXPECT(object.channel == 15 && KJ_REQUIRE_NONNULL(object.object) == "original-object");
  KJ_EXPECT(object.props == kj::none);

  remaining = 32;
  KJ_EXPECT(!writeFacetProps(input, outbound, remaining, describe));
  remaining = 64 * 1024;
  KJ_EXPECT(!writeFacetProps(input, outbound, remaining,
      [&](Frankenvalue::CapTableEntry& entry) -> kj::Maybe<FacetPropChannel> {
    if (kj::downcast<Binding>(entry).channel == 15) return kj::none;
    return describe(entry);
  }));
}

KJ_TEST("deadline decimal spelling and native refusal mappings") {
  for (auto literal: {"123456789.12345679", "123456789.12345678", "9007199254740991",
         "1.2345678901234567e+12", "1e+09", "0.00100", "0"}) {
    auto descriptor = kj::str(
        "{\"decoy\":[{\"lease\":\"x\\\"}y\"}],\"lease\":{\"expiresAtMonotonicMs\":", literal, "}}");
    auto raw = threaded::rawPath(descriptor.cStr(), {"lease", "expiresAtMonotonicMs"});
    KJ_EXPECT(raw == literal);
    auto forwarded = kj::str("{\"deadline\":", kj::heapString(raw.data(), raw.size()), "}");
    KJ_EXPECT(threaded::rawField(forwarded.cStr(), "deadline") == literal);
  }
  for (auto item: {std::pair{LimitCause::CPU, "CPU_LIMIT_EXCEEDED"},
         std::pair{LimitCause::MEMORY, "MEMORY_LIMIT_EXCEEDED"},
         std::pair{LimitCause::PROCESS_MEMORY, "PROCESS_MEMORY_PRESSURE"}}) {
    auto error = limitException(item.first);
    auto detail = KJ_REQUIRE_NONNULL(error.getDetail(jsg::COWORKERD_CODE_DETAIL_ID));
    KJ_EXPECT(detail.size() == strlen(item.second) &&
        memcmp(detail.begin(), item.second, detail.size()) == 0);
    fprintf(stdout, "NATIVE_ERROR_CODE %.*s\n", int(detail.size()),
        reinterpret_cast<const char*>(detail.begin()));
  }
  auto capacity = capacityError();
  KJ_EXPECT(capacity.getDetail(CAPACITY_DETAIL) != kj::none);
  KJ_EXPECT(KJ_EXCEPTION(FAILED, "PROCESS_MEMORY_PRESSURE").getDetail(CAPACITY_DETAIL) == kj::none);
  using R = threaded::Refusal;
  struct Expected {
    R reason;
    unsigned status;
    std::string_view code;
  };
  for (auto row:
      {Expected{R::UNSUPPORTED, 501, "NOT_SUPPORTED"}, Expected{R::CPU, 503, "CPU_LIMIT_EXCEEDED"},
        Expected{R::MEMORY, 503, "MEMORY_LIMIT_EXCEEDED"},
        Expected{R::OVERLOADED, 503, "OVERLOADED"},
        Expected{R::DRAINING, 503, "PROCESS_DRAINING"}}) {
    auto info = threaded::refusalInfo(row.reason);
    KJ_EXPECT(info.status == row.status && info.code == row.code);
    fprintf(stdout, "NATIVE_ERROR_STATUS %u %.*s\n", info.status, int(info.code.size()),
        info.code.data());
  }
  fflush(stdout);
}

KJ_TEST("legacy named HTTP entrypoints select the global handler") {
  KJ_EXPECT(threaded::normalizeEntrypoint("v1", "Named") == "default");
  KJ_EXPECT(threaded::normalizeEntrypoint("v1", "") == "default");
  KJ_EXPECT(threaded::normalizeEntrypoint("v2", "") == "default");
  KJ_EXPECT(threaded::normalizeEntrypoint("v2", "Named") == "Named");
}

KJ_TEST("bridge and decode failures retain their distinct trial stages") {
  using L = threaded::LoadStage;
  auto bridge = threaded::loadStageInfo(L::LOAD);
  auto package = threaded::loadStageInfo(L::PACKAGE);
  auto decode = threaded::loadStageInfo(L::DECODE);
  auto evaluation = threaded::loadStageInfo(L::EVALUATION);
  KJ_EXPECT(bridge.stage == "bridge" && bridge.category == "LOAD_FAILED");
  KJ_EXPECT(package.stage == "code" && package.codeStage == "package" && package.name == "Error");
  KJ_EXPECT(decode.stage == "code" && decode.codeStage == "decode" && decode.name == "SyntaxError");
  KJ_EXPECT(evaluation.stage == "evaluation" && evaluation.category == "TOP_LEVEL_THROW");
}

KJ_TEST("the naive ordering counterexample inherits the parent order") {
  KJ_EXPECT(threaded::childOrderSequence(false, "9", 1) == "0");
  KJ_EXPECT(threaded::childOrderSequence(true, "0", 7) == "7");
  KJ_EXPECT(threaded::childOrderSequence(true, "\"0\"", 7) == "7");
  KJ_EXPECT(threaded::childOrderSequence(true, "9", 1) == "9");
  KJ_EXPECT(threaded::childOrderSequence(true, "\"9\"", 1) == "\"9\"");
}

KJ_TEST("descriptor preflight raises a low soft limit and rejects a low hard limit") {
  for (bool hardLow: {false, true}) {
    auto pid = fork();
    KJ_REQUIRE(pid >= 0);
    if (pid == 0) {
      struct rlimit limits;
      if (getrlimit(RLIMIT_NOFILE, &limits) != 0) _exit(2);
      limits.rlim_cur = hardLow ? 128 : 1024;
      if (hardLow) limits.rlim_max = 128;
      if (setrlimit(RLIMIT_NOFILE, &limits) != 0) _exit(3);
      auto accepted = prepareDescriptors(512);
      if (getrlimit(RLIMIT_NOFILE, &limits) != 0) _exit(4);
      _exit(hardLow ? (accepted ? 5 : 0) : (accepted && limits.rlim_cur >= 4352 ? 0 : 6));
    }
    int status;
    KJ_REQUIRE(waitpid(pid, &status, 0) == pid);
    KJ_EXPECT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }
}

KJ_TEST("validation reports retain the original JavaScript exception") {
  Reporter reporter;
  reporter.addError(kj::str("Uncaught SyntaxError: Unexpected token '='\nat index.js:1:6"));
  reporter.addError(kj::str("a later validation error"));
  bool rejected = false;
  try {
    reporter.requireValid();
  } catch (const kj::Exception& error) {
    rejected = true;
    KJ_EXPECT(
        error.getDescription() == "Uncaught SyntaxError: Unexpected token '='\nat index.js:1:6");
  }
  KJ_EXPECT(rejected);
}

KJ_TEST("source JSON failures preserve the actual Worker decoder exception") {
  initializeV8();
  IsolateLimits limits(100, 32, kj::str("\"workerId\":\"a/source\","), nullptr, newGroup());
  bool failed = false;
  KJ_IF_SOME(error, sourceJsonFailure("not JSON", limits)) {
    failed = true;
    KJ_EXPECT(error == "SyntaxError: Unexpected token 'o', \"not JSON\" is not valid JSON");
  }
  KJ_EXPECT(failed);
  IsolateLimits valid(100, 32, kj::str("\"workerId\":\"a/source\","), nullptr, newGroup());
  KJ_EXPECT(sourceJsonFailure("{}", valid) == kj::none);
}

KJ_TEST("compatibility refusals are tenant validation before package decoding") {
  Slot slot;
  Reporter reporter;
  JsonDoc invalid(kj::str(
      "{\"compatibilityDate\":\"2026-11-01\",\"compatibilityFlags\":[\"no_such_threaded_test_flag\"]}"));
  bool rejected = false;
  try {
    compileTenantFlags(slot, invalid.get(), reporter);
  } catch (const kj::Exception&) {
    rejected = true;
  }
  KJ_EXPECT(rejected && slot.loadStage == threaded::LoadStage::EVALUATION);
  Slot validSlot;
  Reporter validReporter;
  JsonDoc valid(kj::str("{\"compatibilityDate\":\"2026-11-01\",\"compatibilityFlags\":[]}"));
  KJ_EXPECT(!compileTenantFlags(validSlot, valid.get(), validReporter).getPreciseTimers());
}

KJ_TEST("waitUntil drain outlives child-call authority and ends 30 seconds after the response") {
  initializeV8();
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  // The timer's cached time lags the monotonic clock by the last JavaScript
  // turn; the deadline must come from the clock, not from the cached time.
  auto lagging = kj::systemPreciseMonotonicClock().now() - 5 * kj::SECONDS;
  kj::TimerImpl clock(lagging);
  Timer timer(clock);
  IsolateLimits isolateLimits(100, 32, kj::str("\"workerId\":\"a/background\","), nullptr, newGroup());
  Slot slot;
  slot.limits = &isolateLimits;
  Measurement measurement;
  RequestLimits limits(slot, measurement, timer, kj::str("\"workerId\":\"a/background\","));
  KJ_EXPECT(WAIT_UNTIL_LIMIT == 30 * kj::SECONDS);
  // limitDrain() is called when the response has ended. A 2 s child-call
  // lease and later waitUntil work (the 2.4 s legacy trigger) stay inside it.
  auto before = kj::systemPreciseMonotonicClock().now();
  auto drain = limits.limitDrain();
  auto after = kj::systemPreciseMonotonicClock().now();
  clock.advanceTo(before + 2400 * kj::MILLISECONDS);
  KJ_EXPECT(!drain.poll(waitScope));
  // A delay from the cached time would have ended here, 5 s early.
  clock.advanceTo(lagging + WAIT_UNTIL_LIMIT);
  KJ_EXPECT(!drain.poll(waitScope), "the deadline must not lose the last JavaScript turn");
  clock.advanceTo(before + WAIT_UNTIL_LIMIT - 1 * kj::MILLISECONDS);
  KJ_EXPECT(!drain.poll(waitScope));
  clock.advanceTo(after + WAIT_UNTIL_LIMIT);
  bool ended = drain.poll(waitScope);
  KJ_EXPECT(ended, "waitUntil work must end at the drain limit");
  if (ended) drain.wait(waitScope);
}

// A streamed body keeps the call's response open: the 30 seconds of waitUntil
// work start when it ends (or the client goes away), not when the handler
// returned (Cloudflare's Context page; v138 section 4).
KJ_TEST("waitUntil's 30 seconds start when the response ends, not when the handler returns") {
  initializeV8();
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  const auto start = kj::systemPreciseMonotonicClock().now();
  kj::TimerImpl clock(start);
  Timer timer(clock);
  IsolateLimits isolateLimits(100, 32, kj::str("\"workerId\":\"a/stream\","), nullptr, newGroup());
  Slot slot;
  slot.limits = &isolateLimits;
  Measurement measurement;
  auto responseEnd = kj::newPromiseAndFulfiller<void>();
  measurement.responseEnded = responseEnd.promise.fork();
  RequestLimits limits(slot, measurement, timer, kj::str("\"workerId\":\"a/stream\","));
  auto drain = limits.limitDrain();  // the handler returned; the body streams for 40 s
  clock.advanceTo(start + 35 * kj::SECONDS);
  KJ_EXPECT(!drain.poll(waitScope), "a 35 s waitUntil task must outlive the handler while the body streams");
  clock.advanceTo(start + 40 * kj::SECONDS);
  responseEnd.fulfiller->fulfill();
  KJ_EXPECT(!drain.poll(waitScope));
  clock.advanceTo(start + 70 * kj::SECONDS - 1 * kj::MILLISECONDS);
  KJ_EXPECT(!drain.poll(waitScope), "the 30 s start at the response's end");
  clock.advanceTo(start + 70 * kj::SECONDS + 1 * kj::MILLISECONDS);
  bool ended = drain.poll(waitScope);
  KJ_EXPECT(ended, "waitUntil work must end 30 s after the response ended");
  if (ended) drain.wait(waitScope);
}

KJ_TEST("a call thread cancels background work that outlives the drain limit") {
  kj::EventLoop loop;
  kj::WaitScope waitScope(loop);
  kj::TimerImpl clock(kj::origin<kj::TimePoint>());
  Errors errors;
  kj::TaskSet tasks(errors);
  // Work that ends in time is awaited, not canceled.
  auto finished = kj::newPromiseAndFulfiller<void>();
  tasks.add(kj::mv(finished.promise));
  auto settled = settleBackground(clock, tasks);
  clock.advanceTo(clock.now() + WAIT_UNTIL_LIMIT);
  KJ_EXPECT(!settled.poll(waitScope));
  finished.fulfiller->fulfill();
  bool awaited = settled.poll(waitScope);
  KJ_EXPECT(awaited);
  if (awaited) settled.wait(waitScope);
  KJ_EXPECT(tasks.isEmpty());
  // A task that never ends (a timer loop the drain failed to cancel) holds
  // the thread only until the backstop, then it is canceled.
  bool canceled = false;
  tasks.add(kj::Promise<void>(kj::NEVER_DONE).attach(kj::defer([&canceled] { canceled = true; })));
  auto start = clock.now();
  auto backstop = settleBackground(clock, tasks);
  clock.advanceTo(start + BACKGROUND_BACKSTOP - 1 * kj::MILLISECONDS);
  KJ_EXPECT(!backstop.poll(waitScope) && !canceled);
  clock.advanceTo(start + BACKGROUND_BACKSTOP);
  bool returned = backstop.poll(waitScope);
  KJ_EXPECT(returned, "the call thread must return at the backstop");
  if (returned) backstop.wait(waitScope);
  KJ_EXPECT(canceled && tasks.isEmpty());
  KJ_EXPECT(BACKGROUND_BACKSTOP > WAIT_UNTIL_LIMIT);
}

class TeardownWalChannel final: public server::DurableWalChannel {
 public:
  kj::Vector<kj::StringPtr> events;

  bool enabled() const override {
    return true;
  }
  uint64_t nextRequest() override {
    return ++request;
  }

  uint64_t registerShipper(server::DurableLogShipper&) override {
    return nextRequest();
  }
  void unregisterShipper(server::DurableLogShipper& shipper) override {
    auto openId = shipper.getOpenId();
    KJ_REQUIRE(openId == 1 || openId == 2);
    events.add(openId == 1 ? "unregister root"_kj : "unregister facet"_kj);
  }
  void send(uint64_t openId, kj::Array<kj::byte> message) override {
    KJ_REQUIRE(message.size() >= 13);
    KJ_REQUIRE(openId == 1 || openId == 2);
    KJ_EXPECT(server::wal::getLe64(message.begin() + 5) == openId);
    switch (message[4]) {
      case server::wal::MSG_OPEN:
        events.add(openId == 1 ? "root OPEN"_kj : "facet OPEN"_kj);
        break;
      case server::wal::MSG_CLOSED:
        events.add(openId == 1 ? "root CLOSED"_kj : "facet CLOSED"_kj);
        break;
      case server::wal::MSG_ALARM:
        KJ_EXPECT(openId == 1);
        break;
      default:
        KJ_FAIL_ASSERT("unexpected WAL message", message[4]);
    }
  }
  kj::Promise<void> sendFrames(
      server::DurableLogShipper&, kj::Array<kj::byte>, uint64_t, kj::Timer&) override {
    KJ_FAIL_ASSERT("the shutdown fixture does not write frames");
  }

 private:
  uint64_t request = 0;
};

KJ_TEST("a thread home cancels actor tasks and closes SQLite before releasing the WAL Home") {
  ThreadedServer server{
    1, "", "teardown:1", -1, kj::newInMemoryDirectory(kj::nullClock()), {}, &initializeV8()};
  // The fixture owns the resources so it can record early handle release safely.
  TeardownWalChannel channel;
  auto& events = channel.events;
  Slot slot;
  {
    ThreadState state;
    state.durableSlot = std::shared_ptr<Slot>(&slot, [](Slot*) {});
    std::weak_ptr<Slot> copy = state.durableSlot;
    state.wal = kj::attachRef(channel, kj::defer([&events, copy] {
      KJ_EXPECT(copy.expired());
      events.add("release Home"_kj);
    }));

    constexpr auto source = R"(
      import { DurableObject } from 'cloudflare:workers';
      export class Store extends DurableObject {}
      export default { fetch() { return new Response('ok'); } };
    )"_kj;
    JsonDoc descriptor(kj::str(R"({"workerId":"test/teardown","versionId":"v1",
        "mainModule":"index.js","compatibilityDate":"2026-11-01","contentHash":")",
        digest(source.asBytes()).c_str(), "\"}"));
    slot.worker = server.compile(slot, descriptor.get(), source);
    auto storageId = digest("teardown"_kj.asBytes());
    auto tree = std::make_shared<DurableTreeContext>(DurableTreeContext{
      .state = state,
      .server = server,
      .slot = state.durableSlot,
      .tree = 1,
      .uniqueKey = kj::str("coworkerd-do-root-v1-", storageId.c_str()),
      .storageId = kj::str(storageId.c_str()),
      .objectName = kj::str("test/teardown/1"),
      .idle = std::make_shared<server::DurableIdleLifecycle>(
          server::DurableIdleLifecycle::Time{}, std::chrono::seconds(70)),
    });
    auto& worker = *KJ_ASSERT_NONNULL(slot.worker);
    auto root =
        makeDurableActor(tree, worker, kj::str("teardown"), "Store"_kj, Frankenvalue(), 0, 0);
    auto facet = makeDurableActor(
        tree, worker, kj::str("facet"), "Store"_kj, Frankenvalue(), 1, 1, nullptr, 0, "facet"_kj);
    KJ_REQUIRE(state.durableActorTrees.size() == 2);
    KJ_EXPECT(state.durableActorTrees.contains(root.value.get()));
    KJ_EXPECT(state.durableActorTrees.contains(facet.value.get()));

    // Model the drain queued when a root's I/O context cancels a facet session. The drain
    // retains a real facet actor and its storage until the second round of cancellation.
    auto queueDrain = kj::defer([&state, &events, facet = kj::mv(facet.value)]() mutable {
      KJ_EXPECT(state.durableActorTrees.size() == 1);
      KJ_EXPECT(state.durableActorTrees.contains(facet.get()));
      events.add("queue facet drain"_kj);
      auto cancelDrain = kj::defer([&state, &events, facet = kj::mv(facet)]() mutable {
        KJ_EXPECT(state.durableRoots.empty());
        events.add("cancel facet drain"_kj);
        facet = nullptr;
        KJ_EXPECT(state.durableActorTrees.empty());
        events.add("actor index empty"_kj);
      });
      state.tasks.add(kj::Promise<void>(kj::NEVER_DONE).attach(kj::mv(cancelDrain)));
    });
    auto releaseOnCancel = kj::defer([&events, actor = kj::addRef(*root.value)]() mutable {
      actor = nullptr;
      events.add("cancel root task"_kj);
    });
    state.durableRoots.emplace(tree->tree,
        ThreadState::DurableRoot{
          .actor = kj::mv(root.value).attachToThisReference(kj::mv(queueDrain)),
          .tree = tree,
          .hooks = root.hooks,
          .idle = tree->idle,
        });
    state.tasks.add(kj::Promise<void>(kj::NEVER_DONE).attach(kj::mv(releaseOnCancel)));
    KJ_REQUIRE(!state.tasks.isEmpty());
    KJ_EXPECT(events.asPtr() == kj::arr("root OPEN"_kj, "facet OPEN"_kj).asPtr());
  }
  auto expected = kj::arr("root OPEN"_kj, "facet OPEN"_kj, "cancel root task"_kj,
      "unregister root"_kj, "root CLOSED"_kj, "queue facet drain"_kj, "cancel facet drain"_kj,
      "unregister facet"_kj, "facet CLOSED"_kj, "actor index empty"_kj, "release Home"_kj);
  KJ_EXPECT(events.asPtr() == expected.asPtr());
}

KJ_TEST("normal retirement collects actor handles while a neighbor and stored state survive") {
  ThreadedServer server{
    1, "", "retirement:1", -1, kj::newInMemoryDirectory(kj::nullClock()), {}, &initializeV8()};
  kj::AutoCloseFd managerWake(eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK));
  server.managerWake = managerWake.get();
  auto home = std::make_unique<ThreadedServer::Thread>(false, false);
  kj::AutoCloseFd homeWake(home->wake);
  home->homeId = 1;
  home->home.emplace();
  home->home->warmDefinition = "definition";
  server.threads.push_back(std::move(home));
  server.ready.store(true);

  ThreadState state;
  state.durableHome = 1;
  state.durableSlot = std::make_shared<Slot>();
  auto& slot = *state.durableSlot;
  slot.planRevision = "definition";
  constexpr auto source = R"(
    import { DurableObject } from 'cloudflare:workers';
    export class Store extends DurableObject {}
    export default { fetch() { return new Response('ok'); } };
  )"_kj;
  JsonDoc descriptor(kj::str(R"({"workerId":"test/retirement","versionId":"v1",
      "mainModule":"index.js","compatibilityDate":"2026-11-01","memoryLimitMiB":128,"contentHash":")",
      digest(source.asBytes()).c_str(), "\"}"));
  slot.worker = server.compile(slot, descriptor.get(), source);
  auto& worker = *KJ_ASSERT_NONNULL(slot.worker);
  const auto takeLock = Worker::Lock::TakeSynchronously(kj::none);

  struct Marker {
    v8::Global<v8::Object> weak;
    bool collected = false;
  } retired, neighbor;
  const auto now = durableIdleNow();
  const auto oldUse = now - server::DurableAdmission::IDLE_DELAY;
  auto open = [&](kj::StringPtr name, Marker& marker, bool keepEventActive) {
    DurableRoute route{{"tenant", name.cStr()}, {1, 1, 1, 1, "definition", "Store"}, {}, {}, {}};
    auto& admission = *server.durableAdmission;
    auto ticket = server.queueDurable(route, 0, now + std::chrono::seconds(5), now).ticket;
    KJ_REQUIRE(ticket.has_value());
    KJ_REQUIRE(admission.begin(*ticket, 1, route.key, route.open, now) ==
        server::DurableAdmission::Delivery::ACCEPTED);
    auto storageId = digest(name.asBytes());
    auto tree = std::make_shared<DurableTreeContext>(DurableTreeContext{
      .state = state,
      .server = server,
      .slot = state.durableSlot,
      .tree = ticket->tree,
      .uniqueKey = kj::str("coworkerd-do-root-v1-", storageId.c_str()),
      .storageId = kj::str(storageId.c_str()),
      .objectName = kj::str(name),
      .idle = std::make_shared<server::DurableIdleLifecycle>(oldUse, std::chrono::seconds(70)),
    });
    if (!keepEventActive) {
      auto database = server.durableStorage->open(tree->uniqueKey, tree->storageId);
      database->run("CREATE TABLE saved (value INTEGER)");
      database->run("INSERT INTO saved VALUES (29)");
    }
    auto actor = makeDurableActor(tree, worker, kj::str(name), "Store"_kj, Frankenvalue(), 0, 0);
    // This handle follows the actor's lifetime and is deferred when it dies outside the lock.
    auto owned = worker.runInLockScope(takeLock, [&](Worker::Lock& lock) {
      auto* isolate = lock.getIsolate();
      v8::HandleScope handles(isolate);
      v8::Context::Scope context(lock.getContext());
      auto object = v8::Object::New(isolate);
      marker.weak.Reset(isolate, object);
      marker.weak.SetWeak(&marker, [](const v8::WeakCallbackInfo<Marker>& info) {
        auto& marker = *info.GetParameter();
        marker.weak.Reset();
        marker.collected = true;
      }, v8::WeakCallbackType::kParameter);
      return static_cast<jsg::Lock&>(lock).v8Ref(object);
    });
    state.durableRoots.emplace(tree->tree, ThreadState::DurableRoot{
      .actor = kj::mv(actor.value).attachToThisReference(kj::mv(owned)),
      .tree = tree,
      .hooks = actor.hooks,
      .idle = tree->idle,
      .facetsBusy = [](bool) { return false; },
      .facetHasOrdinaryWebSocket = [] { return false; },
      .stopFacets = [](const kj::Exception*) {},
    });
    server.publishDurableRoot(1, tree->tree);
    if (!keepEventActive) {
      KJ_REQUIRE(admission.complete(*ticket, 1));
    }
    return tree;
  };
  auto oldTree = open("retired"_kj, retired, false);
  auto liveTree = open("neighbor"_kj, neighbor, true);
  server.retireIdleDurableRoots(state, now);
  KJ_EXPECT(server.durableIdleRetirements.load() == 1);
  KJ_EXPECT(!state.durableRoots.contains(oldTree->tree));
  KJ_EXPECT(state.durableRoots.contains(liveTree->tree));
  KJ_EXPECT(!retired.collected && !server.releaseDurableMemoryRequested.load());

  server.retireIdleDurableRoots(state, now + std::chrono::seconds(2));
  KJ_EXPECT(retired.collected);
  KJ_EXPECT(server.releaseDurableMemoryRequested.load());
  KJ_EXPECT(!neighbor.collected && state.durableRoots.contains(liveTree->tree));
  KJ_EXPECT(server.durableAdmission->hasActiveEvent(liveTree->tree));
  auto reopened = server.durableStorage->open(oldTree->uniqueKey, oldTree->storageId);
  KJ_EXPECT(reopened->run("SELECT value FROM saved").getInt(0) == 29);
  worker.runInLockScope(takeLock, [&](Worker::Lock&) { neighbor.weak.Reset(); });
}

KJ_TEST("a reclaim requested during maintenance never waits for that pass") {
  initializeV8();
  auto budget = std::make_shared<ResourceBudget>(2ull << 30, 4, 1);
  std::promise<bool> result;
  auto future = result.get_future();
  // An eviction inside the pass can reach reserve()/observe(). The pass entry
  // point the server's maintain() uses sets the flag itself.
  std::thread maintenance([&] {
    KJ_EXPECT(!ResourceBudget::maintaining);
    budget->runMaintenance([&] {
      KJ_EXPECT(ResourceBudget::maintaining, "the maintenance pass must mark its own thread");
      result.set_value(budget->reclaim(1));
    });
    KJ_EXPECT(!ResourceBudget::maintaining);
  });
  bool returned = future.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
  KJ_EXPECT(returned, "the maintenance thread waited for its own reclaim pass");
  if (!returned) budget->finishReclaim(true);
  maintenance.join();
  KJ_EXPECT(!future.get() && budget->reclaimRequested.load());
  // The server's own entry (ThreadedServer::maintain()) marks the pass it runs.
  struct StandIn {
    std::shared_ptr<ResourceBudget> resources;
    bool marked = false;
    void maintainPass() { marked = ResourceBudget::maintaining; }
  } standIn{budget};
  runMaintenanceOf(standIn);
  KJ_EXPECT(standIn.marked, "the server's maintenance entry must mark its own pass");
  KJ_EXPECT(!ResourceBudget::maintaining);
  // Any other thread still waits for a completed pass.
  std::atomic<bool> stop{false};
  std::thread pass([&] {
    while (!stop.load()) {
      if (budget->reclaimRequested.exchange(false)) budget->finishReclaim(false);
      std::this_thread::yield();
    }
  });
  KJ_EXPECT(budget->reclaim(1));
  stop.store(true);
  pass.join();
}

KJ_TEST("HTTP entry evidence comes from delivered requests, not canceled probes") {
  int fds[2];
  KJ_ASSERT(pipe2(fds, O_NONBLOCK | O_CLOEXEC) == 0);
  KJ_DEFER(close(fds[0]); close(fds[1]));
  kj::HttpHeaderTable::Builder builder;
  auto callId = builder.add("x-placement-call-id");
  auto table = builder.build();
  kj::HttpHeaders headers(*table);
  JsonDoc input(kj::str("{\"serverId\":\"s2\",\"processId\":\"p1\","
                        "\"processGeneration\":\"3\",\"workerId\":\"caller\",\"versionId\":\"v1\","
                        "\"invocationId\":\"i7\",\"treeId\":\"t1\"}"));
  char buffer[2048];
  httpEntryObserver(input.get(), "", headers, callId, fds[1])->delivered();
  KJ_EXPECT(read(fds[0], buffer, sizeof(buffer)) == -1 && errno == EAGAIN);
  headers.setPtr(callId, "probe-7");
  { auto canceled = httpEntryObserver(input.get(), "", headers, callId, fds[1]); }
  KJ_EXPECT(read(fds[0], buffer, sizeof(buffer)) == -1 && errno == EAGAIN);
  httpEntryObserver(input.get(), "", headers, callId, fds[1])->delivered();
  auto count = read(fds[0], buffer, sizeof(buffer));
  KJ_ASSERT(count > 0);
  auto stored = kj::str(kj::arrayPtr(buffer, count));
  auto line = stored.asPtr();
  KJ_ASSERT(line.startsWith("[tenant-entry] ") && line.endsWith("\n"));
  JsonDoc row(kj::str(line.slice(strlen("[tenant-entry] "), line.size() - 1)));
  KJ_EXPECT(text(row.get(), "serverId") == "s2");
  KJ_EXPECT(text(row.get(), "processId") == "p1");
  KJ_EXPECT(field(row.get(), "processGeneration").getNumber() == 3);
  KJ_EXPECT(text(row.get(), "invocationId") == "i7");
  KJ_EXPECT(text(row.get(), "target") == "caller@v1");
  KJ_EXPECT(text(row.get(), "entrypoint") == "default");
  KJ_EXPECT(text(row.get(), "callId") == "probe-7");
}

}  // namespace
}  // namespace workerd
