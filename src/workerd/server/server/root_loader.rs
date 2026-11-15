// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Private root loading over an ordinary service binding owned by the platform runtime.
//! Compiled instances and stable namespace/class providers have separate lifetimes.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::time::{Duration, SystemTime};

use futures::future::LocalBoxFuture;
use kj::http::{
    ConnectResponse, ConnectSettings, Headers, HeadersRef, Method, Service, ServiceResponse,
};
use kj::io::{AsyncInputStream, AsyncIoStream};
use kj_rs::KjOwn;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use worker::Interface;
use workerd_capnp::config;

use crate::Result;
use crate::actor::ActorNamespace;
use crate::bindings::{ActorConfig, SPECIAL_SUBREQUEST_CHANNEL_COUNT};
use crate::bridge::ffi;
use crate::channels::{
    ActorClass, Channel, NewActor, PendingToken, RequestMetadata, TokenUsage, WorkerInterface,
};
use crate::config::{Factory, Reporter, capnp_error, collect_actor_configs};
use crate::services::{header_table, not_transferable, unsupported};
use crate::worker::{LinkedChannels, WorkerService};

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct CreationRecord {
    present: bool,
    tenant: String,
    worker_id: String,
    version_id: String,
    generation: u64,
    cpu_limit_ms: u32,
    memory_limit_mib: u32,
    #[serde(default)]
    startup_identity: String,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Module {
    name: String,
    source: String,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Namespace {
    id: String,
    class_name: String,
    binding: String,
    unique_key: String,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Deployment {
    kind: String,
    worker_id: String,
    version_id: String,
    compatibility_date: String,
    #[serde(default)]
    compatibility_flags: Vec<String>,
    modules: Vec<Module>,
    namespaces: Vec<Namespace>,
    creation_record: Option<CreationRecord>,
}

#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerKey {
    worker_id: String,
    version_id: String,
    cpu_limit_ms: u32,
    memory_limit_mib: u32,
    generation: u64,
}

impl Deployment {
    fn record(&self) -> Result<&CreationRecord> {
        let record = self
            .creation_record
            .as_ref()
            .filter(|record| record.present)
            .ok_or_else(|| {
                kj::failed!("ROOT_CREATION_REQUIRED: a tenant root needs a creation record")
            })?;
        if self.kind != "tenant" {
            return Err(kj::failed!(
                "this root loader does not implement the requested worker kind"
            ));
        }
        if record.tenant.is_empty()
            || record.worker_id != self.worker_id
            || record.version_id != self.version_id
            || self.worker_id.is_empty()
            || self.version_id.is_empty()
        {
            return Err(kj::failed!("root deployment and creation record disagree"));
        }
        Ok(record)
    }

    fn key(&self, generation: u64) -> Result<WorkerKey> {
        let record = self.record()?;
        Ok(WorkerKey {
            worker_id: self.worker_id.clone(),
            version_id: self.version_id.clone(),
            cpu_limit_ms: record.cpu_limit_ms,
            memory_limit_mib: record.memory_limit_mib,
            generation,
        })
    }

    fn plan_key(&self) -> Result<String> {
        serde_json::to_string(&self.key(self.record()?.generation)?)
            .map_err(|error| kj::failed!("root key: {error}"))
    }

    fn validate(&self) -> Result<()> {
        self.record()?;
        if self.modules.is_empty() || self.namespaces.is_empty() {
            return Err(kj::failed!(
                "a root deployment needs modules and namespaces"
            ));
        }
        let mut modules = HashSet::new();
        for module in &self.modules {
            if module.name.is_empty() || !modules.insert(&module.name) {
                return Err(kj::failed!(
                    "root module names must be nonempty and distinct"
                ));
            }
        }
        let (mut ids, mut classes, mut bindings, mut keys) = (
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
        );
        for namespace in &self.namespaces {
            if !::cluster::compose::is_path_component(&namespace.unique_key)
                || namespace.id.is_empty()
                || namespace.class_name.is_empty()
                || namespace.binding.is_empty()
                || namespace.binding == "ROOTS"
                || !ids.insert(&namespace.id)
                || !classes.insert(&namespace.class_name)
                || !bindings.insert(&namespace.binding)
                || !keys.insert(&namespace.unique_key)
            {
                return Err(kj::failed!(
                    "invalid or duplicate root namespace description"
                ));
            }
        }
        Ok(())
    }
}

struct Plan {
    deployment: Deployment,
    lock: tokio::sync::Mutex<()>,
    generation: Cell<Option<u64>>,
    loaded: RefCell<Option<WorkerKey>>,
}

struct NamespaceEntry {
    namespace: Rc<ActorNamespace>,
    /// A class provider is independent of the compiled instance that currently implements it.
    source: Rc<RefCell<String>>,
    class_name: String,
    unique_key: String,
}

pub struct RootRegistry {
    this: Weak<Self>,
    factory: Rc<Factory>,
    storage: String,
    install_lock: tokio::sync::Mutex<()>,
    plans: RefCell<HashMap<String, Rc<Plan>>>,
    namespaces: RefCell<HashMap<String, NamespaceEntry>>,
    compiled: RefCell<HashMap<WorkerKey, Rc<WorkerService>>>,
    ignored_retirements: Cell<u64>,
}

impl RootRegistry {
    pub fn new(factory: Rc<Factory>, storage: String) -> Result<Rc<Self>> {
        if !std::path::Path::new(&storage).is_absolute() {
            return Err(kj::failed!(
                "the private root loader needs an absolute storagePath"
            ));
        }
        Ok(Rc::new_cyclic(|this| Self {
            this: this.clone(),
            factory,
            storage,
            install_lock: tokio::sync::Mutex::new(()),
            plans: RefCell::new(HashMap::new()),
            namespaces: RefCell::new(HashMap::new()),
            compiled: RefCell::new(HashMap::new()),
            ignored_retirements: Cell::new(0),
        }))
    }

    fn loaded(&self, plan: &Plan) -> Option<Rc<WorkerService>> {
        plan.loaded
            .borrow()
            .as_ref()
            .and_then(|key| self.compiled.borrow().get(key).cloned())
    }

    async fn compile(&self, deployment: &Deployment, key: &WorkerKey) -> Result<Rc<WorkerService>> {
        // A binding invocation must yield its calling isolate before compiling another isolate.
        self.factory.sleep(Duration::ZERO).await;
        let errors = Rc::new(RefCell::new(Vec::new()));
        let found = Rc::clone(&errors);
        let report = Reporter::new(
            Box::new(move |error| found.borrow_mut().push(error)),
            Box::new(|warning| tracing::warn!("{warning}")),
            false,
        );
        let service_name = deployment.plan_key()?;
        let mut message = capnp::message::Builder::new_default();
        {
            let mut service = message
                .init_root::<config::Builder>()
                .init_services(1)
                .get(0);
            service.set_name(service_name.as_str());
            let mut conf = service.init_worker();
            conf.set_compatibility_date(deployment.compatibility_date.as_str());
            let mut flags = conf
                .reborrow()
                .init_compatibility_flags(deployment.compatibility_flags.len() as u32);
            for (index, flag) in deployment.compatibility_flags.iter().enumerate() {
                flags.set(index as u32, flag.as_str());
            }
            let mut modules = conf
                .reborrow()
                .init_modules(deployment.modules.len() as u32);
            for (index, source) in deployment.modules.iter().enumerate() {
                let mut module = modules.reborrow().get(index as u32);
                module.set_name(source.name.as_str());
                module.set_es_module(source.source.as_str());
            }
            conf.reborrow()
                .init_durable_object_storage()
                .set_local_disk("root-storage");
            let mut spaces = conf
                .reborrow()
                .init_durable_object_namespaces(deployment.namespaces.len() as u32);
            for (index, namespace) in deployment.namespaces.iter().enumerate() {
                let mut space = spaces.reborrow().get(index as u32);
                space.set_class_name(namespace.class_name.as_str());
                space.set_unique_key(namespace.unique_key.as_str());
                space.set_enable_sql(true);
            }
            let mut bindings = conf
                .reborrow()
                .init_bindings(deployment.namespaces.len() as u32);
            for (index, namespace) in deployment.namespaces.iter().enumerate() {
                let mut binding = bindings.reborrow().get(index as u32);
                binding.set_name(namespace.binding.as_str());
                binding
                    .init_durable_object_namespace()
                    .set_class_name(namespace.class_name.as_str());
            }
        }
        let conf = message
            .get_root_as_reader::<config::Reader>()
            .map_err(capnp_error)?;
        let configs = collect_actor_configs(conf, self.factory.experimental(), &report)
            .map_err(capnp_error)?;
        let workerd_capnp::service::Which::Worker(worker_conf) = conf
            .get_services()
            .map_err(capnp_error)?
            .get(0)
            .which()
            .map_err(|error| kj::failed!("root worker config: {error}"))?
        else {
            return Err(kj::failed!("root config has no worker"));
        };
        let record = deployment.record()?;
        let identity = ffi::CoworkerdLoadIdentity {
            startup_identity: record.startup_identity.clone(),
            present: true,
            tenant: record.tenant.clone(),
            worker_id: record.worker_id.clone(),
            version_id: record.version_id.clone(),
            generation: key.generation,
            cpu_limit_ms: record.cpu_limit_ms,
            memory_limit_mib: record.memory_limit_mib,
        };
        let worker = WorkerService::new_root(
            Rc::clone(&self.factory),
            &service_name,
            worker_conf.map_err(capnp_error)?,
            &configs,
            identity,
            &report,
        )
        .await?;
        if report.refuses()
            || deployment
                .namespaces
                .iter()
                .any(|namespace| !worker.has_actor_class(&namespace.class_name))
        {
            worker.unlink();
            return Err(kj::failed!(
                "root worker failed to compile or is missing an exported class: {}",
                errors.borrow().join("; ")
            ));
        }
        Ok(worker)
    }

    fn link(
        self: &Rc<Self>,
        plan_key: &str,
        plan: &Plan,
        key: &WorkerKey,
        worker: &Rc<WorkerService>,
        spaces: &HashMap<String, NamespaceEntry>,
    ) -> Result<()> {
        for namespace in &plan.deployment.namespaces {
            let entry = spaces
                .get(&namespace.id)
                .ok_or_else(|| kj::failed!("root namespace missing before link"))?;
            worker.set_root_namespace(&namespace.class_name, Rc::clone(&entry.namespace));
        }
        let pending = worker
            .take_pending_link()
            .ok_or_else(|| kj::failed!("root worker was already linked"))?;
        if !pending.bindings.subrequest.is_empty()
            || !pending.bindings.actor_classes.is_empty()
            || !pending.bindings.worker_loaders.is_empty()
            || !pending.tails.is_empty()
            || !pending.streaming_tails.is_empty()
        {
            return Err(kj::failed!("root worker binding kind is not implemented"));
        }
        let mut subrequest: Vec<Rc<dyn Channel>> = (0..SPECIAL_SUBREQUEST_CHANNEL_COUNT)
            .map(|_| Rc::new(Denied) as Rc<dyn Channel>)
            .collect();
        for name in &pending.loopback.subrequest_entrypoints {
            subrequest.push(worker.loopback_entrypoint(name.as_deref())?);
        }
        let mut actor = Vec::new();
        for binding in &pending.bindings.actors {
            actor.push(worker.namespace(&binding.class_name));
        }
        for class in &pending.loopback.actor_namespaces {
            actor.push(worker.namespace(class));
        }
        let actor_class = pending
            .loopback
            .actor_classes
            .iter()
            .map(|name| worker.loopback_actor_class(name))
            .collect::<Result<_>>()?;
        let registry = Rc::downgrade(self);
        let expected = Rc::downgrade(worker);
        let plan_key = plan_key.to_owned();
        let key = key.clone();
        let factory = Rc::clone(&self.factory);
        worker.link(
            LinkedChannels {
                first_subrequest: 0,
                first_actor_class: 0,
                subrequest,
                actor,
                actor_class,
                cache: None,
                tails: Vec::new(),
                streaming_tails: Vec::new(),
                worker_loaders: Vec::new(),
                access_binding_channel: None,
                has_debug_port: false,
                server: Weak::new(),
                abort_isolate: Some(Box::new(move || {
                    let (registry, expected, plan_key, key) = (
                        registry.clone(),
                        expected.clone(),
                        plan_key.clone(),
                        key.clone(),
                    );
                    factory.spawn_detached(async move {
                        if let Some(registry) = registry.upgrade() {
                            registry.retire(&plan_key, &key, Some(expected)).await;
                        }
                    });
                })),
            },
            Some(&self.storage),
        )?;
        worker.root_ready()
    }

    fn make_spaces(
        &self,
        deployment: &Deployment,
        plan_key: &str,
    ) -> Result<HashMap<String, NamespaceEntry>> {
        let mut result = HashMap::new();
        for space in &deployment.namespaces {
            let source = Rc::new(RefCell::new(plan_key.to_owned()));
            let class: Rc<dyn ActorClass> = Rc::new(RootClass {
                registry: self.this.clone(),
                source: Rc::clone(&source),
                class_name: space.class_name.clone(),
            });
            let namespace = ActorNamespace::new(
                Rc::clone(&self.factory),
                plan_key.to_owned(),
                space.class_name.clone(),
                ActorConfig::Durable {
                    unique_key: space.unique_key.clone(),
                    evictable: true,
                    enable_sql: true,
                    workflow: None,
                    container: None,
                },
                class,
                false,
            );
            namespace.link(Some(&self.storage))?;
            result.insert(
                space.id.clone(),
                NamespaceEntry {
                    namespace,
                    source,
                    class_name: space.class_name.clone(),
                    unique_key: space.unique_key.clone(),
                },
            );
        }
        Ok(result)
    }

    async fn install(self: &Rc<Self>, deployment: Deployment) -> Result<String> {
        deployment.validate()?;
        let plan_key = deployment.plan_key()?;
        let _guard = self.install_lock.lock().await;
        if let Some(existing) = self.plans.borrow().get(&plan_key) {
            if existing.deployment != deployment {
                return Err(kj::failed!(
                    "a root plan cannot be replaced under its existing key"
                ));
            }
            return Ok(plan_key);
        }
        {
            let spaces = self.namespaces.borrow();
            for namespace in &deployment.namespaces {
                if spaces.contains_key(&namespace.id)
                    || spaces
                        .values()
                        .any(|entry| entry.unique_key == namespace.unique_key)
                {
                    return Err(kj::failed!(
                        "a root namespace is already owned by another plan"
                    ));
                }
            }
        }
        let key = deployment.key(deployment.record()?.generation)?;
        let plan = Rc::new(Plan {
            deployment,
            lock: tokio::sync::Mutex::new(()),
            generation: Cell::new(Some(key.generation)),
            loaded: RefCell::new(None),
        });
        let worker = self.compile(&plan.deployment, &key).await?;
        let spaces = match self.make_spaces(&plan.deployment, &plan_key) {
            Ok(spaces) => spaces,
            Err(error) => {
                worker.unlink();
                return Err(error);
            }
        };
        if let Err(error) = self.link(&plan_key, &plan, &key, &worker, &spaces) {
            worker.unlink();
            return Err(error);
        }
        self.namespaces.borrow_mut().extend(spaces);
        self.compiled.borrow_mut().insert(key.clone(), worker);
        *plan.loaded.borrow_mut() = Some(key);
        self.plans.borrow_mut().insert(plan_key.clone(), plan);
        Ok(plan_key)
    }

    async fn worker(self: &Rc<Self>, plan_key: &str) -> Result<Rc<WorkerService>> {
        let plan = self
            .plans
            .borrow()
            .get(plan_key)
            .cloned()
            .ok_or_else(|| kj::failed!("unknown root plan"))?;
        let _guard = plan.lock.lock().await;
        if let Some(worker) = self.loaded(&plan) {
            return Ok(worker);
        }
        let generation = plan
            .generation
            .get()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| kj::failed!("root load generation exhausted"))?;
        plan.generation.set(Some(generation));
        let key = plan.deployment.key(generation)?;
        let worker = self.compile(&plan.deployment, &key).await?;
        let linked = self.link(plan_key, &plan, &key, &worker, &self.namespaces.borrow());
        if let Err(error) = linked {
            worker.unlink();
            return Err(error);
        }
        self.compiled
            .borrow_mut()
            .insert(key.clone(), Rc::clone(&worker));
        *plan.loaded.borrow_mut() = Some(key);
        Ok(worker)
    }

    async fn retire(
        &self,
        plan_key: &str,
        key: &WorkerKey,
        expected: Option<Weak<WorkerService>>,
    ) -> bool {
        let Some(plan) = self.plans.borrow().get(plan_key).cloned() else {
            return false;
        };
        let _guard = plan.lock.lock().await;
        let current = self.compiled.borrow().get(key).cloned();
        let matches = plan.loaded.borrow().as_ref() == Some(key)
            && current.as_ref().is_some_and(|current| {
                expected
                    .as_ref()
                    .is_none_or(|expected| Weak::ptr_eq(expected, &Rc::downgrade(current)))
            });
        if !matches {
            self.ignored_retirements
                .set(self.ignored_retirements.get() + 1);
            return false;
        }
        self.compiled.borrow_mut().remove(key);
        *plan.loaded.borrow_mut() = None;
        for entry in self.namespaces.borrow().values() {
            if entry.source.borrow().as_str() == plan_key {
                entry
                    .namespace
                    .abort_all(Some(&kj::disconnected!("root worker unloaded")));
            }
        }
        if let Some(worker) = current {
            worker.unlink();
        }
        true
    }

    fn snapshot(&self) -> Value {
        let mut plans: Vec<_> = self.plans.borrow().iter().map(|(id, plan)| json!({
            "plan": id, "workerId": plan.deployment.worker_id, "versionId": plan.deployment.version_id,
            "loaded": plan.loaded.borrow().is_some(), "generation": plan.generation.get(),
            "key": *plan.loaded.borrow(), "namespaces": plan.deployment.namespaces,
        })).collect();
        plans.sort_by(|a, b| a["plan"].as_str().cmp(&b["plan"].as_str()));
        let mut namespaces: Vec<_> = self.namespaces.borrow().iter().map(|(id, entry)| json!({
            "id": id, "className": entry.class_name, "uniqueKey": entry.unique_key, "source": *entry.source.borrow(),
        })).collect();
        namespaces.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        json!({"plans": plans, "namespaces": namespaces, "compiledWorkers": self.compiled.borrow().len(),
            "ignoredRetirements": self.ignored_retirements.get(), "pid": std::process::id()})
    }

    pub fn shutdown(&self) {
        for entry in self.namespaces.borrow().values() {
            entry
                .namespace
                .abort_all(Some(&kj::disconnected!("root registry shutdown")));
        }
        let workers = std::mem::take(&mut *self.compiled.borrow_mut());
        for worker in workers.values() {
            worker.unlink();
        }
        self.namespaces.borrow_mut().clear();
        self.plans.borrow_mut().clear();
    }
}

impl Drop for RootRegistry {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct RootClass {
    registry: Weak<RootRegistry>,
    source: Rc<RefCell<String>>,
    class_name: String,
}

impl RootClass {
    fn registry(&self) -> Result<Rc<RootRegistry>> {
        self.registry
            .upgrade()
            .ok_or_else(|| kj::disconnected!("root registry gone"))
    }
    fn class(&self) -> Result<Rc<dyn ActorClass>> {
        let registry = self.registry()?;
        let plan = registry
            .plans
            .borrow()
            .get(self.source.borrow().as_str())
            .cloned()
            .ok_or_else(|| kj::failed!("root source gone"))?;
        registry
            .loaded(&plan)
            .ok_or_else(|| kj::disconnected!("root worker unloaded"))?
            .actor_class(Some(&self.class_name), None, false)
            .ok_or_else(|| kj::failed!("root class missing"))
    }
}

impl ActorClass for RootClass {
    fn when_ready(&self) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let source = self.source.borrow().clone();
            self.registry()?.worker(&source).await.map(drop)
        })
    }
    fn new_actor(&self, request: NewActor<'_>) -> Result<KjOwn<ffi::ActorHandle>> {
        self.class()?.new_actor(request)
    }
    fn start_request(
        &self,
        metadata: KjOwn<RequestMetadata>,
        actor: &ffi::ActorHandle,
    ) -> Result<KjOwn<WorkerInterface>> {
        self.class()?.start_request(metadata, actor)
    }
    fn run_on_hibernate<'a>(
        &'a self,
        metadata: KjOwn<RequestMetadata>,
        actor: &'a ffi::ActorHandle,
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async move { self.class()?.run_on_hibernate(metadata, actor).await })
    }
    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(not_transferable("Runtime root class"))
    }
}

struct Denied;
impl Channel for Denied {
    fn start_request(&self, _metadata: KjOwn<RequestMetadata>) -> Result<KjOwn<WorkerInterface>> {
        Err(kj::failed!(
            "jsg.Error: root worker has no outbound capability"
        ))
    }
    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(not_transferable("Root outbound"))
    }
}

impl Channel for RootRegistry {
    fn start_request(&self, metadata: KjOwn<RequestMetadata>) -> Result<KjOwn<WorkerInterface>> {
        let registry = self
            .this
            .upgrade()
            .ok_or_else(|| kj::disconnected!("root registry gone"))?;
        Ok(RootRequest {
            registry,
            metadata: Some(metadata),
        }
        .into_kj())
    }
    fn require_allows_transfer(&self) -> Result<()> {
        Err(not_transferable("Private root loader"))
    }
    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(not_transferable("Private root loader"))
    }
}

struct RootRequest {
    registry: Rc<RootRegistry>,
    metadata: Option<KjOwn<RequestMetadata>>,
}

async fn read_json(mut body: Pin<&mut AsyncInputStream>) -> Result<Value> {
    let mut bytes = Vec::new();
    let mut block = [0u8; 16384];
    loop {
        let size = body.as_mut().try_read(&mut block, 1).await?;
        if size == 0 {
            break;
        }
        if bytes.len() + size > 16 * 1024 * 1024 {
            return Err(kj::failed!("root description exceeds 16 MiB"));
        }
        bytes.extend_from_slice(&block[..size]);
    }
    serde_json::from_slice(&bytes).map_err(|error| kj::failed!("invalid root description: {error}"))
}

#[async_trait::async_trait(?Send)]
impl Service for RootRequest {
    async fn request<'a>(
        &'a mut self,
        method: Method,
        url: &'a [u8],
        headers: HeadersRef<'a>,
        body: Pin<&'a mut AsyncInputStream>,
        response: ServiceResponse<'a>,
    ) -> Result<()> {
        let text = std::str::from_utf8(url).map_err(|error| kj::failed!("root URL: {error}"))?;
        let url = url::Url::parse(text).map_err(|error| kj::failed!("root URL: {error}"))?;
        let value = match (method, url.path()) {
            (Method::POST, "/install") => {
                let deployment = serde_json::from_value(read_json(body).await?)
                    .map_err(|error| kj::failed!("root deployment: {error}"))?;
                let plan = self.registry.install(deployment).await?;
                json!({"plan": plan, "state": self.registry.snapshot()})
            }
            (Method::GET, "/state") => self.registry.snapshot(),
            (Method::POST, "/retire") => {
                let value = read_json(body).await?;
                let plan_key = value["plan"]
                    .as_str()
                    .ok_or_else(|| kj::failed!("retire needs a plan"))?;
                let generation = value["generation"]
                    .as_u64()
                    .ok_or_else(|| kj::failed!("retire needs a generation"))?;
                let plan = self
                    .registry
                    .plans
                    .borrow()
                    .get(plan_key)
                    .cloned()
                    .ok_or_else(|| kj::failed!("unknown root plan"))?;
                let key = plan.deployment.key(generation)?;
                let retired = self.registry.retire(plan_key, &key, None).await;
                json!({"retired": retired, "state": self.registry.snapshot()})
            }
            (_, "/invoke") => {
                let plan = url
                    .query_pairs()
                    .find(|(key, _)| key == "plan")
                    .map(|(_, value)| value.into_owned())
                    .ok_or_else(|| kj::failed!("invoke needs a plan"))?;
                let worker = self.registry.worker(&plan).await?;
                let metadata = self
                    .metadata
                    .take()
                    .ok_or_else(|| kj::failed!("root event already used"))?;
                let interface = worker.start_request(None, None, None, metadata, false)?;
                let mut interface = worker::CxxWorkerInterface::new(interface);
                return Service::request(
                    &mut interface,
                    method,
                    text.as_bytes(),
                    headers,
                    body,
                    response,
                )
                .await;
            }
            _ => return Err(kj::failed!("unknown private root loader operation")),
        };
        let bytes =
            serde_json::to_vec(&value).map_err(|error| kj::failed!("root reply: {error}"))?;
        let headers = Headers::new(header_table(&self.registry.factory));
        response
            .send(200, "OK", &headers, Some(bytes.len() as u64))?
            .write(&bytes)
            .await
    }

    async fn connect<'a>(
        &'a mut self,
        _host: &'a [u8],
        _headers: HeadersRef<'a>,
        _connection: Pin<&'a mut AsyncIoStream>,
        _response: ConnectResponse<'a>,
        _settings: ConnectSettings<'a>,
    ) -> Result<()> {
        Err(unsupported("Private root loader CONNECT"))
    }
}

#[async_trait::async_trait(?Send)]
impl Interface for RootRequest {
    async fn run_scheduled(
        &mut self,
        _time: &SystemTime,
        _cron: &str,
    ) -> Result<worker::ScheduledResult> {
        Err(unsupported("Private root loader"))
    }
    async fn run_alarm(
        &mut self,
        _time: &SystemTime,
        _retry_count: u32,
    ) -> Result<worker::AlarmResult> {
        Err(unsupported("Private root loader"))
    }
}
