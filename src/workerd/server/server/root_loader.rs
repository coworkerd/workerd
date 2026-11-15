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

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use futures::future::Shared;
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

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RootSelection {
    namespace_id: String,
    object_id: String,
    data_epoch: String,
    plan: String,
    dispatch_checked: bool,
    #[serde(default)]
    tenant_cf: Value,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RootTarget {
    service: String,
    entrypoint: Option<String>,
    #[serde(default)]
    props: Value,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
struct RootBinding {
    metadata: Value,
    target: RootTarget,
}

#[derive(Clone)]
struct RootOrigin {
    namespace_id: String,
    object_id: String,
    plan: String,
}
impl From<&RootSelection> for RootOrigin {
    fn from(value: &RootSelection) -> Self {
        Self {
            namespace_id: value.namespace_id.clone(),
            object_id: value.object_id.clone(),
            plan: value.plan.clone(),
        }
    }
}
#[derive(Clone)]
struct RootObject {
    origin: RootOrigin,
    class_name: String,
    registration: Rc<()>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RootClassReference {
    worker_id: String,
    plan: String,
}
const ROOT_CLASS_REFERENCE: &str = "coworkerd:root-class:";

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
    #[serde(default)]
    environment: std::collections::BTreeMap<String, Value>,
    #[serde(default)]
    durable_objects: Vec<RootBinding>,
    global_outbound: Option<RootTarget>,
    streaming_tail: Option<RootTarget>,
}

#[derive(Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerKey {
    plan_key: String,
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
        Ok(WorkerKey {
            plan_key: self.plan_key()?,
            generation,
        })
    }

    fn plan_key(&self) -> Result<String> {
        let record = self.record()?;
        serde_json::to_string(&json!({
            "workerId": self.worker_id, "versionId": self.version_id,
            "cpuLimitMs": record.cpu_limit_ms, "memoryLimitMib": record.memory_limit_mib,
            "generation": record.generation,
        }))
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
        let (mut ids, mut bindings, mut keys) = (HashSet::new(), HashSet::new(), HashSet::new());
        for namespace in &self.namespaces {
            if !::cluster::compose::is_path_component(&namespace.unique_key)
                || namespace.id.is_empty()
                || namespace.class_name.is_empty()
                || namespace.binding.is_empty()
                || namespace.binding == "ROOTS"
                || !ids.insert(&namespace.id)
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
    retiring: RefCell<Option<Shared<LocalBoxFuture<'static, ()>>>>,
}

#[derive(Clone)]
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
    objects: RefCell<HashMap<(String, String), RootObject>>,
    selection_locks: RefCell<HashMap<(String, String), Weak<tokio::sync::Mutex<()>>>>,
    compiled: RefCell<HashMap<WorkerKey, Rc<WorkerService>>>,
    retiring: RefCell<HashMap<WorkerKey, Rc<WorkerService>>>,
    ignored_retirements: Cell<u64>,
    server: RefCell<Weak<crate::config::Server>>,
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
            objects: RefCell::new(HashMap::new()),
            selection_locks: RefCell::new(HashMap::new()),
            compiled: RefCell::new(HashMap::new()),
            retiring: RefCell::new(HashMap::new()),
            ignored_retirements: Cell::new(0),
            server: RefCell::new(Weak::new()),
        }))
    }

    fn loaded(&self, plan: &Plan) -> Option<Rc<WorkerService>> {
        plan.loaded
            .borrow()
            .as_ref()
            .and_then(|key| self.compiled.borrow().get(key).cloned())
    }

    pub fn set_server(&self, server: Weak<crate::config::Server>) {
        *self.server.borrow_mut() = server;
    }

    pub fn exported_class(
        &self,
        plan_key: &str,
        class_name: &str,
        props: Option<KjOwn<ffi::Frankenvalue>>,
        persistent: bool,
    ) -> Option<Rc<dyn ActorClass>> {
        let reference: RootClassReference =
            serde_json::from_str(plan_key.strip_prefix(ROOT_CLASS_REFERENCE)?).ok()?;
        let plan_key = reference.plan.as_str();
        let worker_id = reference.worker_id;
        // A saved class can outlive its obsolete source plan. Restore only a dormant proxy for
        // a worker this registry still owns; the parent root binds it before construction.
        if !self
            .plans
            .borrow()
            .values()
            .any(|plan| plan.deployment.worker_id == worker_id)
        {
            return None;
        }
        Some(Rc::new(RootExportedClass {
            registry: self.this.clone(),
            plan_key: plan_key.to_owned(),
            worker_id,
            class_name: class_name.to_owned(),
            props,
            persistent,
            generation: Cell::new(None),
            running_worker: RefCell::new(Weak::new()),
        }))
    }

    pub fn namespace_by_key(&self, unique_key: &str) -> Option<Rc<ActorNamespace>> {
        self.namespaces
            .borrow()
            .values()
            .find(|entry| entry.unique_key == unique_key)
            .map(|entry| Rc::clone(&entry.namespace))
    }

    fn remove_object(&self, index: &(String, String), registration: &Rc<()>) {
        let matches = self
            .objects
            .borrow()
            .get(index)
            .is_some_and(|object| Rc::ptr_eq(&object.registration, registration));
        if matches {
            self.objects.borrow_mut().remove(index);
            self.prune_plans();
        }
    }

    fn prune_plans(&self) {
        let mut needed: HashSet<String> = self
            .namespaces
            .borrow()
            .values()
            .map(|entry| entry.source.borrow().clone())
            .collect();
        needed.extend(
            self.objects
                .borrow()
                .values()
                .map(|object| object.origin.plan.clone()),
        );
        needed.extend(
            self.retiring
                .borrow()
                .keys()
                .map(|key| key.plan_key.clone()),
        );
        let unused: Vec<_> = self
            .plans
            .borrow()
            .keys()
            .filter(|key| !needed.contains(*key))
            .cloned()
            .collect();
        let mut workers = Vec::new();
        for key in unused {
            self.plans.borrow_mut().remove(&key);
            let instances: Vec<_> = self
                .compiled
                .borrow()
                .keys()
                .filter(|instance| instance.plan_key == key)
                .cloned()
                .collect();
            for instance in instances {
                if let Some(worker) = self.compiled.borrow_mut().remove(&instance) {
                    workers.push(worker);
                }
            }
        }
        for worker in workers {
            worker.unlink();
        }
    }

    fn collect_inactive(&self) {
        let namespaces: Vec<_> = self
            .namespaces
            .borrow()
            .values()
            .map(|entry| Rc::clone(&entry.namespace))
            .collect();
        for namespace in namespaces {
            namespace.prune_idle_roots();
        }
        self.prune_plans();
    }

    fn target(&self, target: &RootTarget, props: &Value) -> Result<Rc<dyn Channel>> {
        self.server
            .borrow()
            .upgrade()
            .ok_or_else(|| kj::disconnected!("root server gone"))?
            .root_binding(&target.service, target.entrypoint.as_deref(), props)
    }

    fn root_props(&self, selection: &RootOrigin) -> Result<KjOwn<ffi::Frankenvalue>> {
        let plan = self
            .plans
            .borrow()
            .get(&selection.plan)
            .cloned()
            .ok_or_else(|| kj::failed!("root plan gone"))?;
        let mut props = ffi::frankenvalue_new();
        let mut bindings = ffi::frankenvalue_from_json("[]");
        for (index, binding) in plan.deployment.durable_objects.iter().enumerate() {
            let mut target_props = binding.target.props.clone();
            if !target_props.is_object() {
                target_props = json!({});
            }
            target_props["caller"] =
                json!({"namespaceId": selection.namespace_id, "objectId": selection.object_id});
            target_props["workerId"] = json!(plan.deployment.worker_id);
            target_props["versionId"] = json!(plan.deployment.version_id);
            let channel = self.target(&binding.target, &target_props)?;
            let mut entry = ffi::frankenvalue_from_json(&binding.metadata.to_string());
            ffi::frankenvalue_set_service_stub(
                entry.pin_mut(),
                "fetcher",
                crate::channels::SubrequestChannel::new(channel),
            );
            ffi::frankenvalue_set_value(bindings.pin_mut(), &index.to_string(), entry);
        }
        ffi::frankenvalue_set_value(props.pin_mut(), "durableObjects", bindings);
        Ok(props)
    }

    fn stamp_metadata(
        &self,
        selection: &RootSelection,
        metadata: &mut KjOwn<RequestMetadata>,
    ) -> Result<()> {
        let plan = self
            .plans
            .borrow()
            .get(&selection.plan)
            .cloned()
            .ok_or_else(|| kj::failed!("root plan gone"))?;
        let mut cf = serde_json::Map::new();
        for name in [
            "nodeId",
            "serverId",
            "processId",
            "processGeneration",
            "contentHash",
            "invocationId",
            "treeId",
            "ingressServerId",
            "lease",
            "orderSequence",
            "depth",
        ] {
            if let Some(value) = selection.tenant_cf.get(name) {
                cf.insert(name.to_owned(), value.clone());
            }
        }
        cf.insert("workerId".to_owned(), json!(plan.deployment.worker_id));
        cf.insert("versionId".to_owned(), json!(plan.deployment.version_id));
        let cf = Value::Object(cf).to_string();
        ffi::request_metadata_set_cf_blob_json(metadata.pin_mut(), &cf);
        ffi::request_metadata_set_coworkerd_invocation_identity(metadata.pin_mut(), &cf);
        Ok(())
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
            // Root namespaces belong only to the trusted registry. Declaring them here would
            // also expose native namespace capabilities through the tenant's ctx.exports.
            conf.reborrow().init_durable_object_namespaces(0);
            let mut bindings = conf
                .reborrow()
                .init_bindings(deployment.environment.len() as u32);
            for (index, (name, value)) in deployment.environment.iter().enumerate() {
                let mut binding = bindings.reborrow().get(index as u32);
                binding.set_name(name.as_str());
                if let Some(text) = value.as_str() {
                    binding.set_text(text);
                } else {
                    binding.set_json(value.to_string().as_str());
                }
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
        let _ = spaces;
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
        let global = match &plan.deployment.global_outbound {
            Some(target) => self.target(target, &target.props)?,
            None => Rc::new(Denied) as Rc<dyn Channel>,
        };
        let streaming_tails = match &plan.deployment.streaming_tail {
            Some(target) => vec![self.target(target, &target.props)?],
            None => Vec::new(),
        };
        let mut subrequest: Vec<Rc<dyn Channel>> = (0..SPECIAL_SUBREQUEST_CHANNEL_COUNT)
            .map(|_| Rc::clone(&global))
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
            .map(|name| {
                Rc::new(RootExportedClass {
                    registry: self.this.clone(),
                    plan_key: plan_key.to_owned(),
                    worker_id: plan.deployment.worker_id.clone(),
                    class_name: name.clone(),
                    props: None,
                    persistent: false,
                    generation: Cell::new(Some(key.generation)),
                    running_worker: RefCell::new(Weak::new()),
                }) as Rc<dyn ActorClass>
            })
            .collect();
        let registry = Rc::downgrade(self);
        let expected = Rc::downgrade(worker);
        let plan_key = plan_key.to_owned();
        let key = key.clone();
        worker.link(
            LinkedChannels {
                first_subrequest: 0,
                first_actor_class: 0,
                subrequest,
                actor,
                actor_class,
                cache: None,
                tails: Vec::new(),
                streaming_tails,
                worker_loaders: Vec::new(),
                access_binding_channel: None,
                has_debug_port: false,
                server: self.server.borrow().clone(),
                abort_isolate: Some(Box::new(move |reason| {
                    if let Some(registry) = registry.upgrade() {
                        registry.begin_retire(
                            &plan_key,
                            &key,
                            Some(expected.clone()),
                            reason == "idle",
                        );
                    }
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
            if let Some(existing) = self.namespaces.borrow().get(&space.id).cloned() {
                result.insert(space.id.clone(), existing);
                continue;
            }
            let source = Rc::new(RefCell::new(plan_key.to_owned()));
            let class: Rc<dyn ActorClass> = Rc::new(RootClass {
                registry: self.this.clone(),
                namespace_id: space.id.clone(),
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
        let server = self
            .server
            .borrow()
            .upgrade()
            .ok_or_else(|| kj::disconnected!("root server gone"))?;
        if deployment
            .namespaces
            .iter()
            .any(|namespace| server.has_configured_namespace_key(&namespace.unique_key))
        {
            return Err(kj::failed!(
                "a root storage key is already used by a configured namespace"
            ));
        }
        let existing = self.plans.borrow().get(&plan_key).cloned();
        if let Some(existing) = existing {
            if existing.deployment != deployment {
                return Err(kj::failed!(
                    "a root plan cannot be replaced under its existing key"
                ));
            }
            for namespace in &deployment.namespaces {
                if let Some(entry) = self.namespaces.borrow().get(&namespace.id) {
                    *entry.source.borrow_mut() = plan_key.clone();
                }
            }
            self.prune_plans();
            return Ok(plan_key);
        }
        {
            let spaces = self.namespaces.borrow();
            for namespace in &deployment.namespaces {
                if let Some(existing) = spaces.get(&namespace.id) {
                    let source = existing.source.borrow();
                    let plans = self.plans.borrow();
                    let owner = plans
                        .get(source.as_str())
                        .ok_or_else(|| kj::failed!("namespace source missing"))?;
                    if existing.unique_key != namespace.unique_key
                        || owner.deployment.worker_id != deployment.worker_id
                        || owner.deployment.record()?.tenant != deployment.record()?.tenant
                    {
                        return Err(kj::failed!(
                            "a root namespace cannot change its owner or storage"
                        ));
                    }
                } else if spaces
                    .values()
                    .any(|entry| entry.unique_key == namespace.unique_key)
                {
                    return Err(kj::failed!(
                        "a storage directory is already owned by another namespace"
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
            retiring: RefCell::new(None),
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
        for entry in spaces.values() {
            *entry.source.borrow_mut() = plan_key.clone();
        }
        self.namespaces.borrow_mut().extend(spaces);
        assert!(
            self.compiled
                .borrow_mut()
                .insert(key.clone(), worker)
                .is_none(),
            "root instance key collision"
        );
        *plan.loaded.borrow_mut() = Some(key);
        self.plans.borrow_mut().insert(plan_key.clone(), plan);
        self.prune_plans();
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
        let retiring = plan.retiring.borrow().clone();
        if let Some(retiring) = retiring {
            retiring.await;
        }
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
        assert!(
            self.compiled
                .borrow_mut()
                .insert(key.clone(), Rc::clone(&worker))
                .is_none(),
            "root instance key collision"
        );
        *plan.loaded.borrow_mut() = Some(key);
        Ok(worker)
    }

    async fn root(self: &Rc<Self>, selection: RootSelection) -> Result<Rc<dyn Channel>> {
        if !selection.dispatch_checked {
            return Err(kj::failed!(
                "jsg.Error: the node has not authorized this root dispatch"
            ));
        }
        if selection.object_id.len() != 64
            || !selection
                .object_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || selection.data_epoch.is_empty()
            || selection.data_epoch.parse::<u64>().is_err()
        {
            return Err(kj::failed!("jsg.Error: invalid explicit root selection"));
        }
        let plan = self
            .plans
            .borrow()
            .get(&selection.plan)
            .cloned()
            .ok_or_else(|| kj::failed!("unknown root plan"))?;
        let descriptor = plan
            .deployment
            .namespaces
            .iter()
            .find(|space| space.id == selection.namespace_id)
            .ok_or_else(|| kj::failed!("this plan does not own the root namespace"))?;
        self.worker(&selection.plan).await?;
        let name = format!(
            "{}/{}/{}",
            selection.namespace_id, selection.object_id, selection.data_epoch
        );
        let id = ffi::actor_id_for_root(&descriptor.unique_key, &name);
        let key = ffi::actor_id_key(&id);
        let index = (selection.namespace_id.clone(), key.clone());
        let lock = {
            let mut locks = self.selection_locks.borrow_mut();
            match locks.get(&index).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Rc::new(tokio::sync::Mutex::new(()));
                    locks.insert(index.clone(), Rc::downgrade(&lock));
                    lock
                }
            }
        };
        let _cleanup = SelectionLock {
            registry: self.this.clone(),
            index: index.clone(),
            lock: Rc::downgrade(&lock),
        };
        let _guard = lock.lock().await;
        let namespace = self
            .namespaces
            .borrow()
            .get(&selection.namespace_id)
            .map(|entry| Rc::clone(&entry.namespace))
            .ok_or_else(|| kj::failed!("root namespace missing"))?;
        namespace.wait_root_closed(&key).await;
        let previous = self.objects.borrow().get(&index).cloned();
        let old_retirement = previous.as_ref().and_then(|old| {
            self.plans
                .borrow()
                .get(&old.origin.plan)
                .and_then(|plan| plan.retiring.borrow().clone())
        });
        if let Some(old_retirement) = old_retirement {
            old_retirement.await;
        }
        if !self.plans.borrow().contains_key(&selection.plan) {
            return Err(kj::disconnected!("root selection plan removed"));
        }
        if previous
            .as_ref()
            .is_some_and(|previous| previous.origin.plan != selection.plan)
        {
            let namespace = self
                .namespaces
                .borrow()
                .get(&selection.namespace_id)
                .map(|entry| Rc::clone(&entry.namespace))
                .ok_or_else(|| kj::failed!("root namespace missing"))?;
            namespace.abort_root(
                &key,
                &kj::failed!("jsg.Error: the Worker's active version changed"),
            );
            namespace.wait_root_closed(&key).await;
        }
        if !self
            .namespaces
            .borrow()
            .contains_key(&selection.namespace_id)
        {
            return Err(kj::disconnected!("root namespace removed"));
        }
        self.objects.borrow_mut().insert(
            index,
            RootObject {
                origin: RootOrigin::from(&selection),
                class_name: descriptor.class_name.clone(),
                registration: previous
                    .as_ref()
                    .filter(|old| old.origin.plan == selection.plan)
                    .map(|old| Rc::clone(&old.registration))
                    .unwrap_or_else(|| Rc::new(())),
            },
        );
        let spaces = self.namespaces.borrow();
        let namespace = spaces
            .get(&selection.namespace_id)
            .ok_or_else(|| kj::failed!("root namespace missing"))?;
        Ok(namespace.namespace.channel(id, false))
    }

    fn begin_retire(
        self: &Rc<Self>,
        plan_key: &str,
        key: &WorkerKey,
        expected: Option<Weak<WorkerService>>,
        idle: bool,
    ) -> bool {
        let Some(plan) = self.plans.borrow().get(plan_key).cloned() else {
            return false;
        };
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
        let worker = self
            .compiled
            .borrow_mut()
            .remove(key)
            .expect("matched root instance");
        *plan.loaded.borrow_mut() = None;
        assert!(
            self.retiring
                .borrow_mut()
                .insert(key.clone(), Rc::clone(&worker))
                .is_none()
        );
        let (done, wait) = futures::channel::oneshot::channel::<()>();
        *plan.retiring.borrow_mut() = Some(
            async move {
                let _ = wait.await;
            }
            .boxed_local()
            .shared(),
        );
        let roots: Vec<_> = self
            .objects
            .borrow()
            .iter()
            .filter(|(_, object)| object.origin.plan == plan_key)
            .filter_map(|((namespace, storage_key), _)| {
                self.namespaces
                    .borrow()
                    .get(namespace)
                    .map(|entry| (Rc::clone(&entry.namespace), storage_key.clone()))
            })
            .collect();
        let registry = Rc::clone(self);
        let key = key.clone();
        // The plan is detached synchronously above. Requests cannot acquire the retiring isolate;
        // they wait for actor teardown before compiling a new generation on the same storage.
        self.factory.spawn_detached(async move {
            let mut completed = true;
            for (namespace, storage_key) in roots {
                if idle {
                    if let Err(error) = namespace.evict_root_for_unload(&storage_key).await {
                        tracing::error!("retiring idle root: {error:?}");
                        completed = false;
                    }
                } else {
                    namespace.abort_root(
                        &storage_key,
                        &kj::disconnected!("root worker limit exceeded"),
                    );
                    namespace.wait_root_closed(&storage_key).await;
                }
            }
            if completed {
                worker.unlink();
            } else {
                // Do not start a different isolate against a still-running actor on failed
                // graceful teardown. Keep the original generation available for another event.
                assert!(
                    registry
                        .compiled
                        .borrow_mut()
                        .insert(key.clone(), Rc::clone(&worker))
                        .is_none()
                );
                *plan.loaded.borrow_mut() = Some(key.clone());
            }
            registry.retiring.borrow_mut().remove(&key);
            plan.retiring.borrow_mut().take();
            registry.collect_inactive();
            let _ = done.send(());
        });
        true
    }

    async fn retire(self: &Rc<Self>, plan_key: &str, key: &WorkerKey) -> bool {
        let retired = self.begin_retire(plan_key, key, None, true);
        let wait = self
            .plans
            .borrow()
            .get(plan_key)
            .and_then(|plan| plan.retiring.borrow().clone());
        if let Some(wait) = wait {
            wait.await;
        }
        retired
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
            "residentRoots": entry.namespace.resident_roots(),
        })).collect();
        namespaces.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        json!({"plans": plans, "namespaces": namespaces, "compiledWorkers": self.compiled.borrow().len(),
            "objectEntries": self.objects.borrow().len(),
            "objectContainers": self.namespaces.borrow().values().map(|entry| entry.namespace.container_count()).sum::<usize>(),
            "selectionLocks": self.selection_locks.borrow().len(),
            "ignoredRetirements": self.ignored_retirements.get(), "pid": std::process::id()})
    }

    pub fn shutdown(&self) {
        let namespaces = std::mem::take(&mut *self.namespaces.borrow_mut());
        for entry in namespaces.values() {
            entry
                .namespace
                .abort_all(Some(&kj::disconnected!("root registry shutdown")));
        }
        let workers = std::mem::take(&mut *self.compiled.borrow_mut());
        for worker in workers.values() {
            worker.unlink();
        }
        for worker in std::mem::take(&mut *self.retiring.borrow_mut()).values() {
            worker.unlink();
        }
        drop(namespaces);
        self.objects.borrow_mut().clear();
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
    namespace_id: String,
}

struct SelectionLock {
    registry: Weak<RootRegistry>,
    index: (String, String),
    lock: Weak<tokio::sync::Mutex<()>>,
}
impl Drop for SelectionLock {
    fn drop(&mut self) {
        if self.lock.strong_count() == 1 {
            if let Some(registry) = self.registry.upgrade() {
                let mut locks = registry.selection_locks.borrow_mut();
                if locks
                    .get(&self.index)
                    .is_some_and(|lock| Weak::ptr_eq(lock, &self.lock))
                {
                    locks.remove(&self.index);
                }
            }
        }
    }
}

impl ActorClass for RootClass {
    fn runtime_root(&self) -> bool {
        true
    }
    fn for_root(&self, id: &ffi::ActorIdHandle) -> Option<Rc<dyn ActorClass>> {
        let registry = self.registry.upgrade()?;
        let index = (self.namespace_id.clone(), ffi::actor_id_key(id));
        let object = registry.objects.borrow().get(&index).cloned()?;
        if !registry.plans.borrow().contains_key(&object.origin.plan) {
            return None;
        }
        let props = registry.root_props(&object.origin).ok()?;
        Some(Rc::new(RootObjectClass {
            index,
            registration: Rc::clone(&object.registration),
            public_id: object.origin.object_id,
            class: RootExportedClass {
                registry: self.registry.clone(),
                worker_id: registry
                    .plans
                    .borrow()
                    .get(&object.origin.plan)?
                    .deployment
                    .worker_id
                    .clone(),
                plan_key: object.origin.plan,
                class_name: object.class_name,
                props: Some(props),
                persistent: false,
                generation: Cell::new(None),
                running_worker: RefCell::new(Weak::new()),
            },
        }))
    }
    fn new_actor(&self, _request: NewActor<'_>) -> Result<KjOwn<ffi::ActorHandle>> {
        Err(kj::failed!(
            "jsg.Error: the root has no explicit node selection"
        ))
    }
    fn start_request(
        &self,
        _metadata: KjOwn<RequestMetadata>,
        _actor: &ffi::ActorHandle,
    ) -> Result<KjOwn<WorkerInterface>> {
        Err(kj::failed!("root class was not specialized"))
    }
    fn run_on_hibernate<'a>(
        &'a self,
        _metadata: KjOwn<RequestMetadata>,
        _actor: &'a ffi::ActorHandle,
    ) -> LocalBoxFuture<'a, Result<()>> {
        Box::pin(async { Err(kj::failed!("root class was not specialized")) })
    }
    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(not_transferable("Private root namespace"))
    }
}

/// One container's immutable load plan. Namespace promotion never retargets an existing actor.
struct RootObjectClass {
    index: (String, String),
    registration: Rc<()>,
    public_id: String,
    class: RootExportedClass,
}

// Class credentials carry the immutable plan and props, never a strong isolate reference.
// A child facet can survive an unload and resolve its own class on the next invocation.
struct RootExportedClass {
    registry: Weak<RootRegistry>,
    plan_key: String,
    worker_id: String,
    class_name: String,
    props: Option<KjOwn<ffi::Frankenvalue>>,
    persistent: bool,
    generation: Cell<Option<u64>>,
    running_worker: RefCell<Weak<WorkerService>>,
}
impl RootExportedClass {
    fn registry(&self) -> Result<Rc<RootRegistry>> {
        self.registry
            .upgrade()
            .ok_or_else(|| kj::disconnected!("root registry gone"))
    }
    fn class(&self) -> Result<Rc<dyn ActorClass>> {
        let registry = self.registry()?;
        let key = WorkerKey {
            plan_key: self.plan_key.clone(),
            generation: self
                .generation
                .get()
                .ok_or_else(|| kj::failed!("root class is not ready"))?,
        };
        let worker = registry.compiled.borrow().get(&key).cloned();
        worker
            .ok_or_else(|| kj::disconnected!("root worker unloaded"))?
            .actor_class(
                Some(&self.class_name),
                self.props.as_deref().map(ffi::frankenvalue_clone),
                self.persistent,
            )
            .ok_or_else(|| kj::failed!("root class missing"))
    }
}
impl ActorClass for RootExportedClass {
    fn for_root_plan(&self, root_plan: &str) -> Result<Option<Rc<dyn ActorClass>>> {
        let registry = self.registry()?;
        let owner = registry
            .plans
            .borrow()
            .get(root_plan)
            .map(|plan| plan.deployment.worker_id.clone())
            .ok_or_else(|| kj::disconnected!("the root class plan was removed"))?;
        if owner != self.worker_id {
            return Ok(None);
        }
        Ok(Some(Rc::new(Self {
            registry: self.registry.clone(),
            plan_key: root_plan.to_owned(),
            worker_id: self.worker_id.clone(),
            class_name: self.class_name.clone(),
            props: self.props.as_deref().map(ffi::frankenvalue_clone),
            persistent: self.persistent,
            generation: Cell::new(None),
            running_worker: RefCell::new(Weak::new()),
        })))
    }
    fn when_ready(&self) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let registry = self.registry()?;
            let worker = registry.worker(&self.plan_key).await?;
            *self.running_worker.borrow_mut() = Rc::downgrade(&worker);
            self.generation.set(
                registry
                    .plans
                    .borrow()
                    .get(&self.plan_key)
                    .and_then(|plan| plan.generation.get()),
            );
            Ok(())
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
        Box::pin(async move {
            // This hook belongs to an existing actor. It never constructs a replacement from
            // a retiring isolate; class() above resolves only published compiled instances.
            let worker = self
                .running_worker
                .borrow()
                .upgrade()
                .ok_or_else(|| kj::disconnected!("root hook worker gone"))?;
            let class = worker
                .actor_class(
                    Some(&self.class_name),
                    self.props.as_deref().map(ffi::frankenvalue_clone),
                    self.persistent,
                )
                .ok_or_else(|| kj::failed!("root hook class missing"))?;
            class.run_on_hibernate(metadata, actor).await
        })
    }
    fn token(&self, usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        let registry = self.registry()?;
        let props = self
            .props
            .as_deref()
            .ok_or_else(|| kj::failed!("an unspecialized root class has no token"))?;
        let reference = serde_json::to_string(&RootClassReference {
            worker_id: self.worker_id.clone(),
            plan: self.plan_key.clone(),
        })
        .map_err(|error| kj::failed!("root class reference: {error}"))?;
        let service = format!("{ROOT_CLASS_REFERENCE}{reference}");
        Ok(ffi::factory_encode_actor_class_token(
            registry.factory.raw(),
            &service,
            &self.class_name,
            Some(props).into(),
            self.persistent,
            usage,
        )?)
    }
    fn for_props(
        &self,
        props: KjOwn<ffi::Frankenvalue>,
        persistent: bool,
    ) -> Result<Rc<dyn ActorClass>> {
        if self.props.is_some() {
            return Err(kj::failed!("can't override root class props"));
        }
        Ok(Rc::new(Self {
            registry: self.registry.clone(),
            plan_key: self.plan_key.clone(),
            worker_id: self.worker_id.clone(),
            class_name: self.class_name.clone(),
            props: Some(props),
            persistent,
            generation: Cell::new(self.generation.get()),
            running_worker: RefCell::new(self.running_worker.borrow().clone()),
        }))
    }
}

impl ActorClass for RootObjectClass {
    fn root_plan(&self) -> Option<&str> {
        Some(&self.class.plan_key)
    }
    fn container_removed(&self) {
        if let Some(registry) = self.class.registry.upgrade() {
            registry.remove_object(&self.index, &self.registration);
        }
    }
    fn when_ready(&self) -> LocalBoxFuture<'_, Result<()>> {
        self.class.when_ready()
    }
    fn new_actor(&self, mut request: NewActor<'_>) -> Result<KjOwn<ffi::ActorHandle>> {
        // Host/app presents O as a string. The native container index and storage spec retain S.
        request.id = ffi::actor_id_from_name(&self.public_id);
        self.class.new_actor(request)
    }
    fn start_request(
        &self,
        metadata: KjOwn<RequestMetadata>,
        actor: &ffi::ActorHandle,
    ) -> Result<KjOwn<WorkerInterface>> {
        self.class.start_request(metadata, actor)
    }
    fn run_on_hibernate<'a>(
        &'a self,
        metadata: KjOwn<RequestMetadata>,
        actor: &'a ffi::ActorHandle,
    ) -> LocalBoxFuture<'a, Result<()>> {
        self.class.run_on_hibernate(metadata, actor)
    }
    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(not_transferable("Private root instance class"))
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
    fn for_props(
        &self,
        props: KjOwn<ffi::Frankenvalue>,
        _persistent: bool,
    ) -> Result<Rc<dyn Channel>> {
        let selection: RootSelection = serde_json::from_str(&ffi::frankenvalue_plain_json(&props)?)
            .map_err(|error| kj::failed!("invalid root selection: {error}"))?;
        if !selection.dispatch_checked {
            return Err(kj::failed!("jsg.Error: node dispatch checks are required"));
        }
        Ok(Rc::new(RootSelectedChannel {
            registry: self
                .this
                .upgrade()
                .ok_or_else(|| kj::disconnected!("root registry gone"))?,
            selection,
        }))
    }

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

struct RootSelectedChannel {
    registry: Rc<RootRegistry>,
    selection: RootSelection,
}
impl Channel for RootSelectedChannel {
    fn start_request(
        &self,
        mut metadata: KjOwn<RequestMetadata>,
    ) -> Result<KjOwn<WorkerInterface>> {
        let registry = Rc::clone(&self.registry);
        let selection = self.selection.clone();
        registry.stamp_metadata(&selection, &mut metadata)?;
        Ok(worker::PromisedInterface::new(async move {
            registry.root(selection).await?.start_request(metadata)
        })
        .into_kj())
    }
    fn require_allows_transfer(&self) -> Result<()> {
        Err(not_transferable("Private selected root"))
    }
    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(not_transferable("Private selected root"))
    }
}

struct RootRequest {
    registry: Rc<RootRegistry>,
    metadata: Option<KjOwn<RequestMetadata>>,
}

impl RootRequest {
    async fn target(&mut self) -> Result<KjOwn<WorkerInterface>> {
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| kj::failed!("root event already used"))?;
        let cf: Option<String> = ffi::request_metadata_cf_blob_json(metadata).into();
        let cf: Value = serde_json::from_str(cf.as_deref().unwrap_or("null"))
            .map_err(|error| kj::failed!("root metadata: {error}"))?;
        let selection: RootSelection = serde_json::from_value(cf["coworkerdRoot"].clone())
            .map_err(|error| {
                kj::failed!("jsg.Error: an explicit node root selection is required: {error}")
            })?;
        let mut metadata = self
            .metadata
            .take()
            .ok_or_else(|| kj::failed!("root event already used"))?;
        self.registry.stamp_metadata(&selection, &mut metadata)?;
        self.registry.root(selection).await?.start_request(metadata)
    }
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
            (Method::POST, "/collect") => {
                self.registry.collect_inactive();
                self.registry.snapshot()
            }
            (Method::POST, "/remove-namespace") => {
                let value = read_json(body).await?;
                let id = value["namespaceId"]
                    .as_str()
                    .ok_or_else(|| kj::failed!("remove-namespace needs a namespaceId"))?;
                let entry = self.registry.namespaces.borrow_mut().remove(id);
                let removed = entry.is_some();
                if let Some(entry) = entry {
                    entry
                        .namespace
                        .abort_all(Some(&kj::disconnected!("root namespace removed")));
                    entry.namespace.wait_roots_closed().await;
                    drop(entry);
                }
                self.registry.prune_plans();
                json!({"removed": removed, "state": self.registry.snapshot()})
            }
            (Method::POST, "/hibernate") => {
                let namespaces: Vec<_> = self
                    .registry
                    .namespaces
                    .borrow()
                    .values()
                    .map(|entry| Rc::clone(&entry.namespace))
                    .collect();
                for namespace in namespaces {
                    namespace.evict_all_for_test(true).await?;
                }
                json!({"hibernated": true, "state": self.registry.snapshot()})
            }
            (_, "/object") => {
                let mut target = worker::CxxWorkerInterface::new(self.target().await?);
                return Service::request(
                    &mut target,
                    method,
                    text.as_bytes(),
                    headers,
                    body,
                    response,
                )
                .await;
            }
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
                let retired = self.registry.retire(plan_key, &key).await;
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
    async fn custom_event(
        &mut self,
        event: KjOwn<worker::CustomEvent>,
    ) -> Result<worker::CustomEventResult> {
        let metadata = self
            .metadata
            .take()
            .ok_or_else(|| kj::failed!("root event already used"))?;
        let registry = Rc::clone(&self.registry);
        // The promised interface reports resolution errors to the RPC event before dropping it.
        let promised = worker::PromisedInterface::new(async move {
            RootRequest {
                registry,
                metadata: Some(metadata),
            }
            .target()
            .await
        });
        let mut target = worker::CxxWorkerInterface::new(promised.into_kj());
        Interface::custom_event(&mut target, event).await
    }
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
