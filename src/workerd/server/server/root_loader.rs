// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Private root loading over an ordinary service binding owned by the platform runtime.
//! Compiled instances and stable namespace/class providers have separate lifetimes.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant, SystemTime};

use base64::Engine as _;
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
use crate::actor::{ActorNamespace, RootCloseFences, wait_for_root};
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
    #[serde(default)]
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
    #[serde(rename = "type", default)]
    module_type: String,
}

#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Namespace {
    id: String,
    class_name: String,
    binding: String,
    unique_key: String,
}

#[derive(Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RootSelection {
    #[serde(default)]
    kind: SelectionKind,
    #[serde(default)]
    namespace_id: String,
    #[serde(default)]
    object_id: String,
    #[serde(default)]
    data_epoch: String,
    plan: String,
    dispatch_checked: bool,
    #[serde(default)]
    tenant_cf: Value,
    #[serde(default)]
    alarm_facet_id: Option<u32>,
    #[serde(default)]
    preparation_nonce: String,
    #[serde(default)]
    entrypoint: String,
    #[serde(default)]
    requires_child_sequence: bool,
    #[serde(default)]
    catalog_generation: u64,
    #[serde(default)]
    active_plan: bool,
}

#[derive(Clone, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
enum SelectionKind {
    #[default]
    Object,
    Plain,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlainSelection {
    plain_selection: RootSelection,
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
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    platform_class: Option<RootTarget>,
    worker_id: String,
    version_id: String,
    #[serde(default)]
    version_fingerprint: String,
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
    #[serde(default)]
    service_bindings: Vec<RootBinding>,
    #[serde(default)]
    structured_props: bool,
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
    fn is_plain(&self) -> bool {
        self.kind == "tenant" && self.namespaces.is_empty()
    }

    fn is_platform(&self) -> bool {
        self.kind == "platform"
    }

    fn owner_tenant(&self) -> Result<&str> {
        if self.is_platform() {
            Ok(&self.tenant)
        } else {
            Ok(&self.record()?.tenant)
        }
    }

    fn initial_generation(&self) -> Result<u64> {
        if self.is_platform() {
            Ok(0)
        } else {
            Ok(self.record()?.generation)
        }
    }

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
        if self.is_platform() {
            return serde_json::to_string(&json!({
                "kind": "platform", "tenant": self.tenant, "workerId": self.worker_id,
                "versionId": self.version_id, "platformClass": self.platform_class,
                "namespaces": self.namespaces,
            }))
            .map_err(|error| kj::failed!("root key: {error}"));
        }
        let record = self.record()?;
        let mut key = json!({
            "workerId": self.worker_id, "versionId": self.version_id,
            "cpuLimitMs": record.cpu_limit_ms, "memoryLimitMib": record.memory_limit_mib,
            "generation": record.generation,
            "versionFingerprint": self.version_fingerprint,
        });
        if self.is_plain() { key["kind"] = json!("plain"); }
        serde_json::to_string(&key)
        .map_err(|error| kj::failed!("root key: {error}"))
    }

    fn validate(&self) -> Result<()> {
        if self.is_platform() {
            let class = self
                .platform_class
                .as_ref()
                .ok_or_else(|| kj::failed!("platform root class missing"))?;
            if self.tenant.is_empty()
                || self.worker_id.is_empty()
                || self.version_id.is_empty()
                || class.service.is_empty()
                || class.entrypoint.is_none()
                || self.creation_record.is_some()
                || !self.modules.is_empty()
                || !self.environment.is_empty()
                || !self.durable_objects.is_empty()
                || !self.service_bindings.is_empty()
                || self.structured_props
                || self.global_outbound.is_some()
                || self.streaming_tail.is_some()
            {
                return Err(kj::failed!(
                    "platform root plans cannot load tenant code or records"
                ));
            }
        } else {
            self.record()?;
            if self.platform_class.is_some() {
                return Err(kj::failed!(
                    "tenant root plans cannot select a platform class"
                ));
            }
        }
        if (!self.is_platform() && self.modules.is_empty()) || (self.is_platform() && self.namespaces.is_empty()) {
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
            if self.is_platform()
                && self
                    .platform_class
                    .as_ref()
                    .and_then(|class| class.entrypoint.as_ref())
                    != Some(&namespace.class_name)
            {
                return Err(kj::failed!("platform root namespace class mismatch"));
            }
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
    key: String,
    deployment: Deployment,
    lock: tokio::sync::Mutex<()>,
    generation: Cell<Option<u64>>,
    loaded: RefCell<Option<WorkerKey>>,
    retiring: RefCell<Option<Shared<LocalBoxFuture<'static, ()>>>>,
    selection_pins: Cell<usize>,
    namespace_refs: Cell<usize>,
    object_refs: Cell<usize>,
    objects: RefCell<HashSet<(String, String)>>,
    retiring_refs: Cell<usize>,
}

const FAILED_INSTALL_CAPACITY: usize = 256;
const FAILED_INSTALL_RESET_AFTER: Duration = Duration::from_secs(60);

struct FailedInstall {
    error: crate::Error,
    expires_at: Instant,
    failed_at: Instant,
    failures: u8,
    last_used: u64,
}

impl Plan {
    fn unused(&self) -> bool {
        if self.deployment.is_plain() && self.loaded.borrow().is_some() {
            return false;
        }
        self.selection_pins.get() == 0 && self.namespace_refs.get() == 0
            && self.object_refs.get() == 0 && self.retiring_refs.get() == 0
    }
}

struct PlanPin(Rc<Plan>, Weak<RootRegistry>);
impl PlanPin {
    fn new(plan: Rc<Plan>, registry: Weak<RootRegistry>) -> Self {
        plan.selection_pins.set(plan.selection_pins.get() + 1);
        Self(plan, registry)
    }
}
impl Drop for PlanPin {
    fn drop(&mut self) {
        self.0.selection_pins.set(self.0.selection_pins.get() - 1);
        if self.0.selection_pins.get() == 0 {
            if let Some(registry) = self.1.upgrade() {
                registry.schedule_prune(&self.0.key);
            }
        }
    }
}

#[derive(Clone)]
struct NamespaceEntry {
    namespace: Rc<ActorNamespace>,
    /// A class provider is independent of the compiled instance that currently implements it.
    source: Rc<RefCell<String>>,
    class_name: String,
    unique_key: String,
    removing: Rc<Cell<bool>>,
}

struct InstallFlight {
    deployment: RefCell<Option<Rc<Deployment>>>,
    namespaces: Vec<String>,
    cancelled: Cell<bool>,
    result: RefCell<Option<Result<String>>>,
    completed: tokio::sync::Notify,
}

impl InstallFlight {
    async fn wait(&self) -> Result<String> {
        loop {
            let notified = self.completed.notified();
            if let Some(result) = self.result.borrow().clone() {
                return result;
            }
            notified.await;
        }
    }
}

pub struct RootRegistry {
    this: Weak<Self>,
    factory: Rc<Factory>,
    storage: String,
    plain_only: bool,
    plain_generation: Cell<u64>,
    latest_plain: RefCell<HashMap<String, (u64, String)>>,
    install_lock: tokio::sync::Mutex<()>,
    installing: RefCell<HashMap<String, Rc<InstallFlight>>>,
    failed_installs: RefCell<HashMap<String, FailedInstall>>,
    failure_use: Cell<u64>,
    tenant_compilations: Cell<u64>,
    removed_namespaces: RefCell<HashSet<String>>,
    collect_queued: Cell<bool>,
    prune_pending: RefCell<Vec<String>>,
    plans: RefCell<HashMap<String, Rc<Plan>>>,
    namespaces: RefCell<HashMap<String, NamespaceEntry>>,
    closing_roots: Rc<RootCloseFences>,
    objects: RefCell<HashMap<(String, String), RootObject>>,
    selection_locks: RefCell<HashMap<(String, String), Weak<tokio::sync::Mutex<()>>>>,
    compiled: RefCell<HashMap<WorkerKey, Rc<WorkerService>>>,
    retiring: RefCell<HashMap<WorkerKey, Rc<WorkerService>>>,
    ignored_retirements: Cell<u64>,
    server: RefCell<Weak<crate::config::Server>>,
}

impl RootRegistry {
    pub fn new(factory: Rc<Factory>, storage: String, plain_only: bool) -> Result<Rc<Self>> {
        if !plain_only && !std::path::Path::new(&storage).is_absolute() {
            return Err(kj::failed!(
                "the private root loader needs an absolute storagePath"
            ));
        }
        Ok(Rc::new_cyclic(|this| Self {
            this: this.clone(),
            factory,
            storage,
            plain_only,
            plain_generation: Cell::new(0),
            latest_plain: RefCell::new(HashMap::new()),
            install_lock: tokio::sync::Mutex::new(()),
            installing: RefCell::new(HashMap::new()),
            failed_installs: RefCell::new(HashMap::new()),
            failure_use: Cell::new(0),
            tenant_compilations: Cell::new(0),
            removed_namespaces: RefCell::new(HashSet::new()),
            collect_queued: Cell::new(false),
            prune_pending: RefCell::new(Vec::new()),
            plans: RefCell::new(HashMap::new()),
            namespaces: RefCell::new(HashMap::new()),
            closing_roots: Rc::default(),
            objects: RefCell::new(HashMap::new()),
            selection_locks: RefCell::new(HashMap::new()),
            compiled: RefCell::new(HashMap::new()),
            retiring: RefCell::new(HashMap::new()),
            ignored_retirements: Cell::new(0),
            server: RefCell::new(Weak::new()),
        }))
    }

    fn cached_failure(&self, key: &str) -> Option<crate::Error> {
        let mut failures = self.failed_installs.borrow_mut();
        let expires_at = failures.get(key)?.expires_at;
        // Successful hot plans do not read the clock for an unrelated failed plan.
        let now = Instant::now();
        if now >= expires_at {
            if now.duration_since(failures.get(key)?.failed_at) >= FAILED_INSTALL_RESET_AFTER {
                failures.remove(key);
            }
            return None;
        }
        let used = self.failure_use.get().saturating_add(1);
        self.failure_use.set(used);
        let failure = failures.get_mut(key).expect("unexpired failed install");
        failure.last_used = used;
        Some(failure.error.clone())
    }

    fn remember_failure(&self, key: &str, error: &crate::Error) {
        if !error.description().contains("ROOT_TENANT_CODE: ") {
            return;
        }
        let now = Instant::now();
        let mut failures = self.failed_installs.borrow_mut();
        failures.retain(|_, failure| now.duration_since(failure.failed_at) < FAILED_INSTALL_RESET_AFTER);
        let consecutive = failures.get(key).map_or(1, |failure| failure.failures.saturating_add(1));
        let delay = Duration::from_secs((1u64 << u32::from(consecutive.saturating_sub(1).min(5))).min(30));
        if failures.len() >= FAILED_INSTALL_CAPACITY && !failures.contains_key(key) {
            let oldest = failures.iter().min_by_key(|(_, failure)| failure.last_used)
                .map(|(key, _)| key.clone()).expect("full failed install cache");
            failures.remove(&oldest);
        }
        let used = self.failure_use.get().saturating_add(1);
        self.failure_use.set(used);
        failures.insert(key.to_owned(), FailedInstall {
            error: error.clone(), expires_at: now + delay, failed_at: now,
            failures: consecutive, last_used: used,
        });
    }

    fn loaded(&self, plan: &Plan) -> Option<Rc<WorkerService>> {
        plan.loaded
            .borrow()
            .as_ref()
            .and_then(|key| self.compiled.borrow().get(key).cloned())
    }

    fn next_plain_generation(&self) -> Result<u64> {
        let generation = self.plain_generation.get().checked_add(1)
            .ok_or_else(|| kj::failed!("plain load generation exhausted"))?;
        self.plain_generation.set(generation);
        Ok(generation)
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
        // Even an obsolete plan's canonical key names its owner. Do not accept a reference
        // merely because some unrelated plan of the claimed Worker happens to remain loaded.
        let referenced_key: Value = serde_json::from_str(plan_key).ok()?;
        if referenced_key.get("workerId")?.as_str()? != worker_id
            || self
                .plans
                .borrow()
                .get(plan_key)
                .is_some_and(|plan| plan.deployment.worker_id != worker_id)
        {
            return None;
        }
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
            let object = self.objects.borrow_mut().remove(index).unwrap();
            if let Some(plan) = self.plans.borrow().get(&object.origin.plan) {
                plan.object_refs.set(plan.object_refs.get() - 1);
                plan.objects.borrow_mut().remove(index);
            }
            self.schedule_prune(&object.origin.plan);
        }
    }

    fn schedule_prune(&self, key: &str) {
        if !self.plans.borrow().get(key).is_some_and(|plan| plan.unused()) { return; }
        self.prune_pending.borrow_mut().push(key.to_owned());
        if self.collect_queued.replace(true) { return; }
        let Some(registry) = self.this.upgrade() else { return; };
        let factory = Rc::clone(&self.factory);
        let timer = Rc::clone(&factory);
        factory.spawn_detached(async move {
            timer.sleep(Duration::ZERO).await;
            loop {
                registry.prune_plans();
                if registry.prune_pending.borrow().is_empty() { break; }
                timer.sleep(Duration::from_millis(1)).await;
            }
            registry.collect_queued.set(false);
        });
    }

    fn prune_plans(&self) {
        for _ in 0..64 {
            let Some(key) = self.prune_pending.borrow_mut().pop() else { break; };
            if !self.plans.borrow().get(&key).is_some_and(|plan| plan.unused()) { continue; }
            let plan = self.plans.borrow_mut().remove(&key).unwrap();
            if plan.deployment.is_plain() {
                let latest = self.latest_plain.borrow().get(&plan.deployment.worker_id)
                    .is_some_and(|(_, current)| current == &key);
                if latest { self.latest_plain.borrow_mut().remove(&plan.deployment.worker_id); }
            }
            let instance = plan.loaded.borrow_mut().take();
            if let Some(instance) = instance {
                let worker = self.compiled.borrow_mut().remove(&instance);
                if let Some(worker) = worker {
                    if !plan.deployment.is_platform() { worker.unlink(); }
                }
            }
        }
    }

    fn change_source(&self, entry: &NamespaceEntry, key: &str) {
        let old = entry.source.replace(key.to_owned());
        if old == key { return; }
        if let Some(plan) = self.plans.borrow().get(key) {
            plan.namespace_refs.set(plan.namespace_refs.get() + 1);
        }
        if let Some(plan) = self.plans.borrow().get(&old) {
            plan.namespace_refs.set(plan.namespace_refs.get() - 1);
        }
        self.schedule_prune(&old);
    }

    fn forget_namespace(&self, id: &str) {
        let entry = self.namespaces.borrow_mut().remove(id);
        if let Some(entry) = entry {
            let key = entry.source.borrow();
            if let Some(plan) = self.plans.borrow().get(key.as_str()) {
                plan.namespace_refs.set(plan.namespace_refs.get() - 1);
            }
            self.schedule_prune(&key);
        }
    }

    fn finish_retirement(&self, key: &WorkerKey, plan: &Plan) {
        if self.retiring.borrow_mut().remove(key).is_some() {
            plan.retiring_refs.set(plan.retiring_refs.get() - 1);
            self.schedule_prune(&plan.key);
        }
    }

    async fn collect_inactive_in_batches(&self) {
        let namespaces: Vec<_> = self.namespaces.borrow().values()
            .map(|entry| Rc::clone(&entry.namespace)).collect();
        for namespace in namespaces { namespace.prune_idle_roots_in_batches().await; }
        if self.objects.borrow().is_empty() { self.objects.borrow_mut().shrink_to_fit(); }
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

    fn plain_channel(&self, selection: RootSelection) -> Result<Rc<dyn Channel>> {
        if !selection.namespace_id.is_empty() || !selection.object_id.is_empty() || !selection.data_epoch.is_empty() {
            return Err(kj::failed!("an ordinary worker selection cannot name an object"));
        }
        let plan = self.plans.borrow().get(&selection.plan).cloned()
            .ok_or_else(|| kj::failed!("unknown root plan"))?;
        if !plan.deployment.is_plain() {
            return Err(kj::failed!("an ordinary worker selection needs a namespace-free tenant plan"));
        }
        // The hot path neither waits on the plan lock nor creates a deferred load.
        let worker = self.loaded(&plan).ok_or_else(|| kj::failed!("unknown root plan: instance was evicted"))?;
        // The node's catalog generation orders publication independently of compilation.
        // A delayed call pinned to an older definition cannot supersede the current plan.
        let previous = if selection.active_plan {
            let mut latest = self.latest_plain.borrow_mut();
            let previous = latest.get(&plan.deployment.worker_id);
            if previous.is_none_or(|(generation, _)| selection.catalog_generation >= *generation) {
                let changed = previous.is_none_or(|(_, key)| key != &selection.plan);
                if changed || previous.is_some_and(|(generation, _)| *generation != selection.catalog_generation) {
                    latest.insert(plan.deployment.worker_id.clone(), (selection.catalog_generation, selection.plan.clone()))
                        .filter(|(_, key)| key != &selection.plan).map(|(_, key)| key)
                } else { None }
            } else { None }
        } else { None };
        if let Some(previous) = previous {
            let old = self.plans.borrow().get(&previous).cloned();
            if let Some(worker) = old.as_ref().and_then(|old| self.loaded(old)) { worker.retire_when_idle()?; }
        }
        let mut services = ffi::frankenvalue_new();
        let mut objects = ffi::frankenvalue_from_json("[]");
        let call = |target: &RootTarget| -> Value {
            let mut props = target.props.clone();
            if !props.is_object() { props = json!({}); }
            props["treeId"] = selection.tenant_cf["treeId"].clone();
            props["parentInvocationId"] = selection.tenant_cf["invocationId"].clone();
            props["orderSequence"] = selection.tenant_cf["orderSequence"].clone();
            props["expiresAtMonotonicMs"] = selection.tenant_cf["lease"]["expiresAtMonotonicMs"].clone();
            props["requiresChildSequence"] = json!(selection.requires_child_sequence);
            props
        };
        for binding in &plan.deployment.service_bindings {
            let name = binding.metadata["name"].as_str().ok_or_else(|| kj::failed!("ordinary service name missing"))?;
            ffi::frankenvalue_set_service_stub(services.pin_mut(), name,
                crate::channels::SubrequestChannel::new(self.target(&binding.target, &call(&binding.target))?));
        }
        for (index, binding) in plan.deployment.durable_objects.iter().enumerate() {
            let mut entry = ffi::frankenvalue_from_json(&binding.metadata.to_string());
            let available = binding.metadata["namespaceId"].as_str().is_some_and(|id| !id.is_empty());
            if available {
                ffi::frankenvalue_set_service_stub(entry.pin_mut(), "fetcher",
                    crate::channels::SubrequestChannel::new(self.target(&binding.target, &call(&binding.target))?));
            }
            ffi::frankenvalue_set_value(objects.pin_mut(), &index.to_string(), entry);
        }
        let props = if plan.deployment.structured_props {
            let mut props = ffi::frankenvalue_from_json(&json!({"entrypoint": if selection.entrypoint.is_empty() { "default" } else { &selection.entrypoint }}).to_string());
            ffi::frankenvalue_set_value(props.pin_mut(), "bindings", services);
            ffi::frankenvalue_set_value(props.pin_mut(), "durableObjects", objects);
            props
        } else { services };
        let entrypoint = worker.entrypoint(None, Some(props), false)
            .ok_or_else(|| kj::failed!("ordinary bridge has no default entrypoint"))?;
        Ok(Rc::new(PlainSelectedChannel {registry: self.this.upgrade().ok_or_else(|| kj::disconnected!("root registry gone"))?,
            selection, entrypoint, _pin: PlanPin::new(plan, self.this.clone())}))
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
        if let Some(class) = &deployment.platform_class {
            return self
                .server
                .borrow()
                .upgrade()
                .ok_or_else(|| kj::disconnected!("root server gone"))?
                .root_platform_worker(
                    &class.service,
                    class.entrypoint.as_deref().unwrap_or("default"),
                );
        }
        // A binding invocation must yield its calling isolate before compiling another isolate.
        self.tenant_compilations.set(self.tenant_compilations.get() + 1);
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
                match source.module_type.as_str() {
                    "" | "js" => module.set_es_module(source.source.as_str()),
                    "cjs" => module.set_common_js_module(source.source.as_str()),
                    "text" => module.set_text(source.source.as_str()),
                    "json" => module.set_json(source.source.as_str()),
                    "data" | "wasm" => {
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(&source.source)
                            .map_err(|error| kj::failed!("root module base64: {error}"))?;
                        if source.module_type == "data" {
                            module.set_data(&bytes);
                        } else {
                            module.set_wasm(&bytes);
                        }
                    }
                    _ => return Err(kj::failed!("unsupported root module type")),
                }
            }
            if !deployment.is_plain() {
                conf.reborrow().init_durable_object_storage().set_local_disk("root-storage");
            }
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
            startup_identity: if deployment.is_plain() {
                json!({"workerId": deployment.worker_id, "versionId": deployment.version_id,
                    "invocationId": format!("load:{}:{}", deployment.version_fingerprint, key.generation)}).to_string()
            } else { record.startup_identity.clone() },
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
                "ROOT_TENANT_CODE: root worker failed to compile or is missing an exported class: {}",
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
        if plan.deployment.is_platform() {
            return Ok(());
        }
        let _ = spaces;
        let pending = worker
            .take_pending_link()
            .ok_or_else(|| kj::failed!("root worker was already linked"))?;
        if plan.deployment.is_plain() && (!pending.loopback.actor_namespaces.is_empty()
            || !pending.loopback.actor_classes.is_empty() || !pending.bindings.actors.is_empty()) {
            return Err(kj::failed!("ROOT_TENANT_CODE: a plain plan cannot create actor storage"));
        }
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
            if plan.deployment.is_plain() { None } else { Some(&self.storage) },
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
                if existing.removing.get() {
                    return Err(kj::disconnected!(
                        "root namespace removal is still in progress; retry"
                    ));
                }
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
            namespace.set_root_close_fences(Rc::clone(&self.closing_roots));
            namespace.link(Some(&self.storage))?;
            result.insert(
                space.id.clone(),
                NamespaceEntry {
                    namespace,
                    source,
                    class_name: space.class_name.clone(),
                    unique_key: space.unique_key.clone(),
                    removing: Rc::new(Cell::new(false)),
                },
            );
        }
        Ok(result)
    }

    async fn install(self: &Rc<Self>, deployment: Deployment) -> Result<String> {
        deployment.validate()?;
        self.validate_install(&deployment)?;
        let plan_key = deployment.plan_key()?;
        if let Some(error) = self.cached_failure(&plan_key) {
            return Err(error);
        }
        let existing = self.plans.borrow().get(&plan_key).cloned();
        if let Some(existing) = existing {
            if existing.deployment != deployment {
                return Err(kj::failed!("a root plan cannot be replaced under its existing key"));
            }
            for namespace in &deployment.namespaces {
                if let Some(entry) = self.namespaces.borrow().get(&namespace.id) {
                    if entry.removing.get() {
                        return Err(kj::disconnected!("root namespace removal is still in progress; retry"));
                    }
                    self.change_source(entry, &plan_key);
                }
            }
            if deployment.is_plain() { self.worker(&plan_key).await?; }
            return Ok(plan_key);
        }
        let existing = self.installing.borrow().get(&plan_key).cloned();
        let flight = if let Some(flight) = existing {
            if flight.deployment.borrow().as_ref().is_none_or(|value| **value != deployment) {
                return Err(kj::failed!("a root plan cannot be replaced under its existing key"));
            }
            flight
        } else {
            let flight = Rc::new(InstallFlight {
                namespaces: deployment.namespaces.iter().map(|space| space.id.clone()).collect(),
                deployment: RefCell::new(Some(Rc::new(deployment))),
                cancelled: Cell::new(false),
                result: RefCell::new(None),
                completed: tokio::sync::Notify::new(),
            });
            self.installing.borrow_mut().insert(plan_key.clone(), Rc::clone(&flight));
            let registry = Rc::clone(self);
            let task = Rc::clone(&flight);
            let key = plan_key.clone();
            // Compilation belongs to the registry; cancellation of a waiting caller cannot
            // strand its source or cause a second concurrent compilation of the same key.
            self.factory.spawn_detached(async move {
                let deployment = task.deployment.borrow().as_ref().cloned().expect("install deployment");
                let result = registry.install_compiled((*deployment).clone(), &task).await;
                if let Err(error) = &result {
                    registry.remember_failure(&key, error);
                } else {
                    registry.failed_installs.borrow_mut().remove(&key);
                }
                drop(deployment);
                task.deployment.borrow_mut().take();
                *task.result.borrow_mut() = Some(result);
                registry.installing.borrow_mut().remove(&key);
                task.completed.notify_waiters();
            });
            flight
        };
        wait_for_root(
            &self.factory,
            flight.namespaces.first().map(String::as_str).unwrap_or(&plan_key),
            "",
            "plan installation",
            flight.wait(),
        )
        .await?
    }

    fn validate_install(&self, deployment: &Deployment) -> Result<()> {
        if self.plain_only && !deployment.is_plain() {
            return Err(kj::failed!("this process accepts only ordinary worker plans"));
        }
        if deployment.is_plain() { return Ok(()); }
        if deployment.namespaces.iter().any(|space| self.removed_namespaces.borrow().contains(&space.id)) {
            return Err(kj::disconnected!("the root namespace was deleted by its node"));
        }
        if self.namespaces.borrow().iter().any(|(id, entry)| {
            entry.removing.get()
                && deployment
                    .namespaces
                    .iter()
                    .any(|space| space.id == *id || space.unique_key == entry.unique_key)
        }) {
            return Err(kj::disconnected!(
                "root namespace removal is still in progress; retry"
            ));
        }
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
                        || owner.deployment.owner_tenant()? != deployment.owner_tenant()?
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
        Ok(())
    }

    async fn install_compiled(self: &Rc<Self>, deployment: Deployment, flight: &InstallFlight) -> Result<String> {
        self.validate_install(&deployment)?;
        let plan_key = deployment.plan_key()?;
        let generation = if deployment.is_plain() { self.next_plain_generation()? }
            else { deployment.initial_generation()? };
        let key = deployment.key(generation)?;
        let plan = Rc::new(Plan {
            key: plan_key.clone(),
            deployment,
            lock: tokio::sync::Mutex::new(()),
            generation: Cell::new(Some(key.generation)),
            loaded: RefCell::new(None),
            retiring: RefCell::new(None),
            selection_pins: Cell::new(0),
            namespace_refs: Cell::new(0),
            object_refs: Cell::new(0),
            objects: RefCell::new(HashSet::new()),
            retiring_refs: Cell::new(0),
        });
        let worker = self.compile(&plan.deployment, &key).await?;
        let _guard = wait_for_root(&self.factory,
            plan.deployment.namespaces.first().map(|space| space.id.as_str()).unwrap_or(&plan_key), "",
            "registry publication", self.install_lock.lock()).await?;
        if flight.cancelled.get() || self.validate_install(&plan.deployment).is_err() {
            if !plan.deployment.is_platform() { worker.unlink(); }
            return Err(kj::disconnected!("root namespace removed while its plan compiled"));
        }
        let spaces = match self.make_spaces(&plan.deployment, &plan_key) {
            Ok(spaces) => spaces,
            Err(error) => {
                if !plan.deployment.is_platform() {
                    worker.unlink();
                }
                return Err(error);
            }
        };
        if let Err(error) = self.link(&plan_key, &plan, &key, &worker, &spaces) {
            if !plan.deployment.is_platform() {
                worker.unlink();
            }
            return Err(error);
        }
        self.plans.borrow_mut().insert(plan_key.clone(), Rc::clone(&plan));
        for (id, entry) in &spaces {
            if self.namespaces.borrow().contains_key(id) {
                self.change_source(entry, &plan_key);
            } else {
                plan.namespace_refs.set(plan.namespace_refs.get() + 1);
            }
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
        let _pin = PlanPin::new(Rc::clone(&plan), self.this.clone());
        let _guard = wait_for_root(
            &self.factory,
            &plan.deployment.worker_id,
            "",
            "plan load lock",
            plan.lock.lock(),
        )
        .await?;
        let retiring = plan.retiring.borrow().clone();
        if let Some(retiring) = retiring {
            wait_for_root(
                &self.factory,
                &plan.deployment.worker_id,
                "",
                "plan retirement",
                retiring,
            )
            .await?;
        }
        if let Some(worker) = self.loaded(&plan) {
            return Ok(worker);
        }
        if let Some(error) = self.cached_failure(plan_key) {
            return Err(error);
        }
        let generation = if plan.deployment.is_plain() { self.next_plain_generation()? } else {
            plan.generation.get().and_then(|value| value.checked_add(1))
                .ok_or_else(|| kj::failed!("root load generation exhausted"))?
        };
        plan.generation.set(Some(generation));
        let key = plan.deployment.key(generation)?;
        let worker = match self.compile(&plan.deployment, &key).await {
            Ok(worker) => worker,
            Err(error) => {
                self.remember_failure(plan_key, &error);
                return Err(error);
            }
        };
        self.failed_installs.borrow_mut().remove(plan_key);
        let linked = self.link(plan_key, &plan, &key, &worker, &self.namespaces.borrow());
        if let Err(error) = linked {
            if !plan.deployment.is_platform() {
                worker.unlink();
            }
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
        if selection.kind == SelectionKind::Plain {
            return self.plain_channel(selection.clone())
                .map_err(|error| preparation_error(&selection.preparation_nonce, error));
        }
        if self.plain_only {
            return Err(kj::failed!("this process cannot select durable objects"));
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
        let _guard = wait_for_root(
            &self.factory,
            &selection.namespace_id,
            &key,
            "root selection lock",
            lock.lock(),
        )
        .await?;
        let entry = self
            .namespaces
            .borrow()
            .get(&selection.namespace_id)
            .cloned()
            .ok_or_else(|| kj::failed!("root namespace missing"))?;
        if entry.removing.get() || entry.unique_key != descriptor.unique_key {
            return Err(kj::disconnected!(
                "root namespace changed or is being removed; retry"
            ));
        }
        let same_owner = self
            .plans
            .borrow()
            .get(entry.source.borrow().as_str())
            .is_some_and(|owner| {
                owner.deployment.worker_id == plan.deployment.worker_id
                    && owner
                        .deployment
                        .creation_record
                        .as_ref()
                        .map(|record| &record.tenant)
                        == plan
                            .deployment
                            .creation_record
                            .as_ref()
                            .map(|record| &record.tenant)
            });
        if !same_owner {
            return Err(kj::failed!(
                "the selected plan does not own the current root namespace"
            ));
        }
        let namespace = Rc::clone(&entry.namespace);
        namespace.wait_root_closed(&key).await?;
        let previous = self.objects.borrow().get(&index).cloned();
        let old_retirement = previous.as_ref().and_then(|old| {
            self.plans
                .borrow()
                .get(&old.origin.plan)
                .and_then(|plan| plan.retiring.borrow().clone())
        });
        if let Some(old_retirement) = old_retirement {
            wait_for_root(
                &self.factory,
                &selection.namespace_id,
                &key,
                "previous plan retirement",
                old_retirement,
            )
            .await?;
        }
        if !self.plans.borrow().contains_key(&selection.plan) {
            return Err(kj::disconnected!("root selection plan removed"));
        }
        if previous
            .as_ref()
            .is_some_and(|previous| previous.origin.plan != selection.plan)
        {
            if entry.removing.get()
                || !self
                    .namespaces
                    .borrow()
                    .get(&selection.namespace_id)
                    .is_some_and(|current| Rc::ptr_eq(&current.namespace, &entry.namespace))
            {
                return Err(kj::disconnected!("root namespace removed"));
            }
            namespace.abort_root(
                &key,
                &kj::failed!("jsg.Error: the Worker's active version changed"),
            );
            namespace.wait_root_closed(&key).await?;
        }
        if !self
            .namespaces
            .borrow()
            .get(&selection.namespace_id)
            .is_some_and(|current| {
                !current.removing.get() && Rc::ptr_eq(&current.namespace, &entry.namespace)
            })
        {
            return Err(kj::disconnected!("root namespace removed"));
        }
        let registration = previous
            .as_ref()
            .filter(|old| old.origin.plan == selection.plan)
            .map(|old| Rc::clone(&old.registration))
            .unwrap_or_else(|| Rc::new(()));
        let old = self.objects.borrow_mut().insert(
            index.clone(),
            RootObject {
                origin: RootOrigin::from(&selection),
                class_name: descriptor.class_name.clone(),
                registration: Rc::clone(&registration),
            },
        );
        if old.as_ref().is_none_or(|old| old.origin.plan != selection.plan) {
            if let Some(plan) = self.plans.borrow().get(&selection.plan) {
                plan.object_refs.set(plan.object_refs.get() + 1);
                plan.objects.borrow_mut().insert(index.clone());
            }
            if let Some(old) = old {
                if let Some(plan) = self.plans.borrow().get(&old.origin.plan) {
                    plan.object_refs.set(plan.object_refs.get() - 1);
                    plan.objects.borrow_mut().remove(&index);
                }
                self.schedule_prune(&old.origin.plan);
            }
        }
        let result = match selection.alarm_facet_id {
            Some(facet) => namespace.root_alarm_channel(id, facet, selection.preparation_nonce),
            None => namespace.root_channel(id, selection.preparation_nonce),
        };
        if result.is_err() && !namespace.has_root_container(&key) {
            self.remove_object(&index, &registration);
        }
        result
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
        if plan.deployment.is_platform() {
            return false;
        }
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
        plan.retiring_refs.set(plan.retiring_refs.get() + 1);
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
        let roots: Vec<_> = plan
            .objects
            .borrow()
            .iter()
            .filter_map(|(namespace, storage_key)| {
                self.namespaces
                    .borrow()
                    .get(namespace)
                    .map(|entry| (Rc::clone(&entry.namespace), storage_key.clone()))
            })
            .collect();
        let registry = Rc::clone(self);
        let key = key.clone();
        if !idle {
            self.factory.spawn_detached(async move {
                // Leave the limit callback's stack before cancelling actors: cancelling the
                // currently executing KJ promise would otherwise destroy its own callback.
                // Stop every old container before releasing the plan, then wait on storage
                // keys independently, without keeping healthy objects behind one old handle.
                for (index, (namespace, storage_key)) in roots.iter().enumerate() {
                    namespace.abort_root(
                        storage_key,
                        &kj::disconnected!("root worker limit exceeded"),
                    );
                    if (index + 1) % 16 == 0 {
                        registry.factory.sleep(Duration::from_millis(1)).await;
                    }
                }
                plan.retiring.borrow_mut().take();
                let _ = done.send(());
                futures::future::join_all(
                    roots
                        .iter()
                        .map(|(namespace, key)| namespace.root_closed(key)),
                )
                .await;
                worker.unlink();
                registry.finish_retirement(&key, &plan);
                registry.collect_inactive_in_batches().await;
            });
            return true;
        }
        // Graceful evictions run concurrently. Caller deadlines do not cancel this task.
        self.factory.spawn_detached(async move {
            let mut completed = true;
            let mut closing = Vec::with_capacity(roots.len());
            for (index, (namespace, storage_key)) in roots.iter().enumerate() {
                let factory = Rc::clone(&registry.factory);
                let namespace = Rc::clone(namespace);
                let storage_key = storage_key.clone();
                closing.push(registry.factory.spawn(async move {
                        let result = wait_for_root(
                            &factory,
                            namespace.unique_key().unwrap_or_default(),
                            &storage_key,
                            "idle tree retirement",
                            namespace.evict_root_for_unload(&storage_key),
                        )
                        .await?;
                        if let Err(error) = &result {
                            tracing::error!(
                                namespace = namespace.unique_key().unwrap_or_default(),
                                storage_key,
                                "retiring idle root: {error:?}"
                            );
                        }
                        result
                }));
                // Start independent closes gradually; one slow hook does not hold later roots.
                if (index + 1) % 64 == 0 {
                    registry.factory.sleep(Duration::from_millis(1)).await;
                }
            }
            let results = futures::future::join_all(closing).await;
            for result in results {
                if let Err(error) = result {
                    tracing::error!("retiring idle root: {error:?}");
                    completed = false;
                }
            }
            if !completed && roots.iter().all(|(namespace, key)| !namespace.has_resident_root(key)) {
                futures::future::join_all(roots.iter().map(|(namespace, key)| namespace.root_closed(key))).await;
                completed = true;
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
            registry.finish_retirement(&key, &plan);
            plan.retiring.borrow_mut().take();
            let _ = done.send(());
            registry.collect_inactive_in_batches().await;
        });
        true
    }

    async fn retire(self: &Rc<Self>, plan_key: &str, key: &WorkerKey) -> Result<bool> {
        let retired = self.begin_retire(plan_key, key, None, true);
        let wait = self
            .plans
            .borrow()
            .get(plan_key)
            .and_then(|plan| plan.retiring.borrow().clone());
        if let Some(wait) = wait {
            wait_for_root(
                &self.factory,
                plan_key,
                "",
                "explicit plan retirement",
                wait,
            )
            .await?;
        }
        Ok(retired)
    }

    fn snapshot(&self) -> Value {
        let mut plans: Vec<_> = self.plans.borrow().iter().map(|(id, plan)| json!({
            "plan": id, "workerId": plan.deployment.worker_id, "versionId": plan.deployment.version_id,
            "loaded": plan.loaded.borrow().is_some(), "generation": plan.generation.get(),
            "key": *plan.loaded.borrow(), "namespaces": plan.deployment.namespaces,
            "kind": plan.deployment.kind, "tenant": plan.deployment.owner_tenant().ok(),
            "platformClass": plan.deployment.platform_class,
            "references": {"namespaces": plan.namespace_refs.get(), "objects": plan.object_refs.get(),
                "retiring": plan.retiring_refs.get(), "pins": plan.selection_pins.get()},
            "configuredWorker": if plan.deployment.is_platform() {
                self.loaded(plan).map(|worker| format!("{:p}", Rc::as_ptr(&worker)))
            } else { None },
        })).collect();
        plans.sort_by(|a, b| a["plan"].as_str().cmp(&b["plan"].as_str()));
        let mut namespaces: Vec<_> = self.namespaces.borrow().iter().map(|(id, entry)| json!({
            "id": id, "className": entry.class_name, "uniqueKey": entry.unique_key, "source": *entry.source.borrow(),
            "residentRoots": entry.namespace.resident_roots(),
        })).collect();
        namespaces.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        json!({"plans": plans, "namespaces": namespaces, "compiledWorkers": self.compiled.borrow().len(),
            "installingNamespaces": self.installing.borrow().values().flat_map(|flight| flight.namespaces.iter().cloned()).collect::<Vec<_>>(),
            "installingPlans": self.installing.borrow().len(), "tenantCompilations": self.tenant_compilations.get(),
            "failedInstalls": self.failed_installs.borrow().len(),
            "closingFences": self.closing_roots.pending(),
            "retiredNamespaceCount": self.removed_namespaces.borrow().len(),
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
        for (key, worker) in &workers {
            if !self
                .plans
                .borrow()
                .get(&key.plan_key)
                .is_some_and(|plan| plan.deployment.is_platform())
            {
                worker.unlink();
            }
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
    fn request_ready(&self) -> bool {
        let Ok(registry) = self.registry() else {
            return false;
        };
        self.generation.get().is_some_and(|generation| {
            registry.compiled.borrow().contains_key(&WorkerKey {
                plan_key: self.plan_key.clone(),
                generation,
            })
        })
    }
    fn for_root_plan(&self, root_plan: &str) -> Result<Option<Rc<dyn ActorClass>>> {
        let registry = self.registry()?;
        let owner = registry
            .plans
            .borrow()
            .get(root_plan)
            .map(|plan| plan.deployment.worker_id.clone())
            .ok_or_else(|| kj::disconnected!("the root class plan was removed"))?;
        if owner != self.worker_id {
            return Err(kj::failed!(
                "jsg.Error: the child facet class belongs to a different Worker"
            ));
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
    fn request_ready(&self) -> bool {
        self.class.request_ready()
    }
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
        // The public object id is O. The native container index and storage spec retain S.
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
        let json = ffi::frankenvalue_plain_json(&props)?;
        let selection: RootSelection = if json.starts_with("{\"plainSelection\":") {
            let value: PlainSelection = serde_json::from_str(&json)
                .map_err(|error| kj::failed!("invalid ordinary selection: {error}"))?;
            if value.plain_selection.kind != SelectionKind::Plain {
                return Err(kj::failed!("an ordinary selection requires kind plain"));
            }
            value.plain_selection
        } else {
            serde_json::from_str(&json).map_err(|error| kj::failed!("invalid root selection: {error}"))?
        };
        if !selection.dispatch_checked {
            return Err(kj::failed!("jsg.Error: node dispatch checks are required"));
        }
        if selection.kind == SelectionKind::Plain {
            return self.plain_channel(selection.clone())
                .map_err(|error| preparation_error(&selection.preparation_nonce, error));
        }
        if self.plain_only {
            return Err(kj::failed!("this process cannot select durable objects"));
        }
        // Let the trusted runtime refresh a cached key before starting an object event.
        if !self.plans.borrow().contains_key(&selection.plan) {
            return Err(preparation_error(&selection.preparation_nonce, kj::failed!("unknown root plan")));
        }
        let pin = PlanPin::new(Rc::clone(self.plans.borrow().get(&selection.plan).unwrap()), self.this.clone());
        if pin.0.deployment.is_plain() {
            return Err(kj::failed!("an ordinary worker plan has no objects"));
        }
        Ok(Rc::new(RootSelectedChannel {
            registry: self
                .this
                .upgrade()
                .ok_or_else(|| kj::disconnected!("root registry gone"))?,
            selection,
            _pin: pin,
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

struct PlainSelectedChannel {
    registry: Rc<RootRegistry>,
    selection: RootSelection,
    entrypoint: Rc<dyn Channel>,
    _pin: PlanPin,
}
impl Channel for PlainSelectedChannel {
    fn start_request(&self, mut metadata: KjOwn<RequestMetadata>) -> Result<KjOwn<WorkerInterface>> {
        self.registry.stamp_metadata(&self.selection, &mut metadata)?;
        self.entrypoint.start_request(metadata)
    }
    fn require_allows_transfer(&self) -> Result<()> { Err(not_transferable("Private ordinary worker")) }
    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> { Err(not_transferable("Private ordinary worker")) }
}

struct RootSelectedChannel {
    registry: Rc<RootRegistry>,
    selection: RootSelection,
    _pin: PlanPin,
}

impl Channel for RootSelectedChannel {
    fn start_request(
        &self,
        mut metadata: KjOwn<RequestMetadata>,
    ) -> Result<KjOwn<WorkerInterface>> {
        let registry = Rc::clone(&self.registry);
        let selection = self.selection.clone();
        let nonce = selection.preparation_nonce.clone();
        registry.stamp_metadata(&selection, &mut metadata)
            .map_err(|error| preparation_error(&nonce, error))?;
        let pin = PlanPin::new(Rc::clone(&self._pin.0), self.registry.this.clone());
        Ok(worker::PromisedInterface::new(async move {
            let _pin = pin;
            let channel = registry.root(selection).await
                .map_err(|error| preparation_error(&nonce, error))?;
            channel.start_request(metadata)
                .map_err(|error| preparation_error(&nonce, error))
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

/// Only private selection and actor setup call this, before a tenant event exists.
/// The nonce belongs to this runtime dispatch and is never included in tenant metadata.
pub(crate) fn preparation_error(nonce: &str, error: crate::Error) -> crate::Error {
    if nonce.is_empty() { return error; }
    let message = error.description();
    let kind = if message.contains("ROOT_TENANT_CODE: ") { "tenant-code" }
        else if message.contains("unknown root plan") || message.contains("root selection plan removed") { "plan-missing" }
        else { "platform" };
    kj::failed!("jsg.Error: COWORKERD_ROOT_PREPARATION:{nonce}:{kind}: {message}")
}

fn preparation_failure(message: &str) -> (u32, Value) {
    let code = "ROOT_TENANT_CODE: ";
    let (status, kind, reason) = if let Some(index) = message.find(code) {
        (500, "tenant-code", &message[index + code.len()..])
    } else if message.contains("unknown root plan") || message.contains("root selection plan removed") {
        (503, "plan-missing", message)
    } else {
        (503, "platform", message)
    };
    (status, json!({"error": {"kind": kind, "message": reason}}))
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
        let mut status = 200;
        let value = match (method, url.path()) {
            (Method::POST, "/lookup") => {
                let deployment: Deployment = serde_json::from_value(read_json(body).await?)
                    .map_err(|error| kj::failed!("root lookup: {error}"))?;
                let key = deployment.plan_key()?;
                if let Some(error) = self.registry.cached_failure(&key) {
                    let (code, value) = preparation_failure(error.description());
                    status = code;
                    value
                } else {
                    let plans = self.registry.plans.borrow();
                    let installed = plans.get(&key);
                    json!({"plan": installed.map(|_| &key), "info": installed.map(|plan| json!({
                        "loaded": plan.loaded.borrow().is_some(), "generation": plan.generation.get()
                    }))})
                }
            }
            (Method::POST, "/install") => {
                let deployment = serde_json::from_value(read_json(body).await?)
                    .map_err(|error| kj::failed!("root deployment: {error}"))?;
                match self.registry.install(deployment).await {
                    Ok(plan) => {
                        let plans = self.registry.plans.borrow();
                        if let Some(installed) = plans.get(&plan) {
                        json!({"plan": plan, "info": {"kind": installed.deployment.kind,
                            "loaded": installed.loaded.borrow().is_some(), "generation": installed.generation.get(),
                            "tenant": installed.deployment.owner_tenant().ok(), "workerId": installed.deployment.worker_id,
                            "configuredWorker": if installed.deployment.is_platform() {
                                self.registry.loaded(installed).map(|worker| format!("{:p}", Rc::as_ptr(&worker)))
                            } else { None }, "platformClass": installed.deployment.platform_class}})
                        } else {
                            status = 503;
                            json!({"error":{"kind":"plan-missing","message":"root plan is no longer registered"}})
                        }
                    },
                    Err(error) => {
                        tracing::error!("root installation: {error:?}");
                        let (code, value) = preparation_failure(error.description());
                        status = code;
                        value
                    }
                }
            }
            (Method::GET, "/state") => self.registry.snapshot(),
            (Method::POST, "/collect") => {
                self.registry.collect_inactive_in_batches().await;
                json!({"collected": true})
            }
            (Method::POST, "/remove-namespace") => {
                let value = read_json(body).await?;
                let id = value["namespaceId"]
                    .as_str()
                    .ok_or_else(|| kj::failed!("remove-namespace needs a namespaceId"))?;
                if value["permanent"].as_bool() == Some(true) {
                    self.registry.removed_namespaces.borrow_mut().insert(id.to_owned());
                }
                let compiling: Vec<_> = self.registry.installing.borrow().values()
                    .filter(|flight| flight.namespaces.iter().any(|namespace| namespace == id))
                    .cloned().collect();
                for flight in &compiling { flight.cancelled.set(true); }
                let entry = self.registry.namespaces.borrow().get(id).cloned();
                let removed = entry.is_some();
                if let Some(entry) = entry {
                    if !entry.removing.replace(true) {
                        let closing = entry.namespace.abort_runtime_roots(
                            kj::disconnected!("root namespace removed"),
                        );
                        let registry = Rc::clone(&self.registry);
                        let old = entry.clone();
                        let id = id.to_owned();
                        self.registry.factory.spawn_detached(async move {
                            closing.await;
                            old.namespace.roots_closed().await;
                            let matches =
                                registry.namespaces.borrow().get(&id).is_some_and(|entry| {
                                    Rc::ptr_eq(&entry.namespace, &old.namespace)
                                });
                            if matches {
                                registry.forget_namespace(&id);
                            }
                            registry.prune_plans();
                        });
                    }
                    if let Err(error) = wait_for_root(
                        &self.registry.factory,
                        id,
                        "",
                        "namespace removal",
                        async {
                            // Wait for the independent task to close every batch and its fences,
                            // including batches that have not started when this request arrives.
                            loop {
                                let present = self.registry.namespaces.borrow().get(id).is_some_and(|current| {
                                    Rc::ptr_eq(&current.namespace, &entry.namespace)
                                });
                                if !present { break; }
                                self.registry.factory.sleep(Duration::from_millis(1)).await;
                            }
                        },
                    )
                    .await {
                        let bytes = serde_json::to_vec(&json!({"error":{"kind":"namespace-closing","message":error.description()}}))
                            .map_err(|error| kj::failed!("root reply: {error}"))?;
                        let headers = Headers::new(header_table(&self.registry.factory));
                        return response.send(503, "Namespace still closing", &headers, Some(bytes.len() as u64))?.write(&bytes).await;
                    }
                    // Let the cancellation-independent removal task publish its completion.
                    self.registry.factory.sleep(Duration::ZERO).await;
                }
                for flight in compiling {
                    if let Err(error) = wait_for_root(&self.registry.factory, id, "", "cancelled namespace compilation", flight.wait()).await {
                        let bytes = serde_json::to_vec(&json!({"error":{"kind":"namespace-closing","message":error.description()}}))
                            .map_err(|error| kj::failed!("root reply: {error}"))?;
                        let headers = Headers::new(header_table(&self.registry.factory));
                        return response.send(503, "Namespace still closing", &headers, Some(bytes.len() as u64))?.write(&bytes).await;
                    }
                }
                self.registry.prune_plans();
                json!({"removed": removed})
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
                let retired = self.registry.retire(plan_key, &key).await?;
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
            .send(status, if status == 200 { "OK" } else { "Root preparation failed" }, &headers, Some(bytes.len() as u64))?
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
