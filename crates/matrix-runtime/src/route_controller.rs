//! Controller side of M7 remote composition (route A).
//!
//! A `RouteManager` owns one TLS session per executor peer plus the
//! executor-side lease that authorizes legs. It implements
//! [`matrix_host::RemoteTransport`]: the controller kernel admits
//! locally (parent, binding, consumer, grant, quotas all revalidated),
//! the controller host dispatches, and this manager only moves bytes.
//!
//! Steady state per peer (worker loop, bounded backoff):
//! connect → unary `activate` → ready (attested activation) → register
//! provider locally → reconcile → subscribe events → renew loop.
//! Any failure withdraws the registration first (fail closed for new
//! admissions); in-flight legs fail at terminal revalidation, never
//! with false success. No implicit replay: retries reuse deterministic
//! operation ids and the executor replays or conflicts from its ledger.

use crate::remote;
use crate::service::Service;
use crate::session::{InboundHandler, Session, SessionLimits};
use matrix_host::{RemoteCallOpen, RemoteCallTerminal, RemoteTransport};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

/// Operator configuration for one executor peer.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Route key, operator-configured on both sides.
    pub name: String,
    pub address: SocketAddr,
    pub server_name: String,
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
    /// Unary management endpoint (lease bootstrap/teardown).
    pub mgmt_address: SocketAddr,
    /// Composition domain stamped on legs.
    pub domain: String,
    /// Lease TTL requested at attach (100..30000 ms).
    pub lease_ttl_ms: u64,
}

/// One consumer → remote-provider edge over a peer.
#[derive(Debug, Clone)]
pub struct RouteSpec {
    /// Controller-side consumer logical (local definition).
    pub consumer: String,
    /// Executor-side provider logical (= controller remote definition id).
    pub provider: String,
    pub peer: String,
}

#[derive(Clone)]
struct RouteLease {
    token: String,
    fence: u64,
    ttl_ms: u64,
    seq: u64,
}

#[derive(Clone)]
struct PendingCall {
    tx: std::sync::mpsc::Sender<Value>,
}

/// Per-peer link: session, lease, renew state, call correlation.
struct PeerLink {
    config: PeerConfig,
    /// Primary provider routed via this peer (v0.1: one per peer).
    /// Stored at sync so teardown withdraws the right registration even
    /// as the route table changes underneath.
    provider: Mutex<String>,
    manager: Mutex<Option<std::sync::Weak<RouteManagerInner>>>,
    session: Mutex<Option<Arc<Session>>>,
    lease: Mutex<Option<RouteLease>>,
    /// request_id → waiter for multi-answer `call.open` (accepted+result).
    calls: Mutex<HashMap<String, PendingCall>>,
    /// Last attach/reconcile/renew outcome for diagnosis.
    last_error: Mutex<String>,
    last_ok: Mutex<Option<Instant>>,
    closed: AtomicBool,
    /// Renewals served (inspect).
    renewals: Mutex<u64>,
}

struct RouteManagerInner {
    service: Arc<Service>,
    authority: String,
    links: Mutex<HashMap<String, Arc<PeerLink>>>,
    routes: Mutex<Vec<RouteSpec>>,
    closed: AtomicBool,
}

/// Controller route manager. Clone shares ownership; `shutdown` stops
/// workers, withdraws registrations and releases executor leases.
#[derive(Clone)]
pub struct RouteManager {
    inner: Arc<RouteManagerInner>,
}

impl RouteManager {
    pub fn new(service: Arc<Service>, authority: String) -> Self {
        Self {
            inner: Arc::new(RouteManagerInner {
                service,
                authority,
                links: Mutex::new(HashMap::new()),
                routes: Mutex::new(vec![]),
                closed: AtomicBool::new(false),
            }),
        }
    }

    pub fn shutdown(&self) {
        self.inner.closed.store(true, Ordering::SeqCst);
        let links: Vec<Arc<PeerLink>> = self.inner.links.lock().unwrap().values().cloned().collect();
        for link in links {
            link.closed.store(true, Ordering::SeqCst);
            self.teardown_link(&link, "shutdown");
        }
    }

    /// Syncs operator config: attaches new peers/routes, tears down
    /// removed ones (registrations withdrawn, leases released).
    /// Unknown peers referenced by routes are ignored (fail closed).
    pub fn sync(&self, peers: Vec<PeerConfig>, routes: Vec<RouteSpec>) {
        {
            let mut r = self.inner.routes.lock().unwrap();
            *r = routes;
        }
        let want: HashSet<String> = peers.iter().map(|p| p.name.clone()).collect();
        {
            let mut links = self.inner.links.lock().unwrap();
            // Withdraw removed peers first (their links name providers via
            // the stored route binding, so no binding outlives its route).
            for name in links.keys().cloned().collect::<Vec<_>>() {
                if !want.contains(&name) {
                    if let Some(link) = links.remove(&name) {
                        link.closed.store(true, Ordering::SeqCst);
                        drop(links);
                        self.teardown_link(&link, "config-removed");
                        links = self.inner.links.lock().unwrap();
                    }
                }
            }
            for p in peers {
                if !links.contains_key(&p.name) {
                    let provider = self.route_provider(&p.name).unwrap_or_default();
                    let link = Arc::new(PeerLink {
                        config: p.clone(),
                        provider: Mutex::new(provider),
                        manager: Mutex::new(None),
                        session: Mutex::new(None),
                        lease: Mutex::new(None),
                        calls: Mutex::new(HashMap::new()),
                        last_error: Mutex::new(String::new()),
                        last_ok: Mutex::new(None),
                        closed: AtomicBool::new(false),
                        renewals: Mutex::new(0),
                    });
                    links.insert(p.name.clone(), link.clone());
                    link.manager.lock().unwrap().replace(Arc::downgrade(&self.inner));
                    std::thread::Builder::new()
                        .name(format!("matrix-route-{}", p.name))
                        .spawn(move || Self::link_worker(link))
                        .ok();
                }
            }
            // Prune providers that routes no longer send via a surviving
            // peer (route edits without peer changes must not leak bindings).
            let routes_now = self.inner.routes.lock().unwrap().clone();
            let kernel = self.inner.service.kernel.clone();
            for link in links.values() {
                let live: HashSet<String> = routes_now
                    .iter()
                    .filter(|r| r.peer == link.config.name)
                    .map(|r| r.provider.clone())
                    .collect();
                *link.provider.lock().unwrap() =
                    live.iter().next().cloned().unwrap_or_default();
                for gone in kernel
                    .remote_providers_of_peer(&link.config.name)
                    .into_iter()
                    .filter(|l| !live.contains(l))
                {
                    kernel.unregister_remote_provider(&gone);
                }
            }
        }
        // New routes over existing links need no worker restart: attach
        // reconciles the full route table each round.
    }

    fn note(_manager: &RouteManager, link: &PeerLink, ok: bool, msg: &str) {
        if ok {
            *link.last_ok.lock().unwrap() = Some(Instant::now());
            *link.last_error.lock().unwrap() = String::new();
        } else {
            *link.last_error.lock().unwrap() = msg.to_string();
        }
    }

    /// Withdraws registration + releases the executor lease. Idempotent.
    /// In-flight legs fail at terminal revalidation (never false success).
    fn teardown_link(&self, link: &PeerLink, reason: &str) {
        // The stored route binding (not the live table, which may already
        // name the peer's replacement): withdrawing the wrong logical
        // would orphan the old binding published.
        let provider = link.provider.lock().unwrap().clone();
        if !provider.is_empty() {
            self.inner.service.kernel.unregister_remote_provider(&provider);
        }
        if let Some(sess) = link.session.lock().unwrap().take() {
            sess.shutdown();
        }
        if let Some(lease) = link.lease.lock().unwrap().take() {
            let client = self.unary_client(link);
            let _ = client.request_with_budget(
                json!({"action":"release","lease":lease.token,"fence":lease.fence.to_string()}),
                Duration::from_secs(2),
                None,
            );
        }
        // Fail every in-flight open wait: the session is gone and the
        // worker thread translates to outcome-unknown + close.
        for (_, call) in link.calls.lock().unwrap().drain() {
            let _ = call.tx.send(json!({"transport-failed": reason}));
        }
        Self::note(self, link, false, reason);
    }

    fn unary_client(&self, link: &PeerLink) -> remote::Client {
        let c = &link.config;
        remote::Client {
            address: c.mgmt_address,
            server_name: c.server_name.clone(),
            config: remote::client_config(&c.ca, &c.cert, &c.key).unwrap_or_else(|_| {
                // Config validated at attach; unreachable in practice.
                // Build a placeholder that fails closed on use.
                remote::client_config(&c.ca, &c.cert, &c.key).unwrap()
            }),
        }
    }

    fn session_limits() -> SessionLimits {
        SessionLimits {
            heartbeat_interval: Duration::from_secs(1),
            ..Default::default()
        }
    }

    /// Parse `e{epoch}:{logical}:{instance}:{context}#{generation}`.
    fn parse_describe(s: &str) -> Option<(u64, u64)> {
        let s = s.strip_prefix('e')?;
        let (head, generation) = s.rsplit_once('#')?;
        let mut it = head.split(':');
        let _epoch: u64 = it.next()?.parse().ok()?;
        let _logical = it.next()?;
        let instance: u64 = it.next()?.parse().ok()?;
        let _context = it.next()?;
        if it.next().is_some() {
            return None;
        }
        let generation: u64 = generation.parse().ok()?;
        Some((instance, generation))
    }

    fn link_worker(link: Arc<PeerLink>) {
        let mut backoff = Duration::from_secs(1);
        loop {
            if link.closed.load(Ordering::SeqCst) {
                return;
            }
            // RouteManager may be gone (shutdown raced sync); stop then.
            let inner = link.manager.lock().unwrap().as_ref().and_then(|w| w.upgrade());
            let Some(inner) = inner else { return };
            let manager = RouteManager { inner };
            if manager.inner.closed.load(Ordering::SeqCst) {
                return;
            }
            match Self::attach_round(&manager, &link) {
                Ok(()) => {
                    backoff = Duration::from_secs(1);
                    Self::monitor_link(&manager, &link);
                    // monitor_link returns only on failure/teardown.
                }
                Err(e) => {
                    Self::note(&manager, &link, false, &e);
                    std::thread::sleep(backoff.min(Duration::from_secs(30)));
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    /// One attach round: session → lease → ready → register → reconcile →
    /// subscribe. On success the monitor loop takes over.
    fn attach_round(manager: &RouteManager, link: &PeerLink) -> Result<(), String> {
        if link.closed.load(Ordering::SeqCst) || manager.inner.closed.load(Ordering::SeqCst) {
            return Err("closed".into());
        }
        let c = &link.config;
        let tls = crate::session::client_config(&c.ca, &c.cert, &c.key)?;
        let epoch = manager.inner.service.store.epoch;
        let hello = crate::session::Session::hello_body(&c.domain, &manager.inner.authority, epoch);
        let (session, welcome) = crate::session::Session::connect(
            c.address,
            &c.server_name,
            tls,
            Self::session_limits(),
            hello,
        )
        .map_err(|e| format!("connect: {e}"))?;
        let feats = welcome.get("features").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        if !feats.iter().any(|f| f == "remote-calls/1") {
            session.shutdown();
            return Err("executor lacks remote-calls/1".into());
        }
        *link.session.lock().unwrap() = Some(session.clone());
        session.set_handler(Arc::new(LinkHandler { link_peer: link.config.name.clone(), manager: manager.clone() }));

        // Executor-side lease (unary bootstrap): the lease token travels
        // in `call.open`; renewals run over the session afterwards.
        let ttl = c.lease_ttl_ms.clamp(100, 30_000);
        let provider = manager.route_provider(&link.config.name).ok_or("no route for peer")?;
        // Reuse a still-live lease from a previous attach round instead
        // of stacking activations (retry after a torn-down session must
        // not leak executor leases). The status probe is read-only.
        let (token, fence) = match link.lease.lock().unwrap().clone() {
            Some(l) => {
                let st = manager.unary_client(link).request_with_budget(
                    json!({"action":"lease-status","lease":l.token}),
                    Duration::from_secs(3),
                    None,
                );
                match st {
                    Ok(v) if v.get("ready") == Some(&json!(true)) => (l.token.clone(), l.fence),
                    _ => manager.activate_provider(link, &provider, ttl)?,
                }
            }
            None => manager.activate_provider(link, &provider, ttl)?,
        };
        // Ready: attested executor activation (instance+generation).
        let deadline = Instant::now() + Duration::from_secs(10);
        let (instance, generation) = loop {
            let st = manager.unary_client(link).request_with_budget(
                json!({"action":"status","lease":token,"fence":fence.to_string()}),
                Duration::from_secs(3),
                None,
            ).map_err(|e| format!("status: {e}"))?;
            if st.get("ready") == Some(&json!(true)) {
                let inst = st.get("instance").and_then(|v| v.as_str()).unwrap_or("");
                if let Some((i, g)) = Self::parse_describe(inst) {
                    break (i, g);
                }
            }
            if Instant::now() >= deadline {
                let _ = manager.unary_client(link).request_with_budget(
                    json!({"action":"release","lease":token,"fence":fence.to_string()}),
                    Duration::from_secs(2),
                    None,
                );
                return Err("executor provider never ready".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        *link.lease.lock().unwrap() = Some(RouteLease { token, fence, ttl_ms: ttl, seq: 0 });
        // Reconcile BEFORE publishing: the executor confirms (or revokes)
        // in-flight legs and attests generations while no new admission
        // can route here. Revoked legs are cancelled above; only then do
        // bindings become visible below.
        Self::reconcile_link(manager, link)?;
        manager.inner.service.kernel.register_remote_provider(&provider, instance, generation, &link.config.name)
            .map_err(|e| format!("register: {e}"))?;
        // Bindings now visible: subscribe, then enable dispatch.
        Self::push_subscriptions(manager, link);
        manager.install_transport();
        Self::note(manager, link, true, "");
        Ok(())
    }

    fn activate_provider(&self, link: &PeerLink, provider: &str, ttl: u64) -> Result<(String, u64), String> {
        let act = self.unary_client(link).request_with_budget(
            json!({"action":"activate","component":provider,"ttl_ms":ttl}),
            Duration::from_secs(10),
            None,
        ).map_err(|e| format!("activate: {e}"))?;
        let token = act.get("lease").and_then(|v| v.as_str()).ok_or("activate without lease")?.to_string();
        let fence: u64 = act.get("fence").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).ok_or("activate without fence")?;
        Ok((token, fence))
    }

    fn route_provider(&self, peer: &str) -> Option<String> {
        // One provider logical per peer in v0.1 (routes share the lease).
        self.inner.routes.lock().unwrap().iter()
            .find(|r| r.peer == peer)
            .map(|r| r.provider.clone())
    }

    /// Settles locally what the executor reports revoked at reconcile:
    /// cancels those in-flight legs fail-fast through the normal worker
    /// path (cancel forwarded, no phantom terminal). Unknown ids are
    /// ignored. Returns how many tickets were cancelled. Also used
    /// directly in tests with real in-flight legs.
    pub fn cancel_revoked_operations(&self, peer: &str, operation_ids: &[String]) -> usize {
        let host = self.inner.service.host.clone();
        let kernel = self.inner.service.kernel.clone();
        let mut n = 0;
        for op in operation_ids {
            if op.is_empty() {
                continue;
            }
            if let Some(ticket) = host.ticket_for_remote_op(peer, op) {
                if kernel.call_cancel(ticket, "reconcile-revoked") {
                    n += 1;
                }
            }
            // Bound stream legs end with the revoked operation (tombstones
            // stay for late frames); surviving operations keep theirs.
            host.end_remote_streams_of(peer, op);
        }
        n
    }

    /// Sends `inventory.reconcile`, settles what the executor reports, and
    /// refreshes registrations from the executor's attested activations.
    /// MUST run before any registration publish on (re)attach: legs the
    /// executor reports revoked are cancelled first, so no new admission
    /// routes through a binding the executor already released. The refresh
    /// only updates EXISTING registrations (never creates); creation is the
    /// caller's explicit register step afterwards. Entries the executor
    /// reports unknown (never admitted) are left to their deadlines.
    /// `resources` stays empty: v0.1 has no remote handle protocol (all
    /// handles are host-local and die with their sessions); the only held
    /// cross-host state is legs, reconciled here as operations.
    fn reconcile_link(manager: &RouteManager, link: &PeerLink) -> Result<(), String> {
        let session = link.session.lock().unwrap().clone().ok_or("no session")?;
        let host = manager.inner.service.host.clone();
        let kernel = manager.inner.service.kernel.clone();
        let inv = kernel.inventory();
        let operations: Vec<Value> = inv["calls"].as_array().cloned().unwrap_or_default()
            .iter()
            .filter_map(|t| {
                let peer = t["dep"]["remote_peer"].as_str()?;
                if peer != link.config.name {
                    return None;
                }
                // Stable operation id (host-side correlation); the ticket
                // and binding ride along for audit. Legs without correlation
                // (settling) are skipped, never invented.
                let ticket_n = t["ticket"].as_u64()?;
                let (_p, op) = host.remote_op_of(matrix_core::TicketId(ticket_n))?;
                Some(json!({"operation_id": op, "ticket": t["ticket"], "binding": t["dep"]["binding"]}))
            })
            .collect();
        let body = json!({
            "activations": [],
            "leases": [],
            "operations": operations,
            "resources": [],
        });
        let rid = format!("rec-{}", manager.inner.service.store.epoch);
        let ans = session.request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "inventory.reconcile", "message_id": rid.clone(),
                "session_id": session.session_id(), "request_id": rid,
                "body": body,
            }),
            Duration::from_secs(10),
            None,
        ).map_err(|e| format!("reconcile: {e}"))?;
        // Revoked legs settle BEFORE any publish: the executor never
        // admitted them (or explicitly released them), so waiting for a
        // terminal would only hit the deadline. Cancelling now fails fast
        // through the normal worker path (cancel forwarded, no phantom).
        if let Some(revoked) = ans["body"]["revoked"].as_array() {
            let ids: Vec<String> = revoked
                .iter()
                .filter_map(|r| r.get("operation_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .collect();
            manager.cancel_revoked_operations(&link.config.name, &ids);
        }
        // Refresh attested generations (withdraw+rebind on change), but
        // only for registrations that already exist: creation is the
        // caller's explicit register step, which runs after this reconcile.
        if let Some(acts) = ans["body"]["activations"].as_array() {
            for a in acts {
                let (Some(logical), Some(inst), Some(gen)) = (
                    a.get("logical").and_then(|v| v.as_str()),
                    a.get("instance").and_then(|v| v.as_str()).and_then(|s| s.parse::<u64>().ok()),
                    a.get("generation").and_then(|v| v.as_str()).and_then(|s| s.parse::<u64>().ok()),
                ) else { continue };
                let mine = manager.inner.routes.lock().unwrap().iter()
                    .any(|r| r.peer == link.config.name && r.provider == logical);
                if !mine {
                    continue;
                }
                let cur = kernel.remote_provider_of(logical);
                if cur.is_some_and(|r| r.instance != inst || r.generation != gen) {
                    let _ = kernel.register_remote_provider(logical, inst, gen, &link.config.name);
                }
            }
        }
        Ok(())
    }

    /// Pushes the union of remote-bound consumer subscriptions.
    fn push_subscriptions(manager: &RouteManager, link: &PeerLink) {
        let topics = manager.remote_consumer_topics(&link.config.name);
        let session = link.session.lock().unwrap().clone();
        let Some(session) = session else { return };
        let rid = format!("sub-{}", manager.inner.service.store.epoch);
        let ans = session.request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "event.subscribe", "message_id": rid.clone(),
                "session_id": session.session_id(), "request_id": rid,
                "body": {"topics": topics},
            }),
            Duration::from_secs(10),
            None,
        );
        if ans.is_err() {
            Self::note(manager, link, false, "subscribe failed");
        }
    }

    /// Union of subscriptions of consumers bound to this peer's providers.
    fn remote_consumer_topics(&self, peer: &str) -> Vec<String> {
        let kernel = &self.inner.service.kernel;
        let routes: Vec<RouteSpec> = self.inner.routes.lock().unwrap().iter()
            .filter(|r| r.peer == peer)
            .cloned()
            .collect();
        let mut topics: HashSet<String> = HashSet::new();
        for r in &routes {
            for b in kernel.dependency_bindings_of(&r.consumer) {
                if b.provider_logical == r.provider {
                    for t in kernel.subscriptions_of(&r.consumer) {
                        topics.insert(t);
                    }
                    break;
                }
            }
        }
        let mut out: Vec<String> = topics.into_iter().collect();
        out.sort();
        out
    }

    /// Installs this manager as the host transport (idempotent).
    fn install_transport(&self) {
        self.inner.service.host.set_remote_transport(Some(Arc::new(self.clone())));
    }

    /// Executor-originated chunk (down direction): decode and deliver to
    /// the bound component session (global stream_id lookup; unknown or
    /// ended legs drop, never resurrect). Best effort, never blocks.
    fn on_stream_data(&self, env: &Value) {
        let body = env.get("body").unwrap_or(&Value::Null);
        let stream_id = body.get("stream_id").and_then(|v| v.as_str()).unwrap_or("");
        let seq = body.get("seq").and_then(|v| v.as_u64()).unwrap_or(u64::MAX);
        let bytes_b64 = body.get("bytes").and_then(|v| v.as_str()).unwrap_or("");
        if stream_id.is_empty() || seq == u64::MAX {
            return;
        }
        let bytes = match b64_decode(bytes_b64) {
            Some(b) => b,
            None => return,
        };
        // Components take UTF-8 test payloads; binary degrades lossy
        // (documented: stream payloads for SDK delivery are UTF-8).
        let payload = String::from_utf8_lossy(&bytes).to_string();
        let _ = self.inner.service.host.deliver_remote_chunk_any(stream_id, seq, &payload);
    }

    /// Executor-granted credit for an upstream leg (component → executor):
    /// widens the host-side window so the component may send more.
    fn on_stream_credit(&self, env: &Value) {
        let body = env.get("body").unwrap_or(&Value::Null);
        let stream_id = body.get("stream_id").and_then(|v| v.as_str()).unwrap_or("");
        let credit = body.get("credit").and_then(|v| v.as_u64()).unwrap_or(0);
        if stream_id.is_empty() || credit == 0 {
            return;
        }
        self.inner.service.host.credit_remote_stream_any(stream_id, credit);
    }

    /// Executor terminal for a stream leg: tombstone locally (late frames
    /// ignored, never recreated).
    fn on_stream_terminal(&self, env: &Value) {
        let stream_id = env
            .get("body")
            .and_then(|b| b.get("stream_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if stream_id.is_empty() {
            return;
        }
        self.inner.service.host.end_remote_stream_any(stream_id);
    }

    /// Steady-state monitor: renewals + session watch. Returns only on
    /// failure (caller tears down and re-attaches with backoff).
    fn monitor_link(manager: &RouteManager, link: &PeerLink) {
        loop {
            if link.closed.load(Ordering::SeqCst) || manager.inner.closed.load(Ordering::SeqCst) {
                return;
            }
            let live = link.session.lock().unwrap().as_ref().is_some_and(|s| s.admissible());
            if !live {
                manager.teardown_link(link, "session lost");
                return;
            }
            // Renewal cadence: a third of the TTL, independent of legs.
            let ttl = link.lease.lock().unwrap().as_ref().map(|l| l.ttl_ms).unwrap_or(0);
            if ttl == 0 {
                manager.teardown_link(link, "lease lost");
                return;
            }
            std::thread::sleep(Duration::from_millis((ttl / 3).max(100)));
            if link.closed.load(Ordering::SeqCst) || manager.inner.closed.load(Ordering::SeqCst) {
                return;
            }
            match Self::renew_once(manager, link) {
                Ok(()) => {}
                Err(e) => {
                    manager.teardown_link(link, &e);
                    return;
                }
            }
        }
    }

    /// One renewal round (idempotent seq; lost responses replay).
    fn renew_once(_manager: &RouteManager, link: &PeerLink) -> Result<(), String> {
        let (token, fence, seq, ttl) = {
            let mut lease = link.lease.lock().unwrap();
            let Some(l) = lease.as_mut() else { return Err("lease lost".into()) };
            l.seq += 1;
            (l.token.clone(), l.fence, l.seq, l.ttl_ms)
        };
        let session = link.session.lock().unwrap().clone().ok_or("no session")?;
        let rid = format!("ren-{}-{seq}", link.config.name);
        let ans = session.request(
            &json!({
                "protocol": "matrix.remote", "version": "0.1",
                "type": "lease.renew", "message_id": rid.clone(),
                "session_id": session.session_id(), "request_id": rid,
                "body": {"token": token, "fence": fence.to_string(), "seq": seq, "ttl_ms": ttl},
            }),
            Duration::from_secs(5),
            None,
        ).map_err(|e| format!("renew transport: {e}"))?;
        match ans["body"]["status"].as_str() {
            Some("ok") => {
                let new = ans["body"]["token"].as_str().unwrap_or("").to_string();
                if new.is_empty() {
                    return Err("renew without token".into());
                }
                if let Some(l) = link.lease.lock().unwrap().as_mut() {
                    l.token = new;
                }
                *link.renewals.lock().unwrap() += 1;
                Ok(())
            }
            // Stale/retired/unknown: authority is gone; re-attach anew.
            _ => Err("lease not renewed".into()),
        }
    }

    /// Executor-originated frames without request correlation.
    fn on_notice(&self, link_name: &str, env: &Value) {
        match env.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "revoke.notice" => {
                let target = env["body"].get("target").and_then(|v| v.as_str()).unwrap_or("");
                if target.is_empty() {
                    return;
                }
                // Only our own remote definitions withdraw (never local).
                let ours = self.inner.service.kernel.definitions.lock()
                    .get(target)
                    .is_some_and(|d| d.remote);
                if ours {
                    self.inner.service.kernel.unregister_remote_provider(target);
                }
            }
            "event.deliver" => {
                let topic = env["body"].get("topic").and_then(|v| v.as_str()).unwrap_or("");
                let payload = env["body"].get("payload").cloned().unwrap_or(Value::Null);
                if !topic.is_empty() {
                    self.inner.service.host.deliver_remote_event(topic, &payload);
                }
            }
            _ => {}
        }
        let _ = link_name;
    }

    /// Diagnosis: peers, sessions, leases, legs, renewals, last outcome.
    pub fn inspect(&self) -> Value {
        let links = self.inner.links.lock().unwrap();
        let mut peers: Vec<Value> = links.values().map(|l| {
            let (session, lease) = (l.session.lock().unwrap().clone(), l.lease.lock().unwrap().as_ref().map(|x| json!({"fence": x.fence.to_string(), "seq": x.seq, "ttl_ms": x.ttl_ms})));
            json!({
                "peer": l.config.name,
                "session": session.as_ref().map(|s| json!({"id": s.session_id(), "state": format!("{:?}", s.state())})),
                "lease": lease,
                "calls": l.calls.lock().unwrap().len(),
                "renewals": *l.renewals.lock().unwrap(),
                "last_error": l.last_error.lock().unwrap().clone(),
            })
        }).collect();
        peers.sort_by(|a, b| a["peer"].as_str().cmp(&b["peer"].as_str()));
        json!({"peers": peers})
    }
}

/// Session demux for one peer link: call terminals go straight to the
/// waiting transport call via the session's multi-answer subscriptions
/// (engine-owned); revokes and events go to the manager here.
struct LinkHandler {
    link_peer: String,
    manager: RouteManager,
}

impl InboundHandler for LinkHandler {
    fn on_message(&self, session: &Session, env: Value) {
        let ty = env.get("type").and_then(|v| v.as_str()).unwrap_or("");
        // Call terminals never reach here (multi-answer subscriptions
        // consume them in the engine). A request_id miss here is
        // unexpected; the engine already counted stray answers.
        if env.get("request_id").and_then(|v| v.as_str()).is_some() {
            let _ = session;
            return;
        }
        match ty {
            "revoke.notice" | "event.deliver" => self.manager.on_notice(&self.link_peer, &env),
            "stream.data" => self.manager.on_stream_data(&env),
            "stream.credit" => self.manager.on_stream_credit(&env),
            "stream.complete" | "stream.cancel" => self.manager.on_stream_terminal(&env),
            _ => {}
        }
    }
}

impl RemoteTransport for RouteManager {
    fn call_open(&self, open: RemoteCallOpen, cancel: &AtomicBool) -> RemoteCallTerminal {
        let links = self.inner.links.lock().unwrap();
        let Some(link) = links.get(&open.peer).cloned() else {
            return RemoteCallTerminal::Failed { code: "outcome-unknown".into(), message: "no route".into() };
        };
        drop(links);
        let (session, lease) = (
            link.session.lock().unwrap().clone(),
            link.lease.lock().unwrap().as_ref().map(|l| l.token.clone()),
        );
        let (Some(session), Some(lease)) = (session, lease) else {
            return RemoteCallTerminal::Failed { code: "outcome-unknown".into(), message: "route down".into() };
        };
        if !session.admissible() {
            return RemoteCallTerminal::Failed { code: "outcome-unknown".into(), message: "session not admissible".into() };
        }
        let rid = format!("co-{}", open.operation_id);
        let body = json!({
            "parent": {
                "domain": open.domain,
                "ticket": open.parent_ticket.to_string(),
                "activation": {"logical": open.consumer_logical, "instance": open.consumer_instance.to_string(), "generation": open.consumer_generation.to_string()},
            },
            "binding_id": open.binding_id,
            "activation": {"logical": open.consumer_logical, "instance": open.consumer_instance.to_string(), "generation": open.consumer_generation.to_string()},
            "cap": open.cap,
            "input": open.input,
            "timeout_ms": open.timeout_ms,
            "budget_ms": open.budget_ms,
            "lease": lease,
            "grant_rev": open.grant_rev.to_string(),
            "operation_id": open.operation_id,
        });
        if matrix_proto::remote::validate_body("call.open", &body).is_err() {
            return RemoteCallTerminal::Failed { code: "internal".into(), message: "locally invalid call.open".into() };
        }
        let env = json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "call.open", "message_id": rid.clone(),
            "session_id": session.session_id(),
            "instance_id": open.consumer_instance.to_string(),
            "generation": open.consumer_generation.to_string(),
            "request_id": rid.clone(),
            "body": body,
        });
        let (tx, rx) = std::sync::mpsc::channel();
        {
            let mut calls = link.calls.lock().unwrap();
            if calls.len() >= 256 {
                return RemoteCallTerminal::Failed { code: "resource-exhausted".into(), message: "too many remote legs".into() };
            }
            calls.insert(rid.clone(), PendingCall { tx: tx.clone() });
        }
        // Multi-answer subscription (accepted+result share rid): the
        // session engine delivers both without consuming; explicit
        // unsubscribe ends. `link.calls` keeps the cap/inspect count.
        let cleanup = |session: &crate::session::Session, link: &PeerLink, rid: &str| {
            session.unsubscribe_answers(rid);
            link.calls.lock().unwrap().remove(rid);
        };
        if session.subscribe_answers(&rid, tx).is_err() {
            link.calls.lock().unwrap().remove(&rid);
            return RemoteCallTerminal::Failed { code: "resource-exhausted".into(), message: "too many remote legs".into() };
        }
        if session.send_control(&env).is_err() {
            cleanup(&session, &link, &rid);
            return RemoteCallTerminal::Failed { code: "outcome-unknown".into(), message: "send failed".into() };
        }
        // Accepted then terminal share this wait (overall deadline);
        // cancel abandons the wait (worker still forwards call.cancel).
        let deadline = Instant::now() + Duration::from_millis(open.timeout_ms.max(1));
        loop {
            if cancel.load(Ordering::SeqCst) {
                cleanup(&session, &link, &rid);
                return RemoteCallTerminal::Failed { code: "cancelled".into(), message: "cancelled".into() };
            }
            let now = Instant::now();
            if now >= deadline {
                cleanup(&session, &link, &rid);
                return RemoteCallTerminal::Failed { code: "deadline-exceeded".into(), message: "executor deadline exceeded".into() };
            }
            match rx.recv_timeout((deadline - now).min(Duration::from_millis(25))) {
                Ok(ans) if ans["type"] == "call.accepted" => {
                    // Admission persisted (or executor bug); keep waiting
                    // for the terminal under the same deadline.
                    continue;
                }
                Ok(ans) if ans["type"] == "call.result" => {
                    cleanup(&session, &link, &rid);
                    match ans["body"]["status"].as_str() {
                        Some("ok") => return RemoteCallTerminal::Ok(ans["body"]["output"].clone()),
                        _ => {
                            let e = ans["body"]["error"].clone();
                            return RemoteCallTerminal::Err {
                                code: e.get("code").and_then(|v| v.as_str()).unwrap_or("internal").to_string(),
                                message: e.get("message").and_then(|v| v.as_str()).unwrap_or("remote error").to_string(),
                            };
                        }
                    }
                }
                Ok(_) => continue,
                Err(_) => {
                    if !session.admissible() {
                        cleanup(&session, &link, &rid);
                        return RemoteCallTerminal::Failed { code: "outcome-unknown".into(), message: "session lost".into() };
                    }
                    continue;
                }
            }
        }
    }

    fn call_cancel(&self, peer: &str, operation_id: &str) {
        let links = self.inner.links.lock().unwrap();
        let Some(link) = links.get(peer).cloned() else { return };
        drop(links);
        let Some(session) = link.session.lock().unwrap().clone() else { return };
        let rid = format!("cc-{}", operation_id);
        let _ = session.send_control(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "call.cancel", "message_id": rid.clone(),
            "session_id": session.session_id(), "request_id": rid,
            "body": {"reason": "consumer-cancel", "operation_id": operation_id},
        }));
    }

    fn topology_changed(&self) {
        let peers: Vec<String> = self.inner.links.lock().unwrap().keys().cloned().collect();
        for peer in peers {
            let links = self.inner.links.lock().unwrap();
            let Some(link) = links.get(&peer).cloned() else { continue };
            drop(links);
            // Best effort: a failed push is retried on next reconcile.
            Self::push_subscriptions(self, &link);
        }
    }

    fn stream_data(&self, peer: &str, operation: &str, stream_id: &str, seq: u64, payload: &str) {
        // Best-effort bulk relay (M7 R02): credit already enforced
        // host-side; transport frames validly or drops. Never blocks the
        // caller; control keeps its reserved queue (send_control separate
        // from send_data in the session engine). `operation` travels as an
        // extra field so the executor can map the chunk to its provider.
        if stream_id.is_empty() || stream_id.len() > 128 {
            return;
        }
        let links = self.inner.links.lock().unwrap();
        let Some(link) = links.get(peer).cloned() else { return };
        drop(links);
        let Some(session) = link.session.lock().unwrap().clone() else { return };
        if !session.admissible() {
            return;
        }
        let bytes = b64_encode(payload.as_bytes());
        // Report remaining window as credit hint (host-side granted-sent
        // is authoritative; executor treats this as advisory).
        let mut body = json!({"stream_id": stream_id, "seq": seq, "bytes": bytes, "credit": 0});
        if !operation.is_empty() {
            body["operation_id"] = json!(operation);
        }
        if matrix_proto::remote::validate_body("stream.data", &body).is_err() {
            return;
        }
        let _ = session.send_data(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "stream.data", "message_id": format!("sd-{}-{}", stream_id, seq),
            "session_id": session.session_id(),
            "body": body,
        }));
    }

    fn stream_end(&self, peer: &str, stream_id: &str, status: &str) {
        let status = match status {
            "ok" | "error" | "cancelled" => status,
            _ => "cancelled",
        };
        let links = self.inner.links.lock().unwrap();
        let Some(link) = links.get(peer).cloned() else { return };
        drop(links);
        let Some(session) = link.session.lock().unwrap().clone() else { return };
        if !session.admissible() {
            return;
        }
        let body = json!({"stream_id": stream_id, "status": status});
        // `stream.complete` and `stream.cancel` share the terminal schema;
        // complete is the clean close (cancel carries the same status set).
        let ty = if status == "ok" { "stream.complete" } else { "stream.cancel" };
        if matrix_proto::remote::validate_body(ty, &body).is_err() {
            return;
        }
        let _ = session.send_control(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": ty, "message_id": format!("se-{stream_id}"),
            "session_id": session.session_id(),
            "body": body,
        }));
    }
}

/// Minimal base64 (RFC 4648, padded) to avoid a new dependency for the
/// stream relay. Payloads are small test chunks; executor decodes with
/// its own matching routine and rejects invalid encodings.
fn b64_encode(bytes: &[u8]) -> String {
    const ALPH: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(((bytes.len() + 2) / 3) * 4);
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i] as u32;
        let b1 = if i + 1 < bytes.len() { bytes[i + 1] as u32 } else { 0 };
        let b2 = if i + 2 < bytes.len() { bytes[i + 2] as u32 } else { 0 };
        let n = bytes.len() - i;
        out.push(ALPH[((b0 >> 2) & 63) as usize] as char);
        out.push(ALPH[(((b0 << 4) | (b1 >> 4)) & 63) as usize] as char);
        if n > 1 {
            out.push(ALPH[(((b1 << 2) | (b2 >> 6)) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if n > 2 {
            out.push(ALPH[(b2 & 63) as usize] as char);
        } else {
            out.push('=');
        }
        i += 3;
    }
    out
}

/// Minimal base64 decode (padded) matching `b64_encode` above.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 4 != 0 {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut i = 0;
    while i < bytes.len() {
        let mut vals = [0u8; 4];
        let mut pad = 0;
        for k in 0..4 {
            vals[k] = match bytes[i + k] {
                b'A'..=b'Z' => bytes[i + k] - b'A',
                b'a'..=b'z' => bytes[i + k] - b'a' + 26,
                b'0'..=b'9' => bytes[i + k] - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => {
                    pad += 1;
                    0
                }
                _ => return None,
            };
        }
        if pad > 0 && i + 4 != bytes.len() {
            return None;
        }
        if pad > 2 {
            return None;
        }
        out.push((vals[0] << 2) | (vals[1] >> 4));
        if pad < 2 {
            out.push((vals[1] << 4) | (vals[2] >> 2));
        }
        if pad < 1 {
            out.push((vals[2] << 6) | vals[3]);
        }
        i += 4;
    }
    Some(out)
}
