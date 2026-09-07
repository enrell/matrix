//! Executor side of M7 remote composition (route B).
//!
//! A `Route` binds one accepted `matrix.remote/0.1` session to the local
//! [`Service`]: it serves `call.open` against locally-executing
//! capabilities, answers `op.query` from the durable ledger, serves
//! `lease.renew` on the control channel, and emits `revoke.notice` when
//! local authority disappears. It never invents authority: every frame
//! is revalidated against the local kernel, grants, leases and fences.
//!
//! The controller side lives in `route_controller.rs`: admit locally,
//! forward `call.open`, translate the terminal back through
//! `dependency_accept`.

use crate::service::Service;
use crate::session::{InboundHandler, Session};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn sid_of(env: &Value) -> String {
    env.get("session_id").and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn rid_of(env: &Value) -> String {
    env.get("request_id").and_then(|v| v.as_str()).unwrap_or("").to_string()
}

/// Local execution of one admitted remote leg: presence in the map is
/// the replay key (the ledger holds the rest). Legs are never
/// re-executed, only replayed from persisted terminals.
struct Leg;

/// One executor-side route: session + local service + legs.
pub struct Route {
    me: Mutex<Option<std::sync::Weak<Route>>>,
    service: Arc<Service>,
    session: Arc<Session>,
    /// controller operation_id → leg (bounded; settled legs are removed).
    legs: Mutex<HashMap<String, Leg>>,
    /// request_id → controller operation_id for in-flight `call.open`.
    opening: Mutex<HashMap<String, String>>,
    /// operation_id → cancelled before/at dispatch (cooperative cancel).
    cancelled: Mutex<HashMap<String, ()>>,
    /// topic → subscriber count, pushed by the controller (M7 events).
    subs: Mutex<HashMap<String, u64>>,
    /// (provider logical, topic) → last delivered seq.
    seqs: Mutex<HashMap<(String, String), u64>>,
    /// Delivered / dropped counters (inspect; loss is observable).
    delivered: Mutex<u64>,
    dropped: Mutex<u64>,
    max_legs: usize,
    /// Admitted operation → explicit ownership for stream termination
    /// (M7 R02 temporal validity): the provider logical PLUS the full
    /// activation reference and fence attested at admission. Injecting a
    /// chunk revalidates both (current owner still equals; lease still
    /// live), so replacing the provider never redirects stale chunks to
    /// the new activation. Survives the call terminal (streams outlive
    /// calls); bounded by oldest-first eviction.
    op_provider: Mutex<HashMap<String, OpProvider>>,
    /// Insertion order for `op_provider` eviction (oldest first).
    op_order: Mutex<std::collections::VecDeque<String>>,
    /// Remote stream accounting (M7 R02): stream_id → state. Bounded
    /// (128 legs); over-credit/over-cap ends the leg with an error,
    /// session survives. Credit is receiver-granted in bytes.
    streams: Mutex<HashMap<String, ExecStream>>,
    /// Stream bytes received/dropped (inspect; no payloads).
    stream_received: Mutex<u64>,
    stream_dropped: Mutex<u64>,
}

/// Explicit stream-termination ownership for one admitted operation:
/// which provider activation (and fence) may still receive its chunks.
/// Revalidated on every inject; replacing the provider (or losing the
/// lease) stops delivery without touching accounting.
#[derive(Debug, Clone)]
struct OpProvider {
    logical: String,
    owner: matrix_core::InstanceRef,
    fence: u64,
}

/// Executor-side stream leg: sequential seq, byte-grant window, terminal
/// tombstone (late/duplicate/reorder ignored, never re-executed).
#[derive(Debug, Clone)]
struct ExecStream {
    next_seq: u64,
    received: u64,
    granted: u64,
    ended: bool,
    /// Operation this leg was first seen under (empty when unknown):
    /// lets reconcile tombstone exactly the orphaned legs.
    operation: String,
}

impl Route {
    pub fn new(service: Arc<Service>, session: Arc<Session>) -> Arc<Self> {
        let route = Arc::new(Self {
            me: Mutex::new(None),
            service,
            session,
            legs: Mutex::new(HashMap::new()),
            opening: Mutex::new(HashMap::new()),
            cancelled: Mutex::new(HashMap::new()),
            subs: Mutex::new(HashMap::new()),
            seqs: Mutex::new(HashMap::new()),
            delivered: Mutex::new(0),
            dropped: Mutex::new(0),
            max_legs: 256,
            op_provider: Mutex::new(HashMap::new()),
            op_order: Mutex::new(std::collections::VecDeque::new()),
            streams: Mutex::new(HashMap::new()),
            stream_received: Mutex::new(0),
            stream_dropped: Mutex::new(0),
        });
        *route.me.lock().unwrap() = Some(Arc::downgrade(&route));
        route
    }

    fn weak_self(&self) -> Option<Arc<Self>> {
        self.me.lock().unwrap().as_ref().and_then(|w| w.upgrade())
    }

    /// Installs the route as the session's inbound handler.
    pub fn attach(self: &Arc<Self>) {
        self.session.set_handler(self.clone());
    }

    fn answer(&self, ty: &str, rid: &str, body: Value) {
        if rid.is_empty() {
            return;
        }
        let _ = self.session.send_control(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": ty, "message_id": rid,
            "session_id": self.session.session_id(),
            "request_id": rid, "body": body,
        }));
    }

    fn on_call_open(self: Arc<Self>, env: Value) {
        let rid = rid_of(&env);
        let principal = self.session.peer_principal().to_string();
        let body = env.get("body").cloned().unwrap_or(Value::Null);
        let operation = body.get("operation_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if operation.is_empty() || operation.len() > 128 {
            self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": false}));
            self.terminal(&rid, &operation, "error", None, Some(("invalid-message", "bad operation_id")));
            return;
        }
        // Idempotent re-delivery: a persisted terminal is replayed, never
        // re-executed (no replay of unknown effects).
        if self.legs.lock().unwrap().contains_key(&operation) {
            // Settled legs are replayed from the ledger, never re-executed.
            match self.service.store.operation(&principal, &operation) {
                Ok(done) if done["state"] == "completed" => {
                    let result = done["result"].clone();
                    self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": true}));
                    self.terminal(&rid, &operation, "ok-replay", Some(result), None);
                    return;
                }
                _ => {
                    self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": false}));
                    self.terminal(&rid, &operation, "error", None, Some(("outcome-unknown", "leg in flight; query later")));
                    return;
                }
            }
        }
        if self.legs.lock().unwrap().len() >= self.max_legs {
            self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": false}));
            self.terminal(&rid, &operation, "error", None, Some(("resource-exhausted", "too many remote legs")));
            return;
        }
        let lease = body.get("lease").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let cap = body.get("cap").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let input = body.get("input").cloned().unwrap_or(Value::Null);
        let timeout_ms = body.get("timeout_ms").and_then(|v| v.as_u64()).unwrap_or(0);
        // The lease token binds principal + logical + fence server-side
        // (tokens are unique); the wire carries no separate fence.
        let (logical, owner, fence) = match self.service.lease_snapshot_by_token(&principal, &lease) {
            Some(t) => t,
            None => {
                self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": false}));
                self.terminal(&rid, &operation, "error", None, Some(("permission-denied", "lease refused")));
                return;
            }
        };
        if !self.service.grant_covers(&principal, &cap) {
            self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": false}));
            self.terminal(&rid, &operation, "error", None, Some(("permission-denied", "capability not granted")));
            return;
        }
        if !self
            .service
            .kernel
            .definitions
            .lock()
            .get(&logical)
            .is_some_and(|d| d.caps.iter().any(|c| c == &cap))
        {
            self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": false}));
            self.terminal(&rid, &operation, "error", None, Some(("permission-denied", "capability not provided")));
            return;
        }
        // Admit against the durable ledger first: duplicates (retry after
        // a lost ack) replay instead of re-executing.
        let request = json!({"logical": logical, "cap": cap, "input": input});
        match self.service.store.admit(&principal, &operation, &request) {
            Ok(crate::store::Admission::Completed(done)) => {
                self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": true}));
                // Replay the persisted business terminal (never re-execute).
                // The ledger holds the Service wrapper; the wire carries
                // only the business output/error like a fresh leg.
                if done.get("ok") == Some(&json!(true)) {
                    let out = done.get("value").cloned().unwrap_or(Value::Null);
                    self.terminal(&rid, &operation, "ok-replay", Some(out), None);
                } else {
                    let v = done.get("value").cloned().unwrap_or(Value::Null);
                    let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("remote-error").to_string();
                    let msg = v.to_string();
                    self.terminal(&rid, &operation, "error", None, Some((&code, &msg)));
                }
                return;
            }
            Ok(crate::store::Admission::Unknown) => {
                self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": true}));
                self.terminal(&rid, &operation, "error", None, Some(("outcome-unknown", "operation unknown; query later")));
                return;
            }
            Ok(crate::store::Admission::New) => {}
            Err(e) => {
                let code = if e == "operation-id-conflict" { "operation-id-conflict" } else { "resource-exhausted" };
                self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": false}));
                self.terminal(&rid, &operation, "error", None, Some((code, "admission refused")));
                return;
            }
        }
        self.answer("call.accepted", &rid, json!({"status": "admitted", "persisted": false}));
        self.opening.lock().unwrap().insert(rid.clone(), operation.clone());
        // Provider mapping for stream termination (survives the terminal:
        // streams outlive calls; bounded by oldest-first eviction). Stores
        // the full attested reference so injects revalidate ownership.
        {
            let mut map = self.op_provider.lock().unwrap();
            let mut order = self.op_order.lock().unwrap();
            if !map.contains_key(&operation) {
                order.push_back(operation.clone());
            }
            map.insert(operation.clone(), OpProvider { logical: logical.clone(), owner: owner.clone(), fence });
            while map.len() > self.max_legs {
                if let Some(old) = order.pop_front() {
                    map.remove(&old);
                } else {
                    break;
                }
            }
        }
        if let Some(this) = self.weak_self() {
            std::thread::Builder::new()
                .name("matrix-remote-leg".into())
                .spawn(move || this.run_leg(rid, operation, principal, logical, owner, fence, cap, input, timeout_ms))
                .ok();
        }
    }

    fn run_leg(
        &self,
        rid: String,
        operation: String,
        principal: String,
        logical: String,
        owner: matrix_core::InstanceRef,
        fence: u64,
        cap: String,
        input: Value,
        timeout_ms: u64,
    ) {
        let budget = Duration::from_millis(timeout_ms.max(1).min(35_000));
        let start = Instant::now();
        // Executor-side authority was validated at open; revalidate
        // currency here (withdraw/revoke between open and dispatch fails
        // closed, like the local path's post-registration revalidation).
        if self.service.kernel.instance_ref_of(&logical).as_ref() != Some(&owner) {
            self.opening.lock().unwrap().remove(&rid);
            let _ = self.service.store.finish(&principal, &operation, &json!({"ok": false, "value": {"code": "stale-generation"}}), false);
            self.terminal(&rid, &operation, "error", None, Some(("stale-generation", "activation superseded")));
            return;
        }
        // Cooperative cancel point: a cancel that arrived while opening
        // settles without executing (ledger stays admitted → unknown, so
        // a later query reports unknown, never a phantom result).
        if self.cancelled(&operation) {
            self.opening.lock().unwrap().remove(&rid);
            let _ = self.service.store.finish(&principal, &operation, &json!({"ok": false, "value": {"code": "cancelled"}}), true);
            self.terminal(&rid, &operation, "error", None, Some(("cancelled", "cancelled before dispatch")));
            return;
        }
        let remaining = budget.saturating_sub(start.elapsed());
        let (value, ok) = self.service.invoke_with_budget(&owner, &cap, &input, remaining);
        // Cooperative cancel wins over a finished execution that has not
        // persisted yet (no phantom result after an acknowledged cancel).
        let was_cancelled = self.cancelled(&operation);
        // Liveness recheck before persisting: revocation between execution
        // and finish downgrades to unknown (never a false durable ack).
        let current = self.service.lease_live(&principal, &logical, fence, &owner);
        let code = value.get("code").and_then(|v| v.as_str()).unwrap_or("");
        let unknown = !current || matches!(code, "outcome-unknown" | "deadline-exhausted" | "cancelled");
        let result = if !current {
            json!({"ok": false, "value": {"code": "outcome-unknown"}, "durability": "durable"})
        } else if was_cancelled {
            json!({"ok": false, "value": {"code": "cancelled"}, "durability": "durable"})
        } else {
            json!({"ok": ok, "value": value, "durability": "durable"})
        };
        let _ = self.service.store.finish(&principal, &operation, &result, unknown || was_cancelled);
        self.opening.lock().unwrap().remove(&rid);
        self.cancelled.lock().unwrap().remove(&operation);
        if was_cancelled {
            self.terminal(&rid, &operation, "error", None, Some(("cancelled", "cancelled before persist")));
            return;
        }
        if unknown {
            self.terminal(&rid, &operation, "error", None, Some(("outcome-unknown", "authority lost before persist")));
            return;
        }
        self.legs.lock().unwrap().insert(operation.clone(), Leg);
        if ok {
            // Wire carries the business output only (the durability
            // wrapper stays in the ledger); local and remote legs share
            // the same terminal shape.
            self.terminal(&rid, &operation, "ok", Some(value), None);
        } else {
            let code = result["value"]["code"].as_str().unwrap_or("remote-error").to_string();
            let msg = result["value"].to_string();
            self.terminal(&rid, &operation, "error", None, Some((&code, &msg)));
        }
    }

    fn terminal(&self, rid: &str, operation: &str, kind: &str, ok_result: Option<Value>, err: Option<(&str, &str)>) {
        // The provider mapping is intentionally kept past the terminal:
        // stream legs outlive calls, and late chunks must still terminate.
        // It is bounded by oldest-first eviction at insert.
        let _ = operation;
        let body = match kind {
            "ok" | "ok-replay" => json!({"status": "ok", "output": ok_result.unwrap_or(Value::Null), "terminal": true}),
            _ => {
                let (code, message) = err.unwrap_or(("internal", "remote leg failed"));
                json!({"status": "error", "error": {"code": code, "message": message}, "terminal": true})
            }
        };
        self.answer("call.result", rid, body);
    }

    fn on_call_cancel(&self, env: &Value) {
        // Cooperative cancel, indexed by stable operation: marks legs
        // that have not finished persisting. Running execution is not
        // interrupted mid-kernel-call; its terminal becomes `cancelled`
        // (or `outcome-unknown` if authority is gone), never a false ok.
        // Late cancels (after persist) change nothing.
        if let Some(op) = env["body"].get("operation_id").and_then(|v| v.as_str()) {
            if !op.is_empty() {
                self.cancelled.lock().unwrap().insert(op.to_string(), ());
            }
        }
    }

    fn cancelled(&self, operation: &str) -> bool {
        self.cancelled.lock().unwrap().contains_key(operation)
    }

    fn on_op_query(&self, env: &Value) {
        let rid = rid_of(env);
        let principal = self.session.peer_principal().to_string();
        let operation = env["body"].get("operation_id").and_then(|v| v.as_str()).unwrap_or("");
        // Queries never execute: ledger read only, scoped to the peer.
        // Absent-after-retention is reported as unknown, never as proof
        // the operation never ran.
        match self.service.store.operation(&principal, operation) {
            Ok(done) => self.answer(
                "op.result",
                &rid,
                json!({"state": done["state"], "result": done["result"]}),
            ),
            Err(_) => self.answer("op.result", &rid, json!({"state": "unknown"})),
        }
        let _ = sid_of(env);
    }

    fn on_lease_renew(&self, env: &Value) {
        // Renewal is executor-local authority with client sequences
        // (idempotent retry on lost responses; delayed replays harmless).
        let rid = rid_of(env);
        let principal = self.session.peer_principal().to_string();
        let token = env["body"].get("token").and_then(|v| v.as_str()).unwrap_or("");
        let fence: u64 = env["body"].get("fence").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(u64::MAX);
        let ttl_ms = env["body"].get("ttl_ms").and_then(|v| v.as_u64()).unwrap_or(0);
        let seq = env["body"].get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
        match self.service.renew_seq(&principal, token, fence, seq, ttl_ms) {
            Ok(v) => {
                self.answer("lease.renewed", &rid, json!({"status": "ok", "token": v["lease"], "seq": seq}));
            }
            Err(e) if e == "stale-generation" => {
                self.answer("lease.renewed", &rid, json!({"status": "stale", "token": token, "seq": seq}));
            }
            Err(_) => {
                self.answer("lease.renewed", &rid, json!({"status": "retired", "token": token, "seq": seq}));
            }
        }
    }

    fn on_inventory(&self, env: &Value) {
        // Reconcile-before-publish: report locally revoked/unknown entries
        // so the controller settles them before routing. Entries are
        // matched by stable operation id against the principal-scoped
        // ledger: admitted or completed ids are kept (no entry); ids the
        // ledger never saw are revoked (the controller cancels those legs
        // fail-fast). Entries without an operation id are skipped, never
        // revoked (legacy tolerance). `resources`/`leases` carry no remote
        // handles in v0.1 (all handles are host-local); the answer keeps
        // `unknown` empty and reports activations for registration refresh.
        let rid = rid_of(env);
        let mut revoked = vec![];
        let unknown: Vec<Value> = vec![];
        for op in env["body"].get("operations").and_then(|v| v.as_array()).cloned().unwrap_or_default() {
            let id = op.get("operation_id").and_then(|v| v.as_str()).unwrap_or("");
            if id.is_empty() {
                continue;
            }
            let principal = self.session.peer_principal();
            if self.service.store.operation(principal, id).is_err() {
                revoked.push(json!({"operation_id": id}));
            }
        }
        // Tombstone exactly the stream legs tagged with revoked operations:
        // their chunks must never terminate again (no resurrection), while
        // accounting stays intact. Surviving (kept) operations keep theirs.
        if !revoked.is_empty() {
            let ids: std::collections::HashSet<&str> = revoked
                .iter()
                .filter_map(|r| r.get("operation_id").and_then(|v| v.as_str()))
                .collect();
            for st in self.streams.lock().unwrap().values_mut() {
                if ids.contains(st.operation.as_str()) {
                    st.ended = true;
                }
            }
        }
        // Attested local activations for the controller's registration
        // refresh (bounded; Active providers only).
        let mut activations: Vec<Value> = self
            .service
            .kernel
            .inventory()["instances"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter(|c| c["state"] == "Active")
            .map(|c| {
                json!({
                    "logical": c["logical"],
                    "instance": c["instance"].to_string(),
                    "generation": c["generation"].to_string(),
                })
            })
            .collect();
        activations.truncate(256);
        activations.sort_by(|a, b| a["logical"].as_str().cmp(&b["logical"].as_str()));
        self.answer("inventory.result", &rid, json!({"revoked": revoked, "unknown": unknown, "activations": activations}));
    }

    /// Replaces the event subscription set from the controller (union is
    /// computed controller-side; here it is authoritative per session).
    /// Bounded (128); unknown/oversize frames never reach this validated
    /// handler. Returns the effective set for the ack.
    fn on_subscribe(&self, env: &Value) -> Value {
        let topics: Vec<String> = env["body"]
            .get("topics")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|t| t.as_str().map(|s| s.to_string()))
            .take(128)
            .collect();
        let mut subs = self.subs.lock().unwrap();
        subs.clear();
        for t in &topics {
            subs.insert(t.clone(), 1);
        }
        json!({"topics": topics})
    }

    /// Executor-side emission tap: forwards locally emitted events that
    /// match this route's subscriptions. Best effort with per-route loss
    /// counting; send quotas bound the wire (drops never stall).
    pub fn on_local_emit(&self, topic: &str, payload: &Value) {
        if !self.subs.lock().unwrap().contains_key(topic) {
            return;
        }
        // Provider attribution: first Active local logical is audit
        // context only (authority was the lease+grant at open).
        let seq = {
            let mut seqs = self.seqs.lock().unwrap();
            let k = ("*".to_string(), topic.to_string());
            let n = seqs.get(&k).copied().unwrap_or(0) + 1;
            seqs.insert(k, n);
            n
        };
        let inst = self
            .service
            .kernel
            .contexts
            .current_all()
            .values()
            .next()
            .map(|c| (c.instance.0.to_string(), c.generation.to_string()))
            .unwrap_or_else(|| ("0".to_string(), "0".to_string()));
        let ok = self.session.send_control(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "event.deliver", "message_id": format!("ev-{seq}"),
            "session_id": self.session.session_id(),
            "instance_id": inst.0, "generation": inst.1,
            "body": {"topic": topic, "payload": payload, "seq": seq},
        })).is_ok();
        if ok {
            *self.delivered.lock().unwrap() += 1;
        } else {
            *self.dropped.lock().unwrap() += 1;
        }
    }

    /// Executor-side stream intake (M7 R02): sequential, credit-granted,
    /// bounded. First chunk creates the leg (64 KiB grant); duplicates,
    /// reorders and late frames after terminal are ignored (never
    /// re-executed, never counted twice). Over-grant ends the leg with
    /// an error; the session survives. Accepted bytes are counted only
    /// (payloads are not retained — no unbounded accumulation while the
    /// consumer is slow). Credit top-ups are best-effort control frames.
    /// Accepted chunks carrying an `operation_id` additionally terminate
    /// into the provider session owning that leg (up-direction delivery);
    /// chunks without one (or with an unknown/settled operation) account
    /// without injecting.
    fn on_stream_data(&self, env: &Value) {
        const INITIAL_GRANT: u64 = 65536;
        const MAX_STREAM_BYTES: u64 = 1 << 20;
        const TOPUP: u64 = 32768;
        let body = env.get("body").unwrap_or(&Value::Null);
        let stream_id = body.get("stream_id").and_then(|v| v.as_str()).unwrap_or("");
        let seq = body.get("seq").and_then(|v| v.as_u64()).unwrap_or(u64::MAX);
        let bytes_b64 = body.get("bytes").and_then(|v| v.as_str()).unwrap_or("");
        let operation = body.get("operation_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if stream_id.is_empty() || stream_id.len() > 128 || seq == u64::MAX {
            return;
        }
        let decoded = match b64_decode(bytes_b64) {
            Some(b) => b,
            None => return,
        };
        let decoded_len = decoded.len() as u64;
        enum Out {
            Accept { grant_topup: u64 },
            Over,
            Ignore,
        }
        let out = {
            let mut streams = self.streams.lock().unwrap();
            // Bound the table before inserting (fail closed on flood).
            if !streams.contains_key(stream_id) && streams.len() >= 128 {
                return;
            }
            let st = streams.entry(stream_id.to_string()).or_insert_with(|| ExecStream {
                next_seq: 0,
                received: 0,
                granted: INITIAL_GRANT,
                ended: false,
                operation: String::new(),
            });
            // Tag the leg with its operation on first sight (reconcile can
            // then tombstone exactly orphaned legs; empty stays empty).
            if st.operation.is_empty() && !operation.is_empty() {
                st.operation = operation.clone();
            }
            if st.ended {
                Out::Ignore
            } else if st.next_seq == 0 && st.received == 0 {
                // First chunk sets the base: component→host delivery is
                // reliable, but a host-side bind race may sink the prefix
                // locally before the relay binds (disjoint namespaces).
                // Contiguity is enforced from the base on.
                st.next_seq = seq + 1;
                st.received += decoded_len;
                let mut topup = 0;
                if st.received + TOPUP > st.granted && st.granted < MAX_STREAM_BYTES {
                    topup = TOPUP.min(MAX_STREAM_BYTES - st.granted);
                    st.granted += topup;
                }
                Out::Accept { grant_topup: topup }
            } else if seq != st.next_seq {
                Out::Ignore
            } else if st.received.saturating_add(decoded_len) > st.granted
                || st.received.saturating_add(decoded_len) > MAX_STREAM_BYTES
            {
                st.ended = true;
                Out::Over
            } else {
                st.next_seq += 1;
                st.received += decoded_len;
                // Top up the window as it drains (bounded by the cap).
                let mut topup = 0;
                if st.received + TOPUP > st.granted && st.granted < MAX_STREAM_BYTES {
                    topup = TOPUP.min(MAX_STREAM_BYTES - st.granted);
                    st.granted += topup;
                }
                Out::Accept { grant_topup: topup }
            }
        };
        match out {
            Out::Ignore => {
                *self.stream_dropped.lock().unwrap() += 1;
            }
            Out::Over => {
                *self.stream_dropped.lock().unwrap() += 1;
                let _ = self.session.send_control(&json!({
                    "protocol": "matrix.remote", "version": "0.1",
                    "type": "stream.cancel", "message_id": format!("sx-{stream_id}"),
                    "session_id": self.session.session_id(),
                    "body": {"stream_id": stream_id, "status": "error"},
                }));
            }
            Out::Accept { grant_topup } => {
                *self.stream_received.lock().unwrap() += decoded_len;
                if grant_topup > 0 {
                    let _ = self.session.send_control(&json!({
                        "protocol": "matrix.remote", "version": "0.1",
                        "type": "stream.credit", "message_id": format!("sc-{stream_id}-{}", seq),
                        "session_id": self.session.session_id(),
                        "body": {"stream_id": stream_id, "credit": grant_topup},
                    }));
                }
                // Up-direction termination: hand the chunk to the provider
                // session owning this operation — but only while the exact
                // attested activation still owns it AND its lease is live.
                // A replaced provider (new instance/generation) or a dead
                // lease stops delivery here; accounting above already
                // bounded the leg, so this is purely a validity gate.
                if !operation.is_empty() {
                    let mapping = self.op_provider.lock().unwrap().get(&operation).cloned();
                    if let Some(m) = mapping {
                        let principal = self.session.peer_principal().to_string();
                        let current = self.service.kernel.instance_ref_of(&m.logical);
                        if current.as_ref() == Some(&m.owner)
                            && self.service.lease_live(&principal, &m.logical, m.fence, &m.owner)
                        {
                            // The validated reference travels to session
                            // selection with no re-resolution by bare name
                            // (see `inject_provider_chunk_owned`): whatever
                            // replaces the provider past this point cannot
                            // steal the chunk.
                            let payload = String::from_utf8_lossy(&decoded).to_string();
                            let _ = self.service.host.inject_provider_chunk_owned(&m.owner, stream_id, seq, &payload);
                        }
                    }
                }
            }
        }
    }

    fn on_stream_terminal(&self, env: &Value) {
        let stream_id = env
            .get("body")
            .and_then(|b| b.get("stream_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if stream_id.is_empty() {
            return;
        }
        if let Some(st) = self.streams.lock().unwrap().get_mut(stream_id) {
            st.ended = true;
        }
    }

    /// Executor-originated chunk (provider → consumer direction, M7 R02
    /// bidirectional). Best-effort bulk frame; credit is enforced by the
    /// controller host on delivery. Never blocks; drops on a dead session.
    pub fn send_stream_chunk(&self, stream_id: &str, seq: u64, payload: &[u8]) {
        if stream_id.is_empty() || stream_id.len() > 128 {
            return;
        }
        let body = json!({"stream_id": stream_id, "seq": seq, "bytes": b64_encode(payload), "credit": 0});
        if matrix_proto::remote::validate_body("stream.data", &body).is_err() {
            return;
        }
        let _ = self.session.send_data(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "stream.data", "message_id": format!("xd-{stream_id}-{seq}"),
            "session_id": self.session.session_id(),
            "body": body,
        }));
    }

    /// Route diagnosis (no credentials, no payloads).
    pub fn inspect(&self) -> Value {
        json!({
            "legs": self.legs.lock().unwrap().len(),
            "opening": self.opening.lock().unwrap().len(),
            "subs": self.subs.lock().unwrap().keys().cloned().collect::<Vec<_>>(),
            "delivered": *self.delivered.lock().unwrap(),
            "dropped": *self.dropped.lock().unwrap(),
            "streams": self.streams.lock().unwrap().len(),
            "stream_received": *self.stream_received.lock().unwrap(),
            "stream_dropped": *self.stream_dropped.lock().unwrap(),
        })
    }

    pub(crate) fn session_state(&self) -> crate::session::SessionState {
        self.session.state()
    }
    /// Providers this route serves with their current fences: every
    /// distinct logical behind settled/opening legs that still has a
    /// live lease for this peer. The watcher diffs it for revokes.
    pub(crate) fn served_providers(&self) -> Vec<(String, u64)> {
        let principal = self.session.peer_principal().to_string();
        let ops: Vec<String> = self
            .legs
            .lock()
            .unwrap()
            .keys()
            .chain(self.opening.lock().unwrap().values())
            .cloned()
            .collect();
        // Legs are keyed by controller operation id, not logical; ask the
        // service which (logical, fence) pairs are live for this peer and
        // report those backing current legs. Simpler honest rule: report
        // every live (logical, fence) for the peer — the watcher only
        // fires on live→dead transitions.
        let _ = ops;
        self.service.live_provider_fences(&principal)
    }
}

impl InboundHandler for Route {
    fn on_message(&self, session: &Session, env: Value) {
        let _ = session;
        // Only the route's own session is served (single-session binding).
        match env.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "call.open" => {
                // Bound worker count via legs cap (admission inside run_leg).
                if let Some(this) = self.weak_self() {
                    std::thread::Builder::new()
                        .name("matrix-remote-open".into())
                        .spawn(move || this.on_call_open(env))
                        .ok();
                }
            }
            "call.cancel" => self.on_call_cancel(&env),
            "op.query" => self.on_op_query(&env),
            "lease.renew" => self.on_lease_renew(&env),
            "inventory.reconcile" => self.on_inventory(&env),
            "event.subscribe" => {
                let rid = rid_of(&env);
                let body = self.on_subscribe(&env);
                self.answer("event.subscribed", &rid, body);
            }
            // Heartbeats and session.close are handled by the session
            // engine, not the call route.
            "stream.data" => self.on_stream_data(&env),
            "stream.complete" | "stream.cancel" => self.on_stream_terminal(&env),
            _ => {}
        }
    }
}

/// Minimal base64 encode (RFC 4648, padded) for stream payloads.
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

/// Full base64 decode (padded) matching `b64_encode` above.
/// Returns `None` on invalid encoding (caller drops).
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
        // Padding only valid at the tail.
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

impl Route {
    /// Executor-side revocation push (route manager calls on local
    /// withdraw/revoke): best effort, never blocks.
    pub fn notify_revoked(&self, target: &str, fence: u64) {
        let _ = self.session.send_control(&json!({
            "protocol": "matrix.remote", "version": "0.1",
            "type": "revoke.notice", "message_id": format!("rev-{}", fence),
            "session_id": self.session.session_id(),
            "body": {"target": target, "fence": fence.to_string()},
        }));
    }
}
