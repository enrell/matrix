//! Local process host (M2.2).
//!
//! Runs external plugins: receives [`LifecycleEvent`]s from the kernel (via
//! `LifecycleHook`, without blocking the kernel), spawns child processes,
//! keeps sessions over Unix socket speaking `matrix.component`
//! v0.1, and delivers calls ([`CallForwarder`]).
//!
//! Rules (cf. `docs/PROTOCOL.md`, future `docs/REMOTE.md`):
//! - Session bound to (logical, instance, generation): results from an old
//!   generation never feed the new one; unknown `request_id`s are dropped.
//! - Mid-call disconnect = `outcome-unknown`: no false success,
//!   no automatic retry.
//! - External-code cancellation is best effort (in-flight signal +
//!   wait abandonment); authority revocation holds at boundaries.
//! - A process missing the dispose deadline is killed (SIGKILL) and reaped.
//!
//! Local-profile deadlines (documented, finite): handshake 5s, register
//! 10s, activate 5s, quiesce/dispose 2s each, spawn→register 10s.

use matrix_core::{
    expand_args, CallForwarder, DepAdmit, ForwardOutcome, ForwardRequest, Kernel, LifecycleEvent,
    LifecycleHook, TicketId,
};
use matrix_core::{EventSink, ResourceHandle, ResourceKind};
use matrix_proto::{
    encode, hello_offer, negotiate, parse_frame_payload, read_frame, split_frame, write_frame_deadline, Envelope,
    IdWindow, IdVerdict, DEFAULT_MAX_FRAME,
};
use matrix_proto::{validate_dependency_body, DEPENDENCY_CALLS_1, DUPLICATE_REQUEST, UNSUPPORTED_FEATURE};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::Child;
use matrix_guard::Sandbox;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use std::time::{Duration, Instant};

pub const HELLO_MS: u64 = 5000;
pub const REGISTER_MS: u64 = 10000;
pub const ACTIVATE_MS: u64 = 5000;
pub const QUIESCE_MS: u64 = 2000;
pub const DISPOSE_MS: u64 = 2000;
pub const SPAWN_REGISTER_MS: u64 = 10000;
const CALL_POLL_MS: u64 = 5;
/// Simultaneous forwarded calls per session (I09, M2.3). Also:
/// `call.error resource-exhausted`, never queueing without bound.
pub const MAX_CALLS_PER_SESSION: usize = 16;

/// Per-frame write deadline on the session socket (M6 closing): without
/// it, a never-reading consumer blocks the writer forever. Reads
/// and writes have independent OS deadlines; this one never touches reads.
const WRITE_TIMEOUT_MS: u64 = 5000;

/// Default outstanding-bytes quota per session when the component does not
/// declara `outbound.limits.max_queued_bytes` (M6 fechamento).
const DEFAULT_SEND_QUOTA: usize = 1024 * 1024;
/// Respostas aguardadas no host inteiro (I09, M2.3).
pub const MAX_PENDING_PER_HOST: usize = 1024;
/// Initial byte window per stream (M2.3). Data beyond the grant
/// ends the stream with an error; the session survives.
pub const STREAM_INITIAL_GRANT: u64 = 65536;

#[derive(Debug)]
pub enum HostError {
    Io(String),
    Proto(String),
    Gone(String),
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostError::Io(e) => write!(f, "io: {}", e),
            HostError::Proto(e) => write!(f, "proto: {}", e),
            HostError::Gone(e) => write!(f, "gone: {}", e),
        }
    }
}

impl std::error::Error for HostError {}

struct Pending {
    session: String,
    tx: mpsc::Sender<Envelope>,
    kind: PendingKind,
}

#[derive(Debug, Clone)]
enum PendingKind {
    /// Forwarded call (plugin ticket, for on-wire cancel).
    Call { ticket: String },
    Lifecycle,
}

/// Parses a `resource.acquire` body into a kernel kind (M6.3).
/// Test-only `fail-*` kinds are refused on the wire.
fn parse_resource_kind(body: &Value) -> Result<ResourceKind, String> {
    let kind = body.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    let label = body.get("label").and_then(|v| v.as_str()).unwrap_or("").to_string();
    if label.is_empty() || label.len() > 256 {
        return Err("label must be 1..=256 chars".to_string());
    }
    match kind {
        "cap" => Ok(ResourceKind::Cap { name: label }),
        "sub" => Ok(ResourceKind::Sub { topic: label }),
        "timer" => {
            let ms = body.get("interval_ms").and_then(|v| v.as_u64()).unwrap_or(0);
            Ok(ResourceKind::Timer { label, interval_ms: ms })
        }
        "task" => Ok(ResourceKind::Task { label }),
        _ => Err(format!("unknown resource kind {:?}", kind)),
    }
}

/// Remote-leg transport for M7 child calls (route A: controller admits
/// locally, executes remotely). Implemented by the route manager; the
/// host owns admission, quotas, terminal translation and lifecycle.
#[derive(Debug, Clone)]
pub struct RemoteCallOpen {
    /// Executor route key (operator-configured both sides).
    pub peer: String,
    /// Controller-side consumer activation (exact).
    pub consumer_logical: String,
    pub consumer_instance: u64,
    pub consumer_generation: u64,
    /// Controller parent ticket (decimal) and composition domain.
    pub parent_ticket: u64,
    pub domain: String,
    /// Opaque callee binding (`rb-N`) issued for this activation.
    pub binding_id: String,
    pub cap: String,
    pub input: Value,
    /// Milliseconds remaining on the admitted child deadline.
    pub timeout_ms: u64,
    pub budget_ms: u64,
    /// Executor-side lease token authorizing execution.
    pub lease: String,
    /// Captured operator-grant revision (controller side).
    pub grant_rev: u64,
    /// Stable operation id (dedup/query scope: principal/controller).
    pub operation_id: String,
}

/// Terminal of a remote leg, mirroring `ForwardOutcome` vocabulary.
#[derive(Debug, Clone)]
pub enum RemoteCallTerminal {
    Ok(Value),
    Err { code: String, message: String },
    Failed { code: String, message: String },
}

pub trait RemoteTransport: Send + Sync {
    /// Blocking open of one admitted remote leg. Observes `cancel`
    /// cooperatively; late cancel after the terminal is a no-op for the
    /// caller (executor revocation still goes through `call_cancel`).
    /// Never retries internally: at most one `call.open` per call.
    fn call_open(&self, open: RemoteCallOpen, cancel: &AtomicBool) -> RemoteCallTerminal;
    /// Best-effort cancel by stable operation id (executor drops the
    /// terminal; running execution settles as cancelled/unknown).
    fn call_cancel(&self, peer: &str, operation_id: &str);
    /// Topology signal: re-sync event subscriptions after local consumer
    /// activate/withdraw/remove. Best effort, never blocks.
    fn topology_changed(&self);
    /// Relay one component stream chunk over the route (credit already
    /// enforced host-side; transport frames it best-effort, never blocks).
    /// `operation` is the stable leg the chunk belongs to (audit + the
    /// executor-side provider mapping); it travels as an extra
    /// `operation_id` field the schema ignores on old peers.
    fn stream_data(&self, peer: &str, operation: &str, stream_id: &str, seq: u64, payload: &str);
    /// Close the remote side of a stream leg (terminal or abuse).
    /// `status` is `ok` (clean close), `error` (over-credit/abuse) or
    /// `cancelled`; validated before send, best-effort like data.
    fn stream_end(&self, peer: &str, stream_id: &str, status: &str);
}

/// Opening accepted at the gate, awaiting admission and dispatch.
struct DepJob {
    consumer_sid: String,
    consumer_logical: String,
    cinstance: u64,
    cgeneration: u64,
    request_id: String,
    binding: String,
    parent: TicketId,
    timeout_ms: u64,
    input: Value,
}

/// parent_ticket do fio: decimal puro (`17`, forma da spec) ou eco do
/// delivered ticket (`tkt-17`). Anything else is invalid.
fn parse_parent_ticket(s: &str) -> Option<TicketId> {
    let s = s.strip_prefix("tkt-").unwrap_or(s);
    if s.is_empty() || s.len() > 20 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u64>().ok().map(TicketId)
}

struct Session {
    id: String,
    logical: String,
    instance: u64,
    generation: u64,
    /// Serialized session writer. `Arc` so sends never hold the global
    /// sessions lock (M6 closing: blocking writes under the global lock
    /// freeze control and other sessions).
    writer: Arc<Mutex<UnixStream>>,
    dedup: Mutex<IdWindow>,
    /// Chamadas encaminhadas aguardando resposta (teto M2.3).
    in_flight: Mutex<usize>,
    /// Bytes outstanding in data exchanges (quota; control never counts).
    send_used: Mutex<usize>,
    /// Outstanding-bytes cap (`max_queued_bytes` or default).
    send_quota: usize,
    /// Plugin streams: granted credit vs received bytes (M2.3).
    streams: Mutex<HashMap<String, StreamState>>,
    /// Remote-bound stream legs (M7): disjoint namespace from `streams`.
    /// Shared (not map-borrowed) so table ops never hold the global lock.
    rstreams: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, RemoteStream>>>,
    /// Extensions negotiated in this handshake (M6.1 step 1).
    features: Vec<String>,
    /// Seen `dependency.open` requests (no eviction; policy cap).
    dep_seen: Mutex<HashSet<String>>,
    /// Resource handles acquired via this session (released on drop).
    ext_handles: Mutex<Vec<ResourceHandle>>,
    /// Event-delivery topics (manifest snapshot at bind).
    topics: Vec<String>,
    /// Dispatched children: open request → ticket (for late cancel
    /// idempotent; dies with the session).
    dep_requests: Mutex<HashMap<String, TicketId>>,
    /// Early stream chunks parked while their remote leg dispatches
    /// (M7 R02): component→host delivery is reliable, but admission may
    /// lag the first chunks. Bounded + expiring; drained in order once
    /// the leg maps (or on later chunk arrival). Local accounting still
    /// sees every chunk (disjoint namespace), so unbound ids keep exact
    /// local semantics.
    parked: Mutex<Vec<ParkedChunk>>,
    /// Session open×cancel sync: requests whose worker has not yet
    /// mapping + cancels that arrived before mapping.
    dep_sync: Mutex<DepSync>,
}

/// Open×cancel sync within a session (M6.1 step 3).
#[derive(Default)]
struct DepSync {
    /// Gate-accepted opens whose worker has not mapped the child yet.
    pending: HashSet<String>,
    /// Cancels that arrived before mapping (worker honors and aborts).
    wanted: HashSet<String>,
}

#[derive(Debug, Clone)]
struct StreamState {
    next_seq: u64,
    granted: u64,
    received: u64,
    /// Ended for excess (tombstone): late data ignored, never recreated.
    ended: bool,
}

/// Remote-bound stream leg (M7): component frames relay over the route
/// instead of the local stream table. Stream ids are per-direction legs
/// (profile-conformant): one id carries a single direction, with its own
/// sequence and credit window each way, so bidirectional use binds two
/// ids (or reuses one id whose send and receive sequence spaces are
/// tracked independently below).
#[derive(Debug, Clone)]
struct RemoteStream {
    /// Executor route key.
    peer: String,
    /// Stable operation the stream belongs to (audit correlation and the
    /// executor-side provider mapping; empty for legs learned from the
    /// executor side, which need no local mapping to deliver).
    operation: String,
    /// Next outbound seq expected from the component.
    send_next: u64,
    /// Bytes the executor granted us to send (credit window).
    send_granted: u64,
    /// Bytes sent under the grant.
    send_sent: u64,
    /// Next inbound seq expected from the executor.
    recv_next: u64,
    /// Bytes the component may still receive under this leg's window.
    recv_granted: u64,
    /// Bytes received from the executor on this leg.
    recv_received: u64,
    /// Terminal: late frames ignored, never recreated.
    ended: bool,
}

/// Cap for remote-bound legs per session (noise from tapped provider
/// streams never grows the table without bound).
const MAX_REMOTE_STREAMS_PER_SESSION: usize = 128;

/// Early stream chunk awaiting leg mapping (see `Session::parked`).
#[derive(Debug, Clone)]
struct ParkedChunk {
    stream_id: String,
    seq: u64,
    payload: String,
    at: Instant,
}

/// Cap and retention for parked chunks (per session; dispatch skew only).
const MAX_PARKED_PER_SESSION: usize = 32;
const PARKED_TTL: Duration = Duration::from_secs(2);

/// Stream ids the host may associate with a remote leg on its own
/// (call-associated streams). Anything else stays local unless bound
/// explicitly (`bind_remote_stream`): local telemetry never crosses
/// hosts by accident (fail closed default).
fn is_call_stream_id(stream_id: &str) -> bool {
    stream_id.starts_with("remote/")
}

/// Guards the session in-flight decrement (no-leak cap).
struct InFlightGuard {
    host: Host,
    sid: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Some(s) = self.host.inner.sessions.lock().unwrap().get(&self.sid) {
            let mut n = s.in_flight.lock().unwrap();
            *n = n.saturating_sub(1);
        }
    }
}

/// Holds an outstanding-data charge until the exchange ends (M6 closing).
/// Drop releases even on revocation, timeout, or session loss: quotas
/// always return to baseline, observably in tests.
struct SendBudget {
    host: Host,
    sid: String,
    bytes: usize,
}

impl Drop for SendBudget {
    fn drop(&mut self) {
        self.host.release_send(&self.sid, self.bytes);
    }
}

struct ChildEntry {
    child: Mutex<Child>,
}

#[derive(Clone, Default)]
pub struct HostPolicy {
    /// Missing entries are denied in secure mode. None = explicitly trusted process.
    pub components: HashMap<String, Option<Sandbox>>,
    pub secure: bool,
    /// M6.1 step 1: advertise `dependency-calls/1` in welcome. Production
    /// stays `false` until the circuit completes (steps 2/3): the spec
    /// requires announcing only when implemented and enabled. Tests
    /// exercise negotiation with explicit `true`.
    pub enable_dependency_calls: bool,
    /// Composition domain stamped on remote legs (M7 `call.open.parent`).
    /// Empty = single-host operator (still deterministic per kernel).
    pub domain: String,
}

struct Inner {
    policy: HostPolicy,
    launch_tokens: Mutex<HashMap<String, (u64, String)>>,
    kernel: Arc<Kernel>,
    sock_path: PathBuf,
    sessions: Mutex<HashMap<String, Session>>,
    by_instance: Mutex<HashMap<(String, u64), String>>,
    pending_activation: Mutex<HashMap<String, (u64, u64)>>,
    children: Mutex<HashMap<String, ChildEntry>>,
    pending: Mutex<HashMap<String, Pending>>,
    register_ids: Mutex<IdWindow>,
    next_id: Mutex<u64>,
    shutdown: AtomicBool,
    connecting: std::sync::atomic::AtomicUsize,
    /// M7 remote-leg transport (route manager). Absent = remote legs fail
    /// closed at dispatch (never silently local).
    remote_transport: Mutex<Option<Arc<dyn RemoteTransport>>>,
    /// Executor-side event tap (M7 route): second consumer of local
    /// emissions, forwarding to subscribed remote sessions.
    event_tap: Mutex<Option<Arc<dyn Fn(&str, &Value) + Send + Sync>>>,
    /// Executor-side stream tap (M7 route): observes component stream
    /// chunks after local accounting, forwarding them over the session
    /// to the controller. Set only by `serve_remote_session`; the
    /// controller side never sets it (its relay owns those frames).
    stream_tap: Mutex<Option<Arc<dyn Fn(&str, &str, u64, &str) + Send + Sync>>>,
    /// Stable operation ids for in-flight remote legs (ticket → peer +
    /// operation), for cancel forwarding and diagnosis.
    remote_ops: Mutex<HashMap<TicketId, (String, String)>>,
}

#[derive(Clone)]
pub struct Host {
    inner: Arc<Inner>,
    ev_tx: mpsc::Sender<LifecycleEvent>,
}

impl CallForwarder for Host {
    fn forward(&self, req: &ForwardRequest) -> ForwardOutcome {
        self.forward_call(req)
    }
}

impl LifecycleHook for Host {
    fn on_lifecycle(&self, ev: LifecycleEvent) {
        // Non-blocking by contract (unbounded channel, low volume).
        let _ = self.ev_tx.send(ev);
    }
}

impl EventSink for Host {
    /// Event fan-out to external subscribers (M6.3): best effort per
    /// session, never blocking the emitter (the write deadline bounds each send).
    fn on_event(&self, topic: &str, payload: &Value) {
        self.deliver_local_event(topic, payload);
        // Executor route tap (M7): a second, bounded consumer of the same
        // emission. Never blocks the emitter or local sessions.
        if let Some(tap) = self.inner.event_tap.lock().unwrap().clone() {
            tap(topic, payload);
        }
    }
}

impl Host {
    /// Anexa o host ao kernel: registra forwarder + hook, abre o listener,
    /// processes the backlog (already-active external instances) and releases the threads.
    pub fn attach(kernel: Arc<Kernel>, sock_dir: &Path) -> io::Result<Arc<Host>> {
        Self::attach_with_policy(kernel, sock_dir, HostPolicy::default())
    }

    pub fn attach_with_policy(kernel: Arc<Kernel>, sock_dir: &Path, policy: HostPolicy) -> io::Result<Arc<Host>> {
        std::fs::create_dir_all(sock_dir)?;
        let sock_path = sock_dir.join("host.sock");
        let _ = std::fs::remove_file(&sock_path);
        let listener = UnixListener::bind(&sock_path)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o600))?;
        let (ev_tx, ev_rx) = mpsc::channel();
        let host = Arc::new(Host {
            inner: Arc::new(Inner {
                policy,
                launch_tokens: Mutex::new(HashMap::new()),
                kernel: kernel.clone(),
                sock_path: sock_path.clone(),
                sessions: Mutex::new(HashMap::new()),
                by_instance: Mutex::new(HashMap::new()),
                pending_activation: Mutex::new(HashMap::new()),
                children: Mutex::new(HashMap::new()),
                pending: Mutex::new(HashMap::new()),
                register_ids: Mutex::new(IdWindow::new(512)),
                next_id: Mutex::new(1),
                shutdown: AtomicBool::new(false),
                connecting: std::sync::atomic::AtomicUsize::new(0),
                remote_transport: Mutex::new(None),
                event_tap: Mutex::new(None),
                stream_tap: Mutex::new(None),
                remote_ops: Mutex::new(HashMap::new()),
            }),
            ev_tx,
        });
        kernel.set_forwarder(host.clone() as Arc<dyn CallForwarder>);
        kernel.set_hook(host.clone() as Arc<dyn LifecycleHook>);
        kernel.set_event_sink(Some(host.clone() as Arc<dyn EventSink>));

        // Backlog: already-active external definitions gain a process.
        let backlog: Vec<(String, u64, u64, String, Vec<String>, u64)> = {
            let defs = kernel.definitions.lock().clone();
            let states = kernel.contexts.current_all();
            defs.values()
                .filter(|d| matches!(d.execution, matrix_core::ExecutionKind::External { .. }))
                .filter_map(|d| {
                    let st = states.get(&d.id)?;
                    if st.state.canonical() != matrix_core::Fsm::Active {
                        return None;
                    }
                    match &d.execution {
                        matrix_core::ExecutionKind::External { entrypoint, args, timeout_ms } => {
                            Some((d.id.clone(), st.instance.0, st.generation, entrypoint.clone(), args.clone(), *timeout_ms))
                        }
                        _ => None,
                    }
                })
                .collect()
        };
        for (logical, inst, gen, ep, args, tmo) in backlog {
            host.spawn_child(&logical, inst, gen, &ep, &args, tmo);
        }

        // Loop de eventos do kernel.
        let h1 = host.clone();
        std::thread::Builder::new()
            .name("matrix-host-events".into())
            .spawn(move || h1.event_loop(ev_rx))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        // Loop de accept.
        let h2 = host.clone();
        std::thread::Builder::new()
            .name("matrix-host-accept".into())
            .spawn(move || h2.accept_loop(listener))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(host)
    }

    pub fn sock_path(&self) -> PathBuf {
        self.inner.sock_path.clone()
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        let ids: Vec<_> = self.inner.children.lock().unwrap().keys().cloned().collect();
        for id in ids { self.kill_child(&id); }
        // Acorda o accept e os leitores.
        let _ = UnixStream::connect(self.inner.sock_path.clone());
        let writers: Vec<_> = self
            .inner
            .sessions
            .lock()
            .unwrap()
            .values()
            .map(|s| s.writer.clone())
            .collect();
        for w in writers {
            if let Ok(dup) = w.lock().unwrap().try_clone() {
                let _ = dup.shutdown(std::net::Shutdown::Both);
            }
        }
    }

    fn fresh_id(&self, prefix: &str) -> String {
        let mut n = self.inner.next_id.lock().unwrap();
        let id = format!("{}-{}", prefix, *n);
        *n += 1;
        id
    }

    // ---- lado kernel: eventos ----

    fn event_loop(&self, rx: mpsc::Receiver<LifecycleEvent>) {
        while !self.inner.shutdown.load(Ordering::SeqCst) {
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(ev) => self.handle_event(ev),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.reap_finished();
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn handle_event(&self, ev: LifecycleEvent) {
        match ev {
            LifecycleEvent::Activated { logical, instance, generation, entrypoint, args, timeout_ms } => {
                self.kill_child(&logical);
                self.spawn_child(&logical, instance, generation, &entrypoint, &args, timeout_ms);
            }
            LifecycleEvent::Withdrawn { logical, instance, generation } => {
                self.quiesce_session(&logical, instance, generation);
            }
            LifecycleEvent::Removed { logical } => {
                // Withdraws the current session/instance (if any) and forgets the child.
                let cur = self.inner.kernel.contexts.current(&logical);
                if let Some(c) = cur {
                    self.quiesce_session(&logical, c.instance.0, c.generation);
                }
                self.inner.pending_activation.lock().unwrap().remove(&logical);
                self.kill_child(&logical);
            }
        }
        // M7: consumer topology may have changed (activate/withdraw/
        // remove); the route manager re-syncs event subscriptions.
        // Best effort, never blocks lifecycle handling.
        if let Some(t) = self.inner.remote_transport.lock().unwrap().clone() {
            t.topology_changed();
        }
        self.reap_finished();
    }

    fn spawn_child(
        &self,
        logical: &str,
        instance: u64,
        generation: u64,
        entrypoint: &str,
        args: &[String],
        _timeout_ms: u64,
    ) {
        // Serialize spawn publication with kill_child. A queued Activated event
        // from an already-retired generation must never resurrect a process.
        let mut children = self.inner.children.lock().unwrap();
        if !self.inner.kernel.contexts.current(logical).is_some_and(|c|
            c.instance.0 == instance && c.generation == generation && c.state.canonical() == matrix_core::Fsm::Active) { return; }
        let sock = self.inner.sock_path.to_string_lossy().to_string();
        let argv = expand_args(args, &sock, logical);
        if self.inner.policy.secure && !self.inner.policy.components.contains_key(logical) { return; }
        let sandbox = self.inner.policy.components.get(logical).and_then(|s| s.as_ref());
        let token = match matrix_guard::random_token() { Ok(t) => t, Err(_) => return };
        self.inner.launch_tokens.lock().unwrap().insert(logical.to_string(), (instance, token.clone()));
        self.inner.pending_activation.lock().unwrap().insert(logical.to_string(), (instance, generation));
        let child = matrix_guard::spawn(entrypoint, &argv, &token, &self.inner.sock_path, sandbox);
        match child {
            Ok(c) => {
                children.insert(
                    logical.to_string(),
                    ChildEntry { child: Mutex::new(c) },
                );
                self.inner.pending_activation.lock().unwrap()
                    .insert(logical.to_string(), (instance, generation));
            }
            Err(_) => {
                self.inner.launch_tokens.lock().unwrap().remove(logical);
                self.inner.pending_activation.lock().unwrap().remove(logical);
                // Spawn failure: no session; calls fail Gone (no false success).
            }
        }
    }

    pub fn kill_child(&self, logical: &str) -> bool {
        let mut children = self.inner.children.lock().unwrap();
        self.inner.launch_tokens.lock().unwrap().remove(logical);
        if let Some(entry) = children.remove(logical) {
            let cleaned = entry.child.lock().map(|mut c| matrix_guard::kill_group(&mut c).is_ok()).unwrap_or(false);
            if !cleaned {children.insert(logical.to_string(),entry);return false;}
            self.inner.pending_activation.lock().unwrap().remove(logical);
            true
        } else {
            false
        }
    }

    /// Waits for the process to exit (for assertions; reaps zombies).
    pub fn wait_no_child(&self, logical: &str, timeout: Duration) -> bool {
        let t0 = Instant::now();
        while t0.elapsed() < timeout {
            let gone = !self.inner.children.lock().unwrap().contains_key(logical);
            if gone {
                return true;
            }
            self.reap_finished();
            std::thread::sleep(Duration::from_millis(5));
        }
        !self.inner.children.lock().unwrap().contains_key(logical)
    }

    fn reap_finished(&self) {
        // Reaps children that already exited and have no live session.
        let mut gone: Vec<String> = vec![];
        {
            let children = self.inner.children.lock().unwrap();
            for (logical, entry) in children.iter() {
                let exited = entry
                    .child
                    .lock()
                    .map(|mut c| matches!(c.try_wait(), Ok(Some(_))))
                    .unwrap_or(true);
                if exited {
                    let sess_live = self
                        .inner
                        .by_instance
                        .lock()
                        .unwrap()
                        .keys()
                        .any(|(l, _)| l == logical);
                    if !sess_live {
                        gone.push(logical.clone());
                    }
                }
            }
        }
        for logical in gone {
            if let Some(entry) = self.inner.children.lock().unwrap().remove(&logical) {
                if let Ok(mut c) = entry.child.lock() {
                    let _ = c.wait();
                }
            }
        }
    }

    fn quiesce_session(&self, logical: &str, instance: u64, generation: u64) {
        let sid = self.inner.by_instance.lock().unwrap().remove(&(logical.to_string(), instance));
        if let Some(sid) = sid {
            // Cancela no fio ANTES dos transacts: ordem no socket garante que
            // the plugin observes cancel before quiesce/dispose (M2.3).
            self.cancel_session_calls(&sid, "withdraw");
            // quiesce → dispose com prazos, independente de resposta.
            let op = self.fresh_id("op");
            let _ = self.transact(
                &sid, logical, instance, generation,
                "lifecycle.quiesce",
                json!({"operation_id": op.clone(), "deadline_ms": QUIESCE_MS}),
                QUIESCE_MS,
            );
            let op2 = self.fresh_id("op");
            let _ = self.transact(
                &sid, logical, instance, generation,
                "lifecycle.dispose",
                json!({"operation_id": op2.clone(), "deadline_ms": DISPOSE_MS}),
                DISPOSE_MS,
            );
            self.drop_session(&sid);
        }
        self.kill_child(logical);
        let _ = generation;
    }

    /// Sends `call.cancel` to every forwarded call of the session
    /// (fire-and-forget; a resposta cai em request_id desconhecido).
    /// Called at quiesce start for deterministic delivery.
    fn cancel_session_calls(&self, sid: &str, reason: &str) {
        let jobs: Vec<(Arc<Mutex<UnixStream>>, String, String, String, String)> = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return };
            self.inner
                .pending
                .lock()
                .unwrap()
                .values()
                .filter(|p| p.session == sid)
                .filter_map(|p| match &p.kind {
                    PendingKind::Call { ticket } => Some(ticket.clone()),
                    PendingKind::Lifecycle => None,
                })
                .map(|ticket| {
                    (
                        s.writer.clone(),
                        s.id.clone(),
                        s.instance.to_string(),
                        s.generation.to_string(),
                        ticket,
                    )
                })
                .collect()
        };
        for (writer, id, instance, generation, ticket) in jobs {
            let msg = json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "call.cancel",
                "message_id": self.fresh_id("msg"),
                "session_id": id,
                "instance_id": instance,
                "generation": generation,
                "request_id": self.fresh_id("req"),
                "body": {"ticket": ticket, "reason": reason},
            });
            let raw = serde_json::to_vec(&msg).unwrap_or_default();
            if let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) {
                let _ = Self::write_frame(&writer, &f);
            }
        }
    }

    fn drop_session(&self, sid: &str) {
        let sess = self.inner.sessions.lock().unwrap().remove(sid);
        if let Some(s) = sess {
            self.inner.by_instance.lock().unwrap().remove(&(s.logical.clone(), s.instance));
            // Orphaned calls: outcome-unknown, no retry.
            let mut pending = self.inner.pending.lock().unwrap();
            let owned: Vec<String> = pending.iter()
                .filter(|(_, p)| p.session == sid)
                .map(|(k, _)| k.clone())
                .collect();
            for k in owned {
                if let Some(p) = pending.remove(&k) {
                    let _ = p.tx.send(self.gone_envelope(&k, &s));
                }
            }
            // Filhas do consumidor morto: revoga autoridade de imediato
            // (workers observe the dropped session and settle the tickets).
            for child in s.dep_requests.lock().unwrap().values().copied() {
                self.inner.kernel.call_cancel(child, "session-lost");
            }
            // Resources acquired via this session: releases them (the instance
            // may survive; orphaned handles may not).
            for h in s.ext_handles.lock().unwrap().drain(..) {
                let _ = self.inner.kernel.release(h);
            }
            let writer = s.writer.clone();
            let dup = writer.lock().unwrap().try_clone();
            if let Ok(dup) = dup {
                let _ = dup.shutdown(std::net::Shutdown::Both);
            }
        }
    }

    fn gone_envelope(&self, request_id: &str, s: &Session) -> Envelope {
        Envelope {
            ty: "call.error".to_string(),
            message_id: self.fresh_id("msg"),
            session_id: Some(s.id.clone()),
            instance_id: Some(s.instance.to_string()),
            generation: Some(s.generation),
            request_id: Some(request_id.to_string()),
            body: json!({"ticket": "", "error": {"code": "outcome-unknown", "message": "session lost"}}),
        }
    }

    // ---- lado kernel: entrega de chamadas ----

    fn forward_call(&self, req: &ForwardRequest) -> ForwardOutcome {
        // Global cap first: under overload, rejects fast without working.
        if self.inner.pending.lock().unwrap().len() >= MAX_PENDING_PER_HOST {
            return ForwardOutcome::Err {
                code: "resource-exhausted".to_string(),
                message: "host reply queue full".to_string(),
            };
        }
        let sid = self
            .inner
            .by_instance
            .lock()
            .unwrap()
            .get(&(req.logical.clone(), req.instance.0))
            .cloned();
        let Some(sid) = sid else {
            return ForwardOutcome::Failed(matrix_core::ForwardError::Gone(format!(
                "no session for {} inst-{}",
                req.logical, req.instance.0
            )));
        };
        // Pinned generation: an old-generation session never serves the new one.
        let gen_ok = self
            .inner
            .sessions
            .lock()
            .unwrap()
            .get(&sid)
            .map(|s| s.generation == req.generation)
            .unwrap_or(false);
        if !gen_ok {
            return ForwardOutcome::Failed(matrix_core::ForwardError::Gone(format!(
                "stale session generation for {}", req.logical
            )));
        }
        // Per-session cap (M2.3): past the limit, immediate error with no queue.
        {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(&sid) else {
                return ForwardOutcome::Failed(matrix_core::ForwardError::Gone(format!(
                    "no session for {} inst-{}",
                    req.logical, req.instance.0
                )));
            };
            let mut n = s.in_flight.lock().unwrap();
            if *n >= MAX_CALLS_PER_SESSION {
                return ForwardOutcome::Err {
                    code: "resource-exhausted".to_string(),
                    message: format!("too many in-flight calls on {}", req.logical),
                };
            }
            *n += 1;
        }
        let _in_flight = InFlightGuard { host: self.clone(), sid: sid.clone() };
        // Outstanding-data quota (M6 closing): bound provider-bound bytes
        // per session; control traffic never charges (reserved budget).
        let input_bytes = serde_json::to_vec(&req.input).map(|v| v.len()).unwrap_or(0);
        if !self.charge_send(&sid, input_bytes) {
            return ForwardOutcome::Err {
                code: "resource-exhausted".to_string(),
                message: format!("session send quota exceeded on {}", req.logical),
            };
        }
        let _budget = SendBudget { host: self.clone(), sid: sid.clone(), bytes: input_bytes };
        let request_id = self.fresh_id("req");
        let (tx, rx) = mpsc::channel();
        self.inner.pending.lock().unwrap().insert(
            request_id.clone(),
            Pending { session: sid.clone(), tx, kind: PendingKind::Call { ticket: format!("tkt-{}", req.ticket.0) } },
        );
        let body = json!({
            "ticket": format!("tkt-{}", req.ticket.0),
            "capability": req.cap,
            "input": req.input,
            "timeout_ms": req.timeout_ms,
        });
        if let Err(e) = self.send_to(
            &sid, &req.logical, req.instance.0, req.generation,
            "call.open", Some(request_id.clone()), body,
        ) {
            self.inner.pending.lock().unwrap().remove(&request_id);
            return ForwardOutcome::Failed(matrix_core::ForwardError::Gone(e));
        }
        let deadline = Instant::now() + Duration::from_millis(req.timeout_ms.max(1));
        loop {
            if req.cancel.load(Ordering::SeqCst) {
                // Best effort: avisa no fio e abandona a espera.
                let _ = self.send_to(
                    &sid, &req.logical, req.instance.0, req.generation,
                    "call.cancel",
                    Some(self.fresh_id("req")),
                    json!({"ticket": format!("tkt-{}", req.ticket.0), "reason": "cancelled"}),
                );
                self.inner.pending.lock().unwrap().remove(&request_id);
                return ForwardOutcome::Failed(matrix_core::ForwardError::Cancelled);
            }
            let now = Instant::now();
            if now >= deadline {
                self.inner.pending.lock().unwrap().remove(&request_id);
                return ForwardOutcome::Failed(matrix_core::ForwardError::Timeout);
            }
            match rx.recv_timeout(Duration::from_millis(CALL_POLL_MS).min(deadline - now)) {
                Ok(env) => {
                    if env.ty == "call.result" {
                        let status = env.body.get("status").and_then(|v| v.as_str()).unwrap_or("");
                        if status == "ok" {
                            return ForwardOutcome::Ok(
                                env.body.get("output").cloned().unwrap_or(Value::Null),
                            );
                        }
                        let err = env.body.get("error").cloned().unwrap_or(json!({}));
                        let code = err.get("code").and_then(|v| v.as_str()).unwrap_or("internal").to_string();
                        let msg = err.get("message").and_then(|v| v.as_str()).unwrap_or("remote error").to_string();
                        return ForwardOutcome::Err { code, message: msg };
                    }
                    if env.ty == "call.error" {
                        // Schema: body {ticket, error: {code, message}}.
                        // Embedded lost-session errors land here too.
                        let err = env.body.get("error").cloned().unwrap_or_else(|| env.body.clone());
                        let code = err.get("code").and_then(|v| v.as_str()).unwrap_or("outcome-unknown").to_string();
                        let msg = err.get("message").and_then(|v| v.as_str()).unwrap_or("session lost").to_string();
                        return ForwardOutcome::Err { code, message: msg };
                    }
                    // Embedded drop-of-session call.error.
                    return ForwardOutcome::Failed(matrix_core::ForwardError::Gone(
                        "session lost".to_string(),
                    ));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return ForwardOutcome::Failed(matrix_core::ForwardError::Gone(
                        "session lost".to_string(),
                    ));
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// Clones the session writer without holding the sessions lock across
    /// I/O (M6 closing): callers serialize on the per-session writer and
    /// never block the map, control, or other sessions on a slow peer.
    fn session_sink(&self, sid: &str) -> Option<(Arc<Mutex<UnixStream>>, String, u64, u64)> {
        let sessions = self.inner.sessions.lock().unwrap();
        sessions.get(sid).map(|s| (s.writer.clone(), s.logical.clone(), s.instance, s.generation))
    }

    /// Charges `bytes` against the session outstanding-data quota
    /// (`max_queued_bytes` or default). Control traffic never charges:
    /// it owns a reserved budget by construction. Returns false when the
    /// session is over quota (caller refuses with resource-exhausted).
    ///
    /// Transport budget, stated once: per session, pinned data traffic
    /// (in-flight inputs + in-progress payload sends) never exceeds
    /// quota + one max frame; control traffic (cancel/heartbeat/lifecycle/
    /// credit/end/local errors) bypasses the quota, is tiny and bounded
    /// per message, and never blocks the sessions map or other sessions.
    fn charge_send(&self, sid: &str, bytes: usize) -> bool {
        let sessions = self.inner.sessions.lock().unwrap();
        let Some(s) = sessions.get(sid) else { return false };
        let mut used = s.send_used.lock().unwrap();
        if used.saturating_add(bytes) > s.send_quota {
            return false;
        }
        *used = used.saturating_add(bytes);
        true
    }

    /// Releases a previous charge (saturating; sessions may rotate).
    fn release_send(&self, sid: &str, bytes: usize) {
        if let Some(s) = self.inner.sessions.lock().unwrap().get(sid) {
            let mut used = s.send_used.lock().unwrap();
            *used = used.saturating_sub(bytes);
        }
    }

    /// Writes one frame through a cloned writer (outside the sessions lock),
    /// bounded by the per-frame send budget (mutex wait + write share it).
    fn write_frame(writer: &Mutex<UnixStream>, f: &[u8]) -> std::io::Result<()> {
        write_frame_deadline(writer, f, Duration::from_millis(WRITE_TIMEOUT_MS))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::TimedOut, e.to_string()))
    }

    fn send_to(
        &self,
        sid: &str,
        logical: &str,
        instance: u64,
        generation: u64,
        ty: &str,
        request_id: Option<String>,
        body: Value,
    ) -> Result<(), String> {
        let env = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": ty,
            "message_id": self.fresh_id("msg"),
            "session_id": sid,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "request_id": request_id,
            "body": body,
        });
        // The schema is enforced on what we send (both sides).
        let raw = serde_json::to_vec(&env).map_err(|e| e.to_string())?;
        let frame = encode(&raw, DEFAULT_MAX_FRAME).map_err(|e| e.to_string())?;
        let Some((writer, got_logical, got_instance, got_generation)) = self.session_sink(sid) else {
            return Err(format!("no session {}", sid));
        };
        // Best-effort binding check (the session may rotate after the
        // snapshot; correlation and send errors remain authoritative).
        if got_logical != logical || got_instance != instance || got_generation != generation {
            return Err("stale session binding".to_string());
        }
        Self::write_frame(&writer, &frame).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Synchronous transaction with the plugin (lifecycle): registers the
    /// respondent, sends, waits for the terminal with a deadline. `Ok(None)` = no session.
    fn transact(
        &self,
        sid: &str,
        logical: &str,
        instance: u64,
        generation: u64,
        ty: &str,
        body: Value,
        timeout_ms: u64,
    ) -> Result<Option<Envelope>, String> {
        let sessions = self.inner.sessions.lock().unwrap();
        if !sessions.contains_key(sid) {
            return Ok(None);
        }
        drop(sessions);
        if self.inner.pending.lock().unwrap().len() >= MAX_PENDING_PER_HOST {
            return Err("host reply queue full".to_string());
        }
        let request_id = self.fresh_id("req");
        let (tx, rx) = mpsc::channel();
        self.inner.pending.lock().unwrap().insert(
            request_id.clone(),
            Pending { session: sid.to_string(), tx, kind: PendingKind::Lifecycle },
        );
        if let Err(e) = self.send_to(sid, logical, instance, generation, ty, Some(request_id.clone()), body) {
            self.inner.pending.lock().unwrap().remove(&request_id);
            return Err(e);
        }
        match rx.recv_timeout(Duration::from_millis(timeout_ms.max(1))) {
            Ok(env) => Ok(Some(env)),
            Err(_) => {
                self.inner.pending.lock().unwrap().remove(&request_id);
                Err("lifecycle timeout".to_string())
            }
        }
    }

    // ---- plugin side: accept + sessions ----

    fn accept_loop(&self, listener: UnixListener) {
        let _ = listener.set_nonblocking(false);
        loop {
            if self.inner.shutdown.load(Ordering::SeqCst) {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    // Shutdown wakeup (byteless connection): ignores and rechecks.
                    if self.inner.connecting.fetch_add(1, Ordering::SeqCst) >= 32 {
                        self.inner.connecting.fetch_sub(1, Ordering::SeqCst);continue;
                    }
                    let h = self.clone();
                    if std::thread::Builder::new()
                        .name("matrix-host-conn".into())
                        .spawn(move || {
                            struct Guard(Host);
                            impl Drop for Guard {fn drop(&mut self) {self.0.inner.connecting.fetch_sub(1, Ordering::SeqCst);}}
                            let guard=Guard(h);
                            guard.0.serve_conn(stream);
                        }).is_err() {self.inner.connecting.fetch_sub(1, Ordering::SeqCst);}
                }
                Err(_) => {
                    if self.inner.shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    fn serve_conn(&self, mut stream: UnixStream) {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(HELLO_MS)));
        // 1. hello → welcome/reject.
        let raw = match read_frame(&mut stream, DEFAULT_MAX_FRAME) {
            Ok(Some(r)) => r,
            _other => {
                return;
            }
        };
        let env = match parse_frame_payload(&raw) {
            Ok(e) if e.ty == "hello" => e,
            Ok(_) => {
                let _ = self.write_reject(&mut stream, "m?", "invalid-message", "first frame must be hello");
                return;
            }
            Err(e) => {
                let _ = self.write_reject(&mut stream, "m?", &e.code, "bad hello");
                return;
            }
        };
        let (versions, want_frame, offered_features) = match hello_offer(&env) {
            Ok(v) => v,
            Err(e) => {
                let _ = self.write_reject(&mut stream, &env.message_id, &e.code, "bad hello body");
                return;
            }
        };
        let session_id = self.fresh_id("sess");
        // Announcement under explicit policy: unenabled, nothing is
        // negotiated even if the client offers it (M6.1 step 1).
        let offered: &[String] = if self.inner.policy.enable_dependency_calls {
            &offered_features
        } else {
            &[]
        };
        let welcome = match negotiate(&versions, offered, want_frame, session_id.clone()) {
            Ok(w) => w,
            Err(e) => {
                let _ = self.write_reject(&mut stream, &env.message_id, &e.code, "version");
                return;
            }
        };
        let max_frame = welcome.max_frame.min(DEFAULT_MAX_FRAME);
        let wmsg = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "welcome",
            "message_id": env.message_id,
            "session_id": session_id,
            "body": {"version": welcome.version, "max_frame": max_frame,
                     "limits": {"max_calls": 64, "max_queue_bytes": 4194304, "max_streams": 16},
                     "features": welcome.features},
        });
        if self.write_value(&mut stream, &wmsg, max_frame).is_err() {
            return;
        }
        // 2. component.register → binds to the instance (definition rules).
        let _ = stream.set_read_timeout(Some(Duration::from_millis(REGISTER_MS)));
        let raw = match read_frame(&mut stream, max_frame) {
            Ok(Some(r)) => r,
            _ => return,
        };
        let reg = match parse_frame_payload(&raw) {
            Ok(e) if e.ty == "component.register" => e,
            other => {
                let _ = other;
                return;
            }        };
        // The hello's session must be cited.
        if reg.session_id.as_deref() != Some(session_id.as_str()) {
            return;
        }
        let child_id = reg.body.get("manifest").and_then(|m| m.get("id")).and_then(|v| v.as_str()).unwrap_or("").to_string();
        if child_id.is_empty() {
            return;
        }
        // Binds: our pending activation (spawn) or the live Active instance.
        let binding: Option<(u64, u64)> = self.inner.pending_activation.lock().unwrap().get(&child_id).cloned()
            .or_else(|| {
                self.inner.kernel.contexts.current(&child_id).and_then(|c| {
                    if c.state.canonical() == matrix_core::Fsm::Active {
                        Some((c.instance.0, c.generation))
                    } else {
                        None
                    }
                })
            });
        let Some((instance, generation)) = binding else {
            let _ = self.write_reject(&mut stream, &reg.message_id, "context-not-active", "no active instance");
            return;
        };
        if self.inner.policy.secure {
            let supplied = env.body.get("launch_token").and_then(|v| v.as_str()).unwrap_or("");
            let valid = self.inner.launch_tokens.lock().unwrap().get(&child_id)
                .is_some_and(|(i,t)| *i == instance && matrix_guard::token_eq(t, supplied));
            if !valid {
                let _ = self.write_reject(&mut stream, &reg.message_id, "permission-denied", "invalid launch authority");
                return;
            }
        }
        // One live session per instance (no binding theft).
        {
            let by = self.inner.by_instance.lock().unwrap();
            if by.contains_key(&(child_id.clone(), instance)) {
                let _ = self.write_reject(&mut stream, &reg.message_id, "internal", "session already bound");
                return;
            }
        }
        // Register dedup (idempotent replay within host retention).
        let canonical = serde_json::to_string(&reg.body).unwrap_or_default();
        let verdict = self
            .inner
            .register_ids
            .lock()
            .unwrap()
            .check(&format!("register:{}:{}:{}", child_id, instance, reg.message_id), &canonical);
        if verdict == IdVerdict::DuplicateDivergent {
            return;
        }
        let writer = match stream.try_clone() {
            Ok(w) => w,
            Err(_) => return,
        };
        // Bounded writes: a never-reading peer must not freeze the writer
        // forever (M6 closing). Read/write timeouts are independent.
        let _ = writer.set_write_timeout(Some(Duration::from_millis(WRITE_TIMEOUT_MS)));
        // Per-session outstanding-data quota: declared outbound limit or default.
        let send_quota = self
            .inner
            .kernel
            .outbound_policy_of(&child_id)
            .map(|p| p.limits.max_queued_bytes.min(isize::MAX as u64) as usize)
            .unwrap_or(DEFAULT_SEND_QUOTA)
            .max(1);
        self.inner.sessions.lock().unwrap().insert(
            session_id.clone(),
            Session {
                id: session_id.clone(),
                logical: child_id.clone(),
                instance,
                generation,
                writer: Arc::new(Mutex::new(writer)),
                dedup: Mutex::new(IdWindow::new(256)),
                in_flight: Mutex::new(0),
                send_used: Mutex::new(0),
                send_quota,
                streams: Mutex::new(HashMap::new()),
                rstreams: Arc::new(Mutex::new(HashMap::new())),
                features: welcome.features.clone(),
                dep_seen: Mutex::new(HashSet::new()),
                dep_requests: Mutex::new(HashMap::new()),
                parked: Mutex::new(vec![]),
                dep_sync: Mutex::new(DepSync::default()),
                ext_handles: Mutex::new(vec![]),
                topics: self.inner.kernel.subscriptions_of(&child_id),
            },
        );
        self.inner.by_instance.lock().unwrap().insert((child_id.clone(), instance), session_id.clone());
        self.inner.pending_activation.lock().unwrap().remove(&child_id);
        let registered = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "registered",
            "message_id": reg.message_id,
            "session_id": session_id,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "body": {"logical": child_id},
        });
        let wres = self.write_value(&mut stream, &registered, max_frame);
        if wres.is_err() {
            self.drop_session(&session_id);
            return;
        }
        // 3. Read loop on its own thread BEFORE activate: the activate
        // (and call) responses only arrive via socket reads, so transacting
        // transacionar na thread leitora seria deadlock certo.
        let read_stream = match stream.try_clone() {
            Ok(s) => s,
            Err(_) => {
                self.drop_session(&session_id);
                return;
            }
        };
        let reader = self.clone();
        let reader_sid = session_id.clone();
        std::thread::Builder::new()
            .name("matrix-host-read".into())
            .spawn(move || reader.read_loop(read_stream, &reader_sid, max_frame))
            .ok();
        // 4. activate with a deadline (the kernel already activated; this is the host handshake).
        let op = self.fresh_id("op");
        // Opaque activation bindings (M6.1 step 3): negotiated sessions only;
        // legacy sessions ignore the field and keep operating.
        let dep_bindings = if welcome.features.iter().any(|f| f == DEPENDENCY_CALLS_1) {
            self.inner
                .kernel
                .dependency_bindings_of(&child_id)
                .iter()
                .map(|b| json!({"binding_id": b.id, "capability": b.capability}))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        let mut act_body = json!({"operation_id": op, "manifest": {"id": child_id}, "bindings": []});
        if welcome.features.iter().any(|f| f == DEPENDENCY_CALLS_1) {
            act_body["dependency_bindings"] = json!(dep_bindings);
        }
        let tact = self.transact(
            &session_id, &child_id, instance, generation,
            "lifecycle.activate",
            act_body,
            ACTIVATE_MS,
        );
        match tact {
            Ok(Some(_)) => {}
            _ => {
                self.drop_session(&session_id);
                self.kill_child(&child_id);
                return;
            }
        }
        // serve_conn ends here; read_loop owns the session lifetime.
    }

    /// Session read loop: slices frames and dispatches. Exits on EOF/error/
    /// abuse/shutdown and drops the session (no later false success).
    fn read_loop(&self, mut stream: UnixStream, sid: &str, max_frame: usize) {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
        let mut buf: Vec<u8> = vec![];
        let mut tmp = [0u8; 8192];
        use std::io::Read;
        loop {
            if self.inner.shutdown.load(Ordering::SeqCst) {
                break;
            }
            match stream.read(&mut tmp) {
                Ok(0) => {
                    break; // EOF: morte/EOF do filho.
                }
                Ok(n) => {
                    buf.extend_from_slice(&tmp[..n]);
                    if buf.len() > max_frame + 4 {
                        break; // abuse: exceeds the negotiated max frame.
                    }
                    loop {
                        match split_frame(&buf, max_frame) {
                            Ok(Some((payload, rest))) => {
                                let rest = rest.to_vec();
                                self.on_plugin_frame(sid, payload);
                                buf = rest;
                            }
                            Ok(None) => break,
                            Err(_) => {
                                // Tamanho declarado acima do negociado (abuso):
                                // ends the session in a bounded way (M2.3).
                                buf.clear();
                                self.drop_session(sid);
                                return;
                            }
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => continue,
                Err(_) => {
                    break;
                }
            }
        }
        self.drop_session(sid);
    }

    fn on_plugin_frame(&self, sid: &str, payload: &[u8]) {
        let env = match parse_frame_payload(payload) {
            Ok(e) => e,
            Err(_) => return, // garbage: ignores without dropping the session (tighter in M2.3).
        };
        // Verifies the session binding (identities are verified claims).
        let bound_ok = self
            .inner
            .sessions
            .lock()
            .unwrap()
            .get(sid)
            .map(|s| {
                env.session_id.as_deref() == Some(s.id.as_str())
                    && env.instance_id.as_deref().map(|v| v == s.instance.to_string()).unwrap_or(true)
                    && env.generation.map(|g| g == s.generation).unwrap_or(true)
            })
            .unwrap_or(false);
        if !bound_ok {
            return;
        }
        // Dedup by message_id within the session (idempotent replay).
        {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return };
            let canon = serde_json::to_string(&env.body).unwrap_or_default();
            let v = s.dedup.lock().unwrap().check(&env.message_id, &canon);
            if v != IdVerdict::New {
                // Extension exception (M6.1): an identical open retransmit
                // answers `duplicate-request` instead of dropping silently.
                if env.ty.as_str() == "dependency.open" && v == IdVerdict::DuplicateSame {
                    let (instance, generation) = (s.instance, s.generation);
                    let negotiated = s.features.iter().any(|f| f == DEPENDENCY_CALLS_1);
                    drop(sessions);
                    if negotiated {
                        if let Some(rid) = env.request_id.clone() {
                            self.send_dep_result(sid, instance, generation, &rid, DUPLICATE_REQUEST, "duplicate open", None);
                        }
                    }
                }
                return; // identical replay: no re-execution; divergent: no execution.
            }
        }
        // dependency-calls/1 extension (M6.1 step 1): its own gate —
        // negotiation, semantics, authorization, and request reservation.
        if env.ty.as_str().starts_with("dependency.") {
            self.on_dependency_frame(sid, &env);
            return;
        }
        match env.ty.as_str() {
            t if t == "call.result" || t == "call.error" || t == "lifecycle.result" => {
                if let Some(rid) = env.request_id.clone() {
                    let tx = self.inner.pending.lock().unwrap().remove(&rid).map(|p| p.tx);
                    if let Some(tx) = tx {
                        let _ = tx.send(env);
                    }
                    // request_id desconhecido/tardio: descartado (M2.5).
                }
            }
            "session.heartbeat" => {
                let hb = json!({
                    "protocol": matrix_proto::PROTOCOL_ID,
                    "version": matrix_proto::PROTOCOL_VERSION,
                    "type": "session.heartbeat",
                    "message_id": self.fresh_id("msg"),
                    "session_id": sid,
                    "body": {},
                });
                let sessions = self.inner.sessions.lock().unwrap();
                if let Some(s) = sessions.get(sid) {
                    let raw = serde_json::to_vec(&hb).unwrap_or_default();
                    if let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) {
                        let writer = s.writer.clone();
                        drop(sessions);
                        let _ = Self::write_frame(&writer, &f);
                    }
                }
            }
            "session.close" => {
                self.drop_session(sid);
            }
            "stream.data" | "stream.credit" | "stream.end" => {
                self.on_stream_frame(sid, &env);
            }
            "resource.acquire" | "resource.release" => {
                self.on_resource_frame(sid, &env);
            }
            _ => {}
        }
    }

    /// Responde `resource.result` com o `request_id` do pedido (M6.3).
    fn send_resource_result(
        &self,
        sid: &str,
        instance: u64,
        generation: u64,
        request_id: &str,
        operation_id: Value,
        status: &str,
        extra: Value,
    ) {
        let mut body = serde_json::Map::new();
        body.insert("operation_id".into(), operation_id);
        body.insert("status".into(), Value::String(status.to_string()));
        if let Value::Object(m) = extra {
            for (k, v) in m {
                body.insert(k, v);
            }
        }
        let v = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "resource.result",
            "message_id": self.fresh_id("msg"),
            "session_id": sid,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "request_id": request_id,
            "body": body,
        });
        let raw = serde_json::to_vec(&v).unwrap_or_default();
        let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) else { return };
        if let Some((writer, _, _, _)) = self.session_sink(sid) {
            let _ = Self::write_frame(&writer, &f);
        }
    }

    /// Component-driven acquisition/release of activation resources (M6.3).
    /// Synchronous and bounded: same core validation + per-context cap;
    /// context withdraw releases everything (no residue).
    fn on_resource_frame(&self, sid: &str, env: &Envelope) {
        let (logical, instance, generation, rid, op) = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return };
            (
                s.logical.clone(),
                s.instance,
                s.generation,
                env.request_id.clone().unwrap_or_default(),
                env.body.get("operation_id").cloned().unwrap_or(Value::Null),
            )
        };
        let answer = |host: &Self, status: &str, extra: Value| {
            host.send_resource_result(sid, instance, generation, &rid, op.clone(), status, extra)
        };
        match env.ty.as_str() {
            "resource.acquire" => {
                match parse_resource_kind(&env.body) {
                    Err(e) => answer(self, "error", json!({"code": "invalid-message", "message": e})),
                    Ok(kind) => match self.inner.kernel.acquire_external(
                        &logical,
                        kind,
                        matrix_core::InstanceId(instance),
                        generation,
                    ) {
                        Err(e) => {
                            let code = if e == "resource-exhausted" { "resource-exhausted" } else { "invalid-message" };
                            answer(self, "error", json!({"code": code, "message": e}))
                        }
                        Ok(h) => {
                            if let Some(s) = self.inner.sessions.lock().unwrap().get(sid) {
                                s.ext_handles.lock().unwrap().push(h);
                            }
                            answer(self, "ok", json!({"handle": h.0.to_string()}))
                        }
                    },
                }
            }
            "resource.release" => {
                match env.body.get("handle").and_then(|v| v.as_str()).and_then(|s| s.parse::<u64>().ok()) {
                    None => answer(self, "error", json!({"code": "invalid-message", "message": "bad handle"})),
                    Some(n) => {
                        // Ownership at the external boundary: a session may only
                        // release its own activation's resources. Knowing another
                        // handle id authorizes nothing.
                        let owned = self
                            .inner
                            .kernel
                            .resources
                            .record(ResourceHandle(n))
                            .is_some_and(|r| {
                                r.owner_instance.0 == instance
                                    && r.owner_logical == logical
                                    && r.generation == generation
                            });
                        if !owned {
                            answer(self, "error", json!({"code": "permission-denied", "message": "foreign or stale handle"}));
                            return;
                        }
                        let h = ResourceHandle(n);
                        match self.inner.kernel.release(h) {
                            Err(e) => answer(self, "error", json!({"code": "invalid-message", "message": e})),
                            Ok(()) => {
                                if let Some(s) = self.inner.sessions.lock().unwrap().get(sid) {
                                    s.ext_handles.lock().unwrap().retain(|x| *x != h);
                                }
                                answer(self, "ok", json!({}))
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Extension answer (step 3): error or ok `dependency.result`,
    /// echoing `request_id`, doing no work beyond answering.
    /// `origin` names the provider in business errors (without reinterpreting
    /// message as control). Outside caller locks.
    fn send_dep_result(
        &self,
        sid: &str,
        instance: u64,
        generation: u64,
        request_id: &str,
        code: &str,
        message: &str,
        origin: Option<&str>,
    ) {
        self.send_dep_ok(sid, instance, generation, request_id, None, Some((code, message, origin)));
    }

    /// Envia `dependency.result` ok (com `output`) ou de erro.
    fn send_dep_ok(
        &self,
        sid: &str,
        instance: u64,
        generation: u64,
        request_id: &str,
        output: Option<Value>,
        error: Option<(&str, &str, Option<&str>)>,
    ) {
        let body = match (output, error) {
            (Some(out), _) => json!({"status": "ok", "output": out}),
            (_, Some((code, message, origin))) => {
                let mut e = json!({"code": code, "message": message});
                if let Some(o) = origin {
                    e["origin"] = json!(o);
                }
                json!({"status": "error", "error": e})
            }
            _ => json!({"status": "error", "error": {"code": "internal", "message": "empty result"}}),
        };
        let v = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "dependency.result",
            "message_id": self.fresh_id("msg"),
            "session_id": sid,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "request_id": request_id,
            "body": body,
        });
        let raw = serde_json::to_vec(&v).unwrap_or_default();
        let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) else { return };
        if let Some((writer, _, _, _)) = self.session_sink(sid) {
            let _ = Self::write_frame(&writer, &f);
        }
    }

    /// Sends `dependency.accepted` (admitted child; terminal comes after).
    fn send_dep_accepted(&self, sid: &str, instance: u64, generation: u64, open_rid: &str, child: TicketId) {
        let v = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "dependency.accepted",
            "message_id": self.fresh_id("msg"),
            "session_id": sid,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "request_id": open_rid,
            "body": {"child_ticket": child.0.to_string()},
        });
        let raw = serde_json::to_vec(&v).unwrap_or_default();
        let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) else { return };
        if let Some((writer, _, _, _)) = self.session_sink(sid) {
            let _ = Self::write_frame(&writer, &f);
        }
    }

    /// Sends `dependency.cancel.result` (correlated by the cancel request).
    fn send_dep_cancel_result(&self, sid: &str, instance: u64, generation: u64, cancel_rid: &str, target: &str, state: &str) {        let v = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "dependency.cancel.result",
            "message_id": self.fresh_id("msg"),
            "session_id": sid,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "request_id": cancel_rid,
            "body": {"target_request_id": target, "state": state},
        });
        let raw = serde_json::to_vec(&v).unwrap_or_default();
        let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) else { return };
        if let Some((writer, _, _, _)) = self.session_sink(sid) {
            let _ = Self::write_frame(&writer, &f);
        }
    }

    /// dependency-calls/1 gate: negotiation → semantics →
    /// authorization → request reservation → dispatch on a dedicated worker
    /// (never blocks the reader). Worker quotas come from the kernel's
    /// admission; pre-checks here only bound threads (authoritative later).
    fn on_dependency_frame(&self, sid: &str, env: &Envelope) {
        enum Outcome {
            Answer { code: &'static str, msg: String },
            Spawn(DepJob),
            Drop,
        }
        let outcome: Outcome = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return };
            let negotiated = s.features.iter().any(|f| f == DEPENDENCY_CALLS_1);
            if !negotiated {
                Outcome::Answer { code: UNSUPPORTED_FEATURE, msg: "extension not negotiated".into() }
            } else if validate_dependency_body(&env.ty, &env.body).is_err() {
                Outcome::Answer { code: "invalid-message", msg: "invalid dependency message".into() }
            } else if env.ty.as_str() == "dependency.cancel" {
                drop(sessions);
                self.on_dep_cancel(sid, env);
                return;
            } else if env.ty.as_str() != "dependency.open" {
                // `accepted`/`result` run host→component: validated
                // above, dropped without effect.
                Outcome::Drop
            } else {
                // Authorization (declared outbound intent; the grant is step 2).
                match self.inner.kernel.outbound_policy_of(&s.logical) {
                    None => Outcome::Answer { code: "permission-denied", msg: "no outbound policy".into() },
                    Some(policy) => {
                        let rid = env.request_id.clone().unwrap_or_default();
                        let parent = parse_parent_ticket(
                            env.body.get("parent_ticket").and_then(|v| v.as_str()).unwrap_or(""),
                        );
                        let mut seen = s.dep_seen.lock().unwrap();
                        if parent.is_none() {
                            Outcome::Answer { code: "invalid-message", msg: "bad parent_ticket".into() }
                        } else if seen.contains(&rid) {
                            Outcome::Answer { code: DUPLICATE_REQUEST, msg: "duplicate open".into() }
                        } else if seen.len() as u64 >= policy.limits.max_seen_requests {
                            Outcome::Answer { code: "resource-exhausted", msg: "seen-request table full".into() }
                        } else if self.inner.pending.lock().unwrap().len() >= MAX_PENDING_PER_HOST {
                            Outcome::Answer { code: "resource-exhausted", msg: "host reply queue full".into() }
                        } else if s.dep_sync.lock().unwrap().pending.len() >= MAX_PENDING_PER_HOST {
                            // Dispatch-worker cap for still-unmapped opens
                            // (admission is authoritative; here it only bounds threads).
                            // NOTE: no consumer `in_flight` pre-check: forwarded
                            // pais encaminhados ocupam esses slots e as filhas
                            // parents occupy those slots and children would
                            // starve; admission session/global quotas bound the real work.
                            Outcome::Answer { code: "resource-exhausted", msg: "too many pending opens".into() }
                        } else {
                            seen.insert(rid.clone());
                            let mut sync = s.dep_sync.lock().unwrap();
                            sync.pending.insert(rid.clone());
                            Outcome::Spawn(DepJob {
                                consumer_sid: sid.to_string(),
                                consumer_logical: s.logical.clone(),
                                cinstance: s.instance,
                                cgeneration: s.generation,
                                request_id: rid,
                                binding: env.body.get("binding_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                // `None` is unreachable (invalid-message arm above);
                                // the substitute denies at admission (fail-safe).
                                parent: parent.unwrap_or(TicketId(u64::MAX)),
                                timeout_ms: env.body.get("timeout_ms").and_then(|v| v.as_u64()).unwrap_or(0),
                                input: env.body.get("input").cloned().unwrap_or(Value::Null),
                            })
                        }
                    }
                }
            }
        };
        match outcome {
            Outcome::Drop => {}
            Outcome::Answer { code, msg } => {
                let (instance, generation, rid) = {
                    let sessions = self.inner.sessions.lock().unwrap();
                    let Some(s) = sessions.get(sid) else { return };
                    (s.instance, s.generation, env.request_id.clone().unwrap_or_default())
                };
                self.send_dep_result(sid, instance, generation, &rid, code, &msg, None);
            }
            Outcome::Spawn(job) => {
                let h = self.clone();
                if std::thread::Builder::new()
                    .name("matrix-dep-open".into())
                    .spawn(move || h.run_dep_open(job))
                    .is_err()
                {
                    // Threadless: undoes the reservation and denies (never blocks control).
                    if let Some(s) = self.inner.sessions.lock().unwrap().get(sid) {
                        s.dep_sync.lock().unwrap().pending.remove(&env.request_id.clone().unwrap_or_default());
                    }
                    let (instance, generation, rid) = {
                        let sessions = self.inner.sessions.lock().unwrap();
                        let Some(s) = sessions.get(sid) else { return };
                        (s.instance, s.generation, env.request_id.clone().unwrap_or_default())
                    };
                    self.send_dep_result(sid, instance, generation, &rid, "resource-exhausted", "no worker", None);
                }
            }
        }
    }

    /// Cancela filha do consumidor (passo 3): mapeada → revoga autoridade;
    /// worker without mapping yet → marks for abort; unknown →
    /// `unknown-request` error. Always answers (correlated by the cancel
    /// by the cancel request, never the open's).
    fn on_dep_cancel(&self, sid: &str, env: &Envelope) {
        let target = env.body.get("target_request_id").and_then(|v| v.as_str()).unwrap_or("");
        let cancel_rid = env.request_id.clone().unwrap_or_default();
        enum CancelOut {
            Result { state: &'static str },
            Unknown,
        }
        let out: CancelOut = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return };
            let mapped = s.dep_requests.lock().unwrap().get(target).copied();
            match mapped {
                Some(child) => match self.inner.kernel.calls.get(child) {
                    None => CancelOut::Result { state: "terminal" },
                    Some(rec) if rec.state == matrix_core::TicketState::Admitted => {
                        if self.inner.kernel.call_cancel(child, "consumer-cancel") {
                            // Remote leg: forward the cancel over the route
                            // (executor drops the terminal by operation).
                            if let Some((peer, op)) = self.inner.remote_ops.lock().unwrap().get(&child).cloned() {
                                if let Some(t) = self.inner.remote_transport.lock().unwrap().clone() {
                                    t.call_cancel(&peer, &op);
                                }
                            }
                            CancelOut::Result { state: "revoked" }
                        } else {
                            CancelOut::Result { state: "terminal" }
                        }
                    }
                    Some(_) => CancelOut::Result { state: "terminal" },
                },
                None => {
                    let mut sync = s.dep_sync.lock().unwrap();
                    if sync.pending.contains(target) {
                        // Worker has not mapped yet: marks it; the worker aborts and
                        // responde `cancelled`. Idempotente.
                        sync.wanted.insert(target.to_string());
                        CancelOut::Result { state: "revoked" }
                    } else {
                        CancelOut::Unknown
                    }
                }
            }
        };
        let (instance, generation) = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return };
            (s.instance, s.generation)
        };
        match out {
            CancelOut::Result { state } => {
                self.send_dep_cancel_result(sid, instance, generation, &cancel_rid, target, state)
            }
            // Unknown ids get the `unknown-request` code in an error
            // `dependency.result` (state only admits revoked|terminal).
            CancelOut::Unknown => {
                self.send_dep_result(sid, instance, generation, &cancel_rid, "unknown-request", "unknown request", None)
            }
        }
    }

    /// Dispatches an admitted open to the provider and translates the
    /// terminal (step 3). Runs on a quota-bounded worker thread; every outcome
    /// closes the kernel ticket (no ghost ticket) and answers the open —
    /// closes the ticket — except a dead consumer (nobody to answer).
    fn run_dep_open(&self, job: DepJob) {
        use matrix_core::TicketState;
        let key = job.request_id.clone();
        // Admits at the coordinator (authoritative; pre-checks were bounds).
        let child = match self.inner.kernel.dependency_admit(&DepAdmit {
            parent: job.parent,
            binding: job.binding.clone(),
            caller_logical: job.consumer_logical.clone(),
            caller_instance: job.cinstance,
            caller_generation: job.cgeneration,
            session: job.consumer_sid.clone(),
            timeout_ms: job.timeout_ms,
        }) {
            Err(d) => {
                self.unpend_dep(&job.consumer_sid, &key);
                self.send_dep_result(
                    &job.consumer_sid, job.cinstance, job.cgeneration,
                    &key, d.code, &d.reason, None,
                );
                return;
            }
            Ok(c) => c,
        };
        // Maps (for idempotent late cancel) and leaves pending.
        let pre_cancelled = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(&job.consumer_sid) else {
                // Consumer died before mapping: revokes and settles.
                self.inner.kernel.call_cancel(child, "session-lost");
                self.inner.kernel.call_close(child);
                return;
            };
            s.dep_requests.lock().unwrap().insert(key.clone(), child);

            let mut sync = s.dep_sync.lock().unwrap();
            sync.pending.remove(&key);
            sync.wanted.remove(&key)
        };
        if pre_cancelled {
            // Cancel arrived before mapping: aborts without dispatching.
            self.inner.kernel.call_cancel(child, "cancelled-before-dispatch");
            self.send_dep_result(
                &job.consumer_sid, job.cinstance, job.cgeneration,
                &key, "cancelled", "cancelled before dispatch", None,
            );
            self.inner.kernel.call_close(child);
            return;
        }
        // Outstanding-data quota on the consumer session (its policy):
        // bounds bytes pinned in live exchanges; control never counts.
        let input_bytes = serde_json::to_vec(&job.input).map(|v| v.len()).unwrap_or(0);
        if !self.charge_send(&job.consumer_sid, input_bytes) {
            self.send_dep_result(
                &job.consumer_sid, job.cinstance, job.cgeneration,
                &key, "resource-exhausted", "session send quota exceeded", None,
            );
            self.inner.kernel.call_cancel(child, "quota-exceeded");
            self.inner.kernel.call_close(child);
            return;
        }
        let _budget = SendBudget { host: self.clone(), sid: job.consumer_sid.clone(), bytes: input_bytes };
        // Won admission: `accepted` before the terminal (normative
        // sequence; not a durable ack nor execution proof).
        self.send_dep_accepted(&job.consumer_sid, job.cinstance, job.cgeneration, &key, child);
        // Remote leg (M7): the ticket carries the executor peer; the
        // route manager owns the wire. Local legs continue below.
        // `_budget` keeps the input charge alive across the remote leg.
        let remote_peer = self.inner.kernel.calls.get(child).and_then(|r| {
            r.dep.as_ref().and_then(|d| d.remote_peer.clone())
        });
        if let Some(peer) = remote_peer {
            self.run_dep_open_remote(job, key, child, peer, _budget);
            return;
        }
        // Resolves the provider session from the admitted ticket.
        let prov = self.inner.kernel.calls.get(child).map(|r| (r.logical, r.instance, r.generation, r.cap));
        let Some((plogical, pinstance, pgeneration, cap)) = prov else {
            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "internal", "child vanished", None);
            self.inner.kernel.call_close(child);
            return;
        };
        let psid = self
            .inner
            .by_instance
            .lock()
            .unwrap()
            .get(&(plogical.clone(), pinstance.0))
            .cloned()
            .filter(|sid| {
                self.inner.sessions.lock().unwrap().get(sid).is_some_and(|s| s.generation == pgeneration)
            });
        let Some(psid) = psid else {
            // Executor vanished between admission and dispatch: no false success.
            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "outcome-unknown", "provider session lost", None);
            self.inner.kernel.call_cancel(child, "session-lost");
            self.inner.kernel.call_close(child);
            return;
        };
        // Provider per-session cap (rechecked; admission is authoritative).
        {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(&psid) else {
                self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "outcome-unknown", "provider session lost", None);
                self.inner.kernel.call_cancel(child, "session-lost");
                self.inner.kernel.call_close(child);
                return;
            };
            let mut n = s.in_flight.lock().unwrap();
            if *n >= MAX_CALLS_PER_SESSION {
                drop(n);
                drop(sessions);
                self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "resource-exhausted", "provider busy", None);
                self.inner.kernel.call_cancel(child, "provider-busy");
                self.inner.kernel.call_close(child);
                return;
            }
            *n += 1;
        }
        let _in_flight = InFlightGuard { host: self.clone(), sid: psid.clone() };
        // The child's absolute deadline (fixed at admission).
        let deadline = self
            .inner
            .kernel
            .calls
            .get(child)
            .and_then(|t| t.drain_until)
            .unwrap_or_else(|| Instant::now() + Duration::from_millis(job.timeout_ms.max(1)));
        let preq = self.fresh_id("req");
        let (tx, rx) = mpsc::channel();
        {
            let mut pending = self.inner.pending.lock().unwrap();
            if pending.len() >= MAX_PENDING_PER_HOST {
                drop(pending);
                self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "resource-exhausted", "host reply queue full", None);
                self.inner.kernel.call_cancel(child, "host-busy");
                self.inner.kernel.call_close(child);
                return;
            }
            pending.insert(
                preq.clone(),
                Pending { session: psid.clone(), tx, kind: PendingKind::Call { ticket: format!("tkt-{}", child.0) } },
            );
        }
        let remaining_ms = deadline.saturating_duration_since(Instant::now()).as_millis().max(1) as u64;
        if self
            .send_to(
                &psid, &plogical, pinstance.0, pgeneration,
                "call.open", Some(preq.clone()),
                json!({
                    "ticket": format!("tkt-{}", child.0),
                    "capability": cap,
                    "input": job.input,
                    "timeout_ms": remaining_ms,
                }),
            )
            .is_err()
        {
            self.inner.pending.lock().unwrap().remove(&preq);
            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "outcome-unknown", "provider send failed", None);
            self.inner.kernel.call_cancel(child, "send-failed");
            self.inner.kernel.call_close(child);
            return;
        }
        // Waits for the provider terminal while observing revocation, deadline,
        // and consumer liveness (never depends on handler cooperation
        // to revoke authority: revocation is a state mark).
        loop {
            // Dead consumer: revokes, tells the provider on the wire, settles.
            if !self.inner.sessions.lock().unwrap().contains_key(&job.consumer_sid) {
                self.inner.kernel.call_cancel(child, "session-lost");
                let _ = self.send_to(
                    &psid, &plogical, pinstance.0, pgeneration,
                    "call.cancel", Some(self.fresh_id("req")),
                    json!({"ticket": format!("tkt-{}", child.0), "reason": "session-lost"}),
                );
                self.inner.pending.lock().unwrap().remove(&preq);
                self.inner.kernel.call_close(child);
                return;
            }
            // Authority revoked another way: aborts with no false success.
            match self.inner.kernel.calls.get(child).map(|t| t.state) {
                Some(TicketState::Cancelled) => {
                    let _ = self.send_to(
                        &psid, &plogical, pinstance.0, pgeneration,
                        "call.cancel", Some(self.fresh_id("req")),
                        json!({"ticket": format!("tkt-{}", child.0), "reason": "cancelled"}),
                    );
                    self.inner.pending.lock().unwrap().remove(&preq);
                    self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "cancelled", "child revoked", None);
                    self.inner.kernel.call_close(child);
                    return;
                }
                Some(TicketState::Expired) => {
                    let _ = self.send_to(
                        &psid, &plogical, pinstance.0, pgeneration,
                        "call.cancel", Some(self.fresh_id("req")),
                        json!({"ticket": format!("tkt-{}", child.0), "reason": "expired"}),
                    );
                    self.inner.pending.lock().unwrap().remove(&preq);
                    self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "deadline-exceeded", "child expired", None);
                    self.inner.kernel.call_close(child);
                    return;
                }
                _ => {}
            }
            if Instant::now() >= deadline {
                self.inner.kernel.dependency_timeout(child);
                let _ = self.send_to(
                    &psid, &plogical, pinstance.0, pgeneration,
                    "call.cancel", Some(self.fresh_id("req")),
                    json!({"ticket": format!("tkt-{}", child.0), "reason": "deadline-exceeded"}),
                );
                self.inner.pending.lock().unwrap().remove(&preq);
                self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "deadline-exceeded", "child deadline exceeded", None);
                self.inner.kernel.call_close(child);
                return;
            }
            match rx.recv_timeout(Duration::from_millis(CALL_POLL_MS)) {
                Ok(env) if env.ty == "call.result" => {
                    self.inner.pending.lock().unwrap().remove(&preq);
                    let status = env.body.get("status").and_then(|v| v.as_str()).unwrap_or("");
                    // Remote-sourced payloads go through egress quota on the
                    // consumer session (a small input must not smuggle a huge
                    // output past max_queued_bytes). Quota miss downgrades to
                    // a small local error — never hangs, never pins.
                    let payload_bytes = |v: &Value| serde_json::to_vec(v).map(|b| b.len()).unwrap_or(0);
                    if status == "ok" {
                        let output = env.body.get("output").cloned().unwrap_or(Value::Null);
                        let out_len = payload_bytes(&output);
                        if !self.charge_send(&job.consumer_sid, out_len) {
                            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "resource-exhausted", "session send quota exceeded", None);
                            self.inner.kernel.call_close(child);
                            return;
                        }
                        match self.inner.kernel.dependency_accept(child) {
                            Ok(()) => {
                                self.send_dep_ok(&job.consumer_sid, job.cinstance, job.cgeneration, &key, Some(output), None);
                            }
                            Err(d) => {
                                self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, d.code, &d.reason, None);
                            }
                        }
                        self.release_send(&job.consumer_sid, out_len);
                    } else {
                        // Provider business error: accepts the (revalidated)
                        // terminal preserving code/message + origin.
                        let err = env.body.get("error").cloned().unwrap_or(json!({}));
                        let code = err.get("code").and_then(|v| v.as_str()).unwrap_or("internal").to_string();
                        let msg = err.get("message").and_then(|v| v.as_str()).unwrap_or("remote error").to_string();
                        if !self.charge_send(&job.consumer_sid, payload_bytes(&err)) {
                            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "resource-exhausted", "session send quota exceeded", None);
                            self.inner.kernel.call_close(child);
                            return;
                        }
                        match self.inner.kernel.dependency_accept(child) {
                            Ok(()) => {
                                self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, &code, &msg, Some(&plogical));
                            }
                            Err(d) => {
                                self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, d.code, &d.reason, None);
                            }
                        }
                        self.release_send(&job.consumer_sid, payload_bytes(&err));
                    }
                    self.inner.kernel.call_close(child);
                    return;
                }
                Ok(env) => {
                    // `call.error` (includes lost-session outcome-unknown)
                    // or any other terminal: no false success.
                    self.inner.pending.lock().unwrap().remove(&preq);
                    let (code, msg) = if env.ty == "call.error" {
                        let err = env.body.get("error").cloned().unwrap_or_else(|| env.body.clone());
                        (
                            err.get("code").and_then(|v| v.as_str()).unwrap_or("outcome-unknown").to_string(),
                            err.get("message").and_then(|v| v.as_str()).unwrap_or("session lost").to_string(),
                        )
                    } else {
                        ("outcome-unknown".to_string(), "provider gone".to_string())
                    };
                    self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, &code, &msg, None);
                    self.inner.kernel.call_cancel(child, "provider-failed");
                    self.inner.kernel.call_close(child);
                    return;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.inner.pending.lock().unwrap().remove(&preq);
                    self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "outcome-unknown", "provider gone", None);
                    self.inner.kernel.call_cancel(child, "provider-gone");
                    self.inner.kernel.call_close(child);
                    return;
                }
            }
        }
    }

    /// Removes an open's pending marker (pre-dispatch denial).
    fn unpend_dep(&self, sid: &str, req: &str) {
        if let Some(s) = self.inner.sessions.lock().unwrap().get(sid) {
            s.dep_sync.lock().unwrap().pending.remove(req);
        }
    }

    /// Stable operation id for a remote leg (R08 dedup/query scope).
    /// Composite unambiguous identity: domain, controller boot epoch,
    /// consumer (logical, instance, generation), parent ticket, binding,
    /// invocation request, input length and SHA-256 over all of the above
    /// plus the canonical input bytes. The boot epoch (per-boot entropy)
    /// separates restarts: parent tickets, bindings and component request
    /// counters may all repeat after a restart, but the epoch never does
    /// (up to 64-bit collision), so a new call can never replay an old
    /// ledger entry. The request id is the within-boot invocation
    /// identity: two intentional equal calls under one parent carry
    /// different request ids (different operations, both execute); a wire
    /// retransmit of one open reuses its request id (same operation,
    /// replayed by the gate dedup, never a second ticket). Equal ids mean
    /// equal logical invocations (retry replays); divergent content under
    /// one id is a second line of defense at the executor
    /// (`operation-id-conflict`), never silent reuse.
    /// Collision policy: ids collide only on SHA-256 collision; the
    /// executor still refuses divergent content under a reused id, so a
    /// collision can deny but never cross-wire two different calls.
    /// Over-long names fall back to a deterministic `op-<sha256>` over
    /// the same unambiguous preimage (128-char wire cap).
    fn remote_operation_id(&self, job: &DepJob) -> String {
        use sha2::{Digest, Sha256};
        let input = serde_json::to_vec(&job.input).unwrap_or_default();
        let epoch_hex = format!("{:016x}", self.inner.kernel.epoch());
        let mut h = Sha256::new();
        h.update(epoch_hex.as_bytes());
        h.update([0u8]);
        h.update(job.consumer_logical.as_bytes());
        h.update([0u8]);
        h.update(job.cinstance.to_be_bytes());
        h.update(job.cgeneration.to_be_bytes());
        h.update(job.parent.0.to_be_bytes());
        h.update(job.binding.as_bytes());
        h.update([0u8]);
        h.update(job.request_id.as_bytes());
        h.update([0u8]);
        h.update(&input);
        let digest: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
        let domain = self.inner.policy.domain.clone();
        let scope = if domain.is_empty() {
            format!(
                "{}:{}:{}:{}:{}:{}:{}",
                epoch_hex,
                job.consumer_logical,
                job.cinstance,
                job.cgeneration,
                job.parent.0,
                job.binding,
                job.request_id
            )
        } else {
            format!(
                "{}:{}:{}:{}:{}:{}:{}:{}",
                domain,
                epoch_hex,
                job.consumer_logical,
                job.cinstance,
                job.cgeneration,
                job.parent.0,
                job.binding,
                job.request_id
            )
        };
        let full = format!("{scope}:{}:{digest}", input.len());
        if full.len() <= 128 {
            return full;
        }
        let fallback: String = Sha256::digest(full.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
        format!("op-{fallback}")
    }

    /// Remote-leg dispatch (M7 route A). Mirrors the local terminal
    /// translation (quota → accept → answer → close) with the provider
    /// interaction owned by the route transport. Runs on the same
    /// quota-bounded worker; every outcome closes the kernel ticket.
    /// `_budget` holds the caller's input charge across the remote leg.
    fn run_dep_open_remote(
        &self,
        job: DepJob,
        key: String,
        child: TicketId,
        peer: String,
        _budget: SendBudget,
    ) {
        use matrix_core::TicketState;
        let operation_id = self.remote_operation_id(&job);
        // Correlate ticket ↔ (peer, operation) for cancel forwarding.
        self.inner.remote_ops.lock().unwrap().insert(child, (peer.clone(), operation_id.clone()));

        // The leg just mapped: drain chunks the component sent while it
        // dispatched (arrival order), so skew either way converges.
        self.drain_parked_for_session(&job.consumer_sid);
        let done = |host: &Self| {
            host.inner.remote_ops.lock().unwrap().remove(&child);
            host.inner.kernel.call_close(child);
        };
        // Route lookup (fail closed: absent/removed route never goes local).
        let transport = self.inner.remote_transport.lock().unwrap().clone();
        let Some(transport) = transport else {
            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "outcome-unknown", "no route to executor", None);
            self.inner.kernel.call_cancel(child, "no-route");
            done(self);
            return;
        };
        // Executor lease comes from the route (owned by the manager).
        // The host never mints or caches it: the transport fills it in.
        let grant_rev = self.inner.kernel.calls.get(child).and_then(|r| {
            r.dep.as_ref().map(|d| d.grant_rev)
        }).unwrap_or(0);
        let deadline = self
            .inner
            .kernel
            .calls
            .get(child)
            .and_then(|t| t.drain_until)
            .unwrap_or_else(|| Instant::now() + Duration::from_millis(job.timeout_ms.max(1)));
        let remaining_ms = deadline.saturating_duration_since(Instant::now()).as_millis().max(1) as u64;
        let cap = self.inner.kernel.calls.get(child)
            .map(|r| r.cap.clone())
            .unwrap_or_default();
        let open = RemoteCallOpen {
            peer: peer.clone(),
            consumer_logical: job.consumer_logical.clone(),
            consumer_instance: job.cinstance,
            consumer_generation: job.cgeneration,
            parent_ticket: job.parent.0,
            domain: self.inner.policy.domain.clone(),
            binding_id: job.binding.clone(),
            cap,
            input: job.input.clone(),
            timeout_ms: remaining_ms,
            budget_ms: remaining_ms,
            lease: String::new(),
            grant_rev,
            operation_id: operation_id.clone(),
        };
        // Blocking transport call on an inner thread; this worker keeps
        // observing revocation, deadline and consumer liveness (same as
        // the local loop: authority is a state mark, never cooperation).
        // A bounded grace lets an in-flight terminal land after cancel.
        let transport_w = transport.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_w = cancel.clone();
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("matrix-dep-remote".into())
            .spawn(move || {
                let _ = tx.send(transport_w.call_open(open, &cancel_w));
            })
            .ok();
        loop {
            if !self.inner.sessions.lock().unwrap().contains_key(&job.consumer_sid) {
                cancel.store(true, Ordering::SeqCst);
                transport.call_cancel(&peer, &operation_id);
                let _ = rx.recv_timeout(Duration::from_millis(500));
                self.inner.kernel.call_cancel(child, "session-lost");
                self.end_remote_streams_of(&peer, &operation_id);
                done(self);
                return;
            }
            match self.inner.kernel.calls.get(child).map(|t| t.state) {
                Some(TicketState::Cancelled) => {
                    cancel.store(true, Ordering::SeqCst);
                    transport.call_cancel(&peer, &operation_id);
                    let _ = rx.recv_timeout(Duration::from_millis(500));
                    self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "cancelled", "child revoked", None);
                    self.end_remote_streams_of(&peer, &operation_id);
                    done(self);
                    return;
                }
                Some(TicketState::Expired) => {
                    cancel.store(true, Ordering::SeqCst);
                    transport.call_cancel(&peer, &operation_id);
                    let _ = rx.recv_timeout(Duration::from_millis(500));
                    self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "deadline-exceeded", "child expired", None);
                    self.end_remote_streams_of(&peer, &operation_id);
                    done(self);
                    return;
                }
                _ => {}
            }
            if Instant::now() >= deadline {
                self.inner.kernel.dependency_timeout(child);
                cancel.store(true, Ordering::SeqCst);
                transport.call_cancel(&peer, &operation_id);
                let _ = rx.recv_timeout(Duration::from_millis(500));
                self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "deadline-exceeded", "child deadline exceeded", None);
                self.end_remote_streams_of(&peer, &operation_id);
                done(self);
                return;
            }
            match rx.recv_timeout(Duration::from_millis(CALL_POLL_MS)) {
                Ok(RemoteCallTerminal::Ok(output)) => {
                    let out_len = serde_json::to_vec(&output).map(|b| b.len()).unwrap_or(0);
                    if !self.charge_send(&job.consumer_sid, out_len) {
                        self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "resource-exhausted", "session send quota exceeded", None);
                        done(self);
                        return;
                    }
                    match self.inner.kernel.dependency_accept(child) {
                        Ok(()) => {
                            self.send_dep_ok(&job.consumer_sid, job.cinstance, job.cgeneration, &key, Some(output), None);
                        }
                        Err(d) => {
                            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, d.code, &d.reason, None);
                        }
                    }
                    self.release_send(&job.consumer_sid, out_len);
                    done(self);
                    return;
                }
                Ok(RemoteCallTerminal::Err { code, message }) => {
                    let err_len = code.len() + message.len();
                    if !self.charge_send(&job.consumer_sid, err_len) {
                        self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "resource-exhausted", "session send quota exceeded", None);
                        done(self);
                        return;
                    }
                    match self.inner.kernel.dependency_accept(child) {
                        Ok(()) => {
                            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, &code, &message, Some(&peer));
                        }
                        Err(d) => {
                            self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, d.code, &d.reason, None);
                        }
                    }
                    self.release_send(&job.consumer_sid, err_len);
                    done(self);
                    return;
                }
                Ok(RemoteCallTerminal::Failed { code, message }) => {
                    self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, &code, &message, None);
                    self.inner.kernel.call_cancel(child, "provider-failed");
                    done(self);
                    return;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.send_dep_result(&job.consumer_sid, job.cinstance, job.cgeneration, &key, "outcome-unknown", "route worker lost", None);
                    self.inner.kernel.call_cancel(child, "provider-gone");
                    done(self);
                    return;
                }
            }
        }
    }

    /// Credit-based streams (M2.3): the receiving host grants bytes;
    /// data past the grant ends the stream with an error, session survives.
    /// O primeiro `stream.data` de um id cria o stream com a janela inicial
    /// e recebe o `stream.credit` correspondente.
    fn on_stream_frame(&self, sid: &str, env: &Envelope) {
        let stream_id = env.body.get("stream_id").and_then(|v| v.as_str()).unwrap_or("");
        if stream_id.is_empty() || stream_id.len() > 256 {
            return;
        }
        // Remote-bound streams (M7): component frames whose id is owned
        // by a remote leg relay over the route instead of the local
        // table (separate namespace: no collision class either way).
        if self.relay_stream_frame(sid, env, stream_id) {
            return;
        }
        // Unclaimed data: bind when exactly one in-flight remote leg owns
        // this session (call-associated streams: the component picks an
        // id under `remote/`, the host supplies route + operation),
        // draining chunks parked while the leg dispatched, then relay.
        // Otherwise park (bounded, expiring) and fall through to local
        // accounting: zero or ambiguous legs stay local, fail closed.
        // Ids outside `remote/` never auto-associate (local telemetry
        // must not cross hosts); explicit binds still claim any id above.
        if env.ty.as_str() == "stream.data" {
            if is_call_stream_id(stream_id) && self.bind_and_drain(sid, stream_id) {
                self.relay_stream_frame(sid, env, stream_id);
                return;
            }
            if is_call_stream_id(stream_id) {
                self.park_chunk(sid, env, stream_id);
            }
        }
        if env.ty.as_str() == "stream.end"
            && is_call_stream_id(stream_id)
            && self.bind_and_drain(sid, stream_id)
        {
            self.clear_parked(sid, stream_id);
            self.relay_stream_frame(sid, env, stream_id);
            return;
        }
        match env.ty.as_str() {
            "stream.data" => {
                let seq = env.body.get("seq").and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok());
                let payload_len = env.body.get("payload").and_then(|v| v.as_str())
                    .map(|s| s.len() as u64).unwrap_or(0);
                let Some(seq) = seq else { return };
                let end = {
                    let sessions = self.inner.sessions.lock().unwrap();
                    let Some(s) = sessions.get(sid) else { return };
                    let mut streams = s.streams.lock().unwrap();
                    let st = streams.entry(stream_id.to_string()).or_insert(StreamState {
                        next_seq: 0,
                        granted: STREAM_INITIAL_GRANT,
                        received: 0,
                        ended: false,
                    });
                    if st.ended {
                        // Tombstone: latecomers ignored, never recreated nor counted.
                        None
                    } else {
                        let fresh = st.next_seq == 0;
                        if seq != st.next_seq {
                            // Duplicate/reorder: ignores without re-executing.
                            None
                        } else {
                            st.next_seq += 1;
                            st.received += payload_len;
                            let over = st.received > st.granted;
                            if over {
                                st.ended = true;
                            }
                            Some((fresh, over, st.received, st.granted))
                        }
                    }
                };
                let Some((fresh, over, received, granted)) = end else { return };
                if fresh {
                    self.send_stream_credit(sid, stream_id, STREAM_INITIAL_GRANT);
                }
                if over {
                    self.end_stream(sid, stream_id, "error", received, granted);
                }
                // Executor-side tap (M7 route): forwards accepted chunks over
                // the remote session best-effort. Never blocks accounting or
                // other sessions; unset on controller and single-host rigs.
                // Over-credit chunks end the leg and are NOT forwarded.
                if !over {
                    if let Some(tap) = self.inner.stream_tap.lock().unwrap().clone() {
                        // seq was validated above (`let ... else return`).
                        let seq = env.body.get("seq").and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                        let payload = env.body.get("payload").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        tap(sid, stream_id, seq, &payload);
                    }
                }
            }
            "stream.end" => {
                // A plugin ending its own stream: forgets without error.
                if let Some(s) = self.inner.sessions.lock().unwrap().get(sid) {
                    s.streams.lock().unwrap().remove(stream_id);
                }
                self.clear_parked(sid, stream_id);
            }
            // Host-bound plugin credit is unused in the local profile (the host
            // sends streams in M2); ignored without error.
            _ => {}
        }
    }

    fn send_stream_credit(&self, sid: &str, stream_id: &str, bytes: u64) {
        let meta = {
            let sessions = self.inner.sessions.lock().unwrap();
            sessions.get(sid).map(|s| (s.id.clone(), s.instance, s.generation))
        };
        let Some((id, instance, generation)) = meta else { return };
        let msg = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "stream.credit",
            "message_id": self.fresh_id("msg"),
            "session_id": id,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "body": {"stream_id": stream_id, "bytes": bytes},
        });
        let raw = serde_json::to_vec(&msg).unwrap_or_default();
        if let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) {
            if let Some((writer, _, _, _)) = self.session_sink(sid) {
                let _ = Self::write_frame(&writer, &f);
            }
        }
    }

    fn end_stream(&self, sid: &str, stream_id: &str, status: &str, received: u64, granted: u64) {
        let meta = {
            let sessions = self.inner.sessions.lock().unwrap();
            sessions.get(sid).map(|s| (s.id.clone(), s.instance, s.generation))
        };
        let Some((id, instance, generation)) = meta else { return };
        // Keeps the tombstone (late in-flight data ignored, never recreated).
        let msg = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "stream.end",
            "message_id": self.fresh_id("msg"),
            "session_id": id,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "body": {"stream_id": stream_id, "status": status,
                     "reason": format!("over credit: received {} granted {}", received, granted)},
        });
        let raw = serde_json::to_vec(&msg).unwrap_or_default();
        if let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) {
            if let Some((writer, _, _, _)) = self.session_sink(sid) {
                let _ = Self::write_frame(&writer, &f);
            }
        }
    }

    fn write_value(&self, stream: &mut UnixStream, v: &Value, max_frame: usize) -> Result<(), String> {
        // The executable schema also covers what we send.
        let raw = serde_json::to_vec(v).map_err(|e| e.to_string())?;
        if raw.len() > max_frame {
            return Err("frame above negotiated max".to_string());
        }
        let frame = encode(&raw, max_frame).map_err(|e| e.to_string())?;
        use std::io::Write;
        stream.write_all(&frame).map_err(|e| e.to_string())?;
        stream.flush().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn write_reject(&self, stream: &mut UnixStream, in_reply_to: &str, code: &str, reason: &str) -> Result<(), String> {
        self.write_value(
            stream,
            &json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "reject",
                "message_id": in_reply_to,
                "body": {"code": code, "reason": reason},
            }),
            DEFAULT_MAX_FRAME,
        )
    }

    // ---- test inspection ----

    pub fn has_session(&self, logical: &str, instance: u64) -> bool {
        self.inner.by_instance.lock().unwrap().contains_key(&(logical.to_string(), instance))
    }

    /// Installs the M7 remote-leg transport (route manager). Replacing it
    /// never disturbs local dispatch; removing it fails remote legs closed.
    pub fn set_remote_transport(&self, t: Option<Arc<dyn RemoteTransport>>) {
        let clearing = t.is_none();
        *self.inner.remote_transport.lock().unwrap() = t;
        if clearing {
            // Route teardown: forget in-flight remote correlation (tickets
            // keep failing closed at terminal revalidation).
            self.inner.remote_ops.lock().unwrap().clear();
        }
    }

    /// Stable operation correlation for a remote leg (cancel/diagnosis).
    pub fn remote_op_of(&self, child: TicketId) -> Option<(String, String)> {
        self.inner.remote_ops.lock().unwrap().get(&child).cloned()
    }

    /// Reverse lookup: in-flight ticket for one route's stable operation.
    /// Used to settle legs the executor reports revoked at reconcile
    /// (before any new bindings publish). Unknown ids return None.
    pub fn ticket_for_remote_op(&self, peer: &str, operation: &str) -> Option<TicketId> {
        self.inner
            .remote_ops
            .lock()
            .unwrap()
            .iter()
            .find_map(|(t, (p, op))| (p == peer && op == operation).then_some(*t))
    }

    /// Delivers a remotely-originated event to subscribed local sessions
    /// (M7 controller side). Same checks as local fan-out: topic match at
    /// bind, per-subscriber egress quota, current-activation stamping
    /// (stale generations filter on the component side). Best effort.
    /// Returns per-session outcomes for diagnosis (no payloads).
    pub fn deliver_remote_event(&self, topic: &str, payload: &Value) -> Vec<(String, bool)> {
        if topic.is_empty() || topic.len() > 256 {
            return vec![];
        }
        self.deliver_local_event_counted(topic, payload)
    }

    fn deliver_local_event(&self, topic: &str, payload: &Value) {
        let _ = self.deliver_local_event_counted(topic, payload);
    }

    /// Same fan-out, returning per-session delivery outcomes
    /// (`(session_id, delivered)`; `false` = quota-exhausted drop).
    /// Outcomes carry no payloads (diagnosis only).
    fn deliver_local_event_counted(&self, topic: &str, payload: &Value) -> Vec<(String, bool)> {
        let targets: Vec<(String, u64, u64)> = {
            self.inner
                .sessions
                .lock()
                .unwrap()
                .values()
                .filter(|s| s.topics.iter().any(|t| t == topic))
                .map(|s| (s.id.clone(), s.instance, s.generation))
                .collect()
        };
        // Egress quota is charged on the full payload once per subscriber.
        let bytes = serde_json::to_vec(payload).map(|b| b.len()).unwrap_or(0);
        let mut out = Vec::with_capacity(targets.len());
        for (sid, instance, generation) in targets {
            // Egress quota per subscriber (best effort: drops on exhaustion,
            // never pins, never stalls the emitter or other sessions).
            if !self.charge_send(&sid, bytes) {
                out.push((sid, false));
                continue;
            }
            let v = json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "event.deliver",
                "message_id": self.fresh_id("msg"),
                "session_id": sid,
                "instance_id": instance.to_string(),
                "generation": generation.to_string(),
                "body": {"topic": topic, "payload": payload},
            });
            let raw = serde_json::to_vec(&v).unwrap_or_default();
            let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) else {
                self.release_send(&sid, bytes);
                out.push((sid, false));
                continue;
            };
            if let Some((writer, _, _, _)) = self.session_sink(&sid) {
                let _ = Self::write_frame(&writer, &f);
            }
            self.release_send(&sid, bytes);
            out.push((sid, true));
        }
        out
    }

    /// Installs the executor-side event tap (M7 route): forwards local
    /// emissions to subscribed remote sessions. Replacing/removing it
    /// never affects local delivery.
    pub fn set_event_tap(&self, t: Option<Arc<dyn Fn(&str, &Value) + Send + Sync>>) {
        *self.inner.event_tap.lock().unwrap() = t;
    }

    /// Installs the executor-side stream tap (M7 route): observes
    /// component stream chunks after local accounting, forwarding them
    /// over the remote session. Replacing/removing it never affects
    /// local accounting or controller-side relay.
    pub fn set_stream_tap(&self, t: Option<Arc<dyn Fn(&str, &str, u64, &str) + Send + Sync>>) {
        *self.inner.stream_tap.lock().unwrap() = t;
    }

    pub fn session_count(&self) -> usize {
        self.inner.sessions.lock().unwrap().len()
    }

    pub fn child_running(&self, logical: &str) -> bool {
        self.inner.children.lock().unwrap().contains_key(logical)
    }

    /// Forwarded calls waiting per session (M2.3 cap).
    pub fn in_flight_for(&self, logical: &str, instance: u64) -> usize {
        let by = self.inner.by_instance.lock().unwrap();
        let Some(sid) = by.get(&(logical.to_string(), instance)) else { return 0 };
        self.inner.sessions.lock().unwrap().get(sid)
            .map(|s| *s.in_flight.lock().unwrap())
            .unwrap_or(0)
    }

    /// Streams with credit tracked per session (M2.3; open ones only).
    pub fn stream_count_for(&self, logical: &str, instance: u64) -> usize {
        let by = self.inner.by_instance.lock().unwrap();
        let Some(sid) = by.get(&(logical.to_string(), instance)) else { return 0 };
        self.inner.sessions.lock().unwrap().get(sid)
            .map(|s| s.streams.lock().unwrap().values().filter(|st| !st.ended).count())
            .unwrap_or(0)
    }

    /// Remote-bound stream legs per session (M7; open ones only, with
    /// executor peer + operation for audit correlation).
    pub fn remote_streams_for(&self, logical: &str, instance: u64) -> Vec<(String, String, String)> {
        let rstreams = {
            let by = self.inner.by_instance.lock().unwrap();
            let Some(sid) = by.get(&(logical.to_string(), instance)).cloned() else { return vec![] };
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(&sid) else { return vec![] };
            s.rstreams.clone()
        };
        let guard = rstreams.lock().unwrap();
        let out: Vec<(String, String, String)> = guard
            .iter()
            .filter(|(_, st)| !st.ended)
            .map(|(id, st)| (id.clone(), st.peer.clone(), st.operation.clone()))
            .collect();
        out
    }

    /// Streams ended for excess still held as tombstones (M2.3).
    pub fn ended_stream_count_for(&self, logical: &str, instance: u64) -> usize {
        let by = self.inner.by_instance.lock().unwrap();
        let Some(sid) = by.get(&(logical.to_string(), instance)) else { return 0 };
        self.inner.sessions.lock().unwrap().get(sid)
            .map(|s| s.streams.lock().unwrap().values().filter(|st| st.ended).count())
            .unwrap_or(0)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let mut children = self.children.lock().unwrap();
        for (_, entry) in children.drain() {
            if let Ok(mut c) = entry.child.lock() {
                let _ = matrix_guard::kill_group(&mut c);
            }
        }
        let _ = std::fs::remove_file(&self.sock_path);
    }
}

/// Remote stream relay (M7). Component frames for a stream id owned
/// by a remote leg go over the route; executor-originated chunks for a
/// bound leg are delivered to the component session like local streams
/// (credit + tombstones). Quotas: relayed bytes charge the consumer
/// session quota both ways.
impl Host {
    /// Binds a component stream id to a remote leg (called by the route
    /// transport when a `stream.open` handshake completes for one of
    /// this host's sessions). Unknown sessions refuse (fail closed).
    pub fn bind_remote_stream(
        &self,
        sid: &str,
        stream_id: &str,
        peer: &str,
        operation: &str,
        initial_credit: u64,
    ) -> Result<(), String> {
        if stream_id.is_empty() || stream_id.len() > 256 {
            return Err("invalid-message".into());
        }
        let rstreams = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return Err("unknown session".into()) };
            s.rstreams.clone()
        };
        let mut rst = rstreams.lock().unwrap();
        if rst.contains_key(stream_id) {
            return Err("duplicate-request".into());
        }
        if rst.len() >= MAX_REMOTE_STREAMS_PER_SESSION {
            return Err("resource-exhausted".into());
        }
        rst.insert(
            stream_id.to_string(),
            RemoteStream {
                peer: peer.to_string(),
                operation: operation.to_string(),
                send_next: 0,
                send_granted: initial_credit,
                send_sent: 0,
                recv_next: 0,
                recv_granted: initial_credit,
                recv_received: 0,
                ended: false,
            },
        );
        Ok(())
    }

    /// Auto-binds an unclaimed component stream to the session's single
    /// in-flight remote leg (call-associated streams, M7 R02): the
    /// component only picks its id; the host supplies route + operation
    /// from the leg admitted for this session. Zero legs → local sink
    /// (existing behavior); ambiguous (several legs) → local sink, fail
    /// closed (never guess the operation). Returns true when the id is
    /// bound afterwards (freshly or concurrently).
    /// This session's single in-flight remote leg, if exactly one: the
    /// route + operation an unclaimed stream id associates with. Zero or
    /// several legs → None (stay local, fail closed, never guess).
    fn single_remote_leg(&self, sid: &str) -> Option<(String, String)> {
        let sessions = self.inner.sessions.lock().unwrap();
        let s = sessions.get(sid)?;
        let reqs = s.dep_requests.lock().unwrap();
        let ops = self.inner.remote_ops.lock().unwrap();
        let mut legs = reqs.values().filter_map(|t| ops.get(t).cloned());
        let first = legs.next()?;
        if legs.next().is_some() {
            return None;
        }
        Some(first)
    }

    /// Binds an unclaimed id to the session's single leg (if any) and
    /// relays chunks parked while the leg dispatched, in arrival order.
    /// Returns true when the id is bound afterwards.
    fn bind_and_drain(&self, sid: &str, stream_id: &str) -> bool {
        let Some((peer, operation)) = self.single_remote_leg(sid) else { return false };
        match self.bind_remote_stream(sid, stream_id, &peer, &operation, STREAM_INITIAL_GRANT) {
            Ok(()) | Err(_) => {}
        }
        // Drain parked chunks in arrival order (relay rechecks seq/duples;
        // stale ones are harmless no-ops). Whether the bind is fresh or
        // concurrent, reaching here means bound-or-full; full is handled
        // by falling through only when the table refuses a fresh id —
        // checked below via a second lookup.
        let parked = self.take_parked(sid, stream_id);
        if !parked.is_empty() {
            for p in &parked {
                let env = matrix_proto::Envelope {
                    ty: "stream.data".to_string(),
                    message_id: format!("parked-{sid}-{}-{}", p.seq, p.payload.len()),
                    session_id: Some(sid.to_string()),
                    instance_id: None,
                    generation: None,
                    request_id: None,
                    body: serde_json::json!({"stream_id": p.stream_id, "seq": p.seq.to_string(), "payload": p.payload}),
                };
                self.relay_stream_frame(sid, &env, &p.stream_id);
            }
        }
        // Confirm the id is actually bound (a full table refuses fresh ids).
        let sessions = self.inner.sessions.lock().unwrap();
        sessions.get(sid).is_some_and(|s| s.rstreams.lock().unwrap().contains_key(stream_id))
    }

    /// Parks one unclaimed chunk for a dispatching leg (bounded, expiring).
    /// Unparsable frames are not parked (the local arm ignores them too).
    fn park_chunk(&self, sid: &str, env: &Envelope, stream_id: &str) {
        let seq = env.body.get("seq").and_then(|v| v.as_str()).and_then(|s| s.parse::<u64>().ok());
        let payload = env.body.get("payload").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let Some(seq) = seq else { return };
        let sessions = self.inner.sessions.lock().unwrap();
        let Some(s) = sessions.get(sid) else { return };
        let mut parked = s.parked.lock().unwrap();
        parked.retain(|p| p.at.elapsed() < PARKED_TTL);
        if parked.len() >= MAX_PARKED_PER_SESSION {
            parked.remove(0);
        }
        parked.push(ParkedChunk { stream_id: stream_id.to_string(), seq, payload, at: Instant::now() });
    }

    /// Takes (and clears) parked chunks for one id, in arrival order.
    fn take_parked(&self, sid: &str, stream_id: &str) -> Vec<ParkedChunk> {
        let sessions = self.inner.sessions.lock().unwrap();
        let Some(s) = sessions.get(sid) else { return vec![] };
        let mut parked = s.parked.lock().unwrap();
        parked.retain(|p| p.at.elapsed() < PARKED_TTL);
        let (hit, rest): (Vec<_>, Vec<_>) = parked.drain(..).partition(|p| p.stream_id == stream_id);
        *parked = rest;
        hit
    }

    /// Forgets parked chunks for one id (terminal paths).
    fn clear_parked(&self, sid: &str, stream_id: &str) {
        let sessions = self.inner.sessions.lock().unwrap();
        if let Some(s) = sessions.get(sid) {
            s.parked.lock().unwrap().retain(|p| p.stream_id != stream_id);
        }
    }

    /// Drains parked chunks for a session whose leg just mapped (called
    /// after `remote_ops` insert at dispatch, so skew either way converges
    /// without timers or retries).
    fn drain_parked_for_session(&self, sid: &str) {
        let ids: Vec<String> = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return };
            let parked = s.parked.lock().unwrap();
            let mut ids: Vec<String> = parked.iter().map(|p| p.stream_id.clone()).collect();
            ids.sort();
            ids.dedup();
            ids
        };
        for id in ids {
            // Parked chunks relay inside (arrival order); nothing further
            // here (no live envelope to forward).
            let _ = self.bind_and_drain(sid, &id);
        }
    }

    /// Delivers an executor-side chunk into the exact provider activation
    /// named by `expected` (M7 up-direction termination): frames
    /// `stream.data` to the session registered for
    /// `(logical, instance)`, but only if it still stamps the
    /// expected generation. The validated [`matrix_core::InstanceRef`]
    /// travels from the route's ownership check to this selection with
    /// no re-resolution by bare name: a replaced provider (new
    /// generation) refuses, a withdrawn session refuses (or still
    /// delivers to the lingering old session, linearizing before the
    /// withdraw) — never redirects to the new activation. Best effort:
    /// quota miss drops downstream (accounting already bounded the leg).
    /// Returns false when nothing was addressed.
    pub fn inject_provider_chunk_owned(
        &self,
        expected: &matrix_core::InstanceRef,
        stream_id: &str,
        seq: u64,
        payload: &str,
    ) -> bool {
        if expected.logical.is_empty() || stream_id.is_empty() {
            return false;
        }
        // One atomic selection under both locks: whatever is registered
        // for (logical, instance) with the expected generation is the
        // destination — or nothing is. Generations never mutate in place
        // and session ids never repeat, so no interleaving can steer this
        // to a newer activation.
        let dest = {
            let by = self.inner.by_instance.lock().unwrap();
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(sid) = by.get(&(expected.logical.clone(), expected.instance)).cloned() else {
                return false;
            };
            let Some(s) = sessions.get(&sid) else { return false };
            if s.instance != expected.instance || s.generation != expected.generation {
                return false;
            }
            (sid, s.instance, s.generation)
        };
        self.send_stream_chunk(&dest.0, dest.1, dest.2, stream_id, seq, payload);
        true
    }
    /// Executor-granted credit for a bound leg (from `stream.credit`).
    /// Widens the outbound window only; the inbound window is ours to
    /// grant (currently the bind-time window). Unknown/ended legs ignore
    /// (tombstones never resurrect).
    pub fn credit_remote_stream(&self, sid: &str, stream_id: &str, credit: u64) {
        let rstreams: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, RemoteStream>>> = {
            let sessions = self.inner.sessions.lock().unwrap();
            match sessions.get(sid) {
                Some(s) => s.rstreams.clone(),
                None => return,
            }
        };
        let mut g = rstreams.lock().unwrap();
        if let Some(st) = g.get_mut(stream_id) {
            if !st.ended {
                st.send_granted = st.send_granted.saturating_add(credit);
            }
        }
    }

    /// Ends a bound leg locally (terminal or abuse). Tombstone retained.
    pub fn end_remote_stream(&self, sid: &str, stream_id: &str) {
        let rstreams: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, RemoteStream>>> = {
            let sessions = self.inner.sessions.lock().unwrap();
            match sessions.get(sid) {
                Some(s) => s.rstreams.clone(),
                None => return,
            }
        };
        rstreams.lock().unwrap().get_mut(stream_id).map(|st| {
            st.ended = true;
        });
    }

    /// Global stream_id lookup for the controller route (M7 R02): the
    /// remote profile keeps stream_ids unique per owner+operation, so a
    /// controller-side id identifies one bound session. Returns false
    /// when unbound (caller drops). Iterates sessions without holding
    /// the global lock across delivery (bounded: session count is capped
    /// by the host's child table).
    pub fn deliver_remote_chunk_any(&self, stream_id: &str, seq: u64, payload: &str) -> bool {
        let sid = {
            let sessions = self.inner.sessions.lock().unwrap();
            sessions
                .iter()
                .find_map(|(id, s)| {
                    s.rstreams
                        .lock()
                        .unwrap()
                        .contains_key(stream_id)
                        .then(|| id.clone())
                })
        };
        let Some(sid) = sid else { return false };
        self.deliver_remote_chunk(&sid, stream_id, seq, payload)
    }

    /// Global credit grant by stream_id (see above for uniqueness).
    /// Unknown/ended legs ignore.
    pub fn credit_remote_stream_any(&self, stream_id: &str, credit: u64) {
        let target = {
            let sessions = self.inner.sessions.lock().unwrap();
            sessions.iter().find_map(|(id, s)| {
                s.rstreams
                    .lock()
                    .unwrap()
                    .contains_key(stream_id)
                    .then(|| id.clone())
            })
        };
        if let Some(sid) = target {
            self.credit_remote_stream(&sid, stream_id, credit);
        }
    }

    /// Ends every bound leg of one route operation on every session
    /// (revoked/cancelled legs release their windows; tombstones stay).
    /// Returns how many legs were ended. Surviving operations keep theirs:
    /// streams outlive successful calls by design.
    pub fn end_remote_streams_of(&self, peer: &str, operation: &str) -> usize {
        if peer.is_empty() || operation.is_empty() {
            return 0;
        }
        let tables: Vec<std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, RemoteStream>>>> = {
            self.inner
                .sessions
                .lock()
                .unwrap()
                .values()
                .map(|s| s.rstreams.clone())
                .collect()
        };
        let mut n = 0;
        for t in tables {
            for st in t.lock().unwrap().values_mut() {
                if !st.ended && st.peer == peer && st.operation == operation {
                    st.ended = true;
                    n += 1;
                }
            }
        }
        n
    }

    /// Session id currently bound to a logical activation, if any
    /// (diagnosis and tests; routing decisions never use it).
    pub fn session_id_of(&self, logical: &str, instance: u64) -> Option<String> {
        self.inner
            .by_instance
            .lock()
            .unwrap()
            .get(&(logical.to_string(), instance))
            .cloned()
    }

    /// Global terminal by stream_id (tombstone retained).
    pub fn end_remote_stream_any(&self, stream_id: &str) {
        let target = {
            let sessions = self.inner.sessions.lock().unwrap();
            sessions.iter().find_map(|(id, s)| {
                s.rstreams
                    .lock()
                    .unwrap()
                    .contains_key(stream_id)
                    .then(|| id.clone())
            })
        };
        if let Some(sid) = target {
            self.end_remote_stream(&sid, stream_id);
        }
    }

    /// Forwards an executor-originated chunk to the bound component
    /// session, enforcing the local credit window (same as local
    /// streams: over-credit ends the leg with an error, never panics).
    /// Returns false when unbound/ended (caller drops + counts).
    pub fn deliver_remote_chunk(
        &self,
        sid: &str,
        stream_id: &str,
        seq: u64,
        payload: &str,
    ) -> bool {
        enum Out {
            Send { instance: u64, generation: u64 },
            Over,
        }
        let (rstreams, instance, generation) = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return false };
            (s.rstreams.clone(), s.instance, s.generation)
        };
        let out = {
            let mut rst = rstreams.lock().unwrap();
            let Some(st) = rst.get_mut(stream_id) else { return false };
            if st.ended || seq != st.recv_next {
                // Tombstone or duplicate/reorder: ignored, never re-executed.
                return false;
            }
            st.recv_next += 1;
            st.recv_received += payload.len() as u64;
            if st.recv_received > st.recv_granted {
                st.ended = true;
                Out::Over
            } else {
                Out::Send { instance, generation }
            }
        };
        match out {
            Out::Send { instance, generation } => {
                self.send_stream_chunk(sid, instance, generation, stream_id, seq, payload);
                true
            }
            Out::Over => {
                self.end_stream(sid, stream_id, "error", 0, 0);
                false
            }
        }
    }

    fn send_stream_chunk(
        &self,
        sid: &str,
        instance: u64,
        generation: u64,
        stream_id: &str,
        seq: u64,
        payload: &str,
    ) {
        // Component-bound bytes go through the session egress quota
        // (bounded fan-out like events); quota miss drops + counts.
        let bytes = payload.len();
        if !self.charge_send(sid, bytes) {
            return;
        }
        let v = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "stream.data",
            "message_id": self.fresh_id("msg"),
            "session_id": sid,
            "instance_id": instance.to_string(),
            "generation": generation.to_string(),
            "body": {"stream_id": stream_id, "seq": seq.to_string(), "payload": payload},
        });
        let raw = serde_json::to_vec(&v).unwrap_or_default();
        if let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) {
            if let Some((writer, _, _, _)) = self.session_sink(sid) {
                let _ = Self::write_frame(&writer, &f);
            }
        }
        self.release_send(sid, bytes);
    }

    /// Relays a component stream frame over its remote leg. Returns true
    /// when claimed (known remote id, terminal or relayed).
    fn relay_stream_frame(&self, sid: &str, env: &Envelope, stream_id: &str) -> bool {
        enum Act {
            Data { peer: String, operation: String, seq: u64, payload_len: usize, over: bool },
            End { peer: String },
            Ignore,
        }
        let rstreams = {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return false };
            s.rstreams.clone()
        };
        let act = {
            let mut rst = rstreams.lock().unwrap();
            let Some(st) = rst.get_mut(stream_id) else { return false };
            match env.ty.as_str() {
                "stream.data" => {
                    if st.ended {
                        return true;
                    }
                    let seq = env.body.get("seq").and_then(|v| v.as_str())
                        .and_then(|x| x.parse::<u64>().ok());
                    let payload_len = env.body.get("payload").and_then(|v| v.as_str())
                        .map(|x| x.len()).unwrap_or(0);
                    let Some(seq) = seq else { return true };
                    if seq != st.send_next {
                        return true;
                    }
                    st.send_next += 1;
                    let over = st.send_sent.saturating_add(payload_len as u64) > st.send_granted;
                    if over {
                        st.ended = true;
                    }
                    Act::Data { peer: st.peer.clone(), operation: st.operation.clone(), seq, payload_len, over }
                }
                "stream.end" => {
                    st.ended = true;
                    Act::End { peer: st.peer.clone() }
                }
                _ => Act::Ignore,
            }
        };
        match act {
            Act::Ignore => true,
            Act::End { peer } => {
                if let Some(t) = self.inner.remote_transport.lock().unwrap().clone() {
                    t.stream_end(&peer, stream_id, "ok");
                }
                true
            }
            Act::Data { peer, operation, seq, payload_len, over } => {
                if over {
                    self.end_stream(sid, stream_id, "error", 0, 0);
                    if let Some(t) = self.inner.remote_transport.lock().unwrap().clone() {
                        t.stream_end(&peer, stream_id, "error");
                    }
                    return true;
                }
                // Egress quota first (bounded; control never charges).
                if !self.charge_send(sid, payload_len) {
                    return true;
                }
                let payload = env.body.get("payload").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let sent = rstreams.lock().unwrap().get_mut(stream_id).is_some_and(|st| {
                    st.send_sent += payload_len as u64;
                    true
                });
                if sent {
                    if let Some(t) = self.inner.remote_transport.lock().unwrap().clone() {
                        t.stream_data(&peer, &operation, stream_id, seq, &payload);
                    }
                }
                self.release_send(sid, payload_len);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {    use super::*;

    /// Global reply-queue cap (M2.3): when full it rejects fast, even
    /// sessionless; once drained, the normal path returns (a different error).
    #[test]
    fn pending_cap_sheds_load_first() {
        let dir = std::env::temp_dir().join(format!(
            "matrix-host-unit-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(dir.join("run")).unwrap();
        let journal =
            matrix_core::Journal::open(&dir.join("run/journal.jsonl"), false, false).unwrap();
        let kernel = Arc::new(Kernel::new(&dir, journal, false));
        let host = Host::attach(kernel, &dir.join("host")).expect("attach");
        // Fills the queue with fake pendings.
        {
            let mut pending = host.inner.pending.lock().unwrap();
            for i in 0..MAX_PENDING_PER_HOST {
                let (tx, _rx) = mpsc::channel();
                pending.insert(
                    format!("dummy-{}", i),
                    Pending { session: "sess-x".into(), tx, kind: PendingKind::Lifecycle },
                );
            }
        }
        let req = ForwardRequest {
            ticket: matrix_core::TicketId(1),
            cap: "x.y@1".into(),
            input: serde_json::json!({}),
            logical: "none".into(),
            instance: matrix_core::InstanceId(1),
            generation: 1,
            timeout_ms: 50,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        match host.forward_call(&req) {
            ForwardOutcome::Err { code, .. } => assert_eq!(code, "resource-exhausted"),
            other => panic!("esperava resource-exhausted, veio {:?}", other),
        }
        host.inner.pending.lock().unwrap().clear();
        match host.forward_call(&req) {
            ForwardOutcome::Failed(_) => {}
            other => panic!("sessionless, expected Gone, got {:?}", other),
        }
        host.shutdown();
    }
}
