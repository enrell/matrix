//! Rust SDK for external components (M2.4).
//!
//! Speaks `matrix.component` v0.1 with the local host: handshake, registration,
//! activation, and call serving with cooperative cancellation.
//! The kernel keeps deciding composition and lifecycle; the SDK only translates
//! the contract into idiomatic Rust (cf. `docs/SDK.md`).
//!
//! ```no_run
//! use matrix_component::{CallCtx, CallOutcome, Component, Handler};
//! use serde_json::{json, Value};
//! use std::sync::atomic::AtomicBool;
//!
//! struct Echo;
//! impl Handler for Echo {
//!     fn on_call(&self, _ctx: &CallCtx, _ticket: &str, _cap: &str, input: &Value, _cancel: &AtomicBool) -> CallOutcome {
//!         CallOutcome::Ok(json!({"echo": input}))
//!     }
//! }
//!
//! let comp = Component::connect(std::path::Path::new("/run/host.sock"), "echo").unwrap();
//! comp.serve(Echo).unwrap();
//! ```

use matrix_proto::{
    encode, parse_frame_payload, read_frame, DEFAULT_MAX_FRAME,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

/// Business call result (wire error code preserved).
#[derive(Debug, Clone)]
pub enum CallOutcome {
    Ok(Value),
    Err { code: String, message: String },
}

/// Opaque binding handle for dependency calls (M6.1).
#[derive(Debug, Clone)]
pub struct DepBinding {
    pub id: String,
    pub capability: String,
}

/// Activation resource error (M6.3).
#[derive(Debug, Clone)]
pub struct ResError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for ResError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ResError {}

/// Dependency-call error (wire code, no reinterpretation).
#[derive(Debug, Clone)]
pub struct DepError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for DepError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DepError {}

static DEP_SEQ: AtomicU64 = AtomicU64::new(1);

/// Call context: bound session + stream sending +
/// activation dependency calls (M6.1).
#[derive(Clone)]
pub struct CallCtx {
    writer: Arc<Mutex<UnixStream>>,
    pub session_id: String,
    pub instance_id: String,
    pub generation: u64,
    pub max_frame: usize,
    /// This call's ticket on the host (`tkt-N`): parent of children (M6.1).
    /// The SDK takes it from the current context; never from payload.
    pub ticket: String,
    /// Parent-call cancel (inherited; observed during the child).
    parent_cancel: Arc<AtomicBool>,
    /// Opaque bindings of this activation (delivered at activate).
    bindings: Vec<DepBinding>,
    /// Negotiated extensions (unnegotiated local attempts refuse).
    features: Vec<String>,
    /// Waits for `dependency.result` terminals by request.
    waiters: Arc<Mutex<HashMap<String, std::sync::mpsc::Sender<Result<Value, DepError>>>>>,
    /// Waits for `resource.result` by request (M6.3).
    res_waiters: Arc<Mutex<HashMap<String, std::sync::mpsc::Sender<Result<Value, ResError>>>>>,
    /// Reader thread (serve runs on it): blocking calls refuse here.
    reader_thread: std::thread::ThreadId,
    /// Bounded event queue + drop counter (M6.3).
    event_queue: Arc<Mutex<EventQueue>>,
}

impl CallCtx {
    /// Sends one stream frame (one-way; the host manages credit).
    pub fn send_stream(&self, stream_id: &str, seq: u64, payload: &str) -> Result<(), String> {        let msg = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "stream.data",
            "message_id": format!("m-{}-{}", stream_id, seq),
            "session_id": self.session_id,
            "instance_id": self.instance_id,
            "generation": self.generation.to_string(),
            "body": {"stream_id": stream_id, "seq": seq.to_string(), "payload": payload},
        });
        let raw = serde_json::to_vec(&msg).map_err(|e| e.to_string())?;
        let frame = encode(&raw, self.max_frame).map_err(|e| e.to_string())?;
        let mut w = self.writer.lock().map_err(|e| e.to_string())?;
        w.write_all(&frame).map_err(|e| e.to_string())?;
        w.flush().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Opaque bindings of this activation for dependency calls.
    pub fn dependencies(&self) -> &[DepBinding] {
        &self.bindings
    }

    /// Events and stream chunks dropped by queue overflow (slow
    /// `on_event`/`on_stream`).
    pub fn event_dropped_count(&self) -> u64 {
        self.event_queue.lock().map(|q| q.dropped).unwrap_or(0)
    }

    /// Stream chunks still queued for `on_stream` (credit backpressure
    /// signal: the host withholds further credit while this is high).
    pub fn pending_stream_count(&self) -> usize {
        self.event_queue.lock().map(|q| q.pending_streams()).unwrap_or(0)
    }

    /// Refuses blocking calls made on the reader thread itself: waiting
    /// there for a host answer deadlocks the read that would deliver it.
    fn check_not_reader(&self) -> Result<(), DepError> {
        if std::thread::current().id() == self.reader_thread {
            return Err(DepError {
                code: "internal".into(),
                message: "blocking call on reader thread".into(),
            });
        }
        Ok(())
    }

    /// Same guard for resource roundtrips.
    fn check_not_reader_res(&self) -> Result<(), ResError> {
        if std::thread::current().id() == self.reader_thread {
            return Err(ResError {
                code: "internal".into(),
                message: "blocking call on reader thread".into(),
            });
        }
        Ok(())
    }

    /// Invokes a dependency by opaque handle (M6.1). Blocks until terminal,
    /// inheriting context cancellation. Without local negotiation, refuses with
    /// `unsupported-feature` without touching the wire.
    pub fn invoke_dependency(
        &self,
        binding: &str,
        input: Value,
        timeout: Duration,
    ) -> Result<Value, DepError> {
        if !self.features.iter().any(|f| f == "dependency-calls/1") {
            return Err(DepError {
                code: "unsupported-feature".into(),
                message: "dependency calls not negotiated".into(),
            });
        }
        let timeout_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;
        if timeout_ms == 0 {
            return Err(DepError {
                code: "invalid-message".into(),
                message: "timeout must be positive".into(),
            });
        }
        self.check_not_reader()?;
        let n = DEP_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let rid = format!("r-dep-{}", n);
        let (tx, rx) = std::sync::mpsc::channel();
        self.waiters.lock().map_err(|e| DepError {
            code: "internal".into(),
            message: format!("waiters: {}", e),
        })?.insert(rid.clone(), tx);
        let send_open = || -> Result<(), String> {
            let msg = json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "dependency.open",
                "message_id": format!("m-dep-{}", n),
                "session_id": self.session_id,
                "instance_id": self.instance_id,
                "generation": self.generation.to_string(),
                "request_id": rid,
                "body": {
                    "parent_ticket": self.ticket,
                    "binding_id": binding,
                    "timeout_ms": timeout_ms,
                    "input": input,
                },
            });
            let raw = serde_json::to_vec(&msg).map_err(|e| e.to_string())?;
            let frame = encode(&raw, self.max_frame).map_err(|e| e.to_string())?;
            let mut w = self.writer.lock().map_err(|e| e.to_string())?;
            w.write_all(&frame).map_err(|e| e.to_string())?;
            w.flush().map_err(|e| e.to_string())?;
            Ok(())
        };
        if let Err(e) = send_open() {
            self.waiters.lock().map(|mut w| w.remove(&rid)).ok();
            return Err(DepError { code: "internal".into(), message: e });
        }
        // Prazo local = pedido + folga de transporte; estouro cancela no fio.
        let wait_until = std::time::Instant::now() + timeout + Duration::from_secs(10);
        loop {
            if self.parent_cancel.load(std::sync::atomic::Ordering::SeqCst) {
                self.send_dep_cancel(&rid);
                self.waiters.lock().map(|mut w| w.remove(&rid)).ok();
                // Drains any late response without blocking (waiter already gone).
                return Err(DepError { code: "cancelled".into(), message: "parent cancelled".into() });
            }
            let now = std::time::Instant::now();
            if now >= wait_until {
                self.send_dep_cancel(&rid);
                self.waiters.lock().map(|mut w| w.remove(&rid)).ok();
                return Err(DepError { code: "outcome-unknown".into(), message: "sdk wait timeout".into() });
            }
            match rx.recv_timeout(std::cmp::min(Duration::from_millis(50), wait_until - now)) {
                Ok(r) => {
                    self.waiters.lock().map(|mut w| w.remove(&rid)).ok();
                    return r;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(DepError { code: "outcome-unknown".into(), message: "demux gone".into() });
                }
            }
        }
    }

    /// Cancela filha no fio (fire-and-forget; terminal chega ao waiter).
    fn send_dep_cancel(&self, target: &str) {
        let n = DEP_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let msg = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "dependency.cancel",
            "message_id": format!("m-dep-cancel-{}", n),
            "session_id": self.session_id,
            "instance_id": self.instance_id,
            "generation": self.generation.to_string(),
            "request_id": format!("r-dep-cancel-{}", n),
            "body": {"target_request_id": target},
        });
        if let Ok(raw) = serde_json::to_vec(&msg) {
            if let Ok(frame) = encode(&raw, self.max_frame) {
                if let Ok(w) = self.writer.lock() {
                    let mut w = w;
                    let _ = w.write_all(&frame);
                    let _ = w.flush();
                }
            }
        }
    }

    /// Sends a resource request and waits for `resource.result` (M6.3).
    /// `operation` is `acquire` or `release`; body carries the fields.
    fn resource_roundtrip(&self, operation: &str, body: Value) -> Result<Value, ResError> {
        self.check_not_reader_res()?;
        let n = DEP_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let rid = format!("r-res-{}", n);
        let mut full = serde_json::Map::new();
        full.insert("operation_id".into(), Value::String(format!("op-res-{}", n)));
        if let Value::Object(m) = body {
            for (k, v) in m {
                full.insert(k, v);
            }
        }
        let (tx, rx) = std::sync::mpsc::channel();
        self.res_waiters
            .lock()
            .map_err(|e| ResError { code: "internal".into(), message: format!("waiters: {}", e) })?
            .insert(rid.clone(), tx);
        let send = || -> Result<(), String> {
            let msg = json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": format!("resource.{}", operation),
                "message_id": format!("m-res-{}", n),
                "session_id": self.session_id,
                "instance_id": self.instance_id,
                "generation": self.generation.to_string(),
                "request_id": rid,
                "body": full,
            });
            let raw = serde_json::to_vec(&msg).map_err(|e| e.to_string())?;
            let frame = encode(&raw, self.max_frame).map_err(|e| e.to_string())?;
            let mut w = self.writer.lock().map_err(|e| e.to_string())?;
            w.write_all(&frame).map_err(|e| e.to_string())?;
            w.flush().map_err(|e| e.to_string())?;
            Ok(())
        };
        if let Err(e) = send() {
            self.res_waiters.lock().map(|mut w| w.remove(&rid)).ok();
            return Err(ResError { code: "internal".into(), message: e });
        }
        // Resource operations are local and fast; bounded wait with
        // inherited parent cancellation.
        let wait_until = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if self.parent_cancel.load(std::sync::atomic::Ordering::SeqCst) {
                self.res_waiters.lock().map(|mut w| w.remove(&rid)).ok();
                return Err(ResError { code: "cancelled".into(), message: "parent cancelled".into() });
            }
            let now = std::time::Instant::now();
            if now >= wait_until {
                self.res_waiters.lock().map(|mut w| w.remove(&rid)).ok();
                return Err(ResError { code: "outcome-unknown".into(), message: "resource wait timeout".into() });
            }
            match rx.recv_timeout(std::cmp::min(Duration::from_millis(50), wait_until - now)) {
                Ok(r) => {
                    self.res_waiters.lock().map(|mut w| w.remove(&rid)).ok();
                    return r;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(ResError { code: "outcome-unknown".into(), message: "demux gone".into() });
                }
            }
        }
    }

    /// Acquires an activation resource (cap/sub/timer/task) via the host.
    /// Timer needs `interval_ms`. Returns the wire handle id.
    pub fn acquire_resource(
        &self,
        kind: &str,
        label: &str,
        interval_ms: Option<u64>,
    ) -> Result<u64, ResError> {
        let mut body = serde_json::Map::new();
        body.insert("kind".into(), Value::String(kind.to_string()));
        body.insert("label".into(), Value::String(label.to_string()));
        if let Some(ms) = interval_ms {
            body.insert("interval_ms".into(), Value::from(ms));
        }
        let extra = self.resource_roundtrip("acquire", Value::Object(body))?;
        extra
            .get("handle")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(ResError { code: "internal".into(), message: "missing handle".into() })
    }

    /// Releases a handle acquired via [`CallCtx::acquire_resource`].
    pub fn release_resource(&self, handle: u64) -> Result<(), ResError> {
        let mut body = serde_json::Map::new();
        body.insert("handle".into(), Value::String(handle.to_string()));
        self.resource_roundtrip("release", Value::Object(body))?;
        Ok(())
    }
}

/// Component logic. `on_call` runs on one thread per call; `cancel` is
/// signaled on `call.cancel` or exit. `on_cancel` is optional
/// (observability; the worker cooperates via `cancel`). `on_event`
/// receives bus events for manifest-declared subscriptions (M6.3) on a
/// dedicated dispatcher thread: observe fast — overflows drop oldest
/// first and count in `event_dropped_count`. `on_stream` receives stream
/// chunks addressed to this activation on the same dispatcher: observe
/// fast — the host only grants more credit as the queue drains, so a
/// slow consumer throttles the sender instead of growing memory.
pub trait Handler: Send + Sync + 'static {
    fn on_call(
        &self,
        ctx: &CallCtx,
        ticket: &str,
        cap: &str,
        input: &Value,
        cancel: &AtomicBool,
    ) -> CallOutcome;

    fn on_cancel(&self, _ticket: &str) {}

    fn on_event(&self, _topic: &str, _payload: &Value) {}

    fn on_stream(&self, _stream_id: &str, _seq: u64, _payload: &str) {}
}

/// Event or stream item for the dispatcher thread.
enum DepEvent {
    Event { topic: String, payload: Value },
    Stream { stream_id: String, seq: u64, payload: String },
}

/// Bounded event queue: drops oldest on overflow, counts the loss.
/// Stream chunks share the queue: a slow consumer throttles the sender
/// (host withholds credit while items are queued) instead of growing
/// memory; drops still count and stay observable.
struct EventQueue {
    queue: std::collections::VecDeque<DepEvent>,
    cap: usize,
    dropped: u64,
}

impl EventQueue {
    fn push(&mut self, ev: DepEvent) {
        if self.queue.len() >= self.cap {
            self.queue.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.queue.push_back(ev);
    }

    fn drain(&mut self) -> Vec<DepEvent> {
        std::mem::take(&mut self.queue).into()
    }

    fn pending_streams(&self) -> usize {
        self.queue
            .iter()
            .filter(|e| matches!(e, DepEvent::Stream { .. }))
            .count()
    }
}

struct CallCtl {
    cancel: Arc<AtomicBool>,
}

/// Connected component: negotiated, activated session.
pub struct Component {
    writer: Arc<Mutex<UnixStream>>,
    reader: UnixStream,
    session_id: String,
    instance_id: String,
    generation: u64,
    max_frame: usize,
    /// Extensions negotiated in welcome (M6.1 step 1; no behavior yet).
    features: Vec<String>,
    /// Opaque activation bindings (M6.1 step 3).
    dep_bindings: Vec<DepBinding>,
}

impl Component {
    /// Extensions negotiated in welcome (M6.1 step 1).
    pub fn negotiated_features(&self) -> &[String] {
        &self.features
    }

    /// Connects, negotiates version, registers `logical`, completes activate.
    pub fn connect(sock: &Path, logical: &str) -> Result<Self, String> {
        let mut stream =
            UnixStream::connect(sock).map_err(|e| format!("connect: {}", e))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| e.to_string())?;
        let writer = Arc::new(Mutex::new(
            stream.try_clone().map_err(|e| e.to_string())?,
        ));
        let send = |v: &Value, mf: usize| -> Result<(), String> {
            let raw = serde_json::to_vec(v).map_err(|e| e.to_string())?;
            let frame = encode(&raw, mf).map_err(|e| e.to_string())?;
            let mut w = writer.lock().map_err(|e| e.to_string())?;
            w.write_all(&frame).map_err(|e| e.to_string())?;
            w.flush().map_err(|e| e.to_string())?;
            Ok(())
        };
        send(
            &json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "hello",
                "message_id": "h1",
                "body": {"launch_token": std::env::var("MATRIX_LAUNCH_TOKEN").unwrap_or_default(), "versions": ["0.1"], "max_frame": DEFAULT_MAX_FRAME, "client": "matrix-component", "features": ["dependency-calls/1"]},
            }),
            DEFAULT_MAX_FRAME,
        )?;
        let raw = read_frame(&mut stream, DEFAULT_MAX_FRAME)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "sem welcome".to_string())?;
        let welcome =
            parse_frame_payload(&raw).map_err(|e| format!("welcome: {:?}", e))?;
        if welcome.ty != "welcome" {
            return Err(format!("esperava welcome, veio {}", welcome.ty));
        }
        let session_id = welcome.session_id.clone().ok_or("welcome without session")?;
        let max_frame = welcome
            .body
            .get("max_frame")
            .and_then(|v| v.as_u64())
            .unwrap_or(DEFAULT_MAX_FRAME as u64) as usize;
        // M6.1 step 1: stores negotiated extensions; legacy session = empty.
        let features = welcome
            .body
            .get("features")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|e| e.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        send(
            &json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "component.register",
                "message_id": "reg1",
                "session_id": session_id,
                "body": {"manifest": {"id": logical}},
            }),
            max_frame,
        )?;
        let raw = read_frame(&mut stream, max_frame)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "sem registered".to_string())?;
        let reg =
            parse_frame_payload(&raw).map_err(|e| format!("registered: {:?}", e))?;
        if reg.ty != "registered" {
            return Err(format!("registro rejeitado: {:?}", reg.body));
        }
        let instance_id = reg.instance_id.clone().ok_or("registered without instance")?;
        let generation = reg.generation.ok_or("registered without generation")?;
        // Aguarda o activate do host e confirma.
        let raw = read_frame(&mut stream, max_frame)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "sem activate".to_string())?;
        let act =
            parse_frame_payload(&raw).map_err(|e| format!("activate: {:?}", e))?;
        if act.ty != "lifecycle.activate" {
            return Err(format!("esperava activate, veio {}", act.ty));
        }
        let op = act.body.get("operation_id").cloned().unwrap_or(json!("op?"));
        // M6.1 step 3: opaque activation bindings for children.
        let dep_bindings: Vec<DepBinding> = act
            .body
            .get("dependency_bindings")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|e| {
                        let id = e.get("binding_id")?.as_str()?;
                        let capability = e.get("capability")?.as_str()?;
                        if id.is_empty() || capability.is_empty() {
                            None
                        } else {
                            Some(DepBinding { id: id.to_string(), capability: capability.to_string() })
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        send(
            &json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "lifecycle.result",
                "message_id": "lc1",
                "session_id": session_id,
                "instance_id": instance_id,
                "generation": generation.to_string(),
                "request_id": act.request_id,
                "body": {"operation_id": op, "status": "ok", "pending": []},
            }),
            max_frame,
        )?;
        Ok(Self {
            writer,
            reader: stream,
            session_id,
            instance_id,
            generation,
            max_frame,
            features,
            dep_bindings,
        })
    }

    /// Serves until EOF/error, quiesce, or dispose (which answers and returns).
    /// Consumes the component; `Ok(())` = clean dispose, `Err` = crash.
    pub fn serve<H: Handler>(self, handler: H) -> Result<(), String> {
        let handler = Arc::new(handler);
        let calls: Arc<Mutex<HashMap<String, CallCtl>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let dep_waiters: Arc<Mutex<HashMap<String, std::sync::mpsc::Sender<Result<Value, DepError>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let res_waiters: Arc<Mutex<HashMap<String, std::sync::mpsc::Sender<Result<Value, ResError>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        // Base do contexto por chamada (M6.1): ticket/cancel entram por call.
        let (bw, bsid, biid, bgen, bmf, bbindings, bfeatures) = (
            self.writer.clone(),
            self.session_id.clone(),
            self.instance_id.clone(),
            self.generation,
            self.max_frame,
            self.dep_bindings.clone(),
            self.features.clone(),
        );
        let bwaiters = dep_waiters.clone();
        let breswaiters = res_waiters.clone();
        // Event dispatcher (M6.3): keeps handler slowness off the reader.
        let event_queue = Arc::new(Mutex::new(EventQueue {
            queue: std::collections::VecDeque::new(),
            cap: 64,
            dropped: 0,
        }));
        let (event_wake_tx, event_wake_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let reader_thread = std::thread::current().id();
        let dh = handler.clone();
        let dq = event_queue.clone();
        let ev_stop = Arc::new(AtomicBool::new(false));
        let ev_stop_worker = ev_stop.clone();
        let dispatcher = std::thread::Builder::new()
            .name("matrix-component-events".into())
            .spawn(move || {
                loop {
                    match event_wake_rx.recv_timeout(Duration::from_millis(100)) {
                        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            let batch = dq.lock().map(|mut q| q.drain()).unwrap_or_default();
                            for ev in batch {
                                match ev {
                                    DepEvent::Event { topic, payload } => dh.on_event(&topic, &payload),
                                    DepEvent::Stream { stream_id, seq, payload } => {
                                        dh.on_stream(&stream_id, seq, &payload)
                                    }
                                }
                            }
                            if ev_stop_worker.load(Ordering::SeqCst) {
                                return;
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            let batch = dq.lock().map(|mut q| q.drain()).unwrap_or_default();
                            for ev in batch {
                                match ev {
                                    DepEvent::Event { topic, payload } => dh.on_event(&topic, &payload),
                                    DepEvent::Stream { stream_id, seq, payload } => {
                                        dh.on_stream(&stream_id, seq, &payload)
                                    }
                                }
                            }
                            return;
                        }
                    }
                }
            });
        let mut stream = self.reader;
        let (session_id, instance_id, generation, max_frame) = (
            self.session_id.clone(),
            self.instance_id.clone(),
            self.generation,
            self.max_frame,
        );
        let writer0 = self.writer.clone();
        let out: Result<(), String> = loop {
            let raw = match read_frame(&mut stream, max_frame) {
                Ok(Some(r)) => r,
                Ok(None) => break Err("host eof".to_string()),
                Err(e) => break Err(format!("read: {}", e)),
            };
            let env = match parse_frame_payload(&raw) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let bound_ok = env.session_id.as_deref() == Some(session_id.as_str())
                && env
                    .instance_id
                    .as_deref()
                    .map(|v| v == instance_id)
                    .unwrap_or(true)
                && env.generation.map(|g| g == generation).unwrap_or(true);
            if !bound_ok {
                continue;
            }
            let req_id = env.request_id.clone();
            match env.ty.as_str() {
                "lifecycle.prepare" | "lifecycle.activate" | "lifecycle.quiesce" => {
                    let op = env
                        .body
                        .get("operation_id")
                        .cloned()
                        .unwrap_or(json!("op?"));
                    send_lifecycle_result(&writer0, &session_id, &instance_id, generation, max_frame, &op, req_id.as_deref());
                }
                "lifecycle.dispose" => {
                    let op = env
                        .body
                        .get("operation_id")
                        .cloned()
                        .unwrap_or(json!("op?"));
                    send_lifecycle_result(&writer0, &session_id, &instance_id, generation, max_frame, &op, req_id.as_deref());
                    break Ok(());
                }
                "call.open" => {
                    let ticket = env
                        .body
                        .get("ticket")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let cap = env
                        .body
                        .get("capability")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let input = env.body.get("input").cloned().unwrap_or(Value::Null);
                    let cancel = Arc::new(AtomicBool::new(false));
                    calls.lock().unwrap().insert(
                        ticket.clone(),
                        CallCtl { cancel: cancel.clone() },
                    );
                    let h = handler.clone();
                    let c = CallCtx {
                        writer: bw.clone(),
                        session_id: bsid.clone(),
                        instance_id: biid.clone(),
                        generation: bgen,
                        max_frame: bmf,
                        ticket: ticket.clone(),
                        parent_cancel: cancel.clone(),
                        bindings: bbindings.clone(),
                        features: bfeatures.clone(),
                        waiters: bwaiters.clone(),
                        res_waiters: breswaiters.clone(),
                        reader_thread,
                        event_queue: event_queue.clone(),
                    };
                    let calls2 = calls.clone();
                    let w = self.writer.clone();
                    let (sid, iid, gen, mf) = (
                        session_id.clone(),
                        instance_id.clone(),
                        generation,
                        max_frame,
                    );
                    let rid = req_id.clone();
                    std::thread::spawn(move || {
                        let out = h.on_call(&c, &ticket, &cap, &input, &cancel);
                        calls2.lock().unwrap().remove(&ticket);
                        // Late after cancel: stays silent (host already abandoned
                        // neither false success nor phantom error).
                        if cancel.load(std::sync::atomic::Ordering::SeqCst) {
                            return;
                        }
                        let body = match out {
                            CallOutcome::Ok(output) => {
                                json!({"ticket": ticket, "status": "ok", "output": output})
                            }
                            CallOutcome::Err { code, message } => {
                                json!({"ticket": ticket, "status": "error",
                                       "error": {"code": code, "message": message}})
                            }
                        };
                        send_envelope(
                            &w,
                            "call.result",
                            &format!("m-call-{}", ticket),
                            &sid,
                            &iid,
                            gen,
                            rid.as_deref(),
                            body,
                            mf,
                        );
                    });
                }
                "call.cancel" => {
                    let ticket =
                        env.body.get("ticket").and_then(|v| v.as_str()).unwrap_or("");
                    // Signals the worker (holding its Arc clone) and notifies
                    // handler. A entrada sai no fim do worker (idempotente).
                    if let Some(c) = calls.lock().unwrap().get(ticket) {
                        c.cancel.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    handler.on_cancel(ticket);
                }
                // Terminais de filhas: roteia ao waiter do open (M6.1) ou
                // drops it (late answer without a waiter). `accepted` only marks
                // progress and needs no answer.
                "dependency.result" => {
                    if let Some(rid) = req_id.clone() {
                        let tx = dep_waiters.lock().unwrap().remove(&rid);
                        if let Some(tx) = tx {
                            let status = env.body.get("status").and_then(|v| v.as_str()).unwrap_or("");
                            if status == "ok" {
                                let out = env.body.get("output").cloned().unwrap_or(Value::Null);
                                let _ = tx.send(Ok(out));
                            } else {
                                let err = env.body.get("error").cloned().unwrap_or(json!({}));
                                let code = err.get("code").and_then(|v| v.as_str()).unwrap_or("internal").to_string();
                                let message = err.get("message").and_then(|v| v.as_str()).unwrap_or("remote error").to_string();
                                let _ = tx.send(Err(DepError { code, message }));
                            }
                        }
                    }
                }
                "dependency.accepted" | "dependency.cancel.result" => {}
                // Activation resources and bus events (M6.3).
                "resource.result" => {
                    if let Some(rid) = req_id.clone() {
                        let tx = res_waiters.lock().unwrap().remove(&rid);
                        if let Some(tx) = tx {
                            let status = env.body.get("status").and_then(|v| v.as_str()).unwrap_or("");
                            if status == "ok" {
                                let mut extra = serde_json::Map::new();
                                if let Value::Object(m) = env.body.clone() {
                                    for (k, v) in m {
                                        if k != "operation_id" && k != "status" {
                                            extra.insert(k, v);
                                        }
                                    }
                                }
                                let _ = tx.send(Ok(Value::Object(extra)));
                            } else {
                                let code = env.body.get("code").and_then(|v| v.as_str()).unwrap_or("internal").to_string();
                                let message = env.body.get("message").and_then(|v| v.as_str()).unwrap_or("remote error").to_string();
                                let _ = tx.send(Err(ResError { code, message }));
                            }
                        }
                    }
                }
                "event.deliver" => {
                    // Never runs handler code on the reader: enqueue for the
                    // dispatcher (bounded, drops oldest) and wake it.
                    let topic = env.body.get("topic").and_then(|v| v.as_str()).unwrap_or("");
                    let payload = env.body.get("payload").cloned().unwrap_or(Value::Null);
                    if !topic.is_empty() {
                        event_queue.lock().map(|mut q| {
                            q.push(DepEvent::Event { topic: topic.to_string(), payload })
                        }).ok();
                        let _ = event_wake_tx.try_send(());
                    }
                }
                "stream.data" => {
                    // Same dispatcher path as events (never the reader).
                    // Credit is host-managed: the host only forwards what
                    // fits the grant, and withholds more while the queue
                    // is deep, so a slow consumer throttles the sender.
                    let stream_id = env.body.get("stream_id").and_then(|v| v.as_str()).unwrap_or("");
                    let seq = env.body.get("seq").and_then(|v| v.as_str()).and_then(|s| s.parse::<u64>().ok()).unwrap_or(u64::MAX);
                    let payload = env.body.get("payload").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    if !stream_id.is_empty() && seq != u64::MAX {
                        // Overflow drops oldest (counted) exactly like
                        // events: chunks are best-effort at the edge, the
                        // stream contract lives host-side (seq + credit).
                        event_queue.lock().map(|mut q| {
                            q.push(DepEvent::Stream { stream_id: stream_id.to_string(), seq, payload })
                        }).ok();
                        let _ = event_wake_tx.try_send(());
                    }
                }
                _ => {}
            }
        };
        // Stop the event dispatcher (flag + join); stragglers already drained.
        ev_stop.store(true, Ordering::SeqCst);
        if let Ok(dispatcher) = dispatcher {
            let _ = dispatcher.join();
        }
        out
    }

}

fn send_lifecycle_result(
    writer: &Arc<Mutex<UnixStream>>,
    session_id: &str,
    instance_id: &str,
    generation: u64,
    max_frame: usize,
    operation_id: &Value,
    request_id: Option<&str>,
) {
    send_envelope(
        writer,
        "lifecycle.result",
        &format!("m-lc-{}", operation_id.as_str().unwrap_or("op")),
        session_id,
        instance_id,
        generation,
        request_id,
        json!({"operation_id": operation_id, "status": "ok", "pending": []}),
        max_frame,
    );
}

fn send_envelope(
    writer: &Arc<Mutex<UnixStream>>,
    ty: &str,
    message_id: &str,
    session_id: &str,
    instance_id: &str,
    generation: u64,
    request_id: Option<&str>,
    body: Value,
    max_frame: usize,
) {
    let msg = json!({
        "protocol": matrix_proto::PROTOCOL_ID,
        "version": matrix_proto::PROTOCOL_VERSION,
        "type": ty,
        "message_id": message_id,
        "session_id": session_id,
        "instance_id": instance_id,
        "generation": generation.to_string(),
        "request_id": request_id,
        "body": body,
    });
    if let Ok(raw) = serde_json::to_vec(&msg) {
        if let Ok(frame) = encode(&raw, max_frame) {
            if let Ok(w) = writer.lock() {
                let mut w = w;
                use std::io::Write;
                let _ = w.write_all(&frame);
                let _ = w.flush();
            }
        }
    }
}
