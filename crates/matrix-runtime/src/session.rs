//! Persistent multiplexed remote sessions (M7 transport).
//!
//! One TLS connection per session, one I/O thread per session. Application
//! threads never touch the socket: they enqueue small frames on bounded
//! control/data queues, and the engine sends control first — queued data
//! never delays queued control. Bytes already in kernel/TLS buffers are
//! FIFO (single TCP connection); saturation of the transport window
//! surfaces as Suspect/Detached via timers, never as unbounded buffering.
//!
//! Absolute deadlines cover handshake, every frame write (lock-free
//! queues + TLS + flush via non-blocking I/O and `poll`, never SNDTIMEO
//! which Linux rearms on partial progress), and liveness. A failed
//! partial frame poisons the channel (`shutdown`); nothing is appended
//! to a truncated sequence. Malformed frames drop silently with a
//! counter; oversize frames poison (fail closed).
//!
//! Profile contract: `docs/M7-PROFILE.md`, schema `matrix-proto::remote`.

use matrix_proto::remote::{
    validate_envelope as validate_remote, REMOTE_PROFILE, REMOTE_PROTOCOL_ID,
    REMOTE_PROTOCOL_VERSION,
};
use matrix_proto::{encode, DEFAULT_MAX_FRAME};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::AsRawFd;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{sync_channel, Receiver, SyncSender},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn wire_err(e: matrix_proto::error::WireError) -> String {
    format!("{}:{}", e.code, e.details)
}

/// Profile name (also the TLS ALPN).
pub const REMOTE_ALPN: &str = REMOTE_PROFILE;

/// Bounds for one session. Control has its own small reserved queue: it
/// is bounded, never unlimited, and refuses with `resource-exhausted`
/// instead of growing.
#[derive(Clone, Debug)]
pub struct SessionLimits {
    pub max_frame: usize,
    pub control_queue: usize,
    pub data_queue: usize,
    pub commands: usize,
    pub max_pending: usize,
    pub frame_budget: Duration,
    pub handshake_budget: Duration,
    pub heartbeat_interval: Duration,
    pub suspect_after: Duration,
    pub detach_after: Duration,
}

impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_frame: DEFAULT_MAX_FRAME,
            control_queue: 128,
            data_queue: 64,
            commands: 256,
            max_pending: 256,
            frame_budget: Duration::from_secs(5),
            handshake_budget: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(1),
            suspect_after: Duration::from_secs(5),
            detach_after: Duration::from_secs(15),
        }
    }
}

/// TLS configs for the `matrix.remote/0.1` profile (same PKI, own ALPN).
pub fn server_config(
    ca: &std::path::Path,
    chain: &std::path::Path,
    private: &std::path::Path,
) -> Result<Arc<rustls::ServerConfig>, String> {
    crate::remote::provider();
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(
        crate::remote::roots(ca)?,
    ))
    .build()
    .map_err(err)?;
    let mut config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            vec![crate::remote::cert(chain)?],
            crate::remote::key(private)?,
        )
        .map_err(err)?;
    config.alpn_protocols = vec![REMOTE_ALPN.as_bytes().to_vec()];
    Ok(Arc::new(config))
}

/// TLS configs for the `matrix.remote/0.1` profile (same PKI, own ALPN).
pub fn client_config(
    ca: &std::path::Path,
    chain: &std::path::Path,
    private: &std::path::Path,
) -> Result<Arc<rustls::ClientConfig>, String> {
    crate::remote::provider();
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(crate::remote::roots(ca)?)
        .with_client_auth_cert(
            vec![crate::remote::cert(chain)?],
            crate::remote::key(private)?,
        )
        .map_err(err)?;
    config.alpn_protocols = vec![REMOTE_ALPN.as_bytes().to_vec()];
    Ok(Arc::new(config))
}

/// Connection states (`docs/REMOTE.md`, `docs/M7-PROFILE.md`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Connected,
    Suspect,
    Detached,
}

/// Server-push and inbound-request handler (service layer).
pub trait InboundHandler: Send + Sync + 'static {
    fn on_message(&self, session: &Session, env: Value);
}

enum Command {
    Control { frame: Vec<u8> },
    Data { frame: Vec<u8> },
    Request { request_id: String, frame: Vec<u8>, timeout: Duration },
}

struct StateInfo {
    state: SessionState,
    last_inbound: Instant,
    dropped_malformed: u64,
    dropped_late: u64,
    refused_control: u64,
    refused_data: u64,
}

type ReplyTx = std::sync::mpsc::Sender<Result<Value, String>>;

struct Inner {
    cmd_tx: SyncSender<Command>,
    replies: Mutex<HashMap<String, (ReplyTx, Instant)>>,
    /// Multi-answer subscriptions for `call.open` (accepted+result share
    /// one request_id). Checked before stray-drop; explicit unsubscribe
    /// removes. Bounded by `max_pending` like single-shot waits.
    multi: Mutex<HashMap<String, std::sync::mpsc::Sender<Value>>>,
    max_pending: usize,
    handler: Mutex<Option<Arc<dyn InboundHandler>>>,
    info: Mutex<StateInfo>,
    shutdown: AtomicBool,
    session_id: String,
    peer_principal: String,
}

/// Multiplexed remote session handle. Clone shares the session.
#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}

impl Session {
    fn fresh_id(prefix: &str) -> String {
        let rand = matrix_guard::random_token()
            .map(|s| s.chars().take(12).collect::<String>())
            .unwrap_or_else(|_| "x".into());
        format!("{prefix}-{rand}")
    }

    /// Encodes one envelope value into a length-prefixed frame.
    pub fn encode_frame(env: &Value, max_frame: usize) -> Result<Vec<u8>, String> {
        let raw = serde_json::to_vec(env).map_err(err)?;
        encode(&raw, max_frame).map_err(err)
    }

    /// Builds a `session.hello` body.
    pub fn hello_body(domain: &str, authority: &str, controller_epoch: u64) -> Value {
        serde_json::json!({
            "versions": [REMOTE_PROTOCOL_VERSION],
            "features": ["remote-calls/1", "remote-streams/1", "remote-events/1", "remote-ops/1"],
            "domain": domain,
            "authority": authority,
            "controller_epoch": controller_epoch.to_string(),
        })
    }

    /// Client side: connects, handshakes, negotiates, spawns the engine.
    /// Returns the session plus the decoded `session.welcome` body.
    pub fn connect(
        address: SocketAddr,
        server_name: &str,
        config: Arc<rustls::ClientConfig>,
        limits: SessionLimits,
        hello: Value,
    ) -> Result<(Arc<Self>, Value), String> {
        use rustls::pki_types::ServerName;
        let tcp = TcpStream::connect_timeout(&address, Duration::from_secs(3)).map_err(err)?;
        let name = ServerName::try_from(server_name.to_string()).map_err(err)?;
        let conn = rustls::ClientConnection::new(config, name).map_err(err)?;
        let mut io = TlsIo::connect(tcp, rustls::Connection::Client(conn), limits.handshake_budget).map_err(|e| format!("tls-handshake:{e}"))?;
        if io.alpn() != Some(REMOTE_ALPN.as_bytes()) {
            return Err("unsupported profile".into());
        }
        let hello_env = serde_json::json!({
            "protocol": REMOTE_PROTOCOL_ID, "version": REMOTE_PROTOCOL_VERSION,
            "type": "session.hello", "message_id": Self::fresh_id("m"),
            "body": hello,
        });
        matrix_proto::remote::validate_body("session.hello", &hello_env["body"]).map_err(wire_err)?;
        io.send_frame(&hello_env, limits.max_frame, limits.frame_budget).map_err(|e| format!("hello-send:{e}"))?;
        let welcome =
            io.recv_frame(limits.max_frame, limits.frame_budget).map_err(|e| format!("welcome-recv:{e}"))?.ok_or("eof in handshake")?;
        let body = Self::check_welcome(&welcome)?;
        let session_id = welcome
            .get("session_id")
            .and_then(|v| v.as_str())
            .ok_or("welcome without session_id")?
            .to_string();
        let peer_fp = io.peer_fingerprint()?;
        Ok((Self::spawn(io, limits, session_id, peer_fp, "client"), body))
    }

    /// Server side: takes an accepted TCP stream (blocking), handshakes,
    /// authorizes the client principal, reads `session.hello`, answers
    /// `session.welcome`, then spawns the engine. Returns the session plus
    /// the decoded hello body.
    pub fn accept(
        tcp: TcpStream,
        config: Arc<rustls::ServerConfig>,
        limits: SessionLimits,
        authorize: &dyn Fn(&str) -> bool,
        welcome_body: impl FnOnce(&Value) -> Result<Value, String>,
    ) -> Result<(Arc<Self>, Value), String> {
        let conn = rustls::ServerConnection::new(config).map_err(err)?;
        let mut io = TlsIo::connect(tcp, rustls::Connection::Server(conn), limits.handshake_budget).map_err(|e| format!("tls-handshake:{e}"))?;
        if io.alpn() != Some(REMOTE_ALPN.as_bytes()) {
            return Err("unsupported profile".into());
        }
        let peer = io.peer_fingerprint()?;
        if !authorize(&peer) {
            return Err("permission-denied".into());
        }
        let hello =
            io.recv_frame(limits.max_frame, limits.frame_budget).map_err(|e| format!("hello-recv:{e}"))?.ok_or("eof in handshake")?;
        Self::check_hello(&hello)?;
        let body = welcome_body(&hello["body"])?;
        let session_id = Self::fresh_id("sess");
        let welcome = serde_json::json!({
            "protocol": REMOTE_PROTOCOL_ID, "version": REMOTE_PROTOCOL_VERSION,
            "type": "session.welcome", "message_id": Self::fresh_id("m"),
            "session_id": session_id,
            "body": body,
        });
        validate_remote(&welcome).map_err(wire_err)?;
        io.send_frame(&welcome, limits.max_frame, limits.frame_budget).map_err(|e| format!("welcome-send:{e}"))?;
        Ok((Self::spawn(io, limits, session_id, peer, "server"), hello["body"].clone()))
    }

    fn check_hello(hello: &Value) -> Result<(), String> {
        validate_remote(hello).map_err(wire_err)?;
        if hello.get("type").and_then(|v| v.as_str()) != Some("session.hello") {
            return Err("expected session.hello".into());
        }
        Ok(())
    }

    fn check_welcome(welcome: &Value) -> Result<Value, String> {
        validate_remote(welcome).map_err(wire_err)?;
        if welcome.get("type").and_then(|v| v.as_str()) != Some("session.welcome") {
            return Err("expected session.welcome".into());
        }
        Ok(welcome["body"].clone())
    }

    fn spawn(io: TlsIo, limits: SessionLimits, session_id: String, peer: String, side: &'static str) -> Arc<Self> {
        let (cmd_tx, cmd_rx) = sync_channel::<Command>(limits.commands);
        let max_pending = limits.max_pending;
        let inner = Arc::new(Inner {
            cmd_tx,
            replies: Mutex::new(HashMap::new()),
            multi: Mutex::new(HashMap::new()),
            max_pending,
            handler: Mutex::new(None),
            info: Mutex::new(StateInfo {
                state: SessionState::Connected,
                last_inbound: Instant::now(),
                dropped_malformed: 0,
                dropped_late: 0,
                refused_control: 0,
                refused_data: 0,
            }),
            shutdown: AtomicBool::new(false),
            session_id,
            peer_principal: peer,
        });
        let engine = inner.clone();
        let thread_name = format!("matrix-remote-{side}");
        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                Engine::new(io, limits, engine, cmd_rx).run();
            })
            .ok();
        Arc::new(Self { inner })
    }

    pub fn session_id(&self) -> &str {
        &self.inner.session_id
    }

    pub fn peer_principal(&self) -> &str {
        &self.inner.peer_principal
    }

    pub fn state(&self) -> SessionState {
        self.inner.info.lock().unwrap().state
    }

    /// `true` while new admissions may use this session (fail closed otherwise).
    pub fn admissible(&self) -> bool {
        self.state() == SessionState::Connected && !self.inner.shutdown.load(Ordering::SeqCst)
    }

    pub fn set_handler(&self, h: Arc<dyn InboundHandler>) {
        *self.inner.handler.lock().unwrap() = Some(h);
    }

    fn send_cmd(&self, cmd: Command, refused: &str) -> Result<(), String> {
        if self.inner.shutdown.load(Ordering::SeqCst) || self.state() == SessionState::Detached {
            return Err("session detached".into());
        }
        self.inner.cmd_tx.try_send(cmd).map_err(|_| refused.to_string())
    }

    /// Enqueues a control envelope (small; reserved bounded budget).
    pub fn send_control(&self, env: &Value) -> Result<(), String> {
        let frame = Self::encode_frame(env, DEFAULT_MAX_FRAME)?;
        self.send_cmd(Command::Control { frame }, "resource-exhausted").map_err(|e| {
            if e == "resource-exhausted" {
                self.inner.info.lock().unwrap().refused_control += 1;
            }
            e
        })
    }

    /// Enqueues a bulk envelope (credit-governed by the upper layer).
    pub fn send_data(&self, env: &Value) -> Result<(), String> {
        let frame = Self::encode_frame(env, DEFAULT_MAX_FRAME)?;
        self.send_cmd(Command::Data { frame }, "resource-exhausted").map_err(|e| {
            if e == "resource-exhausted" {
                self.inner.info.lock().unwrap().refused_data += 1;
            }
            e
        })
    }

    /// Control request/response by `request_id` with a caller timeout.
    /// A late answer never resolves a newer wait: the entry is removed on
    /// timeout and stragglers only bump a counter. Cancellation is
    /// cooperative: when `cancel` fires, the local wait is abandoned (the
    /// entry is removed so a late answer cannot resolve anything); the
    /// caller must still send `call.cancel` for executor-side revocation.
    pub fn request(
        &self,
        env: &Value,
        timeout: Duration,
        cancel: Option<&Arc<AtomicBool>>,
    ) -> Result<Value, String> {
        let request_id = env
            .get("request_id")
            .and_then(|v| v.as_str())
            .ok_or("request needs request_id")?
            .to_string();
        let (tx, rx) = std::sync::mpsc::channel();
        {
            let mut replies = self.inner.replies.lock().unwrap();
            if replies.len() >= self.inner.max_pending {
                return Err("resource-exhausted".into());
            }
            if replies.contains_key(&request_id) {
                return Err("duplicate-request".into());
            }
            replies.insert(request_id.clone(), (tx, Instant::now() + timeout));
        }
        let frame = Self::encode_frame(env, DEFAULT_MAX_FRAME)?;
        if self.send_cmd(Command::Request { request_id: request_id.clone(), frame, timeout }, "resource-exhausted").is_err() {
            self.inner.replies.lock().unwrap().remove(&request_id);
            self.inner.info.lock().unwrap().refused_control += 1;
            return Err("resource-exhausted".into());
        }
        let step = Duration::from_millis(5).min(timeout);
        let mut waited = Duration::ZERO;
        loop {
            if cancel.is_some_and(|c| c.load(Ordering::SeqCst)) {
                self.inner.replies.lock().unwrap().remove(&request_id);
                return Err("cancelled".into());
            }
            let quantum = (timeout - waited).min(step);
            match rx.recv_timeout(quantum) {
                Ok(r) => return r,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    waited += quantum;
                    if waited >= timeout {
                        // Engine also expires the entry; removal here is idempotent.
                        self.inner.replies.lock().unwrap().remove(&request_id);
                        return Err("outcome-unknown".into());
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    self.inner.replies.lock().unwrap().remove(&request_id);
                    return Err("outcome-unknown".into());
                }
            }
        }
    }

    /// Multi-answer subscription for `call.open` (accepted+result share
    /// one request_id). The waiter stays until explicit unsubscribe;
    /// stragglers after unsubscribe are stray (drop+count, never resolve
    /// a newer wait). Bounded by `max_pending`; duplicates refuse.
    pub fn subscribe_answers(
        &self,
        request_id: &str,
        tx: std::sync::mpsc::Sender<Value>,
    ) -> Result<(), String> {
        let mut multi = self.inner.multi.lock().unwrap();
        if multi.len() >= self.inner.max_pending {
            return Err("resource-exhausted".into());
        }
        if multi.contains_key(request_id) {
            return Err("duplicate-request".into());
        }
        multi.insert(request_id.to_string(), tx);
        Ok(())
    }

    /// Removes a multi-answer subscription (idempotent). Late answers
    /// afterwards are stray.
    pub fn unsubscribe_answers(&self, request_id: &str) {
        self.inner.multi.lock().unwrap().remove(request_id);
    }

    /// Diagnostic counters (no credentials, no payloads).
    pub fn inspect(&self) -> Value {
        let i = self.inner.info.lock().unwrap();
        serde_json::json!({
            "session_id": self.inner.session_id,
            "state": format!("{:?}", i.state),
            "peer": self.inner.peer_principal,
            "dropped_malformed": i.dropped_malformed,
            "dropped_late": i.dropped_late,
            "refused_control": i.refused_control,
            "refused_data": i.refused_data,
        })
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
    }
}

/// TLS over a non-blocking TCP stream with absolute deadlines.
/// Never relies on SNDTIMEO (rearmed on partial progress): every wait is
/// `poll` bounded by the remaining budget, every I/O attempt returns
/// immediately.
struct TlsIo {
    tcp: TcpStream,
    conn: rustls::Connection,
    assembler: Assembler,
    /// Decoded frames read ahead during the handshake (coalesced reads).
    stash: VecDeque<Value>,
    /// TCP hit EOF: no more records will ever arrive. Buffered plaintext
    /// still drains first; whatever is incomplete afterwards is dirty.
    tcp_eof: bool,
}

impl TlsIo {
    /// Switches to non-blocking I/O and completes the TLS handshake.
    fn connect(tcp: TcpStream, conn: rustls::Connection, budget: Duration) -> Result<Self, String> {
        tcp.set_nonblocking(true).map_err(err)?;
        let mut io = Self { tcp, conn, assembler: Assembler::default(), stash: VecDeque::new(), tcp_eof: false };
        let deadline = Instant::now() + budget;
        let r = io.drive(|c| !c.is_handshaking(), deadline);
        if r.is_err() {
            let _ = io.tcp.shutdown(std::net::Shutdown::Both);
        }
        r.map(|_| io).map_err(|e| {
            if e == "timeout" {
                "timeout".into()
            } else {
                e
            }
        })
    }

    fn fd(&self) -> i32 {
        self.tcp.as_raw_fd()
    }

    /// Waits for `events` until the absolute `deadline`. `false` = expired.
    fn poll_until(&self, events: i16, deadline: Instant) -> Result<bool, String> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            let ms = deadline
                .saturating_duration_since(now)
                .as_millis()
                .min(i32::MAX as u128) as libc::c_int;
            let mut pfd = libc::pollfd { fd: self.fd(), events, revents: 0 };
            // SAFETY: single valid pollfd; timeout >= 0.
            let r = unsafe { libc::poll(&mut pfd, 1, ms) };
            if r == 0 {
                return Ok(false);
            }
            if r < 0 {
                let e = std::io::Error::last_os_error();
                if e.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e.to_string());
            }
            if (pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL)) != 0
                && (pfd.revents & events) == 0
            {
                return Err("peer closed".into());
            }
            return Ok(true);
        }
    }

    /// Drives rustls until `done` holds, never past `deadline`.
    /// Every `complete_io` is non-blocking (immediate); waits go through
    /// `poll` with the remaining budget.
    fn drive(&mut self, mut done: impl FnMut(&rustls::Connection) -> bool, deadline: Instant) -> Result<(), String> {
        loop {
            if done(&self.conn) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("timeout".into());
            }
            {
                let Self { tcp, conn, .. } = self;
                match conn.complete_io(tcp) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
            if done(&self.conn) {
                return Ok(());
            }
            let mut events = 0;
            if self.conn.wants_write() {
                events |= libc::POLLOUT;
            }
            if self.conn.wants_read() {
                events |= libc::POLLIN;
            }
            if events == 0 {
                return Ok(());
            }
            if !self.poll_until(events, deadline)? {
                return Err("timeout".into());
            }
        }
    }

    fn poison(&self) {
        let _ = self.tcp.shutdown(std::net::Shutdown::Both);
    }

    fn alpn(&self) -> Option<&[u8]> {
        self.conn.alpn_protocol()
    }

    fn peer_fingerprint(&self) -> Result<String, String> {
        self.conn
            .peer_certificates()
            .and_then(|c| c.first())
            .map(|c| crate::remote::fingerprint(c.as_ref()))
            .ok_or_else(|| "unauthenticated".into())
    }

    /// Sends one length-prefixed frame; the whole call stays within `budget`
    /// past which it fails (poisoning when any byte may be out).
    fn send_frame(&mut self, env: &Value, max_frame: usize, budget: Duration) -> Result<(), String> {
        let raw = serde_json::to_vec(env).map_err(err)?;
        let frame = encode(&raw, max_frame).map_err(err)?;
        let deadline = Instant::now() + budget;
        // Plaintext buffering is in-memory; the wire flush below is bounded.
        self.conn
            .writer()
            .write_all(&frame)
            .map_err(err)?;
        let fed = true;
        match self.drive(|c| !c.wants_write(), deadline) {
            Ok(()) => {
                if Instant::now() >= deadline {
                    // Late completion is still a miss; bytes are out.
                    if fed {
                        self.poison();
                    }
                    return Err("timeout".into());
                }
                // Flush read side errors are impossible here (memory only).
                Ok(())
            }
            Err(e) => {
                if fed {
                    self.poison();
                }
                Err(e)
            }
        }
    }

    /// Receives one frame within `budget`; `Ok(None)` = clean EOF.
    /// Malformed JSON is an error to the caller (engine drops + counts);
    /// oversize length poisons (fail closed). EOF with an empty assembler
    /// is clean; EOF over a partial frame is a transport error.
    fn recv_frame(&mut self, max_frame: usize, budget: Duration) -> Result<Option<Value>, String> {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(stashed) = self.stash.pop_front() {
                return Ok(Some(stashed));
            }
            if let Some(raw) = self.assembler.take_frame()? {
                return Self::decode_frame(&raw).map(Some);
            }
            if self.tcp_eof {
                return Ok(None);
            }
            if Instant::now() >= deadline {
                return Err("timeout".into());
            }
            // One non-blocking TLS round, then wait for readability.
            let eof = {
                let Self { tcp, conn, .. } = self;
                match conn.complete_io(tcp) {
                    Ok(_) => false,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => false,
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => true,
                    Err(e) => return Err(e.to_string()),
                }
            };
            if eof {
                self.tcp_eof = true;
            }
            if let Err(e) = self.conn.process_new_packets() {
                return Err(e.to_string());
            }
            // Drain decrypted bytes (in-memory; WouldBlock/Ok(0) = none
            // available; UnexpectedEof surfaces once pending data is read).
            loop {
                let mut chunk = [0u8; 8192];
                match self.conn.reader().read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        // Handshake frames are tiny, but coalesced reads
                        // still complete several: stash the extras.
                        let mut rest = &chunk[..n];
                        loop {
                            let fed = self.assembler.feed(rest, max_frame);
                            match fed {
                                Ok(r) => rest = r,
                                Err(_) => {
                                    self.poison();
                                    return Err("frame too large".into());
                                }
                            }
                            let mut took = false;
                            loop {
                                let taken = self.assembler.take_frame();
                                match taken {
                                    Ok(Some(raw)) => {
                                        took = true;
                                        match Self::decode_frame(&raw) {
                                            Ok(v) => self.stash.push_back(v),
                                            Err(e) => {
                                                self.poison();
                                                return Err(e);
                                            }
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(e) => {
                                        self.poison();
                                        return Err(e);
                                    }
                                }
                            }
                            if rest.is_empty() {
                                break;
                            }
                            if !took {
                                // Unreachable given feed's contract, but fail
                                // closed instead of spinning.
                                self.poison();
                                return Err("assembler stall".into());
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e.to_string()),
                }
            }
            if self.assembler.has_frame() || !self.stash.is_empty() {
                // A complete frame is already buffered (possibly stashed
                // from a coalesced read): return it without waiting for
                // more wire data. Polling here would deadlock the peer,
                // which is itself waiting for our reply.
                continue;
            }
            if self.tcp_eof {
                // Bytes drained above; what remains decides clean vs dirty.
                if self.assembler.is_empty() {
                    return Ok(None);
                }
                return Err("truncated frame".into());
            }
            if !self.poll_until(libc::POLLIN, deadline)? {
                return Err("timeout".into());
            }
        }
    }

    fn decode_frame(raw: &[u8]) -> Result<Value, String> {
        matrix_proto::envelope::scan_raw(raw).map_err(|e| e.code.to_string())
    }
}

/// Length-prefix assembler over decrypted bytes. Never allocates above
/// the negotiated max: oversize length fails before buffering the body.
#[derive(Default)]
struct Assembler {
    header: [u8; 4],
    header_got: usize,
    pending_len: Option<usize>,
    body: Vec<u8>,
}

impl Assembler {
    /// Feeds bytes; returns the unconsumed tail. Stops at a completed
    /// frame so the caller takes it before feeding more — never spins on
    /// frame-complete-with-tail (coalesced reads always straddle frame
    /// boundaries eventually).
    fn feed<'a>(&mut self, bytes: &'a [u8], max_frame: usize) -> Result<&'a [u8], ()> {
        let mut rest = bytes;
        while !rest.is_empty() {
            if self.pending_len.is_none() {
                let take = (4 - self.header_got).min(rest.len());
                self.header[self.header_got..self.header_got + take]
                    .copy_from_slice(&rest[..take]);
                self.header_got += take;
                rest = &rest[take..];
                if self.header_got == 4 {
                    let len = u32::from_be_bytes(self.header) as usize;
                    if len == 0 || len > max_frame {
                        return Err(());
                    }
                    self.pending_len = Some(len);
                    self.body = Vec::with_capacity(len.min(65536));
                }
                continue;
            }
            if self.has_frame() {
                break;
            }
            let want = self.pending_len.unwrap().saturating_sub(self.body.len());
            if want == 0 {
                break;
            }
            let take = want.min(rest.len());
            self.body.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
        }
        Ok(rest)
    }

    fn has_frame(&self) -> bool {
        self.pending_len.is_some_and(|n| self.body.len() >= n)
    }

    fn is_empty(&self) -> bool {
        self.header_got == 0 && self.pending_len.is_none() && self.body.is_empty()
    }

    fn take_frame(&mut self) -> Result<Option<Vec<u8>>, String> {
        if !self.has_frame() {
            return Ok(None);
        }
        let len = self.pending_len.unwrap();
        let out = std::mem::replace(&mut self.body, Vec::new());
        self.pending_len = None;
        self.header_got = 0;
        if out.len() != len {
            return Err("assembler drift".into());
        }
        Ok(Some(out))
    }
}

struct QueuedFrame {
    bytes: Vec<u8>,
    deadline: Instant,
}

struct Engine {
    io: TlsIo,
    limits: SessionLimits,
    session: Arc<Inner>,
    cmd_rx: Receiver<Command>,
    control_q: VecDeque<QueuedFrame>,
    data_q: VecDeque<QueuedFrame>,
    pending: HashMap<String, (ReplyTx, Instant)>,
    last_heartbeat: Instant,
}

impl Engine {
    fn new(io: TlsIo, limits: SessionLimits, session: Arc<Inner>, cmd_rx: Receiver<Command>) -> Self {
        Self {
            io,
            limits,
            session,
            cmd_rx,
            control_q: VecDeque::new(),
            data_q: VecDeque::new(),
            pending: HashMap::new(),
            last_heartbeat: Instant::now(),
        }
    }

    fn set_state(&self, state: SessionState) {
        let mut info = self.session.info.lock().unwrap();
        // Detached is terminal; Suspect recovers on inbound traffic.
        if info.state == SessionState::Detached {
            return;
        }
        info.state = state;
    }

    fn note_inbound(&self) {
        let mut info = self.session.info.lock().unwrap();
        info.last_inbound = Instant::now();
        if info.state == SessionState::Suspect {
            info.state = SessionState::Connected;
        }
    }

    fn fail_all(&mut self, reason: &str) {
        for (_, (tx, _)) in self.pending.drain() {
            let _ = tx.send(Err(reason.to_string()));
        }
        for (_, (tx, _)) in self.session.replies.lock().unwrap().drain() {
            let _ = tx.send(Err(reason.to_string()));
        }
    }

    fn detach(&mut self, reason: &str) {
        self.io.poison();
        self.session.info.lock().unwrap().state = SessionState::Detached;
        self.fail_all(reason);
    }

    fn drain_commands(&mut self) {
        while let Ok(cmd) = self.cmd_rx.try_recv() {
            match cmd {
                Command::Control { frame } => {
                    if self.control_q.len() >= self.limits.control_queue {
                        self.session.info.lock().unwrap().refused_control += 1;
                        continue;
                    }
                    self.control_q.push_back(QueuedFrame {
                        bytes: frame,
                        deadline: Instant::now() + self.limits.frame_budget,
                    });
                }
                Command::Data { frame } => {
                    if self.data_q.len() >= self.limits.data_queue {
                        self.session.info.lock().unwrap().refused_data += 1;
                        continue;
                    }
                    self.data_q.push_back(QueuedFrame {
                        bytes: frame,
                        deadline: Instant::now() + self.limits.frame_budget,
                    });
                }
                Command::Request { request_id, frame, timeout } => {
                    // Move the caller's reply slot into engine ownership so
                    // timeouts and stragglers are handled in one place.
                    let reply = self.session.replies.lock().unwrap().remove(&request_id);
                    if self.pending.len() >= self.limits.max_pending || reply.is_none() {
                        if let Some((tx, _)) = reply {
                            let _ = tx.send(Err("resource-exhausted".into()));
                        }
                        continue;
                    }
                    if self.control_q.len() >= self.limits.control_queue {
                        self.session.info.lock().unwrap().refused_control += 1;
                        if let Some((tx, _)) = reply {
                            let _ = tx.send(Err("resource-exhausted".into()));
                        }
                        continue;
                    }
                    let (tx, _) = reply.unwrap();
                    self.pending.insert(request_id, (tx, Instant::now() + timeout));
                    self.control_q.push_back(QueuedFrame {
                        bytes: frame,
                        deadline: Instant::now() + self.limits.frame_budget,
                    });
                }
            }
        }
    }

    /// Sends one queued frame within its absolute deadline. Control always
    /// goes first; returns `false` when the session must detach.
    fn flush_one(&mut self) -> Result<bool, ()> {
        let next_is_control = !self.control_q.is_empty();
        let q = if next_is_control { &mut self.control_q } else { &mut self.data_q };
        let Some(frame) = q.pop_front() else { return Ok(true) };
        if Instant::now() >= frame.deadline {
            // Expired while queued: drop and count. Bytes of this frame
            // never went out (frames flush fully in order), so framing
            // stays intact and the channel survives; waiters unblock via
            // their own timeouts. Sustained stalls still detach through
            // the Suspect/Detached timers.
            self.session.info.lock().unwrap().dropped_late += 1;
            return Ok(true);
        }
        // Feed plaintext incrementally: rustls caps the in-memory send
        // buffer, so a large frame will not fit in one `write`. Feed and
        // flush in turns — still atomic on the wire, since this engine
        // feeds exactly one frame at a time — all within the frame's
        // absolute deadline.
        let mut fed = 0usize;
        loop {
            if Instant::now() >= frame.deadline {
                self.io.poison();
                return Err(());
            }
            if fed < frame.bytes.len() {
                match self.io.conn.writer().write(&frame.bytes[fed..]) {
                    Ok(0) if !self.io.conn.wants_write() => {
                        self.io.poison();
                        return Err(());
                    }
                    Ok(n) => fed += n,
                    Err(_) => {
                        self.io.poison();
                        return Err(());
                    }
                }
            }
            if fed == frame.bytes.len() && !self.io.conn.wants_write() {
                break;
            }
            if self.io.drive(|c| !c.wants_write(), frame.deadline).is_err() {
                self.io.poison();
                return Err(());
            }
        }
        if Instant::now() >= frame.deadline {
            self.io.poison();
            return Err(());
        }
        Ok(true)
    }

    fn expire_pending(&mut self) {
        let now = Instant::now();
        let mut late = vec![];
        self.pending.retain(|id, (tx, deadline)| {
            if now >= *deadline {
                late.push(id.clone());
                let _ = tx.send(Err("outcome-unknown".into()));
                return false;
            }
            true
        });
        if !late.is_empty() {
            self.session.info.lock().unwrap().dropped_late += late.len() as u64;
        }
        // Caller-side waits that already gave up: drop their slots so a
        // late answer never resolves a newer wait.
        self.session.replies.lock().unwrap().retain(|_, (_, deadline)| now < *deadline);
    }

    fn dispatch(&mut self, env: Value) {
        self.note_inbound();
        let ty = env.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();
        // Response correlation by request_id (any type; the waiter checks).
        if let Some(rid) = env.get("request_id").and_then(|v| v.as_str()) {
            if let Some((tx, _)) = self.pending.remove(rid) {
                let _ = tx.send(Ok(env));
                return;
            }
            // Multi-answer (`call.open` accepted+result): delivered without
            // consuming (explicit unsubscribe ends). Checked before
            // stray-drop so route legs work; truly stray answers (no
            // waiter) still drop+count below.
            if let Some(tx) = self.session.multi.lock().unwrap().get(rid).cloned() {
                let _ = tx.send(env);
                return;
            }
            // Miss: answers to nothing are stray (drop + count); any other
            // type is an inbound request for the service handler, which
            // answers with the same request_id.
            if matrix_proto::remote::is_answer_type(&ty) {
                self.session.info.lock().unwrap().dropped_late += 1;
                return;
            }
        }
        // Malformed envelopes drop silently (broken-wire rule); validated
        // ones go to the service handler.
        if validate_remote(&env).is_err() {
            self.session.info.lock().unwrap().dropped_malformed += 1;
            return;
        }
        if let Some(h) = self.session.handler.lock().unwrap().clone() {
            let session = Session { inner: self.session.clone() };
            h.on_message(&session, env);
        }
    }

    fn heartbeat_due(&mut self) {
        if self.last_heartbeat.elapsed() < self.limits.heartbeat_interval {
            return;
        }
        self.last_heartbeat = Instant::now();
        let env = serde_json::json!({
            "protocol": REMOTE_PROTOCOL_ID, "version": REMOTE_PROTOCOL_VERSION,
            "type": "heartbeat", "message_id": Session::fresh_id("m"),
            "session_id": self.session.session_id,
            "body": {},
        });
        if let Ok(frame) = Session::encode_frame(&env, self.limits.max_frame) {
            if self.control_q.len() < self.limits.control_queue {
                self.control_q.push_back(QueuedFrame {
                    bytes: frame,
                    deadline: Instant::now() + self.limits.frame_budget,
                });
            }
        }
    }

    fn check_timers(&mut self) -> bool {
        let idle = self.session.info.lock().unwrap().last_inbound.elapsed();
        if idle >= self.limits.detach_after {
            self.detach("session detached");
            return false;
        }
        if idle >= self.limits.suspect_after {
            self.set_state(SessionState::Suspect);
        }
        true
    }

    fn run(&mut self) {
        loop {
            if self.session.shutdown.load(Ordering::SeqCst) {
                // Best-effort close notice, then terminal detach.
                let env = serde_json::json!({
                    "protocol": REMOTE_PROTOCOL_ID, "version": REMOTE_PROTOCOL_VERSION,
                    "type": "session.close", "message_id": Session::fresh_id("m"),
                    "session_id": self.session.session_id,
                    "body": {"reason": "shutdown"},
                });
                if let Ok(frame) = Session::encode_frame(&env, self.limits.max_frame) {
                    let _ = self.io.conn.writer().write_all(&frame);
                    let deadline = Instant::now() + Duration::from_millis(500);
                    let _ = self.io.drive(|c| !c.wants_write(), deadline);
                }
                self.detach("session detached");
                return;
            }
            self.drain_commands();
            self.expire_pending();
            self.heartbeat_due();
            if !self.check_timers() {
                return;
            }
            // Interest: always readable; writable while flushing or queued.
            let mut events = libc::POLLIN;
            if !self.control_q.is_empty()
                || !self.data_q.is_empty()
                || self.io.conn.wants_write()
            {
                events |= libc::POLLOUT;
            }
            match self.io.poll_until(events, Instant::now() + Duration::from_millis(25)) {
                Ok(_) => {}
                Err(_) => {
                    self.detach("transport error");
                    return;
                }
            }
            // Inbound first: control answers must not wait behind our sends.
            match self.pump_reads() {
                Ok(frames) => {
                    for f in frames {
                        self.dispatch(f);
                    }
                }
                Err(e) if e == "clean-eof" => {
                    self.detach("peer closed");
                    return;
                }
                Err(e) => {
                    self.detach(&format!("transport error: {e}"));
                    return;
                }
            }
            // Outbound: at most one frame per round keeps inbound fresh.
            if !self.control_q.is_empty() || !self.data_q.is_empty() {
                if self.flush_one().is_err() {
                    self.detach("frame deadline exceeded");
                    return;
                }
            }
            self.expire_pending();
        }
    }

    /// Non-blocking TLS read round; returns complete decoded frames.
    /// `Err("clean-eof")` only when TCP hit EOF with nothing buffered or
    /// partial; partial buffered bytes make it a transport error instead.
    fn pump_reads(&mut self) -> Result<Vec<Value>, String> {
        let eof = {
            let io = &mut self.io;
            match io.conn.complete_io(&mut io.tcp) {
                Ok(_) => false,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => true,
                Err(e) => return Err(e.to_string()),
            }
        };
        if eof {
            self.io.tcp_eof = true;
        }
        if let Err(e) = self.io.conn.process_new_packets() {
            return Err(e.to_string());
        }
        let max = self.limits.max_frame;
        let mut out = vec![];
        // Bounded work per pump call: the loop runs every round, so capping
        // here only defers (never drops) progress — and turns any drain
        // pathology into a loud poison instead of a silent stall.
        let mut drained_bytes = 0usize;
        let drain_cap = max.saturating_mul(4).max(1 << 20);
        loop {
            if drained_bytes >= drain_cap {
                break;
            }
            let mut chunk = [0u8; 8192];
            match self.io.conn.reader().read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    drained_bytes += n;
                    // Feed/take in turns: a read completing N frames takes
                    // all of them; the tail starts the next frame. Every
                    // pass consumes bytes or takes a frame, so this ends.
                    let mut rest = &chunk[..n];
                    loop {
                        let fed = self.io.assembler.feed(rest, max);
                        match fed {
                            Ok(r) => rest = r,
                            Err(_) => {
                                self.io.poison();
                                return Err("frame too large".into());
                            }
                        }
                        let mut took = false;
                        loop {
                            let taken = self.io.assembler.take_frame();
                            match taken {
                                Ok(Some(raw)) => {
                                    took = true;
                                    match TlsIo::decode_frame(&raw) {
                                        Ok(v) => out.push(v),
                                        Err(_) => {
                                            self.session.info.lock().unwrap().dropped_malformed += 1;
                                        }
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => return Err(err(e)),
                            }
                        }
                        if rest.is_empty() {
                            break;
                        }
                        if !took {
                            self.io.poison();
                            return Err("assembler stall".into());
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.to_string()),
            }
        }
        if self.io.tcp_eof {
            if out.is_empty() && self.io.assembler.is_empty() {
                return Err("clean-eof".into());
            }
            if out.is_empty() {
                return Err("truncated frame".into());
            }
        }
        Ok(out)
    }
}
