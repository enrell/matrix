//! Host local de processos (M2.2).
//!
//! Executa plugins externos: recebe [`LifecycleEvent`]s do kernel (via
//! `LifecycleHook`, sem bloquear o kernel), gera processos filhos,
//! mantém sessões sobre socket Unix com o protocolo `matrix.component`
//! v0.1 e entrega chamadas ([`CallForwarder`]).
//!
//! Regras (cf. `docs/PROTOCOL.md`, `docs/REMOTE.md` futuro):
//! - Sessão amarrada a (lógico, instância, geração): resultado de geração
//!   antiga nunca alimenta a nova; `request_id` desconhecido é descartado.
//! - Desconexão no meio da chamada = `outcome-unknown`: sem sucesso falso,
//!   sem repetição automática.
//! - Cancelamento de código externo é best effort (sinal no fio +
//!   abandono da espera); revogação de autoridade vale nas fronteiras.
//! - Processo que não confirma dispose no prazo é morto (SIGKILL) e colhido.
//!
//! Prazos do perfil local (documentados, finitos): handshake 5s, registro
//! 10s, activate 5s, quiesce/dispose 2s cada, spawn→register 10s.

use matrix_core::{
    expand_args, CallForwarder, ForwardOutcome, ForwardRequest, Kernel, LifecycleEvent,
    LifecycleHook,
};
use matrix_proto::{
    encode, hello_offer, negotiate, parse_frame_payload, read_frame, split_frame, Envelope,
    IdWindow, IdVerdict, DEFAULT_MAX_FRAME,
};
use serde_json::{json, Value};
use std::collections::HashMap;
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
/// Chamadas encaminhadas simultâneas por sessão (I09, M2.3). Além disso:
/// `call.error resource-exhausted`, sem enfileirar sem limite.
pub const MAX_CALLS_PER_SESSION: usize = 16;
/// Respostas aguardadas no host inteiro (I09, M2.3).
pub const MAX_PENDING_PER_HOST: usize = 1024;
/// Janela inicial de bytes por stream (M2.3). Dados além do concedido
/// encerram o stream com erro; a sessão sobrevive.
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
    /// Chamada encaminhada (ticket do plugin, para cancel no fio).
    Call { ticket: String },
    Lifecycle,
}

struct Session {
    id: String,
    logical: String,
    instance: u64,
    generation: u64,
    writer: Mutex<UnixStream>,
    dedup: Mutex<IdWindow>,
    /// Chamadas encaminhadas aguardando resposta (teto M2.3).
    in_flight: Mutex<usize>,
    /// Streams do plugin: crédito concedido vs bytes recebidos (M2.3).
    streams: Mutex<HashMap<String, StreamState>>,
}

#[derive(Debug, Clone)]
struct StreamState {
    next_seq: u64,
    granted: u64,
    received: u64,
    /// Encerrado por excesso (tombstone): dados tardios ignorados, sem recriar.
    ended: bool,
}

/// Guarda decremento do in-flight da sessão (teto sem vazamento).
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

struct ChildEntry {
    child: Mutex<Child>,
}

#[derive(Clone, Default)]
pub struct HostPolicy {
    /// Missing entries are denied in secure mode. None = explicitly trusted process.
    pub components: HashMap<String, Option<Sandbox>>,
    pub secure: bool,
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
        // Não-bloqueante por contrato (canal sem limite, volume baixo).
        let _ = self.ev_tx.send(ev);
    }
}

impl Host {
    /// Anexa o host ao kernel: registra forwarder + hook, abre o listener,
    /// processa o backlog (instâncias externas já ativas) e solta as threads.
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
            }),
            ev_tx,
        });
        kernel.set_forwarder(host.clone() as Arc<dyn CallForwarder>);
        kernel.set_hook(host.clone() as Arc<dyn LifecycleHook>);

        // Backlog: definições externas já ativas ganham processo.
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
        let sessions = self.inner.sessions.lock().unwrap();
        for s in sessions.values() {
            if let Ok(dup) = s.writer.lock().unwrap().try_clone() {
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
                // Retira sessão/instância corrente (se houver) e esquece o filho.
                let cur = self.inner.kernel.contexts.current(&logical);
                if let Some(c) = cur {
                    self.quiesce_session(&logical, c.instance.0, c.generation);
                }
                self.inner.pending_activation.lock().unwrap().remove(&logical);
                self.kill_child(&logical);
            }
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
                // Falha de spawn: sem sessão; chamadas falham Gone (sem sucesso falso).
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

    /// Espera o processo sumir (para asserções; colhe zumbis).
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
        // Colhe filhos que já saíram e não têm mais sessão viva.
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
            // o plugin observa o cancel antes de quiesce/dispose (M2.3).
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

    /// Envia `call.cancel` a todas as chamadas encaminhadas da sessão
    /// (fire-and-forget; a resposta cai em request_id desconhecido).
    /// Chamado no início do quiesce para entrega determinística.
    fn cancel_session_calls(&self, sid: &str, reason: &str) {
        let sessions = self.inner.sessions.lock().unwrap();
        let Some(s) = sessions.get(sid) else { return };
        let tickets: Vec<String> = self
            .inner
            .pending
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.session == sid)
            .filter_map(|p| match &p.kind {
                PendingKind::Call { ticket } => Some(ticket.clone()),
                PendingKind::Lifecycle => None,
            })
            .collect();
        for ticket in tickets {
            let msg = json!({
                "protocol": matrix_proto::PROTOCOL_ID,
                "version": matrix_proto::PROTOCOL_VERSION,
                "type": "call.cancel",
                "message_id": self.fresh_id("msg"),
                "session_id": s.id,
                "instance_id": s.instance.to_string(),
                "generation": s.generation.to_string(),
                "request_id": self.fresh_id("req"),
                "body": {"ticket": ticket, "reason": reason},
            });
            let raw = serde_json::to_vec(&msg).unwrap_or_default();
            if let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) {
                use std::io::Write;
                let mut w = s.writer.lock().unwrap();
                let _ = w.write_all(&f);
                let _ = w.flush();
            }
        }
    }

    fn drop_session(&self, sid: &str) {
        let sess = self.inner.sessions.lock().unwrap().remove(sid);
        if let Some(s) = sess {
            self.inner.by_instance.lock().unwrap().remove(&(s.logical.clone(), s.instance));
            // Chamadas órfãs: outcome-unknown, sem repetição.
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
            if let Ok(dup) = s.writer.lock().unwrap().try_clone() {
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
        // Teto global primeiro: sob sobrecarga, rejeita rápido sem trabalhar.
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
        // Geração amarrada: sessão de geração antiga não serve a nova.
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
        // Teto por sessão (M2.3): além do limite, erro imediato sem fila.
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
                        // Cai aqui também o erro embutido de sessão perdida.
                        let err = env.body.get("error").cloned().unwrap_or_else(|| env.body.clone());
                        let code = err.get("code").and_then(|v| v.as_str()).unwrap_or("outcome-unknown").to_string();
                        let msg = err.get("message").and_then(|v| v.as_str()).unwrap_or("session lost").to_string();
                        return ForwardOutcome::Err { code, message: msg };
                    }
                    // call.error embutido do drop de sessão.
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
        // Valida o que enviamos (schema executável vale para os dois lados).
        let raw = serde_json::to_vec(&env).map_err(|e| e.to_string())?;
        let frame = encode(&raw, DEFAULT_MAX_FRAME).map_err(|e| e.to_string())?;
        let sessions = self.inner.sessions.lock().unwrap();
        let Some(s) = sessions.get(sid) else {
            return Err(format!("no session {}", sid));
        };
        if s.logical != logical || s.instance != instance || s.generation != generation {
            return Err("stale session binding".to_string());
        }
        let mut w = s.writer.lock().unwrap();
        use std::io::Write;
        w.write_all(&frame).map_err(|e| e.to_string())?;
        w.flush().map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Transação síncrona com o plugin (lifecycle): registra respondente,
    /// envia, espera o terminal com prazo. `Ok(None)` = sem sessão.
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

    // ---- lado plugin: accept + sessões ----

    fn accept_loop(&self, listener: UnixListener) {
        let _ = listener.set_nonblocking(false);
        loop {
            if self.inner.shutdown.load(Ordering::SeqCst) {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    // Wake do shutdown (conexão sem bytes): ignora e recheca.
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
            _ => return,
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
        let (versions, want_frame) = match hello_offer(&env) {
            Ok(v) => v,
            Err(e) => {
                let _ = self.write_reject(&mut stream, &env.message_id, &e.code, "bad hello body");
                return;
            }
        };
        let session_id = self.fresh_id("sess");
        let welcome = match negotiate(&versions, want_frame, session_id.clone()) {
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
                     "limits": {"max_calls": 64, "max_queue_bytes": 4194304, "max_streams": 16}},
        });
        if self.write_value(&mut stream, &wmsg, max_frame).is_err() {
            return;
        }
        // 2. component.register → bind à instância (definição manda).
        let _ = stream.set_read_timeout(Some(Duration::from_millis(REGISTER_MS)));
        let raw = match read_frame(&mut stream, max_frame) {
            Ok(Some(r)) => r,
            _ => return,
        };
        let reg = match parse_frame_payload(&raw) {
            Ok(e) if e.ty == "component.register" => e,
            _ => return,
        };
        // Sessão do hello deve ser citada.
        if reg.session_id.as_deref() != Some(session_id.as_str()) {
            return;
        }
        let child_id = reg.body.get("manifest").and_then(|m| m.get("id")).and_then(|v| v.as_str()).unwrap_or("").to_string();
        if child_id.is_empty() {
            return;
        }
        // Amarra: ativação pendente (spawn nosso) ou instância Active vigente.
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
        // Uma sessão viva por instância (sem roubo de binding).
        {
            let by = self.inner.by_instance.lock().unwrap();
            if by.contains_key(&(child_id.clone(), instance)) {
                let _ = self.write_reject(&mut stream, &reg.message_id, "internal", "session already bound");
                return;
            }
        }
        // Dedup de register (repetição idempotente na retenção do host).
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
        self.inner.sessions.lock().unwrap().insert(
            session_id.clone(),
            Session {
                id: session_id.clone(),
                logical: child_id.clone(),
                instance,
                generation,
                writer: Mutex::new(writer),
                dedup: Mutex::new(IdWindow::new(256)),
                in_flight: Mutex::new(0),
                streams: Mutex::new(HashMap::new()),
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
        if self.write_value(&mut stream, &registered, max_frame).is_err() {
            self.drop_session(&session_id);
            return;
        }
        // 3. Loop de leitura numa thread própria ANTES do activate: a resposta
        // do activate (e das chamadas) só chega via leitura do socket, logo
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
        // 4. activate com prazo (o kernel já ativou; aqui é o handshake do host).
        let op = self.fresh_id("op");
        match self.transact(
            &session_id, &child_id, instance, generation,
            "lifecycle.activate",
            json!({"operation_id": op, "manifest": {"id": child_id}, "bindings": []}),
            ACTIVATE_MS,
        ) {
            Ok(Some(_)) => {}
            _ => {
                self.drop_session(&session_id);
                self.kill_child(&child_id);
                return;
            }
        }
        // serve_conn encerra aqui; o read_loop detém o tempo de vida da sessão.
    }

    /// Loop de leitura da sessão: fatia frames e despacha. Sai em EOF/erro/
    /// abuso/shutdown e derruba a sessão (sem sucesso falso posterior).
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
                        break; // abuso: excede frame máximo negociado.
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
                                // encerra a sessão de forma limitada (M2.3).
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
            Err(_) => return, // lixo: ignora sem derrubar a sessão (M2.3 aperta).
        };
        // Confere amarração da sessão (identidades são alegações verificadas).
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
        // Dedup por message_id na sessão (repetição idempotente).
        {
            let sessions = self.inner.sessions.lock().unwrap();
            let Some(s) = sessions.get(sid) else { return };
            let canon = serde_json::to_string(&env.body).unwrap_or_default();
            let v = s.dedup.lock().unwrap().check(&env.message_id, &canon);
            if v != IdVerdict::New {
                return; // repetição idêntica: sem reexecutar; divergente: sem executar.
            }
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
                        use std::io::Write;
                        let mut w = s.writer.lock().unwrap();
                        let _ = w.write_all(&f);
                        let _ = w.flush();
                    }
                }
            }
            "session.close" => {
                self.drop_session(sid);
            }
            "stream.data" | "stream.credit" | "stream.end" => {
                self.on_stream_frame(sid, &env);
            }
            _ => {}
        }
    }

    /// Porta de streams com crédito (M2.3): o receptor (host) concede bytes;
    /// dados sem crédito encerram o stream com erro, sem derrubar a sessão.
    /// O primeiro `stream.data` de um id cria o stream com a janela inicial
    /// e recebe o `stream.credit` correspondente.
    fn on_stream_frame(&self, sid: &str, env: &Envelope) {
        let stream_id = env.body.get("stream_id").and_then(|v| v.as_str()).unwrap_or("");
        if stream_id.is_empty() || stream_id.len() > 256 {
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
                        // Tombstone: tardios ignorados, sem recriar nem contar.
                        None
                    } else {
                        let fresh = st.next_seq == 0;
                        if seq != st.next_seq {
                            // Duplicata/reordenação: ignora sem reexecutar.
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
            }
            "stream.end" => {
                // Plugin encerra o próprio stream: esquece sem erro.
                if let Some(s) = self.inner.sessions.lock().unwrap().get(sid) {
                    s.streams.lock().unwrap().remove(stream_id);
                }
            }
            // Crédito plugin→host não tem uso no perfil local (host não
            // envia streams em M2); ignorado sem erro.
            _ => {}
        }
    }

    fn send_stream_credit(&self, sid: &str, stream_id: &str, bytes: u64) {
        let sessions = self.inner.sessions.lock().unwrap();
        let Some(s) = sessions.get(sid) else { return };
        let msg = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "stream.credit",
            "message_id": self.fresh_id("msg"),
            "session_id": s.id,
            "instance_id": s.instance.to_string(),
            "generation": s.generation.to_string(),
            "body": {"stream_id": stream_id, "bytes": bytes},
        });
        let raw = serde_json::to_vec(&msg).unwrap_or_default();
        if let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) {
            use std::io::Write;
            let mut w = s.writer.lock().unwrap();
            let _ = w.write_all(&f);
            let _ = w.flush();
        }
    }

    fn end_stream(&self, sid: &str, stream_id: &str, status: &str, received: u64, granted: u64) {
        let sessions = self.inner.sessions.lock().unwrap();
        let Some(s) = sessions.get(sid) else { return };
        // Mantém o tombstone (dados em voo tardios ignorados, sem recriar).
        let msg = json!({
            "protocol": matrix_proto::PROTOCOL_ID,
            "version": matrix_proto::PROTOCOL_VERSION,
            "type": "stream.end",
            "message_id": self.fresh_id("msg"),
            "session_id": s.id,
            "instance_id": s.instance.to_string(),
            "generation": s.generation.to_string(),
            "body": {"stream_id": stream_id, "status": status,
                     "reason": format!("over credit: received {} granted {}", received, granted)},
        });
        let raw = serde_json::to_vec(&msg).unwrap_or_default();
        if let Ok(f) = encode(&raw, DEFAULT_MAX_FRAME) {
            use std::io::Write;
            let mut w = s.writer.lock().unwrap();
            let _ = w.write_all(&f);
            let _ = w.flush();
        }
    }

    fn write_value(&self, stream: &mut UnixStream, v: &Value, max_frame: usize) -> Result<(), String> {
        // Schema executável vale para o que enviamos também.
        let raw = serde_json::to_vec(v).map_err(|e| e.to_string())?;
        if raw.len() > max_frame {
            return Err("frame acima do negociado".to_string());
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

    // ---- inspeção de teste ----

    pub fn has_session(&self, logical: &str, instance: u64) -> bool {
        self.inner.by_instance.lock().unwrap().contains_key(&(logical.to_string(), instance))
    }

    pub fn session_count(&self) -> usize {
        self.inner.sessions.lock().unwrap().len()
    }

    pub fn child_running(&self, logical: &str) -> bool {
        self.inner.children.lock().unwrap().contains_key(logical)
    }

    /// Chamadas encaminhadas em espera por sessão (teto M2.3).
    pub fn in_flight_for(&self, logical: &str, instance: u64) -> usize {
        let by = self.inner.by_instance.lock().unwrap();
        let Some(sid) = by.get(&(logical.to_string(), instance)) else { return 0 };
        self.inner.sessions.lock().unwrap().get(sid)
            .map(|s| *s.in_flight.lock().unwrap())
            .unwrap_or(0)
    }

    /// Streams com crédito acompanhados na sessão (M2.3; só os não encerrados).
    pub fn stream_count_for(&self, logical: &str, instance: u64) -> usize {
        let by = self.inner.by_instance.lock().unwrap();
        let Some(sid) = by.get(&(logical.to_string(), instance)) else { return 0 };
        self.inner.sessions.lock().unwrap().get(sid)
            .map(|s| s.streams.lock().unwrap().values().filter(|st| !st.ended).count())
            .unwrap_or(0)
    }

    /// Streams encerrados por excesso ainda retidos como tombstone (M2.3).
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Teto global da fila de respostas (M2.3): cheio rejeita rápido, mesmo
    /// sem sessão; após esvaziar, o caminho normal retorna (outro erro).
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
        // Enche a fila com pendências fictícias.
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
            other => panic!("sem sessão, esperava Gone, veio {:?}", other),
        }
        host.shutdown();
    }
}
