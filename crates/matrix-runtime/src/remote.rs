//! Authenticated remote host profile. TLS verifies chain + server name; operator
//! grants pin client certificate fingerprints. One bounded request per TLS
//! connection; no implicit replay. The managed profile owns the remote lease.
use crate::service::Service;
use crate::store::Result;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

pub const PROFILE: &str = "matrix.managed/0.1";
const MAX_FRAME: usize = 1024 * 1024;
const MAX_CONNECTIONS: usize = 32;
const REQUEST_BUDGET: Duration = Duration::from_secs(35);
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
pub fn fingerprint(cert: &[u8]) -> String {
    Sha256::digest(cert)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub(crate) fn cert(path: &Path) -> Result<CertificateDer<'static>> {
    Ok(CertificateDer::from(fs::read(path).map_err(err)?))
}
pub(crate) fn key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::try_from(fs::read(path).map_err(err)?).map_err(err)
}
pub(crate) fn roots(ca: &Path) -> Result<RootCertStore> {
    let mut r = RootCertStore::empty();
    r.add(cert(ca)?).map_err(err)?;
    Ok(r)
}
pub(crate) fn provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
pub fn server_config(ca: &Path, chain: &Path, private: &Path) -> Result<Arc<ServerConfig>> {
    provider();
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots(ca)?))
        .build()
        .map_err(err)?;
    let mut config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![cert(chain)?], key(private)?)
        .map_err(err)?;
    config.alpn_protocols = vec![PROFILE.as_bytes().to_vec()];
    Ok(Arc::new(config))
}
pub fn client_config(ca: &Path, chain: &Path, private: &Path) -> Result<Arc<ClientConfig>> {
    provider();
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots(ca)?)
        .with_client_auth_cert(vec![cert(chain)?], key(private)?)
        .map_err(err)?;
    config.alpn_protocols = vec![PROFILE.as_bytes().to_vec()];
    Ok(Arc::new(config))
}
struct DeadlineTcp {
    tcp: TcpStream,
    until: Instant,
}
impl Read for DeadlineTcp {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        let left = self
            .until
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "absolute request deadline")
            })?;
        self.tcp.set_read_timeout(Some(left))?;
        self.tcp.read(b)
    }
}
impl Write for DeadlineTcp {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let left = self
            .until
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "absolute request deadline")
            })?;
        self.tcp.set_write_timeout(Some(left))?;
        self.tcp.write(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.tcp.flush()
    }
}
fn receive(r: &mut impl Read) -> Result<Value> {
    let mut header = [0; 4];
    r.read_exact(&mut header).map_err(err)?;
    let n = u32::from_be_bytes(header) as usize;
    if n == 0 || n > MAX_FRAME {
        return Err("invalid frame size".into());
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b).map_err(err)?;
    matrix_proto::envelope::scan_raw(&b).map_err(|e| e.code.to_string())
}
fn send(w: &mut impl Write, v: &Value) -> Result<()> {
    let b = serde_json::to_vec(v).map_err(err)?;
    if b.len() > MAX_FRAME {
        return Err("frame too large".into());
    }
    w.write_all(&(b.len() as u32).to_be_bytes()).map_err(err)?;
    w.write_all(&b).map_err(err)?;
    w.flush().map_err(err)
}
fn string<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v.get(k)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing {k}"))
}
fn fence(v: &Value) -> Result<u64> {
    string(v, "fence")?.parse().map_err(err)
}
fn dispatch(s: &Service, principal: &str, v: &Value) -> Result<Value> {
    if string(v, "profile")? != PROFILE {
        return Err("unsupported-version".into());
    }
    if !s.authorized(principal) {
        return Err("permission-denied".into());
    }
    match string(v, "action")? {
        "activate" => s.activate(
            principal,
            string(v, "component")?,
            v["ttl_ms"].as_u64().ok_or("missing ttl_ms")?,
        ),
        "status" => s.lease_status(principal, string(v, "lease")?, fence(v)?),
        "lease-status" => {
            let token = string(v, "lease")?;
            s.lease_status_by_token(principal, token)
        }
        "renew" => s.renew(
            principal,
            string(v, "lease")?,
            fence(v)?,
            v["ttl_ms"].as_u64().ok_or("missing ttl_ms")?,
        ),
        "release" => s.release(principal, string(v, "lease")?, fence(v)?),
        "invoke" => s.invoke(
            principal,
            string(v, "lease")?,
            fence(v)?,
            string(v, "operation")?,
            string(v, "cap")?,
            v.get("input").ok_or("missing input")?,
        ),
        "effect.commit" => s.commit_effect(
            principal,
            string(v, "lease")?,
            fence(v)?,
            string(v, "operation")?,
            string(v, "key")?,
            v.get("value").ok_or("missing value")?,
        ),
        "operation" => s.store.operation(principal, string(v, "operation")?),
        _ => Err("invalid-message".into()),
    }
}

pub struct Server {
    pub address: SocketAddr,
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}
impl Server {
    /// Binding is explicit; no listener is enabled by Service::open.
    pub fn bind(
        service: Arc<Service>,
        config: Arc<ServerConfig>,
        address: SocketAddr,
    ) -> Result<Self> {
        let listener = TcpListener::bind(address).map_err(err)?;
        listener.set_nonblocking(true).map_err(err)?;
        let address = listener.local_addr().map_err(err)?;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let join = std::thread::spawn(move || {
            let active = Arc::new(AtomicUsize::new(0));
            let mut workers: Vec<std::thread::JoinHandle<()>> = vec![];
            while !flag.load(Ordering::SeqCst) {
                let mut i = 0;
                while i < workers.len() {
                    if workers[i].is_finished() {
                        let _ = workers.swap_remove(i).join();
                    } else {
                        i += 1;
                    }
                }
                match listener.accept() {
                    Ok((tcp, _)) => {
                        if active.load(Ordering::SeqCst) >= MAX_CONNECTIONS {
                            drop(tcp);
                            continue;
                        }
                        active.fetch_add(1, Ordering::SeqCst);
                        let count = active.clone();
                        let service = service.clone();
                        let config = config.clone();
                        workers.push(std::thread::spawn(move || {
                            struct Count(Arc<AtomicUsize>);
                            impl Drop for Count {
                                fn drop(&mut self) {
                                    self.0.fetch_sub(1, Ordering::SeqCst);
                                }
                            }
                            let _count = Count(count);
                            let run = || -> Result<()> {
                                let mut stream = StreamOwned::new(
                                    ServerConnection::new(config).map_err(err)?,
                                    DeadlineTcp {
                                        tcp,
                                        until: Instant::now() + REQUEST_BUDGET,
                                    },
                                );
                                while stream.conn.is_handshaking() {
                                    stream.conn.complete_io(&mut stream.sock).map_err(err)?;
                                }
                                if stream.conn.alpn_protocol() != Some(PROFILE.as_bytes()) {
                                    return Err("unsupported profile".into());
                                }
                                let cert = stream
                                    .conn
                                    .peer_certificates()
                                    .and_then(|v| v.first())
                                    .ok_or("unauthenticated")?;
                                let principal = fingerprint(cert.as_ref());
                                if !service.authorized(&principal) {
                                    send(
                                        &mut stream,
                                        &json!({"ok":false,"error":"permission-denied"}),
                                    )?;
                                    return Ok(());
                                }
                                let request = receive(&mut stream)?;
                                let response = match dispatch(&service, &principal, &request) {
                                    Ok(v) => json!({"ok":true,"value":v}),
                                    Err(e) => json!({"ok":false,"error":e}),
                                };
                                send(&mut stream, &response)?;
                                stream.conn.send_close_notify();
                                stream.flush().map_err(err)?;
                                Ok(())
                            };
                            let _ = run();
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    Err(_) => break,
                }
            }
            for w in workers {
                let _ = w.join();
            }
        });
        Ok(Self {
            address,
            stop,
            join: Some(join),
        })
    }
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Clone)]
pub struct Client {
    pub address: SocketAddr,
    pub server_name: String,
    pub config: Arc<ClientConfig>,
}
impl Client {
    /// A transport error never triggers a second attempt.
    pub fn request(&self, value: Value) -> Result<Value> {
        self.request_with_budget(value, REQUEST_BUDGET, None)
    }
    pub fn request_with_budget(
        &self,
        mut value: Value,
        budget: Duration,
        cancel: Option<Arc<AtomicBool>>,
    ) -> Result<Value> {
        let until = Instant::now() + budget.min(REQUEST_BUDGET);
        value["profile"] = json!(PROFILE);
        let tcp = TcpStream::connect_timeout(
            &self.address,
            budget
                .min(Duration::from_secs(3))
                .max(Duration::from_millis(1)),
        )
        .map_err(err)?;
        struct Watch {
            done: Arc<AtomicBool>,
            join: Option<std::thread::JoinHandle<()>>,
        }
        impl Drop for Watch {
            fn drop(&mut self) {
                self.done.store(true, Ordering::SeqCst);
                if let Some(j) = self.join.take() {
                    let _ = j.join();
                }
            }
        }
        let mut watch = Watch {
            done: Arc::new(AtomicBool::new(false)),
            join: None,
        };
        if let Some(cancel) = cancel {
            let socket = tcp.try_clone().map_err(err)?;
            let done = watch.done.clone();
            watch.join = Some(std::thread::spawn(move || {
                while !done.load(Ordering::SeqCst) {
                    if cancel.load(Ordering::SeqCst) {
                        let _ = socket.shutdown(std::net::Shutdown::Both);
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }));
        }
        let name = ServerName::try_from(self.server_name.clone()).map_err(err)?;
        let mut stream = StreamOwned::new(
            ClientConnection::new(self.config.clone(), name).map_err(err)?,
            DeadlineTcp { tcp, until },
        );
        send(&mut stream, &value)?;
        let result = receive(&mut stream)?;
        if result["ok"] == true {
            Ok(result["value"].clone())
        } else {
            Err(result["error"].as_str().unwrap_or("remote error").into())
        }
    }
}

/// Adapter for a controller kernel: provision the remote component on its host,
/// then bind this forwarder to the controller's exact external instance.
/// Local/remote mappings are operator-controlled. Dropping/revoking the proxy
/// should call release; the owner lease expires even if the controller vanishes.
pub struct RemoteForwarder {
    pub client: Client,
    pub logical: String,
    pub instance: u64,
    pub generation: u64,
    pub lease: String,
    pub fence: u64,
}
impl matrix_core::CallForwarder for RemoteForwarder {
    fn forward(&self, r: &matrix_core::ForwardRequest) -> matrix_core::ForwardOutcome {
        use matrix_core::{ForwardError as E, ForwardOutcome as O};
        if r.logical != self.logical
            || r.instance.0 != self.instance
            || r.generation != self.generation
        {
            return O::Err {
                code: "stale-generation".into(),
                message: "proxy binding retired".into(),
            };
        }
        if r.cancel.load(Ordering::SeqCst) {
            return O::Failed(E::Cancelled);
        }
        let id = match matrix_guard::random_token() {
            Ok(id) => id,
            Err(e) => return O::Failed(E::Gone(e.to_string())),
        };
        let result=self.client.request_with_budget(json!({"action":"invoke","lease":self.lease,"fence":self.fence.to_string(),"operation":id,"cap":r.cap,"input":r.input}),Duration::from_millis(r.timeout_ms.max(1)),Some(r.cancel.clone()));
        if r.cancel.load(Ordering::SeqCst) {
            return O::Failed(E::Cancelled);
        }
        match result {
            Ok(v) if v["ok"] == true => O::Ok(v["value"].clone()),
            Ok(v) => O::Err {
                code: v["value"]["code"].as_str().unwrap_or("remote-error").into(),
                message: v["value"].to_string(),
            },
            Err(e) => O::Failed(E::Gone(e)),
        }
    }
}

/// Lifecycle integration for a single remote proxy. Existing local forwarder
/// and hook are preserved, so a controller can compose local and remote plugins.
/// A bounded worker renews the owner lease and removes the local definition on
/// lost authority. No automatic replay of a failed call.
pub struct RemoteProxy {
    request_gate: std::sync::Mutex<()>,
    forward: std::sync::Mutex<RemoteForwarder>,
    previous_forward: Option<Arc<dyn matrix_core::CallForwarder>>,
    previous_hook: Option<Arc<dyn matrix_core::LifecycleHook>>,
    retired: AtomicBool,
}
impl RemoteProxy {
    pub fn attach(
        kernel: &Arc<matrix_core::Kernel>,
        client: Client,
        remote_component: &str,
        local_manifest: &Path,
    ) -> Result<Arc<Self>> {
        let candidate: Value =
            serde_json::from_slice(&fs::read(local_manifest).map_err(err)?).map_err(err)?;
        let def = matrix_core::kernel::parse_manifest_value(&candidate)?;
        if !def.execution.is_external() {
            return Err("proxy manifest must use external execution".into());
        }
        let lease = client
            .request(json!({"action":"activate","component":remote_component,"ttl_ms":5000}))?;
        let token = string(&lease, "lease")?.to_string();
        let fence = fence(&lease)?;
        let ready_until = Instant::now() + Duration::from_secs(3);
        loop {
            let status = client.request_with_budget(
                json!({"action":"status","lease":token,"fence":fence.to_string()}),
                Duration::from_secs(1),
                None,
            );
            if status.as_ref().is_ok_and(|v| v["ready"] == true) {
                break;
            }
            if status.is_err() || Instant::now() >= ready_until {
                let _ = client.request_with_budget(
                    json!({"action":"release","lease":token,"fence":fence.to_string()}),
                    Duration::from_secs(1),
                    None,
                );
                return Err("remote component not ready".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let logical = match kernel.load_manifest(&local_manifest.to_path_buf()) {
            Ok(id) => id,
            Err(e) => {
                let _ = client
                    .request(json!({"action":"release","lease":token,"fence":fence.to_string()}));
                return Err(e);
            }
        };
        let reference = kernel
            .instance_ref_of(&logical)
            .ok_or("proxy not registered")?;
        if !kernel
            .definitions
            .lock()
            .get(&logical)
            .is_some_and(|d| d.execution.is_external())
        {
            return Err("proxy manifest must use external execution".into());
        }
        let proxy = Arc::new(Self {
            request_gate: std::sync::Mutex::new(()),
            forward: std::sync::Mutex::new(RemoteForwarder {
                client,
                logical,
                instance: reference.instance,
                generation: reference.generation,
                lease: token,
                fence,
            }),
            previous_forward: kernel.forwarder.lock().clone(),
            previous_hook: kernel.hook.lock().clone(),
            retired: AtomicBool::new(false),
        });
        kernel.set_forwarder(proxy.clone());
        kernel.set_hook(proxy.clone());
        let weak = Arc::downgrade(&proxy);
        let k = Arc::downgrade(kernel);
        std::thread::spawn(move || {
            let mut renewal = Instant::now() + Duration::from_secs(1);
            loop {
                std::thread::sleep(Duration::from_millis(20));
                let Some(p) = weak.upgrade() else { break };
                if k.upgrade().is_none() {
                    p.retired.store(true, Ordering::SeqCst);
                }
                if p.retired.load(Ordering::SeqCst) {
                    let f = p.forward.lock().unwrap();
                    let _ = f.client.request_with_budget(
                        json!({"action":"release","lease":f.lease,"fence":f.fence.to_string()}),
                        Duration::from_secs(2),
                        None,
                    );
                    break;
                }
                if Instant::now() < renewal {
                    continue;
                }
                let _gate = p.request_gate.lock().unwrap();
                let (client, token, fence, logical) = {
                    let f = p.forward.lock().unwrap();
                    (
                        f.client.clone(),
                        f.lease.clone(),
                        f.fence,
                        f.logical.clone(),
                    )
                };
                let result = client.request_with_budget(
                    json!({"action":"renew","lease":token,"fence":fence.to_string(),"ttl_ms":5000}),
                    Duration::from_secs(2),
                    None,
                );
                match result {
                    Ok(v) => match string(&v, "lease") {
                        Ok(token) => p.forward.lock().unwrap().lease = token.to_string(),
                        Err(_) => p.retired.store(true, Ordering::SeqCst),
                    },
                    Err(_) => {
                        p.retired.store(true, Ordering::SeqCst);
                        if let Some(k) = k.upgrade() {
                            k.dispose_plugin(&logical);
                        }
                    }
                }
                renewal = Instant::now() + Duration::from_secs(1);
            }
        });
        Ok(proxy)
    }
}
impl matrix_core::CallForwarder for RemoteProxy {
    fn forward(&self, r: &matrix_core::ForwardRequest) -> matrix_core::ForwardOutcome {
        let own = self.forward.lock().unwrap().logical == r.logical;
        if !own {
            return self
                .previous_forward
                .as_ref()
                .map(|p| p.forward(r))
                .unwrap_or(matrix_core::ForwardOutcome::Failed(
                    matrix_core::ForwardError::Gone("no route".into()),
                ));
        }
        let _gate = self.request_gate.lock().unwrap();
        let f = {
            let f = self.forward.lock().unwrap();
            if r.logical != f.logical {
                drop(f);
                return self
                    .previous_forward
                    .as_ref()
                    .map(|p| p.forward(r))
                    .unwrap_or(matrix_core::ForwardOutcome::Failed(
                        matrix_core::ForwardError::Gone("no route".into()),
                    ));
            }
            RemoteForwarder {
                client: f.client.clone(),
                logical: f.logical.clone(),
                instance: f.instance,
                generation: f.generation,
                lease: f.lease.clone(),
                fence: f.fence,
            }
        };
        if self.retired.load(Ordering::SeqCst) {
            return matrix_core::ForwardOutcome::Failed(matrix_core::ForwardError::Gone(
                "remote lease retired".into(),
            ));
        }
        f.forward(r)
    }
}
impl matrix_core::LifecycleHook for RemoteProxy {
    fn on_lifecycle(&self, event: matrix_core::LifecycleEvent) {
        use matrix_core::LifecycleEvent::*;
        let f = self.forward.lock().unwrap();
        let own = match &event {
            Withdrawn {
                logical, instance, ..
            } => logical == &f.logical && *instance == f.instance,
            Removed { logical } => logical == &f.logical,
            Activated {
                logical, instance, ..
            } => logical == &f.logical && *instance != f.instance,
        };
        if own {
            self.retired.store(true, Ordering::SeqCst);
        }
        drop(f);
        if let Some(h) = &self.previous_hook {
            h.on_lifecycle(event);
        }
    }
}
