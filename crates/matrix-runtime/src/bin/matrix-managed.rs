//! Explicit managed profile entrypoint; legacy matrix-rt is unchanged.
use matrix_guard::{RestartPolicy, Sandbox};
use matrix_host::HostPolicy;
use matrix_runtime::{
    remote,
    service::{Grant, Service},
    store::Store,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

static STOP: AtomicBool = AtomicBool::new(false);
static RELOAD: AtomicBool = AtomicBool::new(false);
extern "C" fn signal(n: libc::c_int) {
    if n == libc::SIGHUP {
        RELOAD.store(true, Ordering::SeqCst);
    } else {
        STOP.store(true, Ordering::SeqCst);
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Component {
    manifest: Value,
    sandbox: Option<Sandbox>,
    #[serde(default)]
    trusted: bool,
    restart: Option<RestartPolicy>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Tls {
    listen: std::net::SocketAddr,
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
}
#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct RemotePeer {
    name: String,
    address: std::net::SocketAddr,
    server_name: String,
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    mgmt_address: std::net::SocketAddr,
    domain: String,
    lease_ttl_ms: u64,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct RemoteRoute {
    consumer: String,
    provider: String,
    peer: String,
    /// Capability snapshot for the remote provider (attested
    /// instance/generation still gates routing at runtime).
    #[serde(default)]
    capabilities: Vec<String>,
}

#[derive(Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
struct Remotes {
    /// Composition authority fingerprint (controller principal).
    #[serde(default)]
    authority: String,
    /// Composition domain stamped on remote legs (1..64, [A-Za-z0-9_-]).
    /// Empty = single-host operator (remote legs refuse as invalid).
    #[serde(default)]
    domain: String,
    #[serde(default)]
    peers: Vec<RemotePeer>,
    #[serde(default)]
    routes: Vec<RemoteRoute>,
    /// Executor session listener for `matrix.remote/0.1` (route B).
    /// Absent = controller-only (no inbound sessions).
    #[serde(default)]
    session_listen: Option<std::net::SocketAddr>,
    #[serde(default)]
    session_ca: Option<PathBuf>,
    #[serde(default)]
    session_cert: Option<PathBuf>,
    #[serde(default)]
    session_key: Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    home: PathBuf,
    components: Vec<Component>,
    grants: HashMap<String, Grant>,
    /// Operator outbound grants (consumer → exact capabilities).
    /// Independent of `outbound.request`; missing = denied.
    #[serde(default)]
    outbound_grants: HashMap<String, Vec<String>>,
    tls: Option<Tls>,
    /// M7 remote composition (controller routes + optional executor
    /// session listener). Absent/empty = local-only (no route manager).
    #[serde(default)]
    remotes: Remotes,
}
fn read(path: &str) -> Result<Config, String> {
    serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
}
fn run() -> Result<(), String> {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str){
        Some("serve")=>{
            let path=args.get(2).ok_or("missing config path")?;let cfg=read(path)?;
            if !cfg.home.is_absolute(){return Err("home must be absolute".into());}
            let mut policy=HostPolicy{secure:true,components:HashMap::new(),enable_dependency_calls:false,domain:cfg.remotes.domain.clone()};let mut restart=HashMap::new();
            for c in &cfg.components {
                let id=c.manifest["id"].as_str().ok_or("missing component id")?.to_string();
                if c.sandbox.is_none()&&!c.trusted{return Err(format!("{id}: select sandbox or explicitly trusted"));}
                if policy.components.insert(id.clone(),c.sandbox.clone()).is_some(){return Err("duplicate component".into());}
                if let Some(p)=&c.restart{restart.insert(id,p.clone());}
            }
            let mut peers:HashSet<String>=cfg.grants.keys().cloned().collect();
            let service=Service::open(&cfg.home,policy,cfg.grants,restart)?;
            service.sync_outbound_grants(&cfg.outbound_grants);
            for c in cfg.components {
                // Remote-only definitions (route capability snapshots) never
                // materialize locally; the route manager installs them.
                if c.manifest.get("remote") == Some(&json!(true)) {
                    service.provision_remote(&c.manifest)?;
                } else {
                    service.provision(&c.manifest)?;
                }
            }
            let mut server=cfg.tls.map(|t|remote::Server::bind(service.clone(),remote::server_config(&t.ca,&t.cert,&t.key)?,t.listen)).transpose()?;
            // M7 executor session listener (route B). Shares the 32-connection
            // cap with the unary profile; absent = controller-only.
            let sess_server = match (&cfg.remotes.session_listen, &cfg.remotes.session_ca, &cfg.remotes.session_cert, &cfg.remotes.session_key) {
                (Some(listen), Some(ca), Some(cert), Some(key)) => {
                    let sc = matrix_runtime::session::server_config(ca, cert, key)?;
                    Some(service.serve_remote_session(*listen, sc)?)
                }
                (None, None, None, None) => None,
                _ => return Err("remotes.session_* must be all present or all absent".into()),
            };
            // M7 controller route manager (route A). Empty peers/routes =
            // local-only; sync() on SIGHUP tears down removed peers
            // (registrations withdrawn, leases released) and attaches new ones.
            let routes: Vec<matrix_runtime::route_controller::RouteSpec> = cfg.remotes.routes.iter().map(|r| matrix_runtime::route_controller::RouteSpec { consumer: r.consumer.clone(), provider: r.provider.clone(), peer: r.peer.clone() }).collect();
            // Ensure remote capability snapshots exist for routed providers
            // (operator-declared caps; attested registration still gates routing).
            for r in &cfg.remotes.routes {
                let caps: Vec<Value> = r.capabilities.iter().map(|c| json!(c)).collect();
                let _ = service.provision_remote(&json!({"id": r.provider, "capabilities": caps, "remote": true}));
            }
            let mgr = matrix_runtime::route_controller::RouteManager::new(service.clone(), cfg.remotes.authority.clone());
            let to_peer = |p: &RemotePeer| matrix_runtime::route_controller::PeerConfig {
                name: p.name.clone(), address: p.address, server_name: p.server_name.clone(),
                ca: p.ca.clone(), cert: p.cert.clone(), key: p.key.clone(),
                mgmt_address: p.mgmt_address, domain: p.domain.clone(), lease_ttl_ms: p.lease_ttl_ms,
            };
            mgr.sync(cfg.remotes.peers.iter().map(to_peer).collect(), routes);
            unsafe{libc::signal(libc::SIGINT,signal as *const () as libc::sighandler_t);libc::signal(libc::SIGTERM,signal as *const () as libc::sighandler_t);libc::signal(libc::SIGHUP,signal as *const () as libc::sighandler_t);}
            println!("{}",json!({"ready":true,"profile":remote::PROFILE,"epoch":service.store.epoch.to_string(),"listen":server.as_ref().map(|s|s.address.to_string()),"session":sess_server.as_ref().map(|s|s.address.to_string())}));
            use std::io::Write;std::io::stdout().flush().map_err(|e|e.to_string())?;
            while !STOP.load(Ordering::SeqCst){
                if RELOAD.swap(false,Ordering::SeqCst){
                    match read(path){
                        Ok(new)=>{
                            let current:HashSet<_>=new.grants.keys().cloned().collect();
                            // Retire removed and changed grants first; never widen
                            // authority of already-issued leases silently.
                            for principal in &peers {service.revoke(principal)?;}
                            for (principal,grant) in new.grants{service.grant(principal,grant)?;}
                            peers=current;
                            service.sync_outbound_grants(&new.outbound_grants);
                            // Re-sync remote routes (added/removed peers and routes).
                            // Session listener changes require restart (explicit).
                            let routes: Vec<matrix_runtime::route_controller::RouteSpec> = new.remotes.routes.iter().map(|r| matrix_runtime::route_controller::RouteSpec { consumer: r.consumer.clone(), provider: r.provider.clone(), peer: r.peer.clone() }).collect();
                            for r in &new.remotes.routes {
                                let caps: Vec<Value> = r.capabilities.iter().map(|c| json!(c)).collect();
                                let _ = service.provision_remote(&json!({"id": r.provider, "capabilities": caps, "remote": true}));
                            }
                            mgr.sync(new.remotes.peers.iter().map(to_peer).collect(), routes);
                        },Err(e)=>eprintln!("grant reload rejected: {e}"),
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            mgr.shutdown();
            service.shutdown();if let Some(s)=&mut server{s.shutdown();}let _ = sess_server.map(|mut s| {s.shutdown();});Ok(())
        },
        Some("snapshot")=>{
            let home=PathBuf::from(args.get(2).ok_or("missing home")?);let target=PathBuf::from(args.get(3).ok_or("missing destination")?);
            Store::open(&home.join("state/runtime.sqlite"))?.snapshot(&target)?;println!("snapshot written");Ok(())
        },
        Some("fingerprint")=>{
            let cert=std::fs::read(args.get(2).ok_or("missing DER certificate")?).map_err(|e|e.to_string())?;
            println!("{}",remote::fingerprint(&cert));Ok(())
        },
        Some("request")=>{
            if args.len()!=8{return Err("request <ca.der> <cert.der> <key.der> <address> <server-name> <json>".into());}
            let client=remote::Client{address:args[5].parse().map_err(|e:std::net::AddrParseError|e.to_string())?,server_name:args[6].clone(),config:remote::client_config(std::path::Path::new(&args[2]),std::path::Path::new(&args[3]),std::path::Path::new(&args[4]))?};
            let request=serde_json::from_str(&args[7]).map_err(|e|e.to_string())?;
            println!("{}",client.request(request)?);Ok(())
        },
        _=>Err("usage: matrix-managed serve <config.json> | snapshot <home> <new.db> | fingerprint <cert.der> | request <ca.der> <cert.der> <key.der> <address> <server-name> <json>".into()),
    }
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
