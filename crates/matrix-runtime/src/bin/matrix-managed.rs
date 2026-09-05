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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    home: PathBuf,
    components: Vec<Component>,
    grants: HashMap<String, Grant>,
    tls: Option<Tls>,
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
            let mut policy=HostPolicy{secure:true,components:HashMap::new()};let mut restart=HashMap::new();
            for c in &cfg.components {
                let id=c.manifest["id"].as_str().ok_or("missing component id")?.to_string();
                if c.sandbox.is_none()&&!c.trusted{return Err(format!("{id}: select sandbox or explicitly trusted"));}
                if policy.components.insert(id.clone(),c.sandbox.clone()).is_some(){return Err("duplicate component".into());}
                if let Some(p)=&c.restart{restart.insert(id,p.clone());}
            }
            let mut peers:HashSet<String>=cfg.grants.keys().cloned().collect();
            let service=Service::open(&cfg.home,policy,cfg.grants,restart)?;
            for c in cfg.components{service.provision(&c.manifest)?;}
            let mut server=cfg.tls.map(|t|remote::Server::bind(service.clone(),remote::server_config(&t.ca,&t.cert,&t.key)?,t.listen)).transpose()?;
            unsafe{libc::signal(libc::SIGINT,signal as *const () as libc::sighandler_t);libc::signal(libc::SIGTERM,signal as *const () as libc::sighandler_t);libc::signal(libc::SIGHUP,signal as *const () as libc::sighandler_t);}
            println!("{}",json!({"ready":true,"profile":remote::PROFILE,"epoch":service.store.epoch.to_string(),"listen":server.as_ref().map(|s|s.address.to_string())}));
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
                        },Err(e)=>eprintln!("grant reload rejected: {e}"),
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            service.shutdown();if let Some(s)=&mut server{s.shutdown();}Ok(())
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
