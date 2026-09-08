//! Explicit managed profile entrypoint; legacy matrix-rt is unchanged.
use matrix_runtime::{
    remote,
};
use serde_json::{json, Value};

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
fn run() -> Result<(), String> {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str){
        Some("serve")=>{
            use matrix_runtime::api::{Config, Runtime};
            let cfg_path=args.get(2).ok_or("missing config path")?;
            let raw: Value = serde_json::from_slice(&std::fs::read(cfg_path).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
            let cfg = Config::parse(&raw).map_err(|ds| {
                format!("invalid config: {}", ds.iter().map(|d| format!("{}: {}", d.field, d.message)).collect::<Vec<_>>().join("; "))
            })?;
            // Validate-before-mutate at boot too: nothing starts half configured.
            let diags = cfg.validate();
            if !diags.is_empty() {
                return Err(format!("invalid config: {}", diags.iter().map(|d| format!("{}: {}", d.field, d.message)).collect::<Vec<_>>().join("; ")));
            }
            let rt = Runtime::start(&cfg).map_err(|e| e.to_string())?;
            let epoch = rt.inspect(matrix_runtime::api::InspectOpts::default())["epoch"].clone();
            unsafe{libc::signal(libc::SIGINT,signal as *const () as libc::sighandler_t);libc::signal(libc::SIGTERM,signal as *const () as libc::sighandler_t);libc::signal(libc::SIGHUP,signal as *const () as libc::sighandler_t);}
            println!("{}",json!({"ready":true,"profile":remote::PROFILE,"api":matrix_runtime::api::API_VERSION,"epoch":epoch,"listen":rt.unary_addr().map(|a|a.to_string()),"session":rt.session_addr().map(|a|a.to_string())}));
            use std::io::Write;std::io::stdout().flush().map_err(|e|e.to_string())?;
            while !STOP.load(Ordering::SeqCst){
                if RELOAD.swap(false,Ordering::SeqCst){
                    match serde_json::from_slice::<Value>(&std::fs::read(cfg_path).map_err(|e| e.to_string())?).map_err(|e| e.to_string()).and_then(|raw| Config::parse(&raw).map_err(|ds| format!("invalid config: {}", ds.iter().map(|d| format!("{}: {}", d.field, d.message)).collect::<Vec<_>>().join("; ")))) {
                        Ok(new)=>{
                            // Rotation, restart-gating and per-area report
                            // all live in the facade now: one call, one
                            // verdict, no split behavior per caller.
                            match rt.reload(&new) {
                                Ok(rep) => {
                                    if !rep.revoked.is_empty() {
                                        eprintln!("reload revoked: {}", rep.revoked.join(","));
                                    }
                                    if !rep.errors.is_empty() {
                                        eprintln!("reload partial: {}", rep.errors.join("; "));
                                    }
                                }
                                Err(ds) => eprintln!("reload rejected: {}", ds.iter().map(|d| format!("{}: {}", d.field, d.message)).collect::<Vec<_>>().join("; ")),
                            }
                        },Err(e)=>eprintln!("reload rejected: {e}"),
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            let shut = rt.shutdown();
            eprintln!("shutdown: sessions_before={} sessions_after={} leases_after={} pending_after={} routes_active={}", shut.sessions_before, shut.sessions_after, shut.leases_after, shut.pending_calls_after, shut.routes_active);
            Ok(())
        },
        Some("snapshot")=>{
            // Public backup path (M8 P09): read-only copy of a STOPPED
            // service's store. The origin is preserved bit-for-bit (no
            // epoch advance, no state flips — the old R5 mutation is gone
            // from this path). Refuses live stores, corrupt images and
            // existing destinations.
            let home=PathBuf::from(args.get(2).ok_or("missing home")?);let target=PathBuf::from(args.get(3).ok_or("missing destination")?);
            let src=matrix_runtime::store::Store::open_read_only(&home.join("state/runtime.sqlite"))?;
            src.copy_to(&target)?;
            println!("snapshot written epoch={} states={:?}", src.epoch(), src.operation_states());Ok(())
        },
        Some("restore")=>{
            // Restore a validated backup into a fresh (or explicitly
            // emptied) home. Never overwrites live state: the destination
            // store must be absent and no runtime may own the home.
            // Booting afterwards is a recovery open (epoch advances,
            // admitted→unknown); old leases/sessions never revive.
            let backup=PathBuf::from(args.get(2).ok_or("missing backup")?);let home=PathBuf::from(args.get(3).ok_or("missing home")?);
            matrix_runtime::api::restore_backup(&backup,&home).map_err(|e| e.to_string())?;println!("restore staged; boot reconciles before publishing");Ok(())
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
        _=>Err("usage: matrix-managed serve <config.json> | snapshot <home> <new.db> | restore <backup.db> <home> | fingerprint <cert.der> | request <ca.der> <cert.der> <key.der> <address> <server-name> <json>".into()),
    }
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
