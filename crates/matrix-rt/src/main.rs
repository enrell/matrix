//! matrix-rt — daemon + CLI (UDS, blocking accept).
//! SPEC §1 verbs: run/status/invoke/emit/reload/journal/reset/quit.
//! Inspired by `master3/src/main.rs` (blocking accept bans poll-sleep,
//! thread-per-connection, `{"v":1,...}` envelopes), rewritten from scratch.

use matrix_core::{ContextId, Journal, Kernel, PROTO_V};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn parse_args() -> (String, Vec<String>) {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut verb = String::new();
    let mut args = vec![];
    for a in raw {
        if verb.is_empty() && !a.starts_with('-') {
            verb = a;
        } else if a != "--json" && a != "--dry-run" && a != "--fsync" {
            args.push(a);
        }
    }
    (verb, args)
}

pub fn home() -> PathBuf {
    if let Ok(h) = std::env::var("MATRIX_RT_HOME") {
        return PathBuf::from(h);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(gp) = exe.parent().and_then(|p| p.parent()).and_then(|p| p.parent()) {
            if gp.join("plugins").is_dir() || gp.join("Cargo.toml").exists() {
                return gp.to_path_buf();
            }
        }
    }
    PathBuf::from(".")
}

fn sock_path() -> PathBuf {
    home().join("run/matrix-rt.sock")
}

fn rpc_sock(req: &Value) -> Result<Value, String> {
    let sp = sock_path();
    let mut stream =
        UnixStream::connect(&sp).map_err(|e| format!("no daemon at {}: {}", sp.display(), e))?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(65))).ok();
    let mut env = req.clone();
    env["v"] = json!(PROTO_V);
    let line = serde_json::to_string(&env).map_err(|e| e.to_string())? + "\n";
    stream.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(stream);
    let mut resp = String::new();
    reader.read_line(&mut resp).map_err(|e| e.to_string())?;
    serde_json::from_str(&resp).map_err(|e| format!("bad daemon reply: {}", e))
}

fn cli_exit_rpc(verb: &str, extra: Value) -> ! {
    cli_exit_rpc_full(verb, extra, false)
}

fn cli_exit_rpc_full(verb: &str, extra: Value, always_zero: bool) -> ! {
    let resp = match rpc_sock(&json!({"verb": verb, "extra": extra})) {
        Ok(r) => r,
        Err(e) => {
            println!("{}", json!({"ok": false, "error": e}));
            std::process::exit(1);
        }
    };
    println!("{}", resp);
    if always_zero || resp.get("ok").is_none() {
        std::process::exit(0);
    }
    let ok = resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
    std::process::exit(if ok { 0 } else { 1 });
}

fn main() {
    let (verb, args) = parse_args();
    let dry_run = std::env::args().any(|a| a == "--dry-run");
    let batch_fsync = std::env::args().any(|a| a == "--fsync" || a == "--batch-fsync");
    let v2 = std::env::var("MATRIX_CORE_V2").map(|x| x == "1").unwrap_or(false);
    match verb.as_str() {
        "run" => run_daemon(dry_run, batch_fsync, v2),
        "invoke" => {
            let cap = args.first().cloned().unwrap_or_default();
            let input: Value =
                serde_json::from_str(args.get(1).map(|s| s.as_str()).unwrap_or("{}"))
                    .unwrap_or(json!({}));
            cli_exit_rpc("invoke", json!({"cap": cap, "input": input}));
        }
        "emit" => {
            let topic = args.first().cloned().unwrap_or_default();
            let payload: Value =
                serde_json::from_str(args.get(1).map(|s| s.as_str()).unwrap_or("{}"))
                    .unwrap_or(json!({}));
            cli_exit_rpc("emit", json!({"topic": topic, "payload": payload}));
        }
        "status" => cli_exit_rpc_full("status", json!({}), true),
        "reload" => cli_exit_rpc("reload", json!({})),
        "remove" => {
            let id = args.first().cloned().unwrap_or_default();
            cli_exit_rpc("remove", json!({"id": id}))
        }
        "reset" => cli_exit_rpc("reset", json!({})),
        "quit" => cli_exit_rpc("quit", json!({})),
        "journal" => {
            let tail: usize = args
                .iter()
                .position(|a| a == "--tail")
                .and_then(|i| args.get(i + 1))
                .and_then(|v| v.parse().ok())
                .unwrap_or(usize::MAX);
            let entries = Journal::read_all(&home().join("run/journal.jsonl"));
            let start = entries.len().saturating_sub(tail);
            for e in &entries[start..] {
                println!(
                    "{}",
                    json!({"seq": e.seq, "fiber": e.fiber, "kind": e.kind,
                           "args": e.args, "ts": e.ts, "undo": e.undo, "v": e.v})
                );
            }
            std::process::exit(0);
        }
        _ => {
            eprintln!("usage: matrix-rt <run|status|invoke|emit|reload|remove|journal|reset|quit> [args] [--json]");
            std::process::exit(2);
        }
    }
}

fn run_daemon(dry_run: bool, batch_fsync: bool, v2: bool) {
    let h = home();
    let run_dir = h.join("run");
    let journal_path = run_dir.join("journal.jsonl");
    let _ = std::fs::create_dir_all(&run_dir);
    let sp = run_dir.join("matrix-rt.sock");
    let _ = std::fs::remove_file(&sp);

    let journal = Journal::open(&journal_path, v2, batch_fsync).expect("journal open");
    let k = Arc::new(Kernel::new(&h, journal, dry_run));

    // Local host (M2.2+): runs `execution.process` plugins and delivers
    // calls. Attached before loading to receive the Activated events.
    let host = matrix_host::Host::attach(k.clone(), &run_dir.join("host"))
        .expect("host attach");

    let mut loaded = 0usize;
    if k.plugins_dir.is_dir() {
        if let Ok(rd) = std::fs::read_dir(&k.plugins_dir) {
            let mut files: Vec<PathBuf> = rd
                .flatten()
                .map(|f| f.path())
                .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
                .collect();
            files.sort();
            for p in files {
                // ancient.sha256 is not a manifest
                if p.file_name().and_then(|n| n.to_str()) == Some("ancient.sha256") {
                    continue;
                }
                if k.load_manifest(&p).is_ok() {
                    loaded += 1;
                }
            }
        }
    }
    let (applied, total) = k.replay();

    println!(
        "{}",
        json!({"ready": true, "harness": "matrix", "plugins": loaded,
               "replayed": applied, "journal_entries": total, "v": PROTO_V})
    );

    let listener = match UnixListener::bind(&sp) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("uds bind failed: {}", e);
            std::process::exit(1);
        }
    };
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                if k.quit.load(Ordering::SeqCst) {
                    break;
                }
                let k = k.clone();
                std::thread::Builder::new()
                    .stack_size(256 * 1024)
                    .spawn(move || handle_conn(stream, &k))
                    .ok();
            }
            Err(_) => {
                if k.quit.load(Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
    host.shutdown();
    k.journal.flush_sync();
    let _ = std::fs::remove_file(&sp);
}

fn wake_acceptor() {
    let _ = UnixStream::connect(sock_path());
}

fn handle_conn(stream: UnixStream, k: &Kernel) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() || line.trim().is_empty() {
        return;
    }
    let Ok(req) = serde_json::from_str::<Value>(&line) else { return };
    let verb = req.get("verb").and_then(|v| v.as_str()).unwrap_or("");
    let extra = req.get("extra").cloned().unwrap_or(json!({}));
    let resp = match verb {
        "invoke" => {
            let cap = extra.get("cap").and_then(|v| v.as_str()).unwrap_or("");
            let input = extra.get("input").cloned().unwrap_or(json!({}));
            let (value, ok) = k.invoke(cap, &input);
            json!({"v": PROTO_V, "ok": ok, "value": value})
        }
        "emit" => {
            let topic = extra.get("topic").and_then(|v| v.as_str()).unwrap_or("");
            let payload = extra.get("payload").cloned().unwrap_or(json!({}));
            let seq = k.emit(topic, &payload);
            json!({"v": PROTO_V, "ok": true, "emitted": topic, "seq": seq})
        }
        "reload" => match k.reload() {
            Ok(n) => json!({"v": PROTO_V, "ok": true, "reloaded": n}),
            Err(e) => json!({"v": PROTO_V, "ok": false, "error": e}),
        },
        "remove" => {
            let id = extra.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let out = k.dispose_plugin(id);
            json!({"v": PROTO_V, "ok": true, "removed": id, "outcome": out.as_str()})
        }
        "status" => {
            let ps = k.plugins.lock();
            let mut plugins: Vec<Value> = ps
                .values()
                .map(|p| {
                    json!({"id": p.id, "state": p.state.as_str(), "tier": p.tier,
                           "trust": p.trust, "generation": p.generation,
                           "restart": p.restart_policy_str(), "caps": p.caps,
                           "requires": p.requires.iter().map(|r| {
                               if let Some(pr) = &r.provider {
                                   json!({"interface": r.interface, "provider": pr})
                               } else {
                                   json!({"interface": r.interface})
                               }
                           }).collect::<Vec<_>>(),
                           "bindings": p.bindings.iter().map(|b| json!({
                               "interface": b.interface, "provider": b.provider_logical,
                               "instance": b.provider_instance, "generation": b.provider_generation,
                           })).collect::<Vec<_>>(),
                           "waiting_reason": p.wait_cause,
                           "instance": p.instance_id, "context": p.context_id,
                           "epoch": p.epoch,
                           "active_resources": k.resources.active_for(ContextId(p.context_id))})
                })
                .collect();
            drop(ps);
            plugins.sort_by(|a, b| {
                a.get("id").and_then(|v| v.as_str()).unwrap_or("").cmp(
                    b.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                )
            });
            let n_caps = k.caps.len();
            let inv = k.inventory();
            json!({"v": PROTO_V, "harness": "matrix", "plugins": plugins, "capabilities": n_caps,
                   "epoch": k.epoch(), "inventory": inv})
        }
        "reset" => {
            k.reset_bench();
            json!({"v": PROTO_V, "ok": true, "reset": true})
        }
        "quit" => {
            k.journal.flush_sync();
            k.quit.store(true, Ordering::SeqCst);
            let mut s = reader.into_inner();
            let _ = writeln!(s, "{}", json!({"v": PROTO_V, "ok": true, "bye": true}));
            wake_acceptor();
            return;
        }
        _ => json!({"v": PROTO_V, "ok": false, "error": format!("unknown verb {}", verb)}),
    };
    let mut s = reader.into_inner();
    let _ = writeln!(s, "{}", resp);
}
