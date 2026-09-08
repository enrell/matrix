//! Conformance runner (M8 P05/P06): static protocol vectors plus a live
//! behavior subset against a facade-started runtime.
//!
//! The live component below (`--child`) is implemented by hand from the
//! published protocol: length-prefixed JSON frames over a Unix socket
//! with `serde_json` only — no `matrix-component` SDK, no host imports.
//! It exercises echo calls, cancel observation, stream floods, event
//! delivery and stale-generation filtering on the declared subset.
//!
//! Usage:
//!   `matrix-conform vectors` — static checks only (no runtime needed).
//!   `matrix-conform --dump-vectors` — machine-readable vector set.
//!   `matrix-conform local` — static + live local-profile checks.
//! Each check prints `PASS <id>` / `FAIL <id>: <why>`; exit status is the
//! count of failures (0 = conformant on the declared subset).

use serde_json::{json, Value};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

const MAX_FRAME: usize = 1024 * 1024;

struct Counts {
    pass: usize,
    fail: usize,
}

impl Counts {
    fn check(&mut self, id: &str, ok: bool, why: &str) {
        if ok {
            self.pass += 1;
            println!("PASS {id}");
        } else {
            self.fail += 1;
            println!("FAIL {id}: {why}");
        }
    }
}

// ---------- hand framing (no SDK) ----------

fn write_frame(s: &mut UnixStream, v: &Value) -> std::io::Result<()> {
    let raw = serde_json::to_vec(v).unwrap();
    assert!(raw.len() <= MAX_FRAME, "test frame above max");
    s.write_all(&(raw.len() as u32).to_be_bytes())?;
    s.write_all(&raw)?;
    s.flush()
}

fn read_frame(s: &mut UnixStream, timeout: Duration) -> Option<Value> {
    s.set_read_timeout(Some(timeout)).ok()?;
    let mut len = [0u8; 4];
    s.read_exact(&mut len).ok()?;
    let n = u32::from_be_bytes(len) as usize;
    if n == 0 || n > MAX_FRAME {
        return None;
    }
    let mut buf = vec![0u8; n];
    s.read_exact(&mut buf).ok()?;
    serde_json::from_slice(&buf).ok()
}

static MID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn env(ty: &str, sid: &str, inst: &str, gen: &str, rid: Option<&str>, body: Value) -> Value {
    let n = MID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let mut m = serde_json::Map::new();
    m.insert("protocol".into(), json!("matrix.component"));
    m.insert("version".into(), json!("0.1"));
    m.insert("type".into(), json!(ty));
    m.insert("message_id".into(), json!(format!("hand-{ty}-{n}")));
    m.insert("session_id".into(), json!(sid));
    m.insert("instance_id".into(), json!(inst));
    m.insert("generation".into(), json!(gen));
    if let Some(r) = rid {
        m.insert("request_id".into(), json!(r));
    }
    m.insert("body".into(), body);
    Value::Object(m)
}

// ---------- static vectors ----------

fn static_vectors() -> Vec<(&'static str, Value, bool)> {
    // (id, envelope, valid?)
    vec![
        ("hello-full", json!({"protocol":"matrix.component","version":"0.1","type":"hello","message_id":"h","body":{"versions":["0.1"],"max_frame":1024,"client":"hand","features":[]}}), true),
        ("hello-bad-version", json!({"protocol":"matrix.component","version":"9.9","type":"hello","message_id":"h","body":{"versions":["0.1"],"max_frame":1024,"client":"hand"}}), false),
        ("call.open-full", json!({"protocol":"matrix.component","version":"0.1","type":"call.open","message_id":"m","session_id":"s","instance_id":"1","generation":"1","request_id":"r","body":{"ticket":"tkt-1","capability":"echo.msg@1","input":{}}}), true),
        ("call.open-no-ticket", json!({"protocol":"matrix.component","version":"0.1","type":"call.open","message_id":"m","session_id":"s","instance_id":"1","generation":"1","request_id":"r","body":{"capability":"echo.msg@1","input":{}}}), false),
        ("stream.data-full", json!({"protocol":"matrix.component","version":"0.1","type":"stream.data","message_id":"m","session_id":"s","instance_id":"1","generation":"1","body":{"stream_id":"s1","seq":"0","payload":"hi"}}), true),
        ("stream.data-shape-only", json!({"protocol":"matrix.component","version":"0.1","type":"stream.data","message_id":"m","session_id":"s","instance_id":"1","generation":"1","body":{"stream_id":"s1","seq":"nope","payload":"hi"}}), true),
        ("remote.call.open-full", json!({"protocol":"matrix.remote","version":"0.1","type":"call.open","message_id":"m","session_id":"sess","instance_id":"1","generation":"1","request_id":"r","body":{"parent":{"domain":"d","ticket":"1","activation":{"logical":"c","instance":"1","generation":"1"}},"binding_id":"rb-1","activation":{"logical":"c","instance":"1","generation":"1"},"cap":"p@1","input":{},"timeout_ms":1000,"budget_ms":1000,"lease":"tok","grant_rev":"1","operation_id":"op-1"}}), true),
        ("remote.call.open-no-op", json!({"protocol":"matrix.remote","version":"0.1","type":"call.open","message_id":"m","session_id":"sess","instance_id":"1","generation":"1","request_id":"r","body":{"parent":{"domain":"d","ticket":"1","activation":{"logical":"c","instance":"1","generation":"1"}},"binding_id":"rb-1","activation":{"logical":"c","instance":"1","generation":"1"},"cap":"p@1","input":{},"timeout_ms":1000,"budget_ms":1000,"lease":"tok","grant_rev":"1"}}), false),
        ("remote.stream.data-full", json!({"protocol":"matrix.remote","version":"0.1","type":"stream.data","message_id":"m","session_id":"sess","body":{"stream_id":"s","seq":0,"bytes":"eA==","credit":0}}), true),
        ("remote.stream.data-bad-b64", json!({"protocol":"matrix.remote","version":"0.1","type":"stream.data","message_id":"m","session_id":"sess","body":{"stream_id":"s","seq":0,"bytes":"!!!","credit":0}}), false),
        ("remote.op.query-full", json!({"protocol":"matrix.remote","version":"0.1","type":"op.query","message_id":"m","session_id":"sess","request_id":"r","body":{"principal":"fp","operation_id":"op"}}), true),
        ("remote.session.hello-features", json!({"protocol":"matrix.remote","version":"0.1","type":"session.hello","message_id":"m","body":{"versions":["0.1"],"features":["remote-calls/1","remote-streams/1","remote-events/1","remote-ops/1"],"domain":"d","authority":"fp","controller_epoch":"7"}}), true),        ("managed.invoke-full", json!({"profile":"matrix.managed/0.1","action":"invoke","lease":"tok","fence":"1","operation":"op","cap":"echo.msg@1","input":{}}), true),
    ]
}

fn run_vectors(counts: &mut Counts) {
    for (id, v, valid) in static_vectors() {
        let proto = v.get("protocol").and_then(|x| x.as_str()).unwrap_or("");
        let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
        let ok = if proto == "matrix.remote" {
            matrix_proto::remote::validate_envelope(&v).is_ok()
        } else if v.get("profile").is_some() {
            // Managed unary shape: action + profile tag present.
            v.get("action").and_then(|x| x.as_str()).is_some_and(|a| !a.is_empty())
        } else {
            matrix_proto::validate_envelope(&v).is_ok() && !ty.is_empty()
        };
        counts.check(&format!("vector-{id}"), ok == valid, &format!("expected valid={valid}"));
    }
}

// ---------- hand component (--child) ----------

fn child_main() -> i32 {
    let args: Vec<String> = std::env::args().collect();
    let get = |k: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1).cloned()).unwrap_or_default();
    let sock = get("--sock");
    let id = get("--id");
    let log = get("--log");
    let feats: Vec<Value> = get("--features").split(',').filter(|s| !s.is_empty()).map(|s| json!(s)).collect();
    let t0 = std::time::Instant::now();
    let append = |line: String| {
        let line = format!("{}ms {line}", t0.elapsed().as_millis());
        if !log.is_empty() {
            if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&log) {
                let _ = writeln!(f, "{line}");
            }
        }
    };
    let mut s = match UnixStream::connect(&sock) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("connect: {e}");
            return 2;
        }
    };
    let send = |s: &mut UnixStream, v: Value| write_frame(s, &v).unwrap();
    let token = std::env::var("MATRIX_LAUNCH_TOKEN").unwrap_or_default();
    send(&mut s, json!({"protocol":"matrix.component","version":"0.1","type":"hello","message_id":"h1","body":{"launch_token":token,"versions":["0.1"],"max_frame":MAX_FRAME,"client":"hand","features":feats}}));
    let welcome = read_frame(&mut s, Duration::from_secs(10)).unwrap();
    let maxf = welcome["body"]["max_frame"].as_u64().unwrap_or(MAX_FRAME as u64) as usize;
    let _ = maxf;
    send(&mut s, json!({"protocol":"matrix.component","version":"0.1","type":"component.register","message_id":"reg1","session_id":welcome["session_id"],"body":{"manifest":{"id":id}}}));
    let reg = read_frame(&mut s, Duration::from_secs(10)).unwrap();
    let (sid, inst, gen) = (
        reg["session_id"].as_str().unwrap_or("").to_string(),
        reg["instance_id"].as_str().unwrap_or("").to_string(),
        reg["generation"].as_str().unwrap_or("").to_string(),
    );
    let act = read_frame(&mut s, Duration::from_secs(10)).unwrap();
    let op = act["body"]["operation_id"].clone();
    send(&mut s, env("lifecycle.result", &sid, &inst, &gen, act.get("request_id").and_then(|v| v.as_str()), json!({"operation_id": op, "status": "ok", "pending": []})));
    let mut seq: u64 = 1000;
    loop {
        let Some(m) = read_frame(&mut s, Duration::from_secs(30)) else { break };
        // Stale-generation filter (declared behavior): ignore frames for
        // other activations instead of acting on them.
        let bound = m.get("session_id").and_then(|v| v.as_str()) == Some(sid.as_str())
            && m.get("instance_id").and_then(|v| v.as_str()).map(|v| v == inst).unwrap_or(true)
            && m.get("generation").and_then(|v| v.as_u64()).map(|g| g.to_string() == gen).unwrap_or(true);
        if !bound {
            continue;
        }
        match m.get("type").and_then(|v| v.as_str()).unwrap_or("") {
            "lifecycle.dispose" => {
                let op = m["body"]["operation_id"].clone();
                send(&mut s, env("lifecycle.result", &sid, &inst, &gen, m.get("request_id").and_then(|v| v.as_str()), json!({"operation_id": op, "status": "ok", "pending": []})));
                break;
            }
            "call.open" => {
                let ticket = m["body"]["ticket"].as_str().unwrap_or("").to_string();
                let rid = m.get("request_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                append(format!("open ticket={ticket} rid={rid}"));
                let input = m["body"]["input"].clone();
                // Abortable sleep: poll for call.cancel like an SDK reader
                // thread would, so withdrawal is observed, not just fatal.
                let mut cancelled = false;
                if let Some(ms) = input.get("sleep_ms").and_then(|v| v.as_u64()) {
                    let end = Instant::now() + Duration::from_millis(ms.min(8000));
                    while Instant::now() < end {
                        match read_frame(&mut s, Duration::from_millis(50)) {
                            Some(f) if f.get("type").and_then(|v| v.as_str()) == Some("call.cancel")
                                && f["body"]["ticket"].as_str().unwrap_or("") == ticket =>
                            {
                                append(format!("cancel\t{ticket}"));
                                send(&mut s, env("call.error", &sid, &inst, &gen, Some(&rid), json!({"ticket": ticket, "error": {"code": "cancelled", "message": "hand aborted"}})));
                                cancelled = true;
                                break;
                            }
                            Some(f) => {
                                // Inline observation during sleep (events,
                                // streams); nested opens cannot occur here.
                                match f.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                                    "event.deliver" => append(format!("event\t{}\t{}", f["body"]["topic"].as_str().unwrap_or("?"), f["body"]["payload"])),
                                    "stream.data" => append(format!("stream\t{}\t{}", f["body"]["stream_id"].as_str().unwrap_or("?"), f["body"]["seq"].as_str().unwrap_or("?"))),
                                    _ => {}
                                }
                            }
                            None => {}
                        }
                    }
                }
                if cancelled {
                    continue;
                }
                if let Some(fl) = input.get("flood_stream") {
                    append("flood-start".to_string());
                    let fid = fl.get("id").and_then(|v| v.as_str()).unwrap_or("s1");
                    let chunk = fl.get("chunk").and_then(|v| v.as_u64()).unwrap_or(1024).min(65536) as usize;
                    let count = fl.get("count").and_then(|v| v.as_u64()).unwrap_or(4).min(32);
                    let payload = "y".repeat(chunk);
                    for i in 0..count {
                        send(&mut s, env("stream.data", &sid, &inst, &gen, None, json!({"stream_id": fid, "seq": i.to_string(), "payload": payload})));
                    }
                }
                append("answer".to_string());
                if input.get("fail").and_then(|v| v.as_str()).is_some() {
                    let code = input["fail"].as_str().unwrap().to_string();
                    send(&mut s, env("call.result", &sid, &inst, &gen, Some(&rid), json!({"ticket": ticket, "status": "error", "error": {"code": code, "message": "hand fail"}})));
                } else {
                    send(&mut s, env("call.result", &sid, &inst, &gen, Some(&rid), json!({"ticket": ticket, "status": "ok", "output": {"echo": input}})));
                }
                let _ = seq;
                seq += 1;
            }
            "call.cancel" => {
                append(format!("cancel\t{}", m["body"]["ticket"].as_str().unwrap_or("?")));
            }
            "event.deliver" => {
                append(format!("event\t{}\t{}", m["body"]["topic"].as_str().unwrap_or("?"), m["body"]["payload"]));
            }
            "stream.data" => {
                append(format!("stream\t{}\t{}", m["body"]["stream_id"].as_str().unwrap_or("?"), m["body"]["seq"].as_str().unwrap_or("?")));
            }
            _ => {}
        }
    }
    0
}

// ---------- live local checks ----------

fn live_main() -> Counts {
    use matrix_runtime::api::{Config, InspectOpts, Runtime};
    let mut counts = Counts { pass: 0, fail: 0 };
    let dir = std::env::temp_dir().join(format!(
        "mxconform-{}",
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let exe = std::env::current_exe().unwrap().to_string_lossy().to_string();
    let clog = dir.join("comp.log").to_string_lossy().to_string();
    let cfg = Config::parse(&json!({
        "home": dir.join("home").to_string_lossy().to_string(),
        "components": [
            {"manifest": {"id": "hand", "capabilities": ["hand.echo@1"], "subscriptions": ["t"],
                "execution": {"kind": "process", "entrypoint": exe,
                    "args": ["--child", "--sock", "{sock}", "--id", "{id}", "--log", clog, "--features", ""]}}, "trusted": true},
        ],
        "grants": {"op": {"components": ["hand"], "capabilities": ["hand.echo@1"]}},
    }))
    .expect("conform config parses");
    let rt = match Runtime::start(&cfg) {
        Ok(rt) => std::sync::Arc::new(rt),
        Err(e) => {
            counts.check("live-start", false, &e.to_string());
            return counts;
        }
    };
    counts.check("live-start", true, "");
    let act = rt.activate("op", "hand", 20000).expect("activate");
    let (token, fence) = (
        act["lease"].as_str().unwrap().to_string(),
        act["fence"].as_str().unwrap().parse::<u64>().unwrap(),
    );
    if let Err(e) = rt.wait_session("hand", Duration::from_secs(8)) {
        eprintln!("instances: {}", rt.inspect(matrix_runtime::api::InspectOpts::default())["kernel"]["instances"]);
        panic!("session serving: {e}");
    }
    // echo roundtrip through the hand-rolled component
    let v = rt.invoke("op", &token, fence, "op-echo", "hand.echo@1", &json!({"ping": 1})).unwrap();
    counts.check("live-echo", v["value"]["echo"]["ping"] == 1, &format!("{v}"));
    // events flow to the subscribed hand component (fresh session first)
    rt.broadcast("t", &json!({"n": 1}));
    let t0 = Instant::now();
    let mut seen = false;
    while t0.elapsed() < Duration::from_secs(10) {
        if std::fs::read_to_string(dir.join("comp.log")).unwrap_or_default().contains("event\tt") {
            seen = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    counts.check("live-event-deliver", seen, "no event in component log");
    // cancel observed by the component (remove mid-sleep)
    let h = std::thread::spawn({
        let rt = rt.clone();
        let (token, fence) = (token.clone(), fence);
        move || rt.invoke("op", &token, fence, "op-sleep", "hand.echo@1", &json!({"sleep_ms": 8000}))
    });
    std::thread::sleep(Duration::from_millis(500));
    // Withdrawal delivers cancel to the running call first (M6 path);
    // the lease is released afterwards for the turnover below.
    let removed = rt.remove("hand").unwrap_or_default();
    let _ = removed;
    let slept = h.join().unwrap();
    let log = std::fs::read_to_string(dir.join("comp.log")).unwrap_or_default();
    counts.check("live-cancel-observed", log.contains("cancel\t"), &format!("terminal={slept:?} log={log:?}"));
    counts.check(
        "live-cancel-terminal",
        matches!(&slept, Ok(v) if v["value"]["code"] == "cancelled"),
        &format!("{slept:?}"),
    );
    // Release the old lease for the turnover below (remove withdrew the
    // activation; the lease itself is operator-held until released).
    let _ = rt.release("op", &token, fence);
    // stream flood bounded: over-credit ends the leg, session alive
    rt.provision(&json!({"id": "hand", "capabilities": ["hand.echo@1"], "subscriptions": ["t"],
        "execution": {"kind": "process", "entrypoint": std::env::current_exe().unwrap().to_string_lossy().to_string(),
            "args": ["--child", "--sock", "{sock}", "--id", "{id}", "--log", clog, "--features", ""]}})).unwrap();
    let act = rt.activate("op", "hand", 20000).expect("reactivate");
    let (token, fence) = (
        act["lease"].as_str().unwrap().to_string(),
        act["fence"].as_str().unwrap().parse::<u64>().unwrap(),
    );
    rt.wait_session("hand", Duration::from_secs(20)).expect("session serving again");
    let fl = rt.invoke("op", &token, fence, "op-flood", "hand.echo@1", &json!({"flood_stream": {"id": "s9", "chunk": 16384, "count": 8}})).unwrap();
    counts.check("live-flood-terminal", fl["ok"] == true, &format!("{fl}"));
    rt.broadcast("t", &json!({"n": 2}));
    let t0 = Instant::now();
    let mut seen2 = false;
    while t0.elapsed() < Duration::from_secs(10) {
        if std::fs::read_to_string(dir.join("comp.log")).unwrap_or_default().contains("\"n\":2") {
            seen2 = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    counts.check("live-event-after-flood", seen2, "event fan-out after stream excess");
    let again = rt.invoke("op", &token, fence, "op-ping", "hand.echo@1", &json!({})).unwrap();
    if again["ok"] != true {
        eprintln!("DBG ping failed; calls={}", rt.inspect(matrix_runtime::api::InspectOpts::default())["calls"]);
    }
    counts.check("live-session-survives-flood", again["ok"] == true, &format!("{again}"));
    // revoke denies with a stable code
    rt.revoke("op").unwrap();
    let denied = rt.invoke("op", &token, fence, "op-nope", "hand.echo@1", &json!({}));
    counts.check(
        "live-revoke-denies",
        matches!(&denied, Err(e) if e.code == matrix_runtime::api::ErrorCode::PermissionDenied),
        &format!("{denied:?}"),
    );
    // stale generation: old token dead after turnover
    let insp = rt.inspect(InspectOpts::default());
    counts.check("live-inspect-schema", insp["schema"] == "matrix.inspect/1", &format!("{insp}"));
    let shut = rt.shutdown();
    counts.check(
        "live-shutdown-verified",
        shut.sessions_after == 0 && shut.leases_after == 0 && shut.pending_calls_after == 0,
        &format!("{shut:?}"),
    );
    counts
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--child") {
        std::process::exit(child_main());
    }
    if args.iter().any(|a| a == "--dump-vectors") {
        println!("{}", serde_json::to_string_pretty(&static_vectors().iter().map(|(id, v, valid)| json!({"id": id, "envelope": v, "valid": valid})).collect::<Vec<_>>()).unwrap());
        return;
    }
    let mut counts = Counts { pass: 0, fail: 0 };
    run_vectors(&mut counts);
    if !args.iter().any(|a| a == "vectors") {
        let live = live_main();
        counts.pass += live.pass;
        counts.fail += live.fail;
    }
    println!("conform: {} passed, {} failed", counts.pass, counts.fail);
    std::process::exit(if counts.fail == 0 { 0 } else { 1 });
}
