//! Generic composition node (M6.1 step 3+): echo + chained calls.
//!
//! Usage: `dep_node --matrix-sock <sock> --id <logical>`
//! - `input.chain = true`: invokes the first binding's dependency with
//!   `input.input` (`input.timeout_ms` timeout, default 5000) and answers
//!   `output = {"chained": <output>, "via": <id>}`; child errors become
//!   business errors with the same code (origin preserved on the wire).
//! - `input.sleep_ms` abortable sleep (5ms);
//! - `input.fail = "CODE"` answers a remote business error;
//! - otherwise answers `output = {"echo": input, "via": <id>}`.
//! - `input.amplify = N` answers `{"blob": "x"*N}` (bounded test output);
//! - `input.acquire = {kind, label, interval_ms?}` acquires an
//!   activation and answers `{"acquired": {"handle": N}}`;
//! - `input.release = N` releases the handle;
//! - events on manifest-subscribed topics go to `--event-log`
//!   (one per line: `topic<TAB>payload`), via `on_event`.
//! - `input.stream_send = {stream_id, chunks, chunk_bytes, sleep_ms?}`
//!   sends N stream chunks (bounded) and answers `{"stream_sent": N}`;
//! - `input.chain_with_streams = {stream_id, chunks, chunk_bytes,
//!   interval_ms?, prime_ms?, input, timeout_ms?}` chains while streaming
//!   concurrently on the same session (M7 bidi legs); ids under `remote/`
//!   associate to the in-flight leg, others stay local;
//! - stream chunks addressed to this activation go to `--stream-log`
//!   (one per line: `stream_id<TAB>seq<TAB>payload`), via `on_stream`;
//!   `--stream-slow-ms` sleeps per chunk (slow-consumer tests).
//! Same semantics as `sdk-python/dep_node.py` (C24 parity).

use matrix_component::{CallCtx, CallOutcome, Component, DepError, Handler};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

struct Node {
    id: String,
    event_log: Option<String>,
    stream_log: Option<String>,
    stream_slow_ms: u64,
}

fn append_line(path: &str, line: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{}", line);
    }
}

fn abortable_sleep(ms: u64, cancel: &AtomicBool) -> bool {
    let mut slept = 0u64;
    while slept < ms {
        if cancel.load(Ordering::SeqCst) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
        slept += 5;
    }
    false
}

impl Handler for Node {
    fn on_call(
        &self,
        ctx: &CallCtx,
        _ticket: &str,
        _cap: &str,
        input: &Value,
        cancel: &AtomicBool,
    ) -> CallOutcome {
        let sleep_ms = input.get("sleep_ms").and_then(|v| v.as_u64()).unwrap_or(0);
        if abortable_sleep(sleep_ms, cancel) {
            return CallOutcome::Err {
                code: "cancelled".to_string(),
                message: "aborted".to_string(),
            };
        }
        if let Some(code) = input.get("fail").and_then(|v| v.as_str()) {
            return CallOutcome::Err {
                code: code.to_string(),
                message: format!("remote {}", code),
            };
        }
        // Test-only amplification: small input, big bounded output.
        if let Some(n) = input.get("amplify").and_then(|v| v.as_u64()) {
            let n = n.min(1 << 20) as usize;
            return CallOutcome::Ok(serde_json::json!({"blob": "x".repeat(n), "via": self.id}));
        }
        // Concurrent chain + streams (M7 bidi): streams while the child
        // leg is in flight on this same session, so the host associates
        // the chunks with that leg. `spec = {stream_id, chunks,
        // chunk_bytes, interval_ms?, prime_ms?, input, timeout_ms?}`.
        // `prime_ms` delays the first chunk so admission+dispatch can map
        // the leg first (default 50).
        if let Some(spec) = input.get("chain_with_streams") {
            let stream_id = spec.get("stream_id").and_then(|v| v.as_str()).unwrap_or("s-bidi");
            let chunks = spec.get("chunks").and_then(|v| v.as_u64()).unwrap_or(0).min(32);
            let bytes = spec.get("chunk_bytes").and_then(|v| v.as_u64()).unwrap_or(0).min(1024) as usize;
            let interval = spec.get("interval_ms").and_then(|v| v.as_u64()).unwrap_or(20).min(50);
            let prime = spec.get("prime_ms").and_then(|v| v.as_u64()).unwrap_or(50).min(1000);
            let payload = "x".repeat(bytes);
            let sctx = ctx.clone();
            let sid = stream_id.to_string();
            let streamer = std::thread::spawn(move || {
                if prime > 0 {
                    std::thread::sleep(Duration::from_millis(prime));
                }
                let mut sent = 0u64;
                for seq in 0..chunks {
                    if sctx.send_stream(&sid, seq, &payload).is_ok() {
                        sent += 1;
                    }
                    if interval > 0 {
                        std::thread::sleep(Duration::from_millis(interval));
                    }
                }
                sent
            });
            let Some(binding) = ctx.dependencies().first().map(|b| b.id.clone()) else {
                let sent = streamer.join().unwrap_or(0);
                let _ = sent;
                return CallOutcome::Err {
                    code: "dependency-unavailable".into(),
                    message: "no binding".into(),
                };
            };
            let inner = spec.get("input").cloned().unwrap_or(serde_json::json!({}));
            let timeout = Duration::from_millis(spec.get("timeout_ms").and_then(|v| v.as_u64()).unwrap_or(8000).max(1));
            let out = match ctx.invoke_dependency(&binding, inner, timeout) {
                Ok(out) => out,
                Err(DepError { code, message }) => {
                    let _ = streamer.join();
                    return CallOutcome::Err { code, message };
                }
            };
            let sent = streamer.join().unwrap_or(0);
            return CallOutcome::Ok(serde_json::json!({"chained": out, "via": self.id, "stream_sent": sent}));
        }
        if input.get("chain").and_then(|v| v.as_bool()).unwrap_or(false) {            let Some(binding) = ctx.dependencies().first().map(|b| b.id.clone()) else {
                return CallOutcome::Err {
                    code: "dependency-unavailable".into(),
                    message: "no binding".into(),
                };
            };
            let inner = input.get("input").cloned().unwrap_or(serde_json::json!({}));
            let timeout = Duration::from_millis(input.get("timeout_ms").and_then(|v| v.as_u64()).unwrap_or(5000).max(1));
            match ctx.invoke_dependency(&binding, inner, timeout) {
                Ok(out) => {
                    return CallOutcome::Ok(serde_json::json!({"chained": out, "via": self.id}))
                }
                Err(DepError { code, message }) => {
                    return CallOutcome::Err { code, message };
                }
            }
        }
        if let Some(acq) = input.get("acquire") {
            let kind = acq.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            let label = acq.get("label").and_then(|v| v.as_str()).unwrap_or("");
            let ms = acq.get("interval_ms").and_then(|v| v.as_u64());
            match ctx.acquire_resource(kind, label, ms) {
                Ok(h) => return CallOutcome::Ok(serde_json::json!({"acquired": {"handle": h.to_string()}, "via": self.id})),
                Err(e) => {
                    return CallOutcome::Err { code: e.code, message: e.message };
                }
            }
        }
        if let Some(h) = input.get("release").and_then(|v| v.as_u64()) {
            match ctx.release_resource(h) {
                Ok(()) => return CallOutcome::Ok(serde_json::json!({"released": h.to_string(), "via": self.id})),
                Err(e) => {
                    return CallOutcome::Err { code: e.code, message: e.message };
                }
            }
        }
        // Bounded stream emission (M7 streams): N chunks of `x`.
        if let Some(spec) = input.get("stream_send") {
            let stream_id = spec.get("stream_id").and_then(|v| v.as_str()).unwrap_or("s-test");
            let chunks = spec.get("chunks").and_then(|v| v.as_u64()).unwrap_or(0).min(256);
            let bytes = spec.get("chunk_bytes").and_then(|v| v.as_u64()).unwrap_or(0).min(4096) as usize;
            let sleep = spec.get("sleep_ms").and_then(|v| v.as_u64()).unwrap_or(0);
            let payload = "x".repeat(bytes);
            let mut sent = 0u64;
            for seq in 0..chunks {
                if cancel.load(Ordering::SeqCst) {
                    return CallOutcome::Err { code: "cancelled".into(), message: "aborted".into() };
                }
                match ctx.send_stream(stream_id, seq, &payload) {
                    Ok(()) => sent += 1,
                    Err(e) => {
                        return CallOutcome::Err { code: "stream-refused".into(), message: e };
                    }
                }
                if sleep > 0 {
                    abortable_sleep(sleep.min(50), cancel);
                }
            }
            return CallOutcome::Ok(serde_json::json!({"stream_sent": sent, "via": self.id}));
        }
        CallOutcome::Ok(serde_json::json!({"echo": input, "via": self.id}))
    }

    fn on_event(&self, topic: &str, payload: &Value) {
        if let Some(path) = &self.event_log {
            append_line(path, &format!("{}\t{}", topic, payload));
        }
    }

    fn on_stream(&self, stream_id: &str, seq: u64, payload: &str) {
        if self.stream_slow_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.stream_slow_ms));
        }
        if let Some(path) = &self.stream_log {
            append_line(path, &format!("{}\t{}\t{}", stream_id, seq, payload.len()));
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut sock = String::new();
    let mut id = "dep-node".to_string();
    let mut event_log: Option<String> = None;
    let mut stream_log: Option<String> = None;
    let mut stream_slow_ms: u64 = 0;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--matrix-sock" => {
                i += 1;
                sock = args.get(i).cloned().unwrap_or_default();
            }
            "--id" => {
                i += 1;
                id = args.get(i).cloned().unwrap_or(id);
            }
            "--event-log" => {
                i += 1;
                event_log = args.get(i).cloned().or(event_log);
            }
            "--stream-log" => {
                i += 1;
                stream_log = args.get(i).cloned().or(stream_log);
            }
            "--stream-slow-ms" => {
                i += 1;
                stream_slow_ms = args.get(i).and_then(|v| v.parse().ok()).unwrap_or(stream_slow_ms);
            }
            _ => {}
        }
        i += 1;
    }
    if sock.is_empty() {
        eprintln!("usage: dep_node --matrix-sock <sock> [--id <logical>] [--event-log <path>] [--stream-log <path>] [--stream-slow-ms <n>]");
        std::process::exit(2);
    }
    let comp = Component::connect(std::path::Path::new(&sock), &id).expect("connect");
    match comp.serve(Node { id, event_log, stream_log, stream_slow_ms }) {
        Ok(()) => std::process::exit(0),
        Err(e) if e.contains("eof") => std::process::exit(0),
        Err(e) => {
            eprintln!("serve: {}", e);
            std::process::exit(1);
        }
    }
}
