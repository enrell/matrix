//! Eco de referência sobre o SDK Rust (M2.4).
//!
//! Uso: `ext_echo --matrix-sock <sock> --id <logical>`
//! Mesmo comportamento do exemplo manual que substitui:
//! - `input.sleep_ms` dorme de forma abortável (5ms);
//! - `input.fail = "CODE"` responde erro de negócio remoto;
//! - `input.count_file = path`: anexa o ticket por chamada recebida;
//! - `input.mark_cancel = path`: anexa o ticket ao observar `call.cancel`;
//! - `input.flood_stream = {id, chunk, count}`: envia N `stream.data`;
//! - senão responde `output = {"echo": input}`;
//! - `lifecycle.dispose` responde e sai com 0 (via `serve`).

use matrix_component::{CallCtx, CallOutcome, Component, Handler};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
use std::time::Duration;

struct Echo {
    /// Tickets com marca de cancel pendente (consome em `on_cancel`).
    marks: Mutex<HashMap<String, String>>,
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

impl Handler for Echo {
    fn on_call(
        &self,
        ctx: &CallCtx,
        ticket: &str,
        _cap: &str,
        input: &Value,
        cancel: &AtomicBool,
    ) -> CallOutcome {
        if let Some(cf) = input.get("count_file").and_then(|v| v.as_str()) {
            append_line(cf, ticket);
        }
        if let Some(mc) = input.get("mark_cancel").and_then(|v| v.as_str()) {
            self.marks.lock().unwrap().insert(ticket.to_string(), mc.to_string());
        }
        let sleep_ms = input.get("sleep_ms").and_then(|v| v.as_u64()).unwrap_or(0);
        if abortable_sleep(sleep_ms, cancel) {
            // `on_cancel` marca; aqui só aborta (o SDK cala o tardio).
            return CallOutcome::Err {
                code: "cancelled".to_string(),
                message: "aborted".to_string(),
            };
        }
        if let Some(code) = input.get("fail").and_then(|v| v.as_str()) {
            self.marks.lock().unwrap().remove(ticket);
            return CallOutcome::Err {
                code: code.to_string(),
                message: format!("remote {}", code),
            };
        }
        if let Some(fl) = input.get("flood_stream") {
            let fid = fl.get("id").and_then(|v| v.as_str()).unwrap_or("s1");
            let chunk = fl.get("chunk").and_then(|v| v.as_u64()).unwrap_or(65536).min(1 << 20) as usize;
            let count = fl.get("count").and_then(|v| v.as_u64()).unwrap_or(4).min(64);
            let payload = "x".repeat(chunk);
            for seq in 0..count {
                if cancel.load(Ordering::SeqCst) {
                    return CallOutcome::Err {
                        code: "cancelled".to_string(),
                        message: "aborted".to_string(),
                    };
                }
                if ctx.send_stream(fid, seq, &payload).is_err() {
                    break;
                }
            }
        }
        self.marks.lock().unwrap().remove(ticket);
        CallOutcome::Ok(json_echo(input))
    }

    fn on_cancel(&self, ticket: &str) {
        if let Some(path) = self.marks.lock().unwrap().remove(ticket) {
            append_line(&path, ticket);
        }
    }
}

fn json_echo(input: &Value) -> Value {
    serde_json::json!({"echo": input})
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut sock = String::new();
    let mut id = "ext-echo".to_string();
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
            _ => {}
        }
        i += 1;
    }
    if sock.is_empty() {
        eprintln!("usage: ext_echo --matrix-sock <sock> [--id <logical>]");
        std::process::exit(2);
    }
    let comp = Component::connect(std::path::Path::new(&sock), &id).expect("connect");
    match comp.serve(Echo { marks: Mutex::new(HashMap::new()) }) {
        Ok(()) => std::process::exit(0),
        Err(e) if e.contains("eof") => std::process::exit(0),
        Err(e) => {
            eprintln!("serve: {}", e);
            std::process::exit(1);
        }
    }
}
