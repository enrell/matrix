//! matrix-sdk — generic daemon client (UDS + envelope `v`).
//! Domain-independent interface: `rpc`, `invoke`, `emit`, `status`.
//! Applications and business components live in separate repositories.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

fn sock_path(home: &PathBuf) -> PathBuf {
    if let Ok(h) = std::env::var("MATRIX_RT_HOME") {
        return PathBuf::from(h).join("run/matrix-rt.sock");
    }
    home.join("run/matrix-rt.sock")
}

#[derive(Debug, Clone)]
pub struct MatrixClient {
    pub home: PathBuf,
}

impl MatrixClient {
    pub fn new(home: PathBuf) -> Self {
        Self { home }
    }

    pub fn rpc(&self, verb: &str, extra: Value) -> Result<Value, String> {
        let sp = sock_path(&self.home);
        let mut s =
            UnixStream::connect(&sp).map_err(|e| format!("no daemon at {}: {}", sp.display(), e))?;
        s.set_read_timeout(Some(std::time::Duration::from_secs(65))).ok();
        let req = json!({"v": 1, "verb": verb, "extra": extra});
        let line = serde_json::to_string(&req).map_err(|e| e.to_string())? + "\n";
        s.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
        let mut r = BufReader::new(s);
        let mut resp = String::new();
        r.read_line(&mut resp).map_err(|e| e.to_string())?;
        serde_json::from_str(&resp).map_err(|e| format!("bad reply: {}", e))
    }

    pub fn invoke(&self, cap: &str, input: Value) -> Result<Value, String> {
        let r = self.rpc("invoke", json!({"cap": cap, "input": input}))?;
        if r.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
            Ok(r.get("value").cloned().unwrap_or(Value::Null))
        } else {
            Err(r.to_string())
        }
    }

    pub fn emit(&self, topic: &str, payload: Value) -> Result<Value, String> {
        self.rpc("emit", json!({"topic": topic, "payload": payload}))
    }

    pub fn status(&self) -> Result<Value, String> {
        self.rpc("status", json!({}))
    }
}
