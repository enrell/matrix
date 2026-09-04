//! matrix-sdk — client lib + traits p/ construir agents.
//! Mesmo protocolo do daemon (UDS + envelope `v`); desktop/TUI/CLI usam este client.

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

/// Provedor de modelo (MockModel no bench; LLM real entra aqui).
pub trait Model {
    fn complete(&self, prompt: &str) -> String;
}

/// Ferramenta chamável pelo agent (echo/tool no demo1).
pub trait Tool {
    fn name(&self) -> &str;
    fn call(&self, input: &Value) -> Value;
}

pub struct EchoTool {
    pub client: MatrixClient,
}

impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn call(&self, input: &Value) -> Value {
        self.client.invoke("echo.msg@1", input.clone()).unwrap_or(json!({"error": "tool failed"}))
    }
}

pub struct CannedModel;

impl Model for CannedModel {
    fn complete(&self, prompt: &str) -> String {
        format!("canned-response for: {}", prompt)
    }
}

/// Loop ReAct-ish de 5 passos (cf. agentlab `demos/demo1-agent-loop.sh`):
/// model → tool → model → tool → model(final).
pub struct Agent<M: Model, T: Tool> {
    pub model: M,
    pub tool: T,
}

impl<M: Model, T: Tool> Agent<M, T> {
    pub fn new(model: M, tool: T) -> Self {
        Self { model, tool }
    }

    pub fn run(&self, goal: &str) -> Value {
        let s1 = self.model.complete(goal);
        let t1 = self.tool.call(&json!({"step": 1, "text": s1}));
        let s2 = self.model.complete(&t1.to_string());
        let t2 = self.tool.call(&json!({"step": 2, "text": s2}));
        let done = self.model.complete(&t2.to_string());
        json!({"goal": goal, "final": done, "trace": [s1, t1, s2, t2]})
    }
}
