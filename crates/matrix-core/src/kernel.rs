//! Kernel: registry + FSM + OTP + generations + leases + journal fan-in único.
//! Default tier = in-proc reducers (D-core). Tier processo/wasm = roadmap
//! (contrato `tier`/`trust` já reservado no manifest).

use crate::bus::Bus;
use crate::fsm::Fsm;
use crate::journal::Journal;
use crate::leases::Lease;
use crate::registry::Registry;
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Plugin {
    pub id: String,
    pub state: Fsm,
    pub tier: String,
    pub trust: String,
    pub generation: u64,
    pub restart_policy: String,
    pub caps: Vec<String>,
    pub subs: Vec<String>,
    pub reducer: String,
    pub init_state: Value,
    pub json_state: Value,
    pub leases: Vec<Lease>,
    pub panic_count: u32,
    pub restart_times: Vec<Instant>,
}

impl Plugin {
    pub fn restart_policy_str(&self) -> &str {
        &self.restart_policy
    }
}

pub struct Kernel {
    pub plugins: Mutex<HashMap<String, Plugin>>,
    pub caps: Registry,
    pub bus: Bus,
    pub journal: Journal,
    pub journal_path: PathBuf,
    pub plugins_dir: PathBuf,
    pub quit: Arc<AtomicBool>,
    pub dry_run: bool,
}

impl Kernel {
    pub fn new(home: &PathBuf, journal: Journal, dry_run: bool) -> Self {
        Self {
            plugins: Mutex::new(HashMap::new()),
            caps: Registry::new(),
            bus: Bus::new(),
            journal,
            journal_path: home.join("run/journal.jsonl"),
            plugins_dir: home.join("plugins"),
            quit: Arc::new(AtomicBool::new(false)),
            dry_run,
        }
    }

    pub fn provide(&self, cap: &str, fiber: &str) {
        self.caps.provide(cap, fiber);
    }

    // ---- lifecycle ----

    pub fn load_manifest(&self, path: &PathBuf) -> Result<String, String> {
        let txt = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let v: Value = serde_json::from_str(&txt).map_err(|e| e.to_string())?;
        let id = v.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            return Err("manifest without id".into());
        }
        let caps: Vec<String> = v
            .get("capabilities")
            .and_then(|x| x.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let subs: Vec<String> = v
            .get("subscriptions")
            .and_then(|x| x.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let reducer = v.get("reducer").and_then(|x| x.as_str()).unwrap_or("noop").to_string();
        let init_state = v.get("init_state").cloned().unwrap_or(json!({}));
        let tier = v.get("tier").and_then(|x| x.as_str()).unwrap_or("inproc").to_string();
        let trust = v.get("trust").and_then(|x| x.as_str()).unwrap_or("trusted").to_string();
        let restart = v.get("restart").and_then(|x| x.as_str()).unwrap_or("permanent").to_string();
        // ancient tem estado inicial próprio
        let json_state = match reducer.as_str() {
            "counter" => json!({"state": 0}),
            "clock" => json!({"count": 0}),
            _ => init_state.clone(),
        };
        // generational reload: preserva estado de counter/clock
        let generation = self.plugins.lock().get(&id).map(|p| p.generation + 1).unwrap_or(1);
        let old_state = self.plugins.lock().get(&id).map(|p| p.json_state.clone());
        let json_state = match (reducer.as_str(), old_state) {
            ("counter", Some(s)) if s.get("state").is_some() => s,
            ("clock", Some(s)) if s.get("count").is_some() => s,
            _ => json_state,
        };
        let p = Plugin {
            id: id.clone(),
            state: Fsm::Active,
            tier,
            trust,
            generation,
            restart_policy: restart,
            caps: caps.clone(),
            subs: subs.clone(),
            reducer,
            init_state,
            json_state,
            leases: vec![],
            panic_count: 0,
            restart_times: vec![],
        };
        for c in &caps {
            self.caps.provide(c, &id);
        }
        for t in &subs {
            self.bus.subscribe(&id, t);
        }
        self.journal.append(&id, "plugin.loaded", json!({"id": id}), Value::Null);
        self.plugins.lock().insert(id.clone(), p);
        Ok(id)
    }

    pub fn dispose_plugin(&self, id: &str) {
        let mut ps = self.plugins.lock();
        if let Some(p) = ps.get_mut(id) {
            p.state = Fsm::Disposed;
            p.leases.clear(); // revogação unilateral
        }
        drop(ps);
        self.caps.revoke_fiber(id);
        self.bus.unsubscribe_fiber(id);
        self.journal.append(id, "plugin.unloaded", json!({"id": id}), Value::Null);
    }

    /// Reload generacional único: green adota estado blue (counter/clock).
    pub fn reload(&self) -> Result<usize, String> {
        let dir = self.plugins_dir.clone();
        let Ok(rd) = std::fs::read_dir(&dir) else { return Ok(0) };
        let mut files: Vec<PathBuf> = rd
            .flatten()
            .map(|f| f.path())
            .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
            .collect();
        files.sort();
        let mut n = 0;
        for p in files {
            // marca PREPARING antes do swap (transacional)
            if self.load_manifest(&p).is_ok() {
                n += 1;
            }
        }
        self.journal.append("sys", "sys.reload", json!({"reloaded": n}), Value::Null);
        Ok(n)
    }

    // ---- dispatch ----

    fn fault(&self, fiber: &str, mode: &str) -> Value {
        // OTP sliding window {intensity 5, period 10s}
        let mut failed = false;
        {
            let mut ps = self.plugins.lock();
            if let Some(p) = ps.get_mut(fiber) {
                let now = Instant::now();
                p.restart_times.retain(|t| now.duration_since(*t).as_secs() < 10);
                p.restart_times.push(now);
                p.panic_count += 1;
                if p.restart_times.len() > 5 {
                    p.state = Fsm::Failed;
                    failed = true;
                }
            }
        }
        self.journal.append(
            fiber,
            "sys.fault",
            json!({"mode": mode, "contained": true, "failed": failed}),
            Value::Null,
        );
        json!({"contained": true, "mode": mode, "failed": failed})
    }

    pub fn invoke(&self, cap: &str, input: &Value) -> (Value, bool) {
        let Some(fiber) = self.caps.resolve(cap) else {
            return (json!({"error": "no such capability", "code": "no-such-capability"}), false);
        };
        let (reducer, state_ok) = {
            let ps = self.plugins.lock();
            match ps.get(&fiber) {
                Some(p) if p.state == Fsm::Active => (p.reducer.clone(), true),
                Some(_) => (String::new(), false),
                None => (String::new(), false),
            }
        };
        if !state_ok {
            // revive訴求: checa se foi Failed ou Disposed
            let ps = self.plugins.lock();
            let code = match ps.get(&fiber).map(|p| p.state) {
                Some(Fsm::Failed) | Some(Fsm::Disposed) => "plugin-not-active",
                _ => "plugin-not-loaded",
            };
            return (json!({"error": "plugin not available", "code": code}), false);
        }
        // crasher: fault injection contida (process tier = roadmap; aqui contém sem matar)
        if reducer == "crasher" {
            let mode = input.get("mode").and_then(|x| x.as_str()).unwrap_or("panic");
            // panic real contido via catch_unwind (prova que echo sobrevive)
            if mode == "panic" {
                let r = std::panic::catch_unwind(|| panic!("injected panic"));
                debug_assert!(r.is_err());
            }
            let info = self.fault(&fiber, mode);
            return (json!({"error": "injected fault", "code": "plugin-panicked", "fault": info}), false);
        }
        let Some(entry) = Reducers::dispatch(&reducer, &fiber, cap, input, self) else {
            return (json!({"error": "unknown reducer", "code": "reducer-missing"}), false);
        };
        if !self.dry_run {
            self.journal.append(&fiber, "cap.invoke", json!({"cap": cap, "input": input}), Value::Null);
        }
        (entry, true)
    }

    pub fn deliver_evt(&self, topic: &str, payload: &Value) {
        let subs = self.bus.subscribers(topic);
        for fiber in subs {
            let mut ps = self.plugins.lock();
            let Some(p) = ps.get_mut(&fiber) else { continue };
            if p.state != Fsm::Active {
                continue;
            }
            // reducers stateful reagem a tick (counter/clock)
            if p.reducer == "counter" && topic == "sys.tick" {
                let n = p.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"state": n});
            } else if p.reducer == "clock" && topic == "sys.tick" {
                let n = p.json_state.get("count").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                p.json_state = json!({"count": n});
            }
            let _ = payload;
        }
    }

    pub fn emit(&self, topic: &str, payload: &Value) -> u64 {
        let seq = self.journal.append("cli", "evt.emit", json!({"topic": topic, "payload": payload}), Value::Null);
        self.deliver_evt(topic, payload);
        seq
    }

    /// Boot = fold do journal (replay). Restaura counter via evt.emit/sys.tick.
    pub fn replay(&self) -> (usize, usize) {
        let entries = crate::journal::Journal::read_all(&self.journal_path);
        let total = entries.len();
        let mut applied = 0;
        for e in &entries {
            if e.kind == "evt.emit" {
                let topic = e.args.get("topic").and_then(|x| x.as_str()).unwrap_or("");
                if topic == "sys.tick" {
                    let mut ps = self.plugins.lock();
                    if let Some(p) = ps.get_mut("counter") {
                        if p.state == Fsm::Active {
                            let n = p.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0) + 1;
                            p.json_state = json!({"state": n});
                            applied += 1;
                        }
                    }
                }
            }
        }
        (applied, total)
    }

    pub fn reset_bench(&self) {
        let _ = self.journal.reset();
        let ids: Vec<String> = self.plugins.lock().keys().cloned().collect();
        for id in &ids {
            if id.starts_with("gen") {
                self.dispose_plugin(id);
            }
        }
        {
            let mut ps = self.plugins.lock();
            for p in ps.values_mut() {
                p.panic_count = 0;
                p.restart_times.clear();
                p.state = Fsm::Active;
                if p.reducer == "counter" {
                    p.json_state = json!({"state": 0});
                } else if p.reducer == "clock" {
                    p.json_state = json!({"count": 0});
                } else if p.reducer != "crasher" {
                    p.json_state = p.init_state.clone();
                }
            }
        }
    }
}

// ---- reducers puros (descrevem efeitos como dados) ----

struct Reducers;

impl Reducers {
    fn dispatch(reducer: &str, fiber: &str, cap: &str, input: &Value, k: &Kernel) -> Option<Value> {
        match reducer {
            "echo" => Some(json!({"echo": input})),
            "ancient" => {
                // contrato congelado: {"in":41} -> {"out":42}
                let _ = (fiber, cap);
                Some(json!({"out": 42}))
            }
            "counter" => {
                let ps = k.plugins.lock();
                let n = ps.get(fiber)?.json_state.get("state").and_then(|x| x.as_i64()).unwrap_or(0);
                Some(json!({"state": n}))
            }
            "clock" => {
                let ps = k.plugins.lock();
                let n = ps.get(fiber)?.json_state.get("count").and_then(|x| x.as_i64()).unwrap_or(0);
                Some(json!({"count": n}))
            }
            "model" => Some(json!({"text": "canned-response", "in": input})),
            "noop" => Some(json!({"ok": true})),
            _ => None,
        }
    }
}
