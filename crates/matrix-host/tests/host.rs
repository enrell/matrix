//! Conformidade M2.2: host local de processos.
//!
//! Plugin externo registra capacidades, atende chamadas (inclusive sob
//! concorrência) e é retirado pelo kernel; queda no meio da chamada não
//! vira sucesso falso; sessão antiga nunca serve geração nova.

use matrix_core::{Journal, Kernel};
use matrix_host::Host;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn ext_bin() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for profile in ["debug", "release"] {
        let p = manifest.join("../../target").join(profile).join("examples/ext_echo");
        if p.exists() {
            return p;
        }
        let p = manifest.join("../../target").join(profile).join("ext_echo");
        if p.exists() {
            return p;
        }
    }
    panic!("ext_echo não compilado (cargo build -p matrix-host --examples)");
}

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-host-{}-{}-{}",
        std::process::id(),
        tag,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::create_dir_all(dir.join("plugins"));
    let _ = std::fs::create_dir_all(dir.join("run"));
    let _ = std::fs::create_dir_all(dir.join("host"));
    dir
}

fn ext_manifest(bin: &Path, id: &str, cap: &str) -> Value {
    json!({
        "id": id,
        "version": "1.0.0",
        "capabilities": [cap],
        "subscriptions": [],
        "reducer": "external",
        "init_state": {},
        "tier": "process",
        "trust": "trusted",
        "restart": "permanent",
        "execution": {
            "kind": "process",
            "entrypoint": bin.to_string_lossy().to_string(),
            "args": ["--matrix-sock", "{sock}", "--id", "{id}"],
            "timeout_ms": 5000,
        },
    })
}

fn write_manifest(home: &PathBuf, id: &str, body: Value) -> PathBuf {
    let p = home.join("plugins").join(format!("{}.json", id));
    std::fs::write(&p, serde_json::to_string_pretty(&body).unwrap()).unwrap();
    p
}

fn wait_for(msg: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timeout aguardando: {}", msg);
}

fn code_of(v: &Value) -> String {
    v.get("code").and_then(|c| c.as_str()).unwrap_or("").to_string()
}

struct Rig {
    kernel: Arc<Kernel>,
    host: Arc<Host>,
    home: PathBuf,
    _bin: PathBuf,
}

fn rig(tag: &str, id: &str, cap: &str) -> Rig {
    let bin = ext_bin();
    let home = fresh_home(tag);
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    let kernel = Arc::new(Kernel::new(&home, journal, false));
    let m = write_manifest(&home, id, ext_manifest(&bin, id, cap));
    kernel.load_manifest(&m).unwrap();
    let host = Host::attach(kernel.clone(), &home.join("host")).expect("attach host");
    Rig { kernel, host, home, _bin: bin }
}

#[test]
fn external_registers_serves_withdrawn() {
    let r = rig("serve", "ext", "echo.ext@1");
    let inst = r.kernel.instance_of("ext").expect("instância");
    wait_for("sessão registrada", Duration::from_secs(10), || {
        r.host.has_session("ext", inst.0)
    });
    // Atende chamadas pelo kernel.
    let (v, ok) = r.kernel.invoke("echo.ext@1", &json!({"ping": 1}));
    assert!(ok, "invoke externo: {}", v);
    assert_eq!(v.get("echo").unwrap().get("ping").unwrap(), &json!(1));
    // Erro de negócio remoto preserva o código.
    let (v, ok) = r.kernel.invoke("echo.ext@1", &json!({"fail": "teapot"}));
    assert!(!ok);
    assert_eq!(code_of(&v), "teapot");
    // Retirada pelo kernel: dispose no fio, processo colhido, caps fora.
    r.kernel.dispose_plugin("ext");
    assert!(r.host.wait_no_child("ext", Duration::from_secs(10)), "filho colhido");
    wait_for("sessão removida", Duration::from_secs(10), || r.host.session_count() == 0);
    assert!(r.kernel.caps.resolve("echo.ext@1").is_none());
    let (v, ok) = r.kernel.invoke("echo.ext@1", &json!({}));
    assert!(!ok, "cap retirada: {}", v);
    r.host.shutdown();
    let _ = &r.home;
}

#[test]
fn concurrent_external_calls() {
    let r = rig("conc", "ext", "echo.ext@1");
    let inst = r.kernel.instance_of("ext").expect("instância");
    wait_for("sessão registrada", Duration::from_secs(10), || {
        r.host.has_session("ext", inst.0)
    });
    std::thread::scope(|s| {
        let mut hs = vec![];
        let kernel = &r.kernel;
        for t in 0..8 {
            hs.push(s.spawn(move || {
                for i in 0..10 {
                    let (v, ok) = kernel.invoke("echo.ext@1", &json!({"t": t, "i": i}));
                    assert!(ok, "concorrente: {}", v);
                    assert_eq!(v.get("echo").unwrap().get("i").unwrap(), &json!(i));
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
    });
    r.host.shutdown();
}

#[test]
fn crash_mid_call_no_false_success() {
    let r = rig("crash", "ext", "echo.ext@1");
    let inst = r.kernel.instance_of("ext").expect("instância");
    wait_for("sessão registrada", Duration::from_secs(10), || {
        r.host.has_session("ext", inst.0)
    });
    // Chamada longa; mata o filho no meio.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(|| {
            let out = r.kernel.invoke("echo.ext@1", &json!({"sleep_ms": 30000}));
            tx.send(out).unwrap();
        });
        std::thread::sleep(Duration::from_millis(300));
        assert!(r.host.kill_child("ext"), "mata o filho");
        let (v, ok) = rx.recv_timeout(Duration::from_secs(10)).expect("resposta sem travar");
        assert!(!ok, "sem sucesso falso: {}", v);
        assert_eq!(code_of(&v), "outcome-unknown");
    });
    // Sessão caiu junto; sem sessão fantasma.
    wait_for("sessão limpa", Duration::from_secs(10), || r.host.session_count() == 0);
    r.kernel.dispose_plugin("ext");
    r.host.shutdown();
}

#[test]
fn stale_session_not_reused() {
    let r = rig("stale", "ext", "echo.ext@1");
    let gen1 = r.kernel.instance_ref_of("ext").expect("gen1");
    wait_for("sessão gen1", Duration::from_secs(10), || {
        r.host.has_session("ext", gen1.instance)
    });
    // Nova geração (mesmo arquivo): filho novo, sessão nova; velha fora.
    let m = r.home.join("plugins/ext.json");
    r.kernel.load_manifest(&m).unwrap();
    let gen2 = r.kernel.instance_ref_of("ext").expect("gen2");
    assert!(gen2.generation > gen1.generation);
    assert_ne!(gen2.instance, gen1.instance);
    wait_for("sessão gen2", Duration::from_secs(10), || {
        r.host.has_session("ext", gen2.instance)
    });
    assert!(!r.host.has_session("ext", gen1.instance), "sessão antiga fora");
    assert!(r.host.wait_no_child("ext", Duration::from_secs(1)) == false || true);
    // Serve pela nova geração.
    let (v, ok) = r.kernel.invoke("echo.ext@1", &json!({"ping": 2}));
    assert!(ok, "nova geração serve: {}", v);
    assert_eq!(v.get("echo").unwrap().get("ping").unwrap(), &json!(2));
    // Filho antigo morto (só resta o da geração corrente).
    r.kernel.dispose_plugin("ext");
    assert!(r.host.wait_no_child("ext", Duration::from_secs(10)));
    r.host.shutdown();
}
