use matrix_core::{Journal, Kernel};
use serde_json::json;
use std::path::PathBuf;

fn test_kernel() -> (Kernel, PathBuf) {
    let dir: PathBuf = std::env::temp_dir().join(format!("matrix-test-{}", std::process::id()));
    let _ = std::fs::create_dir_all(dir.join("plugins"));
    for f in ["echo", "counter", "clock", "crasher", "ancient", "model"] {
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../plugins")
            .join(format!("{}.json", f));
        let dst = dir.join("plugins").join(format!("{}.json", f));
        if src.exists() {
            let _ = std::fs::copy(&src, &dst);
        }
    }
    let journal = Journal::open(&dir.join("run/journal.jsonl"), false, false).unwrap();
    let k = Kernel::new(&dir, journal, false);
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join("plugins"))
        .unwrap()
        .flatten()
        .map(|f| f.path())
        .collect();
    files.sort();
    for p in files {
        let _ = k.load_manifest(&p);
    }
    (k, dir)
}

#[test]
fn echo_roundtrip() {
    let (k, _d) = test_kernel();
    let (v, ok) = k.invoke("echo.msg@1", &json!({"ping": true}));
    assert!(ok);
    assert_eq!(v.get("echo").unwrap().get("ping").unwrap(), &json!(true));
}

#[test]
fn ancient_frozen_contract() {
    let (k, _d) = test_kernel();
    let (v, ok) = k.invoke("ancient.api@1", &json!({"in": 41}));
    assert!(ok);
    assert_eq!(v.get("out").unwrap(), &json!(42));
}

#[test]
fn counter_ticks_via_emit() {
    let (k, _d) = test_kernel();
    k.emit("sys.tick", &json!({}));
    k.emit("sys.tick", &json!({}));
    k.emit("sys.tick", &json!({}));
    let (v, ok) = k.invoke("count.state@1", &json!({}));
    assert!(ok);
    assert_eq!(v.get("state").unwrap(), &json!(3));
}

#[test]
fn reload_preserves_counter_and_bumps_generation() {
    let (k, _d) = test_kernel();
    k.emit("sys.tick", &json!({}));
    let gen_before = k.plugins.lock().get("counter").unwrap().generation;
    k.reload().unwrap();
    let ps = k.plugins.lock();
    let p = ps.get("counter").unwrap();
    assert_eq!(p.json_state.get("state").unwrap(), &json!(1));
    assert!(p.generation > gen_before);
}

#[test]
fn crasher_panic_contained_echo_survives() {
    let (k, _d) = test_kernel();
    let (_v, ok) = k.invoke("crash.inject@1", &json!({"mode": "panic"}));
    assert!(!ok);
    let (v, ok) = k.invoke("echo.msg@1", &json!({"ping": true}));
    assert!(ok);
    assert_eq!(v.get("echo").unwrap().get("ping").unwrap(), &json!(true));
}

#[test]
fn unknown_cap_has_code() {
    let (k, _d) = test_kernel();
    let (v, ok) = k.invoke("nope.missing@1", &json!({}));
    assert!(!ok);
    assert_eq!(v.get("code").unwrap(), &json!("no-such-capability"));
}
