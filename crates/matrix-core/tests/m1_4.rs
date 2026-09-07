//! M1.4 conformance: coordinated substitution (C07 + milestone exit).
//!
//! - Candidates validated before publishing; invalid ones keep the live generation.
//! - Disk-removed manifests reconciled (definition cascades out).
//! - Reload under load: no orphans, no generation mixing, no commit
//!   tardio autorizado.
//! - C07: reload remove capability; candidato que falha deixa estado
//!   with an explicit, recoverable failure.

use matrix_core::{CallPolicy, Journal, Kernel};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn fresh_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "matrix-m14-{}-{}-{}",
        std::process::id(),
        tag,
        nanos()
    ));
    let _ = std::fs::create_dir_all(dir.join("plugins"));
    let _ = std::fs::create_dir_all(dir.join("run"));
    dir
}

fn fresh_kernel(tag: &str) -> Kernel {
    let home = fresh_home(tag);
    let journal = Journal::open(&home.join("run/journal.jsonl"), false, false).unwrap();
    Kernel::new(&home, journal, false)
}

fn home_of(k: &Kernel) -> PathBuf {
    k.plugins_dir.parent().unwrap().to_path_buf()
}

fn write_manifest(home: &PathBuf, id: &str, body: Value) -> PathBuf {
    let p = home.join("plugins").join(format!("{}.json", id));
    std::fs::write(&p, serde_json::to_string_pretty(&body).unwrap()).unwrap();
    p
}

fn basic_manifest(id: &str, caps: &[&str], reducer: &str) -> Value {
    json!({
        "id": id,
        "version": "1.0.0",
        "capabilities": caps,
        "subscriptions": [],
        "reducer": reducer,
        "init_state": {},
        "tier": "inproc",
        "trust": "trusted",
        "restart": "permanent",
    })
}

fn with_requires(mut v: Value, requires: Value) -> Value {
    v["requires"] = requires;
    v
}

fn state_of(k: &Kernel, logical: &str) -> String {
    k.context_state_of(logical).unwrap_or_else(|| "<none>".to_string())
}

fn code_of(v: &Value) -> String {
    v.get("code").and_then(|c| c.as_str()).unwrap_or("").to_string()
}

#[derive(Clone)]
struct Gate {
    inner: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl Gate {
    fn new() -> Self {
        Self { inner: Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new())) }
    }
    fn release(&self) {
        *self.inner.0.lock().unwrap() = true;
        self.inner.1.notify_all();
    }
    fn is_released(&self) -> bool {
        *self.inner.0.lock().unwrap()
    }
}

/// Registry without stale owners: every published cap belongs to the
/// corrente e ativa do dono.
fn assert_registry_current(k: &Kernel) {
    for (cap, owner) in k.caps.snapshot() {
        let r = k.caps.resolve_ref(&cap).expect("cap publicada resolve");
        let cur = k.contexts.current(&owner).expect("dono tem corrente");
        assert_eq!(r.instance, cur.instance.0, "cap {} with no generation mixing", cap);
        assert_eq!(cur.state.as_str(), "Active", "dono de {} ativo", cap);
    }
}

/// No Active resources from generations that already left Active.
fn assert_no_active_leftovers(k: &Kernel, logical: &str) {
    let cur_gen = k.contexts.current(logical).map(|c| c.generation).unwrap_or(0);
    let bad = k.resources.inventory().into_iter().filter(|r| {
        r.owner_logical == logical
            && r.state == matrix_core::ResourceState::Active
            && r.generation != cur_gen
    }).count();
    assert_eq!(bad, 0, "no live orphans from {} (current #{})", logical, cur_gen);
}

// ---- manifests removidos ----

#[test]
fn reload_removes_deleted_manifest() {
    let k = fresh_kernel("rm-manifest");
    let home = home_of(&k);
    write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    write_manifest(
        &home,
        "search",
        with_requires(
            basic_manifest("search", &["search.query@1"], "noop"),
            json!([{"interface": "workspace.fs@1", "provider": "workspace"}]),
        ),
    );
    write_manifest(&home, "echo", basic_manifest("echo", &["echo.msg@1"], "echo"));
    assert_eq!(k.reload().unwrap(), 3);
    assert!(k.invoke("search.query@1", &json!({})).1);

    // Delete the search file: definition leaves, caps vanish, rest continues.
    std::fs::remove_file(home.join("plugins/search.json")).unwrap();
    let n = k.reload().unwrap();
    assert_eq!(n, 2, "only the present ones reload");
    assert!(k.definitions.lock().get("search").is_none(), "definition reconciled");
    assert!(k.caps.resolve("search.query@1").is_none());
    assert_eq!(code_of(&k.invoke("search.query@1", &json!({})).0), "no-such-capability");
    assert!(k.invoke("workspace.fs@1", &json!({})).1);
    assert!(k.invoke("echo.msg@1", &json!({})).1);
    assert_registry_current(&k);
}

#[test]
fn reload_removed_provider_cascades_to_waiting() {
    let k = fresh_kernel("rm-provider");
    let home = home_of(&k);
    write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    write_manifest(
        &home,
        "search",
        with_requires(
            basic_manifest("search", &["search.query@1"], "noop"),
            json!([{"interface": "workspace.fs@1", "provider": "workspace"}]),
        ),
    );
    k.reload().unwrap();
    assert_eq!(state_of(&k, "search"), "Active");

    // Provider vanishes from disk: search Waiting (definition persists),
    // workspace without definition.
    std::fs::remove_file(home.join("plugins/workspace.json")).unwrap();
    k.reload().unwrap();
    assert!(k.definitions.lock().get("workspace").is_none());
    assert!(k.definitions.lock().get("search").is_some(), "consumidor persiste");
    assert_eq!(state_of(&k, "search"), "Waiting");
    assert!(k.caps.resolve("workspace.fs@1").is_none());

    // File returns: everything reactivates unassisted.
    write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    k.reload().unwrap();
    assert_eq!(state_of(&k, "search"), "Active");
    assert!(k.invoke("search.query@1", &json!({})).1);
}

// ---- C07 ----

#[test]
fn c07_reload_removes_capability_and_failed_candidate_recovers() {
    let k = fresh_kernel("c07");
    let home = home_of(&k);
    let p = write_manifest(&home, "dual", basic_manifest("dual", &["a.cap@1", "b.cap@1"], "noop"));
    k.reload().unwrap();
    assert!(k.invoke("a.cap@1", &json!({})).1);
    assert!(k.invoke("b.cap@1", &json!({})).1);
    let gen1 = k.instance_ref_of("dual").unwrap();

    // Valid v2 removes b.cap: record vanishes, no old-generation residue.
    write_manifest(&home, "dual", basic_manifest("dual", &["a.cap@1"], "noop"));
    k.reload().unwrap();
    let gen2 = k.instance_ref_of("dual").unwrap();
    assert!(gen2.generation > gen1.generation);
    assert!(k.invoke("a.cap@1", &json!({})).1);
    assert_eq!(code_of(&k.invoke("b.cap@1", &json!({})).0), "no-such-capability");
    assert_no_active_leftovers(&k, "dual");
    assert_registry_current(&k);
    let _ = p;

    // Invalid candidate (broken JSON): live generation intact and serving.
    std::fs::write(home.join("plugins/dual.json"), "{invalid json").unwrap();
    let n = k.reload().unwrap();
    assert_eq!(n, 0, "nothing publishable");
    assert_eq!(k.instance_ref_of("dual").unwrap().generation, gen2.generation);
    assert!(k.invoke("a.cap@1", &json!({})).1);
    // Recupera ao consertar o arquivo.
    write_manifest(&home, "dual", basic_manifest("dual", &["a.cap@1"], "noop"));
    k.reload().unwrap();
    assert!(k.instance_ref_of("dual").unwrap().generation > gen2.generation);
    assert!(k.invoke("a.cap@1", &json!({})).1);
}

// ---- reload sob carga ----

#[test]
fn reload_under_load_no_orphans_no_mixed_generations() {
    let k = fresh_kernel("reload-load");
    let home = home_of(&k);
    let m_ws = write_manifest(&home, "workspace", basic_manifest("workspace", &["workspace.fs@1"], "noop"));
    write_manifest(
        &home,
        "search",
        with_requires(
            basic_manifest("search", &["search.query@1"], "noop"),
            json!([{"interface": "workspace.fs@1", "provider": "workspace"}]),
        ),
    );
    write_manifest(&home, "echo", basic_manifest("echo", &["echo.msg@1"], "echo"));
    k.reload().unwrap();
    let search_gen1 = k.instance_ref_of("search").unwrap();
    let ws_gen1 = k.instance_ref_of("workspace").unwrap();

    // Call blocked on generation 1 (Cancel: revoked at reload).
    let (arrived_tx, arrived_rx) = mpsc::channel();
    let gate = Gate::new();
    let (done_tx, done_rx) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    std::thread::scope(|s| {
        s.spawn(|| {
            let open = k.call_open("search.query@1", &json!({}), Some(CallPolicy::cancel()), &[]).unwrap();
            arrived_tx.send(open.ticket).unwrap();
            let park0 = Instant::now();
            while !gate.is_released() && park0.elapsed() < Duration::from_secs(15) {
                std::thread::sleep(Duration::from_millis(1));
            }
            let c = k.commit_effect(open.ticket, "w", &json!({}));
            let closed = k.call_close(open.ticket);
            done_tx.send((c.map_err(|v| code_of(&v)), closed)).unwrap();
        });
        // Concurrent invokes during reload (light fire, no deadlock).
        s.spawn(|| {
            let stop2 = &stop;
            let mut ok = 0u32;
            while !stop2.load(Ordering::Relaxed) {
                if k.invoke("echo.msg@1", &json!({})).1 {
                    ok += 1;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            ok
        });
        arrived_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Rewrites the provider (same content, new generation) and reloads under load.
        std::fs::write(&m_ws, serde_json::to_string_pretty(&basic_manifest("workspace", &["workspace.fs@1"], "noop")).unwrap()).unwrap();
        k.reload().unwrap();
        stop.store(true, Ordering::Relaxed);

        // Under load the old generation withdrew; the new one serves.
        let ws_now = k.instance_ref_of("workspace").unwrap();
        assert!(ws_now.generation > ws_gen1.generation, "provider on a new generation");
        assert_registry_current(&k);

        gate.release();
        let (commit, closed) = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(commit.is_err(), "old-generation commit rejected: {:?}", commit);
        assert!(closed);
        // Settled: no orphans, no mixing.
        assert_no_active_leftovers(&k, "search");
        assert_no_active_leftovers(&k, "workspace");
        assert_registry_current(&k);
        let bs = k.bindings_of("search");
        assert_eq!(bs.len(), 1);
        assert_eq!(bs[0].provider_instance, ws_now.instance, "binding on the new generation");
        assert!(k.invoke("search.query@1", &json!({})).1);
        assert!(k.invoke("echo.msg@1", &json!({})).1);
        let _ = search_gen1;
    });
}

// ---- id renomeado limpa o antigo ----

#[test]
fn reload_renamed_id_cleans_old() {
    let k = fresh_kernel("rename");
    let home = home_of(&k);
    write_manifest(&home, "svc", basic_manifest("svc", &["svc.cap@1"], "noop"));
    k.reload().unwrap();
    assert!(k.invoke("svc.cap@1", &json!({})).1);
    // The same file now declares another id: old leaves, new enters.
    write_manifest(&home, "svc", basic_manifest("svc2", &["svc2.cap@1"], "noop"));
    k.reload().unwrap();
    assert!(k.caps.resolve("svc.cap@1").is_none(), "registro antigo limpo");
    assert!(k.invoke("svc2.cap@1", &json!({})).1);
    assert_registry_current(&k);
}

// ---- sys.reload audita removidos e falhas ----

#[test]
fn reload_journal_audits_removed_and_failed() {
    let k = fresh_kernel("reload-audit");
    let home = home_of(&k);
    write_manifest(&home, "a", basic_manifest("a", &["a.cap@1"], "noop"));
    k.reload().unwrap();
    std::fs::write(home.join("plugins/broken.json"), "not json").unwrap();
    std::fs::remove_file(home.join("plugins/a.json")).unwrap();
    write_manifest(&home, "b", basic_manifest("b", &["b.cap@1"], "noop"));
    k.reload().unwrap();
    let entries = matrix_core::Journal::read_all(&k.journal_path);
    let last = entries.iter().rev().find(|e| e.kind == "sys.reload").expect("sys.reload");
    assert_eq!(last.args.get("reloaded").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(last.args.get("removed").and_then(|v| v.as_u64()), Some(1));
    assert!(last.args.get("failed").and_then(|v| v.as_array()).map(|a| !a.is_empty()).unwrap_or(false));
    assert!(k.invoke("b.cap@1", &json!({})).1);
    assert!(k.caps.resolve("a.cap@1").is_none());
}
