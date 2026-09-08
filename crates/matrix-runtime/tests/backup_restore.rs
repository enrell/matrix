//! M8 public backup/restore (P09): origin-preserving offline copies,
//! validated restore, dead authority after restore, corrupt rejection.
//! Operator surface under test: `api::{backup_offline, restore_backup}`
//! (also reachable as CLI `snapshot`/`restore`) plus the store backup
//! primitives they wrap. No kernel/host/session imports.

use matrix_runtime::api::{backup_offline, restore_backup, Config, InspectOpts, Runtime};
use matrix_runtime::store::Store;
use serde_json::json;
use std::time::{Duration, Instant};

fn home(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "mxbak-{}-{}",
        tag,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn config(home: &std::path::Path) -> serde_json::Value {
    json!({
        "home": home.to_string_lossy().to_string(),
        "components": [
            {"manifest": {"id": "echo", "capabilities": ["echo.msg@1"], "reducer": "echo"}, "trusted": true},
        ],
        "grants": {"alice": {"components": ["echo"], "capabilities": ["echo.msg@1"]}},
    })
}

fn sha256(path: &std::path::Path) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(std::fs::read(path).unwrap_or_default());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn wait_for(msg: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while t0.elapsed() < timeout {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timeout: {msg}");
}

#[test]
fn backup_guard_blocks_exclusive_open_for_its_lifetime() {
    // Exclusivity race (M8): the backup handle itself holds the shared
    // lock, so no runtime can take ownership mid-copy. While the handle
    // lives, booting the same home fails closed; once dropped, it boots.
    let dir = home("lockguard");
    let cfg = Config::parse(&config(&dir)).expect("valid");
    let rt = Runtime::start(&cfg).expect("start");
    rt.shutdown();
    drop(rt);
    let bak = Store::open_read_only(&dir.join("state/runtime.sqlite")).expect("backup opens");
    let err = match Runtime::start(&cfg) {
        Ok(_) => panic!("second owner booted while a backup handle lives"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("already owned"), "fail-closed reason: {err}");
    drop(bak);
    let rt2 = Runtime::start(&cfg).expect("boots after guard drop");
    rt2.shutdown();
}

#[test]
fn backup_preserves_origin_restore_keeps_authority_dead() {
    let dir = home("p09");
    let cfg = Config::parse(&config(&dir)).expect("valid");
    let rt = Runtime::start(&cfg).expect("start");
    let act = rt.activate("alice", "echo", 20000).expect("activate");
    let token = act["lease"].as_str().unwrap().to_string();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    let done = rt
        .invoke("alice", &token, fence, "op-bak-1", "echo.msg@1", &json!({"v": 1}))
        .expect("invoke");
    assert_eq!(done["ok"], true);
    let epoch_before = rt.inspect(InspectOpts::default())["epoch"].clone();
    // Live online snapshot through the running handle (no epoch advance).
    let live = dir.join("live.sqlite");
    rt.snapshot_backup(&live).expect("online snapshot");
    assert_eq!(Store::open_read_only(&live).unwrap().epoch().to_string(), epoch_before.as_str().unwrap());
    // Offline backup of a LIVE store refuses (exclusivity, not a copy race).
    assert!(backup_offline(&dir, &dir.join("live-refused.sqlite")).is_err());
    assert!(!dir.join("live-refused.sqlite").exists(), "refused backup creates nothing");
    rt.shutdown();
    drop(rt); // stopped daemon releases the store lock with its last handle
    // Offline backup of the stopped service: origin hash identical after.
    // (Calling it live must refuse — exclusivity policy, not a copy race.)
    let db = dir.join("state/runtime.sqlite");
    let hash_before = sha256(&db);
    let bak = dir.join("backup.sqlite");
    backup_offline(&dir, &bak).expect("offline backup");
    assert_eq!(sha256(&db), hash_before, "R5 gone: backup must not mutate the origin");
    let src = Store::open_read_only(&bak).unwrap();
    assert_eq!(src.epoch().to_string(), epoch_before.as_str().unwrap(), "epoch preserved");
    assert_eq!(src.operation_states().get("completed").copied().unwrap_or(0), 1, "durable result kept");
    // Corrupt copies rejected before they can enter the restore path:
    // truncation and non-database bytes both fail validation.
    let full = std::fs::read(&bak).unwrap();
    let trunc_path = dir.join("trunc.sqlite");
    std::fs::write(&trunc_path, &full[..full.len() / 2]).unwrap();
    assert!(Store::open_read_only(&trunc_path).is_err(), "truncated image refused");
    let garbage_path = dir.join("garbage.sqlite");
    std::fs::write(&garbage_path, b"definitely not a database file").unwrap();
    assert!(Store::open_read_only(&garbage_path).is_err(), "garbage refused");
    assert!(restore_backup(&trunc_path, &dir.join("restored-bad")).is_err());
    assert!(restore_backup(&garbage_path, &dir.join("restored-bad2")).is_err());
    // Restore into a fresh home: durable results replay, old authority dead.
    let home2 = home("p09-restored");
    restore_backup(&bak, &home2).expect("restore stages");
    // Refuses to overwrite existing state (explicit removal first).
    assert!(restore_backup(&bak, &home2).is_err(), "never overwrites live state");
    let cfg2 = Config::parse(&config(&home2)).expect("valid");
    let rt2 = Runtime::start(&cfg2).expect("boot after restore");
    // Completed operation replays from the restored ledger (no re-execution)
    // once called under a fresh post-restore lease.
    let act2 = rt2.activate("alice", "echo", 20000).expect("fresh lease after restore");
    let token2 = act2["lease"].as_str().unwrap().to_string();
    let fence2: u64 = act2["fence"].as_str().unwrap().parse().unwrap();
    let replay = rt2
        .invoke("alice", &token2, fence2, "op-bak-1", "echo.msg@1", &json!({"v": 1}))
        .expect("replay after restore");
    assert_eq!(replay, done, "durable result survives restore");
    // Old lease never revives (memory-only); boot is a recovery open.
    let stale = rt2.invoke("alice", &token, fence, "op-bak-2", "echo.msg@1", &json!({}));
    assert!(stale.is_err(), "restored boot honors no old lease: {stale:?}");
    rt2.shutdown();
    // Restored boot does not resurrect revoked principals either.
    let home3 = home("p09-revoked");
    {
        let cfg = Config::parse(&config(&dir)).expect("valid");
        let rt = Runtime::start(&cfg).expect("reboot origin");
        rt.revoke("alice").expect("revoke");
        rt.shutdown();
    }
    let bak2 = dir.join("backup2.sqlite");
    backup_offline(&dir, &bak2).expect("backup with revocation");
    restore_backup(&bak2, &home3).expect("restore");
    let cfg3 = Config::parse(&config(&home3)).expect("valid");
    let rt3 = Runtime::start(&cfg3).expect("boot");
    assert!(rt3.activate("alice", "echo", 20000).is_err(), "revocation persists across restore");
    rt3.shutdown();
    wait_for("quiet", Duration::from_secs(2), || true);
}
