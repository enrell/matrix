use matrix_runtime::store::{Admission, Store};
use serde_json::json;
fn home() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("mxstore-{}", matrix_guard::random_token().unwrap()));
    std::fs::create_dir_all(&p).unwrap();
    p
}
#[test]
fn c16_exclusive_owner_and_recovery_epoch() {
    let p = home();
    let path = p.join("store.db");
    let a = Store::open(&path).unwrap();
    let epoch = a.epoch;
    assert!(Store::open(&path).is_err());
    drop(a);
    let b = Store::open(&path).unwrap();
    assert!(b.epoch > epoch);
}
#[test]
fn c17_dedup_unknown_and_conflicting_payload() {
    let p = home();
    let path = p.join("s.db");
    let a = Store::open(&path).unwrap();
    assert_eq!(
        a.admit("peer", "op", &json!({"x":1})).unwrap(),
        Admission::New
    );
    assert_eq!(
        a.admit("peer", "op", &json!({"x":1})).unwrap(),
        Admission::Unknown
    );
    assert!(a.admit("peer", "op", &json!({"x":2})).is_err());
    a.finish("peer", "op", &json!({"done":true}), false)
        .unwrap();
    drop(a);
    let b = Store::open(&path).unwrap();
    assert_eq!(
        b.admit("peer", "op", &json!({"x":1})).unwrap(),
        Admission::Completed(json!({"done":true}))
    );
    assert_eq!(
        b.admit("other", "op", &json!({"x":2})).unwrap(),
        Admission::New
    );
}
#[test]
fn c18_snapshot_preserves_unknown_desired_and_fences() {
    let p = home();
    let a = Store::open(&p.join("s.db")).unwrap();
    a.set_desired("echo", &json!({"id":"echo"})).unwrap();
    a.admit("peer", "pending", &json!({})).unwrap();
    a.fence("disk", 9).unwrap();
    for i in 0..10010 {
        a.event("test", &json!({"i":i})).unwrap();
    }
    let snap = p.join("snapshot.db");
    a.snapshot(&snap).unwrap();
    assert!(a.snapshot(&snap).is_err());
    let b = Store::open(&snap).unwrap();
    assert_eq!(b.desired().unwrap().len(), 1);
    assert_eq!(b.operation("peer", "pending").unwrap()["state"], "unknown");
    assert!(b.fence("disk", 8).is_err());
    b.fence("disk", 10).unwrap();
}
#[test]
fn c18_corruption_fails_closed() {
    let p = home();
    std::fs::write(p.join("s.db"), b"not a sqlite database").unwrap();
    assert!(Store::open(&p.join("s.db")).is_err());
}
#[test]
fn crash_worker() {
    let Ok(path) = std::env::var("MATRIX_CRASH_STORE") else {
        return;
    };
    let s = Store::open(std::path::Path::new(&path)).unwrap();
    s.admit("peer", "effect", &json!({"action":"external"}))
        .unwrap();
    if std::env::var("MATRIX_CRASH_PHASE").unwrap() == "completed" {
        s.finish("peer", "effect", &json!({"out":42}), false)
            .unwrap();
    }
    unsafe {
        libc::kill(libc::getpid(), libc::SIGKILL);
    }
}
#[test]
fn c16_sigkill_after_admission_and_after_commit() {
    use std::os::unix::process::ExitStatusExt;
    for phase in ["admitted", "completed"] {
        let p = home();
        let path = p.join("s.db");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_worker", "--nocapture"])
            .env("MATRIX_CRASH_STORE", &path)
            .env("MATRIX_CRASH_PHASE", phase)
            .status()
            .unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let s = Store::open(&path).unwrap();
        let v = s.operation("peer", "effect").unwrap();
        assert_eq!(
            v["state"],
            if phase == "admitted" {
                "unknown"
            } else {
                "completed"
            }
        );
    }
}
