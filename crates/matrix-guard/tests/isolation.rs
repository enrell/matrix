use matrix_guard::{kill_group, spawn, RestartBudget, RestartPolicy, Sandbox};
use std::os::unix::net::UnixListener;
use std::time::{Duration, Instant};
fn home() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("mxguard-{}", matrix_guard::random_token().unwrap()));
    std::fs::create_dir_all(&p).unwrap();
    p
}
#[test]
fn c11_files_network_processes_environment_memory_are_restricted() {
    let p = home();
    let socket = p.join("s");
    let _listener = UnixListener::bind(&socket).unwrap();
    let outside = p.join("private-secret");
    std::fs::write(&outside, "never expose").unwrap();
    let work = p.join("workspace");
    std::fs::create_dir(&work).unwrap();
    let s = Sandbox {
        workspace: work.clone(),
        read_only: vec![],
        memory_bytes: 192 * 1024 * 1024,
        cpu_seconds: 3,
        file_bytes: 1024 * 1024,
        open_files: 64,
    };
    let code = format!(
        r#"
import os,socket,json
checks={{}}
checks['private_hidden']=not os.path.exists({outside:?})
try: open('/usr/matrix-escape','w'); checks['readonly']=False
except OSError: checks['readonly']=True
try: os.fork(); checks['fork_denied']=False
except OSError: checks['fork_denied']=True
try: bytearray(512*1024*1024); checks['memory_limited']=False
except MemoryError: checks['memory_limited']=True
try:
 s=socket.socket();s.settimeout(.2);s.connect(('1.1.1.1',443));checks['network_denied']=False
except OSError: checks['network_denied']=True
checks['env_filtered']='MATRIX_TEST_SECRET' not in os.environ
checks['writable']=True
open('/workspace/result.json','w').write(json.dumps(checks))
"#,
        outside = outside.to_string_lossy()
    );
    // The sandbox policy filters all inherited variables, regardless of value.
    std::env::set_var("MATRIX_TEST_SECRET", "test-only-marker");
    let mut c = spawn(
        "/usr/bin/python3",
        &["-c".into(), code],
        "test-token",
        &socket,
        Some(&s),
    )
    .unwrap();
    let status = c.wait().unwrap();
    assert!(status.success(), "sandbox failed: {status}");
    let v: serde_json::Value =
        serde_json::from_slice(&std::fs::read(work.join("result.json")).unwrap()).unwrap();
    for (k, v) in v.as_object().unwrap() {
        assert_eq!(v, true, "{k}");
    }
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn c11_cpu_loop_is_terminated() {
    let p = home();
    let sock = p.join("s");
    let _l = UnixListener::bind(&sock).unwrap();
    let s = Sandbox {
        workspace: p.clone(),
        read_only: vec![],
        memory_bytes: 128 * 1024 * 1024,
        cpu_seconds: 1,
        file_bytes: 1048576,
        open_files: 64,
    };
    let mut c = spawn(
        "/usr/bin/python3",
        &["-c".into(), "while True: pass".into()],
        "t",
        &sock,
        Some(&s),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(status) = c.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        if Instant::now() > deadline {
            kill_group(&mut c).unwrap();
            panic!("CPU budget not enforced");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn c15_restart_budget_and_backoff_are_finite() {
    let now = Instant::now();
    let p = RestartPolicy {
        max_restarts: 2,
        window_ms: 1000,
        backoff_ms: 10,
    };
    let mut b = RestartBudget::default();
    let a = b.reserve(now, &p).unwrap();
    let c = b.reserve(now, &p).unwrap();
    assert!(c > a);
    assert!(b.reserve(now, &p).is_none());
    assert!(b.reserve(now + Duration::from_secs(2), &p).is_some());
}
