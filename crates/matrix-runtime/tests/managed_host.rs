mod common;
use common::*;
use matrix_guard::{RestartPolicy, Sandbox};
use matrix_host::HostPolicy;
use matrix_runtime::service::Service;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;
fn ext_bin() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/release/examples/ext_echo")
}
#[test]
fn c10_secure_host_runs_sandboxed_component_and_cleans_process() {
    let p = home();
    let work = p.join("workspace");
    std::fs::create_dir_all(&work).unwrap();
    let sandbox = Sandbox {
        workspace: work.clone(),
        read_only: vec![],
        memory_bytes: 256 * 1024 * 1024,
        cpu_seconds: 5,
        file_bytes: 1024 * 1024,
        open_files: 64,
    };
    let s = Service::open(
        &p,
        HostPolicy {
            secure: true,
            components: [("echo".into(), Some(sandbox))].into(),
            enable_dependency_calls: false, domain: String::new()
        },
        [("alice".into(), grant())].into(),
        HashMap::new(),
    )
    .unwrap();
    assert!(ext_bin().exists(), "build matrix-host --examples first");
    s.provision(&json!({"id":"echo","capabilities":["echo.msg@1"],"execution":{"kind":"process","entrypoint":ext_bin(),"args":["--matrix-sock","{sock}","--id","{id}"]}})).unwrap();
    let a = s.activate("alice", "echo", 10000).unwrap();
    let r = s.kernel.instance_ref_of("echo").unwrap();
    wait(|| s.host.has_session("echo", r.instance));
    let fence = a["fence"].as_str().unwrap().parse().unwrap();
    let token = a["lease"].as_str().unwrap();
    let v = s
        .invoke(
            "alice",
            token,
            fence,
            "call",
            "echo.msg@1",
            &json!({"count_file":"/workspace/count","ping":true}),
        )
        .unwrap();
    assert_eq!(v["ok"], true, "{v}");
    assert!(work.join("count").exists());
    let again = s
        .invoke(
            "alice",
            token,
            fence,
            "call",
            "echo.msg@1",
            &json!({"count_file":"/workspace/count","ping":true}),
        )
        .unwrap();
    assert_eq!(again, v);
    assert_eq!(
        std::fs::read_to_string(work.join("count"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    s.release("alice", token, fence).unwrap();
    assert!(s.host.wait_no_child("echo", Duration::from_secs(2)));
    s.shutdown();
}
#[test]
fn c15_real_crashing_process_restarts_then_stops() {
    let p = home();
    let s = Service::open(
        &p,
        HostPolicy {
            secure: true,
            components: [("echo".into(), None)].into(),
            enable_dependency_calls: false, domain: String::new()
        },
        [("alice".into(), grant())].into(),
        [(
            "echo".into(),
            RestartPolicy {
                max_restarts: 2,
                window_ms: 10000,
                backoff_ms: 10,
            },
        )]
        .into(),
    )
    .unwrap();
    s.provision(&json!({"id":"echo","capabilities":["echo.msg@1"],"execution":{"kind":"process","entrypoint":"/usr/bin/false"}})).unwrap();
    s.activate("alice", "echo", 10000).unwrap();
    wait(|| s.inspect()["leases"][0]["failed"] == true);
    let all = s.kernel.contexts.current_all();
    let last = all.get("echo").unwrap();
    assert!(last.generation >= 3);
    let generation = last.generation;
    std::thread::sleep(Duration::from_millis(350));
    assert_eq!(
        s.kernel.contexts.current("echo").unwrap().generation,
        generation
    );
    s.shutdown();
}

#[test]
fn c10_unlaunched_component_cannot_impersonate_existing_instance() {
    let p = home();
    let s = service(&p, "alice");
    s.activate("alice", "echo", 1000).unwrap();
    let attempted = matrix_component::Component::connect(&s.host.sock_path(), "echo");
    assert!(
        attempted.is_err(),
        "registration without launch authority must fail"
    );
    s.shutdown();
}

#[test]
fn c24_python_component_also_runs_inside_secure_profile() {
    let p = home();
    let work = p.join("workspace");
    std::fs::create_dir_all(&work).unwrap();
    let sdk = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../sdk-python")
        .canonicalize()
        .unwrap();
    let sandbox = Sandbox {
        workspace: work.clone(),
        read_only: vec![sdk.clone()],
        memory_bytes: 256 * 1024 * 1024,
        cpu_seconds: 5,
        file_bytes: 1024 * 1024,
        open_files: 64,
    };
    let s = Service::open(
        &p,
        HostPolicy {
            secure: true,
            components: [("echo".into(), Some(sandbox))].into(),
            enable_dependency_calls: false, domain: String::new()
        },
        [("alice".into(), grant())].into(),
        HashMap::new(),
    )
    .unwrap();
    s.provision(&json!({"id":"echo","capabilities":["echo.msg@1"],"execution":{"kind":"process","entrypoint":"/usr/bin/python3","args":[sdk.join("ext_echo.py"),"--matrix-sock","{sock}","--id","{id}"]}})).unwrap();
    let a = s.activate("alice", "echo", 10000).unwrap();
    let r = s.kernel.instance_ref_of("echo").unwrap();
    wait(|| s.host.has_session("echo", r.instance));
    let v = s
        .invoke(
            "alice",
            a["lease"].as_str().unwrap(),
            a["fence"].as_str().unwrap().parse().unwrap(),
            "python",
            "echo.msg@1",
            &json!({"language":"python"}),
        )
        .unwrap();
    assert_eq!(v["value"]["echo"]["language"], "python");
    s.shutdown();
}
