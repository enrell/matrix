//! Managed composition (M6): chain under operator grants.
//!
//! `matrix-managed` service with external components: absent grant denies,
//! present grant admits and dispatches, revocation via `sync_outbound_grants`
//! invalidates. Extension announcement enabled in the managed profile.

mod common;
use common::*;
use matrix_host::HostPolicy;
use matrix_runtime::service::{Grant, Service};
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

fn dep_bin() -> std::path::PathBuf {
    let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/release/examples/dep_node");
    assert!(p.exists(), "build matrix-host --examples first: {:?}", p);
    p
}

fn cons_manifest(entry: &str) -> serde_json::Value {
    json!({
        "id": "cons", "capabilities": ["cons.chain@1"],
        "requires": [{"interface": "prov.api@1", "provider": "prov"}],
        "outbound": {"request": ["prov.api@1"], "limits": {
            "max_depth": 3, "max_children_per_parent": 4, "max_calls_per_session": 16,
            "max_calls_global": 64, "max_seen_requests": 64,
            "max_queued_bytes": 65536, "max_deadline_ms": 12000}},
        "execution": {"kind": "process", "entrypoint": entry,
            "args": ["--matrix-sock", "{sock}", "--id", "{id}"]},
    })
}

fn prov_manifest(entry: &str) -> serde_json::Value {
    json!({
        "id": "prov", "capabilities": ["prov.api@1"],
        "execution": {"kind": "process", "entrypoint": entry,
            "args": ["--matrix-sock", "{sock}", "--id", "{id}"]},
    })
}

fn rig(grants: HashMap<String, Vec<String>>) -> (std::path::PathBuf, std::sync::Arc<Service>) {
    let p = home();
    let entry = dep_bin().to_string_lossy().to_string();
    let grant = Grant {
        components: ["cons".into(), "prov".into()].into(),
        capabilities: ["cons.chain@1".into(), "prov.api@1".into()].into(),
    };
    let s = Service::open(
        &p,
        HostPolicy {
            secure: true,
            components: [("cons".into(), None), ("prov".into(), None)].into(),
            enable_dependency_calls: false, // Service::open habilita no perfil gerenciado
            domain: String::new(),
        },
        [("alice".into(), grant)].into(),
        HashMap::new(),
    )
    .unwrap();
    s.sync_outbound_grants(&grants);
    s.provision(&cons_manifest(&entry)).unwrap();
    s.provision(&prov_manifest(&entry)).unwrap();
    (p, s)
}

fn lease(s: &Service, logical: &str) -> (String, u64) {
    let a = s.activate("alice", logical, 20000).unwrap();
    let r = s.kernel.instance_ref_of(logical).unwrap();
    wait(|| s.host.has_session(logical, r.instance));
    (a["lease"].as_str().unwrap().to_string(), a["fence"].as_str().unwrap().parse().unwrap())
}

fn invoke_chain(s: &Service, tok: &str, fence: u64, op: &str, input: serde_json::Value) -> serde_json::Value {
    s.invoke("alice", tok, fence, op, "cons.chain@1", &input).unwrap()
}

#[test]
fn managed_chain_needs_operator_grant() {
    let (_p, s) = rig(HashMap::new());
    // Provider first (cons needs `prov.api@1`; without it, cons waits).
    let (_ptok, _pfence) = lease(&s, "prov");
    let (ctok, cfence) = lease(&s, "cons");
    // No operator grant: a valid request denies at admission.
    let v = invoke_chain(&s, &ctok, cfence, "op-deny", json!({"chain": true, "input": {"value": 1}}));
    assert_eq!(v["ok"], false, "{v}");
    // Operator grant: the same call admits and dispatches.
    s.sync_outbound_grants(&[("cons".to_string(), vec!["prov.api@1".to_string()])].into());
    let v = invoke_chain(&s, &ctok, cfence, "op-ok", json!({"chain": true, "input": {"value": 2}}));
    assert_eq!(v["ok"], true, "{v}");
    assert_eq!(v["value"]["chained"]["echo"]["value"], 2, "{v}");
    // Revocation: denies again; no residue.
    s.sync_outbound_grants(&HashMap::new());
    let v = invoke_chain(&s, &ctok, cfence, "op-revoked", json!({"chain": true, "input": {"value": 3}}));
    assert_eq!(v["ok"], false, "{v}");
    s.shutdown();
    let _ = Duration::from_secs(1);
}
