//! M8 facade contract: external applications use only
//! `matrix_runtime::api` (+ serde_json). This file must not import
//! `service`, `store`, `session`, `route_*`, `remote*`, kernel, host or
//! guard items — `scripts/check-harness-bounds.sh` enforces the same rule
//! on the external harness.

use matrix_runtime::api::{
    Config, ErrorCode, InspectOpts, Runtime, API_VERSION, INSPECT_SCHEMA,
};
use serde_json::{json, Value};

fn home(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "mxapi-{}-{}",
        tag,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn config(home: &std::path::Path) -> Value {
    json!({
        "home": home.to_string_lossy().to_string(),
        "components": [
            {"manifest": {"id": "echo", "capabilities": ["echo.msg@1"], "reducer": "echo"}, "trusted": true},
        ],
        "grants": {"alice": {"components": ["echo"], "capabilities": ["echo.msg@1"]}},
    })
}

#[test]
fn validate_diagnoses_before_any_mutation() {
    let dir = home("validate");
    // Unknown field, untrusted launch, dangling grant, bad route: all
    // reported, nothing started (home stays without state dir).
    // Unknown top-level field rejects at parse (schema first).
    let unknown = json!({"home": "/x", "components": [], "grants": {}, "bogus_top": 1});
    assert!(Config::parse(&unknown).is_err());
    let bad = json!({
        "home": dir.to_string_lossy().to_string(),
        "components": [
            {"manifest": {"id": "echo", "capabilities": ["echo.msg@1"]}},
            {"manifest": {"id": "echo", "capabilities": ["echo.msg@1"]}, "trusted": true},
        ],
        "grants": {"alice": {"components": ["ghost"], "capabilities": ["echo.msg@1"]}},
        "outbound_grants": {"nobody": []},
        "remotes": {"routes": [{"consumer": "echo", "provider": "p", "peer": "nope", "capabilities": []}]},
    });
    let diags = Config::parse(&bad).map(|c| c.validate());
    let diags = match diags {
        Ok(d) => d,
        Err(schema) => schema,
    };
    assert!(!diags.is_empty(), "invalid config diagnoses");
    let fields: Vec<_> = diags.iter().map(|d| d.field.as_str()).collect();
    assert!(fields.iter().any(|f| f.contains("grants")), "{fields:?}");
    assert!(fields.iter().any(|f| f.contains("remotes")), "{fields:?}");
    assert!(!dir.join("state").exists(), "no mutation before validation");
    // The same document must not start a runtime either.
    if let Ok(cfg) = Config::parse(&bad) {
        assert!(Runtime::start(&cfg).is_err());
    }
    assert!(!dir.join("state").exists(), "failed start leaves no state");
    // Forward references resolve regardless of declaration order.
    let fwd = json!({
        "home": dir.to_string_lossy().to_string(),
        "components": [
            {"manifest": {"id": "cons", "capabilities": ["c@1"],
                "requires": [{"interface": "p@1", "provider": "prov"}]}, "trusted": true},
            {"manifest": {"id": "prov", "capabilities": ["p@1"]}, "trusted": true},
        ],
        "grants": {},
    });
    let diags = Config::parse(&fwd).map(|c| c.validate()).unwrap_or_default();
    assert!(!diags.iter().any(|d| d.field.contains("requires")), "{diags:?}");
}

#[test]
fn reload_rotates_and_gates_restart_fields() {
    // Facade owns the rotation: reload drops removed principals itself
    // (no daemon side-channel), and restart-required fields diagnose
    // instead of applying silently.
    let dir = home("reloadrot");
    let base = config(&dir);
    let cfg = Config::parse(&base).expect("valid");
    let rt = Runtime::start(&cfg).expect("start");
    let act = rt.activate("alice", "echo", 20000).expect("activate");
    let token = act["lease"].as_str().unwrap().to_string();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    // Narrow the grants (drop alice entirely): reload revokes + applies.
    let mut narrow = base.clone();
    narrow["grants"] = json!({});
    let narrow_cfg = Config::parse(&narrow).expect("valid");
    let rep = rt.reload(&narrow_cfg).expect("reload applies");
    assert!(rep.revoked.contains(&"alice".to_string()), "{rep:?}");
    assert!(rep.errors.is_empty(), "{rep:?}");
    let denied = rt.invoke("alice", &token, fence, "op-r1", "echo.msg@1", &json!({}));
    assert!(denied.is_err());
    assert_eq!(denied.unwrap_err().code, ErrorCode::PermissionDenied);
    // Restart-required fields diagnose, mutate nothing.
    let mut moved = base.clone();
    moved["tls"] = json!({"listen": "127.0.0.1:9", "ca": "a", "cert": "b", "key": "c"});
    let moved_cfg = Config::parse(&moved).expect("parses");
    let diags = rt.reload(&moved_cfg).expect_err("restart-gated");
    assert!(diags.iter().any(|d| d.field == "tls"), "{diags:?}");
    // Manifests validate fully before any mutation: bad execution refused.
    let mut badexec = base.clone();
    badexec["components"] = json!([{"manifest": {"id": "echo", "capabilities": ["echo.msg@1"],
        "execution": {"kind": "process"}}, "trusted": true}]);
    let badexec_cfg = Config::parse(&badexec).expect("parses");
    let diags = badexec_cfg.validate();
    assert!(diags.iter().any(|d| d.field.contains("manifest")), "{diags:?}");
    // Unknown require provider diagnosed.
    let mut badreq = base.clone();
    badreq["components"] = json!([{"manifest": {"id": "echo", "capabilities": ["echo.msg@1"],
        "requires": [{"interface": "x@1", "provider": "ghost"}]}, "trusted": true}]);
    let badreq_cfg = Config::parse(&badreq).expect("parses");
    let diags = badreq_cfg.validate();
    assert!(diags.iter().any(|d| d.field.contains("requires")), "{diags:?}");
    rt.shutdown();
}

#[test]
fn reload_rejects_non_dynamic_component_fields() {
    // Finding A: sandbox/trusted/restart are boot-time policy. Changing
    // any one of them in isolation must diagnose (field `components`)
    // before any mutation — the runtime keeps serving on the old policy
    // and a repeated identical reload still applies cleanly.
    let dir = home("nondyn");
    let sandbox = json!({"workspace": "/tmp", "read_only": [], "memory_bytes": 67108864,
        "cpu_seconds": 5, "file_bytes": 1048576, "open_files": 64});
    let mut base = config(&dir);
    base["components"] = json!([
        {"manifest": {"id": "echo", "capabilities": ["echo.msg@1"]},
         "sandbox": sandbox, "trusted": true, "restart": null},
    ]);
    let cfg = Config::parse(&base).expect("valid");
    assert!(cfg.validate().is_empty(), "{:?}", cfg.validate());
    let rt = Runtime::start(&cfg).expect("start");
    let act = rt.activate("alice", "echo", 20000).expect("activate");
    let token = act["lease"].as_str().unwrap().to_string();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    let mut variant = base.clone();
    // Case 1: sandbox workspace alone changes.
    variant["components"][0]["sandbox"]["workspace"] = json!("/tmp/other");
    let diags = rt.reload(&Config::parse(&variant).expect("parses")).expect_err("sandbox needs restart");
    assert!(diags.iter().any(|d| d.field == "components"), "{diags:?}");
    // Case 2: trusted alone flips (sandbox still present, stays valid).
    let mut variant = base.clone();
    variant["components"][0]["trusted"] = json!(false);
    let diags = rt.reload(&Config::parse(&variant).expect("parses")).expect_err("trust needs restart");
    assert!(diags.iter().any(|d| d.field == "components"), "{diags:?}");
    // Case 3: restart policy alone appears.
    let mut variant = base.clone();
    variant["components"][0]["restart"] = json!({"max_restarts": 3, "window_ms": 1000, "backoff_ms": 100});
    let diags = rt.reload(&Config::parse(&variant).expect("parses")).expect_err("restart needs restart");
    assert!(diags.iter().any(|d| d.field == "components"), "{diags:?}");
    // Nothing mutated by any rejection: original lease serves, and the
    // identical baseline still reloads cleanly afterwards.
    let v = rt.invoke("alice", &token, fence, "op-nd-1", "echo.msg@1", &json!({})).expect("intact");
    assert_eq!(v["ok"], true);
    let rep = rt.reload(&cfg).expect("baseline applies");
    assert!(rep.errors.is_empty(), "{rep:?}");
    rt.shutdown();
}

#[test]
fn remove_and_reprovision_turns_over() {
    // P08 at facade level: release retires the lease, remove reports the
    // withdrawal, re-provision + fresh lease serve again.
    let dir = home("turnover");
    let cfg = Config::parse(&config(&dir)).expect("valid");
    let rt = Runtime::start(&cfg).expect("start");
    let act = rt.activate("alice", "echo", 20000).expect("activate");
    let token = act["lease"].as_str().unwrap().to_string();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    assert!(rt.invoke("alice", &token, fence, "op-t1", "echo.msg@1", &json!({})).expect("invoke")["ok"] == true);
    // Operator update flow: release the old lease, then remove.
    rt.release("alice", &token, fence).expect("release");
    // Release retires the activation itself; remove is idempotent after.
    assert_eq!(rt.remove("echo").expect("remove"), "AlreadyDisposed");
    let dead = rt.invoke("alice", &token, fence, "op-t2", "echo.msg@1", &json!({}));
    assert!(dead.is_err(), "withdrawn activation serves nothing: {dead:?}");
    rt.provision(&json!({"id": "echo", "capabilities": ["echo.msg@1"], "reducer": "echo"}))
        .expect("reprovision");
    let act2 = rt.activate("alice", "echo", 20000).expect("fresh lease");
    let token2 = act2["lease"].as_str().unwrap().to_string();
    let fence2: u64 = act2["fence"].as_str().unwrap().parse().unwrap();
    assert_ne!(token, token2, "new activation, new credentials");
    let v = rt.invoke("alice", &token2, fence2, "op-t3", "echo.msg@1", &json!({"ping": 2})).expect("serve again");
    assert_eq!(v["value"]["echo"]["ping"], 2);
    // Old credentials stay dead across the turnover.
    assert!(rt.invoke("alice", &token, fence, "op-t4", "echo.msg@1", &json!({})).is_err());
    rt.shutdown();
}

#[test]
fn roundtrip_compose_inspect_shutdown() {
    let dir = home("roundtrip");
    let cfg = Config::parse(&config(&dir)).expect("valid");
    assert!(cfg.validate().is_empty());
    let rt = Runtime::start(&cfg).expect("start");
    let act = rt.activate("alice", "echo", 20000).expect("activate");
    let token = act["lease"].as_str().unwrap().to_string();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    let v = rt
        .invoke("alice", &token, fence, "op-facade-1", "echo.msg@1", &json!({"ping": 1}))
        .expect("invoke");
    assert_eq!(v["ok"], true, "{v}");
    assert_eq!(v["value"]["echo"]["ping"], 1);
    // Same operation replays (no re-execution); unknown principal denies
    // with a stable code, never a stringly check.
    let again = rt
        .invoke("alice", &token, fence, "op-facade-1", "echo.msg@1", &json!({"ping": 1}))
        .expect("replay");
    assert_eq!(again, v);
    let denied = rt.invoke("mallory", &token, fence, "op-x", "echo.msg@1", &json!({}));
    assert!(denied.is_err());
    assert_eq!(denied.unwrap_err().code, ErrorCode::PermissionDenied);
    // Inspection is versioned and redacted even with a live token held.
    let insp = rt.inspect(InspectOpts::default());
    assert_eq!(insp["schema"], INSPECT_SCHEMA);
    assert_eq!(insp["api"], API_VERSION);
    let text = serde_json::to_string(&insp).unwrap();
    assert!(!text.contains(&token), "no credentials in inspect");
    assert!(text.contains("echo"), "inventory visible: {text}");
    // Invalid reload changes nothing; valid reload reports per area.
    let bad_reload = json!({"home": dir.to_string_lossy().to_string(), "components": [], "grants": {}, "remotes": {"peers": [{"name": "x"}]}});
    if let Ok(cfg) = Config::parse(&bad_reload) {
        assert!(rt.reload(&cfg).is_err(), "invalid reload rejected");
    }
    let still = rt
        .invoke("alice", &token, fence, "op-facade-2", "echo.msg@1", &json!({}))
        .expect("state preserved across rejected reload");
    assert_eq!(still["ok"], true);
    let rep = rt.reload(&cfg).expect("valid reload applies");
    assert!(rep.grants_applied && rep.outbound_applied && rep.remotes_applied, "{rep:?}");
    assert!(rep.errors.is_empty(), "{rep:?}");
    // Verified shutdown (second call idempotent, zeros throughout).
    let shut = rt.shutdown();
    assert_eq!(shut.sessions_after, 0, "{shut:?}");
    assert_eq!(shut.leases_after, 0, "{shut:?}");
    assert_eq!(shut.pending_calls_after, 0, "{shut:?}");
    let shut2 = rt.shutdown();
    assert_eq!(shut2.sessions_after, 0, "{shut2:?}");
}

#[test]
fn reload_partial_failure_stays_recoverable() {
    // Two-component base so each principal can hold a lease at once.
    // Finding B: a reload that applies grants but records errors elsewhere
    // (remotes on a local-only runtime) must still track the applied set:
    // dropping such a grant later revokes exactly it — nothing leaks
    // invisibly beside the baseline.
    let dir = home("partial");
    let mut base = config(&dir);
    base["components"] = json!([
        {"manifest": {"id": "echo", "capabilities": ["echo.msg@1"], "reducer": "echo"}, "trusted": true},
        {"manifest": {"id": "echo2", "capabilities": ["echo2.msg@1"], "reducer": "echo"}, "trusted": true},
    ]);
    base["grants"] = json!({"alice": {"components": ["echo"], "capabilities": ["echo.msg@1"]}});
    let cfg = Config::parse(&base).expect("valid");
    let rt = Runtime::start(&cfg).expect("start");
    let act = rt.activate("alice", "echo", 20000).expect("activate");
    let token = act["lease"].as_str().unwrap().to_string();
    let fence: u64 = act["fence"].as_str().unwrap().parse().unwrap();
    let mut wider = base.clone();
    wider["grants"] = json!({
        "alice": {"components": ["echo"], "capabilities": ["echo.msg@1"]},
        "bob": {"components": ["echo2"], "capabilities": ["echo2.msg@1"]},
    });
    wider["remotes"] = json!({
        "peers": [{"name": "p", "address": "127.0.0.1:9", "server_name": "h",
            "ca": "a", "cert": "b", "key": "c", "mgmt_address": "127.0.0.1:8",
            "domain": "test", "lease_ttl_ms": 1000}],
        "routes": [],
    });
    let wider_cfg = Config::parse(&wider).expect("parses");
    let rep = rt.reload(&wider_cfg).expect("report, not refusal");
    assert!(rep.grants_applied, "{rep:?}");
    assert!(!rep.errors.is_empty(), "remotes without a manager must error: {rep:?}");
    // bob's grant went live despite the remotes error: activatable now.
    let bact = rt.activate("bob", "echo2", 20000).expect("bob live");
    assert_eq!(bact["fence"].as_str().unwrap(), "1");
    // Live truth untouched otherwise: alice still serves.
    let v = rt.invoke("alice", &token, fence, "op-p1", "echo.msg@1", &json!({})).expect("intact");
    assert_eq!(v["ok"], true);
    // Recovery: dropping bob revokes exactly bob (tracked baseline), and a
    // clean narrow config applies with no errors afterwards.
    let mut narrow = base.clone();
    narrow["grants"] = json!({"alice": {"components": ["echo"], "capabilities": ["echo.msg@1"]}});
    let rep = rt.reload(&Config::parse(&narrow).expect("parses")).expect("narrow applies");
    assert_eq!(rep.revoked, vec!["bob".to_string()], "only the live grant retires: {rep:?}");
    assert!(rep.errors.is_empty(), "{rep:?}");
    assert!(rt.activate("bob", "echo2", 20000).is_err(), "bob stays dead");
    rt.shutdown();
}
