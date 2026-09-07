mod common;
use common::*;
use matrix_runtime::remote::{RemoteForwarder, Server};
use serde_json::{json, Value};
use std::sync::Arc;
fn f(v: &Value) -> u64 {
    v["fence"].as_str().unwrap().parse().unwrap()
}

#[test]
fn c10_grants_revocation_and_actual_fenced_writes() {
    let p = home();
    let s = service(&p, "alice");
    let a = s.activate("alice", "echo", 5000).unwrap();
    let token = a["lease"].as_str().unwrap();
    assert!(s
        .invoke("bob", token, f(&a), "bad", "echo.msg@1", &json!({}))
        .is_err());
    assert!(s
        .invoke("alice", token, f(&a), "bad", "admin.reset", &json!({}))
        .is_err());
    s.commit_effect("alice", token, f(&a), "write-a", "key", &json!(1))
        .unwrap();
    s.release("alice", token, f(&a)).unwrap();
    let b = s.activate("alice", "echo", 5000).unwrap();
    assert!(f(&b) > f(&a));
    assert!(s
        .commit_effect("alice", token, f(&a), "late", "key", &json!(99))
        .is_err());
    assert!(s
        .store
        .commit_effect("alice", "late-store", "echo", f(&a), "key", &json!(99))
        .is_err());
    assert_eq!(s.store.effect("echo", "key").unwrap(), Some(json!(1)));
    s.revoke("alice").unwrap();
    assert!(s.activate("alice", "echo", 1000).is_err());
    assert_eq!(s.inspect()["leases"].as_array().unwrap().len(), 0);
    s.shutdown();
}
#[test]
fn c20_expiry_removes_resources_without_client_release() {
    let p = home();
    let s = service(&p, "alice");
    let a = s.activate("alice", "echo", 100).unwrap();
    wait(|| s.inspect()["leases"].as_array().unwrap().is_empty());
    assert!(s.kernel.caps.resolve("echo.msg@1").is_none());
    assert!(s
        .renew("alice", a["lease"].as_str().unwrap(), f(&a), 1000)
        .is_err());
    s.shutdown();
}
#[test]
fn c20_delayed_renewal_cannot_extend_rotated_lease() {
    let p = home();
    let s = service(&p, "alice");
    let a = s.activate("alice", "echo", 1000).unwrap();
    let b = s
        .renew("alice", a["lease"].as_str().unwrap(), f(&a), 100)
        .unwrap();
    assert_ne!(a["lease"], b["lease"]);
    assert!(s
        .renew("alice", a["lease"].as_str().unwrap(), f(&a), 30000)
        .is_err());
    wait(|| s.inspect()["leases"].as_array().unwrap().is_empty());
    s.shutdown();
}
#[test]
fn c23_mutual_tls_pin_rotation_and_wrong_server_name() {
    let p = home();
    let principal = peer("client");
    let s = service(&p, &principal);
    let mut server =
        Server::bind(s.clone(), server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let c = client(server.address, "client");
    let a = c
        .request(json!({"action":"activate","component":"echo","ttl_ms":1000}))
        .unwrap();
    let mut wrong = c.clone();
    wrong.server_name = "not-localhost.example".into();
    assert!(wrong
        .request(json!({"action":"activate","component":"echo","ttl_ms":1000}))
        .is_err());
    let rotated = client(server.address, "rotated");
    assert!(rotated
        .request(json!({"action":"activate","component":"echo","ttl_ms":1000}))
        .is_err());
    s.grant(peer("rotated"), grant()).unwrap();
    s.revoke(&principal).unwrap();
    assert!(c.request(json!({"action":"invoke","lease":a["lease"],"fence":a["fence"],"operation":"denied","cap":"echo.msg@1","input":{}})).is_err());
    assert!(rotated
        .request(json!({"action":"activate","component":"echo","ttl_ms":1000}))
        .is_ok());
    server.shutdown();
    s.shutdown();
}
#[test]
fn c21_remote_effect_fence_and_reconnection_query() {
    let p = home();
    let principal = peer("client");
    let s = service(&p, &principal);
    let mut server =
        Server::bind(s.clone(), server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let c = client(server.address, "client");
    let a = c
        .request(json!({"action":"activate","component":"echo","ttl_ms":5000}))
        .unwrap();
    let req = json!({"action":"effect.commit","lease":a["lease"],"fence":a["fence"],"operation":"write","key":"result","value":42});
    c.request(req.clone()).unwrap();
    // A fresh TLS connection can query and deduplicate the durable result.
    assert_eq!(
        c.request(json!({"action":"operation","operation":"write"}))
            .unwrap()["state"],
        "completed"
    );
    c.request(req.clone()).unwrap();
    c.request(json!({"action":"release","lease":a["lease"],"fence":a["fence"]}))
        .unwrap();
    c.request(json!({"action":"activate","component":"echo","ttl_ms":1000}))
        .unwrap();
    assert!(c.request(req).is_err());
    assert_eq!(s.store.effect("echo", "result").unwrap(), Some(json!(42)));
    server.shutdown();
    s.shutdown();
}
#[test]
fn c22_restart_recovers_desired_and_requires_fresh_lease() {
    let p = home();
    let s = service(&p, "alice");
    let a = s.activate("alice", "echo", 5000).unwrap();
    let epoch = s.store.epoch;
    s.invoke(
        "alice",
        a["lease"].as_str().unwrap(),
        f(&a),
        "op",
        "echo.msg@1",
        &json!({"ping":1}),
    )
    .unwrap();
    s.shutdown();
    drop(s);
    let s = service(&p, "alice");
    assert!(s.store.epoch > epoch);
    assert_eq!(s.store.desired().unwrap().len(), 1);
    assert!(s.kernel.caps.resolve("echo.msg@1").is_none());
    assert!(s
        .invoke(
            "alice",
            a["lease"].as_str().unwrap(),
            f(&a),
            "new",
            "echo.msg@1",
            &json!({})
        )
        .is_err());
    let b = s.activate("alice", "echo", 1000).unwrap();
    assert!(f(&b) > f(&a));
    assert_eq!(
        s.store.operation("alice", "op").unwrap()["state"],
        "completed"
    );
    s.shutdown();
}
#[test]
fn remote_plugin_is_callable_through_controller_kernel() {
    let p = home();
    let principal = peer("client");
    let s = service(&p, &principal);
    let mut server =
        Server::bind(s.clone(), server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let c = client(server.address, "client");
    let a = c
        .request(json!({"action":"activate","component":"echo","ttl_ms":5000}))
        .unwrap();
    let local = home();
    std::fs::create_dir_all(local.join("plugins")).unwrap();
    let k = matrix_core::Kernel::new(
        &local,
        matrix_core::Journal::open(&local.join("run/journal.jsonl"), false, false).unwrap(),
        false,
    );
    let manifest = local.join("plugins/proxy.json");
    std::fs::write(&manifest,json!({"id":"proxy","capabilities":["echo.msg@1"],"execution":{"kind":"process","entrypoint":"remote-profile"}}).to_string()).unwrap();
    k.load_manifest(&manifest).unwrap();
    let r = k.instance_ref_of("proxy").unwrap();
    k.set_forwarder(Arc::new(RemoteForwarder {
        client: c.clone(),
        logical: "proxy".into(),
        instance: r.instance,
        generation: r.generation,
        lease: a["lease"].as_str().unwrap().into(),
        fence: f(&a),
    }));
    let (v, ok) = k.invoke("echo.msg@1", &json!({"remote":true}));
    assert!(ok, "{v}");
    assert_eq!(v["echo"]["remote"], true);
    k.load_manifest(&manifest).unwrap();
    let (v, ok) = k.invoke("echo.msg@1", &json!({}));
    assert!(!ok);
    assert_eq!(v["code"], "stale-generation");
    server.shutdown();
    s.shutdown();
}

#[test]
fn c20_proxy_withdrawal_releases_remote_component() {
    let p = home();
    let s = service(&p, &peer("client"));
    let mut server =
        Server::bind(s.clone(), server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let local = home();
    std::fs::create_dir_all(local.join("plugins")).unwrap();
    let k = Arc::new(matrix_core::Kernel::new(
        &local,
        matrix_core::Journal::open(&local.join("run/journal.jsonl"), false, false).unwrap(),
        false,
    ));
    let path = local.join("plugins/proxy.json");
    std::fs::write(&path,json!({"id":"proxy","capabilities":["echo.msg@1"],"execution":{"kind":"process","entrypoint":"remote-profile"}}).to_string()).unwrap();
    let _proxy = matrix_runtime::remote::RemoteProxy::attach(
        &k,
        client(server.address, "client"),
        "echo",
        &path,
    )
    .unwrap();
    let (v, ok) = k.invoke("echo.msg@1", &json!({"connected":true}));
    assert!(ok, "{v}");
    k.dispose_plugin("proxy");
    wait(|| s.inspect()["leases"].as_array().unwrap().is_empty());
    assert!(s.kernel.caps.resolve("echo.msg@1").is_none());
    server.shutdown();
    s.shutdown();
}

#[test]
fn c20_network_loss_cascades_locally_without_harming_independent() {
    let p = home();
    let s = service(&p, &peer("client"));
    let mut server =
        Server::bind(s.clone(), server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let local = home();
    std::fs::create_dir_all(local.join("plugins")).unwrap();
    let k = Arc::new(matrix_core::Kernel::new(
        &local,
        matrix_core::Journal::open(&local.join("run/journal.jsonl"), false, false).unwrap(),
        false,
    ));
    let path = local.join("plugins/proxy.json");
    std::fs::write(&path,json!({"id":"proxy","capabilities":["echo.msg@1"],"execution":{"kind":"process","entrypoint":"remote-profile"}}).to_string()).unwrap();
    let _proxy = matrix_runtime::remote::RemoteProxy::attach(
        &k,
        client(server.address, "client"),
        "echo",
        &path,
    )
    .unwrap();
    for (id, m) in [
        (
            "search",
            json!({"id":"search","capabilities":["search@1"],"requires":["echo.msg@1"],"reducer":"noop"}),
        ),
        (
            "independent",
            json!({"id":"independent","capabilities":["local.echo@1"],"reducer":"echo"}),
        ),
    ] {
        let p = local.join(format!("plugins/{id}.json"));
        std::fs::write(&p, m.to_string()).unwrap();
        k.load_manifest(&p).unwrap();
    }
    assert!(k.invoke("search@1", &json!({})).1);
    server.shutdown(); // Network endpoint gone; remote service/host remains alive.
    wait(|| k.context_state_of("search").as_deref() == Some("Waiting"));
    assert!(k.invoke("local.echo@1", &json!({})).1);
    wait(|| s.inspect()["leases"].as_array().unwrap().is_empty());
    s.shutdown();
}
#[test]
fn c23_certificate_is_required() {
    let p = home();
    let s = service(&p, &peer("client"));
    let mut server =
        Server::bind(s.clone(), server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(
            std::fs::read(certificates().join("ca.der")).unwrap(),
        ))
        .unwrap();
    let mut cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![matrix_runtime::remote::PROFILE.as_bytes().to_vec()];
    let c = matrix_runtime::remote::Client {
        address: server.address,
        server_name: "localhost".into(),
        config: Arc::new(cfg),
    };
    assert!(c
        .request(json!({"action":"activate","component":"echo","ttl_ms":1000}))
        .is_err());
    server.shutdown();
    s.shutdown();
}

#[test]
fn c23_revocation_survives_restart_until_explicit_regrant() {
    let p = home();
    let s = service(&p, "alice");
    s.revoke("alice").unwrap();
    s.shutdown();
    drop(s);
    let s = service(&p, "alice");
    assert!(!s.authorized("alice"));
    assert!(s.activate("alice", "echo", 1000).is_err());
    s.grant("alice".into(), grant()).unwrap();
    assert!(s.activate("alice", "echo", 1000).is_ok());
    s.shutdown();
}
