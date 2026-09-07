#![allow(dead_code)]
pub mod proxy;
use matrix_host::HostPolicy;
use matrix_runtime::{
    remote,
    service::{Grant, Service},
};
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn home() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "mxrt-{}",
        &matrix_guard::random_token().unwrap()[..16]
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
pub fn grant() -> Grant {
    Grant {
        components: ["echo".into()].into(),
        capabilities: ["echo.msg@1".into(), "matrix.effect.write".into()].into(),
    }
}
pub fn service(p: &Path, principal: &str) -> Arc<Service> {
    let s = Service::open(
        p,
        HostPolicy {
            secure: true,
            components: HashMap::new(),
            enable_dependency_calls: false, domain: String::new()
        },
        [(principal.into(), grant())].into(),
        HashMap::new(),
    )
    .unwrap();
    s.provision(&json!({"id":"echo","capabilities":["echo.msg@1"],"reducer":"echo"}))
        .unwrap();
    s
}
pub fn wait(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while Instant::now() < end {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("condition timeout");
}
fn openssl(p: &Path, args: &[&str]) {
    let o = std::process::Command::new("openssl")
        .current_dir(p)
        .args(args)
        .output()
        .unwrap();
    assert!(
        o.status.success(),
        "openssl: {}",
        String::from_utf8_lossy(&o.stderr)
    );
}
pub fn certificates() -> PathBuf {
    static DIR: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    DIR.get_or_init(||{
        let p=home();
        openssl(&p,&["req","-x509","-newkey","ec","-pkeyopt","ec_paramgen_curve:P-256","-nodes","-keyout","ca.key","-out","ca.pem","-days","1","-subj","/CN=Matrix test CA","-addext","basicConstraints=critical,CA:TRUE"]);
        for name in ["server","client","rotated"] {
            openssl(&p,&["req","-new","-newkey","ec","-pkeyopt","ec_paramgen_curve:P-256","-nodes","-keyout",&format!("{name}.key"),"-out",&format!("{name}.csr"),"-subj",&format!("/CN={name}")]);
            let ext=if name=="server" {"subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n"}else{"extendedKeyUsage=clientAuth\nbasicConstraints=CA:FALSE\n"};
            std::fs::write(p.join("ext.cnf"),ext).unwrap();
            openssl(&p,&["x509","-req","-in",&format!("{name}.csr"),"-CA","ca.pem","-CAkey","ca.key","-CAcreateserial","-out",&format!("{name}.pem"),"-days","1","-extfile","ext.cnf"]);
            openssl(&p,&["x509","-in",&format!("{name}.pem"),"-outform","DER","-out",&format!("{name}.der")]);
            openssl(&p,&["pkcs8","-topk8","-nocrypt","-in",&format!("{name}.key"),"-outform","DER","-out",&format!("{name}-key.der")]);
        }
        openssl(&p,&["x509","-in","ca.pem","-outform","DER","-out","ca.der"]);p
    }).clone()
}
pub fn peer(name: &str) -> String {
    remote::fingerprint(&std::fs::read(certificates().join(format!("{name}.der"))).unwrap())
}
pub fn server_config() -> Arc<rustls::ServerConfig> {
    let p = certificates();
    remote::server_config(
        &p.join("ca.der"),
        &p.join("server.der"),
        &p.join("server-key.der"),
    )
    .unwrap()
}
pub fn client(address: std::net::SocketAddr, name: &str) -> remote::Client {
    let p = certificates();
    remote::Client {
        address,
        server_name: "localhost".into(),
        config: remote::client_config(
            &p.join("ca.der"),
            &p.join(format!("{name}.der")),
            &p.join(format!("{name}-key.der")),
        )
        .unwrap(),
    }
}
