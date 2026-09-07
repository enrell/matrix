//! Executor accept loop for `matrix.remote/0.1` (M7 route B listener).
//!
//! Binding is explicit (`Service::serve_remote_session`); nothing
//! listens by default. Each authorized peer gets one [`Route`]; the
//! route serves calls, queries, renewals, inventory and events on the
//! session, plus a liveness watcher that pushes `revoke.notice` when
//! local authority disappears.

use crate::route_executor::Route;
use crate::service::Service;
use crate::session::{self, SessionLimits};
use serde_json::json;
use std::net::{SocketAddr, TcpListener};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

pub struct RemoteSessionServer {
    pub address: SocketAddr,
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl RemoteSessionServer {
    pub fn bind(
        service: Arc<Service>,
        config: Arc<rustls::ServerConfig>,
        address: SocketAddr,
    ) -> Result<Self, String> {
        let listener = TcpListener::bind(address).map_err(err)?;
        listener.set_nonblocking(true).map_err(err)?;
        let address = listener.local_addr().map_err(err)?;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let join = std::thread::spawn(move || {
            let mut workers: Vec<std::thread::JoinHandle<()>> = vec![];
            while !flag.load(Ordering::SeqCst) {
                let mut i = 0;
                while i < workers.len() {
                    if workers[i].is_finished() {
                        let _ = workers.swap_remove(i).join();
                    } else {
                        i += 1;
                    }
                }
                match listener.accept() {
                    Ok((tcp, _)) => {
                        let service = service.clone();
                        let config = config.clone();
                        workers.push(std::thread::spawn(move || {
                            let limits = SessionLimits::default();
                            let r = session::Session::accept(
                                tcp,
                                config,
                                limits,
                                &|p| service.authorized(p),
                                |hello| {
                                    Ok(json!({
                                        "version": "0.1",
                                        "features": ["remote-calls/1", "remote-streams/1", "remote-events/1", "remote-ops/1"],
                                        "executor_epoch": service.store.epoch.to_string(),
                                        "limits": {"max_frame": 1048576},
                                        "domain": hello.get("domain").cloned().unwrap_or_default(),
                                    }))
                                },
                            );
                            match r {
                                Ok((sess, _)) => {
                                    let route = Route::new(service.clone(), sess);
                                    service.track_route(&route);
                                    route.attach();
                                    Route::watch(route);
                                }
                                Err(_) => {
                                    // Refused handshakes fail closed, silently.
                                }
                            }
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    Err(_) => break,
                }
            }
            for w in workers {
                let _ = w.join();
            }
        });
        Ok(Self { address, stop, join: Some(join) })
    }

    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl Drop for RemoteSessionServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Route {
    /// Liveness watcher: pushes `revoke.notice` when a served provider
    /// loses local authority (withdraw/revoke/expiry), then keeps
    /// watching (re-activation is learned via reconcile). Ends with the
    /// session. Best effort, never blocks.
    pub fn watch(route: Arc<Self>) {
        std::thread::Builder::new()
            .name("matrix-route-watch".into())
            .spawn(move || {
                let mut last: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
                loop {
                    std::thread::sleep(Duration::from_millis(100));
                    if route.session_state() == crate::session::SessionState::Detached {
                        return;
                    }
                    let live = route.served_providers();
                    let mut cur: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
                    for (logical, fence) in live {
                        cur.insert(logical.clone(), fence);
                    }
                    for (logical, fence) in &last {
                        if !cur.contains_key(logical) {
                            route.notify_revoked(logical, *fence);
                        }
                    }
                    last = cur;
                }
            })
            .ok();
    }
}
