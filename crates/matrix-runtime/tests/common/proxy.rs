//! Byte-level loopback fault-injection proxy (M7 failure suite).
//!
//! Forwards TCP both ways with per-direction programmable delay, drop
//! and gate (partition). Operates below TLS: no credentials, no payload
//! knowledge. Loopback only; never touches the machine's global network.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

/// Per-direction fault program. All fields live-update via atomics.
pub struct Direction {
    /// Fixed relay delay per chunk.
    pub delay_ms: AtomicU64,
    /// Drop (blackhole) every Nth chunk; 0 disables.
    pub drop_every: AtomicU64,
    /// Closed gate discards without forwarding (partition).
    pub closed: AtomicBool,
    chunks: AtomicU64,
    dropped_chunks: AtomicU64,
}

impl Direction {
    fn new() -> Self {
        Self {
            delay_ms: AtomicU64::new(0),
            drop_every: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            chunks: AtomicU64::new(0),
            dropped_chunks: AtomicU64::new(0),
        }
    }

    fn relay(&self, chunk: &[u8], out: &mut TcpStream) {
        let n = self.chunks.fetch_add(1, Ordering::SeqCst) + 1;
        let every = self.drop_every.load(Ordering::SeqCst);
        if self.closed.load(Ordering::SeqCst) || (every > 0 && n % every == 0) {
            self.dropped_chunks.fetch_add(1, Ordering::SeqCst);
            return;
        }
        let delay = self.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        let _ = out.write_all(chunk);
    }

    pub fn dropped(&self) -> u64 {
        self.dropped_chunks.load(Ordering::SeqCst)
    }
}

pub struct Proxy {
    pub address: SocketAddr,
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
    /// Client → server direction.
    pub up: Arc<Direction>,
    /// Server → client direction.
    pub down: Arc<Direction>,
}

impl Proxy {
    /// Listens on loopback, forwarding to `target`.
    pub fn bind(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let up = Arc::new(Direction::new());
        let down = Arc::new(Direction::new());
        let (up_w, down_w) = (up.clone(), down.clone());
        let join = std::thread::spawn(move || {
            let mut workers = vec![];
            while !flag.load(Ordering::SeqCst) {
                workers.retain(|w: &std::thread::JoinHandle<()>| !w.is_finished());
                match listener.accept() {
                    Ok((client, _)) => {
                        let server = match TcpStream::connect(target) {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        let (up_c, down_c) = (up_w.clone(), down_w.clone());
                        workers.push(std::thread::spawn(move || {
                            Self::pump(client, server, up_c, down_c);
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self { address, stop, join: Some(join), up, down }
    }

    fn pump(client: TcpStream, server: TcpStream, up: Arc<Direction>, down: Arc<Direction>) {
        let (mut c_r, mut c_w) = (client.try_clone().unwrap(), client);
        let (mut s_r, mut s_w) = (server.try_clone().unwrap(), server);
        c_r.set_nonblocking(true).ok();
        s_r.set_nonblocking(true).ok();
        let _ = c_w.set_write_timeout(Some(Duration::from_secs(5)));
        let _ = s_w.set_write_timeout(Some(Duration::from_secs(5)));
        let mut buf = [0u8; 8192];
        loop {
            let mut idle = true;
            match c_r.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    idle = false;
                    up.relay(&buf[..n], &mut s_w);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => break,
            }
            match s_r.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    idle = false;
                    down.relay(&buf[..n], &mut c_w);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => break,
            }
            if idle {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    /// Symmetric partition: both directions blackholed (states preserved,
    /// nothing flows). Reopen with `open()`.
    pub fn partition(&self) {
        self.up.closed.store(true, Ordering::SeqCst);
        self.down.closed.store(true, Ordering::SeqCst);
    }

    pub fn open(&self) {
        self.up.closed.store(false, Ordering::SeqCst);
        self.down.closed.store(false, Ordering::SeqCst);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}
