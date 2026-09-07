//! Version, limits, and id-dedup negotiation (M2.1).
//!
//! - `hello` advertises supported versions + `max_frame` + limits;
//!   `welcome` closes the highest common version + `session_id`; no overlap,
//!   `reject` with `unsupported-version`.
//! - Authentication precedes authorization: with no negotiated session,
//!   nothing but `hello` is accepted (enforced by the host; predicate here).
//! - Idempotent replay: `IdWindow` keeps seen ids under retention; same
//!   id + same content = safe replay; same id + divergent content
//!   = error (never re-executes effects).

use crate::dependency::{self, DEPENDENCY_CALLS_1};
use crate::envelope::{Envelope, PROTOCOL_VERSION};
use crate::error::{WireError, UNSUPPORTED_VERSION};
use std::collections::{HashMap, VecDeque};

/// Versions this side speaks, preferred first.
pub const SUPPORTED_VERSIONS: &[&str] = &[PROTOCOL_VERSION];

/// Protocol extensions with schema on this side. Announcing in welcome
/// is enabled by explicit host policy (M6.1 step 1).
pub const SUPPORTED_FEATURES: &[&str] = &[DEPENDENCY_CALLS_1];

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_frame: usize,
    pub max_calls: usize,
    pub max_queue_bytes: usize,
    pub max_streams: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame: crate::frame::DEFAULT_MAX_FRAME,
            max_calls: 64,
            max_queue_bytes: 4 * 1024 * 1024,
            max_streams: 16,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Welcome {
    pub version: String,
    pub session_id: String,
    pub max_frame: usize,
    pub limits: Limits,
    /// Negotiated extensions (intersection; legacy session = empty).
    pub features: Vec<String>,
}

/// Negotiates from an already-validated `hello`. `offered` = body.versions,
/// `offered_features` = body.features (absent in legacy sessions = empty).
pub fn negotiate(
    offered: &[String],
    offered_features: &[String],
    want_frame: usize,
    session_id: String,
) -> Result<Welcome, WireError> {
    let version = SUPPORTED_VERSIONS
        .iter()
        .find(|v| offered.iter().any(|o| o == **v))
        .ok_or_else(|| {
            WireError::new(UNSUPPORTED_VERSION, "handshake").with_details(serde_json::json!({
                "offered": offered,
                "supported": SUPPORTED_VERSIONS,
            }))
        })?;
    let max_frame = want_frame.min(crate::frame::DEFAULT_MAX_FRAME).max(1024);
    Ok(Welcome {
        version: version.to_string(),
        session_id,
        max_frame,
        limits: Limits { max_frame, ..Limits::default() },
        features: dependency::intersect(SUPPORTED_FEATURES, offered_features),
    })
}

/// Extracts `versions` + `max_frame` + `features` from a validated hello body.
/// Absent `features` = legacy session (empty vec); present requires a
/// string array (M6.1 step 1).
pub fn hello_offer(env: &Envelope) -> Result<(Vec<String>, usize, Vec<String>), WireError> {
    if env.ty != "hello" {
        return Err(WireError::new(crate::error::INVALID_MESSAGE, "handshake")
            .with_details(serde_json::json!({"reason": "not a hello"})));
    }
    let versions = env
        .body
        .get("versions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            WireError::new(crate::error::INVALID_MESSAGE, "handshake")
                .with_details(serde_json::json!({"reason": "versions must be array"}))
        })?
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    if versions.is_empty() {
        return Err(WireError::new(crate::error::INVALID_MESSAGE, "handshake")
            .with_details(serde_json::json!({"reason": "empty versions"})));
    }
    let max_frame = env.body.get("max_frame").and_then(|v| v.as_u64()).unwrap_or(1024) as usize;
    let features = dependency::hello_features(&env.body)?;
    Ok((versions, max_frame, features))
}

/// Seen-id window (retention-bounded dedup).
pub struct IdWindow {
    cap: usize,
    seen: HashMap<String, u64>,
    order: VecDeque<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdVerdict {
    /// First time: process and store the content hash.
    New,
    /// Identical replay inside retention: answer the stored terminal,
    /// without re-executing.
    DuplicateSame,
    /// Same id, divergent content: error, without executing.
    DuplicateDivergent,
}

impl IdWindow {
    pub fn new(cap: usize) -> Self {
        Self { cap: cap.max(1), seen: HashMap::new(), order: VecDeque::new() }
    }

    fn hash_content(canonical: &str) -> u64 {
        // FNV-1a 64: enough for divergence detection under retention.
        let mut h: u64 = 0xcbf29ce484222325;
        for b in canonical.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }

    pub fn check(&mut self, id: &str, content_canonical: &str) -> IdVerdict {
        let h = Self::hash_content(content_canonical);
        match self.seen.get(id) {
            None => {
                if self.order.len() >= self.cap {
                    if let Some(old) = self.order.pop_front() {
                        self.seen.remove(&old);
                    }
                }
                self.seen.insert(id.to_string(), h);
                self.order.push_back(id.to_string());
                IdVerdict::New
            }
            Some(prev) if *prev == h => IdVerdict::DuplicateSame,
            Some(_) => IdVerdict::DuplicateDivergent,
        }
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }
}

/// Shrinks remaining `timeout_ms` by elapsed (never negative).
pub fn shrink_timeout_ms(remaining_ms: u64, elapsed_ms: u64) -> u64 {
    remaining_ms.saturating_sub(elapsed_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiates_common_or_rejects() {
        let w = negotiate(&["0.1".to_string()], &[], 1 << 20, "s".into()).unwrap();
        assert_eq!(w.version, "0.1");
        assert!(w.features.is_empty(), "no offer = no extension");
        assert!(negotiate(&["9.9".to_string()], &[], 1 << 20, "s".into()).is_err());
        let w = negotiate(&["9.9".to_string(), "0.1".to_string()], &[], 512, "s".into()).unwrap();
        assert_eq!(w.version, "0.1");
        assert!(w.max_frame >= 1024, "piso de sanidade");
    }

    #[test]
    fn negotiates_features_by_intersection() {
        let w = negotiate(&["0.1".to_string()], &["dependency-calls/1".to_string()], 1 << 20, "s".into()).unwrap();
        assert_eq!(w.features, vec!["dependency-calls/1".to_string()]);
        let w = negotiate(&["0.1".to_string()], &["other/9".to_string()], 1 << 20, "s".into()).unwrap();
        assert!(w.features.is_empty(), "unknown is not negotiated");
    }

    #[test]
    fn id_window_semantics() {
        let mut win = IdWindow::new(2);
        assert_eq!(win.check("a", "{}"), IdVerdict::New);
        assert_eq!(win.check("a", "{}"), IdVerdict::DuplicateSame);
        assert_eq!(win.check("a", "{\"x\":1}"), IdVerdict::DuplicateDivergent);
        win.check("b", "{}");
        win.check("c", "{}"); // despeja "a"
        assert_eq!(win.check("a", "{}"), IdVerdict::New);
    }

    #[test]
    fn timeout_shrinks() {
        assert_eq!(shrink_timeout_ms(5000, 1200), 3800);
        assert_eq!(shrink_timeout_ms(100, 5000), 0);
    }

    #[test]
    fn deadline_write_ok_free_contended_poisoned() {
        use crate::frame::{encode, read_frame, write_frame_deadline, FrameError, DEFAULT_MAX_FRAME};
        use std::os::unix::net::UnixStream;
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};
        // Free mutex: delivers within budget.
        let (a, mut b) = UnixStream::pair().unwrap();
        let w = Mutex::new(a);
        let frame = encode(b"hi", DEFAULT_MAX_FRAME).unwrap();
        write_frame_deadline(&w, &frame, Duration::from_secs(5)).unwrap();
        b.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let got = read_frame(&mut b, DEFAULT_MAX_FRAME).unwrap().unwrap();
        assert_eq!(got, b"hi");
        // Contended mutex: fails fast with Timeout, well inside a long hang.
        let w = Arc::new(Mutex::new(w.into_inner().unwrap()));
        let w2 = w.clone();
        let holder = std::thread::spawn(move || {
            let _g = w2.lock().unwrap();
            std::thread::sleep(Duration::from_millis(400));
        });
        std::thread::sleep(Duration::from_millis(50));
        let t0 = Instant::now();
        let err = write_frame_deadline(&w, &frame, Duration::from_millis(80)).unwrap_err();
        assert_eq!(err, FrameError::Timeout);
        assert!(t0.elapsed() < Duration::from_secs(5), "bounded wait");
        holder.join().unwrap();
        // Free again after the holder leaves: works.
        write_frame_deadline(&w, &frame, Duration::from_secs(5)).unwrap();
        // Poisoned mutex: fails instead of writing into a torn stream.
        let w3 = Arc::new(Mutex::new(UnixStream::pair().unwrap().0));
        let w4 = w3.clone();
        let _ = std::thread::spawn(move || {
            let _g = w4.lock().unwrap();
            panic!("intentional poison");
        })
        .join();
        assert!(write_frame_deadline(&w3, &frame, Duration::from_secs(1)).is_err());
    }
}
