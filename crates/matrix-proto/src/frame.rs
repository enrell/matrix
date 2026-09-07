//! Initial-profile framing (M2.1, cf. `docs/PROTOCOL.md` §5).
//!
//! UTF-8 JSON messages with u32 big-endian length prefix.
//! Default frame limit: 1 MiB; smaller limits may be negotiated.
//! Size is validated BEFORE allocating the payload (C12).

use std::io::{Read, Write};

/// Default frame limit: 1 MiB.
pub const DEFAULT_MAX_FRAME: usize = 1024 * 1024;
/// Length prefix: u32 big-endian.
pub const LEN_PREFIX: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Payload declares a size above the limit (without allocating).
    TooLarge { declared: u64, max: usize },
    /// Stream ended mid-frame.
    Truncated { want: usize, got: usize },
    /// Underlying I/O.
    Io(String),
    /// Total send budget exhausted (mutex wait + write).
    Timeout,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::TooLarge { declared, max } => {
                write!(f, "frame declares {} bytes, max is {}", declared, max)
            }
            FrameError::Truncated { want, got } => {
                write!(f, "truncated frame: want {} bytes, got {}", want, got)
            }
            FrameError::Io(e) => write!(f, "io: {}", e),
            FrameError::Timeout => write!(f, "send budget exhausted"),
        }
    }
}

impl std::error::Error for FrameError {}

/// Encodes a payload into a full frame (prefix + bytes).
pub fn encode(payload: &[u8], max_frame: usize) -> Result<Vec<u8>, FrameError> {
    if payload.len() > max_frame {
        return Err(FrameError::TooLarge { declared: payload.len() as u64, max: max_frame });
    }
    if payload.len() > u32::MAX as usize {
        return Err(FrameError::TooLarge { declared: payload.len() as u64, max: max_frame });
    }
    let mut out = Vec::with_capacity(LEN_PREFIX + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

/// Writes one frame through a shared writer within an absolute total budget
/// (M6 closing): bounded mutex wait plus non-blocking sends with readiness
/// waits capped by the remaining budget. Neither a socket timeout nor
/// chunked blocking writes is a frame deadline: Linux rearms SNDTIMEO on
/// partial progress (one 4 MB `write` took 49 s with a 500 ms SNDTIMEO),
/// and any single blocking `write` has no time bound at all. This loop
/// therefore never issues a blocking send: each `send(MSG_DONTWAIT)`
/// returns immediately, and each `poll(POLLOUT)` waits at most the time
/// left until the absolute deadline. Total time never exceeds ~budget plus
/// scheduling jitter, no matter how lock or socket behave. A completion
/// that lands after the deadline counts as `Timeout`, not `Ok`.
/// A failed frame poisons the connection (`shutdown(Both)`): appending
/// another frame after a partial one would corrupt framing, and a poisoned
/// mutex already covers panics but not timeouts or plain I/O errors.
/// Callers treat any failure as a dead peer (existing paths already do).
/// The socket's blocking mode is never toggled (the fd is shared with the
/// read loop via `try_clone`): `MSG_DONTWAIT` keeps each send non-blocking
/// without touching file-status flags, so `SNDTIMEO` settings elsewhere
/// are simply ignored on this path.
pub fn write_frame_deadline(
    writer: &std::sync::Mutex<std::os::unix::net::UnixStream>,
    frame: &[u8],
    budget: std::time::Duration,
) -> Result<(), FrameError> {
    use std::net::Shutdown;
    use std::sync::TryLockError;
    use std::time::{Duration, Instant};
    let deadline = Instant::now() + budget;
    let guard = loop {
        match writer.try_lock() {
            Ok(g) => break g,
            Err(TryLockError::WouldBlock) => {
                if Instant::now() >= deadline {
                    return Err(FrameError::Timeout);
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(FrameError::Io("writer poisoned".to_string()));
            }
        }
    };
    // Fail closed: once any byte went out (or any I/O error hit), no
    // later frame may be appended on this socket.
    let poison = |guard: &std::sync::MutexGuard<std::os::unix::net::UnixStream>| {
        let _ = guard.shutdown(Shutdown::Both);
    };
    if frame.is_empty() {
        return Ok(());
    }
    // Raw non-blocking send + poll, without touching O_NONBLOCK (shared
    // with the read loop) or relying on SNDTIMEO (rearmed on progress).
    use std::os::unix::io::AsRawFd;
    #[repr(C)]
    struct PollFd {
        fd: i32,
        events: i16,
        revents: i16,
    }
    const POLLOUT: i16 = 0x0004;
    const POLLERR: i16 = 0x0008;
    const POLLHUP: i16 = 0x0010;
    const POLLNVAL: i16 = 0x0020;
    const MSG_DONTWAIT: std::os::raw::c_int = 0x40;
    const EAGAIN: i32 = 11;
    const EINTR: i32 = 4;
    extern "C" {
        fn poll(fds: *mut PollFd, nfds: usize, timeout: std::os::raw::c_int) -> std::os::raw::c_int;
        fn send(
            sockfd: std::os::raw::c_int,
            buf: *const std::os::raw::c_void,
            len: usize,
            flags: std::os::raw::c_int,
        ) -> isize;
    }
    let fd = guard.as_raw_fd();
    let mut written = 0usize;
    while written < frame.len() {
        if Instant::now() >= deadline {
            if written > 0 {
                poison(&guard);
            }
            return Err(FrameError::Timeout);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout_ms = remaining.as_millis().min(i32::MAX as u128) as std::os::raw::c_int;
        // Wait for buffer space, at most the time left.
        let mut pfd = PollFd { fd, events: POLLOUT, revents: 0 };
        // SAFETY: `pfd` is a valid single-element pollfd array; poll only
        // writes `revents`. `timeout_ms` >= 0 (derived from `remaining`).
        let ready = unsafe { poll(&mut pfd as *mut PollFd, 1, timeout_ms) };
        if ready == 0 {
            // Readiness wait itself hit the absolute deadline.
            if written > 0 {
                poison(&guard);
            }
            return Err(FrameError::Timeout);
        }
        if ready < 0 {
            let e = std::io::Error::last_os_error();
            if e.raw_os_error() == Some(EINTR) {
                continue;
            }
            if written > 0 {
                poison(&guard);
            }
            return Err(FrameError::Io(e.to_string()));
        }
        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 && (pfd.revents & POLLOUT) == 0 {
            poison(&guard);
            return Err(FrameError::Io("peer closed mid-frame".to_string()));
        }
        // Non-blocking send: returns immediately, never outlives the budget.
        // SAFETY: `ptr`/`len` borrow the live `frame` slice; fd is the
        // locked writer. MSG_DONTWAIT keeps this call non-blocking without
        // changing the socket's (shared) blocking mode.
        let n = unsafe {
            send(
                fd,
                frame[written..].as_ptr() as *const std::os::raw::c_void,
                frame.len() - written,
                MSG_DONTWAIT,
            )
        };
        if n > 0 {
            written += n as usize;
            continue;
        }
        if n == 0 {
            poison(&guard);
            return Err(FrameError::Io("peer closed mid-frame".to_string()));
        }
        let e = std::io::Error::last_os_error();
        match e.raw_os_error() {
            // Lost the race between poll and send: re-poll with fresh
            // remaining budget instead of failing or spinning.
            Some(code) if code == EAGAIN || code == EINTR => continue,
            _ => {
                poison(&guard);
                return Err(FrameError::Io(e.to_string()));
            }
        }
    }
    // Late completion is still a miss: bytes went out, so poison — the
    // caller must not treat this frame as delivered on time, and no later
    // frame may be appended to it.
    if Instant::now() >= deadline {
        poison(&guard);
        return Err(FrameError::Timeout);
    }
    Ok(())
}

/// Reads the 4-byte prefix. `Ok(None)` = clean EOF before any byte.
pub fn read_len<R: Read>(r: &mut R) -> Result<Option<u32>, FrameError> {
    let mut hdr = [0u8; LEN_PREFIX];
    let mut got = 0;
    while got < LEN_PREFIX {
        match r.read(&mut hdr[got..]) {
            Ok(0) => {
                if got == 0 {
                    return Ok(None);
                }
                return Err(FrameError::Truncated { want: LEN_PREFIX, got });
            }
            Ok(n) => got += n,
            Err(e) => return Err(FrameError::Io(e.to_string())),
        }
    }
    Ok(Some(u32::from_be_bytes(hdr)))
}

/// Reads one frame after validating the declared size (without over-allocating).
pub fn read_frame<R: Read>(r: &mut R, max_frame: usize) -> Result<Option<Vec<u8>>, FrameError> {
    let Some(len) = read_len(r)? else { return Ok(None) };
    if len as u64 > max_frame as u64 {
        return Err(FrameError::TooLarge { declared: len as u64, max: max_frame });
    }
    let mut buf = vec![0u8; len as usize];
    let mut got = 0;
    while got < buf.len() {
        match r.read(&mut buf[got..]) {
            Ok(0) => return Err(FrameError::Truncated { want: buf.len(), got }),
            Ok(n) => got += n,
            Err(e) => return Err(FrameError::Io(e.to_string())),
        }
    }
    Ok(Some(buf))
}

/// Writes a full frame.
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> Result<(), FrameError> {
    w.write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(|e| FrameError::Io(e.to_string()))?;
    w.write_all(payload).map_err(|e| FrameError::Io(e.to_string()))?;
    w.flush().map_err(|e| FrameError::Io(e.to_string()))?;
    Ok(())
}

/// Extracts the next frame from an accumulated buffer. Returns the frame and the
/// remainder, or `None` if incomplete. Validates size before slicing.
pub fn split_frame(buf: &[u8], max_frame: usize) -> Result<Option<(&[u8], &[u8])>, FrameError> {
    if buf.len() < LEN_PREFIX {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > max_frame {
        return Err(FrameError::TooLarge { declared: len as u64, max: max_frame });
    }
    if buf.len() < LEN_PREFIX + len {
        return Ok(None);
    }
    Ok(Some((&buf[LEN_PREFIX..LEN_PREFIX + len], &buf[LEN_PREFIX + len..])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let f = encode(b"{}", 1024).unwrap();
        let (body, rest) = split_frame(&f, 1024).unwrap().unwrap();
        assert_eq!(body, b"{}");
        assert!(rest.is_empty());
    }

    #[test]
    fn deadline_holds_across_partial_writes() {
        use std::os::unix::net::UnixStream;
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};
        // Slow but progressing reader: 4 KB per 50 ms against a 4 MB frame.
        // Single-shot socket timeouts would renew per syscall (~47 s total);
        // each non-blocking send returns immediately and each poll waits at
        // most the time left, so the absolute deadline holds.
        let budget = Duration::from_millis(500);
        // Explicit tolerance, same order as the budget (scheduling jitter
        // only — not a multiple of it): the call must land within
        // budget + TOL, proving the deadline rather than an improvement.
        let tol = Duration::from_millis(400);
        let (a, mut b) = UnixStream::pair().unwrap();
        let writer = Arc::new(Mutex::new(a));
        let frame = vec![0xABu8; 4 * 1024 * 1024];
        let reader = std::thread::spawn(move || {
            b.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
            let mut chunk = [0u8; 4096];
            use std::io::Read;
            loop {
                match b.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(_) => std::thread::sleep(Duration::from_millis(50)),
                    Err(_) => break,
                }
            }
        });
        let t0 = Instant::now();
        let res = write_frame_deadline(&writer, &frame, budget);
        let dt = t0.elapsed();
        assert_eq!(res, Err(FrameError::Timeout), "total budget enforced: {:?}", res);
        assert!(
            dt <= budget + tol,
            "absolute deadline: budget {:?} + tol {:?}, took {:?}",
            budget,
            tol,
            dt
        );
        drop(writer);
        reader.join().unwrap();
    }

    #[test]
    fn zero_budget_fails_closed_without_hanging() {
        use std::os::unix::net::UnixStream;
        use std::sync::Mutex;
        use std::time::{Duration, Instant};
        // Boundary semantics: a non-empty frame with no budget is a miss
        // (nothing could have been delivered on time); an empty frame is
        // trivially complete. Neither may block.
        let (a, _b) = UnixStream::pair().unwrap();
        let writer = Mutex::new(a);
        let t0 = Instant::now();
        let res = write_frame_deadline(&writer, &[0xAAu8; 64], Duration::ZERO);
        assert_eq!(res, Err(FrameError::Timeout), "zero budget is a miss: {:?}", res);
        assert!(t0.elapsed() < Duration::from_secs(2), "never blocks: {:?}", t0.elapsed());
        assert!(write_frame_deadline(&writer, &[], Duration::ZERO).is_ok());
    }

    #[test]
    fn failed_frame_poisons_connection() {
        use std::os::unix::net::UnixStream;
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};
        // Peer never reads: a 2 MB frame with a 300 ms budget must fail,
        // and the socket must be unusable immediately after (no appended
        // frame can corrupt framing) while the peer observes EOF.
        let (a, mut b) = UnixStream::pair().unwrap();
        let writer = Arc::new(Mutex::new(a));
        let frame = vec![0xCDu8; 2 * 1024 * 1024];
        let res = write_frame_deadline(&writer, &frame, Duration::from_millis(300));
        assert!(res.is_err(), "expected failure, got {:?}", res);
        // Second send fails fast instead of hanging on a full buffer:
        // the connection was poisoned, not left half-open.
        let t0 = Instant::now();
        let res2 = write_frame_deadline(&writer, &[0u8; 1024], Duration::from_secs(5));
        assert!(res2.is_err(), "poisoned socket refuses: {:?}", res2);
        assert!(t0.elapsed() < Duration::from_secs(2), "fast refusal: {:?}", t0.elapsed());
        // Peer drains whatever landed, then sees EOF (shutdown), not a hang.
        b.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        use std::io::Read;
        let mut eof = false;
        let mut chunk = [0u8; 65536];
        let t0 = Instant::now();
        while t0.elapsed() < Duration::from_secs(5) {
            match b.read(&mut chunk) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        assert!(eof, "peer observes EOF after poison");
    }

    #[test]
    fn oversize_rejected_before_alloc() {
        assert!(matches!(
            encode(&vec![0u8; 10], 9),
            Err(FrameError::TooLarge { .. })
        ));
        let mut hdr = (100u32).to_be_bytes().to_vec();
        hdr.extend_from_slice(b"short");
        assert!(matches!(split_frame(&hdr, 50), Err(FrameError::TooLarge { .. })));
    }

    #[test]
    fn incomplete_is_none_not_error() {
        assert!(split_frame(b"\x00\x00", 1024).unwrap().is_none());
        let mut f = encode(b"hello", 1024).unwrap();
        f.truncate(6);
        assert!(split_frame(&f, 1024).unwrap().is_none());
    }
}
