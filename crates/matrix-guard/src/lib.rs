//! Linux execution policy and bounded restart budgets. Policies come from the
//! operator, never from an untrusted component manifest.
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub fn random_token() -> io::Result<String> {
    let mut b = [0u8; 32];
    File::open("/dev/urandom")?.read_exact(&mut b)?;
    Ok(b.iter().map(|v| format!("{v:02x}")).collect())
}
pub fn token_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes().zip(b.bytes()).fold(0u8, |n, (x, y)| n | (x ^ y)) == 0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sandbox {
    /// Only this directory is writable outside the private ephemeral /tmp.
    pub workspace: PathBuf,
    /// Operator-selected read-only paths, kept at their original location.
    #[serde(default)]
    pub read_only: Vec<PathBuf>,
    pub memory_bytes: u64,
    pub cpu_seconds: u64,
    pub file_bytes: u64,
    pub open_files: u64,
}
impl Sandbox {
    pub fn validate(&self) -> io::Result<()> {
        if self.memory_bytes < 64 * 1024 * 1024
            || self.cpu_seconds == 0
            || self.file_bytes == 0
            || self.open_files < 16
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid finite sandbox limits",
            ));
        }
        if !self.workspace.is_absolute() || !self.workspace.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "workspace must be an existing absolute private directory",
            ));
        }
        for path in &self.read_only {
            if !path.is_absolute() || !path.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid read-only mount",
                ));
            }
        }
        Ok(())
    }
}

/// x86_64 profile: threads allowed, new processes denied. PID/mount/network
/// namespaces are additionally enforced by bwrap. Not a build executor profile.
#[cfg(target_arch = "x86_64")]
fn filter() -> io::Result<File> {
    let stmt = |code, k| libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    };
    let jump = |k, jt, jf| libc::sock_filter {
        code: 0x15,
        jt,
        jf,
        k,
    };
    let deny = stmt(0x06, 0x0005_0000 | libc::EPERM as u32);
    let mut p = vec![
        stmt(0x20, 4),
        jump(0xc000003e, 1, 0),
        stmt(0x06, 0x8000_0000),
        stmt(0x20, 0),
    ];
    // Reject x32 ABI bypass and namespace/process escape syscalls.
    p.extend([
        libc::sock_filter {
            code: 0x45,
            jt: 0,
            jf: 1,
            k: 0x40000000,
        },
        deny,
    ]);
    for nr in [
        libc::SYS_fork,
        libc::SYS_vfork,
        libc::SYS_unshare,
        libc::SYS_setns,
        libc::SYS_ptrace,
        libc::SYS_mount,
        libc::SYS_bpf,
        libc::SYS_keyctl,
    ] {
        p.extend([jump(nr as u32, 0, 1), deny]);
    }
    // libc falls back to clone for threads only when clone3 returns ENOSYS.
    p.extend([
        jump(libc::SYS_clone3 as u32, 0, 1),
        stmt(0x06, 0x0005_0000 | libc::ENOSYS as u32),
    ]);
    p.extend([
        jump(libc::SYS_clone as u32, 0, 3),
        stmt(0x20, 16),
        libc::sock_filter {
            code: 0x45,
            jt: 1,
            jf: 0,
            k: libc::CLONE_THREAD as u32,
        },
        deny,
        stmt(0x06, 0x7fff0000),
    ]);
    let fd = unsafe { libc::memfd_create(c"matrix-seccomp".as_ptr(), 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut f = unsafe { File::from_raw_fd(fd) };
    let bytes = unsafe {
        std::slice::from_raw_parts(p.as_ptr().cast::<u8>(), std::mem::size_of_val(p.as_slice()))
    };
    f.write_all(bytes)?;
    use std::io::{Seek, SeekFrom};
    f.seek(SeekFrom::Start(0))?;
    Ok(f)
}
#[cfg(not(target_arch = "x86_64"))]
fn filter() -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "sandbox profile currently requires Linux x86_64",
    ))
}

pub fn spawn(
    entrypoint: &str,
    args: &[String],
    token: &str,
    sock: &Path,
    sandbox: Option<&Sandbox>,
) -> io::Result<Child> {
    let mut filter_file = None;
    let mut command = if let Some(s) = sandbox {
        s.validate()?;
        let executable = std::fs::canonicalize(entrypoint)?;
        let mut c = Command::new("/usr/bin/bwrap");
        c.args([
            "--unshare-all",
            "--die-with-parent",
            "--new-session",
            "--cap-drop",
            "ALL",
            "--clearenv",
            "--ro-bind",
            "/usr",
            "/usr",
            "--symlink",
            "usr/bin",
            "/bin",
            "--symlink",
            "usr/lib",
            "/lib",
            "--symlink",
            "usr/lib",
            "/lib64",
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
        ]);
        c.arg("--bind").arg(&s.workspace).arg("/workspace");
        // Only the socket itself is exposed, not the host directory/other plugins.
        c.arg("--ro-bind").arg(sock).arg(sock);
        if !executable.starts_with("/usr") {
            c.arg("--ro-bind").arg(&executable).arg(&executable);
        }
        for path in &s.read_only {
            c.arg("--ro-bind").arg(path).arg(path);
        }
        c.args([
            "--chdir",
            "/workspace",
            "--setenv",
            "PATH",
            "/usr/bin",
            "--setenv",
            "HOME",
            "/workspace",
            "--setenv",
            "MATRIX_LAUNCH_TOKEN",
            token,
        ]);
        let f = filter()?;
        c.arg("--seccomp").arg(f.as_raw_fd().to_string());
        filter_file = Some(f);
        c.arg("--").arg(executable).args(args);
        c
    } else {
        let mut c = Command::new(entrypoint);
        c.args(args)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("MATRIX_LAUNCH_TOKEN", token);
        c
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let limits = sandbox.cloned();
    // Only async-signal-safe syscalls in pre_exec. No allocation or locks.
    unsafe {
        command.pre_exec(move || {
            if libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            if let Some(s) = &limits {
                for (resource, n) in [
                    (libc::RLIMIT_AS, s.memory_bytes),
                    (libc::RLIMIT_CPU, s.cpu_seconds),
                    (libc::RLIMIT_FSIZE, s.file_bytes),
                    (libc::RLIMIT_NOFILE, s.open_files),
                    (libc::RLIMIT_CORE, 0),
                ] {
                    let r = libc::rlimit {
                        rlim_cur: n,
                        rlim_max: n,
                    };
                    if libc::setrlimit(resource, &r) != 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
            }
            Ok(())
        });
    }
    let result = command.spawn();
    drop(filter_file);
    result
}

pub fn kill_group(child: &mut Child) -> io::Result<()> {
    let pgid = child.id() as i32;
    let rc = unsafe { libc::kill(-pgid, libc::SIGKILL) };
    if rc != 0 && io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
        return Err(io::Error::last_os_error());
    }
    let _ = child.kill();
    child.wait()?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartPolicy {
    pub max_restarts: usize,
    pub window_ms: u64,
    pub backoff_ms: u64,
}
impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            window_ms: 10000,
            backoff_ms: 100,
        }
    }
}
#[derive(Default)]
pub struct RestartBudget {
    attempts: VecDeque<Instant>,
    next: Option<Instant>,
}
impl RestartBudget {
    /// Called once for an observed failure; no sleeping while holding locks.
    pub fn reserve(&mut self, now: Instant, p: &RestartPolicy) -> Option<Instant> {
        if p.window_ms == 0 || p.backoff_ms == 0 || p.max_restarts > 100 {
            return None;
        }
        while self
            .attempts
            .front()
            .is_some_and(|t| now.duration_since(*t) >= Duration::from_millis(p.window_ms))
        {
            self.attempts.pop_front();
        }
        if self.attempts.len() >= p.max_restarts {
            return None;
        }
        let exp = 1u64
            .checked_shl(self.attempts.len().min(10) as u32)
            .unwrap_or(1024);
        let jitter = (self.attempts.len() as u64 * 37) % p.backoff_ms;
        let next = now
            + Duration::from_millis(
                p.backoff_ms
                    .saturating_mul(exp)
                    .saturating_add(jitter)
                    .min(60000),
            );
        self.attempts.push_back(now);
        self.next = Some(next);
        Some(next)
    }
}
