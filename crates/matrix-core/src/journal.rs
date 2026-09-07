//! Write-ahead journal: buffered append, `seq`-ordered, replay = fold.
//! Full durability (batch-fsync) is roadmap; buffered already gives S9 fidelity 1.0
//! on the box (cf. master3 C5: buffered wins by 2.1x for the same fidelity).

use parking_lot::Mutex;
use serde_json::{json, Value};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Entry {
    pub seq: u64,
    pub fiber: String,
    pub kind: String,
    pub args: Value,
    pub ts: String,
    pub undo: Value,
    pub v: u64,
}

#[derive(Debug)]
struct Inner {
    path: PathBuf,
    file: File,
    seq: u64,
    core_v2: bool,
}

#[derive(Debug, Clone)]
pub struct Journal {
    inner: Arc<Mutex<Inner>>,
}

fn now_ts() -> String {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    format!("{}.{:03}", d.as_secs(), d.subsec_millis())
}

impl Journal {
    pub fn open(path: &Path, core_v2: bool, _batch_fsync: bool) -> std::io::Result<Self> {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let last = Self::read_all(path).last().map(|e| e.seq).unwrap_or(0);
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                path: path.to_path_buf(),
                file,
                seq: last,
                core_v2,
            })),
        })
    }

    pub fn adopt_seq(&self, seq: u64) {
        let mut i = self.inner.lock();
        if seq > i.seq {
            i.seq = seq;
        }
    }

    pub fn append(&self, fiber: &str, kind: &str, args: Value, undo: Value) -> u64 {
        let mut i = self.inner.lock();
        i.seq += 1;
        let e = json!({
            "seq": i.seq,
            "fiber": fiber,
            "kind": kind,
            "args": args,
            "ts": now_ts(),
            "undo": undo,
            "v": if i.core_v2 { 2 } else { 1 },
        });
        let line = serde_json::to_string(&e).unwrap_or_default() + "\n";
        let _ = i.file.write_all(line.as_bytes());
        // buffered default (C5 winner); flush on reset/quit/test.
        i.seq
    }

    pub fn flush_sync(&self) {
        let mut i = self.inner.lock();
        let _ = i.file.flush();
        let _ = i.file.sync_all();
    }

    pub fn reset(&self) -> std::io::Result<()> {
        let path;
        let core_v2;
        {
            let i = self.inner.lock();
            path = i.path.clone();
            core_v2 = i.core_v2;
        }
        // Truncates the file and zeroes the sequence (bench isolation).
        std::fs::write(&path, "")?;
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        *self.inner.lock() = Inner { path, file, seq: 0, core_v2 };
        Ok(())
    }

    pub fn read_all(path: &Path) -> Vec<Entry> {
        let Ok(f) = File::open(path) else { return vec![] };
        let mut out = vec![];
        for line in BufReader::new(f).lines().map_while(Result::ok) {
            let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
            out.push(Entry {
                seq: v.get("seq").and_then(|x| x.as_u64()).unwrap_or(0),
                fiber: v.get("fiber").and_then(|x| x.as_str()).unwrap_or("").into(),
                kind: v.get("kind").and_then(|x| x.as_str()).unwrap_or("").into(),
                args: v.get("args").cloned().unwrap_or(Value::Null),
                ts: v.get("ts").and_then(|x| x.as_str()).unwrap_or("").into(),
                undo: v.get("undo").cloned().unwrap_or(Value::Null),
                v: v.get("v").and_then(|x| x.as_u64()).unwrap_or(1),
            });
        }
        out.sort_by_key(|e| e.seq);
        out
    }

    pub fn tail(&self, n: usize) -> Vec<Entry> {
        let path = self.inner.lock().path.clone();
        let all = Self::read_all(&path);
        let s = all.len().saturating_sub(n);
        all[s..].to_vec()
    }
}
