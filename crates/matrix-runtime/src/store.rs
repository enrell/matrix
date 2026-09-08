//! Durable control state. SQLite FULL commits, exclusive owner lock, bounded
//! audit retention and a non-replaying operation ledger. The legacy kernel
//! journal remains diagnostic; durable acknowledgements come from this store.
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::Mutex;

pub type Result<T> = std::result::Result<T, String>;
const MAX_OPS: i64 = 100_000;
const MAX_EVENTS: i64 = 10_000;
const MAX_BODY: usize = 1024 * 1024;

pub struct Store {
    db: Mutex<Connection>,
    _lock: File,
    pub epoch: u64,
}
#[derive(Debug, PartialEq)]
pub enum Admission {
    New,
    Completed(Value),
    Unknown,
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let dir = path.parent().ok_or("no store directory")?;
        std::fs::create_dir_all(dir).map_err(err)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).map_err(err)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(path.with_extension("lock"))
            .map_err(err)?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err("store already owned by another runtime".into());
        }
        let db = Connection::open(path).map_err(err)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(err)?;
        db.busy_timeout(std::time::Duration::from_secs(2))
            .map_err(err)?;
        let schema: i64 = db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(err)?;
        if schema > 1 {
            return Err("unsupported store schema; refusing downgrade".into());
        }
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            INSERT OR IGNORE INTO meta VALUES('epoch',0);
            CREATE TABLE IF NOT EXISTS desired(id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations(principal TEXT NOT NULL,id TEXT NOT NULL,request TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('admitted','completed','unknown')),result TEXT, PRIMARY KEY(principal,id));
            CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS fences(resource TEXT PRIMARY KEY,epoch INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS revoked(principal TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS effects(resource TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(resource,key));
            PRAGMA user_version=1;").map_err(err)?;
        let check: String = db
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(err)?;
        if check != "ok" {
            return Err(format!("store corrupt: {check}"));
        }
        let mut db = db;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(err)?;
        if tx
            .execute(
                "UPDATE meta SET value=value+1 WHERE key='epoch' AND value<9223372036854775807",
                [],
            )
            .map_err(err)?
            != 1
        {
            return Err("epoch exhausted".into());
        }
        let epoch: i64 = tx
            .query_row("SELECT value FROM meta WHERE key='epoch'", [], |r| r.get(0))
            .map_err(err)?;
        tx.execute(
            "UPDATE operations SET state='unknown' WHERE state='admitted'",
            [],
        )
        .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(Self {
            db: Mutex::new(db),
            _lock: lock,
            epoch: epoch as u64,
        })
    }
    pub fn set_revoked(&self, principal: &str, revoked: bool) -> Result<()> {
        let db = self.db.lock().unwrap();
        if revoked {
            db.execute("INSERT OR IGNORE INTO revoked VALUES(?1)", [principal])
                .map_err(err)?;
        } else {
            db.execute("DELETE FROM revoked WHERE principal=?1", [principal])
                .map_err(err)?;
        }
        Ok(())
    }
    pub fn revoked(&self) -> Result<std::collections::HashSet<String>> {
        let db = self.db.lock().unwrap();
        let mut q = db.prepare("SELECT principal FROM revoked").map_err(err)?;
        let rows = q.query_map([], |r| r.get::<_, String>(0)).map_err(err)?;
        rows.map(|r| r.map_err(err)).collect()
    }
    pub fn set_desired(&self, id: &str, body: &Value) -> Result<()> {
        let b = serde_json::to_string(body).map_err(err)?;
        if b.len() > MAX_BODY {
            return Err("resource-exhausted".into());
        }
        let db = self.db.lock().unwrap();
        db.execute(
            "INSERT INTO desired VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
            params![id, b],
        )
        .map_err(err)?;
        Ok(())
    }
    pub fn remove_desired(&self, id: &str) -> Result<()> {
        self.db
            .lock()
            .unwrap()
            .execute("DELETE FROM desired WHERE id=?1", [id])
            .map_err(err)?;
        Ok(())
    }
    pub fn desired(&self) -> Result<Vec<(String, Value)>> {
        let db = self.db.lock().unwrap();
        let mut s = db
            .prepare("SELECT id,body FROM desired ORDER BY id")
            .map_err(err)?;
        let rows = s
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .map_err(err)?;
        rows.map(|r| {
            let (id, b) = r.map_err(err)?;
            Ok((id, serde_json::from_str(&b).map_err(err)?))
        })
        .collect()
    }
    pub fn admit(&self, principal: &str, id: &str, request: &Value) -> Result<Admission> {
        if id.is_empty() || id.len() > 128 || principal.len() > 256 {
            return Err("invalid operation identity".into());
        }
        let request = serde_json::to_string(request).map_err(err)?;
        if request.len() > MAX_BODY {
            return Err("resource-exhausted".into());
        }
        let mut db = self.db.lock().unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(err)?;
        let old = tx
            .query_row(
                "SELECT request,state,result FROM operations WHERE principal=?1 AND id=?2",
                params![principal, id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(err)?;
        if let Some((old, state, result)) = old {
            if old != request {
                return Err("operation-id-conflict".into());
            }
            return if state == "completed" {
                Ok(Admission::Completed(
                    serde_json::from_str(&result.ok_or("missing result")?).map_err(err)?,
                ))
            } else {
                Ok(Admission::Unknown)
            };
        }
        let count: i64 = tx
            .query_row("SELECT count(*) FROM operations", [], |r| r.get(0))
            .map_err(err)?;
        if count >= MAX_OPS {
            return Err(
                "resource-exhausted: durable dedup ledger full; archive/rotate explicitly".into(),
            );
        }
        tx.execute(
            "INSERT INTO operations VALUES(?1,?2,?3,'admitted',NULL)",
            params![principal, id, request],
        )
        .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(Admission::New)
    }
    pub fn finish(&self, principal: &str, id: &str, result: &Value, unknown: bool) -> Result<()> {
        let body = serde_json::to_string(result).map_err(err)?;
        if body.len() > MAX_BODY {
            return Err("result too large; operation remains unknown".into());
        }
        let db = self.db.lock().unwrap();
        let n=db.execute("UPDATE operations SET state=?3,result=?4 WHERE principal=?1 AND id=?2 AND state='admitted'",params![principal,id,if unknown {"unknown"}else{"completed"},body]).map_err(err)?;
        if n != 1 {
            return Err("operation not admitted".into());
        }
        Ok(())
    }
    pub fn operation(&self, principal: &str, id: &str) -> Result<Value> {
        self.db.lock().unwrap().query_row("SELECT state,result FROM operations WHERE principal=?1 AND id=?2",params![principal,id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?))).optional().map_err(err)?
            .map(|(state,result)|Ok(json!({"state":state,"result":result.map(|s|serde_json::from_str::<Value>(&s)).transpose().map_err(err)?}))).unwrap_or_else(||Err("no-such-operation".into()))
    }
    pub fn event(&self, kind: &str, body: &Value) -> Result<u64> {
        let body = serde_json::to_string(body).map_err(err)?;
        if body.len() > MAX_BODY {
            return Err("event too large".into());
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction().map_err(err)?;
        tx.execute(
            "INSERT INTO events(kind,body) VALUES(?1,?2)",
            params![kind, body],
        )
        .map_err(err)?;
        let seq = tx.last_insert_rowid();
        tx.execute("DELETE FROM events WHERE seq<=?1", [seq - MAX_EVENTS])
            .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(seq as u64)
    }
    /// The fence check and effect mutation share a SQLite transaction at the
    /// destination. An old writer cannot slip between validation and commit.
    pub fn commit_effect(
        &self,
        principal: &str,
        id: &str,
        resource: &str,
        epoch: u64,
        key: &str,
        value: &Value,
    ) -> Result<Value> {
        let request = serde_json::to_string(&json!({"resource":resource,"key":key,"value":value}))
            .map_err(err)?;
        if id.is_empty() || id.len() > 128 || key.len() > 256 || request.len() > MAX_BODY {
            return Err("resource-exhausted".into());
        }
        let mut db = self.db.lock().unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(err)?;
        let current: Option<i64> = tx
            .query_row(
                "SELECT epoch FROM fences WHERE resource=?1",
                [resource],
                |r| r.get(0),
            )
            .optional()
            .map_err(err)?;
        if current != Some(i64::try_from(epoch).map_err(err)?) {
            return Err("stale-generation".into());
        }
        let old: Option<(String, String, Option<String>)> = tx
            .query_row(
                "SELECT request,state,result FROM operations WHERE principal=?1 AND id=?2",
                params![principal, id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(err)?;
        if let Some((r, state, result)) = old {
            if r != request {
                return Err("operation-id-conflict".into());
            }
            if state != "completed" {
                return Err("outcome-unknown".into());
            }
            return serde_json::from_str(&result.ok_or("missing result")?).map_err(err);
        }
        let count: i64 = tx
            .query_row("SELECT count(*) FROM operations", [], |r| r.get(0))
            .map_err(err)?;
        if count >= MAX_OPS {
            return Err("resource-exhausted".into());
        }
        let body = serde_json::to_string(value).map_err(err)?;
        tx.execute("INSERT INTO effects VALUES(?1,?2,?3) ON CONFLICT(resource,key) DO UPDATE SET value=excluded.value",params![resource,key,body]).map_err(err)?;
        let result = json!({"committed":true,"fence":epoch.to_string()});
        tx.execute(
            "INSERT INTO operations VALUES(?1,?2,?3,'completed',?4)",
            params![principal, id, request, result.to_string()],
        )
        .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(result)
    }
    pub fn effect(&self, resource: &str, key: &str) -> Result<Option<Value>> {
        let raw: Option<String> = self
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT value FROM effects WHERE resource=?1 AND key=?2",
                params![resource, key],
                |r| r.get(0),
            )
            .optional()
            .map_err(err)?;
        raw.map(|s| serde_json::from_str(&s).map_err(err))
            .transpose()
    }
    pub fn next_fence(&self, resource: &str) -> Result<u64> {
        let mut db = self.db.lock().unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(err)?;
        if tx.execute("INSERT INTO fences VALUES(?1,1) ON CONFLICT(resource) DO UPDATE SET epoch=epoch+1 WHERE epoch<9223372036854775807",[resource]).map_err(err)?!=1 {return Err("fence exhausted".into());}
        let epoch: i64 = tx
            .query_row(
                "SELECT epoch FROM fences WHERE resource=?1",
                [resource],
                |r| r.get(0),
            )
            .map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(epoch as u64)
    }
    pub fn fence(&self, resource: &str, epoch: u64) -> Result<()> {
        let epoch = i64::try_from(epoch).map_err(err)?;
        let db = self.db.lock().unwrap();
        let n=db.execute("INSERT INTO fences VALUES(?1,?2) ON CONFLICT(resource) DO UPDATE SET epoch=excluded.epoch WHERE fences.epoch<=excluded.epoch",params![resource,epoch]).map_err(err)?;
        if n == 0 {
            return Err("stale-generation".into());
        }
        Ok(())
    }
    /// Snapshot is a standalone SQLite database, including unknown operations,
    /// tombstones and fences. Existing paths never overwritten.
    pub fn snapshot(&self, path: &Path) -> Result<()> {
        snapshot_connection(&self.db.lock().unwrap(), path)
    }

    /// Opens a store image read-only for backup/validation (M8 P09).
    /// Unlike [`Store::open`], this never advances the epoch, never flips
    /// operation states, and never creates files: the origin is preserved
    /// bit-for-bit. Refuses when another runtime exclusively owns the
    /// store (stop the service first, or snapshot the live handle via
    /// [`Store::snapshot`]) and when the image fails integrity or version
    /// checks (corrupt/forged backups never enter the restore path).
    pub fn open_read_only(path: &Path) -> Result<OfflineBackup> {
        OfflineBackup::open(path)
    }
}

/// A store image opened read-only: backup source and restore validator.
/// Holds the shared lock for its whole lifetime, so no other runtime can
/// take exclusive ownership mid-backup (the pre-fix race): the guard only
/// drops when the copy/validation is done. Nothing is ever mutated.
pub struct OfflineBackup {
    db: Connection,
    epoch: u64,
    _guard: Option<File>,
}

impl OfflineBackup {
    /// Schema versions this binary restores (see `docs/VERSIONS.md`).
    pub const SUPPORTED_SCHEMA: i64 = 1;

    pub fn open(path: &Path) -> Result<Self> {
        if !path.is_file() {
            return Err("no store image at path".into());
        }
        // Exclusivity policy: a live runtime owns its store exclusively.
        // The shared guard lives in the returned handle: it is held for
        // the whole backup/validation, so a concurrent open can neither
        // start mid-copy nor mutate under it.
        let guard = if let Ok(g) = OpenOptions::new().read(true).open(path.with_extension("lock")) {
            if unsafe { libc::flock(g.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) } != 0 {
                return Err("store owned by a live runtime; stop the service or snapshot it online".into());
            }
            Some(g)
        } else {
            None
        };
        let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| format!("store corrupt or not a database: {e}"))?;
        let schema: i64 = db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(|e| format!("store corrupt or unreadable: {e}"))?;
        if schema != Self::SUPPORTED_SCHEMA {
            return Err(format!("unsupported store schema {schema}; refusing version change"));
        }
        let check: String = db
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(|e| format!("store corrupt or unreadable: {e}"))?;
        if check != "ok" {
            return Err(format!("store corrupt: {check}"));
        }
        let epoch: i64 = db
            .query_row("SELECT value FROM meta WHERE key='epoch'", [], |r| r.get(0))
            .map_err(|_| "store image without epoch".to_string())?;
        Ok(Self { db, epoch: epoch as u64, _guard: guard })
    }

    /// Epoch recorded in the image (boot counter at last recovery open).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Operation counts by state (`admitted`/`completed`/`unknown`).
    pub fn operation_states(&self) -> std::collections::HashMap<String, u64> {
        self.db
            .prepare("SELECT state, COUNT(*) FROM operations GROUP BY state")
            .and_then(|mut q| {
                q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
                    .map(|rows| rows.filter_map(|r| r.ok()).map(|(k, v)| (k, v as u64)).collect())
            })
            .unwrap_or_default()
    }

    /// Copies the image to a new standalone database (existing paths
    /// never overwritten; permissions 0600; fsynced).
    pub fn copy_to(&self, dest: &Path) -> Result<()> {
        snapshot_connection(&self.db, dest)
    }
}

fn snapshot_connection(db: &Connection, path: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map_err(err)?;
    drop(file);
    let mut dst = Connection::open(path).map_err(err)?;
    let backup = rusqlite::backup::Backup::new(db, &mut dst).map_err(err)?;
    backup
        .run_to_completion(128, std::time::Duration::from_millis(5), None)
        .map_err(err)?;
    drop(backup);
    drop(dst);
    File::open(path).map_err(err)?.sync_all().map_err(err)?;
    File::open(path.parent().ok_or("no parent")?)
        .map_err(err)?
        .sync_all()
        .map_err(err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn c16_storage_full_does_not_acknowledge_admission() {
        let p =
            std::env::temp_dir().join(format!("mxfull-{}", matrix_guard::random_token().unwrap()));
        std::fs::create_dir_all(&p).unwrap();
        let s = Store::open(&p.join("store.db")).unwrap();
        {
            let db = s.db.lock().unwrap();
            let pages: i64 = db.query_row("PRAGMA page_count", [], |r| r.get(0)).unwrap();
            db.pragma_update(None, "max_page_count", pages).unwrap();
        }
        assert!(s
            .admit("peer", "not-acked", &json!({"data":"x".repeat(900000)}))
            .is_err());
        assert!(s.operation("peer", "not-acked").is_err());
    }
}
