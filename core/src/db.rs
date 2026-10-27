//! The database: redb, an embedded key-value store in pure Rust (one file,
//! <data>/xb.redb, ACID transactions, crash-safe).
//!
//! It plays the part of Mnesia's disc copies. The live data stays in memory
//! (store.rs) and is read from there; this file only makes it durable. Every
//! change marks the record it touched, and the writer task (main.rs) saves
//! all records marked since the last save in ONE transaction. Under load, many
//! changes share one disk sync ("group commit").
//!
//! Each record is a small JSON value (the same shapes as the state.json snapshot),
//! so the file stays easy to inspect, export and migrate.

use crate::server::Hub;
use crate::store::{DeviceRec, Dirty, Group, PrekeyRec, Queued, Store, UserRec};
use crate::util::js_key_order;
use crate::util::log;
use indexmap::IndexMap;
use redb::{Database, ReadableTable, TableDefinition, TableError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const USERS: TableDefinition<&str, &[u8]> = TableDefinition::new("users");
const LAST_SEEN: TableDefinition<&str, u64> = TableDefinition::new("last_seen");
/// user -> all their devices (JSON object, in order)
const DEVICES: TableDefinition<&str, &[u8]> = TableDefinition::new("devices");
/// (user, end-to-end device) -> public keys
const PREKEYS: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("prekeys");
const GROUPS: TableDefinition<&str, &[u8]> = TableDefinition::new("groups");
/// Before version 2 the offline queue was here; it's in queue_db.rs now.
/// Read once to move it (take_legacy_queue).
/// (owner, dedup key) -> (arrival order, queued at ms, encoded stanza)
const QUEUE: TableDefinition<(&str, &str), (u64, u64, &[u8])> = TableDefinition::new("queue");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

const VERSION: u64 = 1;

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("database: {e}"))
}

/// One record to write (Some) or delete (None).
pub enum Op {
    User(String, Option<Vec<u8>>),
    LastSeen(String, Option<u64>),
    Devices(String, Option<Vec<u8>>),
    Prekey(String, String, Option<Vec<u8>>),
    Group(String, Option<Vec<u8>>),
    Meta(u64),
}

pub struct Db {
    db: Database,
    pub path: PathBuf,
}

impl Db {
    /// Opens (or creates) the file. Fails if another process has it open.
    pub fn open(path: &Path) -> io::Result<Db> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let db = Database::create(path).map_err(err)?;
        Ok(Db { db, path: path.to_path_buf() })
    }

    /// Nothing saved yet (a new install, or one coming from state.json).
    pub fn is_new(&self) -> io::Result<bool> {
        let tx = self.db.begin_read().map_err(err)?;
        match tx.open_table(META) {
            Ok(t) => Ok(t.get("version").map_err(err)?.is_none()),
            Err(TableError::TableDoesNotExist(_)) => Ok(true),
            Err(e) => Err(err(e)),
        }
    }

    /// Fills the in-memory store from the file.
    pub fn load(&self, store: &mut Store) -> io::Result<()> {
        let tx = self.db.begin_read().map_err(err)?;
        let json = |what: &str, k: &str, v: &[u8]| -> io::Result<serde_json::Value> { serde_json::from_slice(v).map_err(|e| err(format!("{what} {k}: {e}"))) };

        for row in tx.open_table(USERS).map_err(err)?.iter().map_err(err)? {
            let (k, v) = row.map_err(err)?;
            let rec: UserRec = serde_json::from_value(json("user", k.value(), v.value())?).map_err(err)?;
            store.users.insert(k.value().to_string(), rec);
        }
        for row in tx.open_table(LAST_SEEN).map_err(err)?.iter().map_err(err)? {
            let (k, v) = row.map_err(err)?;
            store.last_seen.insert(k.value().to_string(), v.value());
        }
        for row in tx.open_table(DEVICES).map_err(err)?.iter().map_err(err)? {
            let (k, v) = row.map_err(err)?;
            let devs: IndexMap<String, DeviceRec> = serde_json::from_slice(v.value()).map_err(|e| err(format!("devices {}: {e}", k.value())))?;
            store.devices.insert(k.value().to_string(), devs);
        }
        for row in tx.open_table(PREKEYS).map_err(err)?.iter().map_err(err)? {
            let (k, v) = row.map_err(err)?;
            let (user, dev) = k.value();
            let rec: PrekeyRec = serde_json::from_slice(v.value()).map_err(|e| err(format!("prekeys {user}/{dev}: {e}")))?;
            store.prekeys.entry(user.to_string()).or_default().insert(dev.to_string(), rec);
        }
        // Device ids in numeric order (the order JSON object keys load in).
        for devs in store.prekeys.values_mut() {
            let order: Vec<String> = js_key_order(devs.keys()).into_iter().cloned().collect();
            *devs = order
                .into_iter()
                .map(|k| {
                    let v = devs.shift_remove(&k).unwrap();
                    (k, v)
                })
                .collect();
        }
        for row in tx.open_table(GROUPS).map_err(err)?.iter().map_err(err)? {
            let (k, v) = row.map_err(err)?;
            let g: Group = serde_json::from_slice(v.value()).map_err(|e| err(format!("group {}: {e}", k.value())))?;
            store.groups.insert(k.value().to_string(), g);
        }
        let meta = tx.open_table(META).map_err(err)?;
        let group_seq = meta.get("group_seq").map_err(err)?.map(|v| v.value()).unwrap_or(0);
        store.counters.group_seq.store(group_seq, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Writes the records in one transaction (durable when this returns).
    pub fn write(&self, ops: Vec<Op>) -> io::Result<()> {
        let tx = self.db.begin_write().map_err(err)?;
        {
            let mut users = tx.open_table(USERS).map_err(err)?;
            let mut last_seen = tx.open_table(LAST_SEEN).map_err(err)?;
            let mut devices = tx.open_table(DEVICES).map_err(err)?;
            let mut prekeys = tx.open_table(PREKEYS).map_err(err)?;
            let mut groups = tx.open_table(GROUPS).map_err(err)?;
            let mut meta = tx.open_table(META).map_err(err)?;
            for op in &ops {
                match op {
                    Op::User(k, Some(v)) => drop(users.insert(k.as_str(), v.as_slice()).map_err(err)?),
                    Op::User(k, None) => drop(users.remove(k.as_str()).map_err(err)?),
                    Op::LastSeen(k, Some(v)) => drop(last_seen.insert(k.as_str(), *v).map_err(err)?),
                    Op::LastSeen(k, None) => drop(last_seen.remove(k.as_str()).map_err(err)?),
                    Op::Devices(k, Some(v)) => drop(devices.insert(k.as_str(), v.as_slice()).map_err(err)?),
                    Op::Devices(k, None) => drop(devices.remove(k.as_str()).map_err(err)?),
                    Op::Prekey(u, d, Some(v)) => drop(prekeys.insert((u.as_str(), d.as_str()), v.as_slice()).map_err(err)?),
                    Op::Prekey(u, d, None) => drop(prekeys.remove((u.as_str(), d.as_str())).map_err(err)?),
                    Op::Group(k, Some(v)) => drop(groups.insert(k.as_str(), v.as_slice()).map_err(err)?),
                    Op::Group(k, None) => drop(groups.remove(k.as_str()).map_err(err)?),
                    Op::Meta(group_seq) => drop(meta.insert("group_seq", *group_seq).map_err(err)?),
                }
            }
            // Marks the file as in use (is_new), whatever the first save held.
            meta.insert("version", VERSION).map_err(err)?;
        }
        tx.commit().map_err(err)
    }

    /// The offline queue as an older version kept it here: to move it to the
    /// queue store. -> (owner, key, arrival order, queued at, stanza), in order
    pub fn take_legacy_queue(&self) -> io::Result<Vec<crate::queue_db::Write>> {
        let tx = self.db.begin_read().map_err(err)?;
        let table = match tx.open_table(QUEUE) {
            Ok(t) => t,
            Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(err(e)),
        };
        let mut items = Vec::new();
        for row in table.iter().map_err(err)? {
            let (k, v) = row.map_err(err)?;
            let (owner, key) = k.value();
            let (seq, at, frame) = v.value();
            items.push((owner.to_string(), key.to_string(), seq, at, frame.to_vec()));
        }
        items.sort_by_key(|i| i.2);
        Ok(items)
    }

    /// Moved: drop the old table.
    pub fn clear_legacy_queue(&self) -> io::Result<()> {
        let tx = self.db.begin_write().map_err(err)?;
        tx.delete_table(QUEUE).map_err(err)?;
        tx.commit().map_err(err)
    }
}

/// Takes what changed since the last call: the records as they are in
/// memory now (or a delete, if they're gone). Called under the state lock;
/// the slow part (Db::write) runs after it's released.
pub fn take_ops(store: &mut Store) -> (Vec<Dirty>, Vec<Op>) {
    let dirty: Vec<Dirty> = std::mem::take(&mut store.dirty).into_iter().collect();
    let ops = dirty.iter().map(|d| op_of(store, d)).collect();
    (dirty, ops)
}

/// Every record, as writes (the standby's first copy, replica.rs).
pub fn all_ops(store: &Store) -> Vec<Op> {
    store.all_records().iter().map(|d| op_of(store, d)).collect()
}

/// One record as it is in memory now (or a delete, if it's gone).
fn op_of(store: &Store, d: &Dirty) -> Op {
    match d {
        Dirty::User(u) => Op::User(u.clone(), store.users.get(u).map(|r| json(r))),
        Dirty::LastSeen(u) => Op::LastSeen(u.clone(), store.last_seen.get(u).copied()),
        Dirty::Devices(u) => Op::Devices(u.clone(), store.devices.get(u).map(|r| json(r))),
        Dirty::Prekey(u, dev) => Op::Prekey(u.clone(), dev.clone(), store.prekeys.get(u).and_then(|d| d.get(dev)).map(|r| json(r))),
        Dirty::Group(g) => Op::Group(g.clone(), store.groups.get(g).map(|r| json(r))),
        Dirty::Meta => Op::Meta(store.counters.group_seq.load(std::sync::atomic::Ordering::Relaxed)),
    }
}

fn json<T: serde::Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).expect("records serialize")
}

// ---------------------------------------------------------------- saving

/// Saves everything changed since the last save, in one transaction.
/// One save at a time (`hub.saving`), so transactions land in the order
/// their changes were made. A failed save is retried by the next one.
pub async fn save(hub: &Arc<Hub>) {
    let _one_at_a_time = hub.saving.lock().await;
    // Every shard's changes (each shard locked only briefly), one transaction.
    let mut dirty: Vec<(usize, Vec<Dirty>)> = Vec::new();
    let mut ops = Vec::new();
    for i in 0..hub.shard_count() {
        // Untouched shards aren't locked at all.
        if !hub.dirty_flags[i].swap(false, std::sync::atomic::Ordering::AcqRel) {
            continue;
        }
        let mut sh = hub.shard(i);
        let (d, o) = take_ops(&mut sh.store);
        // To the standby too (replica.rs), while the shard is still locked:
        // it sees the changes in the order they were made.
        if let Some(r) = &sh.store.repl {
            r.ops(&o);
        }
        drop(sh);
        if !d.is_empty() {
            dirty.push((i, d));
            ops.extend(o);
        }
    }
    if ops.is_empty() {
        return;
    }
    let db = hub.db.clone();
    let records = ops.len() as u64;
    let started = std::time::Instant::now();
    let res = tokio::task::spawn_blocking(move || db.write(ops)).await.map_err(std::io::Error::other).and_then(|r| r);
    let st = &hub.stats;
    st.save_ns.fetch_add(started.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
    if res.is_ok() {
        st.saves.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        st.save_records.fetch_add(records, std::sync::atomic::Ordering::Relaxed);
    }
    if let Err(e) = res {
        st.save_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        st.last_save_error_ms.store(crate::util::now_ms(), std::sync::atomic::Ordering::Relaxed);
        log("store", format!("save failed, will retry: {e}"));
        for (i, ds) in dirty {
            let mut sh = hub.shard(i);
            for d in ds {
                sh.store.dirty.insert(d);
            }
            hub.dirty_flags[i].store(true, std::sync::atomic::Ordering::Release);
            sh.store.notify.notify_one();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Writes queued stanzas that have waited the write delay (or all of them
/// if `force`), and deletes written ones that are gone, in one synced batch.
/// Stanzas delivered within the delay are never written (write-back).
pub async fn flush_queue(hub: &Arc<Hub>, force: bool) {
    let Some(qdb) = hub.qdb.clone() else { return };
    let _one_at_a_time = hub.saving.lock().await;
    let delay = hub.cfg.queue_write_delay.as_millis() as u64;
    let mut taken: Vec<(usize, Vec<crate::queue_db::Write>, Vec<(String, String)>)> = Vec::new();
    for i in 0..hub.shard_count() {
        if !force && !hub.queue_flags[i].load(std::sync::atomic::Ordering::Acquire) {
            continue;
        }
        let (w, d) = hub.shard(i).store.take_queue(force, delay);
        if !w.is_empty() || !d.is_empty() {
            taken.push((i, w, d));
        }
    }
    if taken.is_empty() {
        return;
    }
    let (writes, deletes): (Vec<_>, Vec<_>) = (taken.iter().flat_map(|t| t.1.clone()).collect(), taken.iter().flat_map(|t| t.2.clone()).collect());
    let (nw, nd) = (writes.len() as u64, deletes.len() as u64);
    let started = std::time::Instant::now();
    let res = tokio::task::spawn_blocking(move || qdb.apply(&writes, &deletes)).await.map_err(std::io::Error::other).and_then(|r| r);
    let st = &hub.stats;
    st.save_ns.fetch_add(started.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
    match res {
        Ok(()) => {
            st.queue_written.fetch_add(nw, std::sync::atomic::Ordering::Relaxed);
            st.queue_deleted.fetch_add(nd, std::sync::atomic::Ordering::Relaxed);
        }
        Err(e) => {
            st.save_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            st.last_save_error_ms.store(crate::util::now_ms(), std::sync::atomic::Ordering::Relaxed);
            log("store", format!("queue write failed, will retry: {e}"));
            for (i, w, d) in taken {
                hub.shard(i).store.put_back(w, d);
            }
        }
    }
}

/// Everything to disk now: records and every queued stanza (admin flush, shutdown).
pub async fn flush_all(hub: &Arc<Hub>) {
    save(hub).await;
    flush_queue(hub, true).await;
}

/// The writer task. Records: saved whenever something changed (while one
/// save runs, new changes collect for the next: group commit). Queue: on a
/// timer, whatever has waited the write delay; and every minute, cold queues
/// leave memory.
pub async fn writer(hub: Arc<Hub>, notify: Arc<tokio::sync::Notify>) {
    let delay = hub.cfg.queue_write_delay;
    let every = if delay.is_zero() { Duration::from_millis(250) } else { (delay / 4).clamp(Duration::from_millis(100), Duration::from_secs(1)) };
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_evict = std::time::Instant::now();
    loop {
        tokio::select! {
            _ = notify.notified() => {
                save(&hub).await;
                // Write-through (delay 0): queued stanzas go out with the records.
                if delay.is_zero() {
                    flush_queue(&hub, false).await;
                }
            }
            _ = tick.tick() => {
                flush_queue(&hub, false).await;
                if last_evict.elapsed() >= Duration::from_secs(60) {
                    last_evict = std::time::Instant::now();
                    crate::server::evict_cold(&hub);
                }
            }
        }
    }
}

/// Writes the whole state as one JSON snapshot, state.json (atomic: temp file + rename).
pub fn write_json(store: &Store, file: &std::path::Path) -> std::io::Result<()> {
    let tmp = file.with_extension(format!("json.{}.tmp", std::process::id()));
    std::fs::write(&tmp, store.snapshot())?;
    std::fs::rename(&tmp, file)
}

/// Opens <data>/xb.redb and loads it; the first time, imports state.json.
pub fn open_store(data: &std::path::Path, reimport: bool) -> std::io::Result<(Store, Db)> {
    let path = data.join("xb.redb");
    let qpath = data.join("queue");
    if reimport {
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        if qpath.exists() {
            std::fs::remove_dir_all(&qpath)?;
        }
    }
    let db = Db::open(&path)?;
    let qdb = Arc::new(crate::queue_db::QueueDb::open(&qpath)?);
    let mut store = Store::new(Some(data.to_path_buf()), crate::store::DEFAULT_OFFLINE_TTL_MS);
    store.qdb = Some(qdb.clone());
    let json = data.join("state.json");
    if db.is_new()? {
        if store.load_json()? {
            store.mark_all();
            db.write(take_ops(&mut store).1)?;
            let (w, d) = store.take_queue(true, 0);
            qdb.apply(&w, &d)?;
            log("store", format!("imported {} into {} and {}", json.display(), path.display(), qpath.display()));
        }
    } else {
        db.load(&mut store)?;
        qdb.load(&mut store)?;
        // An older version kept the queue in xb.redb: move it.
        let legacy = db.take_legacy_queue()?;
        if !legacy.is_empty() {
            qdb.apply(&legacy, &[])?;
            for (owner, key, seq, at, _) in &legacy {
                store.counters.queue_seq.fetch_max(*seq, std::sync::atomic::Ordering::Relaxed);
                store.offline.entry(owner.clone()).or_default().insert(key.clone(), Queued { seq: *seq, at: *at, frame: None, on_disk: true });
            }
            db.clear_legacy_queue()?;
            log("store", format!("moved {} queued stanzas from xb.redb to {}", legacy.len(), qpath.display()));
        }
        // A state.json newer than xb.redb holds changes made elsewhere (another
        // tool or server working from the snapshot), not in xb.redb.
        let modified = |p: &std::path::Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
        if let (Some(j), Some(d)) = (modified(&json), modified(&path)) {
            if j > d {
                log("store", "WARNING: state.json is newer than xb.redb (written after this server last ran?). Using xb.redb; to switch to state.json run: xb-server import-json");
            }
        }
    }
    Ok((store, db))
}
