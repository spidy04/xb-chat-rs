//! The offline queue on disk: fjall, a log-structured (LSM) store in pure
//! Rust, in <data>/queue/. Accounts and the rest stay in redb (db.rs).
//!
//! Why a second store: a queue is almost all "append, then delete soon".
//! An LSM tree writes that sequentially (a journal and in-memory tables,
//! compacted in the background), where a B-tree rewrites pages on every
//! change. And with write-back (store.rs) most stanzas are delivered before
//! they'd be written at all, so this mostly holds the long tail: devices
//! that stay offline.
//!
//!   key   = owner \0 dedup key
//!   value = arrival order (8 bytes) | queued at, ms (8 bytes) | encoded stanza

use crate::store::{Queued, Store};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use std::io;
use std::path::Path;

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("queue store: {e}"))
}

fn key(owner: &str, k: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(owner.len() + 1 + k.len());
    v.extend_from_slice(owner.as_bytes());
    v.push(0);
    v.extend_from_slice(k.as_bytes());
    v
}

/// A stanza to write: (owner, dedup key, arrival order, queued at, encoded stanza).
pub type Write = (String, String, u64, u64, Vec<u8>);

pub struct QueueDb {
    ks: Keyspace,
    part: PartitionHandle,
}

impl QueueDb {
    pub fn open(path: &Path) -> io::Result<QueueDb> {
        let ks = Config::new(path).open().map_err(err)?;
        let part = ks.open_partition("queue", PartitionCreateOptions::default()).map_err(err)?;
        Ok(QueueDb { ks, part })
    }

    /// Writes and deletes in one batch, synced to disk before returning.
    pub fn apply(&self, writes: &[Write], deletes: &[(String, String)]) -> io::Result<()> {
        if writes.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        let mut b = self.ks.batch().durability(Some(PersistMode::SyncAll));
        for (owner, k, seq, at, frame) in writes {
            let mut v = Vec::with_capacity(16 + frame.len());
            v.extend_from_slice(&seq.to_be_bytes());
            v.extend_from_slice(&at.to_be_bytes());
            v.extend_from_slice(frame);
            b.insert(&self.part, key(owner, k), v);
        }
        for (owner, k) in deletes {
            b.remove(&self.part, key(owner, k));
        }
        b.commit().map_err(err)
    }

    /// One stanza's bytes, for a queue entry no longer kept in memory.
    pub fn frame(&self, owner: &str, k: &str) -> Option<Vec<u8>> {
        let v = self.part.get(key(owner, k)).ok()??;
        (v.len() >= 16).then(|| v[16..].to_vec())
    }

    /// Fills the store's queues at startup: the index only (order, time); the
    /// stanzas stay on disk until a device asks for them.
    pub fn load(&self, store: &mut Store) -> io::Result<usize> {
        let mut items: Vec<(u64, String, String, u64)> = Vec::new();
        for kv in self.part.iter() {
            let (k, v) = kv.map_err(err)?;
            let Some(sep) = k.iter().position(|&b| b == 0) else {
                continue;
            };
            if v.len() < 16 {
                continue;
            }
            let owner = String::from_utf8_lossy(&k[..sep]).into_owned();
            let dkey = String::from_utf8_lossy(&k[sep + 1..]).into_owned();
            let seq = u64::from_be_bytes(v[..8].try_into().unwrap());
            let at = u64::from_be_bytes(v[8..16].try_into().unwrap());
            items.push((seq, owner, dkey, at));
        }
        items.sort_by_key(|i| i.0);
        let n = items.len();
        for (seq, owner, dkey, at) in items {
            store.counters.queue_seq.fetch_max(seq, std::sync::atomic::Ordering::Relaxed);
            store.offline.entry(owner).or_default().insert(dkey, Queued { seq, at, frame: None, on_disk: true });
        }
        Ok(n)
    }

    /// Everything durable now (the shutdown path).
    pub fn sync(&self) -> io::Result<()> {
        self.ks.persist(PersistMode::SyncAll).map_err(err)
    }
}
