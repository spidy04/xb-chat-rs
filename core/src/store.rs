//! Server state: accounts, devices, groups, last seen,
//! public pre-keys and one offline queue per device.
//!
//! Everything is held in memory (the fast path, like Mnesia's ram_copies).
//! Each change also marks the record it touched (`Dirty`); the writer task
//! saves those records to <data>/xb.redb (see db.rs) in one transaction.
//! state.json, a JSON snapshot of everything, is only read to import it,
//! and written by `xb-server export-json`. Chat history isn't kept here: it
//! lives on the phones.

use crate::codec::{self, Node};
use crate::util::{js_key_order, js_slice, now_ms, sha256_hex};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

pub const DEFAULT_OFFLINE_TTL_MS: u64 = 30 * 24 * 3600 * 1000;
pub const MAX_DEVICES: usize = 5;
pub const MAX_KEY_DEVICES: usize = 16;
pub const MAX_PREKEYS: usize = 500;

fn is_none<T>(v: &Option<T>) -> bool {
    v.is_none()
}

// ---------------------------------------------------------------- records (JSON shapes)

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct StoredPrivacy {
    #[serde(skip_serializing_if = "is_none", default)]
    pub last: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub online: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub photo: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub about: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub receipts: Option<bool>,
}

/// Privacy with the defaults filled in.
#[derive(Clone, Debug)]
pub struct Privacy {
    pub last: String,
    pub online: String,
    pub photo: String,
    pub about: String,
    pub receipts: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserRec {
    #[serde(default = "phone")]
    pub kind: String,
    #[serde(skip_serializing_if = "is_none", default)]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub about: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub picture: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub salt: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub created: u64,
    #[serde(skip_serializing_if = "is_none", default)]
    pub privacy: Option<StoredPrivacy>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub contacts: Option<Vec<String>>,
    /// Fields this version doesn't know, kept as they are.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

fn phone() -> String {
    "phone".into()
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PushRec {
    pub platform: String,
    pub token: String,
    pub at: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeviceRec {
    pub no: u32,
    pub name: String,
    pub platform: String,
    pub added: u64,
    pub used: u64,
    #[serde(skip_serializing_if = "is_none", default)]
    pub e2e: Option<String>,
    #[serde(skip_serializing_if = "is_none", default)]
    pub push: Option<PushRec>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SignedKey {
    pub id: String,
    pub value: String,
    pub sig: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PrekeyRec {
    pub identity: String,
    pub signing: String,
    pub skey: SignedKey,
    /// One-time pre-keys not handed out yet: id -> public key (base64).
    #[serde(default)]
    pub keys: IndexMap<String, String>,
    #[serde(default)]
    pub used: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Group {
    pub subject: String,
    pub creator: String,
    pub created: u64,
    /// user -> "admin" | "member", oldest first.
    pub members: IndexMap<String, String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// A device's public keys, as handed to someone starting a session with it.
pub struct Bundle {
    pub identity: String,
    pub signing: String,
    pub skey: SignedKey,
    pub key: Option<(String, String)>,
}

pub struct Queued {
    /// Order of arrival (kept on disk too, so a restart keeps the order).
    pub seq: u64,
    pub at: u64,
    /// The encoded stanza; None once it's cold (on disk only, see evict_cold).
    pub frame: Option<Vec<u8>>,
    /// Written to the queue store (queue_db.rs) yet?
    pub on_disk: bool,
}

/// Numbers shared by every shard of a server.
#[derive(Default)]
pub struct Counters {
    pub group_seq: AtomicU64,
    pub queue_seq: AtomicU64,
    /// Stanzas delivered before they were ever written to disk (write-back's saving).
    pub queue_skipped: AtomicU64,
}

impl Counters {
    pub fn next_queue(&self) -> u64 {
        self.queue_seq.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// A record that changed in memory and must be written to the database
/// (or deleted there, if it's gone from memory).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Dirty {
    User(String),
    LastSeen(String),
    /// All devices of a user (one record).
    Devices(String),
    /// (user, end-to-end device id)
    Prekey(String, String),
    Group(String),
    /// Counters.
    Meta,
}

// ---------------------------------------------------------------- the store

pub struct Store {
    file: Option<PathBuf>,
    pictures_dir: Option<PathBuf>,
    picture_mem: HashMap<String, Vec<u8>>,
    pub offline_ttl_ms: u64,
    pub users: IndexMap<String, UserRec>,
    pub groups: IndexMap<String, Group>,
    pub last_seen: IndexMap<String, u64>,
    pub devices: IndexMap<String, IndexMap<String, DeviceRec>>,
    pub offline: IndexMap<String, IndexMap<String, Queued>>,
    pub prekeys: IndexMap<String, IndexMap<String, PrekeyRec>>,
    /// Shared by all shards of one server (see `split`).
    pub counters: Arc<Counters>,
    /// user -> the groups they're in (an index of `groups`, which may live in
    /// other shards; rebuilt at startup, not saved).
    pub member_of: HashMap<String, IndexSet<String>>,
    contact_sets: HashMap<String, HashSet<String>>,
    /// What changed since the last write; the writer task (main.rs) turns it
    /// into one database transaction.
    pub dirty: IndexSet<Dirty>,
    /// Set with `dirty`, so the writer can skip shards with nothing to save
    /// without locking them.
    pub dirty_flag: Arc<std::sync::atomic::AtomicBool>,
    pub notify: Arc<Notify>,
    /// The offline queue on disk (queue_db.rs), for stanzas no longer in memory.
    pub qdb: Option<Arc<crate::queue_db::QueueDb>>,
    /// Queued stanzas not on disk yet, oldest first: (owner, key) -> when queued.
    pub qwrites: IndexMap<(String, String), u64>,
    /// Written stanzas since removed: delete from disk.
    pub qdeletes: Vec<(String, String)>,
    /// Queue work waiting (qwrites or qdeletes), readable without the lock.
    pub queue_flag: Arc<std::sync::atomic::AtomicBool>,
    /// Write delay 0: wake the writer on every stanza (write-through).
    pub write_through: bool,
    /// The standby's stream (replica.rs), when this server has one.
    pub repl: Option<Arc<crate::replica::Sink>>,
}

/// A JSON object -> IndexMap in JavaScript's key order (integer-like keys first).
fn js_map<T: for<'de> Deserialize<'de>>(v: Option<&Value>) -> io::Result<IndexMap<String, T>> {
    let mut out = IndexMap::new();
    let Some(Value::Object(m)) = v else {
        return Ok(out);
    };
    for k in js_key_order(m.keys()) {
        let item = serde_json::from_value(m[k].clone()).map_err(|e| io::Error::other(format!("state.json {k}: {e}")))?;
        out.insert(k.clone(), item);
    }
    Ok(out)
}

impl Store {
    pub fn new(dir: Option<PathBuf>, offline_ttl_ms: u64) -> Store {
        Store {
            file: dir.as_ref().map(|d| d.join("state.json")),
            pictures_dir: dir.as_ref().map(|d| d.join("pictures")),
            picture_mem: HashMap::new(),
            offline_ttl_ms,
            users: IndexMap::new(),
            groups: IndexMap::new(),
            last_seen: IndexMap::new(),
            devices: IndexMap::new(),
            offline: IndexMap::new(),
            prekeys: IndexMap::new(),
            counters: Arc::new(Counters::default()),
            member_of: HashMap::new(),
            contact_sets: HashMap::new(),
            dirty: IndexSet::new(),
            dirty_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            qdb: None,
            qwrites: IndexMap::new(),
            qdeletes: Vec::new(),
            queue_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            write_through: false,
            notify: Arc::new(Notify::new()),
            repl: None,
        }
    }

    /// Reads a state.json snapshot (to import it). -> false if there is none
    pub fn load_json(&mut self) -> io::Result<bool> {
        let Some(file) = &self.file else {
            return Ok(false);
        };
        let raw = match std::fs::read_to_string(file) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        };
        let s: Value = serde_json::from_str(&raw).map_err(|e| io::Error::other(format!("state.json: {e}")))?;
        self.counters.group_seq.store(s.get("groupSeq").and_then(Value::as_u64).unwrap_or(0), Ordering::Relaxed);
        self.users = js_map(s.get("users"))?;
        self.last_seen = js_map(s.get("lastSeen"))?;
        self.groups = js_map(s.get("groups"))?;
        for g in self.groups.values_mut() {
            // members was a JS object too
            let keys: Vec<String> = js_key_order(g.members.keys()).into_iter().cloned().collect();
            g.members = keys
                .into_iter()
                .map(|k| {
                    let v = g.members[&k].clone();
                    (k, v)
                })
                .collect();
        }
        let devices: IndexMap<String, Value> = js_map(s.get("devices"))?;
        for (u, d) in devices {
            self.devices.insert(u, js_map(Some(&d))?);
        }
        let prekeys: IndexMap<String, Value> = js_map(s.get("prekeys"))?;
        for (u, d) in prekeys {
            self.prekeys.insert(u, js_map(Some(&d))?);
        }
        if let Some(Value::Object(off)) = s.get("offline") {
            for owner in js_key_order(off.keys()) {
                let mut q = IndexMap::new();
                for item in off[owner].as_array().into_iter().flatten() {
                    let (Some(key), Some(at), Some(b64)) = (item.get(0).and_then(Value::as_str), item.get(1).and_then(Value::as_u64), item.get(2).and_then(Value::as_str)) else {
                        continue;
                    };
                    let frame = B64.decode(b64).map_err(|e| io::Error::other(format!("state.json offline: {e}")))?;
                    let seq = self.counters.next_queue();
                    q.insert(key.to_string(), Queued { seq, at, frame: Some(frame), on_disk: false });
                    self.qwrites.insert((owner.clone(), key.to_string()), at);
                }
                self.offline.insert(owner.clone(), q);
            }
        }
        Ok(true)
    }

    /// Marks every record changed: after an import, everything gets written.
    pub fn mark_all(&mut self) {
        for d in self.all_records() {
            self.mark(d);
        }
    }

    /// Every record there is.
    pub fn all_records(&self) -> Vec<Dirty> {
        let mut all: Vec<Dirty> = vec![Dirty::Meta];
        all.extend(self.users.keys().map(|u| Dirty::User(u.clone())));
        all.extend(self.last_seen.keys().map(|u| Dirty::LastSeen(u.clone())));
        all.extend(self.devices.keys().map(|u| Dirty::Devices(u.clone())));
        for (u, d) in &self.prekeys {
            all.extend(d.keys().map(|dev| Dirty::Prekey(u.clone(), dev.clone())));
        }
        all.extend(self.groups.keys().map(|g| Dirty::Group(g.clone())));
        all
    }

    /// Something changed: the writer task saves it soon, with whatever else
    /// changed meanwhile, in one transaction.
    fn mark(&mut self, d: Dirty) {
        self.dirty.insert(d);
        self.dirty_flag.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    /// The whole state as JSON text (the state.json format).
    pub fn snapshot(&self) -> String {
        let offline: Map<String, Value> =
            self.offline.iter().map(|(owner, q)| (owner.clone(), Value::Array(q.iter().map(|(k, v)| json!([k, v.at, B64.encode(self.frame(owner, k, v).unwrap_or_default())])).collect()))).collect();
        json!({
            "groupSeq": self.counters.group_seq.load(Ordering::Relaxed),
            "users": self.users,
            "lastSeen": self.last_seen,
            "devices": self.devices,
            "prekeys": self.prekeys,
            "groups": self.groups,
            "offline": offline,
        })
        .to_string()
    }

    pub fn file(&self) -> Option<&PathBuf> {
        self.file.as_ref()
    }

    // ------------------------------------------------------------ accounts

    pub fn password_of(&self, user: &str) -> Option<(String, String)> {
        let r = self.users.get(user)?;
        Some((r.salt.clone()?, r.hash.clone()?))
    }

    /// Legacy account; false if the name is taken.
    pub fn add_password_user(&mut self, user: &str, salt: String, hash: String) -> bool {
        if self.users.contains_key(user) {
            return false;
        }
        self.users.insert(
            user.to_string(),
            UserRec { kind: "password".into(), name: None, about: None, picture: None, salt: Some(salt), hash: Some(hash), created: now_ms(), privacy: None, contacts: None, extra: Map::new() },
        );
        self.mark(Dirty::User(user.to_string()));
        true
    }

    pub fn ensure_phone_account(&mut self, phone: &str, name: Option<&str>) {
        let rec = self.users.entry(phone.to_string()).or_insert_with(|| UserRec {
            kind: "phone".into(),
            name: None,
            about: None,
            picture: None,
            salt: None,
            hash: None,
            created: now_ms(),
            privacy: None,
            contacts: None,
            extra: Map::new(),
        });
        if let Some(n) = name.filter(|n| !n.is_empty()) {
            rec.name = Some(js_slice(n, 25));
        }
        self.mark(Dirty::User(phone.to_string()));
    }

    pub fn has_user(&self, user: &str) -> bool {
        self.users.contains_key(user)
    }

    pub fn kind_of(&self, user: &str) -> String {
        self.users.get(user).map(|r| r.kind.clone()).unwrap_or_else(phone)
    }

    pub fn name_of(&self, user: &str) -> Option<String> {
        self.users.get(user).and_then(|r| r.name.clone())
    }

    pub fn set_name(&mut self, user: &str, name: &str) {
        if let Some(r) = self.users.get_mut(user) {
            r.name = Some(js_slice(name, 25));
            self.mark(Dirty::User(user.to_string()));
        }
    }

    /// (name, about, picture id)
    pub fn profile_of(&self, user: &str) -> Option<(Option<String>, Option<String>, Option<String>)> {
        self.users.get(user).map(|r| (r.name.clone(), r.about.clone(), r.picture.clone()))
    }

    // ------------------------------------------------------------ privacy

    pub fn privacy_of(&self, user: &str) -> Privacy {
        let p = self.users.get(user).and_then(|r| r.privacy.clone()).unwrap_or_default();
        Privacy {
            last: p.last.unwrap_or_else(|| "all".into()),
            online: p.online.unwrap_or_else(|| "all".into()),
            photo: p.photo.unwrap_or_else(|| "all".into()),
            about: p.about.unwrap_or_else(|| "all".into()),
            receipts: p.receipts.unwrap_or(true),
        }
    }

    pub fn set_privacy(&mut self, user: &str, changes: StoredPrivacy) {
        let cur = self.privacy_of(user);
        let Some(r) = self.users.get_mut(user) else {
            return;
        };
        r.privacy = Some(StoredPrivacy {
            last: Some(changes.last.unwrap_or(cur.last)),
            online: Some(changes.online.unwrap_or(cur.online)),
            photo: Some(changes.photo.unwrap_or(cur.photo)),
            about: Some(changes.about.unwrap_or(cur.about)),
            receipts: Some(changes.receipts.unwrap_or(cur.receipts)),
        });
        self.mark(Dirty::User(user.to_string()));
    }

    /// My contacts on xb-chat (replaces the list). -> how many were kept
    pub fn set_contacts(&mut self, user: &str, users: Vec<String>) -> usize {
        if !self.users.contains_key(user) {
            return 0;
        }
        let mut seen = HashSet::new();
        // Only registered users: the caller has checked (they may live in other shards).
        let list: Vec<String> = users.into_iter().filter(|u| seen.insert(u.clone())).filter(|u| u != user).collect();
        let n = list.len();
        self.users.get_mut(user).unwrap().contacts = Some(list);
        self.contact_sets.remove(user);
        self.mark(Dirty::User(user.to_string()));
        n
    }

    pub fn is_contact(&mut self, user: &str, other: &str) -> bool {
        if !self.contact_sets.contains_key(user) {
            let set = self.users.get(user).and_then(|r| r.contacts.clone()).unwrap_or_default().into_iter().collect();
            self.contact_sets.insert(user.to_string(), set);
        }
        self.contact_sets[user].contains(other)
    }

    pub fn set_about(&mut self, user: &str, about: &str) {
        if let Some(r) = self.users.get_mut(user) {
            r.about = Some(js_slice(about, 139));
            self.mark(Dirty::User(user.to_string()));
        }
    }

    // ------------------------------------------------------------ profile photo

    /// Some((image, preview)) sets the photo, None removes it. -> the new id
    pub fn set_picture(&mut self, user: &str, pic: Option<(&[u8], &[u8])>) -> io::Result<Option<String>> {
        let Some(old) = self.users.get(user).map(|r| r.picture.clone()) else {
            return Ok(None);
        };
        let new = match pic {
            Some((image, preview)) => {
                let id = sha256_hex(image)[..16].to_string();
                self.write_picture(user, &id, "image", image)?;
                self.write_picture(user, &id, "preview", preview)?;
                Some(id)
            }
            None => None,
        };
        self.users.get_mut(user).unwrap().picture = new.clone();
        if let Some(old) = old {
            if Some(&old) != new.as_ref() {
                self.delete_picture(user, &old);
            }
        }
        self.mark(Dirty::User(user.to_string()));
        Ok(new)
    }

    /// kind: "image" | "preview" -> (id, JPEG)
    pub fn get_picture(&self, user: &str, kind: &str) -> Option<(String, Vec<u8>)> {
        let id = self.users.get(user)?.picture.clone()?;
        let name = format!("{user}.{id}.{kind}");
        let bytes = match &self.pictures_dir {
            None => self.picture_mem.get(&name)?.clone(),
            Some(dir) => std::fs::read(dir.join(format!("{name}.jpg"))).ok()?,
        };
        Some((id, bytes))
    }

    fn write_picture(&mut self, user: &str, id: &str, kind: &str, bytes: &[u8]) -> io::Result<()> {
        let name = format!("{user}.{id}.{kind}");
        if let Some(r) = &self.repl {
            r.picture(&name, Some(bytes));
        }
        let Some(dir) = &self.pictures_dir else {
            self.picture_mem.insert(name, bytes.to_vec());
            return Ok(());
        };
        std::fs::create_dir_all(dir)?;
        let file = dir.join(format!("{name}.jpg"));
        let tmp = dir.join(format!("{name}.jpg.tmp"));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &file)
    }

    fn delete_picture(&mut self, user: &str, id: &str) {
        for kind in ["image", "preview"] {
            let name = format!("{user}.{id}.{kind}");
            if let Some(r) = &self.repl {
                r.picture(&name, None);
            }
            match &self.pictures_dir {
                None => {
                    self.picture_mem.remove(&name);
                }
                Some(dir) => {
                    let _ = std::fs::remove_file(dir.join(format!("{name}.jpg")));
                }
            }
        }
    }

    pub fn set_last_seen(&mut self, user: &str) {
        self.last_seen.insert(user.to_string(), now_ms());
        self.mark(Dirty::LastSeen(user.to_string()));
    }

    // ------------------------------------------------------------ devices

    /// Adds a device (primary: device 0, replacing any previous primary).
    /// -> (its record, devices dropped to make room)
    pub fn add_device(&mut self, user: &str, key: &str, name: &str, platform: &str, primary: bool) -> (DeviceRec, Vec<(String, DeviceRec)>) {
        let d = self.devices.entry(user.to_string()).or_default();
        let mut dropped: Vec<(String, DeviceRec)> = Vec::new();
        if primary {
            let keys: Vec<String> = d.iter().filter(|(_, r)| r.no == 0).map(|(k, _)| k.clone()).collect();
            for k in keys {
                let r = d.shift_remove(&k).unwrap();
                dropped.push((k, r));
            }
        }
        d.shift_remove(key);
        let used: HashSet<u32> = d.values().map(|r| r.no).collect();
        let mut no = 0;
        if !primary {
            no = 1;
            while used.contains(&no) {
                no += 1;
            }
        }
        let rec = DeviceRec { no, name: js_slice(name, 64), platform: js_slice(platform, 32), added: now_ms(), used: now_ms(), e2e: None, push: None, extra: Map::new() };
        d.insert(key.to_string(), rec.clone());
        while d.len() > MAX_DEVICES {
            let oldest = d.iter().filter(|(k, r)| r.no != 0 && k.as_str() != key).min_by_key(|(_, r)| r.used).map(|(k, _)| k.clone());
            let Some(k) = oldest else { break };
            let r = d.shift_remove(&k).unwrap();
            dropped.push((k, r));
        }
        self.mark(Dirty::Devices(user.to_string()));
        (rec, dropped)
    }

    pub fn device(&self, user: &str, key: &str) -> Option<&DeviceRec> {
        self.devices.get(user)?.get(key)
    }

    /// Marks a device used (login). None if `key` is not a device of `user`.
    pub fn use_device(&mut self, user: &str, key: &str) -> Option<DeviceRec> {
        let dev = self.devices.get_mut(user)?.get_mut(key)?;
        dev.used = now_ms();
        let out = dev.clone();
        self.mark(Dirty::Devices(user.to_string()));
        Some(out)
    }

    pub fn set_device_e2e(&mut self, user: &str, key: &str, e2e: &str) {
        let Some(dev) = self.devices.get_mut(user).and_then(|d| d.get_mut(key)) else {
            return;
        };
        if dev.e2e.as_deref() == Some(e2e) {
            return;
        }
        dev.e2e = Some(e2e.to_string());
        self.mark(Dirty::Devices(user.to_string()));
    }

    pub fn set_push(&mut self, user: &str, key: &str, push: Option<PushRec>) {
        let Some(dev) = self.devices.get_mut(user).and_then(|d| d.get_mut(key)) else {
            return;
        };
        dev.push = push;
        self.mark(Dirty::Devices(user.to_string()));
    }

    /// The push token of the device with end-to-end id `e2e`. -> (device key, token)
    pub fn push_of(&self, user: &str, e2e: &str) -> Option<(String, String)> {
        self.devices.get(user)?.iter().find_map(|(k, d)| match &d.push {
            Some(p) if d.e2e.as_deref() == Some(e2e) => Some((k.clone(), p.token.clone())),
            _ => None,
        })
    }

    pub fn list_devices(&self, user: &str) -> Vec<(String, DeviceRec)> {
        let mut v: Vec<(String, DeviceRec)> = self.devices.get(user).map(|d| d.iter().map(|(k, r)| (k.clone(), r.clone())).collect()).unwrap_or_default();
        v.sort_by_key(|(_, r)| r.no);
        v
    }

    /// Removes a device with its keys and queue. -> the removed record
    pub fn remove_device(&mut self, user: &str, key: &str) -> Option<DeviceRec> {
        let d = self.devices.get_mut(user)?;
        let rec = d.shift_remove(key)?;
        if d.is_empty() {
            self.devices.swap_remove(user);
        }
        if let Some(e2e) = &rec.e2e {
            self.remove_keys(user, e2e);
            self.drop_queue(&format!("{user}/{e2e}"));
        }
        self.mark(Dirty::Devices(user.to_string()));
        Some(rec)
    }

    // ------------------------------------------------------------ end-to-end keys

    /// Publishes a device's keys. With `identity` it (re)registers the device;
    /// without, it only adds one-time pre-keys. False: unknown device, no identity.
    pub fn set_keys(&mut self, user: &str, device: &str, identity: Option<(String, String, SignedKey)>, keys: Vec<(String, String)>) -> bool {
        let d = self.prekeys.entry(user.to_string()).or_default();
        if let Some((identity, signing, skey)) = identity {
            d.insert(device.to_string(), PrekeyRec { identity, signing, skey, keys: IndexMap::new(), used: now_ms() });
        }
        let Some(rec) = d.get_mut(device) else {
            if d.is_empty() {
                self.prekeys.swap_remove(user);
            }
            return false;
        };
        for (id, public) in keys {
            if rec.keys.len() >= MAX_PREKEYS {
                break;
            }
            rec.keys.insert(id, public);
        }
        rec.used = now_ms();
        let mut evicted = Vec::new();
        while d.len() > MAX_KEY_DEVICES {
            let oldest = d.iter().min_by_key(|(_, r)| r.used).map(|(k, _)| k.clone()).unwrap();
            d.shift_remove(&oldest);
            evicted.push(oldest);
        }
        self.mark(Dirty::Prekey(user.to_string(), device.to_string()));
        for e in evicted {
            self.mark(Dirty::Prekey(user.to_string(), e));
        }
        true
    }

    /// -> (registered, one-time pre-keys left)
    pub fn key_count(&self, user: &str, device: &str) -> (bool, usize) {
        match self.prekeys.get(user).and_then(|d| d.get(device)) {
            Some(r) => (true, r.keys.len()),
            None => (false, 0),
        }
    }

    pub fn key_devices(&self, user: &str) -> Vec<String> {
        self.prekeys.get(user).map(|d| d.keys().cloned().collect()).unwrap_or_default()
    }

    /// A device's bundle, handing out (and deleting) one one-time pre-key.
    pub fn take_bundle(&mut self, user: &str, device: &str) -> Option<Bundle> {
        let rec = self.prekeys.get_mut(user)?.get_mut(device)?;
        let first = js_key_order(rec.keys.keys()).first().map(|k| (*k).clone());
        let mut b = Bundle { identity: rec.identity.clone(), signing: rec.signing.clone(), skey: rec.skey.clone(), key: None };
        if let Some(id) = first {
            let public = rec.keys.shift_remove(&id).unwrap();
            b.key = Some((id, public));
            self.mark(Dirty::Prekey(user.to_string(), device.to_string()));
        }
        Some(b)
    }

    pub fn remove_keys(&mut self, user: &str, device: &str) -> bool {
        let Some(d) = self.prekeys.get_mut(user) else {
            return false;
        };
        if d.shift_remove(device).is_none() {
            return false;
        }
        if d.is_empty() {
            self.prekeys.swap_remove(user);
        }
        self.mark(Dirty::Prekey(user.to_string(), device.to_string()));
        true
    }

    // ------------------------------------------------------------ offline queues
    //
    // One per device, owner = "<user>/<e2e device id>". A stanza is stored
    // under a dedup key; storing the same key again is a no-op (client retries).

    pub fn enqueue(&mut self, owner: &str, key: &str, node: &Node) -> bool {
        let q = self.offline.entry(owner.to_string()).or_default();
        if q.contains_key(key) {
            return false;
        }
        let seq = self.counters.next_queue();
        let now = now_ms();
        let frame = codec::encode(node);
        if let Some(r) = &self.repl {
            r.put(owner, key, seq, now, &frame);
        }
        q.insert(key.to_string(), Queued { seq, at: now, frame: Some(frame), on_disk: false });
        // Write-back: kept in memory for now; written only if still here
        // after the write delay (db::flush_queue).
        self.qwrites.insert((owner.to_string(), key.to_string()), now);
        self.queue_flag.store(true, Ordering::Release);
        if self.write_through {
            self.notify.notify_one();
        }
        true
    }

    /// Removed from memory: on disk too if it got there, otherwise it never will.
    fn forget(&mut self, owner: &str, key: &str, q: Queued) {
        if let Some(r) = &self.repl {
            r.del(owner, key);
        }
        if q.on_disk {
            self.qdeletes.push((owner.to_string(), key.to_string()));
            self.queue_flag.store(true, Ordering::Release);
        } else if self.qwrites.shift_remove(&(owner.to_string(), key.to_string())).is_some() {
            // Delivered before it was written: the disk never saw it.
            self.counters.queue_skipped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dequeue(&mut self, owner: &str, key: &str) -> bool {
        let Some(q) = self.offline.get_mut(owner) else {
            return false;
        };
        let Some(removed) = q.shift_remove(key) else {
            return false;
        };
        if q.is_empty() {
            self.offline.swap_remove(owner);
        }
        self.forget(owner, key, removed);
        true
    }

    /// A queued stanza's bytes (for the standby's first copy).
    pub fn queued_frame(&self, owner: &str, key: &str, q: &Queued) -> Option<Vec<u8>> {
        self.frame(owner, key, q)
    }

    /// A queued stanza's bytes: from memory, or from disk for a cold one.
    fn frame(&self, owner: &str, key: &str, q: &Queued) -> Option<Vec<u8>> {
        match &q.frame {
            Some(f) => Some(f.clone()),
            None => self.qdb.as_ref()?.frame(owner, key),
        }
    }

    /// Pending stanzas for one device, oldest first.
    pub fn pending(&self, owner: &str) -> Vec<Node> {
        let Some(q) = self.offline.get(owner) else {
            return Vec::new();
        };
        q.iter().filter_map(|(k, v)| self.frame(owner, k, v)).filter_map(|f| codec::decode(&f).ok()).collect()
    }

    /// The next queued stanzas after arrival number `after`, oldest first,
    /// up to about `max_bytes` (always at least one): a batch of a backlog.
    pub fn pending_after(&self, owner: &str, after: u64, max_bytes: usize) -> Vec<(u64, Node)> {
        let mut out = Vec::new();
        let mut bytes = 0;
        for (k, v) in self.offline.get(owner).into_iter().flat_map(|q| q.iter()) {
            if v.seq <= after {
                continue;
            }
            let Some(frame) = self.frame(owner, k, v) else {
                continue;
            };
            if !out.is_empty() && bytes + frame.len() > max_bytes {
                break;
            }
            if let Ok(n) = codec::decode(&frame) {
                bytes += frame.len();
                out.push((v.seq, n));
            }
        }
        out
    }

    /// Stanzas waiting for any device of `user`.
    /// Looks only at this user's devices, not every queue on the server
    /// (queues of devices that no longer exist can never be delivered anyway).
    pub fn pending_count(&self, user: &str) -> usize {
        let mut ids: HashSet<String> = self.key_devices(user).into_iter().collect();
        ids.extend(self.devices.get(user).into_iter().flat_map(|d| d.values().filter_map(|r| r.e2e.clone())));
        ids.iter().filter_map(|e| self.offline.get(&format!("{user}/{e}"))).map(|q| q.len()).sum()
    }

    pub fn drop_queue(&mut self, owner: &str) {
        if let Some(q) = self.offline.swap_remove(owner) {
            for (key, v) in q {
                self.forget(owner, &key, v);
            }
        }
    }

    /// Drops stanzas older than the TTL. -> how many
    pub fn expire(&mut self) -> usize {
        let now = now_ms();
        let ttl = self.offline_ttl_ms;
        let mut removed = Vec::new();
        for (owner, q) in self.offline.iter_mut() {
            let old: Vec<String> = q.iter().filter(|(_, v)| now.saturating_sub(v.at) > ttl).map(|(k, _)| k.clone()).collect();
            for k in old {
                let v = q.shift_remove(&k).unwrap();
                removed.push((owner.clone(), k, v));
            }
        }
        self.offline.retain(|_, q| !q.is_empty());
        let n = removed.len();
        for (o, k, v) in removed {
            self.forget(&o, &k, v);
        }
        n
    }

    /// Queue work for the disk: stanzas still waiting `delay_ms` after they
    /// arrived (all of them if `force`), and deletions of written ones. The
    /// entries taken are marked as on disk; `put_back` undoes that if the
    /// write fails.
    pub fn take_queue(&mut self, force: bool, delay_ms: u64) -> (Vec<crate::queue_db::Write>, Vec<(String, String)>) {
        let now = now_ms();
        let mut writes = Vec::new();
        // Oldest first: stop at the first one that isn't due yet.
        while let Some((&(ref o, ref k), &t)) = self.qwrites.first() {
            if !force && now.saturating_sub(t) < delay_ms {
                break;
            }
            let (o, k) = (o.clone(), k.clone());
            self.qwrites.shift_remove_index(0);
            if let Some(q) = self.offline.get_mut(&o).and_then(|q| q.get_mut(&k)) {
                if let Some(f) = &q.frame {
                    writes.push((o.clone(), k.clone(), q.seq, q.at, f.clone()));
                    q.on_disk = true;
                }
            }
        }
        let deletes = std::mem::take(&mut self.qdeletes);
        if self.qwrites.is_empty() && self.qdeletes.is_empty() {
            self.queue_flag.store(false, Ordering::Release);
        }
        (writes, deletes)
    }

    /// A write to disk failed: those stanzas are memory-only again, and the
    /// deletions are retried.
    pub fn put_back(&mut self, writes: Vec<crate::queue_db::Write>, deletes: Vec<(String, String)>) {
        for (o, k, _, at, _) in writes {
            if let Some(q) = self.offline.get_mut(&o).and_then(|q| q.get_mut(&k)) {
                q.on_disk = false;
                self.qwrites.insert((o, k), at);
            }
        }
        self.qdeletes.extend(deletes);
        self.queue_flag.store(true, Ordering::Release);
    }

    /// Cold queues out of memory: stanzas already on disk, older than
    /// `cold_ms`, for devices not connected (`live` says which are). They're
    /// read back from disk when the device asks. -> bytes freed
    pub fn evict_cold(&mut self, cold_ms: u64, live: impl Fn(&str) -> bool) -> usize {
        let now = now_ms();
        let mut freed = 0;
        for (owner, q) in self.offline.iter_mut() {
            if live(owner) {
                continue;
            }
            for v in q.values_mut() {
                if v.on_disk && now.saturating_sub(v.at) > cold_ms {
                    if let Some(f) = v.frame.take() {
                        freed += f.len();
                    }
                }
            }
        }
        freed
    }

    /// (stanzas queued, of which held in memory, bytes in memory)
    pub fn queue_memory(&self) -> (usize, usize, usize) {
        let (mut n, mut m, mut b) = (0, 0, 0);
        for q in self.offline.values() {
            for v in q.values() {
                n += 1;
                if let Some(f) = &v.frame {
                    m += 1;
                    b += f.len();
                }
            }
        }
        (n, m, b)
    }

    // ------------------------------------------------------------ groups

    /// A new group id: the time, then a sequence number.
    pub fn new_group_id(&self) -> String {
        let seq = self.counters.group_seq.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}{:04}", now_ms(), seq)
    }

    /// Creates the group `id` (from new_group_id). The members' `member_of`
    /// entries are the caller's job: they may be in other shards.
    pub fn create_group(&mut self, id: &str, creator: &str, subject: &str, members: &[String]) -> String {
        let id = id.to_string();
        let mut m = IndexMap::new();
        m.insert(creator.to_string(), "admin".to_string());
        for u in members {
            if u != creator {
                m.insert(u.clone(), "member".to_string());
            }
        }
        self.groups.insert(id.clone(), Group { subject: subject.to_string(), creator: creator.to_string(), created: now_ms(), members: m, extra: Map::new() });
        self.mark(Dirty::Group(id.clone()));
        self.mark(Dirty::Meta);
        id
    }

    pub fn get_group(&self, id: &str) -> Option<&Group> {
        self.groups.get(id)
    }

    /// The groups `user` is in (from the index; `user` must live in this shard).
    pub fn groups_of(&self, user: &str) -> Vec<String> {
        self.member_of.get(user).map(|g| g.iter().cloned().collect()).unwrap_or_default()
    }

    pub fn index_member(&mut self, user: &str, group: &str) {
        self.member_of.entry(user.to_string()).or_default().insert(group.to_string());
    }

    pub fn unindex_member(&mut self, user: &str, group: &str) {
        if let Some(g) = self.member_of.get_mut(user) {
            g.shift_remove(group);
            if g.is_empty() {
                self.member_of.remove(user);
            }
        }
    }

    /// Splits one store into `n` shards: each user (with their devices, keys,
    /// queues) goes to shard `shard_of(user)`, each group to `shard_of(id)`.
    /// The shards share the counters and the "something changed" signal.
    pub fn split(mut self, n: usize, shard_of: impl Fn(&str) -> usize) -> Vec<Store> {
        let mut out: Vec<Store> = (0..n)
            .map(|_| {
                let mut st = Store::new(None, self.offline_ttl_ms);
                st.file = self.file.clone();
                st.pictures_dir = self.pictures_dir.clone();
                st.counters = self.counters.clone();
                st.notify = self.notify.clone();
                st.qdb = self.qdb.clone();
                st.write_through = self.write_through;
                st.repl = self.repl.clone();
                st
            })
            .collect();
        let user_of_owner = |o: &str| o.split('/').next().unwrap_or("").to_string();
        for (u, r) in self.users.drain(..) {
            out[shard_of(&u)].users.insert(u, r);
        }
        for (u, t) in self.last_seen.drain(..) {
            out[shard_of(&u)].last_seen.insert(u, t);
        }
        for (u, d) in self.devices.drain(..) {
            out[shard_of(&u)].devices.insert(u, d);
        }
        for (u, d) in self.prekeys.drain(..) {
            out[shard_of(&u)].prekeys.insert(u, d);
        }
        for (o, q) in self.offline.drain(..) {
            out[shard_of(&user_of_owner(&o))].offline.insert(o, q);
        }
        for ((o, k), t) in self.qwrites.drain(..) {
            let st = &mut out[shard_of(&user_of_owner(&o))];
            st.qwrites.insert((o, k), t);
            st.queue_flag.store(true, Ordering::Release);
        }
        for (o, k) in self.qdeletes.drain(..) {
            let st = &mut out[shard_of(&user_of_owner(&o))];
            st.qdeletes.push((o, k));
            st.queue_flag.store(true, Ordering::Release);
        }
        for (id, g) in self.groups.drain(..) {
            for m in g.members.keys() {
                out[shard_of(m)].index_member(m, &id);
            }
            out[shard_of(&id)].groups.insert(id, g);
        }
        for (k, v) in self.picture_mem.drain() {
            out[shard_of(k.split('.').next().unwrap_or(""))].picture_mem.insert(k, v);
        }
        // Changes not saved yet go along with their records.
        for d in std::mem::take(&mut self.dirty) {
            let i = match &d {
                Dirty::User(u) | Dirty::LastSeen(u) | Dirty::Devices(u) | Dirty::Prekey(u, _) => shard_of(u),
                Dirty::Group(g) => shard_of(g),
                Dirty::Meta => 0,
            };
            out[i].dirty.insert(d);
            out[i].dirty_flag.store(true, Ordering::Release);
        }
        out
    }

    pub fn add_members(&mut self, id: &str, users: &[String]) {
        let Some(g) = self.groups.get_mut(id) else {
            return;
        };
        for u in users {
            g.members.entry(u.clone()).or_insert_with(|| "member".to_string());
        }
        self.mark(Dirty::Group(id.to_string()));
    }

    pub fn remove_member(&mut self, id: &str, user: &str) {
        let Some(g) = self.groups.get_mut(id) else {
            return;
        };
        g.members.shift_remove(user);
        if g.members.is_empty() {
            self.groups.shift_remove(id);
        } else if !g.members.values().any(|r| r == "admin") {
            // Never leave a group without an admin: promote the oldest member.
            *g.members.get_index_mut(0).unwrap().1 = "admin".to_string();
        }
        self.mark(Dirty::Group(id.to_string()));
    }
}
