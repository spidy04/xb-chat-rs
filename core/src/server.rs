//! The chat server: sessions, login, routing, receipts,
//! groups, presence, typing and every `iq`. PROTOCOL.md is the spec.
//!
//! Concurrency. Each connection is its own task (`serve`), and a second task
//! writes its frames. The shared state is split into SHARDS shards by
//! `hash(user)`. A shard holds those users' accounts, devices, keys, queues,
//! live connections and presence watchers, behind its own lock; a group lives
//! in the shard of `hash(group id)`. So two conversations between different
//! people rarely wait for each other, and the same split works across
//! machines later (a shard per node instead of per lock).
//!
//! The one rule that keeps this deadlock-free: a handler holds either ONE
//! shard, or a set of shards taken together, in ascending order (`Hub::users`).
//! It never takes another shard while holding one. Anything that needs more
//! reads what it needs, lets go, then takes the next.
//!
//! Message flow (store-and-forward, at-least-once):
//!   alice -> <message id to><participants><to jid><enc device/>…</to>…</participants></message>
//!   server: queue each <enc> for its device, push to devices online, <ack class="message" t/>
//!   bob   -> <receipt id to type="delivered|read"/>  drops it from bob's device queue,
//!            and is queued to all of alice's devices
//!   alice -> <ack class="receipt" …/>  drops the receipt from her queue

use crate::codec::{Content, Node};
use crate::frame::FrameDecoder;
use crate::push::{PushResult, Pusher};
use crate::store::{DeviceRec, PushRec, SignedKey, Store, StoredPrivacy};
use crate::tokens::{GROUP_SERVER, USER_SERVER};
use crate::transport::{self, Cipher, ConnRead, ConnWrite};
use crate::util::{js_len, js_slice, log, now_ms, now_s, same};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD as B64URL};
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;

const PICTURE_MAX: usize = 300 * 1024;
const PREVIEW_MAX: usize = 24 * 1024;
const LOOKUP_MAX: usize = 1000;
const CONTACTS_MAX: usize = 5000;

/// How many shards the state is split into.
pub const SHARDS: usize = 64;

/// Which shard a user (or group id) lives in: FNV-1a of the id.
pub fn shard_of(key: &str) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in key.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h % SHARDS as u64) as usize
}

// ---------------------------------------------------------------- small checks

fn is_id(s: &str) -> bool {
    (1..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// ^[1-9][0-9]{0,9}$  (device ids and pre-key ids)
fn is_num_id(s: &str) -> bool {
    (1..=10).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit()) && !s.starts_with('0')
}

fn is_account(s: &str) -> bool {
    (3..=32).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn is_phone(s: &str) -> bool {
    (8..=15).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit()) && !s.starts_with('0')
}

fn is_jpeg(b: Option<&[u8]>) -> bool {
    matches!(b, Some(b) if b.len() > 3 && b[0] == 0xff && b[1] == 0xd8 && b[2] == 0xff)
}

pub fn user_jid(user: &str) -> String {
    format!("{user}@{USER_SERVER}")
}

fn group_jid(id: &str) -> String {
    format!("{id}@{GROUP_SERVER}")
}

/// "bob@s.xb" -> ("bob", "s.xb")
fn parse_jid(jid: Option<&str>) -> Option<(String, String)> {
    let s = jid?;
    let at = s.find('@')?;
    if at == 0 {
        return None;
    }
    Some((s[..at].to_string(), s[at + 1..].to_string()))
}

/// A user on this server: Some(local) for "x@s.xb".
fn user_of(jid: Option<&str>) -> Option<String> {
    parse_jid(jid).filter(|(_, s)| s == USER_SERVER).map(|(l, _)| l)
}

/// An `iq` error: <error code text/>.
struct SErr {
    code: &'static str,
    text: Option<String>,
}

fn serr(code: &'static str) -> SErr {
    SErr { code, text: None }
}

fn serr_t(code: &'static str, text: impl Into<String>) -> SErr {
    SErr { code, text: Some(text.into()) }
}

type IqResult = Result<Option<Vec<Node>>, SErr>;

/// Token bucket: `rate` stanzas a second, bursts up to `burst`.
struct RateLimiter {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    fn take(&mut self) -> bool {
        let t = Instant::now();
        self.tokens = (self.tokens + t.duration_since(self.last).as_secs_f64() * self.rate).min(self.burst);
        self.last = t;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

// ---------------------------------------------------------------- connections

pub type Sid = u64;

/// What a connection's writer task does next.
pub enum Out {
    /// An encoded stanza. Encoded when queued, so the queue's size in bytes
    /// is known (Peer::queued) and a slow reader can be cut off.
    Stanza(Vec<u8>),
    /// Send this <stream:error/> (if given), end the stream, drop it after 1 s.
    Close(Option<Node>),
}

struct Link {
    generation: u64,
    refs: u32,
    name: String,
    platform: String,
    code: Option<String>,
}

/// One connection as the rest of the server sees it: where to send, and who
/// it is once logged in. Shards hold `Arc<Peer>`s (connections, watchers).
pub struct Peer {
    pub sid: Sid,
    tx: UnboundedSender<Out>,
    /// Bytes waiting in the send queue, and the most allowed: a connection
    /// that doesn't read fast enough is closed instead of growing without
    /// limit (its messages are safe in the offline queue).
    queued: std::sync::atomic::AtomicUsize,
    queue_max: usize,
    /// Ends the connection at once (no goodbye): for a reader that's too slow.
    kill: CancellationToken,
    hub_slow: Arc<AtomicU64>,
    /// Its offline backlog is still being sent in batches (pump_backlog):
    /// new stanzas wait their turn at the end of it, so order is kept.
    backlog: std::sync::atomic::AtomicBool,
    /// The writer drained some of the queue: the backlog pump sends more.
    drained: tokio::sync::Notify,
    /// base64 Noise static key: the device's login
    pub device_key: String,
    info: Mutex<PeerInfo>,
}

#[derive(Default)]
struct PeerInfo {
    closed: bool,
    user: Option<String>,
    kind: String,
    /// 0 = primary, 1.. = linked, None = legacy connection that isn't remembered
    device_no: Option<u32>,
    /// end-to-end device id, once the client names it
    e2e: Option<String>,
    /// Like WhatsApp: not "online" until the app says it's open.
    available: bool,
    link: Option<Link>,
}

impl Peer {
    fn info(&self) -> std::sync::MutexGuard<'_, PeerInfo> {
        self.info.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn send(&self, node: Node) {
        if self.info().closed {
            return;
        }
        let bytes = crate::codec::encode(&node);
        let now = self.queued.fetch_add(bytes.len(), Ordering::AcqRel) + bytes.len();
        if now > self.queue_max {
            // Not reading: cut it off. Everything it hasn't acknowledged is in
            // the offline queue, so it gets it all again when it reconnects.
            if !self.kill.is_cancelled() {
                self.hub_slow.fetch_add(1, Ordering::Relaxed);
                log("overload", format!("connection {} reads too slowly ({now} bytes queued): closed", self.sid));
                self.info().closed = true;
                self.kill.cancel();
            }
            return;
        }
        let _ = self.tx.send(Out::Stanza(bytes));
    }

    pub fn close(&self, reason: Option<&str>) {
        let mut i = self.info();
        if i.closed {
            return;
        }
        i.closed = true;
        let _ = self.tx.send(Out::Close(reason.map(|code| Node::new("stream:error").attr("code", code))));
    }

    /// The server is about to restart: "come back in `retry` s" (drain).
    pub fn restart(&self, retry: u64) {
        let mut i = self.info();
        if i.closed {
            return;
        }
        i.closed = true;
        let _ = self.tx.send(Out::Close(Some(Node::new("stream:error").attr("code", "restart").attr("retry", retry))));
    }

    fn closed(&self) -> bool {
        self.info().closed
    }

    fn user(&self) -> Option<String> {
        self.info().user.clone()
    }

    fn e2e(&self) -> Option<String> {
        self.info().e2e.clone()
    }

    fn owner(&self) -> Option<String> {
        let i = self.info();
        Some(format!("{}/{}", i.user.as_ref()?, i.e2e.as_ref()?))
    }

    fn device_no(&self) -> Option<u32> {
        self.info().device_no
    }

    fn kind(&self) -> String {
        self.info().kind.clone()
    }

    fn available(&self) -> bool {
        self.info().available
    }
}

/// Counts a socket as open for as long as its task runs.
struct OpenGuard<'a>(&'a Hub);

impl Drop for OpenGuard<'_> {
    fn drop(&mut self) {
        self.0.stats.open.fetch_sub(1, Ordering::AcqRel);
    }
}

/// What only the connection's own task touches: no lock needed.
struct Session {
    peer: Arc<Peer>,
    auth_failures: u32,
    limiter: RateLimiter,
    /// users whose presence this connection watches
    subscribed_to: HashSet<String>,
}

pub struct Config {
    pub idle_timeout: Duration,
    pub max_stanza: usize,
    pub rate: f64,
    pub burst: f64,
    pub link_ttl_ms: u64,
    pub link_refs: u32,
    pub push_cooldown_ms: u64,
    pub otp_echo: bool,
    pub legacy_password: bool,
    /// A new SMS code at most this often per number.
    pub code_resend_ms: u64,
    /// Tests only: keep every stanza received, so a test can check what the server saw.
    pub record: bool,
    /// Overload protection. Above max_connections new sockets are refused;
    /// at most max_handshakes run at once; logins are limited to login_rate a
    /// second (bursts of login_burst) and the rest are told "busy, retry in N s".
    pub max_connections: usize,
    pub max_handshakes: usize,
    pub login_rate: f64,
    pub login_burst: f64,
    /// Write-back: a queued stanza is written to disk only if still waiting
    /// after this long (0 = at once). A crash loses what's younger.
    pub queue_write_delay: Duration,
    /// Queued stanzas of an offline device older than this leave memory (read back on login).
    pub queue_cold: Duration,
    /// A connection's send queue may hold this many bytes …
    pub send_queue_max: usize,
    /// … and one write may take this long, or the reader is too slow: closed.
    pub write_timeout: Duration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            idle_timeout: Duration::from_secs(90),
            max_stanza: 1024 * 1024,
            rate: 50.0,
            burst: 200.0,
            link_ttl_ms: 60_000,
            link_refs: 5,
            push_cooldown_ms: 5000,
            otp_echo: false,
            legacy_password: false,
            code_resend_ms: 60 * 1000,
            record: false,
            max_connections: 1_000_000,
            max_handshakes: 1024,
            login_rate: 5000.0,
            login_burst: 10_000.0,
            send_queue_max: 16 * 1024 * 1024,
            write_timeout: Duration::from_secs(30),
            queue_write_delay: Duration::from_secs(5),
            queue_cold: Duration::from_secs(300),
        }
    }
}

#[derive(Default)]
pub struct Stats {
    /// Sockets open now (handshaking or logged in), for the connection cap.
    pub open: std::sync::atomic::AtomicUsize,
    /// Turned away: over the cap, too many handshakes, logins told "busy",
    /// readers too slow.
    pub refused: AtomicU64,
    pub busy: AtomicU64,
    pub slow: Arc<AtomicU64>,
    pub connections: AtomicU64,
    pub stanzas_in: AtomicU64,
    pub messages: AtomicU64,
    pub pushes: AtomicU64,
    pub push_failures: AtomicU64,
    /// Handler panics, caught: the connection was closed (internal-error).
    pub panics: AtomicU64,
    /// Saving to disk (db::save): transactions, records, time, failures.
    pub saves: AtomicU64,
    pub save_records: AtomicU64,
    pub save_ns: AtomicU64,
    pub save_errors: AtomicU64,
    pub last_save_error_ms: AtomicU64,
    /// Offline queue on disk: stanzas written, deleted; and cold bytes dropped from memory.
    pub queue_written: AtomicU64,
    pub queue_deleted: AtomicU64,
    pub queue_evicted_bytes: AtomicU64,
}

/// Phone-number login: an SMS code per number.
struct PendingCode {
    hash: [u8; 32],
    expires: u64,
    attempts: u32,
    sent_at: u64,
}

/// One shard of the state: the users (and groups) whose id hashes here.
pub struct Shard {
    pub store: Store,
    /// user -> every logged-in connection
    connections: HashMap<String, Vec<Arc<Peer>>>,
    /// "<user>/<e2e>" -> where that device's queue goes
    devices: HashMap<String, Arc<Peer>>,
    /// user -> connections watching their presence
    watchers: HashMap<String, Vec<Arc<Peer>>>,
    last_push: HashMap<String, u64>,
    /// phone -> SMS code waiting to be typed in
    codes: HashMap<String, PendingCode>,
}

/// Where phones upload files (the media service, xb-media), and the
/// tokens that let them: signed here, checked there.
pub struct Uploads {
    pub tokens: xb_proto::upload_token::UploadTokens,
    /// The media service's public URL (<media_conn host>).
    pub public_url: String,
}

pub struct Hub {
    shards: Vec<Mutex<Shard>>,
    /// Per shard: something to save (Store::dirty_flag), readable without the lock.
    pub dirty_flags: Vec<Arc<std::sync::atomic::AtomicBool>>,
    /// Per shard: queue work for the disk (Store::queue_flag).
    pub queue_flags: Vec<Arc<std::sync::atomic::AtomicBool>>,
    /// The offline queue's store (queue_db.rs); None in tests without one.
    pub qdb: Option<Arc<crate::queue_db::QueueDb>>,
    /// QR ref -> connection waiting to be linked
    links: Mutex<HashMap<String, Arc<Peer>>>,
    /// Where the store is saved (db::save), one save at a time.
    pub db: Arc<crate::db::Db>,
    pub saving: tokio::sync::Mutex<()>,
    pub cfg: Config,
    /// Upload tokens for the media service, and its public URL (None: no media).
    pub media: Option<Uploads>,
    pub push: Option<Arc<Pusher>>,
    pub private_key: [u8; 32],
    pub public_key: [u8; 32],
    pub stats: Stats,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    /// Lock statistics, over all shards: time waiting, time held, how often.
    pub lock_wait_ns: AtomicU64,
    pub lock_hold_ns: AtomicU64,
    pub lock_hold_max_ns: AtomicU64,
    pub locks: AtomicU64,
    /// Tests only (cfg.record): every stanza received, encoded.
    pub seen: Mutex<Vec<Vec<u8>>>,
    next_sid: AtomicU64,
    next_gen: AtomicU64,
    handshakes: tokio::sync::Semaphore,
    logins: Mutex<RateLimiter>,
    /// Set when the server is draining before a restart (drain()): /ready
    /// says no, and anyone who still logs in is told to come back later.
    pub draining: CancellationToken,
    /// How long the restarts are spread over (XB_DRAIN_SPREAD_S).
    pub drain_spread: std::sync::atomic::AtomicU64,
}

struct AuthOk {
    user: String,
    primary: bool,
    remember: bool,
    name: Option<String>,
    device_name: String,
    platform: String,
}

type AuthFail = (&'static str, Vec<(&'static str, String)>);

/// A locked shard; records how long it was held when dropped.
pub struct Locked<'a> {
    g: std::sync::MutexGuard<'a, Shard>,
    held: Instant,
    hub: &'a Hub,
}

impl std::ops::Deref for Locked<'_> {
    type Target = Shard;
    fn deref(&self) -> &Shard {
        &self.g
    }
}

impl std::ops::DerefMut for Locked<'_> {
    fn deref_mut(&mut self) -> &mut Shard {
        &mut self.g
    }
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        let ns = self.held.elapsed().as_nanos() as u64;
        self.hub.lock_hold_ns.fetch_add(ns, Ordering::Relaxed);
        self.hub.lock_hold_max_ns.fetch_max(ns, Ordering::Relaxed);
    }
}

/// Cold queues out of memory, shard by shard: stanzas on disk, older than
/// queue_cold, for devices that aren't connected.
pub fn evict_cold(hub: &Arc<Hub>) {
    let cold = hub.cfg.queue_cold.as_millis() as u64;
    let mut freed = 0;
    for i in 0..hub.shard_count() {
        let mut sh = hub.shard(i);
        let Shard { store, devices, .. } = &mut *sh;
        freed += store.evict_cold(cold, |owner| devices.contains_key(owner));
    }
    if freed > 0 {
        hub.stats.queue_evicted_bytes.fetch_add(freed as u64, Ordering::Relaxed);
    }
}

impl Shard {
    /// (users with a connection, devices receiving live) in this shard.
    pub fn online(&self) -> (usize, usize) {
        (self.connections.len(), self.devices.len())
    }
}

/// Several shards locked together (in ascending order: no deadlocks).
pub struct Multi<'a> {
    gs: Vec<(usize, Locked<'a>)>,
}

impl Multi<'_> {
    /// The shard of `key`; it must be one of the locked ones.
    fn of(&mut self, key: &str) -> &mut Shard {
        let i = shard_of(key);
        let pos = self.gs.binary_search_by_key(&i, |(j, _)| *j).expect("shard was locked");
        &mut self.gs[pos].1
    }
}

impl Hub {
    pub fn new(store: Store, db: Arc<crate::db::Db>, cfg: Config, media: Option<Uploads>, push: Option<Arc<Pusher>>, private_key: [u8; 32], public_key: [u8; 32]) -> Arc<Hub> {
        let (cfg_handshakes, cfg_login_rate, cfg_login_burst) = (cfg.max_handshakes, cfg.login_rate, cfg.login_burst);
        let qdb = store.qdb.clone();
        let mut store = store;
        store.write_through = cfg.queue_write_delay.is_zero();
        let stores = store.split(SHARDS, shard_of);
        let dirty_flags = stores.iter().map(|s| s.dirty_flag.clone()).collect();
        let queue_flags = stores.iter().map(|s| s.queue_flag.clone()).collect();
        let shards = stores
            .into_iter()
            .map(|store| Mutex::new(Shard { store, connections: HashMap::new(), devices: HashMap::new(), watchers: HashMap::new(), last_push: HashMap::new(), codes: HashMap::new() }))
            .collect();
        Arc::new(Hub {
            shards,
            dirty_flags,
            queue_flags,
            qdb,
            links: Mutex::new(HashMap::new()),
            db,
            saving: tokio::sync::Mutex::new(()),
            cfg,
            media,
            push,
            private_key,
            public_key,
            stats: Stats::default(),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            lock_wait_ns: AtomicU64::new(0),
            lock_hold_ns: AtomicU64::new(0),
            lock_hold_max_ns: AtomicU64::new(0),
            locks: AtomicU64::new(0),
            seen: Mutex::new(Vec::new()),
            next_sid: AtomicU64::new(1),
            next_gen: AtomicU64::new(1),
            handshakes: tokio::sync::Semaphore::new(cfg_handshakes),
            logins: Mutex::new(RateLimiter { rate: cfg_login_rate, burst: cfg_login_burst, tokens: cfg_login_burst, last: Instant::now() }),
            draining: CancellationToken::new(),
            drain_spread: AtomicU64::new(10),
        })
    }

    /// A retry time for one client of a drain: random over the spread, so
    /// the new process gets them back evenly instead of all in one second.
    pub fn drain_retry(&self) -> u64 {
        let spread = self.drain_spread.load(Ordering::Relaxed).max(1);
        1 + rand::random::<u64>() % spread
    }

    /// Before a restart: every logged-in connection is told "restart, come
    /// back in N s" (N spread out) and closed. Their unacknowledged messages
    /// are in the offline queue, so nothing is lost. Returns how many.
    pub fn restart_all(&self) -> usize {
        let mut peers = Vec::new();
        for i in 0..self.shard_count() {
            let sh = self.shard(i);
            peers.extend(sh.connections.values().flatten().cloned());
        }
        for p in &peers {
            p.restart(self.drain_retry());
        }
        peers.len()
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Locks shard `i`, measuring the wait ([stats] line).
    pub fn shard(&self, i: usize) -> Locked<'_> {
        let t = Instant::now();
        // A panic in one handler must not take the server down with a
        // poisoned lock: keep using the state.
        let g = self.shards[i].lock().unwrap_or_else(|e| e.into_inner());
        let held = Instant::now();
        self.lock_wait_ns.fetch_add((held - t).as_nanos() as u64, Ordering::Relaxed);
        self.locks.fetch_add(1, Ordering::Relaxed);
        Locked { g, held, hub: self }
    }

    /// The shard of one user (or group id).
    pub fn user(&self, key: &str) -> Locked<'_> {
        self.shard(shard_of(key))
    }

    /// The shards of several users, locked together in ascending order.
    fn users<S: AsRef<str>>(&self, keys: &[S]) -> Multi<'_> {
        let mut idx: Vec<usize> = keys.iter().map(|k| shard_of(k.as_ref())).collect();
        idx.sort_unstable();
        idx.dedup();
        Multi { gs: idx.into_iter().map(|i| (i, self.shard(i))).collect() }
    }

    /// (users online, devices online)
    pub fn online(&self) -> (usize, usize) {
        let mut out = (0, 0);
        for i in 0..self.shards.len() {
            let sh = self.shard(i);
            out.0 += sh.connections.len();
            out.1 += sh.devices.len();
        }
        out
    }

    fn links(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Peer>>> {
        self.links.lock().unwrap_or_else(|e| e.into_inner())
    }
}

// ---------------------------------------------------------------- one connection

/// Runs one connection from the first byte to the end.
pub async fn serve(hub: Arc<Hub>, mut r: ConnRead, mut w: ConnWrite) {
    // Over the cap: refuse at once, before spending anything on it. The
    // client backs off (with jitter) and tries again.
    let open = hub.stats.open.fetch_add(1, Ordering::AcqRel) + 1;
    let _open = OpenGuard(&hub);
    if open > hub.cfg.max_connections {
        hub.stats.refused.fetch_add(1, Ordering::Relaxed);
        return;
    }
    hub.stats.connections.fetch_add(1, Ordering::Relaxed);
    // At most max_handshakes at once (each costs X25519 work): a reconnect
    // storm waits its turn briefly instead of eating every CPU.
    let Ok(Ok(slot)) = tokio::time::timeout(Duration::from_secs(5), hub.handshakes.acquire()).await else {
        hub.stats.refused.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let mut decoder = FrameDecoder::new(hub.cfg.max_stanza + 64);
    // Box::pin: the handshake's state lives on the heap only while it runs.
    // Otherwise every connection's task would keep room for it, for its
    // whole life (a task is as big as its largest state).
    let handshake = Box::pin(transport::accept(&mut r, &mut w, &mut decoder, &hub.private_key));
    let secured = tokio::time::timeout(Duration::from_secs(10), handshake).await;
    drop(slot);
    let secured = match secured {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                log("transport", e.to_string());
            }
            return;
        }
        Err(_) => {
            log("transport", "handshake timeout");
            return;
        }
    };
    let (tx, rx) = unbounded_channel();
    let cancel = CancellationToken::new();
    let activity = Arc::new(AtomicU64::new(now_ms()));
    let (peer, mut s) = new_session(&hub, tx, cancel.clone(), B64.encode(&secured.remote_static));
    let wl = write_loop(hub.clone(), peer.clone(), w, secured.send, rx, cancel.clone(), activity.clone());
    if std::env::var("XB_SIZES").is_ok() {
        log("sizes", format!("writer task: {} bytes, cipher: {} bytes", std::mem::size_of_val(&wl), std::mem::size_of::<Cipher>()));
    }
    let writer = tokio::spawn(wl);
    let mut recv = secured.recv;
    let idle = hub.cfg.idle_timeout.as_millis() as u64;

    'conn: loop {
        // First every frame already buffered. The handshake may have left
        // some: a client sends its last handshake message and <auth> back to
        // back, and both can arrive in one read.
        loop {
            let payload = match decoder.next_frame() {
                Ok(Some(p)) => p,
                Ok(None) => break,
                Err(e) => {
                    log("transport", format!("{e:?}"));
                    break 'conn;
                }
            };
            match recv.open(&payload, hub.cfg.max_stanza) {
                Ok(node) => on_node(&hub, &mut s, node).await,
                Err(e) => {
                    log("transport", e.to_string());
                    break 'conn;
                }
            }
        }
        // Then wait for more bytes.
        let wait = (activity.load(Ordering::Relaxed) + idle).saturating_sub(now_ms());
        tokio::select! {
            _ = cancel.cancelled() => break,
            chunk = r.chunk() => {
                let Ok(Some(chunk)) = chunk else { break };
                activity.store(now_ms(), Ordering::Relaxed);
                hub.bytes_in.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                decoder.push(&chunk);
            }
            _ = tokio::time::sleep(Duration::from_millis(wait)) => {
                if now_ms() - activity.load(Ordering::Relaxed) >= idle {
                    peer.close(Some("timeout"));
                    activity.store(now_ms(), Ordering::Relaxed); // the writer closes it within a second
                }
            }
        }
    }
    cancel.cancel();
    let _ = writer.await;
    on_close(&hub, &mut s);
}

/// A logged-out session for a connection whose device key is `device_key`.
fn new_session(hub: &Arc<Hub>, tx: UnboundedSender<Out>, cancel: CancellationToken, device_key: String) -> (Arc<Peer>, Session) {
    let peer = Arc::new(Peer {
        sid: hub.next_sid.fetch_add(1, Ordering::Relaxed),
        tx,
        queued: std::sync::atomic::AtomicUsize::new(0),
        queue_max: hub.cfg.send_queue_max,
        kill: cancel,
        hub_slow: hub.stats.slow.clone(),
        backlog: std::sync::atomic::AtomicBool::new(false),
        drained: tokio::sync::Notify::new(),
        device_key,
        info: Mutex::new(PeerInfo::default()),
    });
    let s =
        Session { peer: peer.clone(), auth_failures: 0, limiter: RateLimiter { rate: hub.cfg.rate, burst: hub.cfg.burst, tokens: hub.cfg.burst, last: Instant::now() }, subscribed_to: HashSet::new() };
    (peer, s)
}

// ---------------------------------------------------------------- relayed connections

/// What the core sends a gateway for one of its connections (relay.rs).
pub enum Relayed {
    /// An encoded stanza for the client.
    Stanza(u64, Vec<u8>),
    /// End it, after this <stream:error/> (encoded) if given.
    Close(u64, Option<Vec<u8>>),
}

/// A connection held by a gateway (relay.rs): the gateway did the Noise
/// handshake and does the encryption; here is the same session as for a
/// socket of our own (serve). `inbound`: the client's stanzas, encoded;
/// it ends when the client is gone. `gw_sid`: the gateway's number for it.
pub async fn serve_relayed(hub: Arc<Hub>, device_key: String, gw_sid: u64, mut inbound: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>, out: UnboundedSender<Relayed>) {
    let open = hub.stats.open.fetch_add(1, Ordering::AcqRel) + 1;
    let _open = OpenGuard(&hub);
    if open > hub.cfg.max_connections {
        hub.stats.refused.fetch_add(1, Ordering::Relaxed);
        let _ = out.send(Relayed::Close(gw_sid, None));
        return;
    }
    hub.stats.connections.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = unbounded_channel();
    let cancel = CancellationToken::new();
    let (peer, mut s) = new_session(&hub, tx, cancel.clone(), device_key);
    // The writer: stanzas go to the gateway's link, which never blocks here
    // (a client that reads too slowly is the gateway's to close).
    let writer = {
        let (peer, out, cancel) = (peer.clone(), out.clone(), cancel.clone());
        tokio::spawn(async move {
            loop {
                let m = tokio::select! {
                    _ = cancel.cancelled() => break,
                    m = rx.recv() => m,
                };
                match m {
                    None => break,
                    Some(Out::Stanza(bytes)) => {
                        let n = bytes.len();
                        if out.send(Relayed::Stanza(gw_sid, bytes)).is_err() {
                            break;
                        }
                        peer.queued.fetch_sub(n, Ordering::AcqRel);
                        peer.drained.notify_one();
                    }
                    Some(Out::Close(error)) => {
                        let _ = out.send(Relayed::Close(gw_sid, error.map(|e| crate::codec::encode(&e))));
                        return;
                    }
                }
            }
            // Cut off (kill) or the session ended: the gateway closes the client.
            let _ = out.send(Relayed::Close(gw_sid, None));
        })
    };
    let idle = hub.cfg.idle_timeout;
    loop {
        let got = tokio::select! {
            _ = cancel.cancelled() => break,
            m = tokio::time::timeout(idle, inbound.recv()) => m,
        };
        match got {
            Ok(Some(bytes)) => {
                hub.bytes_in.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                if bytes.len() > hub.cfg.max_stanza {
                    break;
                }
                match crate::codec::decode(&bytes) {
                    Ok(node) => on_node(&hub, &mut s, node).await,
                    Err(e) => {
                        log("transport", format!("relayed stanza: {e}"));
                        break;
                    }
                }
            }
            Ok(None) => break, // the client is gone
            Err(_) => peer.close(Some("timeout")),
        }
    }
    cancel.cancel();
    let _ = writer.await;
    on_close(&hub, &mut s);
}

async fn write_loop(hub: Arc<Hub>, peer: Arc<Peer>, mut w: ConnWrite, mut cipher: Box<Cipher>, mut rx: tokio::sync::mpsc::UnboundedReceiver<Out>, cancel: CancellationToken, activity: Arc<AtomicU64>) {
    loop {
        let out = tokio::select! {
            _ = cancel.cancelled() => break,
            m = rx.recv() => m,
        };
        match out {
            None => break,
            Some(Out::Stanza(bytes)) => {
                let n = bytes.len();
                let f = cipher.seal_encoded(bytes);
                hub.bytes_out.fetch_add(f.len() as u64, Ordering::Relaxed);
                // A peer that stops reading fills its socket buffer and this
                // write waits: past write_timeout it's too slow, and closed.
                let written = tokio::select! {
                    // Cut off as too slow (Peer::send): stop at once, even mid-write.
                    _ = cancel.cancelled() => break,
                    r = tokio::time::timeout(hub.cfg.write_timeout, w.write(&f)) => r,
                };
                match written {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => break,
                    Err(_) => {
                        hub.stats.slow.fetch_add(1, Ordering::Relaxed);
                        log("overload", format!("connection {} stopped reading (write timed out): closed", peer.sid));
                        break;
                    }
                }
                peer.queued.fetch_sub(n, Ordering::AcqRel);
                peer.drained.notify_one();
                activity.store(now_ms(), Ordering::Relaxed);
            }
            Some(Out::Close(error)) => {
                if let Some(error) = error {
                    let _ = w.write(&cipher.seal(&error)).await;
                }
                w.shutdown().await;
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                break;
            }
        }
    }
    cancel.cancel();
}

/// One stanza from a connection. Stanzas of one connection are handled in
/// order: the connection task waits for this before reading the next.
async fn on_node(hub: &Arc<Hub>, s: &mut Session, node: Node) {
    if hub.cfg.record {
        hub.seen.lock().unwrap().push(crate::codec::encode(&node));
    }
    hub.stats.stanzas_in.fetch_add(1, Ordering::Relaxed);
    if s.peer.closed() {
        return;
    }
    if s.peer.user().is_none() {
        if node.tag != "auth" {
            return s.peer.close(Some("not-authorized"));
        }
        // Connected just as this process began draining: come back later
        // (to the new process).
        if hub.draining.is_cancelled() {
            return s.peer.restart(hub.drain_retry());
        }
        // Too many logins right now (a reconnect storm after a restart):
        // "busy, retry in N s", spread over a few seconds so they don't all
        // come back together.
        let allowed = hub.logins.lock().unwrap_or_else(|e| e.into_inner()).take();
        if !allowed {
            hub.stats.busy.fetch_add(1, Ordering::Relaxed);
            let retry = 1 + rand::random::<u64>() % 5;
            s.peer.send(Node::new("failure").attr("reason", "busy").attr("retry", retry));
            return s.peer.close(None);
        }
        let mode = node.get("mode").unwrap_or("login");
        if hub.cfg.legacy_password && (mode == "login" || mode == "register") {
            // The legacy password login hashes (slow, ~50 ms) outside any lock.
            let result = Box::pin(password_auth(hub, &node)).await;
            if !s.peer.closed() {
                finish_auth(hub, s, result);
            }
            return;
        }
        return guarded(hub, s, &node, on_auth);
    }
    if !s.limiter.take() {
        return s.peer.send(Node::new("error").attr("code", "rate-limit"));
    }
    guarded(hub, s, &node, dispatch);
}

fn dispatch(hub: &Arc<Hub>, s: &mut Session, node: &Node) {
    match node.tag.as_str() {
        "message" => on_message(hub, s, node),
        "receipt" => on_receipt(hub, s, node),
        "ack" => on_ack(hub, s, node),
        "iq" => on_iq(hub, s, node),
        "presence" => on_presence(hub, s, node),
        "chatstate" => on_chat_state(hub, s, node),
        other => s.peer.send(Node::new("error").attr("code", "bad-request").attr("text", format!("unknown <{other}>"))),
    }
}

/// Runs a handler; if it panics (a bug some input reached), the panic is
/// logged and counted, and this connection is closed with internal-error.
/// The rest of the server goes on: shard locks
/// recover from poisoning, and the connection's cleanup still runs.
fn guarded(hub: &Arc<Hub>, s: &mut Session, node: &Node, handler: fn(&Arc<Hub>, &mut Session, &Node)) {
    let run = std::panic::AssertUnwindSafe(|| handler(hub, s, node));
    if let Err(e) = std::panic::catch_unwind(run) {
        hub.stats.panics.fetch_add(1, Ordering::Relaxed);
        let what = e.downcast_ref::<&str>().map(|s| s.to_string()).or_else(|| e.downcast_ref::<String>().cloned()).unwrap_or_default();
        log("panic", format!("<{}> from connection {}: {what}", node.tag, s.peer.sid));
        s.peer.close(Some("internal-error"));
    }
}

/// The connection ended.
fn on_close(hub: &Arc<Hub>, s: &mut Session) {
    let peer = s.peer.clone();
    stop_link(hub, &peer);
    for target in s.subscribed_to.drain() {
        if let Some(w) = hub.user(&target).watchers.get_mut(&target) {
            w.retain(|p| p.sid != peer.sid);
        }
    }
    let Some(user) = peer.user() else { return };
    let last_one = {
        let mut sh = hub.user(&user);
        if let Some(owner) = peer.owner() {
            if sh.devices.get(&owner).is_some_and(|p| p.sid == peer.sid) {
                sh.devices.remove(&owner);
            }
        }
        let Some(conns) = sh.connections.get_mut(&user) else {
            return;
        };
        let before = conns.len();
        conns.retain(|p| p.sid != peer.sid);
        if conns.len() == before {
            return;
        }
        let empty = conns.is_empty();
        if empty {
            sh.connections.remove(&user);
            sh.store.set_last_seen(&user);
        }
        empty
    };
    if last_one {
        broadcast_presence(hub, &user);
    }
    log("logout", format!("{user} device {}", peer.device_no().map(|n| n.to_string()).unwrap_or("-".into())));
}

// ---------------------------------------------------------------- legacy password login

/// At least one letter, so a username never looks like a phone number.
fn is_username(s: &str) -> bool {
    is_account(s) && s.bytes().any(|b| b.is_ascii_lowercase() || b == b'_')
}

fn scrypt_hash(pass: &str, salt: &[u8]) -> [u8; 32] {
    // scrypt with N = 16384, r = 8, p = 1 (the common defaults).
    let params = scrypt::Params::new(14, 8, 1, 32).expect("valid scrypt params");
    let mut out = [0u8; 32];
    scrypt::scrypt(pass.as_bytes(), salt, &params, &mut out).expect("32-byte output");
    out
}

async fn password_auth(hub: &Arc<Hub>, node: &Node) -> Result<Option<AuthOk>, AuthFail> {
    let user = node.get("user").unwrap_or("").to_string();
    let pass = node.get("pass").unwrap_or("").to_string();
    let mode = node.get("mode").unwrap_or("login").to_string();
    if !is_username(&user) {
        return Err(("invalid-user", vec![]));
    }
    if !(6..=256).contains(&js_len(&pass)) {
        return Err(("invalid-user", vec![]));
    }
    if mode == "register" {
        if hub.user(&user).store.has_user(&user) {
            return Err(("user-exists", vec![]));
        }
        let salt: [u8; 16] = rand::random();
        let hash = tokio::task::spawn_blocking(move || scrypt_hash(&pass, &salt)).await.expect("scrypt task");
        // Another registration may have won while we were hashing.
        if !hub.user(&user).store.add_password_user(&user, B64.encode(salt), B64.encode(hash)) {
            return Err(("user-exists", vec![]));
        }
    } else {
        let rec = hub.user(&user).store.password_of(&user);
        // Hash anyway for unknown users so timing doesn't reveal who exists.
        let salt = rec.as_ref().and_then(|(s, _)| B64.decode(s).ok()).unwrap_or(vec![0u8; 16]);
        let hash = tokio::task::spawn_blocking(move || scrypt_hash(&pass, &salt)).await.expect("scrypt task");
        let ok = rec.and_then(|(_, h)| B64.decode(h).ok()).is_some_and(|want| same(&hash, &want));
        if !ok {
            return Err(("not-authorized", vec![]));
        }
    }
    Ok(Some(AuthOk {
        user,
        primary: false,
        remember: node.get("remember") == Some("true"),
        name: None,
        device_name: node.get("device").filter(|d| !d.is_empty()).unwrap_or("device").to_string(),
        platform: "legacy".into(),
    }))
}

// ---------------------------------------------------------------- auth
//
//   mode="device"         user: the handshake proved we hold this device's key
//   mode="code"|"verify"  phone number + SMS code: this phone becomes the primary
//   mode="link"           a companion (web): QR codes until the phone approves one
//   mode="login"|"register"  legacy username + password, if enabled

fn on_auth(hub: &Arc<Hub>, s: &mut Session, node: &Node) {
    let mode = node.get("mode").unwrap_or("login");
    if mode == "device" {
        let user = node.get("user").unwrap_or("");
        if !is_account(user) {
            return fail(s, "invalid-user", vec![]);
        }
        let dev = hub.user(user).store.use_device(user, &s.peer.device_key);
        let Some(dev) = dev else {
            return fail(s, "not-authorized", vec![]);
        };
        return login(hub, &s.peer, user, Some(dev));
    }
    if mode == "link" {
        return start_link(hub, &s.peer, node);
    }
    let result = match mode {
        "code" | "verify" => phone_auth(hub, &s.peer, node),
        _ => Err(("bad-request", vec![])), // login/register without legacy passwords enabled
    };
    finish_auth(hub, s, result);
}

/// Three wrong credentials close the connection; typos and rate limits don't count.
fn fail(s: &mut Session, reason: &str, attrs: Vec<(&str, String)>) {
    let mut n = Node::new("failure").attr("reason", reason);
    for (k, v) in attrs {
        n = n.attr(k, v);
    }
    s.peer.send(n);
    if reason == "not-authorized" || reason == "wrong-code" {
        s.auth_failures += 1;
        if s.auth_failures >= 3 {
            s.peer.close(Some("not-authorized"));
        }
    }
}

fn finish_auth(hub: &Arc<Hub>, s: &mut Session, result: Result<Option<AuthOk>, AuthFail>) {
    let ok = match result {
        Err((reason, attrs)) => return fail(s, reason, attrs),
        Ok(None) => return, // e.g. "code sent", no login yet
        Ok(Some(ok)) => ok,
    };
    if s.peer.closed() {
        return;
    }
    let key = s.peer.device_key.clone();
    let dev = {
        let mut sh = hub.user(&ok.user);
        if ok.primary {
            // A verified phone becomes the primary device. Verifying from a new
            // phone logs out every other device, like moving WhatsApp to a new phone.
            sh.store.ensure_phone_account(&ok.user, ok.name.as_deref());
            for (k, _) in sh.store.list_devices(&ok.user) {
                if k != key {
                    drop_device(&mut sh, &ok.user, &k, "device-removed");
                }
            }
            Some(sh.store.add_device(&ok.user, &key, &ok.device_name, &ok.platform, true).0)
        } else if ok.remember {
            let (rec, dropped) = sh.store.add_device(&ok.user, &key, &ok.device_name, &ok.platform, false);
            for (k, d) in dropped {
                kick_device(&mut sh, &ok.user, &k, &d, "device-removed");
            }
            Some(rec)
        } else {
            None
        }
    };
    login(hub, &s.peer, &ok.user, dev);
}

fn phone_auth(hub: &Arc<Hub>, peer: &Peer, a: &Node) -> Result<Option<AuthOk>, AuthFail> {
    const CODE_LEN: usize = 6;
    const CODE_TTL_MS: u64 = 10 * 60 * 1000;
    const MAX_ATTEMPTS: u32 = 5;
    let resend = hub.cfg.code_resend_ms;
    let hash = |phone: &str, code: &str| -> [u8; 32] {
        use sha2::{Digest, Sha256};
        Sha256::digest(format!("{phone}:{code}").as_bytes()).into()
    };
    let phone = a.get("phone").unwrap_or("").to_string();
    if !is_phone(&phone) {
        return Err(("invalid-phone", vec![]));
    }
    let now = now_ms();
    let mut sh = hub.user(&phone);
    if a.get("mode") == Some("code") {
        if let Some(p) = sh.codes.get(&phone) {
            if now - p.sent_at < resend {
                let retry = (resend - (now - p.sent_at)).div_ceil(1000);
                return Err(("rate-limit", vec![("retry", retry.to_string())]));
            }
        }
        let code = format!("{:06}", rand::random::<u32>() % 1_000_000);
        sh.codes.insert(phone.clone(), PendingCode { hash: hash(&phone, &code), expires: now + CODE_TTL_MS, attempts: 0, sent_at: now });
        drop(sh);
        // The SMS provider: for now the code goes to this log (development).
        log("otp", format!("SMS to +{phone}: Your xb-chat code: {}-{}. Don't share it.", &code[..3], &code[3..]));
        peer.send(Node::new("code").attr("phone", &phone).attr("length", CODE_LEN).attr("retry", resend / 1000).opt("dev", hub.cfg.otp_echo.then_some(code)));
        return Ok(None);
    }
    // mode = "verify"
    let Some(p) = sh.codes.get_mut(&phone).filter(|p| now <= p.expires) else {
        return Err(("code-expired", vec![]));
    };
    p.attempts += 1;
    if p.attempts > MAX_ATTEMPTS {
        sh.codes.remove(&phone);
        return Err(("too-many-attempts", vec![]));
    }
    let digits: String = a.get("code").unwrap_or("").chars().filter(char::is_ascii_digit).collect();
    if !same(&hash(&phone, &digits), &p.hash) {
        return Err(("wrong-code", vec![]));
    }
    sh.codes.remove(&phone);
    Ok(Some(AuthOk {
        user: phone,
        primary: true,
        remember: false,
        name: a.get("name").map(str::to_string),
        device_name: a.get("device").filter(|d| !d.is_empty()).unwrap_or("Phone").to_string(),
        platform: a.get("platform").filter(|d| !d.is_empty()).unwrap_or("unknown").to_string(),
    }))
}

/// Logs the connection in as `user` (device record `dev`, or None for a
/// legacy connection that isn't remembered).
fn login(hub: &Arc<Hub>, peer: &Arc<Peer>, user: &str, dev: Option<DeviceRec>) {
    if peer.closed() {
        return;
    }
    stop_link(hub, peer);
    {
        let mut sh = hub.user(user);
        let kind = sh.store.kind_of(user);
        // One connection per device: the newest wins (legacy: one per account).
        for old in sh.connections.get(user).cloned().unwrap_or_default() {
            if kind == "password" || old.device_key == peer.device_key {
                old.close(Some("conflict"));
            }
        }
        {
            let mut i = peer.info();
            i.user = Some(user.to_string());
            i.kind = kind;
            i.device_no = dev.as_ref().map(|d| d.no);
        }
        sh.connections.entry(user.to_string()).or_default().push(peer.clone());
        let success = Node::new("success")
            .attr("jid", user_jid(user))
            .opt("device", dev.as_ref().map(|d| d.no))
            .opt("name", sh.store.name_of(user))
            .attr("t", now_s())
            .attr("offline", sh.store.pending_count(user));
        peer.send(success);
        // A device we know: its queue can go out now. Others first name their
        // end-to-end device (<count device/>).
        if let Some(e2e) = dev.as_ref().and_then(|d| d.e2e.clone()) {
            bind_device(hub, &mut sh, peer, &e2e);
        }
    }
    broadcast_presence(hub, user);
    log("login", format!("{user} device {}", dev.map(|d| d.no.to_string()).unwrap_or("-".into())));
}

/// The connection names its end-to-end device: it now gets that device's
/// queue. `sh` is the connection's user's shard.
fn bind_device(hub: &Arc<Hub>, sh: &mut Shard, peer: &Arc<Peer>, e2e: &str) {
    if peer.e2e().as_deref() == Some(e2e) {
        return;
    }
    if let Some(owner) = peer.owner() {
        if sh.devices.get(&owner).is_some_and(|p| p.sid == peer.sid) {
            sh.devices.remove(&owner);
        }
    }
    let (user, no) = {
        let mut i = peer.info();
        i.e2e = Some(e2e.to_string());
        (i.user.clone().unwrap_or_default(), i.device_no)
    };
    let owner = format!("{user}/{e2e}");
    if no.is_some() {
        sh.store.set_device_e2e(&user, &peer.device_key, e2e);
    }
    sh.last_push.remove(&owner);
    if let Some(other) = sh.devices.get(&owner) {
        if other.sid != peer.sid {
            other.close(Some("conflict"));
        }
    }
    sh.devices.insert(owner.clone(), peer.clone());
    // Its queue goes out in batches, as fast as it reads them: a phone back
    // after weeks offline must not be cut off as "too slow" by its own backlog.
    if sh.store.offline.contains_key(&owner) {
        peer.backlog.store(true, Ordering::Release);
        tokio::spawn(pump_backlog(hub.clone(), peer.clone(), owner));
    }
}

/// Sends a device's offline queue in batches of about a quarter of the send
/// queue limit, the next one when the writer has drained the last. Stanzas
/// that arrive meanwhile are queued, not sent live (deliver checks
/// `backlog`), and go out here in order. It ends with the shard locked, so
/// nothing can slip in between the last batch and live delivery resuming.
async fn pump_backlog(hub: Arc<Hub>, peer: Arc<Peer>, owner: String) {
    let user = owner.split('/').next().unwrap_or("").to_string();
    let budget = (hub.cfg.send_queue_max / 4).clamp(1024, 1024 * 1024);
    let mut cursor = 0u64;
    loop {
        if peer.closed() {
            return;
        }
        {
            let sh = hub.user(&user);
            // Another connection took over this device: it has its own pump.
            if !sh.devices.get(&owner).is_some_and(|p| p.sid == peer.sid) {
                return;
            }
            let batch = sh.store.pending_after(&owner, cursor, budget);
            if batch.is_empty() {
                peer.backlog.store(false, Ordering::Release);
                return;
            }
            for (seq, n) in batch {
                cursor = seq;
                peer.send(n);
            }
        }
        // Wait until most of it has been written to the socket.
        while peer.queued.load(Ordering::Acquire) > budget / 2 && !peer.closed() {
            tokio::select! {
                _ = peer.drained.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }
}

// ---------------------------------------------------------------- linking (QR)

fn start_link(hub: &Arc<Hub>, peer: &Arc<Peer>, a: &Node) {
    {
        let mut i = peer.info();
        if i.link.is_some() {
            return;
        }
        let name = js_slice(a.get("name").filter(|s| !s.is_empty()).unwrap_or("Browser"), 64);
        let platform = js_slice(a.get("platform").filter(|s| !s.is_empty()).unwrap_or("web"), 32);
        i.link = Some(Link { generation: 0, refs: 0, name, platform, code: None });
    }
    next_link_ref(hub, peer);
}

/// A fresh QR ref every link_ttl, like WhatsApp Web's rotating QR code.
fn next_link_ref(hub: &Arc<Hub>, peer: &Arc<Peer>) {
    let generation = hub.next_gen.fetch_add(1, Ordering::Relaxed);
    let code = B64URL.encode(rand::random::<[u8; 12]>());
    let (old, over) = {
        let mut i = peer.info();
        if i.closed {
            return;
        }
        let Some(l) = i.link.as_mut() else { return };
        let old = l.code.take();
        l.refs += 1;
        let over = l.refs > hub.cfg.link_refs;
        if !over {
            l.code = Some(code.clone());
            l.generation = generation;
        }
        (old, over)
    };
    {
        let mut links = hub.links();
        if let Some(old) = old {
            links.remove(&old);
        }
        if !over {
            links.insert(code.clone(), peer.clone());
        }
    }
    if over {
        return peer.close(Some("timeout"));
    }
    peer.send(Node::new("link").attr("ref", code).attr("ttl", (hub.cfg.link_ttl_ms as f64 / 1000.0).round() as u64));
    let (hub2, peer2, ttl) = (hub.clone(), peer.clone(), hub.cfg.link_ttl_ms);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(ttl)).await;
        let current = peer2.info().link.as_ref().map(|l| l.generation);
        if current == Some(generation) {
            next_link_ref(&hub2, &peer2);
        }
    });
}

fn stop_link(hub: &Arc<Hub>, peer: &Peer) {
    let Some(l) = peer.info().link.take() else {
        return;
    };
    if let Some(code) = l.code {
        hub.links().remove(&code);
    }
}

/// The primary approves a scanned QR code: <iq xmlns="md" type="set"><link ref key/></iq>.
/// `key` (from the QR) must be the Noise key of the connection waiting with
/// that ref, so a QR code can't be used by anyone else.
fn iq_link(hub: &Arc<Hub>, s: &mut Session, node: &Node) -> IqResult {
    let op = node.child("link").filter(|_| node.get("type") == Some("set")).ok_or(serr("bad-request"))?;
    if s.peer.device_no() != Some(0) {
        return Err(serr_t("forbidden", "only the primary phone can link devices"));
    }
    let waiting = hub.links().get(op.get("ref").unwrap_or("")).cloned().filter(|w| !w.closed());
    let Some(waiting) = waiting else {
        return Err(serr_t("item-not-found", "QR code expired"));
    };
    if op.get("key") != Some(waiting.device_key.as_str()) {
        return Err(serr_t("item-not-found", "QR code does not match"));
    }
    let Some((name, platform)) = waiting.info().link.as_ref().map(|l| (l.name.clone(), l.platform.clone())) else {
        return Err(serr_t("item-not-found", "QR code expired"));
    };
    let user = s.peer.user().unwrap_or_default();
    let rec = {
        let mut sh = hub.user(&user);
        let (rec, dropped) = sh.store.add_device(&user, &waiting.device_key, &name, &platform, false);
        for (k, d) in dropped {
            kick_device(&mut sh, &user, &k, &d, "device-removed");
        }
        rec
    };
    let no = rec.no;
    login(hub, &waiting, &user, Some(rec));
    log("link", format!("{user} device {no} ({name})"));
    Ok(Some(vec![Node::new("device").attr("no", no).attr("name", name).attr("platform", platform)]))
}

/// Closes a device's connections and forgets it (keys and queue too).
/// `sh` is `user`'s shard.
fn drop_device(sh: &mut Shard, user: &str, key: &str, reason: &str) -> Option<DeviceRec> {
    let rec = sh.store.remove_device(user, key)?;
    kick_device(sh, user, key, &rec, reason);
    Some(rec)
}

fn kick_device(sh: &mut Shard, user: &str, key: &str, rec: &DeviceRec, reason: &str) {
    for p in sh.connections.get(user).cloned().unwrap_or_default() {
        if p.device_key == key {
            p.close(Some(reason));
        }
    }
    if let Some(e2e) = &rec.e2e {
        sh.store.remove_keys(user, e2e);
        sh.store.drop_queue(&format!("{user}/{e2e}"));
    }
}

// ---------------------------------------------------------------- messages

/// Content is end-to-end encrypted: one <participants> with a <to jid> per
/// receiving user, holding one <enc> per device. The <enc>s must cover
/// exactly the devices with keys (recipient's or members', plus my other
/// devices); if not, "stale-devices" and the sender re-encrypts.
/// The device list a sender-key group message was made for (the client's
/// phash, PROTOCOL.md §10.5): HMAC-SHA256, key "xb-phash", of the sorted "user.device" list
/// joined with commas; the first 8 bytes in hex.
fn phash_of(mut addrs: Vec<String>) -> String {
    use hmac::Mac;
    addrs.sort();
    let mut m = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"xb-phash").expect("any key length");
    m.update(addrs.join(",").as_bytes());
    m.finalize().into_bytes()[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// A group message may use sender keys: <participants phash> with pairwise
/// <enc>s only for devices getting the sender's key, then one
/// <enc type="skmsg"> that every member device receives.
fn on_message(hub: &Arc<Hub>, s: &mut Session, node: &Node) {
    let peer = &s.peer;
    let id = node.get("id").unwrap_or("").to_string();
    let to = node.get("to").unwrap_or("").to_string();
    let nack = |code: &str| peer.send(Node::new("ack").attr("id", &id).attr("class", "message").attr("error", code));
    if !is_id(&id) {
        return nack("bad-request");
    }
    let Some((tlocal, tserver)) = parse_jid(node.get("to")) else {
        return nack("bad-request");
    };
    let Some(my_e2e) = peer.e2e() else {
        return nack("bad-request");
    }; // publish keys first
    let (parts, sk) = match node.kids() {
        [p] if node.has_list() && p.tag == "participants" => (p, None),
        [p, e] if node.has_list() && p.tag == "participants" && e.tag == "enc" && e.get("type") == Some("skmsg") && e.content_bytes().is_some() => (p, Some(e)),
        _ => return nack("bad-request"),
    };
    if parts.content.is_some() && !parts.has_list() {
        return nack("bad-request");
    }

    let mut per_user: IndexMap<String, Vec<&Node>> = IndexMap::new();
    for p in parts.kids() {
        let j = parse_jid(p.get("jid"));
        let encs: Vec<&Node> = p.kids().iter().collect();
        let ok = encs.iter().all(|e| e.tag == "enc" && e.content_bytes().is_some() && e.get("device").is_some_and(is_num_id));
        match j {
            Some((local, server)) if p.tag == "to" && server == USER_SERVER && !per_user.contains_key(&local) && ok => {
                per_user.insert(local, encs);
            }
            _ => return nack("bad-request"),
        }
    }
    let me = peer.user().unwrap_or_default();
    let my_jid = user_jid(&me);
    // Exactly these devices, no more and no less.
    let covers = |sh: &Shard, user: &str, encs: Option<&Vec<&Node>>| -> bool {
        let want: Vec<String> = sh.store.key_devices(user).into_iter().filter(|d| !(user == me && *d == my_e2e)).collect();
        let encs: &[&Node] = encs.map(|v| v.as_slice()).unwrap_or(&[]);
        let got: HashSet<&str> = encs.iter().filter_map(|e| e.get("device")).collect();
        got.len() == encs.len() && got.len() == want.len() && want.iter().all(|d| got.contains(d.as_str()))
    };

    let t = now_s();
    let (users, key, group) = if tserver == USER_SERVER {
        let users = if tlocal == me { vec![me.clone()] } else { vec![tlocal.clone(), me.clone()] };
        (users, format!("m:{my_jid}::{id}"), false)
    } else if tserver == GROUP_SERVER {
        // The group's shard first (members), then the members' shards.
        let members = {
            let sh = hub.user(&tlocal);
            let Some(g) = sh.store.get_group(&tlocal) else {
                return nack("item-not-found");
            };
            if !g.members.contains_key(&me) {
                return nack("not-member");
            }
            g.members.keys().cloned().collect::<Vec<_>>()
        };
        (members, format!("m:{to}:{my_jid}:{id}"), true)
    } else {
        return nack("item-not-found");
    };

    // Every shard involved, locked together.
    let mut m = hub.users(&users);
    if !group {
        if !m.of(&tlocal).store.has_user(&tlocal) {
            return nack("item-not-found");
        }
        if tlocal != me && m.of(&tlocal).store.key_devices(&tlocal).is_empty() {
            return nack("no-keys");
        }
    }
    if per_user.keys().any(|u| !users.contains(u)) {
        return nack("stale-devices");
    }
    if let Some(sk) = sk {
        if !group {
            return nack("bad-request");
        }
        // Every member device gets the skmsg; the pairwise ones only theirs.
        let want: Vec<(String, Vec<String>)> = users.iter().map(|u| (u.clone(), m.of(u).store.key_devices(u).into_iter().filter(|d| !(*u == me && *d == my_e2e)).collect())).collect();
        let addrs: Vec<String> = want.iter().flat_map(|(u, ds)| ds.iter().map(move |d| format!("{u}.{d}"))).collect();
        if parts.get("phash") != Some(phash_of(addrs).as_str()) {
            return nack("stale-devices");
        }
        let mut pairwise: HashMap<String, &Node> = HashMap::new();
        for (u, encs) in &per_user {
            let devs = &want.iter().find(|(w, _)| w == u).unwrap().1;
            for e in encs {
                let d = e.get("device").unwrap();
                if !devs.iter().any(|x| x == d) || pairwise.insert(format!("{u}.{d}"), e).is_some() {
                    return nack("stale-devices");
                }
            }
        }
        let notify = m.of(&me).store.name_of(&me);
        for (u, devs) in &want {
            for d in devs {
                let mut content = Vec::with_capacity(2);
                if let Some(p) = pairwise.get(&format!("{u}.{d}")) {
                    content.push((*p).clone());
                }
                content.push(sk.clone());
                let msg = Node::new("message").attr("id", &id).attr("from", &to).attr("participant", &my_jid).attr("type", "group").attr("t", t).opt("notify", notify.clone()).nodes(content);
                deliver(hub, m.of(u), &format!("{u}/{d}"), &key, msg);
            }
        }
        drop(m);
        hub.stats.messages.fetch_add(1, Ordering::Relaxed);
        return peer.send(Node::new("ack").attr("id", &id).attr("class", "message").attr("t", t));
    }
    for u in &users {
        if !covers(m.of(u), u, per_user.get(u)) {
            return nack("stale-devices");
        }
    }
    let notify = m.of(&me).store.name_of(&me);
    for (u, encs) in &per_user {
        for enc in encs {
            let attrs = if group {
                Node::new("message").attr("id", &id).attr("from", &to).attr("participant", &my_jid).attr("type", "group").attr("t", t).opt("notify", notify.clone())
            } else {
                Node::new("message").attr("id", &id).attr("from", &my_jid).attr("type", "chat").attr("t", t).opt("notify", notify.clone()).opt("recipient", (*u == me).then(|| to.clone()))
            };
            let msg = attrs.nodes(vec![(*enc).clone()]);
            deliver(hub, m.of(u), &format!("{u}/{}", enc.get("device").unwrap()), &key, msg);
        }
    }
    drop(m);
    hub.stats.messages.fetch_add(1, Ordering::Relaxed);
    peer.send(Node::new("ack").attr("id", &id).attr("class", "message").attr("t", t));
}

/// Queue first, then push: if the push is lost the stanza is still queued.
/// `sh` is the shard of the owner's user.
fn deliver(hub: &Arc<Hub>, sh: &mut Shard, owner: &str, key: &str, node: Node) {
    if !sh.store.enqueue(owner, key, &node) {
        return; // a retry of something already queued or delivered
    }
    let is_message = node.tag == "message";
    if let Some(live) = sh.devices.get(owner) {
        // Its backlog is still going out: this one follows it (pump_backlog).
        if !live.backlog.load(Ordering::Acquire) {
            live.send(node);
        }
    } else if is_message {
        wake(hub, sh, owner);
    }
}

/// A message waits for a device that isn't connected: if it gave us a push
/// token, wake it (content-free; it connects and takes its queue).
fn wake(hub: &Arc<Hub>, sh: &mut Shard, owner: &str) {
    let Some(push) = hub.push.clone() else { return };
    let Some((user, e2e)) = owner.split_once('/') else {
        return;
    };
    let Some((key, token)) = sh.store.push_of(user, e2e) else {
        return;
    };
    let now = now_ms();
    if now - sh.last_push.get(owner).copied().unwrap_or(0) < hub.cfg.push_cooldown_ms {
        return; // one is on its way
    }
    sh.last_push.insert(owner.to_string(), now);
    hub.stats.pushes.fetch_add(1, Ordering::Relaxed);
    let (hub2, user, e2e) = (hub.clone(), user.to_string(), e2e.to_string());
    tokio::spawn(async move {
        match push.send(&token, "wake").await {
            PushResult::Ok => log("push", format!("{user} woke device {e2e}")),
            PushResult::Invalid => {
                hub2.stats.push_failures.fetch_add(1, Ordering::Relaxed);
                hub2.user(&user).store.set_push(&user, &key, None);
                log("push", format!("{user} device {e2e}: token no longer valid, removed"));
            }
            PushResult::Error(e) => {
                hub2.stats.push_failures.fetch_add(1, Ordering::Relaxed);
                log("push", format!("{user} device {e2e}: {e}"));
            }
        }
    });
}

/// To every device of `user` (except the e2e device `except`). `sh` is `user`'s shard.
fn deliver_to_user(hub: &Arc<Hub>, sh: &mut Shard, user: &str, key: &str, node: Node, except: Option<&str>) {
    for d in sh.store.key_devices(user) {
        if Some(d.as_str()) != except {
            deliver(hub, sh, &format!("{user}/{d}"), key, node.clone());
        }
    }
}

fn on_receipt(hub: &Arc<Hub>, s: &mut Session, node: &Node) {
    let id = node.get("id").unwrap_or("").to_string();
    let typ = node.get("type").unwrap_or("delivered").to_string();
    if !is_id(&id) || !(typ == "delivered" || typ == "read") {
        return;
    }
    let to = node.get("to").unwrap_or("").to_string();
    let Some((tlocal, tserver)) = parse_jid(node.get("to")) else {
        return;
    };
    let me = s.peer.user().unwrap_or_default();
    let my_jid = user_jid(&me);
    let my_e2e = s.peer.e2e();
    let owner = s.peer.owner();
    let t = now_s();
    if tserver == USER_SERVER {
        let mut m = hub.users(&[&me, &tlocal]);
        if let Some(o) = &owner {
            m.of(&me).store.dequeue(o, &format!("m:{to}::{id}"));
        }
        if !m.of(&tlocal).store.has_user(&tlocal) || tlocal == me {
            return; // my own copy acknowledged
        }
        if typ == "read" {
            // My other devices: this chat has been read here.
            let n = Node::new("receipt").attr("id", &id).attr("from", &to).attr("type", "read-self").attr("t", t);
            deliver_to_user(hub, m.of(&me), &me, &format!("r:{to}::{id}:read-self"), n, my_e2e.as_deref());
            // Read receipts off on either side: no blue ticks (groups always send them).
            if !m.of(&me).store.privacy_of(&me).receipts || !m.of(&tlocal).store.privacy_of(&tlocal).receipts {
                return;
            }
        }
        let n = Node::new("receipt").attr("id", &id).attr("from", &my_jid).attr("type", &typ).attr("t", t);
        deliver_to_user(hub, m.of(&tlocal), &tlocal, &format!("r:{my_jid}::{id}:{typ}"), n, None);
    } else if tserver == GROUP_SERVER {
        let participant = node.get("participant").unwrap_or("").to_string();
        let Some(sender) = user_of(node.get("participant")) else {
            return;
        };
        if let Some(o) = &owner {
            hub.user(&me).store.dequeue(o, &format!("m:{to}:{participant}:{id}"));
        }
        let member = hub.user(&tlocal).store.get_group(&tlocal).is_some_and(|g| g.members.contains_key(&me));
        if !member || sender == me {
            return;
        }
        let mut m = hub.users(&[&me, &sender]);
        if !m.of(&sender).store.has_user(&sender) {
            return;
        }
        if typ == "read" {
            let n = Node::new("receipt").attr("id", &id).attr("from", &to).attr("participant", &participant).attr("type", "read-self").attr("t", t);
            deliver_to_user(hub, m.of(&me), &me, &format!("r:{to}:{participant}:{id}:read-self"), n, my_e2e.as_deref());
        }
        let n = Node::new("receipt").attr("id", &id).attr("from", &to).attr("participant", &my_jid).attr("type", &typ).attr("t", t);
        deliver_to_user(hub, m.of(&sender), &sender, &format!("r:{to}:{my_jid}:{id}:{typ}"), n, None);
    }
}

fn on_ack(hub: &Arc<Hub>, s: &mut Session, node: &Node) {
    if node.get("class") != Some("receipt") {
        return;
    }
    let Some(owner) = s.peer.owner() else { return };
    let f = |k| node.get(k).unwrap_or("undefined");
    let key = format!("r:{}:{}:{}:{}", f("from"), node.get("participant").unwrap_or(""), f("id"), f("type"));
    let me = s.peer.user().unwrap_or_default();
    hub.user(&me).store.dequeue(&owner, &key);
}

// ---------------------------------------------------------------- presence / typing

fn on_presence(hub: &Arc<Hub>, s: &mut Session, node: &Node) {
    let typ = node.get("type").unwrap_or("available");
    let me = s.peer.user().unwrap_or_default();
    if typ == "subscribe" || typ == "unsubscribe" {
        let Some(target) = user_of(node.get("to")) else {
            return;
        };
        {
            let mut sh = hub.user(&target);
            if !sh.store.has_user(&target) {
                return;
            }
            let w = sh.watchers.entry(target.clone()).or_default();
            if typ == "subscribe" {
                if !w.iter().any(|p| p.sid == s.peer.sid) {
                    w.push(s.peer.clone());
                }
            } else {
                w.retain(|p| p.sid != s.peer.sid);
            }
        }
        if typ == "subscribe" {
            s.subscribed_to.insert(target.clone());
            s.peer.send(presence_of(hub, &target, &me));
        } else {
            s.subscribed_to.remove(&target);
        }
        return;
    }
    if typ == "available" || typ == "unavailable" {
        let available = typ == "available";
        s.peer.info().available = available;
        if !available {
            hub.user(&me).store.set_last_seen(&me);
        }
        broadcast_presence(hub, &me);
    }
}

/// May `viewer` see `owner`'s "last" (seen), "online", "photo" or "about"?
/// `sh` is `owner`'s shard (privacy settings and contacts live there).
fn can_see(sh: &mut Shard, owner: &str, viewer: &str, what: &str) -> bool {
    if owner == viewer {
        return true;
    }
    let p = sh.store.privacy_of(owner);
    let level = match what {
        "online" if p.online == "match" => p.last,
        "online" => p.online,
        "last" => p.last,
        "photo" => p.photo,
        _ => p.about,
    };
    match level.as_str() {
        "all" => true,
        "contacts" => sh.store.is_contact(owner, viewer),
        _ => false,
    }
}

/// What `viewer` may see of `user`'s presence: online if any connection
/// says the app is open, else last seen (privacy settings applied).
fn presence_of(hub: &Arc<Hub>, user: &str, viewer: &str) -> Node {
    let (online, last) = {
        let mut sh = hub.user(user);
        let online = sh.connections.get(user).is_some_and(|c| c.iter().any(|p| p.available())) && can_see(&mut sh, user, viewer, "online");
        let last = if can_see(&mut sh, user, viewer, "last") { sh.store.last_seen.get(user).copied() } else { None };
        (online, last)
    };
    if online {
        return Node::new("presence").attr("from", user_jid(user)).attr("type", "available");
    }
    // Like WhatsApp: who hides their own last seen sees nobody's. (The
    // viewer's settings live in the viewer's shard: a second, separate lock.)
    let reciprocal = viewer == user || hub.user(viewer).store.privacy_of(viewer).last != "none";
    let last = last.filter(|&l| l > 0 && reciprocal).map(|l| l / 1000);
    Node::new("presence").attr("from", user_jid(user)).attr("type", "unavailable").opt("lastseen", last)
}

fn broadcast_presence(hub: &Arc<Hub>, user: &str) {
    let watchers = hub.user(user).watchers.get(user).cloned().unwrap_or_default();
    for w in watchers {
        let viewer = w.user().unwrap_or_default();
        w.send(presence_of(hub, user, &viewer));
    }
}

fn send_to_user(hub: &Arc<Hub>, user: &str, node: &Node) {
    let peers = hub.user(user).connections.get(user).cloned().unwrap_or_default();
    for p in peers {
        p.send(node.clone());
    }
}

/// Typing indicators are fire-and-forget: never queued.
fn on_chat_state(hub: &Arc<Hub>, s: &mut Session, node: &Node) {
    let Some((tlocal, tserver)) = parse_jid(node.get("to")) else {
        return;
    };
    let Some(state) = node.first().map(|c| c.tag.clone()).filter(|t| t == "composing" || t == "paused") else {
        return;
    };
    let content = vec![Node::new(&state)];
    let me = s.peer.user().unwrap_or_default();
    let my_jid = user_jid(&me);
    if tserver == USER_SERVER {
        if tlocal == me {
            return;
        }
        send_to_user(hub, &tlocal, &Node::new("chatstate").attr("from", my_jid).nodes(content));
    } else if tserver == GROUP_SERVER {
        let members = {
            let sh = hub.user(&tlocal);
            let Some(g) = sh.store.get_group(&tlocal) else {
                return;
            };
            if !g.members.contains_key(&me) {
                return;
            }
            g.members.keys().cloned().collect::<Vec<_>>()
        };
        let n = Node::new("chatstate").attr("from", node.get("to").unwrap()).attr("participant", my_jid).nodes(content);
        for m in members.iter().filter(|m| **m != me) {
            send_to_user(hub, m, &n);
        }
    }
}

// ---------------------------------------------------------------- iq

fn on_iq(hub: &Arc<Hub>, s: &mut Session, node: &Node) {
    let id = node.get("id").unwrap_or("").to_string();
    if !is_id(&id) {
        return;
    }
    let result = match node.get("xmlns").unwrap_or("") {
        "w:p" => Ok(None), // ping
        "status" => iq_status(hub, s, node),
        "media" => iq_media(hub, s),
        "g" => iq_group(hub, s, node),
        "devices" => iq_devices(hub, s, node),
        "push" => iq_push(hub, s, node),
        "privacy" => iq_privacy(hub, s, node),
        "md" => iq_link(hub, s, node),
        "encrypt" => iq_encrypt(hub, s, node),
        "profile" => iq_profile(hub, s, node),
        other => Err(serr_t("bad-request", format!("unknown xmlns {other}"))),
    };
    let reply = match result {
        Ok(content) => {
            let mut n = Node::new("iq").attr("id", &id).attr("type", "result");
            n.content = content.map(Content::Nodes);
            n
        }
        Err(e) => {
            let text = e.text.unwrap_or_else(|| e.code.to_string());
            Node::new("iq").attr("id", &id).attr("type", "error").nodes(vec![Node::new("error").attr("code", e.code).attr("text", text)])
        }
    };
    s.peer.send(reply);
}

/// get: the account's devices.  set <remove key/>: unlink one. The primary
/// can remove any companion; a companion can remove itself (log out).
fn iq_devices(hub: &Arc<Hub>, s: &mut Session, node: &Node) -> IqResult {
    let user = s.peer.user().unwrap_or_default();
    let my_key = s.peer.device_key.clone();
    if let Some(op) = node.first().filter(|op| node.get("type") == Some("set") && op.tag == "remove") {
        let key = op.get("key").filter(|k| !k.is_empty()).unwrap_or(&my_key).to_string();
        let mut sh = hub.user(&user);
        let Some(target_no) = sh.store.device(&user, &key).map(|d| d.no) else {
            return Err(serr("item-not-found"));
        };
        if key != my_key && s.peer.device_no() != Some(0) && s.peer.kind() == "phone" {
            return Err(serr_t("forbidden", "only the primary phone can unlink other devices"));
        }
        if target_no == 0 && key != my_key {
            return Err(serr_t("forbidden", "the primary phone can not be unlinked"));
        }
        if key == my_key {
            // Logging out this device: answer first, then close it.
            let hub2 = hub.clone();
            tokio::spawn(async move {
                let mut sh = hub2.user(&user);
                drop_device(&mut sh, &user, &key, "device-removed");
            });
        } else {
            drop_device(&mut sh, &user, &key, "device-removed");
        }
        return Ok(None);
    }
    let list = hub.user(&user).store.list_devices(&user);
    Ok(Some(
        list.into_iter()
            .map(|(k, d)| {
                Node::new("device").attr("key", &k).attr("no", d.no).attr("name", d.name).attr("platform", d.platform).attr("t", d.used / 1000).opt("current", (k == my_key).then_some("true"))
            })
            .collect(),
    ))
}

/// Which of `users` exist, each checked in its own shard (one lock per shard).
fn existing(hub: &Arc<Hub>, users: Vec<String>) -> Vec<String> {
    let mut by_shard: HashMap<usize, Vec<usize>> = HashMap::new();
    for (i, u) in users.iter().enumerate() {
        by_shard.entry(shard_of(u)).or_default().push(i);
    }
    let mut keep = vec![false; users.len()];
    for (shard, idx) in by_shard {
        let sh = hub.shard(shard);
        for i in idx {
            keep[i] = sh.store.has_user(&users[i]);
        }
    }
    users.into_iter().zip(keep).filter(|(_, k)| *k).map(|(u, _)| u).collect()
}

/// Privacy, like WhatsApp's:
///   get                                -> <privacy last online photo about receipts/>
///   set <privacy last? online? photo? about? receipts?/>
///   set <contacts><user jid/>…</contacts>  my contacts on xb-chat -> <contacts count/>
fn iq_privacy(hub: &Arc<Hub>, s: &mut Session, node: &Node) -> IqResult {
    let user = s.peer.user().unwrap_or_default();
    let show = || {
        let p = hub.user(&user).store.privacy_of(&user);
        Ok(Some(vec![Node::new("privacy").attr("last", p.last).attr("online", p.online).attr("photo", p.photo).attr("about", p.about).attr("receipts", p.receipts)]))
    };
    if node.get("type") != Some("set") {
        return show();
    }
    if let Some(contacts) = node.child("contacts") {
        let users: Vec<String> = contacts.children("user").filter_map(|u| user_of(u.get("jid"))).collect();
        if users.len() > CONTACTS_MAX {
            return Err(serr_t("bad-request", format!("at most {CONTACTS_MAX} contacts")));
        }
        let users = existing(hub, users);
        let count = hub.user(&user).store.set_contacts(&user, users);
        broadcast_presence(hub, &user); // "My contacts" may now be different people
        return Ok(Some(vec![Node::new("contacts").attr("count", count)]));
    }
    let p = node.child("privacy").ok_or(serr("bad-request"))?;
    let mut changes = StoredPrivacy::default();
    for k in ["last", "photo", "about"] {
        let Some(v) = p.get(k) else { continue };
        if !["all", "contacts", "none"].contains(&v) {
            return Err(serr_t("bad-request", format!("{k}: all, contacts or none")));
        }
        let v = Some(v.to_string());
        match k {
            "last" => changes.last = v,
            "photo" => changes.photo = v,
            _ => changes.about = v,
        }
    }
    if let Some(v) = p.get("online") {
        if v != "all" && v != "match" {
            return Err(serr_t("bad-request", "online: all or match"));
        }
        changes.online = Some(v.to_string());
    }
    if let Some(v) = p.get("receipts") {
        changes.receipts = Some(v == "true");
    }
    hub.user(&user).store.set_privacy(&user, changes);
    broadcast_presence(hub, &user); // watchers see the new rules at once
    show()
}

/// set <token platform="fcm">TOKEN</token>: wake this device with pushes.
/// set <remove/>: stop. Only for devices the server remembers.
fn iq_push(hub: &Arc<Hub>, s: &mut Session, node: &Node) -> IqResult {
    if node.get("type") != Some("set") || s.peer.device_no().is_none() {
        return Err(serr("bad-request"));
    }
    let (user, key) = (s.peer.user().unwrap_or_default(), s.peer.device_key.clone());
    if node.child("remove").is_some() {
        hub.user(&user).store.set_push(&user, &key, None);
        return Ok(None);
    }
    let t = node.child("token");
    let token = t.map(|t| t.text()).unwrap_or_default().trim().to_string();
    let valid = (20..=4096).contains(&token.len()) && token.bytes().all(|b| b.is_ascii_alphanumeric() || b"_:.-".contains(&b));
    if t.and_then(|t| t.get("platform")) != Some("fcm") || !valid {
        return Err(serr_t("bad-request", "token: platform=\"fcm\" and the FCM registration token"));
    }
    hub.user(&user).store.set_push(&user, &key, Some(PushRec { platform: "fcm".into(), token, at: now_ms() }));
    Ok(None)
}

/// Profile, like WhatsApp's: name, "About" and photo.
///   set <name/> <about/> <picture><image/><preview/></picture>  (<picture/> removes it)
///   get <user jid/>…                     -> <user jid exists name? about? picture?/>…
///   get <picture jid type="preview|image"/> -> <picture jid id type>JPEG</picture>
fn iq_profile(hub: &Arc<Hub>, s: &mut Session, node: &Node) -> IqResult {
    let me = s.peer.user().unwrap_or_default();
    if node.get("type") == Some("set") {
        let (n, a, p) = (node.child("name"), node.child("about"), node.child("picture"));
        if n.is_none() && a.is_none() && p.is_none() {
            return Err(serr("bad-request"));
        }
        let mut changed: Vec<(&str, String)> = Vec::new();
        let mut result = None;
        {
            let mut sh = hub.user(&me);
            if let Some(n) = n {
                let name = n.text().trim().to_string();
                if name.is_empty() {
                    return Err(serr_t("bad-request", "empty name"));
                }
                sh.store.set_name(&me, &name);
                changed.push(("name", sh.store.name_of(&me).unwrap_or_default()));
            }
            if let Some(a) = a {
                let about = a.text().trim().to_string();
                if js_len(&about) > 139 {
                    return Err(serr_t("bad-request", "about is 139 characters at most"));
                }
                sh.store.set_about(&me, &about);
                changed.push(("about", about));
            }
            if let Some(p) = p {
                let image = p.child("image").and_then(|c| c.content.as_ref());
                let id = match image {
                    None => sh.store.set_picture(&me, None).map_err(|_| serr("internal-error"))?,
                    Some(img) => {
                        let img = match img {
                            Content::Bytes(b) => Some(b.as_slice()),
                            _ => None,
                        };
                        let preview = p.child("preview").and_then(|c| c.content_bytes());
                        if !is_jpeg(img) || img.unwrap().len() > PICTURE_MAX {
                            return Err(serr_t("bad-request", "picture: a JPEG up to 300 KB"));
                        }
                        if !is_jpeg(preview) || preview.unwrap().len() > PREVIEW_MAX {
                            return Err(serr_t("bad-request", "preview: a JPEG up to 24 KB"));
                        }
                        sh.store.set_picture(&me, Some((img.unwrap(), preview.unwrap()))).map_err(|_| serr("internal-error"))?
                    }
                };
                changed.push(("picture", id.clone().unwrap_or_default()));
                result = Some(vec![Node::new("picture").opt("id", id.filter(|i| !i.is_empty()))]);
            }
        }
        notify_profile(hub, s, &changed);
        return Ok(result);
    }
    if let Some(pic) = node.child("picture") {
        let kind = if pic.get("type") == Some("image") { "image" } else { "preview" };
        let got = match user_of(pic.get("jid")) {
            Some(u) => {
                let mut sh = hub.user(&u);
                if can_see(&mut sh, &u, &me, "photo") { sh.store.get_picture(&u, kind).map(|g| (u.clone(), g)) } else { None }
            }
            None => None,
        };
        let Some((u, (id, bytes))) = got else {
            return Err(serr("item-not-found"));
        };
        return Ok(Some(vec![Node::new("picture").attr("jid", user_jid(&u)).attr("id", id).attr("type", kind).bytes(bytes)]));
    }
    // Contact discovery (a contact sync asks for the whole address book, in chunks).
    let users: Vec<&Node> = node.children("user").collect();
    if users.len() > LOOKUP_MAX {
        return Err(serr_t("bad-request", format!("at most {LOOKUP_MAX} numbers per request")));
    }
    let mut out = Vec::with_capacity(users.len());
    for u in users {
        let mut n = Node::new("user").attr("jid", u.get("jid").unwrap_or("undefined"));
        let local = user_of(u.get("jid"));
        let mut exists = false;
        if let Some(l) = &local {
            let mut sh = hub.user(l);
            if let Some((name, about, picture)) = sh.store.profile_of(l) {
                exists = true;
                n = n.attr("exists", true).opt("name", name);
                if can_see(&mut sh, l, &me, "about") {
                    n = n.opt("about", about);
                }
                if can_see(&mut sh, l, &me, "photo") {
                    n = n.opt("picture", picture);
                }
            }
        }
        if !exists {
            n = n.attr("exists", false);
        }
        out.push(n);
    }
    Ok(Some(out))
}

/// Tells the people likely to be showing this profile that it changed: my
/// other devices, whoever watches my presence and my groups' members who
/// are online. Not queued: offline clients refresh when they next look.
fn notify_profile(hub: &Arc<Hub>, s: &mut Session, changed: &[(&str, String)]) {
    let me = s.peer.user().unwrap_or_default();
    let my_jid = user_jid(&me);
    // Who: my shard first (my connections, my watchers, my groups) …
    let (mut to, groups) = {
        let sh = hub.user(&me);
        let mut to: Vec<Arc<Peer>> = sh.connections.get(&me).cloned().unwrap_or_default().into_iter().filter(|p| p.sid != s.peer.sid).collect();
        to.extend(sh.watchers.get(&me).cloned().unwrap_or_default());
        (to, sh.store.groups_of(&me))
    };
    // … then each group's members, then their connections.
    let mut members: Vec<String> = Vec::new();
    for g in groups {
        if let Some(gr) = hub.user(&g).store.get_group(&g) {
            members.extend(gr.members.keys().filter(|m| **m != me).cloned());
        }
    }
    for m in members {
        to.extend(hub.user(&m).connections.get(&m).cloned().unwrap_or_default());
    }
    let mut seen = HashSet::new();
    to.retain(|p| seen.insert(p.sid));
    // What each one may see: my privacy settings, in my shard again.
    let mut may: HashMap<String, (bool, bool)> = HashMap::new();
    {
        let mut sh = hub.user(&me);
        for p in &to {
            let viewer = p.user().unwrap_or_default();
            if !may.contains_key(&viewer) {
                let v = (can_see(&mut sh, &me, &viewer, "about"), can_see(&mut sh, &me, &viewer, "photo"));
                may.insert(viewer, v);
            }
        }
    }
    for p in to {
        let (about_ok, photo_ok) = may[&p.user().unwrap_or_default()];
        let mut n = Node::new("notification").attr("type", "profile").attr("from", &my_jid);
        for (k, v) in changed {
            // Each one only hears about what they may see.
            if (*k == "about" && !about_ok) || (*k == "picture" && !photo_ok) {
                continue;
            }
            n = n.attr(k, v);
        }
        p.send(n);
    }
}

/// End-to-end keys; the server only stores and hands out public keys.
///   set <keys device><identity/><signing/><skey id><value/><signature/></skey><list><key id/>…</list></keys>
///   set <remove device/>
///   get <count device/>                  -> <count device value registered/>
///   get <devices><user jid/>…</devices>  -> <devices><user jid><device id/>…</user>…</devices>
///   get <key><user jid device?/>…</key>  -> <list><user jid><device id>bundle</device>…</user>…</list>
fn iq_encrypt(hub: &Arc<Hub>, s: &mut Session, node: &Node) -> IqResult {
    let op = node.first().ok_or(serr("bad-request"))?;
    let b64 = |n: Option<&Node>, len: usize| n.and_then(|n| n.content_bytes()).filter(|b| b.len() == len).map(|b| B64.encode(b));
    let device = op.get("device").unwrap_or("").to_string();
    let need_device = || {
        if is_num_id(&device) { Ok(()) } else { Err(serr_t("bad-request", "bad device id")) }
    };
    let me = s.peer.user().unwrap_or_default();

    if node.get("type") == Some("set") {
        if op.tag == "keys" {
            need_device()?;
            let mut keys = Vec::new();
            for k in op.child("list").map(|l| l.children("key").collect::<Vec<_>>()).unwrap_or_default() {
                let id = k.get("id").unwrap_or("");
                match b64(Some(k), 32) {
                    Some(public) if is_num_id(id) => keys.push((id.to_string(), public)),
                    _ => return Err(serr_t("bad-request", "bad pre-key")),
                }
            }
            let mut identity = None;
            if op.child("identity").is_some() {
                let skey = op.child("skey");
                let sk = SignedKey {
                    id: skey.and_then(|s| s.get("id")).unwrap_or("").to_string(),
                    value: b64(skey.and_then(|s| s.child("value")), 32).unwrap_or_default(),
                    sig: b64(skey.and_then(|s| s.child("signature")), 64).unwrap_or_default(),
                };
                let (ident, signing) = (b64(op.child("identity"), 32), b64(op.child("signing"), 32));
                match (ident, signing) {
                    (Some(i), Some(s)) if is_num_id(&sk.id) && !sk.value.is_empty() && !sk.sig.is_empty() => identity = Some((i, s, sk)),
                    _ => return Err(serr_t("bad-request", "bad identity")),
                }
            }
            let mut sh = hub.user(&me);
            if !sh.store.set_keys(&me, &device, identity, keys) {
                return Err(serr_t("item-not-found", "unknown device"));
            }
            bind_device(hub, &mut sh, &s.peer, &device);
            return Ok(None);
        }
        if op.tag == "remove" {
            need_device()?;
            let mut sh = hub.user(&me);
            if !sh.store.remove_keys(&me, &device) {
                return Err(serr("item-not-found"));
            }
            sh.store.drop_queue(&format!("{me}/{device}"));
            return Ok(None);
        }
        return Err(serr_t("bad-request", format!("unknown encrypt op {}", op.tag)));
    }

    // <user jid device?/> children -> (user, device), each checked to exist.
    let users = || -> Result<Vec<(String, Option<String>)>, SErr> {
        let mut out = Vec::new();
        for u in op.children("user") {
            let local = user_of(u.get("jid")).ok_or(serr("bad-request"))?;
            if !hub.user(&local).store.has_user(&local) {
                return Err(serr_t("item-not-found", format!("unknown user {}", u.get("jid").unwrap_or(""))));
            }
            out.push((local, u.get("device").map(str::to_string)));
        }
        Ok(out)
    };
    match op.tag.as_str() {
        "count" => {
            need_device()?;
            let mut sh = hub.user(&me);
            let (registered, count) = sh.store.key_count(&me, &device);
            // A registered device names itself here at login: its queue can go out.
            if registered {
                bind_device(hub, &mut sh, &s.peer, &device);
            }
            Ok(Some(vec![Node::new("count").attr("device", &device).attr("value", count).attr("registered", registered)]))
        }
        "devices" => {
            let list = users()?
                .into_iter()
                .map(|(u, _)| {
                    let ids = hub.user(&u).store.key_devices(&u);
                    Node::new("user").attr("jid", user_jid(&u)).nodes(ids.into_iter().map(|id| Node::new("device").attr("id", id)).collect())
                })
                .collect();
            Ok(Some(vec![Node::new("devices").nodes(list)]))
        }
        "key" => {
            let mut out: IndexMap<String, Vec<Node>> = IndexMap::new();
            for (u, dev) in users()? {
                let mut sh = hub.user(&u);
                let ids = match dev {
                    Some(d) => vec![d],
                    None => sh.store.key_devices(&u),
                };
                for id in ids {
                    let Some(b) = sh.store.take_bundle(&u, &id) else {
                        continue;
                    };
                    let raw = |s: &str| B64.decode(s).unwrap_or_default();
                    let mut content = vec![
                        Node::new("identity").bytes(raw(&b.identity)),
                        Node::new("signing").bytes(raw(&b.signing)),
                        Node::new("skey").attr("id", &b.skey.id).nodes(vec![Node::new("value").bytes(raw(&b.skey.value)), Node::new("signature").bytes(raw(&b.skey.sig))]),
                    ];
                    if let Some((kid, public)) = &b.key {
                        content.push(Node::new("key").attr("id", kid).bytes(raw(public)));
                    }
                    out.entry(user_jid(&u)).or_default().push(Node::new("device").attr("id", id).nodes(content));
                }
            }
            Ok(Some(vec![Node::new("list").nodes(out.into_iter().map(|(jid, devs)| Node::new("user").attr("jid", jid).nodes(devs)).collect())]))
        }
        other => Err(serr_t("bad-request", format!("unknown encrypt op {other}"))),
    }
}

fn iq_status(hub: &Arc<Hub>, s: &mut Session, node: &Node) -> IqResult {
    let target = user_of(node.child("lastseen").and_then(|q| q.get("jid"))).filter(|u| hub.user(u).store.has_user(u)).ok_or(serr("item-not-found"))?;
    let viewer = s.peer.user().unwrap_or_default();
    let p = presence_of(hub, &target, &viewer);
    Ok(Some(vec![Node::new("lastseen").attr("jid", p.get("from").unwrap()).attr("type", p.get("type").unwrap()).opt("t", p.get("lastseen"))]))
}

fn iq_media(hub: &Arc<Hub>, s: &mut Session) -> IqResult {
    let media = hub.media.as_ref().ok_or(serr_t("item-not-found", "media server disabled"))?;
    let (token, expires) = media.tokens.issue(&s.peer.user().unwrap_or_default());
    Ok(Some(vec![Node::new("media_conn").attr("host", &media.public_url).attr("auth_token", token).attr("ttl", (expires - now_ms()) / 1000)]))
}

// ---------------------------------------------------------------- groups
//
// A group lives in the shard of its id; each member's shard keeps an index
// entry (Store::member_of) so "my groups" doesn't scan every group.

fn group_node(hub: &Arc<Hub>, id: &str) -> Option<Node> {
    let sh = hub.user(id);
    let g = sh.store.get_group(id)?;
    Some(
        Node::new("group")
            .attr("jid", group_jid(id))
            .attr("subject", &g.subject)
            .attr("creator", user_jid(&g.creator))
            .attr("t", g.created / 1000)
            .nodes(g.members.iter().map(|(u, role)| Node::new("participant").attr("jid", user_jid(u)).attr("type", role)).collect()),
    )
}

/// <participant jid/> children -> known users.
fn participants(hub: &Arc<Hub>, node: &Node) -> Result<Vec<String>, SErr> {
    let mut users = Vec::new();
    for p in node.children("participant") {
        match user_of(p.get("jid")).filter(|u| hub.user(u).store.has_user(u)) {
            Some(u) => users.push(u),
            None => {
                return Err(serr_t("item-not-found", format!("unknown participant {}", p.get("jid").unwrap_or("undefined"))));
            }
        }
    }
    Ok(users)
}

fn require_group(hub: &Arc<Hub>, me: &str, node: &Node, admin: bool) -> Result<String, SErr> {
    let (local, server) = parse_jid(node.get("jid")).ok_or(serr("item-not-found"))?;
    if server != GROUP_SERVER {
        return Err(serr("item-not-found"));
    }
    let sh = hub.user(&local);
    let g = sh.store.get_group(&local).ok_or(serr("item-not-found"))?;
    let role = g.members.get(me).ok_or(serr("not-member"))?;
    if admin && role != "admin" {
        return Err(serr_t("forbidden", "admin only"));
    }
    Ok(local)
}

/// Group membership changed: update the members' index entries (each in its shard).
fn reindex(hub: &Arc<Hub>, id: &str, added: &[String], removed: &[String]) {
    for u in added {
        hub.user(u).store.index_member(u, id);
    }
    for u in removed {
        hub.user(u).store.unindex_member(u, id);
    }
}

fn iq_group(hub: &Arc<Hub>, s: &mut Session, node: &Node) -> IqResult {
    let op = node.first().ok_or(serr("bad-request"))?;
    let me = s.peer.user().unwrap_or_default();
    match op.tag.as_str() {
        "create" => {
            let subject = js_slice(op.get("subject").unwrap_or(""), 100);
            if subject.is_empty() {
                return Err(serr_t("bad-request", "subject required"));
            }
            let members = participants(hub, op)?;
            if members.len() > 1023 {
                return Err(serr_t("bad-request", "group too large"));
            }
            let id = hub.user(&me).store.new_group_id();
            let all: Vec<String> = {
                let mut sh = hub.user(&id);
                sh.store.create_group(&id, &me, &subject, &members);
                sh.store.get_group(&id).map(|g| g.members.keys().cloned().collect()).unwrap_or_default()
            };
            reindex(hub, &id, &all, &[]);
            log("group", format!("{me} created {id} ({} members)", members.len() + 1));
            Ok(group_node(hub, &id).map(|n| vec![n]))
        }
        "add" => {
            let id = require_group(hub, &me, op, true)?;
            let users = participants(hub, op)?;
            hub.user(&id).store.add_members(&id, &users);
            reindex(hub, &id, &users, &[]);
            Ok(group_node(hub, &id).map(|n| vec![n]))
        }
        "remove" => {
            let id = require_group(hub, &me, op, true)?;
            let users = participants(hub, op)?;
            for u in &users {
                hub.user(&id).store.remove_member(&id, u);
            }
            reindex(hub, &id, &[], &users);
            Ok(group_node(hub, &id).map(|n| vec![n]))
        }
        "leave" => {
            let id = require_group(hub, &me, op, false)?;
            hub.user(&id).store.remove_member(&id, &me);
            reindex(hub, &id, &[], std::slice::from_ref(&me));
            Ok(None)
        }
        "query" => {
            let id = require_group(hub, &me, op, false)?;
            Ok(group_node(hub, &id).map(|n| vec![n]))
        }
        "list" => {
            let ids = hub.user(&me).store.groups_of(&me);
            Ok(Some(ids.iter().filter_map(|id| group_node(hub, id)).collect()))
        }
        other => Err(serr_t("bad-request", format!("unknown group op {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn users_spread_evenly_over_shards() {
        let mut count = [0usize; SHARDS];
        for i in 0..64_000 {
            count[shard_of(&format!("91{:010}", 9_800_000_000u64 + i))] += 1;
        }
        // 1,000 each on average; no shard more than 15% off.
        assert!(count.iter().all(|&c| (850..=1150).contains(&c)), "{count:?}");
        assert_eq!(shard_of("919812345678"), shard_of("919812345678"), "stable");
    }
}

/// A real hub with three users, keys and a group, and sessions that feed it
/// stanzas directly: for the stanza fuzzer (src/fuzz.rs).
#[cfg(test)]
pub(crate) mod rig {
    use super::*;

    pub const USERS: [&str; 3] = ["919800000001", "919800000002", "919800000003"];
    pub const E2E: [&str; 3] = ["11", "22", "33"];

    pub struct Rig {
        pub hub: Arc<Hub>,
        pub group: String,
        sessions: Vec<Session>,
        _dir: std::path::PathBuf,
    }

    fn b64(n: usize) -> String {
        B64.encode(vec![7u8; n])
    }

    fn session(hub: &Arc<Hub>) -> Session {
        let (tx, _rx) = unbounded_channel();
        let peer = Arc::new(Peer {
            sid: hub.next_sid.fetch_add(1, Ordering::Relaxed),
            tx,
            queued: std::sync::atomic::AtomicUsize::new(0),
            queue_max: usize::MAX / 2, // no writer drains it here
            kill: CancellationToken::new(),
            hub_slow: hub.stats.slow.clone(),
            backlog: std::sync::atomic::AtomicBool::new(false),
            drained: tokio::sync::Notify::new(),
            device_key: B64.encode(rand::random::<[u8; 32]>()),
            info: Mutex::new(PeerInfo::default()),
        });
        Session { peer, auth_failures: 0, limiter: RateLimiter { rate: 1e9, burst: 1e9, tokens: 1e9, last: Instant::now() }, subscribed_to: HashSet::new() }
    }

    /// Logged in as USERS[i], device 0, end-to-end device E2E[i], through the real login path.
    fn logged_in(hub: &Arc<Hub>, i: usize) -> Session {
        let s = session(hub);
        let user = USERS[i];
        let dev = {
            let mut sh = hub.user(user);
            for (k, _) in sh.store.list_devices(user) {
                drop_device(&mut sh, user, &k, "device-removed");
            }
            sh.store.add_device(user, &s.peer.device_key, "Phone", "android", true);
            let sk = SignedKey { id: "1".into(), value: b64(32), sig: b64(64) };
            sh.store.set_keys(user, E2E[i], Some((b64(32), b64(32), sk)), (1..=5).map(|k| (k.to_string(), b64(32))).collect());
            sh.store.set_device_e2e(user, &s.peer.device_key, E2E[i]);
            sh.store.use_device(user, &s.peer.device_key)
        };
        login(hub, &s.peer, user, dev);
        s
    }

    pub fn new() -> Rig {
        let dir = std::env::temp_dir().join(format!("xb-fuzz-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Arc::new(crate::db::Db::open(&dir.join("xb.redb")).unwrap());
        let mut store = Store::new(Some(dir.clone()), crate::store::DEFAULT_OFFLINE_TTL_MS);
        for u in USERS {
            store.ensure_phone_account(u, Some("Fuzz"));
        }
        let group = store.new_group_id();
        store.create_group(&group, USERS[0], "Fuzz group", &[USERS[1].to_string(), USERS[2].to_string()]);
        let cfg = Config { send_queue_max: usize::MAX / 2, login_rate: 1e9, login_burst: 1e9, otp_echo: true, ..Config::default() };
        let hub = Hub::new(store, db, cfg, None, None, [9u8; 32], [8u8; 32]);
        let sessions = vec![logged_in(&hub, 0), logged_in(&hub, 1), logged_in(&hub, 2), session(&hub)];
        Rig { hub, group, sessions, _dir: dir }
    }

    impl Rig {
        /// One stanza from session `who` (0-2 logged in, 3 not yet). A session the
        /// server closed is replaced by a fresh one, so the fuzzer keeps going.
        pub async fn feed(&mut self, who: usize, node: Node) {
            if self.sessions[who].peer.closed() {
                on_close(&self.hub, &mut self.sessions[who]);
                self.sessions[who] = if who < 3 { logged_in(&self.hub, who) } else { session(&self.hub) };
            }
            on_node(&self.hub, &mut self.sessions[who], node).await;
        }
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self._dir);
        }
    }
}
