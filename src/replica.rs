//! Replica pairs: two servers, one serving (the primary), one keeping a live
//! copy of everything on its own disk (the standby). If the primary's
//! machine dies, the standby takes over within seconds with everything it
//! had acknowledged, even stanzas that were still only in the primary's
//! memory (write-back). An upgrade drains one while the other serves.
//!
//! Both are started the same way (XB_PEER = the other's replication
//! address); who serves is decided at startup:
//!
//!   peer is serving              -> follow it (copy everything, then stream)
//!   peer is a synced standby     -> it has the newer data (ours may have lost
//!                                   its write-back window in a crash): tell it
//!                                   to take over, then follow it
//!   peer is starting too         -> the lower XB_NODE_ID serves
//!   peer unreachable             -> serve
//!
//! So a node that comes back after a crash never serves stale data: it
//! rejoins as the standby.
//!
//! What travels (primary -> standby), as stanzas on an encrypted link:
//!   keys         the server's Noise key and token secret (same identity)
//!   r            one record written or deleted (exactly what goes to redb)
//!   q / qd       a stanza queued / gone from an offline queue
//!   p            a profile picture file written or deleted
//!   synced       the first full copy is complete; changes follow
//!   ping         every 2 s, so a silent primary is noticed
//!   handover     the primary is draining: take over now
//! and back: ack (how much is applied; h="1" after a handover).
//!
//! The link: a shared secret (XB_REPL_SECRET). Each side sends 32 random
//! bytes; each direction's AES-256-GCM key is SHA-256 of the secret and both
//! nonces, so a peer without the secret can't read or forge anything.
//!
//! Replication is asynchronous: a stanza is acknowledged to its sender before
//! the standby has it. The standby is normally a fraction of a millisecond
//! behind (one LAN hop), so a crash loses at most that, against the primary's
//! own disk which can be 5 s behind (write-back).

use crate::codec::Node;
use crate::db::{Db, Op};
use crate::frame::FrameDecoder;
use crate::queue_db::{QueueDb, Write as QWrite};
use crate::server::Hub;
use crate::transport::Cipher;
use crate::util::log;
use indexmap::IndexMap;
use sha2::{Digest, Sha256};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

pub const MAX_NODE: usize = 32 * 1024 * 1024;
pub const PING: Duration = Duration::from_secs(2);
pub const SILENCE: Duration = Duration::from_secs(6);

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("replica: {e}"))
}

// ---------------------------------------------------------------- the stream (primary side)

/// Where the primary's changes go: the connected standby's send queue, or
/// nowhere (one atomic check) when there is none.
#[derive(Default)]
pub struct Sink {
    on: AtomicBool,
    tx: Mutex<Option<UnboundedSender<Vec<u8>>>>,
    /// Bytes waiting for the standby; past max_bytes it's dropped (it copies
    /// everything again when it reconnects) rather than grow without limit.
    queued: Arc<AtomicUsize>,
    max_bytes: AtomicUsize,
    /// Bumped for each standby that connects (so an old link's cleanup
    /// doesn't switch off a newer one).
    generation: AtomicU64,
    /// The standby confirmed a handover.
    handed_over: Notify,
}

impl Sink {
    pub fn active(&self) -> bool {
        self.on.load(Ordering::Acquire)
    }

    fn send(&self, node: &Node) {
        if !self.active() {
            return;
        }
        let bytes = crate::codec::encode(node);
        let n = bytes.len();
        let mut tx = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let Some(t) = tx.as_ref() else { return };
        if self.queued.fetch_add(n, Ordering::AcqRel) + n > self.max_bytes.load(Ordering::Relaxed) {
            log("replica", "standby too far behind: dropped (it will copy everything again)");
            *tx = None;
            self.on.store(false, Ordering::Release);
            return;
        }
        if t.send(bytes).is_err() {
            *tx = None;
            self.on.store(false, Ordering::Release);
        }
    }

    /// Records about to be saved (db::save, under the shard's lock, so the
    /// standby sees them in the order they happened).
    pub fn ops(&self, ops: &[Op]) {
        if !self.active() {
            return;
        }
        for op in ops {
            self.send(&op_node(op));
        }
    }

    pub fn put(&self, owner: &str, key: &str, seq: u64, at: u64, frame: &[u8]) {
        if self.active() {
            self.send(&Node::new("q").attr("o", owner).attr("k", key).attr("s", seq).attr("a", at).bytes(frame.to_vec()));
        }
    }

    pub fn del(&self, owner: &str, key: &str) {
        if self.active() {
            self.send(&Node::new("qd").attr("o", owner).attr("k", key));
        }
    }

    pub fn picture(&self, name: &str, bytes: Option<&[u8]>) {
        if self.active() {
            let n = Node::new("p").attr("n", name);
            self.send(&match bytes {
                Some(b) => n.bytes(b.to_vec()),
                None => n.attr("del", "1"),
            });
        }
    }
}

fn op_node(op: &Op) -> Node {
    let r = |t: &str, k: &str| Node::new("r").attr("t", t).attr("k", k);
    let val = |n: Node, v: &Option<Vec<u8>>| match v {
        Some(v) => n.bytes(v.clone()),
        None => n.attr("del", "1"),
    };
    match op {
        Op::User(k, v) => val(r("user", k), v),
        Op::Devices(k, v) => val(r("dev", k), v),
        Op::Group(k, v) => val(r("group", k), v),
        Op::Prekey(u, d, v) => val(r("pk", u).attr("d", d), v),
        Op::LastSeen(k, Some(t)) => r("seen", k).attr("n", t),
        Op::LastSeen(k, None) => r("seen", k).attr("del", "1"),
        Op::Meta(n) => r("meta", "group_seq").attr("n", n),
    }
}

fn node_op(n: &Node) -> Option<Op> {
    let k = n.get("k")?.to_string();
    let v = if n.get("del").is_some() { None } else { Some(n.content_bytes().unwrap_or_default().to_vec()) };
    let num = n.get("n").and_then(|x| x.parse::<u64>().ok());
    Some(match n.get("t")? {
        "user" => Op::User(k, v),
        "dev" => Op::Devices(k, v),
        "group" => Op::Group(k, v),
        "pk" => Op::Prekey(k, n.get("d")?.to_string(), v),
        "seen" => Op::LastSeen(k, if n.get("del").is_some() { None } else { Some(num?) }),
        "meta" => Op::Meta(num?),
        _ => return None,
    })
}

// ---------------------------------------------------------------- the link

/// An encrypted link between two servers (a replica pair; a gateway and its
/// core, relay.rs).
pub struct Link {
    pub r: OwnedReadHalf,
    pub w: OwnedWriteHalf,
    pub dec: FrameDecoder,
    pub rx: Box<Cipher>,
    pub tx: Box<Cipher>,
}

fn link_key(secret: &str, dir: &str, a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"xb-repl\0");
    h.update(dir.as_bytes());
    h.update([0]);
    h.update(secret.as_bytes());
    h.update(a);
    h.update(b);
    h.finalize().into()
}

impl Link {
    /// `dialer`: we connected (we send our nonce first).
    pub async fn open(sock: TcpStream, secret: &str, dialer: bool) -> io::Result<Link> {
        let _ = sock.set_nodelay(true);
        let (mut r, mut w) = sock.into_split();
        let mine: [u8; 32] = rand::random();
        let mut theirs = [0u8; 32];
        let hello = async {
            if dialer {
                w.write_all(&mine).await?;
                r.read_exact(&mut theirs).await?;
            } else {
                r.read_exact(&mut theirs).await?;
                w.write_all(&mine).await?;
            }
            io::Result::Ok(())
        };
        tokio::time::timeout(Duration::from_secs(5), hello).await.map_err(|_| err("link handshake timed out"))??;
        let (d, a) = if dialer { (mine, theirs) } else { (theirs, mine) };
        let d2a = link_key(secret, "d2a", &d, &a);
        let a2d = link_key(secret, "a2d", &d, &a);
        let (tx, rx) = if dialer { (d2a, a2d) } else { (a2d, d2a) };
        Ok(Link { r, w, dec: FrameDecoder::new(MAX_NODE + 64), rx: Box::new(Cipher::new(&rx)), tx: Box::new(Cipher::new(&tx)) })
    }

    pub async fn dial(addr: &str, secret: &str) -> io::Result<Link> {
        let sock = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr)).await.map_err(|_| err("connect timed out"))??;
        Link::open(sock, secret, true).await
    }

    pub async fn send(&mut self, node: &Node) -> io::Result<()> {
        self.w.write_all(&self.tx.seal(node)).await
    }

    /// The next stanza; an error if none arrives within `wait`, the link
    /// closed, or it doesn't decrypt (wrong secret).
    pub async fn recv(&mut self, wait: Duration) -> io::Result<Node> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(f) = self.dec.next_frame().map_err(|e| err(format!("{e:?}")))? {
                return self.rx.open(&f, MAX_NODE);
            }
            let mut buf = vec![0u8; 64 * 1024];
            let n = tokio::time::timeout_at(deadline.into(), self.r.read(&mut buf)).await.map_err(|_| err("peer silent"))??;
            if n == 0 {
                return Err(err("link closed"));
            }
            self.dec.push(&buf[..n]);
        }
    }
}

/// Stanzas for a link, many at once: everything waiting in `rx` (after
/// `first`), up to about 1 MB, as ONE encrypted frame ("b"), so a busy link
/// costs one encryption and one write per batch, not per stanza.
pub fn seal_batch(tx: &mut Cipher, first: Node, rx: &mut tokio::sync::mpsc::UnboundedReceiver<Node>) -> Vec<u8> {
    let mut nodes = vec![first];
    let mut bytes = 0usize;
    while bytes < 1024 * 1024 {
        match rx.try_recv() {
            Ok(n) => {
                bytes += n.content_bytes().map_or(64, |b| b.len() + 64);
                nodes.push(n);
            }
            Err(_) => break,
        }
    }
    let n = if nodes.len() == 1 { nodes.pop().unwrap() } else { Node::new("b").nodes(nodes) };
    tx.seal_raw(&crate::codec::encode(&n))
}

/// What arrived on a link: a batch ("b") or a single stanza.
pub fn unbatch(n: Node) -> Vec<Node> {
    if n.tag == "b" {
        match n.content {
            Some(crate::codec::Content::Nodes(v)) => v,
            _ => Vec::new(),
        }
    } else {
        vec![n]
    }
}

// ---------------------------------------------------------------- the pair

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Role {
    Starting,
    Standby { synced: bool },
    Primary,
}

pub struct PairConfig {
    /// The other node's replication address.
    pub peer: String,
    /// Ours, where the other node reaches us.
    pub listen: String,
    pub secret: String,
    /// Breaks the tie when both start at once (the lower one serves).
    pub id: String,
    /// How long the primary may be gone before the standby takes over.
    pub failover: Duration,
    /// At startup, how long to look for the peer before serving alone.
    pub peer_wait: Duration,
    pub max_queue_bytes: usize,
}

pub struct Pair {
    pub cfg: PairConfig,
    role: Mutex<Role>,
    /// Told to take over (by a restarted peer, or a handover).
    promote: Notify,
    /// Set once this node serves.
    hub: OnceLock<Arc<Hub>>,
    pub sink: Arc<Sink>,
    data: PathBuf,
    /// Stanzas applied (standby), for the acks and the log.
    applied: AtomicU64,
}

impl Pair {
    pub fn new(cfg: PairConfig, data: &Path) -> Arc<Pair> {
        let sink = Arc::new(Sink::default());
        sink.max_bytes.store(cfg.max_queue_bytes, Ordering::Relaxed);
        Arc::new(Pair { cfg, role: Mutex::new(Role::Starting), promote: Notify::new(), hub: OnceLock::new(), sink, data: data.to_path_buf(), applied: AtomicU64::new(0) })
    }

    pub fn role(&self) -> Role {
        *self.role.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_role(&self, r: Role) {
        *self.role.lock().unwrap_or_else(|e| e.into_inner()) = r;
    }

    fn hi(&self) -> Node {
        let n = Node::new("hi").attr("id", &self.cfg.id);
        match self.role() {
            Role::Starting => n.attr("role", "starting"),
            Role::Standby { synced } => n.attr("role", "standby").attr("synced", synced as u8),
            Role::Primary => n.attr("role", "primary"),
        }
    }

    /// The replication port, from the start: answers who we are, takes
    /// "follow" (we serve: stream to it) and "promote" (we're the synced
    /// standby: take over).
    pub async fn listen(self: Arc<Self>) -> io::Result<()> {
        let l = TcpListener::bind(&self.cfg.listen).await?;
        log("replica", format!("replication on {} (peer {})", self.cfg.listen, self.cfg.peer));
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = l.accept().await else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                let me = self.clone();
                tokio::spawn(async move {
                    let Ok(mut link) = Link::open(sock, &me.cfg.secret, false).await else {
                        return;
                    };
                    let Ok(their) = link.recv(Duration::from_secs(5)).await else {
                        return;
                    }; // wrong secret ends here
                    if their.tag != "hi" || link.send(&me.hi()).await.is_err() {
                        return;
                    }
                    let Ok(cmd) = link.recv(Duration::from_secs(5)).await else {
                        return;
                    };
                    match cmd.tag.as_str() {
                        "follow" if me.role() == Role::Primary => {
                            if let Some(hub) = me.hub.get().cloned() {
                                me.lead(hub, link).await;
                            }
                        }
                        "promote" if me.role() == (Role::Standby { synced: true }) => {
                            log("replica", "the peer restarted and asks us to take over");
                            me.promote.notify_one();
                        }
                        _ => {}
                    }
                });
            }
        });
        Ok(())
    }

    /// At startup: serve, or follow the peer. Returns once this node is to
    /// serve; if it has to follow first, that happens in here (until it's
    /// promoted), with the data folder kept up to date.
    pub async fn decide(self: &Arc<Self>) -> io::Result<()> {
        let started = Instant::now();
        loop {
            match self.ask_peer().await {
                Some((role, synced, id, link)) => match role.as_str() {
                    "primary" => {
                        if self.follow(link).await? {
                            return Ok(()); // promoted
                        }
                        continue; // lost it before the first copy finished: ask again
                    }
                    "standby" if synced => {
                        // It has everything we had, and more recent than our
                        // disk: it serves, we follow.
                        log("replica", "the peer is a synced standby: asking it to take over");
                        let mut link = link;
                        let _ = link.send(&Node::new("promote")).await;
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                    "standby" => break, // it never finished copying from us: we serve
                    _ if self.cfg.id < id => break,
                    _ => tokio::time::sleep(Duration::from_millis(300)).await,
                },
                None if started.elapsed() >= self.cfg.peer_wait => break,
                None => tokio::time::sleep(Duration::from_millis(300)).await,
            }
        }
        log("replica", "serving (primary)");
        Ok(())
    }

    /// Connects to the peer and asks who it is.
    async fn ask_peer(&self) -> Option<(String, bool, String, Link)> {
        let mut link = Link::dial(&self.cfg.peer, &self.cfg.secret).await.ok()?;
        link.send(&self.hi()).await.ok()?;
        let n = link.recv(Duration::from_secs(5)).await.ok()?;
        if n.tag != "hi" {
            return None;
        }
        Some((n.get("role")?.to_string(), n.get("synced") == Some("1"), n.get("id").unwrap_or("").to_string(), link))
    }

    /// This node serves now (main.rs, once the hub exists).
    pub fn serving(&self, hub: Arc<Hub>) {
        let _ = self.hub.set(hub);
        self.set_role(Role::Primary);
    }

    /// Draining: tell the standby to take over, after everything sent so
    /// far; waits (a few seconds at most) for it to confirm.
    pub async fn hand_over(&self) -> bool {
        if !self.sink.active() {
            return false;
        }
        self.sink.send(&Node::new("handover"));
        tokio::time::timeout(Duration::from_secs(5), self.sink.handed_over.notified()).await.is_ok()
    }

    // ------------------------------------------------------------ primary: stream to the standby

    async fn lead(self: Arc<Self>, hub: Arc<Hub>, mut link: Link) {
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        // Keys first: the standby must be the same server to the clients.
        let mut keys = Node::new("keys");
        let read = |f: &str| std::fs::read(self.data.join(f)).unwrap_or_default();
        keys = keys.attr("secret", String::from_utf8_lossy(&read("secret")).trim()).bytes(read("server_key"));
        if link.send(&keys).await.is_err() {
            return;
        }
        // From here every change goes to the standby. A shard copied below
        // is copied under its lock: changes made before reach the queue
        // first, later ones after, so applying them in order is right.
        {
            let mut t = self.sink.tx.lock().unwrap_or_else(|e| e.into_inner());
            *t = Some(tx);
            self.sink.generation.fetch_add(1, Ordering::AcqRel);
            self.sink.queued.store(0, Ordering::Release);
            self.sink.on.store(true, Ordering::Release);
        }
        let generation = self.sink.generation.load(Ordering::Acquire);
        let t0 = Instant::now();
        let mut records = 0usize;
        for i in 0..hub.shard_count() {
            let sh = hub.shard(i);
            for op in crate::db::all_ops(&sh.store) {
                self.sink.send(&op_node(&op));
                records += 1;
            }
            for (owner, q) in &sh.store.offline {
                for (k, v) in q {
                    if let Some(f) = sh.store.queued_frame(owner, k, v) {
                        self.sink.put(owner, k, v.seq, v.at, &f);
                        records += 1;
                    }
                }
            }
        }
        if let Ok(dir) = std::fs::read_dir(self.data.join("pictures")) {
            for e in dir.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if let (Some(stem), Ok(bytes)) = (name.strip_suffix(".jpg"), std::fs::read(e.path())) {
                    self.sink.picture(stem, Some(&bytes));
                }
            }
        }
        self.sink.send(&Node::new("synced"));
        log("replica", format!("standby connected: copying {records} records and queued stanzas ({} ms to queue)", t0.elapsed().as_millis()));

        let queued = self.sink.queued.clone();
        let mut ping = tokio::time::interval(PING);
        let mut last_heard = Instant::now();
        let (mut r, mut w) = (link.r, link.w);
        let (mut dec, mut rxc, mut txc) = (link.dec, link.rx, link.tx);
        let mut buf = vec![0u8; 16 * 1024];
        let why = loop {
            tokio::select! {
                m = rx.recv() => {
                    let Some(bytes) = m else { break "replaced, or too far behind".to_string() };
                    let n = bytes.len();
                    if let Err(e) = w.write_all(&txc.seal_encoded(bytes)).await { break e.to_string() }
                    queued.fetch_sub(n, Ordering::AcqRel);
                }
                _ = ping.tick() => {
                    if last_heard.elapsed() > SILENCE { break "standby silent".into() }
                    if let Err(e) = w.write_all(&txc.seal(&Node::new("ping"))).await { break e.to_string() }
                }
                got = r.read(&mut buf) => {
                    match got {
                        Ok(0) | Err(_) => break "standby disconnected".into(),
                        Ok(n) => {
                            last_heard = Instant::now();
                            dec.push(&buf[..n]);
                            while let Ok(Some(f)) = dec.next_frame() {
                                if let Ok(a) = rxc.open(&f, MAX_NODE) {
                                    if a.tag == "ack" && a.get("h") == Some("1") {
                                        self.sink.handed_over.notify_one();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        log("replica", format!("standby gone ({why})"));
        // Stop queueing for it, unless a newer standby took its place meanwhile.
        let mut t = self.sink.tx.lock().unwrap_or_else(|e| e.into_inner());
        if self.sink.generation.load(Ordering::Acquire) == generation {
            *t = None;
            self.sink.on.store(false, Ordering::Release);
        }
    }

    // ------------------------------------------------------------ standby: apply the stream

    /// Follows the primary on `link`. Ok(true): promoted, the data folder
    /// is complete and closed, serve now. Ok(false): lost the primary before
    /// the first full copy (nothing usable): decide again.
    async fn follow(self: &Arc<Self>, mut link: Link) -> io::Result<bool> {
        link.send(&Node::new("follow")).await?;
        self.set_role(Role::Standby { synced: false });
        log("replica", format!("following {} (standby): copying everything", self.cfg.peer));
        let mut synced = false;
        loop {
            // A fresh copy: what this folder held is kept in previous/ (never
            // deleted, in case this was a mistake).
            let mut apply = Applier::open(&self.data)?;
            let res = self.apply_stream(&mut link, &mut apply, &mut synced).await;
            apply.flush().await?;
            drop(apply); // closes the files: the server opens them on promotion
            match res {
                Stream::Promote(why) => {
                    log("replica", format!("taking over ({why}); {} changes applied", self.applied.load(Ordering::Relaxed)));
                    return Ok(true);
                }
                Stream::Lost(why) if !synced => {
                    log("replica", format!("lost the primary before the copy finished ({why})"));
                    self.set_role(Role::Starting);
                    return Ok(false);
                }
                Stream::Lost(why) => {
                    log("replica", format!("lost the primary ({why}); taking over in {} s unless it comes back", self.cfg.failover.as_secs()));
                    match self.wait_for_primary().await {
                        Some(l) => {
                            // It's back (a network blip): copy everything again.
                            link = l;
                            link.send(&Node::new("follow")).await?;
                            self.set_role(Role::Standby { synced: false });
                            synced = false;
                            log("replica", "the primary is back: copying everything again");
                        }
                        None => {
                            log("replica", "the primary didn't come back: taking over");
                            return Ok(true);
                        }
                    }
                }
            }
        }
    }

    /// After losing the primary: Some(link) if it answers as primary again
    /// within the failover time; None to take over (time's up, or a
    /// restarted peer asked us to).
    async fn wait_for_primary(self: &Arc<Self>) -> Option<Link> {
        let until = Instant::now() + self.cfg.failover;
        while Instant::now() < until {
            tokio::select! {
                _ = self.promote.notified() => return None,
                found = self.ask_peer() => {
                    if let Some((role, _, _, link)) = found {
                        if role == "primary" {
                            return Some(link);
                        }
                    }
                }
            }
            tokio::select! {
                _ = self.promote.notified() => return None,
                _ = tokio::time::sleep(Duration::from_millis(300)) => {}
            }
        }
        None
    }

    async fn apply_stream(self: &Arc<Self>, link: &mut Link, apply: &mut Applier, synced: &mut bool) -> Stream {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            let got = tokio::select! {
                _ = self.promote.notified() => return Stream::Promote("asked by the restarted peer".into()),
                _ = tick.tick() => {
                    if apply.dirty() {
                        if let Err(e) = apply.flush().await {
                            return Stream::Lost(format!("can't write: {e}"));
                        }
                        let _ = link.send(&Node::new("ack").attr("n", self.applied.load(Ordering::Relaxed))).await;
                    }
                    continue;
                }
                n = link.recv(SILENCE) => n,
            };
            let n = match got {
                Ok(n) => n,
                Err(e) => return Stream::Lost(e.to_string()),
            };
            match n.tag.as_str() {
                "keys" => {
                    if let Some(k) = n.content_bytes().filter(|k| !k.is_empty()) {
                        let _ = crate::util::write_private(&self.data.join("server_key").to_string_lossy(), k);
                    }
                    if let Some(s) = n.get("secret").filter(|s| !s.is_empty()) {
                        let _ = crate::util::write_private(&self.data.join("secret").to_string_lossy(), s.as_bytes());
                    }
                }
                "r" => {
                    if let Some(op) = node_op(&n) {
                        apply.ops.push(op);
                    }
                }
                "q" => {
                    let (Some(o), Some(k)) = (n.get("o"), n.get("k")) else {
                        continue;
                    };
                    let num = |a: &str| n.get(a).and_then(|v| v.parse().ok()).unwrap_or(0);
                    let w: QWrite = (o.to_string(), k.to_string(), num("s"), num("a"), n.content_bytes().unwrap_or_default().to_vec());
                    apply.queue.insert((o.to_string(), k.to_string()), Some(w));
                }
                "qd" => {
                    let (Some(o), Some(k)) = (n.get("o"), n.get("k")) else {
                        continue;
                    };
                    apply.queue.insert((o.to_string(), k.to_string()), None);
                }
                "p" => {
                    let Some(name) = n.get("n").filter(|s| !s.contains(['/', '\\']) && !s.contains("..")) else {
                        continue;
                    };
                    let file = self.data.join("pictures").join(format!("{name}.jpg"));
                    match n.content_bytes() {
                        Some(b) if n.get("del").is_none() => {
                            let _ = std::fs::create_dir_all(self.data.join("pictures"));
                            let _ = std::fs::write(&file, b);
                        }
                        _ => {
                            let _ = std::fs::remove_file(&file);
                        }
                    }
                }
                "synced" => {
                    if let Err(e) = apply.flush().await {
                        return Stream::Lost(format!("can't write: {e}"));
                    }
                    *synced = true;
                    self.set_role(Role::Standby { synced: true });
                    log("replica", format!("copy complete ({} changes); now following live", self.applied.load(Ordering::Relaxed)));
                }
                "handover" => {
                    if let Err(e) = apply.flush().await {
                        return Stream::Lost(format!("can't write: {e}"));
                    }
                    let _ = link.send(&Node::new("ack").attr("n", self.applied.load(Ordering::Relaxed)).attr("h", "1")).await;
                    return Stream::Promote("handover from the draining primary".into());
                }
                _ => {} // ping
            }
            if matches!(n.tag.as_str(), "r" | "q" | "qd" | "p") {
                self.applied.fetch_add(1, Ordering::Relaxed);
                if apply.ops.len() + apply.queue.len() >= 20_000 {
                    if let Err(e) = apply.flush().await {
                        return Stream::Lost(format!("can't write: {e}"));
                    }
                }
            }
        }
    }
}

enum Stream {
    Promote(String),
    Lost(String),
}

/// The standby's copy: the same files the server uses, written in batches
/// (every 50 ms). A stanza queued and delivered within one batch is never
/// written at all.
struct Applier {
    db: Arc<Db>,
    qdb: Arc<QueueDb>,
    ops: Vec<Op>,
    queue: IndexMap<(String, String), Option<QWrite>>,
}

impl Applier {
    fn open(data: &Path) -> io::Result<Applier> {
        // Move the old copy aside (previous/), then start empty.
        let prev = data.join("previous");
        let _ = std::fs::remove_dir_all(&prev);
        for name in ["xb.redb", "queue", "pictures"] {
            let p = data.join(name);
            if p.exists() {
                std::fs::create_dir_all(&prev)?;
                std::fs::rename(&p, prev.join(name))?;
            }
        }
        Ok(Applier { db: Arc::new(Db::open(&data.join("xb.redb"))?), qdb: Arc::new(QueueDb::open(&data.join("queue"))?), ops: Vec::new(), queue: IndexMap::new() })
    }

    fn dirty(&self) -> bool {
        !self.ops.is_empty() || !self.queue.is_empty()
    }

    async fn flush(&mut self) -> io::Result<()> {
        let ops = std::mem::take(&mut self.ops);
        let mut writes = Vec::new();
        let mut deletes = Vec::new();
        for ((o, k), w) in std::mem::take(&mut self.queue) {
            match w {
                Some(w) => writes.push(w),
                None => deletes.push((o, k)),
            }
        }
        let (db, qdb) = (self.db.clone(), self.qdb.clone());
        tokio::task::spawn_blocking(move || {
            if !ops.is_empty() {
                db.write(ops)?;
            }
            qdb.apply(&writes, &deletes)
        })
        .await
        .map_err(err)?
    }
}
