//! xb-tunnel: reach a server behind NAT (a PC at home, a phone on 4G) through
//! a relay that has a public address, like ngrok. Two halves, one program:
//!
//!   relay   on a machine the internet can reach (a VPS). Opens the public
//!           ports and waits for the client.
//!   client  next to the server (this PC). Connects OUT to the relay, so
//!           nothing at home needs to be reachable, and carries every
//!           connection the relay gets to a local port.
//!
//!   phone ──► relay:15222 ═══ one encrypted link ═══ client ──► 127.0.0.1:15222
//!
//! The link is xb-proto's (link.rs): encrypted with a shared secret, so only
//! a client with the secret can attach. Inside it, each connection is a
//! numbered stream:
//!   relay -> client   o s=<stream> p=<public port>   a new connection
//!   both              d s=<stream> <bytes>           data
//!   both              c s=<stream>                   closed
//!   client -> relay   hello p="15222,18080"          the ports to open
//!   relay -> client   ready                          accepted (a wrong secret gets nothing)
//!   both              ping, every 2 s
//!
//!   xb-tunnel relay  --listen 0.0.0.0:7000 [--public-host 0.0.0.0]
//!   xb-tunnel client --relay <vps>:7000 --map 15222=127.0.0.1:15222 --map 18080=127.0.0.1:18080
//!
//! Both read the secret from XB_TUNNEL_SECRET (not an argument: it would show
//! in process lists and shell history).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use xb_proto::codec::Node;
use xb_proto::link::{Link, MAX_NODE, PING, SILENCE, seal_batch, unbatch};
use xb_proto::util::log;

/// The streams on one link: what goes out on it, and where each stream's
/// incoming bytes go.
struct Mux {
    out: UnboundedSender<Node>,
    streams: Mutex<HashMap<u64, UnboundedSender<Vec<u8>>>>,
}

impl Mux {
    fn send(&self, n: Node) -> bool {
        self.out.send(n).is_ok()
    }

    /// Bytes from the other side for stream `s`.
    fn data(&self, s: u64, b: Vec<u8>) {
        if let Some(tx) = self.streams.lock().unwrap().get(&s) {
            let _ = tx.send(b);
        }
    }

    /// The other side closed stream `s`: dropping its sender ends our writer.
    fn closed(&self, s: u64) {
        self.streams.lock().unwrap().remove(&s);
    }
}

/// A new stream `s`: its channel for bytes from the other side.
fn open_stream(mux: &Mux, s: u64) -> UnboundedReceiver<Vec<u8>> {
    let (tx, rx) = unbounded_channel::<Vec<u8>>();
    mux.streams.lock().unwrap().insert(s, tx);
    rx
}

/// Carries one socket over stream `s` (opened with open_stream) until
/// either end closes it.
fn attach(mux: Arc<Mux>, s: u64, sock: TcpStream, mut rx: UnboundedReceiver<Vec<u8>>) {
    let _ = sock.set_nodelay(true);
    let (mut r, mut w) = sock.into_split();
    // Other side -> socket.
    tokio::spawn(async move {
        while let Some(b) = rx.recv().await {
            if w.write_all(&b).await.is_err() {
                break;
            }
        }
        let _ = w.shutdown().await;
    });
    // Socket -> other side.
    tokio::spawn(async move {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match r.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if !mux.send(Node::new("d").attr("s", s).bytes(buf[..n].to_vec())) {
                        break;
                    }
                }
            }
        }
        if mux.streams.lock().unwrap().remove(&s).is_some() {
            mux.send(Node::new("c").attr("s", s));
        }
    });
}

/// Runs a link until it breaks: sends what `rx` holds (in batches), hands
/// what arrives to `on_node`, pings both ways.
async fn run_link(link: Link, mut rx: UnboundedReceiver<Node>, mut on_node: impl FnMut(Node)) -> String {
    let Link { mut r, mut w, mut dec, rx: mut open, tx: mut seal } = link;
    let mut ping = tokio::time::interval(PING);
    let mut last_heard = Instant::now();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            m = rx.recv() => {
                let Some(n) = m else { return "closed".into() };
                let frame = seal_batch(&mut seal, n, &mut rx);
                if let Err(e) = w.write_all(&frame).await { return e.to_string() }
            }
            _ = ping.tick() => {
                if last_heard.elapsed() > SILENCE { return "silent".into() }
                if let Err(e) = w.write_all(&seal.seal(&Node::new("ping"))).await { return e.to_string() }
            }
            got = r.read(&mut buf) => {
                let n = match got {
                    Ok(0) => return "disconnected".into(),
                    Ok(n) => n,
                    Err(e) => return e.to_string(),
                };
                last_heard = Instant::now();
                dec.push(&buf[..n]);
                loop {
                    let f = match dec.next_frame() {
                        Ok(Some(f)) => f,
                        Ok(None) => break,
                        Err(e) => return format!("{e:?}"),
                    };
                    match open.open(&f, MAX_NODE) {
                        Ok(n) => unbatch(n).into_iter().for_each(&mut on_node),
                        Err(e) => return e.to_string(), // wrong secret, or garbage
                    }
                }
            }
        }
    }
}

fn stream_of(n: &Node) -> Option<u64> {
    n.get("s")?.parse().ok()
}

// ---------------------------------------------------------------- relay

async fn relay(listen: String, public_host: String, secret: String) -> std::io::Result<()> {
    let control = TcpListener::bind(&listen).await?;
    log("relay", format!("waiting for the client on {}", control.local_addr()?));
    // The client attached now (one at a time: a new one replaces the old).
    let current: Arc<Mutex<Option<Arc<Mux>>>> = Arc::new(Mutex::new(None));
    let next = Arc::new(AtomicU64::new(1));
    let mut opened: HashSet<u16> = HashSet::new();
    loop {
        let (sock, from) = control.accept().await?;
        let mut link = match Link::open(sock, &secret, false).await {
            Ok(l) => l,
            Err(e) => {
                log("relay", format!("{from}: {e}"));
                continue;
            }
        };
        // The client says which ports to open; anyone without the secret fails here.
        let ports: Vec<u16> = match link.recv(SILENCE).await {
            Ok(h) if h.tag == "hello" => h.get("p").unwrap_or("").split(',').filter_map(|p| p.parse().ok()).collect(),
            Ok(_) => continue,
            Err(e) => {
                log("relay", format!("{from}: {e}"));
                continue;
            }
        };
        for p in ports {
            if opened.insert(p) {
                let l = match TcpListener::bind((public_host.as_str(), p)).await {
                    Ok(l) => l,
                    Err(e) => {
                        log("relay", format!("can't open port {p}: {e}"));
                        opened.remove(&p);
                        continue;
                    }
                };
                log("relay", format!("public port {} open", l.local_addr()?));
                let (current, next) = (current.clone(), next.clone());
                tokio::spawn(async move {
                    loop {
                        let Ok((sock, _)) = l.accept().await else { continue };
                        // No client attached: nothing to carry it to.
                        let Some(mux) = current.lock().unwrap().clone() else { continue };
                        let s = next.fetch_add(1, Ordering::Relaxed);
                        let rx = open_stream(&mux, s);
                        if mux.send(Node::new("o").attr("s", s).attr("p", p)) {
                            attach(mux, s, sock, rx);
                        }
                    }
                });
            }
        }
        if link.send(&Node::new("ready")).await.is_err() {
            continue;
        }
        let (tx, rx) = unbounded_channel();
        let mux = Arc::new(Mux { out: tx, streams: Mutex::new(HashMap::new()) });
        *current.lock().unwrap() = Some(mux.clone());
        log("relay", format!("client {from} attached"));
        let current = current.clone();
        tokio::spawn(async move {
            let m = mux.clone();
            let why = run_link(link, rx, move |n| match (n.tag.as_str(), stream_of(&n)) {
                ("d", Some(s)) => m.data(s, n.content_bytes().unwrap_or_default().to_vec()),
                ("c", Some(s)) => m.closed(s),
                _ => {}
            })
            .await;
            log("relay", format!("client {from} gone ({why})"));
            let mut cur = current.lock().unwrap();
            if cur.as_ref().is_some_and(|c| Arc::ptr_eq(c, &mux)) {
                *cur = None;
            }
            // Its streams end with it (their senders drop with the map).
            mux.streams.lock().unwrap().clear();
        });
    }
}

// ---------------------------------------------------------------- client

async fn client(relay: String, secret: String, maps: HashMap<u16, String>) {
    let ports: Vec<String> = maps.keys().map(|p| p.to_string()).collect();
    let maps = Arc::new(maps);
    let mut wait = Duration::from_millis(500);
    loop {
        let mut link = match Link::dial(&relay, &secret).await {
            Ok(l) => l,
            Err(e) => {
                log("client", format!("can't reach the relay at {relay}: {e}; trying again"));
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        wait = Duration::from_millis(500);
        if link.send(&Node::new("hello").attr("p", ports.join(","))).await.is_err() {
            continue;
        }
        // The relay answers only a client with the right secret.
        match link.recv(SILENCE).await {
            Ok(n) if n.tag == "ready" => {}
            _ => {
                log("client", format!("the relay at {relay} refused us (the wrong XB_TUNNEL_SECRET?); trying again"));
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(Duration::from_secs(30));
                continue;
            }
        }
        log("client", format!("attached to {relay}: {}", maps.iter().map(|(p, t)| format!("{p} -> {t}")).collect::<Vec<_>>().join(", ")));
        let (tx, rx) = unbounded_channel();
        let mux = Arc::new(Mux { out: tx, streams: Mutex::new(HashMap::new()) });
        let m = mux.clone();
        let maps2 = maps.clone();
        let why = run_link(link, rx, move |n| match (n.tag.as_str(), stream_of(&n)) {
            ("o", Some(s)) => {
                let target = n.get("p").and_then(|p| p.parse::<u16>().ok()).and_then(|p| maps2.get(&p).cloned());
                let m = m.clone();
                // The stream exists at once: bytes that arrive while the
                // local socket is still connecting wait in its channel.
                let rx = open_stream(&m, s);
                tokio::spawn(async move {
                    let sock = match target {
                        Some(t) => TcpStream::connect(&t).await.ok(),
                        None => None,
                    };
                    match sock {
                        Some(sock) => attach(m, s, sock, rx),
                        None => {
                            m.streams.lock().unwrap().remove(&s);
                            m.send(Node::new("c").attr("s", s));
                        }
                    }
                });
            }
            ("d", Some(s)) => m.data(s, n.content_bytes().unwrap_or_default().to_vec()),
            ("c", Some(s)) => m.closed(s),
            _ => {}
        })
        .await;
        log("client", format!("link to {relay} lost ({why}); reconnecting"));
        mux.streams.lock().unwrap().clear();
        tokio::time::sleep(wait).await;
    }
}

// ---------------------------------------------------------------- main

fn usage() -> ! {
    eprintln!(
        "usage:\n  xb-tunnel relay  --listen <addr:port> [--public-host <addr>]\n  xb-tunnel client --relay <host:port> --map <public port>=<local host:port> ...\n(the shared secret is read from XB_TUNNEL_SECRET)"
    );
    std::process::exit(2)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let opt = |name: &str| args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone());
    let Some(secret) = std::env::var("XB_TUNNEL_SECRET").ok().filter(|s| s.len() >= 16) else {
        eprintln!("set XB_TUNNEL_SECRET (16 characters or more, the same on both sides)");
        std::process::exit(2)
    };
    match args.first().map(String::as_str) {
        Some("relay") => relay(opt("--listen").unwrap_or_else(|| usage()), opt("--public-host").unwrap_or("0.0.0.0".into()), secret).await,
        Some("client") => {
            let maps: HashMap<u16, String> = args
                .windows(2)
                .filter(|w| w[0] == "--map")
                .filter_map(|w| {
                    let (p, t) = w[1].split_once('=')?;
                    Some((p.parse().ok()?, t.to_string()))
                })
                .collect();
            if maps.is_empty() {
                usage();
            }
            client(opt("--relay").unwrap_or_else(|| usage()), secret, maps).await;
            Ok(())
        }
        _ => usage(),
    }
}
