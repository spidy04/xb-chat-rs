//! Load test for the xb-chat server: many simulated phones chatting.
//!
//!   cargo run --release --bin loadgen -- --users 2000 --rate 5000 --secs 30
//!
//! Each phone is one connection: Noise handshake, phone-number login (the
//! server must run with XB_OTP_ECHO=1 the first time; after that the saved
//! device keys log in alone), one end-to-end device with keys of the right
//! shape, then messages to random other phones at the given total rate.
//! Receivers send delivered receipts, senders ack them: the full message
//! cycle the server does for real phones. The payload isn't real Signal
//! ciphertext, but the server never looks inside, so it does the same work.
//!
//! Prints messages/s and latencies every second:
//!   ack       sender -> server -> ack back      (one tick)
//!   deliver   sender -> server -> recipient     (message on the other phone)
//!   receipt   sender -> ... -> delivered receipt back (two ticks)
//!
//! Options: --host 127.0.0.1 --port 5222 --users 1000 --rate 2000 (messages/s,
//! all phones together) --secs 20 --size 200 (payload bytes) --state loadgen-users.json
//! --prefix 9188 (phone numbers are <prefix><8 digits>)
//! --sources 4 (connect from 127.0.0.1 … 127.0.0.4: more than ~16,000 connections)
//! --rate 0 only holds the connections, for an idle-connection (memory) test

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use xb_server::codec::Node;
use xb_server::frame::{self, FrameDecoder};
use xb_server::tokens::HEADER;
use xb_server::transport::{Cipher, NOISE_PARAMS};

/// Logins tried again after "busy" or a refused socket.
static RETRIES: AtomicU64 = AtomicU64::new(0);

fn arg<T: std::str::FromStr>(name: &str, default: T) -> T {
    let args: Vec<String> = std::env::args().collect();
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Latencies in microseconds, kept for percentiles.
#[derive(Default)]
struct Lat(Vec<u32>);

impl Lat {
    fn add(&mut self, d: Duration) {
        self.0.push(d.as_micros().min(u32::MAX as u128) as u32);
    }
    fn pct(&mut self, p: f64) -> f64 {
        if self.0.is_empty() {
            return 0.0;
        }
        self.0.sort_unstable();
        let i = ((self.0.len() as f64 - 1.0) * p).round() as usize;
        self.0[i] as f64 / 1000.0
    }
}

#[derive(Default)]
struct Stats {
    sent: AtomicU64,
    acked: AtomicU64,
    delivered: AtomicU64,
    receipts: AtomicU64,
    errors: AtomicU64,
    lat_ack: std::sync::Mutex<Lat>,
    lat_deliver: std::sync::Mutex<Lat>,
    lat_receipt: std::sync::Mutex<Lat>,
}

/// One phone's identity, saved so the next run logs in without SMS codes.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct Phone {
    number: String,
    key: [u8; 32],
    device: u32,
    registered: bool,
}

/// Windows gives a program one pool of ~16,000 outgoing ports for all its
/// source addresses. SO_REUSE_UNICASTPORT lets one port serve several
/// connections that differ in their addresses (127.0.0.1, .2 …).
#[cfg(windows)]
fn share_ports(sock: &tokio::net::TcpSocket) {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{SOL_SOCKET, setsockopt};
    const SO_REUSE_UNICASTPORT: i32 = 0x3007;
    let on: u32 = 1;
    unsafe {
        setsockopt(sock.as_raw_socket() as usize, SOL_SOCKET, SO_REUSE_UNICASTPORT, &on as *const u32 as *const u8, 4);
    }
}

#[cfg(not(windows))]
fn share_ports(_: &tokio::net::TcpSocket) {}

struct Conn {
    r: tokio::net::tcp::OwnedReadHalf,
    w: tokio::net::tcp::OwnedWriteHalf,
    send: Cipher,
    recv: Cipher,
    dec: FrameDecoder,
}

impl Conn {
    /// `source`: the local address to connect from (one address only has
    /// about 16,000 outgoing ports; 127.0.0.2, .3 … give more on loopback).
    async fn open(addr: &str, key: &[u8; 32], source: Option<std::net::Ipv4Addr>) -> std::io::Result<Conn> {
        let s = match source {
            None => TcpStream::connect(addr).await?,
            Some(ip) => {
                let sock = tokio::net::TcpSocket::new_v4()?;
                share_ports(&sock);
                sock.bind((ip, 0).into())?;
                let to = tokio::net::lookup_host(addr).await?.find(|a| a.is_ipv4()).ok_or_else(|| std::io::Error::other("no IPv4 address"))?;
                sock.connect(to).await?
            }
        };
        s.set_nodelay(true)?;
        let (mut r, mut w) = s.into_split();
        let noise = |e: snow::Error| std::io::Error::other(e.to_string());
        let mut hs = snow::Builder::new(NOISE_PARAMS.parse().map_err(noise)?).local_private_key(key).map_err(noise)?.prologue(&HEADER).map_err(noise)?.build_initiator().map_err(noise)?;
        let mut buf = vec![0u8; 1024];
        let n = hs.write_message(&[], &mut buf).map_err(noise)?;
        let mut first = HEADER.to_vec();
        first.extend(frame::encode(&buf[..n]).unwrap());
        w.write_all(&first).await?;
        let mut dec = FrameDecoder::new(4 << 20);
        let m2 = read_frame(&mut r, &mut dec).await?;
        hs.read_message(&m2, &mut buf).map_err(noise)?;
        let n = hs.write_message(&[], &mut buf).map_err(noise)?;
        w.write_all(&frame::encode(&buf[..n]).unwrap()).await?;
        let (c1, c2) = hs.dangerously_get_raw_split();
        Ok(Conn { r, w, send: Cipher::new(&c1), recv: Cipher::new(&c2), dec })
    }

    async fn send(&mut self, n: &Node) -> std::io::Result<()> {
        let f = self.send.seal(n);
        self.w.write_all(&f).await
    }

    /// The next stanza (waits for it).
    async fn next(&mut self) -> std::io::Result<Node> {
        let f = read_frame(&mut self.r, &mut self.dec).await?;
        self.recv.open(&f, 4 << 20)
    }

    async fn until(&mut self, tag: &str) -> std::io::Result<Node> {
        loop {
            let n = self.next().await?;
            if std::env::var("LOADGEN_TRACE").is_ok() {
                eprintln!("  <- {} {:?}", n.tag, n.attrs);
            }
            if n.tag == tag || n.tag == "failure" || n.tag == "stream:error" {
                return Ok(n);
            }
        }
    }
}

async fn read_frame(r: &mut tokio::net::tcp::OwnedReadHalf, dec: &mut FrameDecoder) -> std::io::Result<Vec<u8>> {
    let mut chunk = [0u8; 16 * 1024];
    loop {
        if let Some(f) = dec.next_frame().map_err(|e| std::io::Error::other(format!("{e:?}")))? {
            return Ok(f);
        }
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        dec.push(&chunk[..n]);
    }
}

fn jid(number: &str) -> String {
    format!("{number}@s.xb")
}

/// Connects and logs in: SMS code the first time, the device key after that.
async fn login(addr: &str, p: &mut Phone, source: Option<std::net::Ipv4Addr>) -> std::io::Result<Conn> {
    let trace = std::env::var("LOADGEN_TRACE").is_ok();
    let step = |s: &str| {
        if trace {
            eprintln!("{} {s}", p.number)
        }
    };
    step("connecting");
    let mut c = Conn::open(addr, &p.key, source).await?;
    step("handshake done");
    let bad = |n: &Node| std::io::Error::other(format!("{} {:?}", n.tag, n.attrs));
    if p.registered {
        c.send(&Node::new("auth").attr("mode", "device").attr("user", &p.number)).await?;
        let r = c.until("success").await?;
        if r.tag == "failure" && r.get("reason") == Some("busy") {
            return Err(bad(&r)); // try again later; still registered
        }
        if r.tag == "success" {
            // Name our end-to-end device, so its queue comes to us.
            c.send(&Node::new("iq").attr("id", "c1").attr("xmlns", "encrypt").attr("type", "get").nodes(vec![Node::new("count").attr("device", p.device)])).await?;
            return Ok(c);
        }
        p.registered = false; // the server forgot us: register again
        c = Conn::open(addr, &p.key, source).await?;
    }
    c.send(&Node::new("auth").attr("mode", "code").attr("phone", &p.number)).await?;
    step("asked for a code");
    let code = c.until("code").await?;
    let Some(otp) = code.get("dev").map(str::to_string) else {
        return Err(std::io::Error::other(format!("no SMS code echoed ({} {:?}): start the server with XB_OTP_ECHO=1", code.tag, code.attrs)));
    };
    c.send(&Node::new("auth").attr("mode", "verify").attr("phone", &p.number).attr("code", otp).attr("name", "Load").attr("device", "loadgen").attr("platform", "loadgen")).await?;
    let ok = c.until("success").await?;
    if ok.tag != "success" {
        return Err(bad(&ok));
    }
    // Keys of the right shape: the server checks sizes, not the cryptography.
    let b = |n: usize| (0..n).map(|_| rand::random::<u8>()).collect::<Vec<u8>>();
    let keys = (1..=20).map(|i| Node::new("key").attr("id", i).bytes(b(32))).collect();
    let publish = Node::new("iq").attr("id", "k1").attr("xmlns", "encrypt").attr("type", "set").nodes(vec![Node::new("keys").attr("device", p.device).nodes(vec![
        Node::new("identity").bytes(b(32)),
        Node::new("signing").bytes(b(32)),
        Node::new("skey").attr("id", 1).nodes(vec![Node::new("value").bytes(b(32)), Node::new("signature").bytes(b(64))]),
        Node::new("list").nodes(keys),
    ])]);
    c.send(&publish).await?;
    let r = c.until("iq").await?;
    if r.get("type") != Some("result") {
        return Err(bad(&r));
    }
    p.registered = true;
    Ok(c)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let host: String = arg("--host", "127.0.0.1".to_string());
    let port: u16 = arg("--port", 5222);
    let users: usize = arg("--users", 1000);
    let rate: f64 = arg("--rate", 2000.0);
    let secs: u64 = arg("--secs", 20);
    let size: usize = arg("--size", 200);
    let state_file: String = arg("--state", "loadgen-users.json".to_string());
    let prefix: String = arg("--prefix", "9188".to_string());
    let sources: u8 = arg("--sources", 1u8).max(1);
    let addr = format!("{host}:{port}");

    // The phones: 91880000xxxx, saved between runs.
    let mut phones: Vec<Phone> = std::fs::read(&state_file).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    while phones.len() < users {
        let i = phones.len();
        phones.push(Phone { number: format!("{prefix}{:08}", i), key: rand::random(), device: 1 + rand::random::<u32>() % 2_000_000_000, registered: false });
    }
    let directory: Arc<Vec<(String, u32)>> = Arc::new(phones[..users].iter().map(|p| (p.number.clone(), p.device)).collect());

    println!("logging in {users} phones to {addr} …");
    let t0 = Instant::now();
    let mut conns = Vec::with_capacity(users);
    let mut tasks = tokio::task::JoinSet::new();
    let sem = Arc::new(tokio::sync::Semaphore::new(arg("--parallel", 200usize))); // logins in flight
    for (i, mut p) in phones[..users].iter().cloned().enumerate() {
        let (addr, sem) = (addr.clone(), sem.clone());
        let source = (sources > 1).then(|| std::net::Ipv4Addr::new(127, 0, 0, 1 + (i % sources as usize) as u8));
        tasks.spawn(async move {
            let _permit = sem.acquire().await;
            // Like a well-behaved phone: "busy" or a refused socket means wait
            // (the server's hint, plus jitter) and try again.
            let mut tries = 0u32;
            let c = loop {
                tries += 1;
                // A login that takes this long is stuck: report where.
                let c = match tokio::time::timeout(Duration::from_secs(20), login(&addr, &mut p, source)).await {
                    Ok(c) => c,
                    Err(_) => Err(std::io::Error::other("login timed out")),
                };
                let Err(e) = &c else { break c };
                let msg = e.to_string();
                let busy = msg.contains("\"busy\"");
                let refused = matches!(e.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted);
                if tries >= 30 || !(busy || refused) {
                    break c;
                }
                let hint = msg.split("\"retry\", \"").nth(1).and_then(|r| r.split('"').next()).and_then(|r| r.parse::<u64>().ok()).unwrap_or(1);
                RETRIES.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(hint * 1000 + rand::random::<u64>() % 1000)).await;
            };
            (i, p, c)
        });
    }
    let mut failed = 0;
    while let Some(r) = tasks.join_next().await {
        let (i, p, c) = r.expect("login task");
        phones[i] = p;
        match c {
            Ok(c) => conns.push((i, c)),
            Err(e) => {
                failed += 1;
                if failed <= 3 {
                    eprintln!("login {}: {e}", phones[i].number);
                }
            }
        }
    }
    let _ = std::fs::write(&state_file, serde_json::to_vec(&phones).unwrap());
    println!("{} logged in, {failed} failed, in {:.1} s ({} retries after busy/refused)", conns.len(), t0.elapsed().as_secs_f64(), RETRIES.load(Ordering::Relaxed));
    if conns.is_empty() {
        return Ok(());
    }

    let stats = Arc::new(Stats::default());
    let start = Instant::now();
    let end = start + Duration::from_secs(secs);
    // --rate 0: only hold the connections (an idle-connection test).
    let sending = rate > 0.0;
    let per_phone = if sending { Duration::from_secs_f64(conns.len() as f64 / rate) } else { Duration::from_secs(3600) };
    for (i, mut c) in conns {
        let (dir, stats) = (directory.clone(), stats.clone());
        tokio::spawn(async move {
            let me = jid(&dir[i].0);
            let mut sent_at: HashMap<String, Instant> = HashMap::new();
            let mut seq = 0u64;
            // Spread the phones' send times across one interval.
            let offset = per_phone.mul_f64(rand::random::<f64>());
            let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + offset, per_phone);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = tick.tick(), if sending && Instant::now() < end => {
                        let mut j = rand::random::<usize>() % dir.len();
                        if j == i { j = (j + 1) % dir.len(); }
                        let (peer, device) = &dir[j];
                        seq += 1;
                        let id = format!("{:X}{:08X}", i, seq);
                        // payload: the send time (µs since start), then padding
                        let mut payload = (start.elapsed().as_micros() as u64).to_be_bytes().to_vec();
                        payload.resize(size.max(8), 0x55);
                        let msg = Node::new("message").attr("id", &id).attr("to", jid(peer)).nodes(vec![
                            Node::new("participants").nodes(vec![Node::new("to").attr("jid", jid(peer)).nodes(vec![
                                Node::new("enc").attr("device", device).attr("type", "msg").bytes(payload),
                            ])]),
                        ]);
                        sent_at.insert(id, Instant::now());
                        if c.send(&msg).await.is_err() { stats.errors.fetch_add(1, Ordering::Relaxed); return; }
                        stats.sent.fetch_add(1, Ordering::Relaxed);
                    }
                    n = c.next() => {
                        let Ok(n) = n else { stats.errors.fetch_add(1, Ordering::Relaxed); return };
                        match n.tag.as_str() {
                            "ack" if n.get("class") == Some("message") => {
                                if n.get("error").is_some() { stats.errors.fetch_add(1, Ordering::Relaxed); continue; }
                                if let Some(t) = n.get("id").and_then(|id| sent_at.get(id)) {
                                    stats.lat_ack.lock().unwrap().add(t.elapsed());
                                }
                                stats.acked.fetch_add(1, Ordering::Relaxed);
                            }
                            "message" => {
                                let enc = n.first().and_then(|e| e.content_bytes()).unwrap_or(&[]);
                                if enc.len() >= 8 {
                                    let at = u64::from_be_bytes(enc[..8].try_into().unwrap());
                                    let now = start.elapsed().as_micros() as u64;
                                    stats.lat_deliver.lock().unwrap().add(Duration::from_micros(now.saturating_sub(at)));
                                }
                                stats.delivered.fetch_add(1, Ordering::Relaxed);
                                let r = Node::new("receipt").attr("id", n.get("id").unwrap_or("")).attr("to", n.get("from").unwrap_or("")).attr("type", "delivered");
                                if c.send(&r).await.is_err() { return; }
                            }
                            "receipt" => {
                                if let Some(t) = n.get("id").and_then(|id| sent_at.remove(id)) {
                                    stats.lat_receipt.lock().unwrap().add(t.elapsed());
                                }
                                stats.receipts.fetch_add(1, Ordering::Relaxed);
                                let a = Node::new("ack").attr("class", "receipt").attr("id", n.get("id").unwrap_or("")).attr("from", n.get("from").unwrap_or("")).attr("type", n.get("type").unwrap_or(""));
                                if c.send(&a).await.is_err() { return; }
                            }
                            _ => {}
                        }
                    }
                }
                let _ = &me;
            }
        });
    }

    // Report every second.
    let mut last = [0u64; 4];
    let read = |s: &Stats| [s.sent.load(Ordering::Relaxed), s.acked.load(Ordering::Relaxed), s.delivered.load(Ordering::Relaxed), s.receipts.load(Ordering::Relaxed)];
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.tick().await;
    let mut totals = (Lat::default(), Lat::default(), Lat::default());
    for second in 1..=secs + 3 {
        tick.tick().await;
        let now = read(&stats);
        let take = |m: &std::sync::Mutex<Lat>| std::mem::take(&mut *m.lock().unwrap());
        let (mut a, mut d, mut r) = (take(&stats.lat_ack), take(&stats.lat_deliver), take(&stats.lat_receipt));
        println!(
            "{second:>3}s  sent {:>6}/s  acked {:>6}/s  delivered {:>6}/s  receipts {:>6}/s | ack p50 {:>6.2} p99 {:>7.2} ms | deliver p50 {:>6.2} p99 {:>7.2} ms | receipt p99 {:>7.2} ms | errors {}",
            now[0] - last[0],
            now[1] - last[1],
            now[2] - last[2],
            now[3] - last[3],
            a.pct(0.5),
            a.pct(0.99),
            d.pct(0.5),
            d.pct(0.99),
            r.pct(0.99),
            stats.errors.load(Ordering::Relaxed),
        );
        totals.0.0.extend(a.0);
        totals.1.0.extend(d.0);
        totals.2.0.extend(r.0);
        last = now;
    }
    let s = read(&stats);
    println!(
        "TOTAL sent {} acked {} delivered {} receipts {} errors {} | {:.0} msg/s | ack p50 {:.2} p99 {:.2} ms | deliver p50 {:.2} p99 {:.2} p99.9 {:.2} ms | receipt p99 {:.2} ms",
        s[0],
        s[1],
        s[2],
        s[3],
        stats.errors.load(Ordering::Relaxed),
        s[1] as f64 / secs as f64,
        totals.0.pct(0.5),
        totals.0.pct(0.99),
        totals.1.pct(0.5),
        totals.1.pct(0.99),
        totals.1.pct(0.999),
        totals.2.pct(0.99),
    );
    Ok(())
}
