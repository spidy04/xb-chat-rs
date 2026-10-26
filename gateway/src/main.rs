//! xb-gateway: holds phones' connections for a core server (relay.rs).
//!
//! It accepts the connections (TCP, and WebSocket on /ws), runs the Noise
//! handshakes, encrypts and compresses, keeps each connection's send queue
//! and closes readers that are too slow. Every stanza goes, decrypted, over
//! one encrypted link to the core, which does everything else. Run as many
//! as needed (behind a load balancer); none keeps any state.
//!
//!   cargo run --release --bin xb-gateway
//!
//! The core runs with XB_GATEWAY_ADDR and XB_GATEWAY_SECRET; it hands the
//! gateway the server's key when the link comes up, so phones see the same
//! server whichever gateway they reach.
//!
//! Environment (defaults in brackets):
//!   XB_CORE            the core's gateway address; a replica pair: both, comma separated
//!   XB_GATEWAY_SECRET  the link's shared secret
//!   XB_HOST [127.0.0.1], XB_PORT [5222], XB_WEB_PORT [8080]   where phones connect
//!   XB_HTTP_UPSTREAM   other HTTP on the web port (web app, media) goes here (the core's web port)
//!   XB_ORIGINS         extra allowed web origins, comma separated
//!   XB_MAX_CONNECTIONS [1000000], XB_MAX_HANDSHAKES [1024]
//!   XB_SEND_QUEUE_MAX [16 MB], XB_WRITE_TIMEOUT_MS [30000]   a slower reader is closed
//!   XB_DRAIN_SPREAD_S [10]   when the core is lost, or on a drain: phones come back within this
//!   XB_METRICS_ADDR [127.0.0.1:9093]  /metrics, /health, /ready, POST /drain; "none" = off
//!
//! Draining (Ctrl+C or POST /drain): stop listening, tell every phone to
//! come back within XB_DRAIN_SPREAD_S (to another gateway), exit.

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::extract::ws::{WebSocketUpgrade, rejection::WebSocketUpgradeRejection};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;
use xb_proto::codec::Node;
use xb_proto::frame::FrameDecoder;
use xb_proto::link::{Link, MAX_NODE, PING, SILENCE, seal_batch, unbatch};
use xb_proto::transport::{self, ConnRead, ConnWrite};
use xb_proto::util::log;

const MAX_STANZA: usize = 1024 * 1024;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn env_num<T: std::str::FromStr>(k: &str, default: T) -> T {
    env(k).and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// What a connection's writer does next.
enum ToClient {
    Stanza(Vec<u8>),
    /// After this encoded <stream:error/>, if any.
    Close(Option<Vec<u8>>),
}

struct Client {
    tx: UnboundedSender<ToClient>,
    queued: AtomicUsize,
    kill: CancellationToken,
}

/// Counters for /metrics.
#[derive(Default)]
struct Stats {
    connections: AtomicU64,
    refused: AtomicU64,
    handshake_failures: AtomicU64,
    slow: AtomicU64,
    stanzas_up: AtomicU64,
    stanzas_down: AtomicU64,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
    core_links: AtomicU64,
}

struct Gw {
    stats: Stats,
    /// Set by a drain: /ready says no.
    draining: CancellationToken,
    /// The server's private key, from the core.
    key: Mutex<Option<[u8; 32]>>,
    /// To the core (None while there's no link).
    core: Mutex<Option<UnboundedSender<Node>>>,
    clients: Mutex<HashMap<u64, Arc<Client>>>,
    next: AtomicU64,
    open: AtomicUsize,
    handshakes: tokio::sync::Semaphore,
    max_connections: usize,
    queue_max: usize,
    write_timeout: Duration,
    spread: u64,
    origins: Vec<String>,
    upstream: Option<String>,
    http: reqwest::Client,
}

impl Gw {
    fn to_core(&self, n: Node) -> bool {
        self.core.lock().unwrap().as_ref().is_some_and(|t| t.send(n).is_ok())
    }

    /// The core sent a stanza for connection `s`.
    fn deliver(&self, s: u64, bytes: Vec<u8>) {
        let Some(c) = self.clients.lock().unwrap().get(&s).cloned() else {
            return;
        };
        let n = bytes.len();
        self.stats.stanzas_down.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_down.fetch_add(n as u64, Ordering::Relaxed);
        if c.queued.fetch_add(n, Ordering::AcqRel) + n > self.queue_max {
            // Not reading: cut it off. Its messages are safe in the core's queue.
            log("overload", format!("connection {s} reads too slowly: closed"));
            self.stats.slow.fetch_add(1, Ordering::Relaxed);
            c.kill.cancel();
            return;
        }
        let _ = c.tx.send(ToClient::Stanza(bytes));
    }

    /// The link to the core is gone: every phone is told to come back
    /// later, spread out (to whichever core serves then).
    fn lost_core(&self) {
        *self.core.lock().unwrap() = None;
        self.send_away();
    }

    /// Every phone: "restart, come back in N s", N spread out.
    fn send_away(&self) {
        let clients: Vec<Arc<Client>> = self.clients.lock().unwrap().drain().map(|(_, c)| c).collect();
        for c in &clients {
            let retry = 1 + rand::random::<u64>() % self.spread.max(1);
            let err = xb_proto::codec::encode(&Node::new("stream:error").attr("code", "restart").attr("retry", retry));
            let _ = c.tx.send(ToClient::Close(Some(err)));
        }
        if !clients.is_empty() {
            log("core", format!("{} connections told to come back later", clients.len()));
        }
    }
}

/// Keeps a link to the core (trying each address in turn).
async fn core_link(gw: Arc<Gw>, cores: Vec<String>, secret: String) {
    let mut i = 0;
    loop {
        let addr = &cores[i % cores.len()];
        i += 1;
        let Ok(link) = Link::dial(addr, &secret).await else {
            tokio::time::sleep(Duration::from_millis(300)).await;
            continue;
        };
        gw.stats.core_links.fetch_add(1, Ordering::Relaxed);
        let why = serve_link(&gw, link, addr).await;
        log("core", format!("link to {addr} lost ({why})"));
        gw.lost_core();
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn serve_link(gw: &Arc<Gw>, mut link: Link, addr: &str) -> String {
    let keys = match link.recv(SILENCE).await {
        Ok(k) if k.tag == "keys" => k,
        Ok(_) => return "no keys".into(),
        Err(e) => return e.to_string(),
    };
    let Some(key) = keys.content_bytes().and_then(|k| <[u8; 32]>::try_from(k).ok()) else {
        return "bad keys".into();
    };
    *gw.key.lock().unwrap() = Some(key);
    let (tx, mut rx) = unbounded_channel::<Node>();
    *gw.core.lock().unwrap() = Some(tx);
    log("core", format!("connected to the core at {addr}"));
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
                if last_heard.elapsed() > SILENCE { return "core silent".into() }
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
                    let batch = match open.open(&f, MAX_NODE) {
                        Ok(n) => unbatch(n),
                        Err(e) => return e.to_string(),
                    };
                    for node in batch {
                    let Some(s) = node.get("s").and_then(|s| s.parse::<u64>().ok()) else { continue };
                    match node.tag.as_str() {
                        "d" => {
                            if let Some(b) = node.content_bytes() {
                                gw.deliver(s, b.to_vec());
                            }
                        }
                        "x" => {
                            if let Some(c) = gw.clients.lock().unwrap().remove(&s) {
                                let _ = c.tx.send(ToClient::Close(node.content_bytes().map(|b| b.to_vec())));
                            }
                        }
                        _ => {}
                    }
                    }
                }
            }
        }
    }
}

/// One phone's connection, from the first byte to the end.
async fn client(gw: Arc<Gw>, mut r: ConnRead, mut w: ConnWrite) {
    struct Open<'a>(&'a AtomicUsize);
    impl Drop for Open<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }
    let n = gw.open.fetch_add(1, Ordering::AcqRel) + 1;
    let _open = Open(&gw.open);
    // No core yet, or full: refuse; the phone backs off and retries.
    let refuse = || gw.stats.refused.fetch_add(1, Ordering::Relaxed);
    let Some(key) = *gw.key.lock().unwrap() else {
        refuse();
        return;
    };
    if n > gw.max_connections || gw.core.lock().unwrap().is_none() || gw.draining.is_cancelled() {
        refuse();
        return;
    }
    let Ok(Ok(slot)) = tokio::time::timeout(Duration::from_secs(5), gw.handshakes.acquire()).await else {
        refuse();
        return;
    };
    let mut decoder = FrameDecoder::new(MAX_STANZA + 64);
    let secured = tokio::time::timeout(Duration::from_secs(10), Box::pin(transport::accept(&mut r, &mut w, &mut decoder, &key))).await;
    drop(slot);
    let Ok(Ok(secured)) = secured else {
        gw.stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
        return;
    };
    gw.stats.connections.fetch_add(1, Ordering::Relaxed);

    let sid = gw.next.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = unbounded_channel();
    let kill = CancellationToken::new();
    let c = Arc::new(Client { tx, queued: AtomicUsize::new(0), kill: kill.clone() });
    gw.clients.lock().unwrap().insert(sid, c.clone());
    if !gw.to_core(Node::new("o").attr("s", sid).attr("k", B64.encode(secured.remote_static))) {
        gw.clients.lock().unwrap().remove(&sid);
        return;
    }

    let mut cipher = secured.send;
    let writer = {
        let (c, kill, timeout, gw) = (c.clone(), kill.clone(), gw.write_timeout, gw.clone());
        tokio::spawn(async move {
            loop {
                let m = tokio::select! {
                    _ = kill.cancelled() => break,
                    m = rx.recv() => m,
                };
                match m {
                    None => break,
                    Some(ToClient::Stanza(bytes)) => {
                        let n = bytes.len();
                        let f = cipher.seal_encoded(bytes);
                        let done = tokio::select! {
                            _ = kill.cancelled() => break,
                            r = tokio::time::timeout(timeout, w.write(&f)) => r,
                        };
                        match done {
                            Ok(Ok(())) => {}
                            Ok(Err(_)) => break, // gone
                            Err(_) => {
                                // Stopped reading: too slow.
                                gw.stats.slow.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                        c.queued.fetch_sub(n, Ordering::AcqRel);
                    }
                    Some(ToClient::Close(err)) => {
                        if let Some(e) = err {
                            let _ = w.write(&cipher.seal_encoded(e)).await;
                        }
                        w.shutdown().await;
                        tokio::select! {
                            _ = kill.cancelled() => {}
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        }
                        break;
                    }
                }
            }
            kill.cancel();
        })
    };
    let mut recv = secured.recv;
    'conn: loop {
        // Frames already buffered first (the handshake may have left <auth>).
        loop {
            let payload = match decoder.next_frame() {
                Ok(Some(p)) => p,
                Ok(None) => break,
                Err(_) => break 'conn,
            };
            match recv.open_bytes(&payload, MAX_STANZA) {
                Ok(bytes) => {
                    gw.stats.stanzas_up.fetch_add(1, Ordering::Relaxed);
                    gw.stats.bytes_up.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    if !gw.to_core(Node::new("d").attr("s", sid).bytes(bytes)) {
                        break 'conn;
                    }
                }
                Err(_) => break 'conn,
            }
        }
        tokio::select! {
            _ = kill.cancelled() => break,
            chunk = r.chunk() => {
                let Ok(Some(chunk)) = chunk else { break };
                decoder.push(&chunk);
            }
        }
    }
    kill.cancel();
    let _ = writer.await;
    if gw.clients.lock().unwrap().remove(&sid).is_some() {
        gw.to_core(Node::new("c").attr("s", sid));
    }
}

// ---------------------------------------------------------------- monitoring

async fn metrics(State(gw): State<Arc<Gw>>) -> Response {
    let s = &gw.stats;
    let mut out = String::with_capacity(2048);
    let c = |a: &AtomicU64| a.load(Ordering::Relaxed) as f64;
    let mut m = |name: &str, kind: &str, help: &str, v: f64| xb_proto::prom::write_metric(&mut out, name, kind, help, v);
    m("xb_gateway_connections_open", "gauge", "Sockets open now (handshaking or relayed).", gw.open.load(Ordering::Relaxed) as f64);
    m("xb_gateway_connections_max", "gauge", "The connection cap (XB_MAX_CONNECTIONS).", gw.max_connections as f64);
    m("xb_gateway_connections_total", "counter", "Connections relayed to the core since start.", c(&s.connections));
    m("xb_gateway_refused_total", "counter", "Sockets refused: no core, full, draining, or no handshake slot.", c(&s.refused));
    m("xb_gateway_handshake_failures_total", "counter", "Noise handshakes that failed or timed out.", c(&s.handshake_failures));
    m("xb_gateway_slow_total", "counter", "Connections closed for reading too slowly.", c(&s.slow));
    m("xb_gateway_stanzas_up_total", "counter", "Stanzas from phones to the core.", c(&s.stanzas_up));
    m("xb_gateway_stanzas_down_total", "counter", "Stanzas from the core to phones.", c(&s.stanzas_down));
    m("xb_gateway_bytes_up_total", "counter", "Bytes of stanzas from phones (decrypted).", c(&s.bytes_up));
    m("xb_gateway_bytes_down_total", "counter", "Bytes of stanzas to phones (before encryption).", c(&s.bytes_down));
    m("xb_gateway_core_connected", "gauge", "1 while the link to the core is up.", if gw.core.lock().unwrap().is_some() { 1.0 } else { 0.0 });
    m("xb_gateway_core_links_total", "counter", "Times the link to the core came up.", c(&s.core_links));
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response()
}

/// Ready for phones? Not without a core, and not while draining.
async fn ready(State(gw): State<Arc<Gw>>) -> Response {
    if gw.draining.is_cancelled() {
        return (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response();
    }
    if gw.core.lock().unwrap().is_none() {
        return (StatusCode::SERVICE_UNAVAILABLE, "no core").into_response();
    }
    "ready".into_response()
}

async fn drain(State(gw): State<Arc<Gw>>) -> &'static str {
    gw.draining.cancel();
    "draining\n"
}

// ---------------------------------------------------------------- web port

fn origin_ok(gw: &Gw, headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN).and_then(|o| o.to_str().ok()) else {
        return true;
    };
    let host = headers.get(header::HOST).and_then(|h| h.to_str().ok()).unwrap_or("");
    let origin_host = origin.split_once("://").map(|(_, rest)| rest.split('/').next().unwrap_or("")).unwrap_or("");
    (!origin_host.is_empty() && origin_host.eq_ignore_ascii_case(host)) || gw.origins.iter().any(|o| o == origin)
}

async fn ws(State(gw): State<Arc<Gw>>, method: Method, uri: Uri, headers: HeaderMap, upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>) -> Response {
    let Ok(upgrade) = upgrade else {
        return pass(gw, method, uri, headers, Body::empty()).await;
    };
    if !origin_ok(&gw, &headers) {
        return (StatusCode::FORBIDDEN, [(header::CONNECTION, "close")], "Forbidden").into_response();
    }
    let max = MAX_STANZA + 1024;
    upgrade.max_message_size(max).max_frame_size(max).on_upgrade(move |socket| async move {
        let (sink, stream) = socket.split();
        client(gw, ConnRead::Ws(stream), ConnWrite::Ws(sink)).await;
    })
}

async fn fallback(State(gw): State<Arc<Gw>>, method: Method, uri: Uri, headers: HeaderMap, body: Body) -> Response {
    pass(gw, method, uri, headers, body).await
}

/// Everything else on the web port: streamed to XB_HTTP_UPSTREAM.
async fn pass(gw: Arc<Gw>, method: Method, uri: Uri, headers: HeaderMap, body: Body) -> Response {
    let Some(up) = &gw.upstream else {
        return (StatusCode::NOT_FOUND, "Not Found").into_response();
    };
    let url = format!("{up}{}", uri.path_and_query().map(|p| p.as_str()).unwrap_or("/"));
    let Ok(m) = reqwest::Method::from_bytes(method.as_str().as_bytes()) else {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    };
    let mut req = gw.http.request(m, url);
    for name in [header::AUTHORIZATION, header::CONTENT_TYPE, header::CONTENT_LENGTH, header::RANGE] {
        if let Some(v) = headers.get(&name) {
            req = req.header(name.as_str(), v.as_bytes());
        }
    }
    if method != Method::GET && method != Method::HEAD {
        req = req.body(reqwest::Body::wrap_stream(body.into_data_stream()));
    }
    let res = match req.send().await {
        Ok(r) => r,
        Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };
    let mut b = Response::builder().status(res.status().as_u16());
    for (k, v) in res.headers() {
        if !matches!(k.as_str(), "connection" | "transfer-encoding" | "keep-alive") {
            b = b.header(k.as_str(), v.as_bytes());
        }
    }
    b.body(Body::from_stream(res.bytes_stream())).unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cores: Vec<String> = env("XB_CORE").ok_or_else(|| std::io::Error::other("set XB_CORE (the core's XB_GATEWAY_ADDR)"))?.split(',').map(str::to_string).collect();
    let secret = env("XB_GATEWAY_SECRET").ok_or_else(|| std::io::Error::other("set XB_GATEWAY_SECRET"))?;
    let host = env("XB_HOST").unwrap_or("127.0.0.1".into());
    let gw = Arc::new(Gw {
        stats: Stats::default(),
        draining: CancellationToken::new(),
        key: Mutex::new(None),
        core: Mutex::new(None),
        clients: Mutex::new(HashMap::new()),
        next: AtomicU64::new(1),
        open: AtomicUsize::new(0),
        handshakes: tokio::sync::Semaphore::new(env_num("XB_MAX_HANDSHAKES", 1024)),
        max_connections: env_num("XB_MAX_CONNECTIONS", 1_000_000),
        queue_max: env_num("XB_SEND_QUEUE_MAX", 16 * 1024 * 1024),
        write_timeout: Duration::from_millis(env_num("XB_WRITE_TIMEOUT_MS", 30_000)),
        spread: env_num("XB_DRAIN_SPREAD_S", 10),
        origins: env("XB_ORIGINS").map(|o| o.split(',').filter(|s| !s.is_empty()).map(str::to_string).collect()).unwrap_or_default(),
        upstream: env("XB_HTTP_UPSTREAM").map(|u| u.trim_end_matches('/').to_string()),
        http: reqwest::Client::new(),
    });

    let chat = TcpListener::bind((host.as_str(), env_num("XB_PORT", 5222u16))).await?;
    let web = TcpListener::bind((host.as_str(), env_num("XB_WEB_PORT", 8080u16))).await?;
    log("start", format!("xb-gateway: chat on {}, web on {}, core {}", chat.local_addr()?, web.local_addr()?, cores.join(" or ")));
    tokio::spawn(core_link(gw.clone(), cores, secret));
    let metrics_addr = env("XB_METRICS_ADDR").unwrap_or("127.0.0.1:9093".into());
    if metrics_addr != "none" {
        match TcpListener::bind(&metrics_addr).await {
            Ok(l) => {
                log("start", format!("metrics on http://{}/metrics (also /health, /ready, POST /drain)", l.local_addr()?));
                let app = Router::new()
                    .route("/metrics", get(metrics))
                    .route("/health", get(|| async { "ok" }))
                    .route("/ready", get(ready))
                    .route("/drain", axum::routing::post(drain))
                    .with_state(gw.clone());
                tokio::spawn(async move {
                    let _ = axum::serve(l, app).await;
                });
            }
            Err(e) => log("start", format!("metrics: can't listen on {metrics_addr}: {e}")),
        }
    }
    // The listeners are tasks: aborting them closes the ports.
    let app = Router::new().route("/ws", get(ws)).fallback(fallback).with_state(gw.clone());
    let web_task = tokio::spawn(async move {
        let _ = axum::serve(web, app).await;
    });
    let chat_task = {
        let gw = gw.clone();
        tokio::spawn(async move {
            loop {
                let sock = match chat.accept().await {
                    Ok((s, _)) => s,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let _ = sock.set_nodelay(true);
                let (r, w) = sock.into_split();
                tokio::spawn(client(gw.clone(), ConnRead::Tcp(r), ConnWrite::Tcp(w)));
            }
        })
    };
    // Ctrl+C or POST /drain: stop listening, everyone comes back later (to
    // another gateway, through the load balancer), exit.
    tokio::select! {
        r = tokio::signal::ctrl_c() => r?,
        _ = gw.draining.cancelled() => {}
    }
    gw.draining.cancel();
    chat_task.abort();
    web_task.abort();
    let n = gw.open.load(Ordering::Relaxed);
    gw.send_away();
    log("stop", format!("draining: ports closed, {n} connections told to come back within {} s", gw.spread));
    // The goodbyes go out (each writer closes within ~1 s).
    let deadline = Instant::now() + Duration::from_secs(3);
    while gw.open.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    log("stop", "exiting");
    std::process::exit(0)
}
