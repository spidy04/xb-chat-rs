// xb-server: the xb-chat server in Rust. Phones and browsers connect here
// (or through connection gateways); the protocol is in PROTOCOL.md, kept
// with the clients.
//
//   cargo run --release
//
// Environment:
//   XB_PORT         chat port for TCP clients (default 5222)
//   XB_WEB_PORT     WebSocket (/ws), the web app, and media links over HTTP (default 8080)
//   XB_HOST         bind address (default 127.0.0.1; 0.0.0.0 to expose)
//   XB_DATA         data folder (default ./data)
//   XB_APP_DIR      web app to host (default <XB_ROOT>/clients/web; "none" = no app)
//   XB_ROOT         the xb-chat repository, for the web app and its shared
//                   protocol files (default ../xb-chat)
//   XB_PUBLIC_URL   public base URL of the web port
//   XB_ORIGINS      extra allowed web origins, comma separated
//   XB_SECRET       HMAC secret for media upload tokens (generated and saved if unset)
//   XB_OTP_ECHO     1 = also return SMS codes to the client (development only)
//   XB_LEGACY_PASSWORD  1 = also allow the legacy username + password login
//   XB_FCM_CREDENTIALS  Firebase service-account key (JSON) for push wake-ups
// Media: files are always the media service's (xb-media); this process
// only signs upload tokens (with XB_SECRET, the same as xb-media's).
//   XB_MEDIA_URL    the media service's public URL, put in file links
//                   (default: this web port, which passes /upload and /m/ through)
//   XB_MEDIA_UPSTREAM  where this process reaches the media service
//                   (default: XB_MEDIA_URL if set, else http://127.0.0.1:8081)
// Tests only:
//   XB_TEST_ADMIN=1  POST /__test reads the store; XB_FAKE_PUSH=1 records pushes;
//   XB_LINK_TTL_MS, XB_PUSH_COOLDOWN_MS, XB_CODE_RESEND_MS override those timings.
// Overload protection (defaults in brackets):
//   XB_MAX_CONNECTIONS [1000000]  sockets above this are refused
//   XB_MAX_HANDSHAKES [1024]      Noise handshakes running at once
//   XB_LOGIN_RATE [5000] / XB_LOGIN_BURST [10000]  logins a second; more get "busy"
//   XB_SEND_QUEUE_MAX [16 MB] / XB_WRITE_TIMEOUT_MS [30000]  a slower reader is closed
// Offline queue (write-back, see store.rs / queue_db.rs):
//   XB_QUEUE_WRITE_DELAY_MS [5000]  written to disk only if still undelivered after this; 0 = at once
//   XB_QUEUE_COLD_MS [300000]       older stanzas of offline devices leave memory
// Monitoring:
//   XB_METRICS_ADDR [127.0.0.1:9091]  /metrics (Prometheus), /health, /ready, POST /drain; "none" = off
// Restarts without dropping everyone at once (see README, "Upgrades"):
//   Ctrl+C or POST /drain: stop listening, tell every client to come back
//   within XB_DRAIN_SPREAD_S [10] seconds (spread out), save everything, exit.
//   XB_TAKEOVER=1  start the new version while the old one runs: it waits
//                  for the ports and data files (at most XB_TAKEOVER_WAIT_S [120]).
// Replica pair (replica.rs): two nodes, each with its own data folder, both
// started with the other's replication address. One serves, one follows.
//   XB_PEER         the other node's replication address (turns this on)
//   XB_REPL_ADDR [127.0.0.1:5300]  ours
//   XB_REPL_SECRET  shared secret for the link (the same on both)
//   XB_NODE_ID      tie-break when both start at once (default XB_REPL_ADDR)
//   XB_FAILOVER_S [10]  the primary gone this long: the standby takes over
//   XB_PEER_WAIT_S [5]  at startup, look this long for the peer
//   XB_REPL_MAX_MB [512]  a standby further behind than this is dropped
// Connection gateways (relay.rs, src/bin/xb-gateway.rs):
//   XB_GATEWAY_ADDR    where gateways connect (turns this on)
//   XB_GATEWAY_SECRET  shared secret of the link (the same on the gateways)

use base64::Engine;
use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD as B64URL};
use db::{open_store, write_json, writer};
use server::{Config, Hub};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use transport::{ConnRead, ConnWrite};
use util::log;
use xb_core::*;

mod web;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn env_num<T: std::str::FromStr>(k: &str, default: T) -> T {
    env(k).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn load_secret(data: &PathBuf) -> std::io::Result<String> {
    if let Some(s) = env("XB_SECRET") {
        return Ok(s);
    }
    let file = data.join("secret");
    match std::fs::read_to_string(&file) {
        Ok(s) => Ok(s.trim().to_string()),
        Err(_) => {
            let s = B64URL.encode(rand::random::<[u8; 32]>());
            std::fs::create_dir_all(data)?;
            util::write_private(&file.to_string_lossy(), s.as_bytes())?;
            Ok(s)
        }
    }
}

/// Listens on host:port. On a takeover, keeps trying while the old process
/// still holds the port.
async fn bind(host: &str, port: u16, takeover: bool, until: std::time::Instant) -> std::io::Result<TcpListener> {
    loop {
        match TcpListener::bind((host, port)).await {
            Err(e) if takeover && e.kind() == std::io::ErrorKind::AddrInUse && std::time::Instant::now() < until => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            r => return r,
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let data = std::path::absolute(env("XB_DATA").unwrap_or("data".into()))?;
    let host = env("XB_HOST").unwrap_or("127.0.0.1".into());
    let port: u16 = env_num("XB_PORT", 5222);
    let web_port: u16 = env_num("XB_WEB_PORT", 8080);
    let root = std::path::absolute(env("XB_ROOT").unwrap_or("../xb-chat".into()))?;

    // Commands: export-json (write state.json), import-json (read it back).
    match std::env::args().nth(1).as_deref() {
        Some("export-json") => {
            let (store, _db) = open_store(&data, false)?;
            write_json(&store, &data.join("state.json"))?;
            log("store", format!("wrote {}", data.join("state.json").display()));
            return Ok(());
        }
        Some("import-json") => {
            open_store(&data, true)?;
            return Ok(());
        }
        Some(other) => {
            eprintln!("unknown command {other:?}; commands: export-json, import-json (or none, to run the server)");
            std::process::exit(2);
        }
        None => {}
    }
    // XB_PEER: one of a replica pair (replica.rs). Serve, or follow the
    // other node until it's gone; this returns when this node is to serve.
    let pair = match env("XB_PEER") {
        Some(peer) => {
            let listen = env("XB_REPL_ADDR").unwrap_or("127.0.0.1:5300".into());
            let secret = env("XB_REPL_SECRET").ok_or_else(|| std::io::Error::other("XB_PEER needs XB_REPL_SECRET (the same on both nodes)"))?;
            let cfg = replica::PairConfig {
                id: env("XB_NODE_ID").unwrap_or(listen.clone()),
                peer,
                listen,
                secret,
                failover: Duration::from_secs(env_num("XB_FAILOVER_S", 10)),
                peer_wait: Duration::from_secs(env_num("XB_PEER_WAIT_S", 5)),
                max_queue_bytes: env_num("XB_REPL_MAX_MB", 512usize) * 1024 * 1024,
            };
            let p = replica::Pair::new(cfg, &data);
            p.clone().listen().await?;
            p.decide().await?;
            Some(p)
        }
        None => None,
    };

    // XB_TAKEOVER=1: an upgrade. Started while the old process still runs,
    // this one waits for it: it takes the ports the moment the old one lets
    // go (new connections then wait in the system's queue instead of being
    // refused), and the data files the moment it exits.
    let takeover = env("XB_TAKEOVER").as_deref() == Some("1");
    let wait_until = std::time::Instant::now() + Duration::from_secs(env_num("XB_TAKEOVER_WAIT_S", 120));
    if takeover {
        log("start", "takeover: waiting for the running server to drain");
    }

    // Bind first, so the media links can name the real port (XB_WEB_PORT=0 in tests).
    let web_listener = bind(&host, web_port, takeover, wait_until).await?;
    let web_port = web_listener.local_addr()?.port();
    let chat_listener = bind(&host, port, takeover, wait_until).await?;
    let port = chat_listener.local_addr()?.port();

    let (store, db) = loop {
        match open_store(&data, false) {
            Ok(s) => break s,
            // Still open in the old process (redb locks its file).
            Err(e) if takeover && std::time::Instant::now() < wait_until => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e),
        }
    };
    if takeover {
        log("start", "takeover: data files open");
    }
    let mut store = store;
    store.repl = pair.as_ref().map(|p| p.sink.clone());
    let notify = store.notify.clone();
    let (users, groups) = (store.users.len(), store.groups.len());

    let (private_key, public_key) = keys::load_or_create(&data.join("server_key").to_string_lossy())?;

    let public_host = if host == "0.0.0.0" { "127.0.0.1".to_string() } else { host.clone() };
    let public_url = env("XB_PUBLIC_URL").unwrap_or(format!("http://{public_host}:{web_port}")).trim_end_matches('/').to_string();
    // Files are the media service's (xb-media). Phones get its URL and an
    // upload token signed here; by default the links point at this web
    // port, which passes /upload and /m/ through to it (one port for phones).
    let media_url = env("XB_MEDIA_URL").map(|u| u.trim_end_matches('/').to_string());
    let media_upstream = env("XB_MEDIA_UPSTREAM").map(|u| u.trim_end_matches('/').to_string()).or(media_url.clone()).unwrap_or("http://127.0.0.1:8081".into());
    let uploads = server::Uploads { tokens: xb_proto::upload_token::UploadTokens::new(load_secret(&data)?), public_url: media_url.unwrap_or(public_url) };
    let media_public = uploads.public_url.clone();

    let push = if env("XB_FAKE_PUSH").as_deref() == Some("1") {
        Some(Arc::new(push::Pusher::Fake(Default::default())))
    } else if let Some(cred) = env("XB_FCM_CREDENTIALS") {
        let p = push::FcmPusher::from_file(&cred).map_err(std::io::Error::other)?;
        Some(Arc::new(push::Pusher::Fcm(p)))
    } else {
        None
    };
    if let Some(p) = &push {
        push::describe(p);
    }

    let cfg = Config {
        otp_echo: env("XB_OTP_ECHO").as_deref() == Some("1"),
        legacy_password: env("XB_LEGACY_PASSWORD").as_deref() == Some("1"),
        link_ttl_ms: env_num("XB_LINK_TTL_MS", 60_000),
        push_cooldown_ms: env_num("XB_PUSH_COOLDOWN_MS", 5000),
        code_resend_ms: env_num("XB_CODE_RESEND_MS", 60_000),
        max_connections: env_num("XB_MAX_CONNECTIONS", 1_000_000),
        max_handshakes: env_num("XB_MAX_HANDSHAKES", 1024),
        login_rate: env_num("XB_LOGIN_RATE", 5000.0),
        login_burst: env_num("XB_LOGIN_BURST", 10_000.0),
        send_queue_max: env_num("XB_SEND_QUEUE_MAX", 16 * 1024 * 1024),
        write_timeout: Duration::from_millis(env_num("XB_WRITE_TIMEOUT_MS", 30_000)),
        queue_write_delay: Duration::from_millis(env_num("XB_QUEUE_WRITE_DELAY_MS", 5000)),
        queue_cold: Duration::from_millis(env_num("XB_QUEUE_COLD_MS", 300_000)),
        record: env("XB_TEST_ADMIN").as_deref() == Some("1"),
        ..Config::default()
    };
    let hub = Hub::new(store, Arc::new(db), cfg, Some(uploads), push, private_key, public_key);
    if let Some(p) = &pair {
        p.serving(hub.clone());
    }
    // Connection gateways (relay.rs, xb-gateway): they hold the phones'
    // connections and pass the stanzas to this process.
    if let Some(addr) = env("XB_GATEWAY_ADDR") {
        let secret = env("XB_GATEWAY_SECRET").ok_or_else(|| std::io::Error::other("XB_GATEWAY_ADDR needs XB_GATEWAY_SECRET (the same on the gateways)"))?;
        let bound = relay::listen(hub.clone(), &addr, secret).await?;
        log("start", format!("gateways connect on {bound}"));
    }

    // The web app is static files, plus the protocol modules it shares with the other clients.
    let app_dir = match env("XB_APP_DIR") {
        Some(d) if d == "none" => None,
        Some(d) => Some(std::path::absolute(d)?),
        None => Some(root.join("clients/web")).filter(|d| d.is_dir()),
    };
    let static_files: HashMap<String, PathBuf> = match &app_dir {
        Some(_) => ["tokens.js", "signal.js", "e2e.js"].iter().map(|f| (format!("/shared/{f}"), root.join("protocol").join(f))).collect(),
        None => HashMap::new(),
    };
    let gw = Arc::new(web::Web {
        hub: hub.clone(),
        media_upstream: media_upstream.clone(),
        http: reqwest::Client::new(),
        static_dir: app_dir.clone(),
        static_files,
        allowed_origins: env("XB_ORIGINS").map(|o| o.split(',').filter(|s| !s.is_empty()).map(str::to_string).collect()).unwrap_or_default(),
        test_admin: env("XB_TEST_ADMIN").as_deref() == Some("1"),
    });

    tokio::spawn(writer(hub.clone(), notify));
    {
        // Hourly: drop queued stanzas older than 30 days. (Expired files are
        // the media service's to sweep.)
        let hub = hub.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(3600));
            loop {
                t.tick().await;
                for i in 0..hub.shard_count() {
                    hub.shard(i).store.expire();
                }
            }
        });
    }
    {
        let hub = hub.clone();
        tokio::spawn(async move {
            // XB_STATS_SECS: how often (default every minute).
            let every = env_num("XB_STATS_SECS", 60u64).max(1);
            let mut t = tokio::time::interval(Duration::from_secs(every));
            t.tick().await;
            let (mut msgs0, mut locks0, mut wait0, mut hold0) = (0u64, 0u64, 0u64, 0u64);
            loop {
                t.tick().await;
                let (users, devices) = hub.online();
                let msgs = hub.stats.messages.load(std::sync::atomic::Ordering::Relaxed);
                let (bin, bout) = (hub.bytes_in.load(std::sync::atomic::Ordering::Relaxed), hub.bytes_out.load(std::sync::atomic::Ordering::Relaxed));
                let locks = hub.locks.load(std::sync::atomic::Ordering::Relaxed);
                let wait = hub.lock_wait_ns.load(std::sync::atomic::Ordering::Relaxed);
                let hold = hub.lock_hold_ns.load(std::sync::atomic::Ordering::Relaxed);
                let max_hold = hub.lock_hold_max_ns.swap(0, std::sync::atomic::Ordering::Relaxed);
                let (dl, dw, dh) = (locks - locks0, wait - wait0, hold - hold0);
                log(
                    "stats",
                    format!(
                        "online={users} users, {devices} devices msgs={msgs} ({:.0}/s) in={bin}B out={bout}B lock: {:.0}/s, held avg {:.1} µs max {:.1} ms ({:.0}% busy), wait avg {:.1} µs | turned away: {} refused, {} busy, {} slow",
                        (msgs - msgs0) as f64 / every as f64,
                        dl as f64 / every as f64,
                        if dl > 0 { dh as f64 / dl as f64 / 1000.0 } else { 0.0 },
                        max_hold as f64 / 1e6,
                        dh as f64 / (every as f64 * 1e9) * 100.0,
                        if dl > 0 { dw as f64 / dl as f64 / 1000.0 } else { 0.0 },
                        hub.stats.refused.load(std::sync::atomic::Ordering::Relaxed),
                        hub.stats.busy.load(std::sync::atomic::Ordering::Relaxed),
                        hub.stats.slow.load(std::sync::atomic::Ordering::Relaxed),
                    ),
                );
                (msgs0, locks0, wait0, hold0) = (msgs, locks, wait, hold);
            }
        });
    }

    // Metrics, health and readiness: local only by default.
    let metrics_addr = env("XB_METRICS_ADDR").unwrap_or("127.0.0.1:9091".into());
    if metrics_addr != "none" {
        let app = metrics::router(hub.clone());
        tokio::spawn(async move {
            // On a takeover the old process holds this port until it exits.
            let l = loop {
                match TcpListener::bind(&metrics_addr).await {
                    Ok(l) => break l,
                    Err(e) if takeover && std::time::Instant::now() < wait_until + Duration::from_secs(10) => {
                        let _ = e;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        return log("start", format!("metrics: can't listen on {metrics_addr}: {e}"));
                    }
                }
            };
            log("start", format!("metrics on http://{metrics_addr}/metrics (also /health, /ready, POST /drain)"));
            let _ = axum::serve(l, app).await;
        });
    }
    hub.drain_spread.store(env_num("XB_DRAIN_SPREAD_S", 10u64).max(1), std::sync::atomic::Ordering::Relaxed);
    // The two listeners run as tasks: aborting them closes the ports (open
    // connections are tasks of their own and keep going).
    let web = axum::serve(web_listener, web::router(gw));
    let web_task = tokio::spawn(async move {
        if let Err(e) = web.await {
            log("web", e.to_string());
        }
    });
    let chat_task = {
        let hub = hub.clone();
        tokio::spawn(async move {
            loop {
                let socket = match chat_listener.accept().await {
                    Ok((socket, _)) => socket,
                    Err(e) => {
                        // Out of file handles, say: pause instead of spinning
                        // on the error at full CPU.
                        log("accept", e.to_string());
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let _ = socket.set_nodelay(true);
                let (r, w) = socket.into_split();
                let conn = server::serve(hub.clone(), ConnRead::Tcp(r), ConnWrite::Tcp(w));
                if env("XB_SIZES").is_some() {
                    log("sizes", format!("one connection's reader task: {} bytes", std::mem::size_of_val(&conn)));
                }
                tokio::spawn(conn);
            }
        })
    };

    log("start", format!("chat on {host}:{port} (Noise XX), data in {}", data.display()));
    log("start", format!("web gateway on http://{}:{web_port} (WebSocket /ws, media {media_public} -> {media_upstream})", if host == "0.0.0.0" { "localhost" } else { &host }));
    log(
        "start",
        match &app_dir {
            Some(d) => format!("web app from {}", d.display()),
            None => "no web app hosted (XB_APP_DIR=none)".into(),
        },
    );
    log("start", format!("server key: {}", B64.encode(public_key)));
    log("start", format!("{users} users, {groups} groups"));

    // Ctrl+C or POST /drain: drain, so a restart doesn't drop everyone at once.
    tokio::select! {
        r = tokio::signal::ctrl_c() => r?,
        _ = hub.draining.cancelled() => {}
    }
    hub.draining.cancel();
    let t = std::time::Instant::now();
    // 1. Stop listening: a new process (XB_TAKEOVER=1) can take the ports now.
    chat_task.abort();
    web_task.abort();
    let _ = tokio::join!(chat_task, web_task);
    // 2. Everyone: "restart, come back in N s", N spread over XB_DRAIN_SPREAD_S.
    let told = hub.restart_all();
    log("stop", format!("draining: ports closed, {told} connections told to come back within {} s", hub.drain_spread.load(std::sync::atomic::Ordering::Relaxed)));
    // 3. Wait for the goodbyes to go out (each writer closes within ~1 s).
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while hub.stats.open.load(std::sync::atomic::Ordering::Acquire) > 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // 4. Everything to disk: records and every queued stanza.
    db::flush_all(&hub).await;
    // 5. A replica pair: the standby takes over now, with all of it.
    if let Some(p) = &pair {
        let ok = p.hand_over().await;
        log("stop", if ok { "the standby has taken over" } else { "no standby took over" });
    }
    log("stop", format!("drained in {} ms ({} connections still open), exiting", t.elapsed().as_millis(), hub.stats.open.load(std::sync::atomic::Ordering::Relaxed)));
    // Exit now (rather than returning): the data files are released at once,
    // without waiting for leftover connection tasks.
    std::process::exit(0)
}
