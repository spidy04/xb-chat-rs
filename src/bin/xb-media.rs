//! xb-media: the media service on its own (like WhatsApp's separate media
//! servers). Encrypted files only: upload, resumable upload, download,
//! expiry. No chat state at all, so it can run on other machines, scale on
//! its own, and an upload burst never slows chat down.
//!
//!   cargo run --release --bin xb-media
//!
//! The chat server issues the upload tokens; this service checks them with
//! the same secret. Run the chat server with XB_MEDIA_URL=<this service's
//! public URL>, and both with the same XB_SECRET (or the same data folder,
//! where the secret is saved).
//!
//! Environment:
//!   XB_MEDIA_HOST [127.0.0.1]  bind address (0.0.0.0 to expose)
//!   XB_MEDIA_PORT [8081]       port
//!   XB_DATA [./data]           where `secret` is (shared with the chat server)
//!   XB_MEDIA_DIR [<data>/media] where the files go
//!   XB_SECRET                  the token secret (instead of <data>/secret)
//!   XB_MEDIA_PUBLIC_URL        base URL put in file links (default http://<host>:<port>)
//!   XB_MEDIA_MAX_MB [64]       largest file
//!   XB_METRICS_ADDR [127.0.0.1:9094]  /metrics (Prometheus) and /health; "none" = off

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use xb_server::media::MediaServer;
use xb_server::util::log;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

/// The token secret: XB_SECRET, or the one the chat server saved in <data>/secret.
fn secret(data: &std::path::Path) -> std::io::Result<String> {
    if let Some(s) = env("XB_SECRET") {
        return Ok(s);
    }
    std::fs::read_to_string(data.join("secret"))
        .map(|s| s.trim().to_string())
        .map_err(|e| std::io::Error::other(format!("no token secret: set XB_SECRET, or start the chat server once on this data folder ({e})")))
}

/// Any web page may call this (tokens are bearer tokens, never cookies, so
/// there's nothing a foreign page could borrow).
fn cors(mut res: Response) -> Response {
    let h = res.headers_mut();
    h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("authorization, content-type"));
    h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST, OPTIONS"));
    h.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("content-length"));
    res
}

/// Counters for /metrics, per kind of request.
#[derive(Default)]
struct Kind {
    requests: AtomicU64,
    bytes: AtomicU64,
    rejected: AtomicU64, // 4xx: bad token, too big, not found…
    errors: AtomicU64,   // 5xx
    seconds_ns: AtomicU64,
}

#[derive(Default)]
struct Stats {
    upload: Kind,
    download: Kind,
}

struct App {
    media: Arc<MediaServer>,
    stats: Stats,
}

fn content_length(h: &HeaderMap) -> u64 {
    h.get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok()).unwrap_or(0)
}

async fn handle(State(app): State<Arc<App>>, method: Method, uri: Uri, headers: HeaderMap, body: Body) -> Response {
    if method == Method::OPTIONS {
        return cors(StatusCode::NO_CONTENT.into_response());
    }
    if uri.path() == "/health" {
        return "ok".into_response();
    }
    let p = uri.path();
    let kind = if p == "/upload" || p.starts_with("/upload/") {
        Some((&app.stats.upload, content_length(&headers)))
    } else if p.starts_with("/m/") {
        Some((&app.stats.download, 0))
    } else {
        None
    };
    let t = Instant::now();
    let res = app.media.handle(method, uri, headers, body).await;
    if let Some((k, sent)) = kind {
        k.requests.fetch_add(1, Relaxed);
        k.seconds_ns.fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
        let status = res.status().as_u16();
        if status >= 500 {
            k.errors.fetch_add(1, Relaxed);
        } else if status >= 400 {
            k.rejected.fetch_add(1, Relaxed);
        } else {
            // Uploads: what came in; downloads: what goes out (streamed, so as announced).
            k.bytes.fetch_add(if sent > 0 { sent } else { content_length(res.headers()) }, Relaxed);
        }
    }
    cors(res)
}

async fn metrics(State(app): State<Arc<App>>) -> Response {
    let mut out = String::with_capacity(1024);
    for (name, k) in [("upload", &app.stats.upload), ("download", &app.stats.download)] {
        let mut m = |metric: &str, kind: &str, help: &str, v: f64| xb_server::metrics::write_metric(&mut out, &format!("xb_media_{name}_{metric}"), kind, &format!("{name}s: {help}"), v);
        m("requests_total", "counter", "requests", k.requests.load(Relaxed) as f64);
        m("bytes_total", "counter", "bytes of file data", k.bytes.load(Relaxed) as f64);
        m("rejected_total", "counter", "refused (4xx: bad token, too big, not found)", k.rejected.load(Relaxed) as f64);
        m("errors_total", "counter", "failed (5xx)", k.errors.load(Relaxed) as f64);
        m("seconds_total", "counter", "time spent (a download's until its body starts)", k.seconds_ns.load(Relaxed) as f64 / 1e9);
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let data = std::path::absolute(env("XB_DATA").unwrap_or("data".into()))?;
    let host = env("XB_MEDIA_HOST").unwrap_or("127.0.0.1".into());
    let port: u16 = env("XB_MEDIA_PORT").and_then(|p| p.parse().ok()).unwrap_or(8081);
    let dir = env("XB_MEDIA_DIR").map(std::path::PathBuf::from).unwrap_or(data.join("media"));
    std::fs::create_dir_all(&dir)?;

    let listener = TcpListener::bind((host.as_str(), port)).await?;
    let port = listener.local_addr()?.port();
    let public_host = if host == "0.0.0.0" { "127.0.0.1".to_string() } else { host.clone() };
    let public_url = env("XB_MEDIA_PUBLIC_URL").unwrap_or(format!("http://{public_host}:{port}")).trim_end_matches('/').to_string();
    let mut media = MediaServer::new(dir.clone(), secret(&data)?, public_url.clone());
    if let Some(mb) = env("XB_MEDIA_MAX_MB").and_then(|m| m.parse::<u64>().ok()) {
        media.max_bytes = mb * 1024 * 1024;
    }
    let media = Arc::new(media);

    {
        // Hourly: delete expired files.
        let media = media.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(3600));
            loop {
                t.tick().await;
                let m = media.clone();
                let _ = tokio::task::spawn_blocking(move || m.sweep()).await;
            }
        });
    }

    log("start", format!("xb-media on {host}:{port}, files in {}, links {public_url}/m/…", dir.display()));
    let app = Arc::new(App { media, stats: Stats::default() });
    // Metrics on their own port, local by default (like the chat server's).
    let metrics_addr = env("XB_METRICS_ADDR").unwrap_or("127.0.0.1:9094".into());
    if metrics_addr != "none" {
        match TcpListener::bind(&metrics_addr).await {
            Ok(l) => {
                log("start", format!("metrics on http://{}/metrics (also /health)", l.local_addr()?));
                let m = Router::new().route("/metrics", axum::routing::get(metrics)).route("/health", axum::routing::get(|| async { "ok" })).with_state(app.clone());
                tokio::spawn(async move {
                    let _ = axum::serve(l, m).await;
                });
            }
            Err(e) => log("start", format!("metrics: can't listen on {metrics_addr}: {e}")),
        }
    }
    let router = Router::new().fallback(handle).with_state(app);
    axum::serve(listener, router).await
}
