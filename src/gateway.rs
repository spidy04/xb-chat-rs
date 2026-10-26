//! Web gateway (server/gateway.js): the chat protocol for browsers on one
//! HTTP origin.
//!   /ws          WebSocket carrying the same bytes as TCP (header, Noise,
//!                encrypted stanzas), one frame per WebSocket message
//!   /upload, /m  the media server (same origin: no CORS needed)
//!   everything else: the web app's static files, if one is configured

use crate::media::{MediaServer, percent_decode};
use crate::server::{Hub, serve};
use crate::transport::{ConnRead, ConnWrite};
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::extract::ws::{WebSocketUpgrade, rejection::WebSocketUpgradeRejection};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

const SECURITY_HEADERS: [(&str, &str); 4] = [
    (
        "content-security-policy",
        "default-src 'self'; img-src 'self' blob: data:; media-src 'self' blob:; connect-src 'self' ws: wss:; style-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
    ),
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "no-referrer"),
    ("x-frame-options", "DENY"),
];

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()).unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "json" => "application/json",
        "webmanifest" => "application/manifest+json",
        _ => "application/octet-stream",
    }
}

pub struct Gateway {
    pub hub: Arc<Hub>,
    pub media: Option<Arc<MediaServer>>,
    /// Folder served at / (None = no web app).
    pub static_dir: Option<PathBuf>,
    /// Single files by URL path: "/shared/tokens.js" -> file.
    pub static_files: HashMap<String, PathBuf>,
    pub allowed_origins: Vec<String>,
    /// Tests only (XB_TEST_ADMIN=1): POST /__test reads the store.
    pub test_admin: bool,
    /// Files are on a separate media service: /upload and /m/ are streamed
    /// through to it (for apps that still use this port for media).
    pub media_upstream: Option<String>,
    pub http: reqwest::Client,
}

pub fn router(gw: Arc<Gateway>) -> Router {
    Router::new().route("/ws", get(ws)).route("/__test", any(test_admin)).fallback(fallback).with_state(gw)
}

fn text(status: u16, body: &'static str) -> Response {
    let mut b = Response::builder().status(status).header(header::CONTENT_TYPE, "text/plain");
    for (k, v) in SECURITY_HEADERS {
        b = b.header(k, v);
    }
    b.body(Body::from(body)).unwrap()
}

impl Gateway {
    /// Same-origin pages are always allowed; others only if listed.
    fn origin_ok(&self, headers: &HeaderMap) -> bool {
        let Some(origin) = headers.get(header::ORIGIN).and_then(|o| o.to_str().ok()) else {
            return true;
        }; // not a browser
        let host = headers.get(header::HOST).and_then(|h| h.to_str().ok()).unwrap_or("");
        let origin_host = origin.split_once("://").map(|(_, rest)| rest.split('/').next().unwrap_or("")).unwrap_or("");
        if !origin_host.is_empty() && origin_host.eq_ignore_ascii_case(host) {
            return true;
        }
        self.allowed_origins.iter().any(|o| o == origin)
    }

    async fn serve_static(&self, method: &Method, uri: &Uri) -> Response {
        if method != Method::GET && method != Method::HEAD {
            return text(405, "Method Not Allowed");
        }
        let file = match self.static_files.get(uri.path()) {
            Some(f) => f.clone(),
            None => {
                let Some(dir) = &self.static_dir else {
                    return text(404, "Not Found");
                };
                let rel = if uri.path() == "/" { "index.html".to_string() } else { percent_decode(uri.path())[1..].to_string() };
                match inside(dir, &rel) {
                    Some(f) => f,
                    None => return text(404, "Not Found"), // no escaping the folder with ../
                }
            }
        };
        let Ok(body) = tokio::fs::read(&file).await else {
            return text(404, "Not Found");
        };
        let mut b = Response::builder().status(StatusCode::OK).header(header::CONTENT_TYPE, content_type(&file)).header(header::CONTENT_LENGTH, body.len()).header(header::CACHE_CONTROL, "no-cache");
        for (k, v) in SECURITY_HEADERS {
            b = b.header(k, v);
        }
        b.body(if method == Method::HEAD { Body::empty() } else { Body::from(body) }).unwrap()
    }
}

/// `rel` under `dir`, or None if it would leave it.
fn inside(dir: &Path, rel: &str) -> Option<PathBuf> {
    let rel = Path::new(rel);
    let mut out = dir.to_path_buf();
    let mut depth = 0usize;
    for c in rel.components() {
        match c {
            Component::Normal(p) => {
                out.push(p);
                depth += 1;
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if depth == 0 {
                    return None;
                }
                out.pop();
                depth -= 1;
            }
            _ => return None, // absolute paths, drive letters
        }
    }
    (depth > 0).then_some(out)
}

async fn ws(State(gw): State<Arc<Gateway>>, method: Method, uri: Uri, headers: HeaderMap, upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>) -> Response {
    let Ok(upgrade) = upgrade else {
        return gw.serve_static(&method, &uri).await;
    };
    // Blocks other websites from opening a connection from a visitor's browser.
    if !gw.origin_ok(&headers) {
        let mut res = text(403, "Forbidden");
        res.headers_mut().insert(header::CONNECTION, "close".parse().unwrap());
        return res;
    }
    let hub = gw.hub.clone();
    let max = hub.cfg.max_stanza + 1024;
    upgrade.max_message_size(max).max_frame_size(max).on_upgrade(move |socket| async move {
        let (sink, stream) = socket.split();
        serve(hub, ConnRead::Ws(stream), ConnWrite::Ws(sink)).await;
    })
}

async fn fallback(State(gw): State<Arc<Gateway>>, method: Method, uri: Uri, headers: HeaderMap, body: Body) -> Response {
    let p = uri.path();
    let is_media = p == "/upload" || p.starts_with("/upload/") || p.starts_with("/m/");
    if is_media {
        if let Some(up) = &gw.media_upstream {
            return pass_through(gw.as_ref(), up, method, uri, headers, body).await;
        }
        if let Some(media) = &gw.media {
            return media.handle(method, uri, headers, body).await;
        }
    }
    gw.serve_static(&method, &uri).await
}

/// Streams a media request to the media service and its answer back: no
/// buffering, no disk, no hashing here (only the bytes pass through).
async fn pass_through(gw: &Gateway, upstream: &str, method: Method, uri: Uri, headers: HeaderMap, body: Body) -> Response {
    let url = format!("{upstream}{}", uri.path_and_query().map(|p| p.as_str()).unwrap_or("/"));
    let Ok(m) = reqwest::Method::from_bytes(method.as_str().as_bytes()) else {
        return text(405, "Method Not Allowed");
    };
    let mut req = gw.http.request(m, url);
    for name in [header::AUTHORIZATION, header::CONTENT_TYPE, header::CONTENT_LENGTH] {
        if let Some(v) = headers.get(&name) {
            req = req.header(name.as_str(), v.as_bytes());
        }
    }
    if method == Method::POST {
        req = req.body(reqwest::Body::wrap_stream(body.into_data_stream()));
    }
    let res = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            crate::util::log("media", format!("media service unreachable: {e}"));
            return text(502, "Bad Gateway");
        }
    };
    let mut b = Response::builder().status(res.status().as_u16());
    for name in [header::CONTENT_TYPE, header::CONTENT_LENGTH, header::CACHE_CONTROL] {
        if let Some(v) = res.headers().get(name.as_str()) {
            b = b.header(name, v.as_bytes());
        }
    }
    b.body(Body::from_stream(res.bytes_stream())).unwrap_or_else(|_| text(502, "Bad Gateway"))
}

/// Tests only: lets a test read the server's state, like the Node tests
/// read `chat.store`. Off unless XB_TEST_ADMIN=1.
async fn test_admin(State(gw): State<Arc<Gateway>>, body: String) -> Response {
    if !gw.test_admin {
        return text(404, "Not Found");
    }
    let req: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    let f = req["fn"].as_str().unwrap_or("");
    let arg = |i: usize| req["args"][i].as_str().unwrap_or("").to_string();
    let out = crate::admin::call(&gw.hub, f, arg(0), arg(1)).await;
    out.to_string().into_response()
}
