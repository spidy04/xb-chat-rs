//! Media server (server/media.js): a temporary locker for ENCRYPTED files.
//! Clients encrypt with a fresh key, upload the ciphertext, and send the URL
//! and key inside an end-to-end encrypted message. We never see a key.
//!
//!   POST /upload                 Authorization: Bearer <token>, body = ciphertext -> { url, sha256, size }
//!   GET  /m/<sha256>             -> ciphertext
//!   GET  /upload/<sha256>        -> { offset } bytes already here (or { url … } if complete)
//!   POST /upload?sha256&total&offset   body = bytes offset.. -> { url … } or { offset }
//!
//! Files are named by the SHA-256 of their ciphertext, so forwards are
//! deduplicated, and deleted after 30 days. Partial uploads after a day.

use crate::util::{log, now_ms, sha256_hex};
use axum::body::Body;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::Response;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;

const PART_TTL_MS: u64 = 24 * 3600 * 1000;

pub struct MediaServer {
    pub dir: PathBuf,
    secret: String,
    pub public_url: String,
    pub max_bytes: u64,
    pub ttl_ms: u64,
    pub token_ttl_ms: u64,
    busy: Mutex<HashSet<PathBuf>>,
}

fn is_sha(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn is_part(s: &str) -> bool {
    // <sha256>.<16 hex>.part
    let p: Vec<&str> = s.split('.').collect();
    p.len() == 3 && is_sha(p[0]) && p[1].len() == 16 && p[1].bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) && p[2] == "part"
}

fn json(status: u16, v: Value) -> Response {
    let body = v.to_string();
    Response::builder().status(status).header(header::CONTENT_TYPE, "application/json").header(header::CONTENT_LENGTH, body.len()).body(Body::from(body)).unwrap()
}

/// Sets a file's modification time to now (refreshes its expiry).
fn touch(path: &PathBuf) -> std::io::Result<()> {
    std::fs::File::options().write(true).open(path)?.set_modified(SystemTime::now())
}

impl MediaServer {
    pub fn new(dir: PathBuf, secret: String, public_url: String) -> MediaServer {
        MediaServer { dir, secret, public_url, max_bytes: 64 * 1024 * 1024, ttl_ms: 30 * 24 * 3600 * 1000, token_ttl_ms: 10 * 60 * 1000, busy: Mutex::new(HashSet::new()) }
    }

    fn mac(&self, body: &str) -> Vec<u8> {
        let mut m = <Hmac<Sha256> as Mac>::new_from_slice(self.secret.as_bytes()).expect("any key length");
        m.update(body.as_bytes());
        m.finalize().into_bytes().to_vec()
    }

    /// A short-lived upload token, handed out over the chat connection.
    pub fn issue_token(&self, user: &str) -> (String, u64) {
        let exp = now_ms() + self.token_ttl_ms;
        let body = format!("{user}.{exp}");
        let sig = B64URL.encode(self.mac(&body));
        (format!("{body}.{sig}"), exp)
    }

    pub fn check_token(&self, token: &str) -> Option<String> {
        let parts: Vec<&str> = token.split('.').collect();
        let [user, exp, sig] = parts[..] else {
            return None;
        };
        let want = self.mac(&format!("{user}.{exp}"));
        let got = B64URL.decode(sig.trim_end_matches('=')).ok()?;
        if !crate::util::same(&got, &want) {
            return None;
        }
        if exp.parse::<f64>().ok()? < now_ms() as f64 {
            return None;
        }
        Some(user.to_string())
    }

    fn authorize(&self, headers: &HeaderMap) -> Option<String> {
        let auth = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).unwrap_or("");
        self.check_token(auth.strip_prefix("Bearer ").unwrap_or(auth))
    }

    fn file_path(&self, id: &str) -> PathBuf {
        self.dir.join(id)
    }

    /// One partial file per (file, user): nobody can add to another user's upload.
    fn part_path(&self, id: &str, user: &str) -> PathBuf {
        self.dir.join(format!("{id}.{}.part", &sha256_hex(user.as_bytes())[..16]))
    }

    fn done_json(&self, id: &str, size: u64) -> Value {
        json!({ "url": format!("{}/m/{id}", self.public_url), "sha256": id, "size": size })
    }

    /// The finished file's answer, refreshing its expiry (like a forward).
    fn finished(&self, id: &str) -> Option<Value> {
        let path = self.file_path(id);
        let meta = std::fs::metadata(&path).ok()?;
        let _ = touch(&path);
        Some(self.done_json(id, meta.len()))
    }

    pub async fn handle(&self, method: Method, uri: Uri, headers: HeaderMap, body: Body) -> Response {
        let path = uri.path();
        let query: HashMap<String, String> = uri.query().map(|q| url_decode_pairs(q)).unwrap_or_default();
        if method == Method::POST && path == "/upload" {
            if query.contains_key("sha256") {
                return self.upload_part(&headers, &query, body).await;
            }
            return self.upload(&headers, body).await;
        }
        if method == Method::GET {
            if let Some(id) = path.strip_prefix("/upload/").filter(|id| is_sha(id)) {
                return self.upload_status(&headers, id);
            }
            if let Some(id) = path.strip_prefix("/m/").filter(|id| is_sha(id)) {
                return self.download(id).await;
            }
        }
        json(404, json!({"error": "not-found"}))
    }

    async fn upload(&self, headers: &HeaderMap, body: Body) -> Response {
        if self.authorize(headers).is_none() {
            return json(401, json!({"error": "not-authorized"}));
        }
        let declared = headers.get(header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
        if declared.is_some_and(|d| d > self.max_bytes) {
            return json(413, json!({"error": "too-large"}));
        }
        let mut data = Vec::new();
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else {
                return json(400, json!({"error": "bad-request"}));
            };
            if (data.len() + chunk.len()) as u64 > self.max_bytes {
                return json(413, json!({"error": "too-large"}));
            }
            data.extend_from_slice(&chunk);
        }
        if data.is_empty() {
            return json(400, json!({"error": "empty"}));
        }
        let id = sha256_hex(&data);
        let file = self.file_path(&id);
        // Already have it (a forward): just refresh its expiry.
        if touch(&file).is_err() {
            let tmp = self.dir.join(format!("{id}.{}.{}.tmp", std::process::id(), now_ms()));
            if tokio::fs::write(&tmp, &data).await.is_err() || tokio::fs::rename(&tmp, &file).await.is_err() {
                return json(500, json!({"error": "internal-error"}));
            }
        }
        json(200, self.done_json(&id, data.len() as u64))
    }

    fn upload_status(&self, headers: &HeaderMap, id: &str) -> Response {
        let Some(user) = self.authorize(headers) else {
            return json(401, json!({"error": "not-authorized"}));
        };
        if let Some(done) = self.finished(id) {
            return json(200, done);
        }
        let have = std::fs::metadata(self.part_path(id, &user)).map(|m| m.len()).unwrap_or(0);
        json(200, json!({ "offset": have }))
    }

    async fn upload_part(&self, headers: &HeaderMap, q: &HashMap<String, String>, body: Body) -> Response {
        let Some(user) = self.authorize(headers) else {
            return json(401, json!({"error": "not-authorized"}));
        };
        let id = q.get("sha256").cloned().unwrap_or_default();
        let total = q.get("total").and_then(|v| v.parse::<u64>().ok());
        let offset = match q.get("offset") {
            None => Some(0),
            Some(v) if v.is_empty() => Some(0),
            Some(v) => v.parse::<u64>().ok(),
        };
        let (Some(total), Some(offset)) = (total, offset) else {
            return json(400, json!({"error": "bad-request"}));
        };
        if !is_sha(&id) || total == 0 {
            return json(400, json!({"error": "bad-request"}));
        }
        if total > self.max_bytes {
            return json(413, json!({"error": "too-large"}));
        }
        if let Some(done) = self.finished(&id) {
            return json(200, done);
        }
        let part = self.part_path(&id, &user);
        if !self.busy.lock().unwrap().insert(part.clone()) {
            return json(409, json!({"error": "busy"}));
        }
        let res = self.append(&id, &part, total, offset, body).await;
        self.busy.lock().unwrap().remove(&part);
        res
    }

    async fn append(&self, id: &str, part: &PathBuf, total: u64, offset: u64, body: Body) -> Response {
        let have = std::fs::metadata(part).map(|m| m.len()).unwrap_or(0);
        // Must continue exactly where the partial file ends.
        if offset != have {
            return json(409, json!({"error": "offset", "offset": have}));
        }
        // Appends as the bytes arrive: what made it here before a drop stays.
        let Ok(mut fh) = tokio::fs::OpenOptions::new().create(true).append(true).open(part).await else {
            return json(500, json!({"error": "internal-error"}));
        };
        let mut size = have;
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else { break }; // connection dropped: keep what arrived
            if size + chunk.len() as u64 > total {
                let _ = fh.flush().await;
                return json(400, json!({"error": "too-long"}));
            }
            if fh.write_all(&chunk).await.is_err() {
                return json(500, json!({"error": "internal-error"}));
            }
            size += chunk.len() as u64;
        }
        let _ = fh.flush().await;
        drop(fh);
        if size < total {
            return json(200, json!({ "offset": size }));
        }
        let Ok(data) = tokio::fs::read(part).await else {
            return json(500, json!({"error": "internal-error"}));
        };
        if crate::util::hex(&Sha256::digest(&data)) != id {
            let _ = tokio::fs::remove_file(part).await;
            return json(400, json!({"error": "hash-mismatch"}));
        }
        if tokio::fs::rename(part, self.file_path(id)).await.is_err() {
            return json(500, json!({"error": "internal-error"}));
        }
        json(200, self.done_json(id, size))
    }

    async fn download(&self, id: &str) -> Response {
        let path = self.file_path(id);
        let Ok(file) = tokio::fs::File::open(&path).await else {
            return json(404, json!({"error": "not-found"}));
        };
        let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, len)
            .header(header::CACHE_CONTROL, "private, max-age=86400, immutable")
            .body(Body::from_stream(tokio_util::io::ReaderStream::new(file)))
            .unwrap()
    }

    /// Deletes expired files. -> how many
    pub fn sweep(&self) -> usize {
        let now = SystemTime::now();
        let mut removed = 0;
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return 0;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            let part = is_part(&name);
            if !is_sha(&name) && !part {
                continue;
            }
            let ttl = Duration::from_millis(if part { PART_TTL_MS } else { self.ttl_ms });
            let old = e.metadata().and_then(|m| m.modified()).ok().and_then(|m| now.duration_since(m).ok()).is_some_and(|age| age > ttl);
            if old && std::fs::remove_file(e.path()).is_ok() {
                removed += 1;
            }
        }
        if removed > 0 {
            log("media", format!("expired {removed} files"));
        }
        removed
    }
}

/// a=1&b=%2F -> {a: "1", b: "/"}
pub fn url_decode_pairs(q: &str) -> HashMap<String, String> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            let form = |x: &str| percent_decode(&x.replace('+', " "));
            (form(k), form(v))
        })
        .collect()
}

/// %2F -> "/" (decodeURIComponent; bad escapes are left as they are).
pub fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let hex = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
