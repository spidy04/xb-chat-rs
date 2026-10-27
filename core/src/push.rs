//! Push wake-ups: when a message waits for a phone that is
//! not connected, ask Firebase Cloud Messaging to wake it. The push carries
//! no content, only "you have messages"; the phone connects and takes its queue.
//!
//! FCM HTTP v1: sign a JWT with the service-account key (RS256), trade it for
//! an OAuth token (cached for its hour), POST messages:send.

use crate::util::{log, now_ms};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use serde_json::{Value, json};
use std::sync::Mutex;

const SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";

pub enum PushResult {
    Ok,
    /// The app was uninstalled or the token rotated: forget the token.
    Invalid,
    Error(String),
}

pub enum Pusher {
    Fcm(FcmPusher),
    /// Tests (XB_FAKE_PUSH=1): records pushes; tokens starting with "dead" are invalid.
    Fake(Mutex<Vec<(String, String)>>),
}

impl Pusher {
    pub async fn send(&self, token: &str, kind: &str) -> PushResult {
        match self {
            Pusher::Fcm(f) => f.send(token, kind).await,
            Pusher::Fake(list) => {
                list.lock().unwrap().push((token.to_string(), kind.to_string()));
                if token.starts_with("dead") { PushResult::Invalid } else { PushResult::Ok }
            }
        }
    }
}

pub struct FcmPusher {
    pub project_id: String,
    client_email: String,
    key: SigningKey<Sha256>,
    token_uri: String,
    http: reqwest::Client,
    token: tokio::sync::Mutex<Option<(String, u64)>>, // (access token, expires ms)
}

impl FcmPusher {
    pub fn from_file(path: &str) -> Result<FcmPusher, String> {
        let raw = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        let cred: Value = serde_json::from_str(&raw).map_err(|e| format!("{path}: {e}"))?;
        let field = |k: &str| cred.get(k).and_then(Value::as_str).map(str::to_string).ok_or(format!("FCM credentials: missing {k}"));
        let pem = field("private_key")?;
        let key = rsa::RsaPrivateKey::from_pkcs8_pem(&pem).map_err(|e| format!("FCM private key: {e}"))?;
        Ok(FcmPusher {
            project_id: field("project_id")?,
            client_email: field("client_email")?,
            key: SigningKey::<Sha256>::new(key),
            token_uri: field("token_uri").unwrap_or_else(|_| "https://oauth2.googleapis.com/token".into()),
            http: reqwest::Client::new(),
            token: tokio::sync::Mutex::new(None),
        })
    }

    async fn access_token(&self) -> Result<String, String> {
        let mut cached = self.token.lock().await;
        let now = now_ms();
        if let Some((value, exp)) = cached.as_ref() {
            if now + 60_000 < *exp {
                return Ok(value.clone());
            }
        }
        let iat = now / 1000;
        let header = B64URL.encode(json!({"alg": "RS256", "typ": "JWT"}).to_string());
        let claims = B64URL.encode(json!({"iss": self.client_email, "scope": SCOPE, "aud": self.token_uri, "iat": iat, "exp": iat + 3600}).to_string());
        let input = format!("{header}.{claims}");
        let sig = self.key.sign_with_rng(&mut rand::rngs::OsRng, input.as_bytes()).to_bytes();
        let jwt = format!("{input}.{}", B64URL.encode(sig));
        let res = self.http.post(&self.token_uri).form(&[("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"), ("assertion", jwt.as_str())]).send().await.map_err(|e| e.to_string())?;
        let status = res.status();
        let body: Value = res.json().await.unwrap_or(Value::Null);
        let Some(token) = body.get("access_token").and_then(Value::as_str) else {
            return Err(format!("FCM auth failed: {} {}", status.as_u16(), body.get("error").and_then(Value::as_str).unwrap_or("")));
        };
        let expires = now + body.get("expires_in").and_then(Value::as_u64).unwrap_or(3600) * 1000;
        *cached = Some((token.to_string(), expires));
        Ok(token.to_string())
    }

    pub async fn send(&self, token: &str, kind: &str) -> PushResult {
        let access = match self.access_token().await {
            Ok(t) => t,
            Err(e) => return PushResult::Error(e),
        };
        let url = format!("https://fcm.googleapis.com/v1/projects/{}/messages:send", self.project_id);
        let body = json!({
            "message": {
                "token": token,
                "data": { "xb": kind }, // never any message content
                // High priority: delivered at once, even in Doze. Pushes waiting
                // while the phone is offline collapse into one.
                "android": { "priority": "HIGH", "ttl": "86400s", "collapse_key": "xb-wake" },
            }
        });
        let res = match self.http.post(url).bearer_auth(access).json(&body).send().await {
            Ok(r) => r,
            Err(e) => return PushResult::Error(e.to_string()),
        };
        let status = res.status();
        if status.is_success() {
            return PushResult::Ok;
        }
        let body: Value = res.json().await.unwrap_or(Value::Null);
        let err_status = body.pointer("/error/status").and_then(Value::as_str).unwrap_or("");
        let code = body.pointer("/error/details").and_then(Value::as_array).and_then(|d| d.iter().find_map(|x| x.get("errorCode").and_then(Value::as_str))).unwrap_or("");
        if status.as_u16() == 404 || err_status == "NOT_FOUND" || code == "UNREGISTERED" || code == "INVALID_ARGUMENT" {
            return PushResult::Invalid;
        }
        PushResult::Error(format!("{} {err_status}", status.as_u16()))
    }
}

pub fn describe(p: &Pusher) {
    if let Pusher::Fcm(f) = p {
        log("start", format!("- push wake-ups via FCM (project {})", f.project_id));
    } else {
        log("start", "- push wake-ups: fake (XB_FAKE_PUSH, tests only)");
    }
}
