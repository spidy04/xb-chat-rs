//! Media upload tokens: the one contract between the chat server and the
//! media service (xb-media). The server hands one out over the chat
//! connection (<media_conn auth_token>); the media service checks it with
//! the same secret. Neither needs the other's code.
//!
//!   token = <user>.<expires, ms>.<base64url HMAC-SHA256(secret, "<user>.<expires>")>

use crate::util::{now_ms, same};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub struct UploadTokens {
    secret: String,
    /// How long a token works.
    pub ttl_ms: u64,
}

impl UploadTokens {
    pub fn new(secret: String) -> UploadTokens {
        UploadTokens { secret, ttl_ms: 10 * 60 * 1000 }
    }

    fn mac(&self, body: &str) -> Vec<u8> {
        let mut m = <Hmac<Sha256> as Mac>::new_from_slice(self.secret.as_bytes()).expect("any key length");
        m.update(body.as_bytes());
        m.finalize().into_bytes().to_vec()
    }

    /// A short-lived token for `user`. -> (token, expires at ms)
    pub fn issue(&self, user: &str) -> (String, u64) {
        let exp = now_ms() + self.ttl_ms;
        let body = format!("{user}.{exp}");
        let sig = B64URL.encode(self.mac(&body));
        (format!("{body}.{sig}"), exp)
    }

    /// The user it was issued to, if it's genuine and not expired.
    pub fn check(&self, token: &str) -> Option<String> {
        let parts: Vec<&str> = token.split('.').collect();
        let [user, exp, sig] = parts[..] else {
            return None;
        };
        let want = self.mac(&format!("{user}.{exp}"));
        let got = B64URL.decode(sig.trim_end_matches('=')).ok()?;
        if !same(&got, &want) {
            return None;
        }
        if exp.parse::<f64>().ok()? < now_ms() as f64 {
            return None;
        }
        Some(user.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issued_tokens_check_and_forgeries_dont() {
        let t = UploadTokens::new("s".into());
        let (tok, _) = t.issue("alice");
        assert_eq!(t.check(&tok).as_deref(), Some("alice"));
        assert_eq!(UploadTokens::new("other".into()).check(&tok), None);
        assert_eq!(t.check(&tok.replace("alice", "bob")), None);
        let mut old = UploadTokens::new("s".into());
        old.ttl_ms = 0;
        let (expired, _) = old.issue("alice");
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(t.check(&expired), None);
    }
}
