//! The server's identity: a long-term X25519 key pair.
//!
//! Clients pin the server's public key, so it must survive restarts. The
//! file holds the private key in base64, the same format as the Node server
//! (<data>/server_key), so both servers can run on the same data folder and
//! phones keep trusting it. The public key is computed from the private one.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use std::io;

pub fn public_of(private: &[u8; 32]) -> [u8; 32] {
    let secret = x25519_dalek::StaticSecret::from(*private);
    x25519_dalek::PublicKey::from(&secret).to_bytes()
}

/// Loads the key pair from `path`, or creates it the first time. -> (private, public)
pub fn load_or_create(path: &str) -> io::Result<([u8; 32], [u8; 32])> {
    match std::fs::read(path) {
        Ok(raw) => {
            let decoded = std::str::from_utf8(&raw).ok().and_then(|t| B64.decode(t.trim()).ok());
            let bytes = match decoded {
                Some(b) => b,
                // An older format: 64 raw bytes, private then public.
                None if raw.len() == 64 => raw[..32].to_vec(),
                None => return Err(bad(format!("{path}: not a base64 key"))),
            };
            let private: [u8; 32] = bytes.try_into().map_err(|_| bad(format!("{path}: not a 32-byte key")))?;
            Ok((private, public_of(&private)))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let private: [u8; 32] = rand::random();
            if let Some(dir) = std::path::Path::new(path).parent() {
                std::fs::create_dir_all(dir)?;
            }
            crate::util::write_private(path, B64.encode(private).as_bytes())?;
            Ok((private, public_of(&private)))
        }
        Err(e) => Err(e),
    }
}

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}
