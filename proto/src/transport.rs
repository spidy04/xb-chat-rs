//! The connection layer (PROTOCOL.md §2):
//!
//!   client -> HEADER ("XB" 2 1), then Noise XX handshake frames
//!   both   -> encrypted frames: [3-byte length][AES-GCM( flags | stanza )]
//!
//! flags 0x02 = the stanza is zlib-compressed. The same bytes travel over TCP
//! or inside WebSocket messages (the web client), so a connection is either.

use crate::codec::{self, Node};
use crate::frame::{self, FrameDecoder};
use crate::tokens::HEADER;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::io::{self, Read, Write};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub const NOISE_PARAMS: &str = "Noise_XX_25519_AESGCM_SHA256";
pub const FLAG_COMPRESSED: u8 = 0x02;
const HANDSHAKE_MAX: usize = 1024;
const COMPRESS_THRESHOLD: usize = 512;

fn err(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

// ---------------------------------------------------------------- the two kinds of socket

/// The receiving half of a connection.
pub enum ConnRead {
    Tcp(OwnedReadHalf),
    Ws(SplitStream<WebSocket>),
}

/// The sending half.
pub enum ConnWrite {
    Tcp(OwnedWriteHalf),
    Ws(SplitSink<WebSocket, Message>),
}

impl ConnRead {
    /// The next bytes that arrived; None when the other side closed.
    pub async fn chunk(&mut self) -> io::Result<Option<Vec<u8>>> {
        match self {
            // Wait until bytes are there, THEN read them into a buffer that
            // only exists for this moment. An idle connection (most of them)
            // then holds no read buffer at all, instead of 16 KB each.
            ConnRead::Tcp(r) => loop {
                r.readable().await?;
                let mut buf = [0u8; 16 * 1024];
                match r.try_read(&mut buf) {
                    Ok(0) => return Ok(None),
                    Ok(n) => return Ok(Some(buf[..n].to_vec())),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e),
                }
            },
            ConnRead::Ws(r) => loop {
                match r.next().await {
                    None => return Ok(None),
                    Some(Err(e)) => return Err(io::Error::other(e)),
                    Some(Ok(Message::Binary(b))) => return Ok(Some(b.to_vec())),
                    Some(Ok(Message::Close(_))) => return Ok(None),
                    Some(Ok(Message::Text(_))) => {
                        return Err(err("text WebSocket message (binary only)"));
                    }
                    Some(Ok(_)) => continue, // ping / pong: handled by the library
                }
            },
        }
    }
}

impl ConnWrite {
    pub async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        match self {
            ConnWrite::Tcp(w) => w.write_all(data).await,
            ConnWrite::Ws(w) => w.send(Message::Binary(data.to_vec().into())).await.map_err(io::Error::other),
        }
    }

    /// Ends our side (TCP FIN / WebSocket close frame).
    pub async fn shutdown(&mut self) {
        match self {
            ConnWrite::Tcp(w) => {
                let _ = w.shutdown().await;
            }
            ConnWrite::Ws(w) => {
                let _ = w.close().await;
            }
        }
    }
}

// ---------------------------------------------------------------- cipher

/// One direction of the encrypted channel: AES-256-GCM with a counter nonce
/// (4 zero bytes + 64-bit big-endian counter), as the Noise spec says.
pub struct Cipher {
    aes: Aes256Gcm,
    n: u64,
}

impl Cipher {
    pub fn new(key: &[u8; 32]) -> Cipher {
        Cipher { aes: Aes256Gcm::new_from_slice(key).expect("32-byte key"), n: 0 }
    }

    fn nonce(&mut self) -> [u8; 12] {
        let mut iv = [0u8; 12];
        iv[4..].copy_from_slice(&self.n.to_be_bytes());
        self.n += 1;
        iv
    }

    pub fn encrypt(&mut self, plain: &[u8]) -> Vec<u8> {
        let iv = self.nonce();
        self.aes.encrypt(Nonce::from_slice(&iv), plain).expect("AES-GCM encrypt")
    }

    pub fn decrypt(&mut self, cipher: &[u8]) -> io::Result<Vec<u8>> {
        let iv = self.nonce();
        self.aes.decrypt(Nonce::from_slice(&iv), cipher).map_err(|_| err("decrypt failed"))
    }

    /// node -> a frame ready to send (compressed when that makes it smaller).
    pub fn seal(&mut self, node: &Node) -> Vec<u8> {
        self.seal_encoded(codec::encode(node))
    }

    /// An already encoded stanza -> a frame ready to send.
    pub fn seal_encoded(&mut self, mut body: Vec<u8>) -> Vec<u8> {
        let mut flags = 0u8;
        if body.len() >= COMPRESS_THRESHOLD {
            let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            z.write_all(&body).expect("in-memory write");
            let z = z.finish().expect("in-memory write");
            if z.len() < body.len() {
                body = z;
                flags |= FLAG_COMPRESSED;
            }
        }
        let mut plain = Vec::with_capacity(body.len() + 1);
        plain.push(flags);
        plain.extend_from_slice(&body);
        frame::encode(&self.encrypt(&plain)).expect("stanza within the frame limit")
    }

    /// An encoded stanza -> a frame, never compressed (a link between
    /// servers: what it carries is mostly ciphertext, which doesn't shrink).
    pub fn seal_raw(&mut self, body: &[u8]) -> Vec<u8> {
        let mut plain = Vec::with_capacity(body.len() + 1);
        plain.push(0);
        plain.extend_from_slice(body);
        frame::encode(&self.encrypt(&plain)).expect("stanza within the frame limit")
    }

    /// An encrypted frame -> the stanza in it. `max`: biggest stanza accepted.
    pub fn open(&mut self, payload: &[u8], max: usize) -> io::Result<Node> {
        let body = self.open_bytes(payload, max)?;
        codec::decode(&body).map_err(err)
    }

    /// An encrypted frame -> the encoded stanza in it, not decoded (a
    /// gateway passes it on as it is; the core decodes it).
    pub fn open_bytes(&mut self, payload: &[u8], max: usize) -> io::Result<Vec<u8>> {
        let plain = self.decrypt(payload)?;
        let (&flags, body) = plain.split_first().ok_or_else(|| err("empty frame"))?;
        let body = if flags & FLAG_COMPRESSED != 0 {
            // Cap the output so a small "zip bomb" can't exhaust memory.
            let mut out = Vec::new();
            flate2::read::ZlibDecoder::new(body).take(max as u64 + 1).read_to_end(&mut out)?;
            if out.len() > max {
                return Err(err("inflated stanza too large"));
            }
            out
        } else {
            body.to_vec()
        };
        Ok(body)
    }
}

// ---------------------------------------------------------------- handshake

/// Reads until the decoder has a whole frame.
pub async fn read_frame(conn: &mut ConnRead, decoder: &mut FrameDecoder) -> io::Result<Vec<u8>> {
    loop {
        match decoder.next_frame() {
            Ok(Some(f)) => return Ok(f),
            Ok(None) => {}
            Err(e) => return Err(err(format!("{e:?}"))),
        }
        match conn.chunk().await? {
            Some(c) => decoder.push(&c),
            None => {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed during the handshake"));
            }
        }
    }
}

pub struct Secured {
    /// The client's Noise static key: which device this is. (The ciphers are
    /// boxed: 1 KB each, and a connection's tasks would otherwise carry them twice.)
    pub remote_static: Vec<u8>,
    pub send: Box<Cipher>,
    pub recv: Box<Cipher>,
}

/// The header check and the server side of Noise XX.
pub async fn accept(r: &mut ConnRead, w: &mut ConnWrite, decoder: &mut FrameDecoder, private_key: &[u8; 32]) -> io::Result<Secured> {
    let noise = |e: snow::Error| err(format!("noise: {e}"));

    // The 4-byte header comes first, glued to the first frame. Feed the
    // decoder everything after it.
    let mut head = Vec::new();
    while head.len() < HEADER.len() {
        match r.chunk().await? {
            Some(c) => head.extend_from_slice(&c),
            None => {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed before the header"));
            }
        }
    }
    if head[..4] != HEADER {
        return Err(err("bad header (wrong protocol or version)"));
    }
    decoder.push(&head[4..]);

    let params: snow::params::NoiseParams = NOISE_PARAMS.parse().map_err(noise)?;
    let mut hs = snow::Builder::new(params).local_private_key(private_key).map_err(noise)?.prologue(&HEADER).map_err(noise)?.build_responder().map_err(noise)?;
    let mut buf = vec![0u8; HANDSHAKE_MAX];

    // -> e
    let m1 = read_frame(r, decoder).await?;
    if m1.len() > HANDSHAKE_MAX {
        return Err(err("handshake message too large"));
    }
    hs.read_message(&m1, &mut buf).map_err(noise)?;
    // <- e, ee, s, es
    let n = hs.write_message(&[], &mut buf).map_err(noise)?;
    w.write(&frame::encode(&buf[..n]).expect("small")).await?;
    // -> s, se
    let m3 = read_frame(r, decoder).await?;
    if m3.len() > HANDSHAKE_MAX {
        return Err(err("handshake message too large"));
    }
    hs.read_message(&m3, &mut buf).map_err(noise)?;

    let remote_static = hs.get_remote_static().ok_or_else(|| err("no client key"))?.to_vec();
    // Noise's Split(): the first key is initiator -> responder, the second
    // responder -> initiator. We do the transport ciphers ourselves because
    // snow's transport mode caps messages at 64 KB and ours go up to 1 MB.
    let (c1, c2) = hs.dangerously_get_raw_split();
    Ok(Secured { remote_static, send: Box::new(Cipher::new(&c2)), recv: Box::new(Cipher::new(&c1)) })
}
