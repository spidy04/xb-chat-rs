//! An encrypted link between two servers: a replica pair (core/src/replica.rs)
//! or a gateway and its core (core/src/relay.rs, xb-gateway).
//!
//! A shared secret. Each side sends 32 random bytes; each direction's
//! AES-256-GCM key is SHA-256 of the secret and both nonces, so a peer
//! without the secret can't read or forge anything. Then stanzas, in the
//! same frames as phones use (transport.rs).

use crate::codec::Node;
use crate::frame::FrameDecoder;
use crate::transport::Cipher;
use sha2::{Digest, Sha256};
use std::io;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

pub const MAX_NODE: usize = 32 * 1024 * 1024;
pub const PING: Duration = Duration::from_secs(2);
pub const SILENCE: Duration = Duration::from_secs(6);

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("link: {e}"))
}

/// An encrypted link between two servers (a replica pair; a gateway and its
/// core, relay.rs).
pub struct Link {
    pub r: OwnedReadHalf,
    pub w: OwnedWriteHalf,
    pub dec: FrameDecoder,
    pub rx: Box<Cipher>,
    pub tx: Box<Cipher>,
}

fn link_key(secret: &str, dir: &str, a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"xb-repl\0");
    h.update(dir.as_bytes());
    h.update([0]);
    h.update(secret.as_bytes());
    h.update(a);
    h.update(b);
    h.finalize().into()
}

impl Link {
    /// `dialer`: we connected (we send our nonce first).
    pub async fn open(sock: TcpStream, secret: &str, dialer: bool) -> io::Result<Link> {
        let _ = sock.set_nodelay(true);
        let (mut r, mut w) = sock.into_split();
        let mine: [u8; 32] = rand::random();
        let mut theirs = [0u8; 32];
        let hello = async {
            if dialer {
                w.write_all(&mine).await?;
                r.read_exact(&mut theirs).await?;
            } else {
                r.read_exact(&mut theirs).await?;
                w.write_all(&mine).await?;
            }
            io::Result::Ok(())
        };
        tokio::time::timeout(Duration::from_secs(5), hello).await.map_err(|_| err("link handshake timed out"))??;
        let (d, a) = if dialer { (mine, theirs) } else { (theirs, mine) };
        let d2a = link_key(secret, "d2a", &d, &a);
        let a2d = link_key(secret, "a2d", &d, &a);
        let (tx, rx) = if dialer { (d2a, a2d) } else { (a2d, d2a) };
        Ok(Link { r, w, dec: FrameDecoder::new(MAX_NODE + 64), rx: Box::new(Cipher::new(&rx)), tx: Box::new(Cipher::new(&tx)) })
    }

    pub async fn dial(addr: &str, secret: &str) -> io::Result<Link> {
        let sock = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr)).await.map_err(|_| err("connect timed out"))??;
        Link::open(sock, secret, true).await
    }

    pub async fn send(&mut self, node: &Node) -> io::Result<()> {
        self.w.write_all(&self.tx.seal(node)).await
    }

    /// The next stanza; an error if none arrives within `wait`, the link
    /// closed, or it doesn't decrypt (wrong secret).
    pub async fn recv(&mut self, wait: Duration) -> io::Result<Node> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(f) = self.dec.next_frame().map_err(|e| err(format!("{e:?}")))? {
                return self.rx.open(&f, MAX_NODE);
            }
            let mut buf = vec![0u8; 64 * 1024];
            let n = tokio::time::timeout_at(deadline.into(), self.r.read(&mut buf)).await.map_err(|_| err("peer silent"))??;
            if n == 0 {
                return Err(err("link closed"));
            }
            self.dec.push(&buf[..n]);
        }
    }
}

/// Stanzas for a link, many at once: everything waiting in `rx` (after
/// `first`), up to about 1 MB, as ONE encrypted frame ("b"), so a busy link
/// costs one encryption and one write per batch, not per stanza.
pub fn seal_batch(tx: &mut Cipher, first: Node, rx: &mut tokio::sync::mpsc::UnboundedReceiver<Node>) -> Vec<u8> {
    let mut nodes = vec![first];
    let mut bytes = 0usize;
    while bytes < 1024 * 1024 {
        match rx.try_recv() {
            Ok(n) => {
                bytes += n.content_bytes().map_or(64, |b| b.len() + 64);
                nodes.push(n);
            }
            Err(_) => break,
        }
    }
    let n = if nodes.len() == 1 { nodes.pop().unwrap() } else { Node::new("b").nodes(nodes) };
    tx.seal_raw(&crate::codec::encode(&n))
}

/// What arrived on a link: a batch ("b") or a single stanza.
pub fn unbatch(n: Node) -> Vec<Node> {
    if n.tag == "b" {
        match n.content {
            Some(crate::codec::Content::Nodes(v)) => v,
            _ => Vec::new(),
        }
    } else {
        vec![n]
    }
}
