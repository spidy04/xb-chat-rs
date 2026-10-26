//! Connection gateways, the core's side. A gateway (src/bin/xb-gateway.rs)
//! holds the phones' connections: the sockets, the Noise handshakes, the
//! encryption and compression, and each connection's send queue. It passes
//! the decrypted stanzas over one encrypted link per gateway to this
//! process, the core, which runs the same session as for its own sockets
//! (server::serve_relayed). The core keeps all the state and logic; the
//! connections, the part that runs out first at millions of phones (memory,
//! sockets, crypto), spread over as many gateways as needed.
//!
//!   phones ──► gateway 1 ─┐
//!   phones ──► gateway 2 ─┼──► core (+ its standby, replica.rs)
//!   phones ──► gateway 3 ─┘
//!
//! On the link (stanzas, encrypted with XB_GATEWAY_SECRET, replica.rs Link):
//!   core -> gateway   keys (the server's private key, so it is the same server)
//!                     d s=<conn> <stanza>     a stanza for that connection
//!                     x s=<conn> [<stream:error>]   close it
//!   gateway -> core   o s=<conn> k=<device key, base64>   a new connection
//!                     d s=<conn> <stanza>     a stanza from it
//!                     c s=<conn>              it's gone
//!   both              ping, every 2 s
//!
//! If the link breaks, the core ends those sessions (as if their sockets
//! closed) and the gateway tells its phones "restart, come back in N s", so
//! they reconnect spread out, to whichever core is serving then.

use crate::codec::Node;
use crate::server::{Hub, Relayed, serve_relayed};
use crate::util::log;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use xb_proto::link::{Link, MAX_NODE, PING, SILENCE, seal_batch, unbatch};

/// Listens for gateways. -> the address it listens on.
pub async fn listen(hub: Arc<Hub>, addr: &str, secret: String) -> io::Result<String> {
    let l = TcpListener::bind(addr).await?;
    let bound = l.local_addr()?.to_string();
    tokio::spawn(async move {
        loop {
            let Ok((sock, from)) = l.accept().await else {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            };
            let (hub, secret) = (hub.clone(), secret.clone());
            tokio::spawn(async move {
                match Link::open(sock, &secret, false).await {
                    Ok(link) => {
                        log("relay", format!("gateway {from} connected"));
                        let why = run(hub, link).await;
                        log("relay", format!("gateway {from} gone ({why})"));
                    }
                    Err(e) => log("relay", format!("gateway {from}: {e}")),
                }
            });
        }
    });
    Ok(bound)
}

/// Serves one gateway until its link ends. Returning drops `sessions`, so
/// every connection of that gateway ends (as if its socket closed).
async fn run(hub: Arc<Hub>, link: Link) -> String {
    let Link { mut r, mut w, mut dec, mut rx, mut tx } = link;
    if let Err(e) = w.write_all(&tx.seal(&Node::new("keys").bytes(hub.private_key.to_vec()))).await {
        return e.to_string();
    }
    let (out_tx, mut out_rx) = unbounded_channel::<Relayed>();
    // Relayed -> link stanzas, so the writer can send them in batches.
    let (node_tx, mut node_rx) = unbounded_channel::<Node>();
    // The gateway's connections: where their stanzas go.
    let mut sessions: HashMap<u64, UnboundedSender<Vec<u8>>> = HashMap::new();
    let mut ping = tokio::time::interval(PING);
    let mut last_heard = Instant::now();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        tokio::select! {
            m = out_rx.recv() => {
                let Some(m) = m else { return "closed".into() };
                let mut m = Some(m);
                while let Some(one) = m.take().or_else(|| out_rx.try_recv().ok()) {
                let n = match one {
                    Relayed::Stanza(s, bytes) => {
                        hub.bytes_out.fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        Node::new("d").attr("s", s).bytes(bytes)
                    }
                    Relayed::Close(s, err) => {
                        sessions.remove(&s);
                        let x = Node::new("x").attr("s", s);
                        match err { Some(e) => x.bytes(e), None => x }
                    }
                };
                let _ = node_tx.send(n);
                }
                let first = node_rx.try_recv().expect("at least one");
                let frame = seal_batch(&mut tx, first, &mut node_rx);
                if let Err(e) = w.write_all(&frame).await { return e.to_string() }
            }
            _ = ping.tick() => {
                if last_heard.elapsed() > SILENCE { return "silent".into() }
                if let Err(e) = w.write_all(&tx.seal(&Node::new("ping"))).await { return e.to_string() }
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
                    let batch = match rx.open(&f, MAX_NODE) {
                        Ok(n) => unbatch(n),
                        Err(e) => return e.to_string(), // wrong secret, or garbage
                    };
                    for node in batch {
                    let Some(s) = node.get("s").and_then(|s| s.parse::<u64>().ok()) else { continue };
                    match node.tag.as_str() {
                        "o" => {
                            let key = node.get("k").unwrap_or("").to_string();
                            let (in_tx, in_rx) = unbounded_channel();
                            sessions.insert(s, in_tx);
                            tokio::spawn(serve_relayed(hub.clone(), key, s, in_rx, out_tx.clone()));
                        }
                        "d" => {
                            if let (Some(tx), Some(b)) = (sessions.get(&s), node.content_bytes()) {
                                let _ = tx.send(b.to_vec());
                            }
                        }
                        "c" => {
                            sessions.remove(&s);
                        }
                        _ => {}
                    }
                    }
                }
            }
        }
    }
}
