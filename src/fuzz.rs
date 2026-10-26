//! Fuzzing: random and mutated input against everything that reads bytes
//! or stanzas from the internet. Each test must never panic, hang, or blow
//! up memory, whatever it's given.
//!
//! Pure Rust (no libFuzzer, which needs a C++ compiler): a seeded random
//! generator, mutations of valid inputs, and a stanza grammar. Quick by
//! default; for a long run:
//!
//!   XB_FUZZ_ITERS=1000000 cargo test --release fuzz_
//!
//! A failure prints its seed; XB_FUZZ_SEED=<seed> replays it.

use crate::codec::{self, Content, Node};
use crate::frame::{self, FrameDecoder, MAX_FRAME};
use crate::server::rig;
use crate::transport::{Cipher, FLAG_COMPRESSED};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn iters(default: usize) -> usize {
    std::env::var("XB_FUZZ_ITERS").ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn rng(name: &str) -> StdRng {
    let seed = std::env::var("XB_FUZZ_SEED").ok().and_then(|v| v.parse().ok()).unwrap_or_else(rand::random::<u64>);
    println!("{name}: XB_FUZZ_SEED={seed}");
    StdRng::seed_from_u64(seed)
}

/// Valid stanzas from the Node codec (tests/fixtures/codec.hex), to mutate.
fn seeds() -> Vec<Vec<u8>> {
    include_str!("../tests/fixtures/codec.hex").lines().filter(|l| !l.is_empty()).map(|l| (0..l.len()).step_by(2).map(|i| u8::from_str_radix(&l[i..i + 2], 16).unwrap()).collect()).collect()
}

/// A few random edits: flip, insert, delete, overwrite with an interesting
/// byte (a type marker, a length edge), truncate, splice.
fn mutate(r: &mut StdRng, mut b: Vec<u8>) -> Vec<u8> {
    const INTERESTING: [u8; 12] = [0, 1, 2, 0x7f, 0x80, 0xff, 236, 247, 248, 249, 252, 254];
    for _ in 0..r.gen_range(1..8) {
        let len = b.len();
        match r.gen_range(0..7) {
            0 if len > 0 => {
                let i = r.gen_range(0..len);
                b[i] ^= 1 << r.gen_range(0..8);
            }
            1 => {
                let i = r.gen_range(0..=len);
                b.insert(i, r.r#gen());
            }
            2 if len > 0 => {
                b.remove(r.gen_range(0..len));
            }
            3 if len > 0 => {
                let i = r.gen_range(0..len);
                b[i] = INTERESTING[r.gen_range(0..INTERESTING.len())];
            }
            4 if len > 0 => b.truncate(r.gen_range(0..len)),
            5 => {
                let extra: Vec<u8> = (0..r.gen_range(1..16)).map(|_| r.r#gen()).collect();
                let i = r.gen_range(0..=len);
                b.splice(i..i, extra);
            }
            _ => {}
        }
    }
    b
}

fn garbage(r: &mut StdRng, max: usize) -> Vec<u8> {
    (0..r.gen_range(0..max)).map(|_| r.r#gen()).collect()
}

#[test]
fn fuzz_decode_never_panics() {
    let mut r = rng("fuzz_decode");
    let seeds = seeds();
    for _ in 0..iters(50_000) {
        let input = if r.gen_bool(0.85) {
            let seed = seeds[r.gen_range(0..seeds.len())].clone();
            mutate(&mut r, seed)
        } else {
            garbage(&mut r, 300)
        };
        // Must return (Ok or Err), never panic. What it accepts must encode
        // and decode again, and be stable from then on. (Not identical the
        // first time: the reserved words xmlstreamstart/end decode as text
        // but are written as bytes, the same in the Node codec; text() reads
        // both the same.)
        if let Ok(node) = codec::decode(&input) {
            let once = codec::decode(&codec::encode(&node)).expect("re-encoding what we decoded");
            let twice = codec::decode(&codec::encode(&once)).expect("re-encoding again");
            assert_eq!(twice, once);
        }
    }
}

#[test]
fn fuzz_frames_never_panic() {
    let mut r = rng("fuzz_frames");
    for _ in 0..iters(20_000) {
        let mut d = FrameDecoder::new(r.gen_range(0..2 * MAX_FRAME));
        let mut stream = Vec::new();
        for _ in 0..r.gen_range(0..6) {
            if r.gen_bool(0.7) {
                stream.extend(frame::encode(&garbage(&mut r, 200)).unwrap());
            } else {
                stream.extend(garbage(&mut r, 8)); // a bad length prefix, say
            }
        }
        // Delivered in random pieces.
        let mut rest = &stream[..];
        while !rest.is_empty() {
            let n = r.gen_range(1..=rest.len());
            d.push(&rest[..n]);
            rest = &rest[n..];
            while let Ok(Some(_)) = d.next_frame() {}
        }
    }
}

#[test]
fn fuzz_cipher_never_panics() {
    let mut r = rng("fuzz_cipher");
    let key: [u8; 32] = r.r#gen();
    for _ in 0..iters(5_000) {
        let (mut tx, mut rx) = (Cipher::new(&key), Cipher::new(&key));
        let payload = match r.gen_range(0..4) {
            // Garbage: authentication fails.
            0 => garbage(&mut r, 200),
            // Correctly encrypted, but the inside is junk.
            1 => tx.encrypt(&garbage(&mut r, 200)),
            // Claims compression, holds junk.
            2 => {
                let mut p = vec![FLAG_COMPRESSED];
                p.extend(garbage(&mut r, 200));
                tx.encrypt(&p)
            }
            // A real zlib "bomb": tiny, inflates far past the limit.
            _ => {
                use std::io::Write;
                let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::best());
                z.write_all(&vec![0u8; 4 * 1024 * 1024]).unwrap();
                let mut p = vec![FLAG_COMPRESSED];
                p.extend(z.finish().unwrap());
                tx.encrypt(&p)
            }
        };
        let _ = rx.open(&payload, 1024 * 1024);
    }
}

// ---------------------------------------------------------------- stanzas

const TOP: [&str; 8] = ["message", "receipt", "ack", "iq", "presence", "chatstate", "auth", "junk"];
const CHILD: [&str; 40] = [
    "participants",
    "to",
    "enc",
    "keys",
    "identity",
    "signing",
    "skey",
    "value",
    "signature",
    "list",
    "key",
    "count",
    "devices",
    "user",
    "remove",
    "link",
    "picture",
    "image",
    "preview",
    "name",
    "about",
    "privacy",
    "contacts",
    "create",
    "add",
    "leave",
    "query",
    "participant",
    "token",
    "lastseen",
    "composing",
    "paused",
    "body",
    "media",
    "quote",
    "forwarded",
    "device",
    "group",
    "x",
    "ping",
];
const KEYS: [&str; 22] =
    ["id", "to", "from", "type", "class", "xmlns", "participant", "device", "mode", "user", "phone", "code", "jid", "ref", "key", "name", "subject", "platform", "last", "online", "receipts", "pass"];
const XMLNS: [&str; 11] = ["w:p", "status", "media", "g", "devices", "push", "privacy", "md", "encrypt", "profile", "nope"];

fn value(r: &mut StdRng, rig: &rig::Rig) -> String {
    let jid = |i: usize| format!("{}@s.xb", rig::USERS[i]);
    match r.gen_range(0..40) {
        0 => String::new(),
        1..=6 => jid(r.gen_range(0..3)),
        7 => format!("{}@g.xb", rig.group),
        8 => "nobody@s.xb".into(),
        9 => "x@g.xb".into(),
        10 => ["@", "a@", "@s.xb", "a@b@c"][r.gen_range(0..4)].into(),
        11..=13 => rig::E2E[r.gen_range(0..3)].into(),
        14 => ["0", "9999999999", "4294967296", "-1", "01"][r.gen_range(0..5)].into(),
        15..=18 => format!("{:X}", r.r#gen::<u64>()), // a message id
        19 => "x".repeat(r.gen_range(60..400)),
        20 => "ü😀\u{0}".into(),
        21 => XMLNS[r.gen_range(0..XMLNS.len())].into(),
        22 => ["get", "set", "result"][r.gen_range(0..3)].into(),
        23 => ["delivered", "read", "read-self", "receipt", "message"][r.gen_range(0..5)].into(),
        24 => ["all", "contacts", "none", "match", "true", "false"][r.gen_range(0..6)].into(),
        25 => ["device", "code", "verify", "link", "login", "register"][r.gen_range(0..6)].into(),
        26 => ["subscribe", "unsubscribe", "available", "unavailable"][r.gen_range(0..4)].into(),
        27 => ["image", "preview", "fcm", "composing", "paused"][r.gen_range(0..5)].into(),
        28 => rig::USERS[r.gen_range(0..3)].into(),
        29 => "919800000009".into(),
        _ => format!("{}", r.gen_range(0..100)),
    }
}

fn node(r: &mut StdRng, rig: &rig::Rig, depth: usize) -> Node {
    let tag = if depth == 0 { TOP[r.gen_range(0..TOP.len())] } else { CHILD[r.gen_range(0..CHILD.len())] };
    let mut n = Node::new(tag);
    for _ in 0..r.gen_range(0..6) {
        let k = KEYS[r.gen_range(0..KEYS.len())];
        let v = if k == "xmlns" && r.gen_bool(0.8) { XMLNS[r.gen_range(0..XMLNS.len())].to_string() } else { value(r, rig) };
        n.set(k, v);
    }
    n.content = match r.gen_range(0..10) {
        0..=2 => None,
        3 => {
            let len = [0, 5, 32, 64, 100][r.gen_range(0..5)];
            Some(Content::Bytes(vec![r.r#gen(); len]))
        }
        4 => Some(Content::Bytes(vec![0xff, 0xd8, 0xff, 0xe0, 1, 2, 3])), // looks like a JPEG
        5 => Some(Content::Text(value(r, rig))),
        _ if depth < 4 => Some(Content::Nodes((0..r.gen_range(0..4)).map(|_| node(r, rig, depth + 1)).collect())),
        _ => None,
    };
    n
}

/// A well-formed message (right devices), so the delivery paths run too.
fn valid_message(r: &mut StdRng, from: usize) -> Node {
    let to = r.gen_range(0..3);
    let enc = |d: &str| Node::new("enc").attr("device", d).bytes(vec![1, 2, 3]);
    let mut parts = vec![Node::new("to").attr("jid", format!("{}@s.xb", rig::USERS[to])).nodes(vec![enc(rig::E2E[to])])];
    if to != from && r.gen_bool(0.5) {
        parts.push(Node::new("to").attr("jid", format!("{}@s.xb", rig::USERS[from])).nodes(vec![]));
    }
    Node::new("message").attr("id", format!("{:X}", r.r#gen::<u32>())).attr("to", format!("{}@s.xb", rig::USERS[to])).nodes(vec![Node::new("participants").nodes(parts)])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_stanzas_never_panic_or_deadlock() {
    let mut r = rng("fuzz_stanzas");
    let mut rig = rig::new();
    let n = iters(20_000);
    // A deadlock between shards would hang here: fail instead.
    let run = async {
        for _ in 0..n {
            let who = r.gen_range(0..4);
            let st = if who < 3 && r.gen_bool(0.15) { valid_message(&mut r, who) } else { node(&mut r, &rig, 0) };
            rig.feed(who, st).await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(300), run).await.expect("stanza fuzzing hung: a deadlock?");
    let panics = rig.hub.stats.panics.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(panics, 0, "{panics} handler panics (see the [panic] lines above)");
}
