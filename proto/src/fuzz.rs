//! Fuzzing: random and mutated input against everything that reads bytes
//! from the internet: the codec, framing, decryption. (The stanza handlers
//! are fuzzed in core/src/fuzz.rs.) Each test must never panic, hang, or blow
//! up memory, whatever it's given.
//!
//! Pure Rust (no libFuzzer, which needs a C++ compiler): a seeded random
//! generator, mutations of valid inputs, and a stanza grammar. Quick by
//! default; for a long run:
//!
//!   XB_FUZZ_ITERS=1000000 cargo test --release -p xb-proto fuzz_
//!
//! A failure prints its seed; XB_FUZZ_SEED=<seed> replays it.

use crate::codec::{self, Content, Node};
use crate::frame::{self, FrameDecoder, MAX_FRAME};
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
