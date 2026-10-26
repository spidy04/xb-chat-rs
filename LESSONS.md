# Learning Rust from this server

The server is finished: see [README.md](README.md). This page is a reading
order for learning Rust from it. Learn the language first, then read
the real code, one file at a time, each with one set of Rust ideas.

## 1. Rust first

Read these before the code. They're free and official:

- [The Rust Book](https://doc.rust-lang.org/book/), chapters 1 to 10:
  ownership, borrowing, structs, enums and `match`, `Result` and `?`, and
  modules. That's most of what this server uses.
- Then chapter 16 (concurrency) and the
  [tokio tutorial](https://tokio.rs/tokio/tutorial) for `async`/`await`.
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/) when you
  want a small, runnable sample of one feature.

## 2. Then the code, in this order

| # | File | What it does | Rust ideas to look for |
|---|---|---|---|
| 1 | [src/frame.rs](src/frame.rs) | 3-byte length frames (the part you wrote, lessons 1 and 2) | structs, `impl`, `&mut self`, slices, `Result<Option<T>, E>`, unit tests |
| 2 | [src/tokens.rs](src/tokens.rs) | the word dictionary | `const`, `&'static str`, `OnceLock`, `HashMap` |
| 3 | [src/codec.rs](src/codec.rs) | binary stanzas | an `enum` with data (`Content`), `match`, builder methods that take `self`, iterators, tests against fixtures |
| 4 | [src/keys.rs](src/keys.rs) | the server key file (lesson 3) | `match` with guards, `std::fs`, error conversion |
| 5 | [src/transport.rs](src/transport.rs) | header, Noise handshake, encryption | `async fn`, an external crate (`snow`), enums over two socket types |
| 6 | [src/store.rs](src/store.rs), [src/db.rs](src/db.rs) | all the data in memory, saved to redb | `serde` derive, `Option`, `IndexMap`, `&mut` methods, `std::mem::take`, transactions |
| 7 | [src/server.rs](src/server.rs), from `serve()` | one connection's life, and the 64 shards | `tokio::spawn`, `select!`, channels, `Arc`, `Mutex`, `Deref`, lock ordering |
| 8 | [src/server.rs](src/server.rs), the handlers | login, messages, receipts, groups… | big `match` trees, closures, early returns |
| 9 | [src/media.rs](src/media.rs), [src/gateway.rs](src/gateway.rs) | HTTP with `axum` | extractors, streaming bodies, WebSocket upgrade |
| 10 | [src/push.rs](src/push.rs) | FCM | `reqwest`, JSON, signing a JWT |
| 11 | [src/main.rs](src/main.rs) | wiring it all together; the drain at the end (upgrades) | `#[tokio::main]`, environment variables, background tasks, `JoinHandle::abort`, `CancellationToken` |
| 11b | [src/replica.rs](src/replica.rs) | replica pairs: copying a live server, failover | a small state machine (`Role`), `Notify`, `OnceLock`, deriving keys with SHA-256, `select!` over a channel, a timer and a socket |
| 11c | [src/relay.rs](src/relay.rs), [src/bin/xb-gateway.rs](src/bin/xb-gateway.rs) | connection gateways | many connections over one link (a map of channels), batching with `try_recv`, a second binary using the library |
| 12 | [src/bin/loadgen.rs](src/bin/loadgen.rs) | the load test | a client in Rust, `JoinSet`, `Semaphore`, measuring latency |

For each file, open its Node original next to it (the table in
[README.md](README.md) maps them). You already know what the code does,
so you can watch *how Rust says it*.

## 3. Small exercises

Each one is a few lines, and `cargo test` plus the Node tests tell you if
it still works.

1. Add a test to `frame.rs`: three frames glued together come out one by one.
2. In `codec.rs`, write a `to_xml(&Node) -> String` like Node's `toXml`, for debug logs.
3. Log how long each `<iq>` takes in `on_iq` (`std::time::Instant`).
4. Add a `/health` route in `gateway.rs` that answers `{"ok":true,"online":N}`.
5. Add `xb-server stats`: open `xb.redb` read-only and print how many users, devices and queued stanzas it holds.
