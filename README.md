# xb-chat server (Rust)

A WhatsApp-style messaging server written from scratch in Rust: binary
XMPP (FunXMPP-style) over Noise, end-to-end encryption with the Signal
protocol (Sender Keys for groups), redb and fjall storage with a write-back
queue, replica pairs, connection gateways, and a separate media service.
Built to understand how WhatsApp-class systems work by building one,
following WhatsApp's published design wherever it exists.

![System design: the Android app and the web client connect over Noise-encrypted binary XMPP to a load balancer, which sends them to the core directly or through optional gateways. The core is a primary and a standby kept in sync, each with redb and fjall. Files go to a separate media service; offline phones get a content-free push.](docs/system-design.png)

Measured on one laptop (Ryzen 9 5900HX, load tool on the same machine):

| | |
|---|---|
| messages through the full send, deliver, receipt cycle | **16,000/s**, none lost |
| server memory per idle connected phone | **~8 KB** |
| queued stanzas written to disk at 5,000 msg/s (write-back) | **0** of 151,090 |
| failover to the standby, with every acknowledged message | **1.6 s** |

The clients (Android in Kotlin, a web client) and the protocol
specification (`PROTOCOL.md`) are in a separate repository, to be
published later.

## Contents

- [Build and run](#build-and-run)
- [What the server does](#what-the-server-does)
- [The code](#the-code)
- [The protocol](#the-protocol)
- [Storage](#storage)
- [Media](#media)
- [Performance](#performance)
- [Resilience](#resilience): overload, upgrades, replica pairs, gateways
- [Monitoring](#monitoring)
- [Settings](#settings)
- [Testing](#testing)
- [What comes from WhatsApp, and what is ours](#what-comes-from-whatsapp-and-what-is-ours)

## Build and run

```bash
cargo build --release
```

Run the media service and the chat server side by side. Both read the
same data folder, which holds the upload-token secret:

```bash
XB_DATA=./data ./target/release/xb-media
```

```bash
XB_DATA=./data ./target/release/xb-server
```

Phones connect on port 5222 (TCP), browsers on 8080 (WebSocket at `/ws`).
For phones on the same network, add `XB_HOST=0.0.0.0`. During development,
`XB_OTP_ECHO=1` returns the SMS login code to the client instead of
sending one; never use it where strangers can reach the server.

## What the server does

- **Connections:** the client header, the Noise XX handshake, and AES-GCM
  frames with zlib, over TCP and WebSocket.
- **Login:** phone number and SMS code; after that, the device's own Noise
  key; QR linking of companion devices (web), with rotating codes.
- **Messages:** one queue per device, at-least-once delivery; acks,
  delivered and read receipts, read-self on your other devices.
- **End-to-end encryption support:** pre-keys and bundles; `stale-devices`
  checks so senders always encrypt for exactly the right devices; group
  messages with Sender Keys, checked against the device list by `phash`.
- **The rest:** groups, presence, last seen and typing; profile name, About
  and photo; privacy settings including "My contacts"; contact discovery.
- **Push:** content-free FCM wake-ups for offline phones (signs the OAuth
  JWT with the service-account key, calls FCM HTTP v1).
- **Web port:** WebSocket for browsers, the web app's files with strict
  security headers, an origin check, and `/upload` and `/m/` passed through
  to the media service.

## The code

A workspace of crates, one per part, so the boundaries are the compiler's:
a crate sees only what it declares.

| Crate | What | Depends on |
|---|---|---|
| [proto](proto/src/lib.rs) | the wire: codec, framing, Noise transport, the server-to-server link, upload tokens, Prometheus format | - |
| [core](core/src/lib.rs) | chat logic and data | proto |
| [server](server/src/main.rs) | the `xb-server` program: core plus its web port | core, proto |
| [gateway](gateway/src/main.rs) | the `xb-gateway` program: holds phones' connections for a core | proto |
| [media](media/src/lib.rs) | the `xb-media` program: encrypted files only | proto |
| [loadgen](loadgen/src/main.rs) | the load test tool: many simulated phones | proto |

| File | What |
|---|---|
| [proto/src/codec.rs](proto/src/codec.rs), [tokens.rs](proto/src/tokens.rs) | binary stanzas |
| [proto/src/frame.rs](proto/src/frame.rs) | 3-byte length framing |
| [proto/src/transport.rs](proto/src/transport.rs) | header, Noise (snow), AES-GCM frames, zlib; TCP or WebSocket |
| [proto/src/link.rs](proto/src/link.rs) | the encrypted link between servers |
| [proto/src/upload_token.rs](proto/src/upload_token.rs) | media upload tokens: the core signs, xb-media checks |
| [core/src/server.rs](core/src/server.rs) | sessions, login, routing, receipts, iq handlers |
| [core/src/store.rs](core/src/store.rs) | accounts, devices, keys, groups, queues, in memory; marks what changed |
| [core/src/db.rs](core/src/db.rs) | redb tables, the writer task, group commit |
| [core/src/queue_db.rs](core/src/queue_db.rs) | the offline queue on fjall |
| [core/src/replica.rs](core/src/replica.rs) | replica pairs |
| [core/src/relay.rs](core/src/relay.rs) | the core's side of connection gateways |
| [core/src/push.rs](core/src/push.rs) | FCM wake-ups |
| [server/src/web.rs](server/src/web.rs) | the web port |

**How it's built:**
- Each connection is a tokio task, with a second task writing its
  encrypted frames, fed by a channel.
- The shared state is split into **64 shards by `hash(user)`**, each with
  its own lock, holding those users' accounts, devices, keys, queues,
  connections and presence watchers. A group lives in the shard of its id.
- **No deadlocks, by one rule:** a handler holds one shard, or several
  taken together in ascending order, and never takes a lock while holding
  one. Locks are never held across an `.await`, so handlers are plain
  synchronous code.
- The slow work runs outside the locks: password hashing, FCM calls and
  database commits.
- A panicking handler is caught: it's logged and counted, only that
  connection closes, and the rest of the server carries on.
- No C compiler needed: the crypto is pure Rust.

## The protocol

XMPP describes chat as small stanzas (a message, a receipt, a presence
change). Like WhatsApp's FunXMPP, xb-chat keeps that model and encodes it
in binary: common words are one-byte tokens from a shared dictionary,
phone numbers pack two digits per byte, message ids pack as hex.

```
<receipt id="3EB0C4A1F27B9D5E" to="919812345678@s.xb" type="read"/>   67 bytes as XML

f8 07  06       22  fb 08 3e b0 c4 a1 f2 7b 9d 5e  23  fa ff 06 91 98 12 34 56 78 03  25 41   27 bytes
list   receipt  id  id, hex-packed                 to  number packed, then s.xb       type read
```

An ack goes from 59 bytes to 24, a presence from 53 to 16.

- Every connection opens with a **Noise XX** handshake (Curve25519,
  AES-GCM, SHA-256). Each stanza then travels in a 3-byte-length frame,
  encrypted with AES-256-GCM, zlib-compressed when that helps.
- Browsers carry the identical bytes inside WebSocket messages, so one
  session type serves both.
- The codec is tested byte for byte against stanzas recorded from a
  reference implementation, and fuzzed (see [Testing](#testing)).

## Storage

Two embedded stores, both pure Rust, each suited to its kind of data:

| Store | Holds | Why this one |
|---|---|---|
| **`<data>/xb.redb`**, [redb](https://www.redb.org) (a B-tree with ACID transactions) | accounts, devices, keys, groups, last seen | read-mostly, must never be lost |
| **`<data>/queue/`**, [fjall](https://github.com/fjall-rs/fjall) (an LSM tree) | the offline queue | almost all "append, then delete soon", which an LSM writes sequentially |

**Memory is the fast path** (as WhatsApp used Mnesia): everything is read
from memory, and the stores only make it durable.

**Records (redb):** every change marks the record it touched; a writer
task saves all records marked since its last save in **one transaction**,
so under load many changes share one disk sync (group commit). Each record
is a small JSON value, easy to inspect and migrate.

**The offline queue (fjall), write-back:**
- a queued stanza stays only in memory for its first
  `XB_QUEUE_WRITE_DELAY_MS` (5 s by default);
- it's written only if still undelivered after that, and deleted again
  once delivered;
- nearly every message and receipt is delivered sooner, so it **never
  touches the disk**.

Measured at 5,000 msg/s for 15 s (2,000 phones, about 75,600 messages):

| | write every stanza (delay 0) | write-back (5 s) |
|---|---|---|
| queued stanzas written to disk, then deleted again | 143,851 + 143,851 | **0 + 0** |
| delivered before being written | 7,419 | **151,090** (all) |
| time committing to disk | 7.5 s | **0.68 s** (only account records) |

**Cold queues leave memory:** a device that stays offline keeps its
stanzas on disk only (after `XB_QUEUE_COLD_MS`, 5 min). After a restart the
queue loads as an index, and stanzas are read back from disk when their
device logs in.

**A message's life on the server:**
1. It arrives already end-to-end encrypted: one ciphertext per receiving
   device, or one for a whole group (Sender Keys).
2. A copy is queued for each receiving device under a dedup key, so a
   client's retry can never create a duplicate. The sender gets its ack.
3. A connected device gets it at once; an offline one gets a content-free
   push, connects and takes its queue in order.
4. The device's "delivered" receipt removes it from the queue, usually
   before it ever touched the disk.
5. The server keeps no history. An uncollected stanza is dropped after 30
   days.

**Durability:** the sender's ack goes out before anything is written, as
WhatsApp does. A single server that crashes inside the write delay loses those
stanzas; a [replica pair](#replica-pairs-a-machine-can-die) covers that
window, since the standby holds every stanza within a fraction of a
millisecond. A server running alone can set the delay to 0 instead.

**Snapshots:** `xb-server export-json` writes the whole state as one JSON
file (`state.json`), and `xb-server import-json` reads it back. A data
folder with a `state.json` and no `xb.redb` is imported on first start.

## Media

Files are handled by a separate process, `xb-media`, as WhatsApp uses
separate media servers.
It has no chat state, so it can run on other machines, scale on its own,
and fail without taking chat down. The chat server contains no media
code: the two share only the upload-token format.

1. The phone encrypts the file with a fresh random key (AES-256-GCM).
2. It asks the chat server for an upload token, valid for 10 minutes and
   signed with a secret shared with xb-media.
3. It uploads the ciphertext. The file is named by its SHA-256, so a
   forwarded file is stored once and never uploaded again. Big files
   resume where a dropped connection stopped.
4. The link, the key and the hash travel to the recipient inside the
   end-to-end encrypted message. The server only ever holds ciphertext.
5. Files expire after 30 days; partial uploads after a day.

By default links point at the chat server's web port, which streams
`/upload` and `/m/` through to xb-media with no buffering, so phones need
one address. Set `XB_MEDIA_URL` to send clients to the media service
directly. When xb-media was killed in the middle of a conversation,
uploads failed until it came back and chat carried on unaffected.

| `xb-media` setting | Default | |
|---|---|---|
| `XB_MEDIA_HOST` / `XB_MEDIA_PORT` | 127.0.0.1 / 8081 | where it listens |
| `XB_DATA` / `XB_MEDIA_DIR` | `./data` / `<data>/media` | where the secret and the files are |
| `XB_MEDIA_PUBLIC_URL` | `http://<host>:<port>` | base of the file links |
| `XB_MEDIA_MAX_MB` | 64 | largest file |
| `XB_METRICS_ADDR` | 127.0.0.1:9094 | `/metrics`, `/health`; `none` = off |

## Performance

[loadgen](loadgen/src/main.rs) runs simulated phones: each does the Noise
handshake, logs in, publishes keys, sends messages to random other phones
at a set total rate, and returns delivered receipts. That's the full cycle
real phones cause.

```bash
cargo run --release --bin loadgen -- --port 5222 --users 2000 --rate 5000 --secs 15
```

Measured on a Ryzen 9 5900HX laptop (16 threads), server and load tool on
the same machine, so these are lower bounds:

| 2,000 phones | one global lock | 64 shards |
|---|---|---|
| 5,000 msg/s | ack p50 9.4 ms, p99 50 ms | ack p50 **3.9 ms**, p99 **18 ms** |
| 20,000 msg/s | collapses: 8,600 msg/s, acks take seconds | **16,000 msg/s**, nothing lost |
| 40,000 msg/s | - | about 26,000 msg/s average, 51,000 at peak; CPU saturated |

**Memory per connection:** an idle connected phone costs about **8 KB**
(16,000 idle connections measured). Getting there: no read buffer held
while a socket is idle, handshake state boxed so each connection's task
stays small, and boxed messages in the send queue. A million connected
phones come to about 8 GB. Testing that many needs a Linux server and
several load machines (Windows limits one load tool to about 16,000
outgoing ports).

## Resilience

### Overload: slow down, don't fall over

- **Too many sockets:** above `XB_MAX_CONNECTIONS`, a new socket is closed
  before any work is spent on it.
- **A reconnect storm:** at most `XB_MAX_HANDSHAKES` handshakes at once,
  logins limited to `XB_LOGIN_RATE` a second, the rest told
  `<failure reason="busy" retry="1-5"/>` so they return spread out.
  Measured: 8,000 phones logging in at once with a 1,000/s limit were all
  back in 9.0 s, and the latency afterwards was unchanged.
- **A client that stops reading:** its send queue is capped
  (`XB_SEND_QUEUE_MAX`) and a single write times out
  (`XB_WRITE_TIMEOUT_MS`); past either it's closed, and nothing is lost:
  what it hadn't acknowledged waits in its queue.
- **A long backlog:** a phone back after a long time offline gets its
  queue in batches as fast as it reads them; new messages wait at the end,
  so the order holds.

### Upgrades: drain, then take over

Ctrl+C or `POST /drain` on the metrics port drains the server:
1. it closes its ports at once;
2. it tells every client `<stream:error code="restart" retry="N"/>`, with N
   spread over `XB_DRAIN_SPREAD_S` (10 s), so reconnects arrive evenly;
3. it saves everything, then exits.

A new version started with `XB_TAKEOVER=1` beforehand takes the ports the
moment they're released and the data files the moment the old process
exits. Measured: the new process was serving **83 ms** after the drain
began, with every queued stanza.

### Replica pairs: a machine can die

Two servers, each with its own data folder, started the same way, each
naming the other ([core/src/replica.rs](core/src/replica.rs)). One serves;
the standby keeps a live copy on its own disk.

```bash
# machine 1                                  # machine 2
XB_REPL_ADDR=0.0.0.0:5300                    XB_REPL_ADDR=0.0.0.0:5300
XB_PEER=machine2:5300                        XB_PEER=machine1:5300
XB_REPL_SECRET=<the same long secret>        XB_REPL_SECRET=<the same>
XB_NODE_ID=1  ./xb-server                    XB_NODE_ID=2  ./xb-server
```

- **What the standby gets**, over an encrypted link: the server's key (so
  it is the same server to the phones), a full copy, then every change as
  it happens: each record as it's saved, each stanza the moment it's
  queued or delivered.
- **Who serves** is decided at startup by asking the peer. A node that
  comes back after a crash finds the other serving and becomes its
  standby, so it never serves stale data. If both start at once, the lower
  `XB_NODE_ID` serves.
- **The primary dies:** the standby notices within 6 s, waits
  `XB_FAILOVER_S`, then serves. Measured: **1.6 s** with a 1 s setting, with
  all acknowledged messages, including ones only in the dead primary's
  memory.
- **The primary drains:** it hands over, and the standby serves in about
  0.3 s. Upgrades then have no gap: drain one machine, upgrade it, it
  rejoins as standby, then the other.
- **Clients** reach whichever node serves through a load balancer
  (HAProxy, or a virtual IP with keepalived): only the serving node
  listens on the chat and web ports.
- **Limits:** replication is asynchronous (a crash loses what was on the
  wire, normally under a millisecond); there is no third "witness", so
  keep the two on the same network.

### Connection gateways: more phones than one machine holds

At millions of phones, a server runs out of connections first: sockets,
handshakes, encryption, send queues. `xb-gateway` holds those; the core
keeps only the logic and data ([core/src/relay.rs](core/src/relay.rs)).

```bash
# core
XB_GATEWAY_ADDR=0.0.0.0:5400 XB_GATEWAY_SECRET=<secret> ./xb-server
# each gateway
XB_CORE=core1:5400,core2:5400 XB_GATEWAY_SECRET=<secret> XB_HOST=0.0.0.0 XB_HTTP_UPSTREAM=http://core1:8080 ./xb-gateway
```

One encrypted link per gateway carries the decrypted stanzas, many to a
frame and uncompressed (message contents are end-to-end encrypted already,
so compression gains little). If the core
restarts or fails over, gateways tell their phones to come back spread
out, and connect to whichever core serves next.

| 2,000 phones, 5,000 msg/s | Throughput | Delivery p50 / p99 | Core CPU | Gateway CPU |
|---|---|---|---|---|
| straight to the core | 4,923 msg/s | 8.6 / 50 ms | 76 s | - |
| through a gateway | 5,020 msg/s | 4.9 / 29 ms | **37 s** | 77 s |

The core's CPU halves; the encryption moves to the gateways, which you can
multiply. Splitting *users* across several cores isn't built: it matters
only when one core can't route the message rate, far beyond what's
measured here.

## Monitoring

Each program has a local monitoring port (`XB_METRICS_ADDR`; `none` turns
it off):

| Program | Default | Paths |
|---|---|---|
| `xb-server` | 127.0.0.1:9091 | `/metrics` (Prometheus), `/health`, `/ready`, `POST /drain` |
| `xb-gateway` | 127.0.0.1:9093 | `/metrics`, `/health`, `/ready`, `POST /drain` |
| `xb-media` | 127.0.0.1:9094 | `/metrics`, `/health` |

- **xb-server metrics:** connections, users and devices online, queued
  stanzas (on disk and in memory), messages, bytes, refused / busy / slow,
  panics, pushes, saves, lock statistics.
- **`/ready`** answers 503 when the server shouldn't get new traffic: near
  the connection cap, after a failed save, or while draining.
- **xb-gateway:** connections, refused, failed handshakes, slow readers,
  stanzas and bytes each way, the core link. **xb-media:** uploads and
  downloads (requests, bytes, rejected, errors, time).

## Settings

`xb-server`, by environment variable:

| Variable | Default | |
|---|---|---|
| `XB_PORT` | 5222 | chat port (TCP) |
| `XB_WEB_PORT` | 8080 | WebSocket `/ws`, the web app, media links |
| `XB_HOST` | 127.0.0.1 | `0.0.0.0` to let other machines in |
| `XB_DATA` | `./data` | data folder (`xb.redb`, `queue/`, `server_key`, `secret`, `pictures/`) |
| `XB_APP_DIR` | - | a web app to host; `none` = no app |
| `XB_PUBLIC_URL` | `http://<host>:<web port>` | public base URL of the web port |
| `XB_ORIGINS` | - | extra allowed web origins, comma separated |
| `XB_SECRET` | generated, saved in `<data>/secret` | upload-token secret (shared with xb-media) |
| `XB_OTP_ECHO` | - | `1` = return SMS codes to the client (development only) |
| `XB_FCM_CREDENTIALS` | - | Firebase service-account JSON for push wake-ups |
| `XB_MEDIA_URL` | the web port | the media service's public URL for file links |
| `XB_MEDIA_UPSTREAM` | `http://127.0.0.1:8081` | where this process reaches the media service |
| `XB_MAX_CONNECTIONS` | 1000000 | sockets above this are refused at once |
| `XB_MAX_HANDSHAKES` | 1024 | Noise handshakes running at the same time |
| `XB_LOGIN_RATE` / `XB_LOGIN_BURST` | 5000 / 10000 | logins a second (and bursts); more are told `busy` |
| `XB_SEND_QUEUE_MAX` | 16 MB | a connection's unsent bytes; past this it's closed |
| `XB_WRITE_TIMEOUT_MS` | 30000 | a single write taking longer: the reader is closed |
| `XB_QUEUE_WRITE_DELAY_MS` | 5000 | write-back delay; 0 = write at once |
| `XB_QUEUE_COLD_MS` | 300000 | an offline device's stanzas older than this leave memory |
| `XB_METRICS_ADDR` | 127.0.0.1:9091 | monitoring port; `none` = off |
| `XB_DRAIN_SPREAD_S` | 10 | a drain tells clients to come back within this |
| `XB_TAKEOVER` | - | `1` = wait for the running server's ports and data (an upgrade) |
| `XB_PEER` / `XB_REPL_ADDR` | - / 127.0.0.1:5300 | replica pair: the other node, and this one |
| `XB_REPL_SECRET` | - | shared secret of the pair's link |
| `XB_NODE_ID` | `XB_REPL_ADDR` | the lower one serves if both start at once |
| `XB_FAILOVER_S` | 10 | how long the standby waits for a vanished primary |
| `XB_GATEWAY_ADDR` / `XB_GATEWAY_SECRET` | - | where connection gateways connect, and their secret |

## Testing

```bash
cargo test --workspace
```

- **Unit tests:** the codec against stanzas recorded from a reference
  implementation, framing, upload tokens, helpers.
- **Fuzzing,** in pure Rust (no libFuzzer, so no C++ compiler needed), run
  as part of `cargo test`. A failure prints its `XB_FUZZ_SEED`, which
  replays it. A 200,000-input run per fuzzer found nothing.
- **Integration tests** drive the real Android, web and test clients
  against this server: chat, end-to-end encryption, multi-device, groups
  with Sender Keys, media, crashes, drain and takeover, replica-pair
  failover, gateways, overload. They live with the clients and will be
  published with them.

The fuzzers:

| Fuzzer | What it throws | Must hold |
|---|---|---|
| `fuzz_decode_never_panics` | mutated real stanzas, random bytes | the decoder never panics; what it accepts re-encodes, stably |
| `fuzz_frames_never_panic` | frames and broken length prefixes, split at random | the frame decoder never panics |
| `fuzz_cipher_never_panics` | garbage, junk inside valid encryption, fake compression, a real zlib bomb | decryption never panics, and a bomb is capped |
| `fuzz_stanzas_never_panic_or_deadlock` | random, nearly valid stanzas of every kind from logged-in users, against the real handlers | no handler panics, and no deadlock between shards |

For a long run:

```bash
XB_FUZZ_ITERS=1000000 cargo test --release --workspace fuzz_
```

## What comes from WhatsApp, and what is ours

| Part | Source |
|---|---|
| Signal protocol, Sender Keys for groups, per-device keys in multi-device | WhatsApp security whitepaper |
| Noise handshake for the connection; client-side media encryption with the key sent in the message | WhatsApp security whitepaper |
| Binary XMPP (FunXMPP), store-and-forward with deletion on delivery, content-free pushes | whitepaper and engineering talks |
| Paired servers replicating each other, in-memory data with write-back, ~2M connections per server | WhatsApp engineering talks (2012, 2014) |
| redb and fjall, the 5 s write-back, drain and takeover, the gateway split, `phash` checks for groups | our own design, in the spirit of the above |

## License

[MIT](LICENSE)
