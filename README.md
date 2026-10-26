# xb-server (Rust)

The xb-chat server in Rust: a complete port of the Node server in
[../xb-chat/server](../xb-chat/server). It speaks the same protocol
([PROTOCOL.md](../xb-chat/protocol/PROTOCOL.md)), takes the same settings,
and uses **the same data folder**. You can stop the Node server and start
this one (or the other way round), and phones, the web app and the Node
client keep working: same server key, same accounts, same queued messages.
It keeps its data in a real database, though: see [Storage](#storage).

```bash
cargo build --release
```

```bash
XB_DATA=../xb-chat/data ./target/release/xb-server
```

## What it does

Everything the Node server does:

- **Connections:** the `XB 2 1` header, the Noise XX handshake, and AES-GCM frames with zlib, over TCP and WebSocket (`/ws`).
- **The binary codec:** FunXMPP layout, byte for byte the same as `codec.js` (tested against bytes from the Node codec).
- **Login:**
  - phone number and SMS code;
  - the device key alone;
  - QR linking of companions, with rotating codes;
  - the legacy password (`XB_LEGACY_PASSWORD=1`).
- **Multi-device:**
  - a queue per device, with at-least-once delivery;
  - acks and delivered/read receipts, plus read-self on your other devices;
  - `stale-devices` checks for end-to-end encryption;
  - group messages with sender keys: one ciphertext for the whole group, checked against the device list by `phash` (PROTOCOL.md §10.5).
- **Keys and the rest:**
  - pre-keys and bundles;
  - groups;
  - presence, last seen and typing;
  - profile name, About and photo;
  - privacy settings and "My contacts";
  - contact discovery.
- **Media:** uploads, resumable uploads, downloads, HMAC tokens, and expiry after 30 days.
- **Push:** FCM wake-ups. It signs the OAuth JWT with the service-account key and calls FCM HTTP v1.
- **Web:** serves the web app with the same security headers, and checks the WebSocket origin.

## Settings

The same environment variables as `server/main.js`:

| Variable | Default | |
|---|---|---|
| `XB_PORT` | 5222 | chat port (TCP) |
| `XB_WEB_PORT` | 8080 | WebSocket `/ws`, media, web app |
| `XB_HOST` | 127.0.0.1 | `0.0.0.0` to let phones on Wi-Fi in |
| `XB_DATA` | `./data` | data folder (`xb.redb`, `queue/`, `server_key`, `secret`, `media/`, `pictures/`) |
| `XB_ROOT` | `../xb-chat` | the xb-chat repository: web app and its shared protocol files |
| `XB_APP_DIR` | `<XB_ROOT>/clients/web` | web app to host; `none` = no app |
| `XB_PUBLIC_URL` | `http://<host>:<web port>` | base URL put in media links |
| `XB_ORIGINS` | — | extra allowed web origins, comma separated |
| `XB_SECRET` | generated, saved in `<data>/secret` | media token secret |
| `XB_OTP_ECHO` | — | `1` = return SMS codes to the client (development) |
| `XB_LEGACY_PASSWORD` | — | `1` = allow username + password accounts |
| `XB_FCM_CREDENTIALS` | — | Firebase service-account JSON for push wake-ups |
| `XB_MEDIA_URL` | — | public URL of a separate media service (`xb-media`); this process then stores no files |
| `XB_MEDIA_UPSTREAM` | `XB_MEDIA_URL` | where this process reaches the media service, if different |
| `XB_MAX_CONNECTIONS` | 1000000 | sockets above this are refused at once |
| `XB_MAX_HANDSHAKES` | 1024 | Noise handshakes running at the same time |
| `XB_LOGIN_RATE` / `XB_LOGIN_BURST` | 5000 / 10000 | logins a second (and bursts); more are told `busy` |
| `XB_SEND_QUEUE_MAX` | 16 MB | a connection's unsent bytes; past this it's too slow, and closed |
| `XB_WRITE_TIMEOUT_MS` | 30000 | one write that takes longer: the reader is too slow, and closed |
| `XB_QUEUE_WRITE_DELAY_MS` | 5000 | write-back: a queued stanza is written only if still undelivered after this; 0 = at once |
| `XB_QUEUE_COLD_MS` | 300000 | an offline device's queued stanzas older than this leave memory |
| `XB_METRICS_ADDR` | 127.0.0.1:9091 | `/metrics`, `/health`, `/ready`, `POST /drain` (local only by default); `none` = off |
| `XB_DRAIN_SPREAD_S` | 10 | a drain tells clients to come back within this many seconds |
| `XB_TAKEOVER` | — | `1` = wait for the running server's ports and data (an upgrade) |
| `XB_PEER` | — | the other node of a replica pair (its `XB_REPL_ADDR`) |
| `XB_REPL_ADDR` | 127.0.0.1:5300 | this node's replication address |
| `XB_REPL_SECRET` | — | shared secret of the pair's link |
| `XB_NODE_ID` | `XB_REPL_ADDR` | the lower one serves if both start at once |
| `XB_FAILOVER_S` | 10 | how long the standby waits for a vanished primary |
| `XB_GATEWAY_ADDR` | — | where connection gateways connect (xb-gateway) |
| `XB_GATEWAY_SECRET` | — | shared secret of the gateways' link |

**The phone setup from [SETUP.md](../xb-chat/docs/SETUP.md):**

```bash
XB_HOST=0.0.0.0 XB_DATA=../xb-chat/data-test XB_PORT=15222 XB_WEB_PORT=18080 XB_OTP_ECHO=1 XB_FCM_CREDENTIALS=../xb-chat/data-test/fcm-service-account.json ./target/release/xb-server
```

**The firewall:** Windows lets `node.exe` accept connections from the
phone, but it asks again for a new program. The first time `xb-server.exe`
listens on `0.0.0.0`, allow it on **private** networks.

**Don't run both servers on one data folder at the same time.** Taking
turns is fine (see below). Two Rust servers can't: the second one can't open
`xb.redb`.

## Media as its own service

Files can run in a separate process, `xb-media`, like WhatsApp's separate
media servers. It has no chat state at all, so it can sit on other
machines, scale on its own, and fail without taking chat down.

```bash
XB_DATA=../xb-chat/data-test XB_MEDIA_PORT=18081 ./target/release/xb-media
```

```bash
XB_MEDIA_URL=http://<this machine>:18081 … ./target/release/xb-server
```

How the two share the work:

- **Tokens:** the chat server still issues upload tokens (over the
  encrypted chat connection). `xb-media` checks them with the **same
  secret**, `XB_SECRET` or the shared `<data>/secret`.
- **Where clients go:** `media_conn` and the links in messages point at
  the media service, so clients upload and download there directly.
- **Apps already installed** still send media to the chat server's web
  port. The chat server streams those requests through to `xb-media` with
  no buffering, no disk and no hashing; only the bytes pass.
- **Android** now goes wherever the server says. Only a loopback address
  (a development server saying `127.0.0.1`) is replaced by the server host
  the phone is set to, keeping the port.
- **CORS:** `xb-media` answers it for web pages (the tokens are bearer
  tokens, never cookies).

| `xb-media` setting | Default | |
|---|---|---|
| `XB_MEDIA_HOST` / `XB_MEDIA_PORT` | 127.0.0.1 / 8081 | where it listens |
| `XB_DATA` / `XB_MEDIA_DIR` | `./data` / `<data>/media` | where the secret and the files are |
| `XB_MEDIA_PUBLIC_URL` | `http://<host>:<port>` | base of the file links |
| `XB_MEDIA_MAX_MB` | 64 | largest file |

**Measured, same laptop** (chat at 5,000 msg/s while 24 workers upload
unique 8 MB files at 310–350 MB/s, three runs each):
- with media in the chat process, ack p99 was 36–40 ms;
- with media in `xb-media`, it was 32–42 ms.

So on one machine the split doesn't change chat latency: both processes
share the same CPU and disk, and the async server already copes with heavy
uploads. What the split buys:

- **Fault isolation**, tested
  ([tests-node/media-split.test.js](tests-node/media-split.test.js)):
  `xb-media` is killed in the middle of a chat, and messages keep flowing.
  Uploads fail, and the old path answers 502, until it's back.
- **Media on other machines**, scaled on their own, or object storage
  behind it later.

The whole suite also runs with every test server split
(`XB_RS_SPLIT_MEDIA=1 node --test tests-node/`); all 69 tests pass.

## Storage

Two embedded stores, both pure Rust, each suited to its kind of data (the
same split WhatsApp makes):

| Store | Holds | Why this one |
|---|---|---|
| **`<data>/xb.redb`**, [redb](https://www.redb.org) (a B-tree with ACID transactions) | accounts, devices, keys, groups, last seen | read-mostly, must never be lost |
| **`<data>/queue/`**, [fjall](https://github.com/fjall-rs/fjall) (an LSM tree) | the offline queue | almost all "append, then delete soon", which an LSM writes sequentially |

**Memory is the fast path** (like WhatsApp's Mnesia): everything is read
from memory, and the stores only make it durable.

**Records (redb):**
- every change marks the record it touched (one user, one device list…);
- a writer task saves all records marked since its last save in **one
  transaction**, so under load many changes share one disk sync (group
  commit);
- each record is JSON, in the same shapes as Node's `state.json`.

**The offline queue (fjall), write-back** (WhatsApp's write-back cache):
- a queued stanza stays only in memory for its first
  `XB_QUEUE_WRITE_DELAY_MS` (5 s by default);
- it's written only if it's still undelivered after that, and deleted again
  once delivered;
- nearly every message and receipt is delivered sooner, so it **never
  touches the disk**.

Measured at 5,000 msg/s for 15 s (2,000 phones, about 75,600 messages):

| | write-through (delay 0) | write-back (5 s) |
|---|---|---|
| queued stanzas written to disk, then deleted again | 143,851 + 143,851 | **0 + 0** |
| delivered before being written | 7,419 | **151,090** (all) |
| time committing to disk | 7.5 s | **0.68 s** (only account records) |

**Cold queues leave memory:** a device that stays offline keeps its
stanzas on disk only (`XB_QUEUE_COLD_MS`, 5 min). After a restart the
queue loads as an index, and stanzas are read back from disk when their
device logs in.

| Table (redb) | Key | Value |
|---|---|---|
| `users` | user | account, profile, privacy, contacts |
| `last_seen` | user | ms |
| `devices` | user | that user's devices (Noise key → record) |
| `prekeys` | (user, device) | identity, signed pre-key, one-time pre-keys |
| `groups` | group id | subject, creator, members |
| `meta` | name | counters, format version |

The queue (fjall): key `owner NUL dedup key`, value `arrival order | time | encoded stanza`.

**Migration:** an older version kept the queue in redb, and the first start
moves it. Tried on a copy of real data, all 1,318 queued stanzas moved, and
exported back to `state.json` they were all there.

**Durability, the trade-off:**
- the sender's ack goes out before anything is written, as at WhatsApp;
- **records** are saved within milliseconds;
- **queued stanzas** are written after the write delay. A crash inside it
  loses those stanzas, though the sender already saw one tick.
- [tests-node/crash.test.js](tests-node/crash.test.js) checks both modes,
  with a hard kill right after 200 acknowledged messages:
  - write-through (`XB_QUEUE_WRITE_DELAY_MS=0`): all survive, even 50 ms later;
  - write-back: all survive once the delay has passed.

WhatsApp covered that window with the replica node's memory. Here the
replica pairs (step 6 of the plan) will do the same. Until then, set the
delay to 0 if a lost message in a crash matters more than the disk work.

**Swapping with the Node server** (it only knows `state.json`):

- **First start** on a folder that has a `state.json` and no `xb.redb`:
  it's imported automatically.
- **Rust → Node:** stop the Rust server, then write `state.json` for Node:

  ```bash
  ./target/release/xb-server export-json
  ```

- **Node → Rust again:** stop Node, then read its `state.json` back in
  (this replaces `xb.redb`):

  ```bash
  ./target/release/xb-server import-json
  ```

  If you forget, the server warns that `state.json` is newer than
  `xb.redb`, and keeps using `xb.redb`.

Both commands use `XB_DATA` like the server.

## Tests

1. Unit tests (codec fixtures from Node, framing, helpers):

   ```bash
   cargo test
   ```

2. The Node integration tests, run against this server. These are copies of
   `../xb-chat/test/{e2e,e2ee,multidevice,web}.test.js`, made by
   `tests-node/port-tests.py`: the same clients and assertions, with a
   different server underneath. After `cargo build`:

   ```bash
   node --test --test-concurrency=1 tests-node/
   ```

   The `XB_TEST_ADMIN=1` hooks let the tests read the store. Never set them in
   production.

3. [tests-node/swap.test.js](tests-node/swap.test.js): Node → Rust → Node
   → Rust on one data folder, with `export-json` and `import-json`.
4. [tests-node/crash.test.js](tests-node/crash.test.js): a hard kill after
   200 acknowledged messages loses none of them.

The state on 24 October 2026: 16 unit tests (with the 4 fuzzers) and 69 Node tests pass (also with every server's media split out); a 200,000-input fuzz run per fuzzer found no panic or deadlock. It was
also checked against the real Android app over Wi-Fi: an FCM wake-up, the
phone logging in with its device key, the queued messages, and the delivered
receipts.

## The code

| File | What | Node original |
|---|---|---|
| [src/main.rs](src/main.rs) | settings, startup, timers, the writer task, `export-json` / `import-json` | `server/main.js` |
| [src/transport.rs](src/transport.rs) | header, Noise (snow), AES-GCM frames, zlib; TCP or WebSocket | `protocol/transport.js`, `noise.js` |
| [src/frame.rs](src/frame.rs) | 3-byte length frames | `protocol/framing.js` |
| [src/codec.rs](src/codec.rs), [src/tokens.rs](src/tokens.rs) | binary stanzas | `protocol/codec.js`, `tokens.js` |
| [src/server.rs](src/server.rs) | sessions, login, routing, receipts, iq handlers | `server/server.js`, `server/auth/*` |
| [src/store.rs](src/store.rs) | accounts, devices, keys, groups, queues, in memory; marks what changed | `server/store.js` |
| [src/db.rs](src/db.rs) | redb tables: load at start, save changed records in one transaction; the writer task | — (Node: the `state.json` snapshot) |
| [src/queue_db.rs](src/queue_db.rs) | the offline queue on fjall (LSM): write-back, cold stanzas read back | — |
| [src/media.rs](src/media.rs) | upload/download, resumable, tokens, expiry | `server/media.js` |
| [src/gateway.rs](src/gateway.rs) | HTTP: `/ws`, media, static web app | `server/gateway.js`, `ws.js` |
| [src/push.rs](src/push.rs) | FCM | `server/push.js` |
| [src/keys.rs](src/keys.rs) | the server's key file | `loadServerKey` in `main.js` |
| [src/admin.rs](src/admin.rs) | test-only hooks | — |
| [src/bin/loadgen.rs](src/bin/loadgen.rs) | load test: many simulated phones | — |
| [src/bin/xb-media.rs](src/bin/xb-media.rs) | the media service on its own | `server/media.js` as a server |
| [src/lib.rs](src/lib.rs) | the modules, shared by both binaries | — |

**How it's built:**
- Each connection is a tokio task, and a second task writes its encrypted
  frames, fed by a channel.
- The shared state is split into **64 shards by `hash(user)`**. Each shard
  has its own lock and holds those users' accounts, devices, keys, queues,
  connections and presence watchers. A group lives in the shard of
  `hash(group id)`; each member's shard keeps an index of their groups.
- **No deadlocks, by one rule:** a handler holds one shard, or several taken
  together in ascending order (a 1:1 message takes sender + recipient; a
  group message takes the members' shards). It never takes a lock while
  holding one. Locks are never held across an `.await`, so each handler is
  plain synchronous code, like the single-threaded Node version.
- The only slow work runs outside the locks: password hashing (on a
  blocking thread), FCM calls and database commits.
- The same split is the plan for several machines: a shard per node instead
  of per lock.

**Differences from Node, on purpose:**
- Transport encryption is done directly with AES-GCM after snow's handshake.
  snow's own transport caps messages at 64 KB, and stanzas go up to 1 MB.
- No C compiler is needed: the crypto is pure Rust, and HTTPS uses Windows'
  own TLS (SChannel). On Linux it uses OpenSSL.

## Performance

[src/bin/loadgen.rs](src/bin/loadgen.rs) runs simulated phones against a
server:
1. each phone does the Noise handshake and logs in with a phone code (the
   first run), then with its device key;
2. it publishes keys;
3. it sends messages to random other phones at a set total rate;
4. it returns delivered receipts and acks them.

That's the full cycle real phones cause.

Start the server with `XB_OTP_ECHO=1` (and `XB_STATS_SECS=5` for lock
statistics in its log), then:

```bash
cargo run --release --bin loadgen -- --port 5222 --users 2000 --rate 5000 --secs 15
```

It prints messages per second and latency percentiles: **ack** (one tick),
**deliver** (on the other phone), and **receipt** (two ticks).

**Measured on 22 October 2026**, on a Ryzen 9 5900HX laptop with 16 threads,
the server and the load generator on the same machine. The numbers are a
lower bound.

| 2,000 phones | one global lock | 64 shards |
|---|---|---|
| 5,000 msg/s | ack p50 9.4 ms, p99 50 ms; lock wait 200 µs | ack p50 **3.9 ms**, p99 **18 ms**; lock wait 3.6 µs |
| 20,000 msg/s | collapses: 8,600 msg/s, acks take seconds | **16,000 msg/s**, nothing lost; ack p50 108 ms |
| 40,000 msg/s | — | about 26,000 msg/s on average, 51,000 at peak; the CPU is saturated |

The load test also found **a real bug**:
- a client that sends its last handshake message and `<auth>` back to back
  could hang, because both arrived in one read and the `<auth>` stayed
  unread in the buffer;
- under load, a third of the logins hung that way;
- it's fixed: buffered frames are processed before waiting for more bytes.

It also found two O(n) operations on every message:
- removing an emptied queue from the map of all queues;
- counting a user's offline stanzas by scanning every queue.

Both are now O(1) or limited to that user.

### Overload: slow down, don't fall over

What happens when there's more work than the machine can do:

- **Too many sockets:** above `XB_MAX_CONNECTIONS`, a new socket is closed
  before any work is spent on it.
- **A reconnect storm** (a restart, or a network coming back):
  - at most `XB_MAX_HANDSHAKES` handshakes run at once;
  - logins are limited to `XB_LOGIN_RATE` a second;
  - the rest get `<failure reason="busy" retry="1-5"/>`, and well-behaved
    clients come back after that, with jitter.
- **A client that stops reading** (a frozen phone, or a deliberately slow
  attacker):
  - its send queue is limited to `XB_SEND_QUEUE_MAX` bytes, and a single
    write to `XB_WRITE_TIMEOUT_MS`;
  - past either, it's closed;
  - nothing is lost: what it hadn't acknowledged waits in its offline queue.
- **A backlog:** a phone back after a long time offline gets its offline
  queue **in batches**, as fast as it reads them. It isn't mistaken for a
  slow reader, and new messages wait at the end of the backlog, so the
  order is kept.
- **Running out of file handles:** the accept loop pauses instead of
  spinning on the error.
- **The clients:**
  - Android and Web wait a random 1–5 s before reconnecting after a dropped
    connection;
  - they back off with jitter on failures;
  - they honour `retry`.

  A restart then doesn't bring every phone back in the same second.

The `[stats]` line counts what was turned away (`refused`, `busy`,
`slow`). [tests-node/overload.test.js](tests-node/overload.test.js) checks
each case.

**Reconnect storm, measured** (22–23 October 2026, same laptop):
- 8,000 registered phones logged in all at once, 4,000 in parallel;
- the login limit was set to 1,000 a second.

All 7,999 registered phones were in after 9.0 s. 4,701 attempts were told
"busy", and each one got in on a later try. Nothing hung, and nothing was
refused. At 5,000 msg/s afterwards the numbers were unchanged (ack p50 5 ms,
p99 21 ms), with no false "slow" cut-offs.

### Monitoring

A second port, `XB_METRICS_ADDR` (default `127.0.0.1:9091`, reachable only
from this machine):

| Path | Answers |
|---|---|
| `/metrics` | Prometheus text: connections (open, cap, total), users and devices online, accounts, queued stanzas, stanzas and messages, bytes in and out, refused / busy / slow, **panics**, pushes and push failures, saves (count, records, time, errors), lock acquisitions, wait and hold time, version |
| `/health` | `200 ok` while the process answers (liveness) |
| `/ready` | `200 ready`, or `503` when it shouldn't get new traffic: at 95% of the connection cap, after a failed save in the last 30 s, or while draining (readiness, for a load balancer) |
| `POST /drain` | drain before a restart (see Upgrades) |

To try it: `curl http://127.0.0.1:9091/metrics`. Point Prometheus at it,
and Grafana for graphs.

The other two programs have the same kind of port (`XB_METRICS_ADDR` each;
`none` turns it off):

| Program | Default | Paths | Metrics |
|---|---|---|---|
| `xb-gateway` | `127.0.0.1:9093` | `/metrics`, `/health`, `/ready` (`503` with no core link, or draining), `POST /drain` | `xb_gateway_*`: connections (open, cap, total), refused, failed handshakes, slow readers closed, stanzas and bytes each way, core link up and how often it came up |
| `xb-media` | `127.0.0.1:9094` | `/metrics`, `/health` | `xb_media_upload_*` and `xb_media_download_*`: requests, bytes, rejected (4xx), errors (5xx), time spent |

A gateway's drain (Ctrl+C or `POST /drain`) closes its ports, tells its
phones "restart, come back within `XB_DRAIN_SPREAD_S`", so the load
balancer sends them to another gateway, and exits.

**Panics:**
- a handler that panics (a bug some input reached) no longer kills its
  connection's task silently;
- it's caught, logged as `[panic]`, counted in `xb_panics_total`, and that
  connection is closed with `internal-error`;
- the rest of the server carries on: shard locks recover, and the
  connection's cleanup runs.

### Upgrades: drain, then take over

A restart used to drop every phone in the same second, and they all came
back in the next few (the login limit then told most of them "busy"). Now
a restart is a **drain**: Ctrl+C or `POST /drain` on the metrics port.

1. The old process closes its ports at once.
2. It tells every connected client `<stream:error code="restart" retry="N"/>`,
   with N random over `XB_DRAIN_SPREAD_S` (default 10). The clients
   (Android, web, Node) wait N s plus up to 1 s, so the reconnects arrive
   evenly over the window.
3. It saves everything (records, and every queued stanza still only in
   memory under write-back), then exits.

The new version starts *before* the drain, with `XB_TAKEOVER=1`:

- it binds the ports as soon as the old process lets go, so new
  connections wait in the system's queue instead of being refused;
- it opens the data files as soon as the old process exits (redb locks
  them, so two processes can never write at once);
- it then serves, with nothing lost.

```bash
XB_TAKEOVER=1 ./xb-server-new &    # waits (at most XB_TAKEOVER_WAIT_S, 120)
curl -X POST http://127.0.0.1:9091/drain
```

[tests-node/drain.test.js](tests-node/drain.test.js) shows the whole thing:
- 20 clients online and 100 stanzas held only in memory;
- the new process serving **83 ms** after the drain began;
- every client told `restart` with spread-out retries;
- all 100 stanzas delivered, in order.

**What it can't do:** with one server, the data can't be open in two
processes at once, so there is still a gap of about a tenth of a second
with nobody accepting. Clients ride through it, because they were told to
come back later anyway. A truly seamless upgrade, where nobody notices,
needs a second node: drain one while the other serves (see Replica pairs).

### Replica pairs: a machine can die

Two servers, each with its own data folder, started the same way, each
naming the other ([src/replica.rs](src/replica.rs)). One serves (the
primary); the other (the standby) keeps a live copy of everything on its
own disk and takes over if the primary goes.

```bash
# machine 1                                  # machine 2
XB_REPL_ADDR=0.0.0.0:5300                    XB_REPL_ADDR=0.0.0.0:5300
XB_PEER=machine2:5300                        XB_PEER=machine1:5300
XB_REPL_SECRET=<the same long secret>        XB_REPL_SECRET=<the same>
XB_NODE_ID=1  ./xb-server                    XB_NODE_ID=2  ./xb-server
```

**What the standby gets**, over an encrypted link:
1. The server's key and token secret, so it *is* the same server to the
   phones (they pin the key).
2. A full copy: every record, every queued stanza, every profile picture.
3. Then every change as it happens: each record as it's saved, each stanza
   the moment it's queued or delivered.

Note the last point. The primary writes a queued stanza to its own disk
only if it's still undelivered after 5 s (write-back), but the standby has
it within a fraction of a millisecond, over the network. So a stanza is
safe once it's on two machines, long before it's on either disk; the
standby still writes it to its own disk within 50 ms. This is how a pair
beats one server's disk, at no extra disk work on the primary.

**Who serves** is decided at startup, by asking the peer:

| The peer is | This node |
|---|---|
| serving | follows it |
| a standby with a complete copy | tells it to take over, then follows it (its copy is newer than our disk, which may have lost its write-back window in a crash) |
| starting too | the lower `XB_NODE_ID` serves |
| unreachable (for `XB_PEER_WAIT_S`, 5) | serves |

So a machine that comes back after a crash never serves stale data: it
rejoins as the standby. A standby moves its old copy to `<data>/previous/`
before copying afresh (nothing is ever deleted).

**When the primary goes:**
- It dies: the standby notices within 6 s (pings every 2 s), waits
  `XB_FAILOVER_S` (10) for it to come back, then serves.
- It drains (Ctrl+C, `POST /drain`): it hands over, so the standby serves
  at once (about 0.3 s), after everything sent so far.

**One address for the clients:** only the serving node listens on the chat
and web ports, so whatever the clients connect to sends each connection to
the node that accepts: a load balancer (HAProxy, a virtual IP with
keepalived) or, for development, [tools/pair-front.mjs](tools/pair-front.mjs).

**Upgrades with no gap:** drain the primary (the standby takes over), then
start the new version on that machine (it follows); then the same on the
other machine.

[tests-node/pair.test.js](tests-node/pair.test.js):
- A serves, B copies; A is killed with 100 acknowledged stanzas only in
  its memory;
- B serves **1.6 s** later (failover set to 1 s), same server key, all 100
  in order;
- A comes back and follows B;
- B drains, and A serves **0.3 s** later, with what B had received.

**Limits:**
- Replication is asynchronous: a stanza is acknowledged before the standby
  has it. A crash loses what was on the wire, normally under a millisecond.
- No witness: if the link between the two breaks while clients can still
  reach both, both could serve (split brain). Keep them on the same
  network; the safe setting for a doubtful network is a long `XB_FAILOVER_S`.
- Media files aren't replicated: run the media service (xb-media) on
  storage both can reach.
- If both machines die, start the one that served last first.
- More connections than one machine holds: connection gateways (next).

### Connection gateways: more phones than one machine holds

At millions of phones, a server runs out of connections first: sockets,
Noise handshakes, encryption and compression, a send queue per phone. The
routing itself (who gets what) is cheap. So those split apart
([src/relay.rs](src/relay.rs), [src/bin/xb-gateway.rs](src/bin/xb-gateway.rs)):

```
phones ──► gateway 1 ─┐
phones ──► gateway 2 ─┼──► core (+ its standby)
phones ──► gateway 3 ─┘
```

- **Gateways** accept the phones (TCP and WebSocket), run the handshakes,
  encrypt and compress, keep each phone's send queue, and close readers
  that are too slow. They keep no state; run as many as needed, behind a
  load balancer.
- **The core** is the server as before (a replica pair, if you like). It
  runs the same session for a phone on a gateway as for one on its own
  port, so everything works the same.
- **Between them**, one encrypted link per gateway (`XB_GATEWAY_SECRET`)
  carries the decrypted stanzas, many to a frame and uncompressed (the
  messages are end-to-end ciphertext, which doesn't shrink). The core hands
  each gateway the server's key, so phones see the same server through any
  of them.
- **If the core restarts** or fails over, the gateways tell their phones
  "restart, come back in N s" and connect to whichever core serves next
  (`XB_CORE` takes both of a pair).

```bash
# core
XB_GATEWAY_ADDR=0.0.0.0:5400 XB_GATEWAY_SECRET=<secret> ./xb-server
# each gateway
XB_CORE=core1:5400,core2:5400 XB_GATEWAY_SECRET=<secret> XB_HOST=0.0.0.0 \
XB_HTTP_UPSTREAM=http://core1:8080 ./xb-gateway
```

**Measured** on this laptop, with everything on one machine (load tool included):

| 2,000 phones, 5,000 msg/s | Throughput | Delivery p50 / p99 | Core CPU | Gateway CPU |
|---|---|---|---|---|
| straight to the core | 4,923 msg/s | 8.6 / 50 ms | 76 s | — |
| through a gateway | 5,020 msg/s | 4.9 / 29 ms | **37 s** | 77 s |

| 10,000 idle phones | Core memory | Gateway memory |
|---|---|---|
| straight to the core | 10.7 KB each | — |
| through a gateway | 7.2 KB each | 7.4 KB each |

- The core's CPU halves: the encryption moves to the gateways, which you
  can multiply. The core is the one part you can't.
- The first version sent each stanza as its own encrypted, compressed frame
  and its own write: the gateway used 3.7 cores and delivery took 3.6 s.
  Batching the link fixed both.
- Every protocol test also runs through a gateway:
  `XB_RS_VIA_GATEWAY=1 node --test tests-node/e2e.test.js …` (59 tests), and
  [tests-node/gateway.test.js](tests-node/gateway.test.js) runs two
  gateways through a core restart.

**Not done:** splitting the *users* over several cores. That only matters
when one core can't route the message rate, far beyond what's measured
here, and it touches every handler (keys, profiles, groups and presence all
cross users).

### Fuzzing

Everything that reads bytes or stanzas from the internet is fuzzed,
[src/fuzz.rs](src/fuzz.rs), in pure Rust. libFuzzer would need a C++
compiler, so these use a seeded random generator, mutations of real
stanzas from the Node codec, and a stanza grammar:

| Fuzzer | What it throws | Must hold |
|---|---|---|
| `fuzz_decode_never_panics` | mutated real stanzas, random bytes | the decoder never panics; what it accepts re-encodes, and is stable after that |
| `fuzz_frames_never_panic` | frames and broken length prefixes, split at random | the frame decoder never panics |
| `fuzz_cipher_never_panics` | garbage, junk inside valid encryption, fake compression, a real zlib bomb | decryption never panics, and a bomb is capped |
| `fuzz_stanzas_never_panic_or_deadlock` | random, nearly valid stanzas of every kind (plus well-formed messages) from three logged-in users in a group, and from a connection that isn't logged in, against the real handlers | no handler panics, and no deadlock between shards (the run has a timeout) |

They run quickly as part of `cargo test`. For a long run:

```bash
XB_FUZZ_ITERS=1000000 cargo test --release --lib fuzz_
```

A failure prints its `XB_FUZZ_SEED`, which replays it exactly.

### How many connections

An idle connection is what most phones are most of the time: logged in,
nothing to send. Measured with `loadgen --rate 0`: 16,000 idle connections,
the server's private memory before and after.

| Version | Per idle connection |
|---|---|
| first measurement | ~40 KB |
| wait until the socket is readable, then read into a temporary buffer (no 16 KB buffer held per connection) | ~14 KB |
| handshake, password check and ciphers on the heap (`Box`), so each connection's task stays small for its whole life | ~11 KB |
| messages boxed in the send queue (its first block of 32 slots shrinks from ~2.6 KB) | **~8 KB** |

The two tasks of a connection now take 1.3 KB (reader) and 0.5 KB
(writer); the AES-GCM keys take 2 KB. Separately, every registered account
costs about 6–7 KB of memory (profile, devices, pre-keys) and about 3.4 KB
on disk.

**What that means:**
- **1 million connected phones ≈ 8 GB**, and **2 million (WhatsApp's 2012 figure
  per server) ≈ 16 GB** of server memory, plus the kernel's socket buffers
  and the accounts' data.
- That fits a single Linux server with 32–64 GB, raised file limits
  (`ulimit -n`) and tuned TCP settings. This has **not been run yet**:
  this laptop can only open about 16,000 test connections, because Windows
  gives the load tool 16,384 outgoing ports.
- A real test of 1M+ needs a Linux server and several client machines.

**More than one machine:** connection gateways (above) hold the
connections; the core keeps 7 KB per phone and half the CPU.
