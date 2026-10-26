// Overload protection: the server turns work away cleanly instead of
// collapsing. Each test starts a Rust server with small limits.
//   - over the connection cap, new sockets are refused
//   - too many logins at once: "busy, retry in N s"
//   - a client that stops reading is cut off, and loses nothing

import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, test } from 'node:test';
import { startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';
import { MemoryKeyStore } from '../../xb-chat/clients/node/signal-node.js';

const dirs = [];
const open = [];
after(async () => {
  for (const c of open) c.close();
  for (const d of dirs) await fs.rm(d, { recursive: true, force: true });
});

async function server(env) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-overload-'));
  dirs.push(dir);
  return startRust({ dir, env });
}

function client(rs, keyStore = new MemoryKeyStore()) {
  const c = new Client({ port: rs.port, keyStore });
  c.on('error', () => {});
  open.push(c);
  return c;
}

const tick = (ms) => new Promise((r) => setTimeout(r, ms));

test('over the connection cap, new sockets are refused; a freed slot is used again', async () => {
  const rs = await server({ XB_MAX_CONNECTIONS: '2' });
  const a = client(rs);
  const b = client(rs);
  await a.connect();
  await b.connect();
  await assert.rejects(client(rs).connect(), 'the third is refused');
  a.close();
  await tick(300);
  await client(rs).connect(); // a slot is free again
  await rs.close();
});

test('too many logins at once: "busy" with a retry hint, and the account is untouched', async () => {
  // Three logins in the bucket, refilled very slowly.
  const rs = await server({ XB_LOGIN_RATE: '0.01', XB_LOGIN_BURST: '3' });
  const keys = new MemoryKeyStore();
  const first = client(rs, keys);
  await first.connect();
  await first.register('alice', 'password1'); // 1
  first.close();
  const second = client(rs, keys);
  await second.connect();
  await second.login('alice', 'password1'); // 2
  second.close();
  const third = client(rs, keys);
  await third.connect();
  await third.login('alice', 'password1'); // 3
  third.close();
  const fourth = client(rs, keys);
  await fourth.connect();
  const err = await fourth.login('alice', 'password1').then(() => null, (e) => e);
  assert.equal(err?.code, 'busy', 'the fourth is turned away');
  assert.ok(err.retry >= 1 && err.retry <= 5, `retry hint ${err.retry}`);
  assert.equal(await rs.store.hasUser('alice'), true, 'a busy answer changes nothing');
  await rs.close();
});

test('a client that stops reading is cut off; its messages wait in its queue', async () => {
  const rs = await server({ XB_SEND_QUEUE_MAX: String(64 * 1024), XB_WRITE_TIMEOUT_MS: '1500' });
  const aliceKeys = new MemoryKeyStore();
  const bobKeys = new MemoryKeyStore();
  for (const [u, k] of [['alice', aliceKeys], ['bob', bobKeys]]) {
    const c = client(rs, k);
    await c.connect();
    await c.register(u, 'password1');
    c.close();
  }
  await tick(200);
  const alice = client(rs, aliceKeys);
  await alice.connect();
  await alice.login('alice', 'password1');
  const bob = client(rs, bobKeys);
  await bob.connect();
  await bob.login('bob', 'password1');
  const bobClosed = new Promise((r) => bob.once('close', r));
  // What reaches Bob, on this connection or the next.
  const got = [];
  bob.on('message', (m) => got.push(Number(m.text.split(' ')[0])));
  // Bob's phone freezes: it stops reading from the socket.
  bob.socket.pause();

  const text = 'x'.repeat(20000);
  const n = 150; // ~3 MB: more than the socket buffers and the 64 KB queue (and under the 200-stanza burst limit)
  for (let i = 0; i < n; i++) {
    try {
      await alice.sendMessage("bob", `${i} ${text}`);
    } catch (e) {
      throw new Error(`message ${i}: ${e.message}`);
    }
  }
  // Alice is never slowed down by Bob: every message was acknowledged.
  for (let i = 0; i < 100 && !rs.log.some((l) => l.includes('[overload]')); i++) await tick(100);
  assert.ok(rs.log.some((l) => l.includes('[overload]')), 'the server cut the slow reader off, and said so in its log');
  // (A paused socket can't see the server's goodbye; once it reads again, it does.)
  bob.socket.resume();
  const closed = await Promise.race([bobClosed.then(() => true), tick(10_000).then(() => false)]);
  assert.ok(closed, "bob's connection is closed");

  // Bob comes back: everything is still there, in order.
  await tick(300);
  assert.ok((await rs.store.pendingCount('bob')) > 100, 'his messages wait in his queue');
  const bob2 = client(rs, bobKeys);
  bob2.on('message', (m) => got.push(Number(m.text.split(' ')[0])));
  await bob2.connect();
  await bob2.login('bob', 'password1');
  for (let i = 0; i < 100 && got.length < n; i++) await tick(100);
  const missing = [...Array(n).keys()].filter((i) => !got.includes(i));
  assert.equal(new Set(got).size, n, `got all ${n} (redelivered copies are de-duplicated); missing ${missing.length}: ${missing.slice(0, 20)} ; still queued ${await rs.store.pendingCount("bob")}`);
  await rs.close();
});
