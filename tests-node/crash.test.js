// Crash safety: kill the server hard (no shutdown, no flush) right after it
// acknowledged 200 messages for an offline phone. After the restart they
// must all be there, in order.
//
// Two settings (XB_QUEUE_WRITE_DELAY_MS):
//   0     write-through: each stanza is on disk within milliseconds.
//   5000  write-back (the default): a stanza is written only if it's still
//         undelivered after 5 s. A crash inside those 5 s loses it, the price
//         of never writing the many that are delivered sooner, like WhatsApp's
//         write-back cache. After the delay they're safe.

import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';
import { MemoryKeyStore } from '../../xb-chat/clients/node/signal-node.js';

async function login(rs, keys, user, register = false) {
  const c = new Client({ port: rs.port, keyStore: keys[user] });
  c.on('error', () => {});
  await c.connect();
  await (register ? c.register(user, 'password1') : c.login(user, 'password1'));
  return c;
}

async function crashAfterAcks({ delayMs, waitMs }) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-crash-'));
  const env = { XB_QUEUE_WRITE_DELAY_MS: String(delayMs) };
  const keys = { alice: new MemoryKeyStore(), bob: new MemoryKeyStore() };
  let rs = await startRust({ dir, env });
  (await login(rs, keys, 'bob', true)).close();
  const alice = await login(rs, keys, 'alice', true);
  const t0 = performance.now();
  for (let i = 0; i < 200; i++) await alice.sendMessage('bob', `m${i}`);
  const ms = performance.now() - t0;
  console.log(`delay ${delayMs} ms: 200 messages acknowledged in ${ms.toFixed(0)} ms`);
  await new Promise((r) => setTimeout(r, waitMs));
  await rs.crash();

  rs = await startRust({ dir, env });
  assert.equal(await rs.store.pendingCount('bob'), 200);
  const bob = await login(rs, keys, 'bob');
  const got = [];
  bob.on('message', (m) => got.push(m.text));
  for (let i = 0; i < 100 && got.length < 200; i++) await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(got, Array.from({ length: 200 }, (_, i) => `m${i}`), 'all of them, in order');
  alice.close();
  bob.close();
  await rs.close();
  await fs.rm(dir, { recursive: true, force: true });
}

test('write-through: acknowledged messages survive a hard kill 50 ms later', () => crashAfterAcks({ delayMs: 0, waitMs: 50 }));

test('write-back: once the write delay has passed, they survive a hard kill', () => crashAfterAcks({ delayMs: 1000, waitMs: 2500 }));
