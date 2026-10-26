// Upgrades: the running server drains (POST /drain) while the new one
// (XB_TAKEOVER=1) waits for its ports and data files. Nothing may be lost:
// stanzas still only in memory (write-back) are saved by the drain, and
// every connected client is told "restart, retry in N s", N spread out.

import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';
import { MemoryKeyStore } from '../../xb-chat/clients/node/signal-node.js';

const freePort = () =>
  new Promise((resolve) => {
    const s = net.createServer().listen(0, '127.0.0.1', () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });

const name = (i) => `user${'abcdefghijklmnopqrst'[i]}`;

async function login(port, keys, user, register = false) {
  const c = new Client({ port, keyStore: keys[user] });
  c.on('error', () => {});
  await c.connect();
  await (register ? c.register(user, 'password1') : c.login(user, 'password1'));
  return c;
}

test('drain + takeover: clients told to come back spread out, nothing lost', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-drain-'));
  const keys = {};
  const users = ['alice', 'carol', ...Array.from({ length: 20 }, (_, i) => name(i))];
  for (const u of users) keys[u] = new MemoryKeyStore();

  // Fixed ports (the new process must take over the same ones). Another
  // test running in parallel can grab a port we picked: then pick again.
  let port, metricsPort, env, old;
  for (let attempt = 0; !old; attempt++) {
    [port, metricsPort] = [await freePort(), await freePort()];
    env = {
      XB_PORT: String(port),
      XB_WEB_PORT: String(await freePort()),
      XB_METRICS_ADDR: `127.0.0.1:${metricsPort}`,
      XB_QUEUE_WRITE_DELAY_MS: '60000', // queued stanzas stay in memory only: the drain must save them
      XB_DRAIN_SPREAD_S: '4',
    };
    old = await startRust({ dir, env }).catch((err) => {
      if (attempt >= 2) throw err;
    });
  }
  (await login(port, keys, 'carol', true)).close(); // offline from now on
  const alice = await login(port, keys, 'alice', true);
  for (let i = 0; i < 100; i++) await alice.sendMessage('carol', `m${i}`);
  const online = [];
  for (let i = 0; i < 20; i++) online.push(await login(port, keys, name(i), true));
  const closes = online.map((c) => new Promise((r) => c.once('close', (reason, retry) => r({ reason, retry }))));

  // The new version, started before the old one goes.
  const next = startRust({ dir, env: { ...env, XB_TAKEOVER: '1' } });
  await new Promise((r) => setTimeout(r, 500));
  assert.equal((await fetch(`http://127.0.0.1:${metricsPort}/ready`)).status, 200);

  const t0 = performance.now();
  const res = await fetch(`http://127.0.0.1:${metricsPort}/drain`, { method: 'POST' });
  assert.equal(res.status, 200);
  const told = await Promise.all(closes);
  for (const t of told) {
    assert.equal(t.reason, 'restart');
    assert.ok(t.retry >= 1 && t.retry <= 4, `retry ${t.retry} within the spread`);
  }
  assert.ok(new Set(told.map((t) => t.retry)).size >= 2, 'spread out, not all the same');

  const rs = await next;
  const gap = performance.now() - t0;
  console.log(`new process serving ${gap.toFixed(0)} ms after the drain began`);
  assert.ok(old.log.some((l) => /drained in/.test(l)), old.log.join('\n'));

  // Everything queued in the old process's memory is in the new one.
  assert.equal(await rs.store.pendingCount('carol'), 100);
  const carol = await login(port, keys, 'carol');
  const got = [];
  carol.on('message', (m) => got.push(m.text));
  for (let i = 0; i < 100 && got.length < 100; i++) await new Promise((r) => setTimeout(r, 50));
  assert.deepEqual(got, Array.from({ length: 100 }, (_, i) => `m${i}`), 'all of them, in order');
  // And the clients come back.
  const back = await login(port, keys, name(0));
  back.close();
  carol.close();
  alice.close();
  for (const c of online) c.close();
  await rs.close();
  await fs.rm(dir, { recursive: true, force: true });
});
