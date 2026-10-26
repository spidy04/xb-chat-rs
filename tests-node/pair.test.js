// Replica pairs (replica.rs): two servers, each with its own data folder.
//
//   1. A serves, B follows (a full copy, then every change live).
//   2. A dies hard, with 100 acknowledged stanzas still only in its memory
//      (write-back): B takes over and has all of them.
//   3. A comes back: B is serving, so A follows it (never serves stale data).
//   4. B drains (an upgrade): A takes over at once, with everything.

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

const keys = {};
async function login(port, user, register = false) {
  keys[user] ??= new MemoryKeyStore();
  const c = new Client({ port, keyStore: keys[user] });
  c.on('error', () => {});
  await c.connect();
  await (register ? c.register(user, 'password1') : c.login(user, 'password1'));
  return c;
}

async function receive(port, user, n) {
  const c = await login(port, user);
  const got = [];
  c.on('message', (m) => got.push(m.text));
  for (let i = 0; i < 200 && got.length < n; i++) await new Promise((r) => setTimeout(r, 50));
  c.close();
  return got;
}

const until = async (what, cond, ms = 15000) => {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 50));
  }
};

test('replica pair: failover with nothing lost, rejoin as standby, handover back', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-pair-'));
  const [a, b] = ['a', 'b'];
  const ports = {};
  for (const n of [a, b]) ports[n] = { chat: await freePort(), web: await freePort(), repl: await freePort(), metrics: await freePort() };
  const env = (me, other) => ({
    XB_PORT: String(ports[me].chat),
    XB_WEB_PORT: String(ports[me].web),
    XB_METRICS_ADDR: `127.0.0.1:${ports[me].metrics}`,
    XB_REPL_ADDR: `127.0.0.1:${ports[me].repl}`,
    XB_PEER: `127.0.0.1:${ports[other].repl}`,
    XB_REPL_SECRET: 'pair-secret',
    XB_NODE_ID: me,
    XB_PEER_WAIT_S: '1',
    XB_FAILOVER_S: '1',
    XB_QUEUE_WRITE_DELAY_MS: '60000', // queued stanzas stay in memory: only replication saves them
  });
  const dir = { a: path.join(root, 'a'), b: path.join(root, 'b') };

  // 1. A serves (B isn't there yet); B follows.
  const rsA = await startRust({ dir: dir.a, env: env(a, b) });
  (await login(ports.a.chat, 'carol', true)).close();
  const alice = await login(ports.a.chat, 'alice', true);
  for (let i = 0; i < 50; i++) await alice.sendMessage('carol', `before ${i}`); // copied in the first sync
  const bServes = startRust({ dir: dir.b, env: env(b, a) }); // resolves when B serves
  await until('B to follow A', () => rsA.log.some((l) => /standby connected/.test(l)));
  // A quiet server: the standby must stay connected (it says it's alive),
  // not be dropped as silent and copy everything again every few seconds.
  await new Promise((r) => setTimeout(r, 9000));
  assert.equal(rsA.log.filter((l) => /standby connected/.test(l)).length, 1, 'one copy, then live: no re-copies while idle');
  for (let i = 0; i < 50; i++) await alice.sendMessage('carol', `live ${i}`); // streamed
  await new Promise((r) => setTimeout(r, 300));
  alice.close();

  // 2. A dies: nothing reached its disk (write-back), all of it is on B.
  await rsA.crash();
  const t0 = performance.now();
  const rsB = await bServes;
  console.log(`B serving ${(performance.now() - t0).toFixed(0)} ms after A died`);
  assert.deepEqual(rsB.publicKey, rsA.publicKey, 'same server identity');
  const want = [...Array.from({ length: 50 }, (_, i) => `before ${i}`), ...Array.from({ length: 50 }, (_, i) => `live ${i}`)];
  assert.equal(await rsB.store.pendingCount('carol'), 100);
  assert.deepEqual(await receive(ports.b.chat, 'carol', 100), want, 'all 100, in order');

  // 3. A comes back and follows B.
  const aServes = startRust({ dir: dir.a, env: env(a, b) });
  await until('A to follow B', () => rsB.log.some((l) => /standby connected/.test(l)));
  const bob = await login(ports.b.chat, 'bob', true);
  for (let i = 0; i < 20; i++) await bob.sendMessage('carol', `from b ${i}`);
  bob.close();
  await new Promise((r) => setTimeout(r, 300));

  // 4. B drains: A takes over with everything.
  const t1 = performance.now();
  await fetch(`http://127.0.0.1:${ports.b.metrics}/drain`, { method: 'POST' });
  const rsA2 = await aServes;
  console.log(`A serving ${(performance.now() - t1).toFixed(0)} ms after B began draining`);
  await until('B to exit', () => rsB.log.some((l) => /drained in/.test(l)));
  assert.ok(rsB.log.some((l) => /standby has taken over/.test(l)), rsB.log.join('\n'));
  assert.deepEqual(await receive(ports.a.chat, 'carol', 20), Array.from({ length: 20 }, (_, i) => `from b ${i}`));
  // Accounts made on B are on A: bob logs in there.
  (await login(ports.a.chat, 'bob')).close();

  await rsA2.close();
  await fs.rm(root, { recursive: true, force: true });
});
