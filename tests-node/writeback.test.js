// The offline queue's write-back (store.rs, queue_db.rs):
//   - stanzas delivered within the write delay never reach the disk
//   - undelivered ones are written after it, and deleted once delivered
//   - cold ones leave memory and are read back from disk at login
//   - after a restart the queue is loaded as an index, stanzas from disk

import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, test } from 'node:test';
import { startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';
import { MemoryKeyStore } from '../../xb-chat/clients/node/signal-node.js';

const dirs = [];
after(async () => {
  for (const d of dirs) await fs.rm(d, { recursive: true, force: true });
});
const tick = (ms) => new Promise((r) => setTimeout(r, ms));

async function setup(env) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-wb-'));
  dirs.push(dir);
  const keys = { alice: new MemoryKeyStore(), bob: new MemoryKeyStore() };
  const rs = await startRust({ dir, env });
  const login = async (user, register = false, server = rs) => {
    const c = new Client({ port: server.port, keyStore: keys[user] });
    c.on('error', () => {});
    await c.connect();
    await (register ? c.register(user, 'password1') : c.login(user, 'password1'));
    return c;
  };
  (await login('bob', true)).close();
  (await login('alice', true)).close();
  await tick(200);
  return { rs, dir, login };
}

test('delivered within the delay: never written to disk', async () => {
  const { rs, login } = await setup({ XB_QUEUE_WRITE_DELAY_MS: '1000' });
  const alice = await login('alice');
  const bob = await login('bob');
  const got = [];
  bob.on('message', (m) => got.push(m.text));
  const before = await rs.store.queueStats();
  for (let i = 0; i < 50; i++) await alice.sendMessage('bob', `hi ${i}`);
  for (let i = 0; i < 50 && got.length < 50; i++) await tick(50);
  await tick(2000); // well past the delay
  const s = await rs.store.queueStats();
  assert.equal(got.length, 50);
  assert.equal(s.written - before.written, 0, 'nothing reached the disk');
  // 50 messages + their 50 delivered receipts, all delivered in time.
  assert.ok(s.skipped - before.skipped >= 100, `skipped ${s.skipped - before.skipped}`);
  alice.close();
  bob.close();
  await rs.close();
});

test('undelivered after the delay: written, then deleted once delivered', async () => {
  const { rs, login } = await setup({ XB_QUEUE_WRITE_DELAY_MS: '500' });
  const alice = await login('alice');
  const before = await rs.store.queueStats();
  for (let i = 0; i < 20; i++) await alice.sendMessage('bob', `later ${i}`);
  await tick(1500);
  const mid = await rs.store.queueStats();
  assert.equal(mid.written - before.written, 20, 'bob is offline: all 20 written');
  const bob = await login('bob');
  const got = [];
  bob.on('message', (m) => got.push(m.text));
  for (let i = 0; i < 50 && got.length < 20; i++) await tick(50);
  await tick(1500);
  const end = await rs.store.queueStats();
  assert.equal(got.length, 20);
  assert.ok(end.deleted - before.deleted >= 20, `deleted ${end.deleted - before.deleted}`);
  alice.close();
  bob.close();
  await rs.close();
});

test('cold stanzas leave memory and are read back from disk at login', async () => {
  const { rs, login } = await setup({ XB_QUEUE_WRITE_DELAY_MS: '200', XB_QUEUE_COLD_MS: '1' });
  const alice = await login('alice');
  for (let i = 0; i < 10; i++) await alice.sendMessage('bob', `cold ${i}`);
  await tick(1000); // written
  await rs.store.evictCold();
  const s = await rs.store.queueStats();
  assert.equal(s.queued, 10);
  assert.equal(s.inMemory, 0, 'all on disk only');
  const bob = await login('bob');
  const got = [];
  bob.on('message', (m) => got.push(m.text));
  for (let i = 0; i < 50 && got.length < 10; i++) await tick(50);
  assert.deepEqual(got, Array.from({ length: 10 }, (_, i) => `cold ${i}`), 'read back from disk, in order');
  alice.close();
  bob.close();
  await rs.close();
});

test('after a restart: the queue comes back as an index, stanzas from disk', async () => {
  const { rs, dir, login } = await setup({ XB_QUEUE_WRITE_DELAY_MS: '200' });
  const alice = await login('alice');
  for (let i = 0; i < 10; i++) await alice.sendMessage('bob', `kept ${i}`);
  alice.close();
  await rs.close(); // flushes
  const rs2 = await startRust({ dir, env: { XB_QUEUE_WRITE_DELAY_MS: '200' } });
  const s = await rs2.store.queueStats();
  assert.equal(s.queued, 10);
  assert.equal(s.inMemory, 0, 'loaded as an index; stanzas stay on disk');
  const bob = await login('bob', false, rs2);
  const got = [];
  bob.on('message', (m) => got.push(m.text));
  for (let i = 0; i < 50 && got.length < 10; i++) await tick(50);
  assert.deepEqual(got, Array.from({ length: 10 }, (_, i) => `kept ${i}`));
  bob.close();
  await rs2.close();
});
