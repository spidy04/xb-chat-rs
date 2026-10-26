// The two servers take turns on ONE data folder, the way you'd move a live
// install from Node to Rust (and back): accounts, device logins, end-to-end
// keys, the server key phones have pinned, and queued messages all carry over.
// The Rust server keeps its data in xb.redb: it imports state.json the first
// time, `xb-server export-json` writes it back for Node, and
// `xb-server import-json` takes Node's changes in again.

import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, test } from 'node:test';
import { rustCommand, startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';
import { MemoryKeyStore } from '../../xb-chat/clients/node/signal-node.js';
import { generateKeyPair } from '../../xb-chat/protocol/noise.js';
import { PhoneAuth } from '../../xb-chat/server/auth/phone.js';
import { ChatServer } from '../../xb-chat/server/server.js';
import { Store } from '../../xb-chat/server/store.js';

let dir;
const serverKey = generateKeyPair();
const open = [];

before(async () => {
  dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-swap-'));
  // Like server/main.js: the key lives in <data>/server_key.
  await fs.writeFile(path.join(dir, 'server_key'), serverKey.private.toString('base64'));
});

after(async () => {
  for (const c of open) c.close();
  await fs.rm(dir, { recursive: true, force: true });
});

async function startNode() {
  const store = new Store({ dir, flushDelayMs: 5 });
  await store.load();
  const chat = new ChatServer({ store, auth: [new PhoneAuth({ exposeCode: true })], staticKeyPair: serverKey });
  await chat.listen(0);
  return { port: chat.port, close: () => chat.close() };
}

const tick = (ms = 80) => new Promise((r) => setTimeout(r, ms));
const next = (em, ev, ms = 4000) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${ev}`)), ms);
    em.once(ev, (v) => {
      clearTimeout(timer);
      resolve(v);
    });
  });

// A phone: its Noise key (login) and end-to-end keys survive "restarts".
const asha = { phone: '919800000901', staticKeyPair: generateKeyPair(), keyStore: new MemoryKeyStore() };
const ravi = { phone: '919800000902', staticKeyPair: generateKeyPair(), keyStore: new MemoryKeyStore() };

async function connect(server, dev) {
  // Pinned: refuses any server that doesn't hold the same key.
  const c = new Client({ port: server.port, serverPublicKey: serverKey.public, staticKeyPair: dev.staticKeyPair, keyStore: dev.keyStore });
  c.on('error', () => {});
  open.push(c);
  await c.connect();
  return c;
}

test('Node -> Rust -> Node on the same data folder', async () => {
  // 1. Node: both register with SMS codes; Ravi writes to Asha, who is offline.
  let server = await startNode();
  for (const d of [asha, ravi]) {
    const c = await connect(server, d);
    const { dev: code } = await c.requestCode(d.phone);
    await c.verifyCode(d.phone, code, { name: d === asha ? 'Asha' : 'Ravi' });
    await tick();
    c.close();
  }
  let r = await connect(server, ravi);
  await r.loginDevice(ravi.phone);
  await r.sendMessage(`${asha.phone}@s.xb`, 'sent while Node ran');
  r.close();
  await tick();
  await server.close();

  // 2. Rust, same folder: Asha logs in with her device key alone and gets it.
  server = await startRust({ dir });
  let a = await connect(server, asha);
  const got = next(a, 'message');
  const ok = await a.loginDevice(asha.phone);
  assert.equal(ok.offline, 1, 'the queued message survived the switch');
  assert.equal((await got).text, 'sent while Node ran');
  await a.sendMessage(`${ravi.phone}@s.xb`, 'answered on Rust');
  a.close();
  await tick();
  await server.close();

  // 3. Back to Node: Ravi gets Asha's answer and her delivered receipt from before.
  rustCommand(dir, 'export-json');
  server = await startNode();
  r = await connect(server, ravi);
  const answer = next(r, 'message');
  await r.loginDevice(ravi.phone);
  assert.equal((await answer).text, 'answered on Rust');
  await tick();
  r.close();
  await tick();
  await server.close();

  // 4. Rust again, taking in what changed under Node: Ravi's queue is empty now.
  rustCommand(dir, 'import-json');
  server = await startRust({ dir });
  r = await connect(server, ravi);
  assert.equal((await r.loginDevice(ravi.phone)).offline, 0, 'what Node delivered is not delivered again');
  r.close();
  await server.close();
});
