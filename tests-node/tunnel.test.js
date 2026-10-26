// xb-tunnel: the chat server reached only through a relay and a client
// (both on this machine here; on a VPS the relay is the only public part).
// Chat both ways, a photo over the web port, a client without the secret
// turned away, and the client coming back after a restart.

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { promises as fs } from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';
import { MemoryKeyStore } from '../../xb-chat/clients/node/signal-node.js';

const here = import.meta.dirname;
const exe = process.platform === 'win32' ? 'xb-tunnel.exe' : 'xb-tunnel';
const TUNNEL = process.env.XB_RS_BIN ? path.join(path.dirname(process.env.XB_RS_BIN), exe) : path.resolve(here, '..', 'target-dev', 'debug', exe);
const SECRET = 'tunnel-test-secret-123';

const freePort = () =>
  new Promise((resolve) => {
    const s = net.createServer().listen(0, '127.0.0.1', () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });

// Runs xb-tunnel; resolves once a line matching `ready` is printed.
function tunnel(args, ready, secret = SECRET) {
  const child = spawn(TUNNEL, args, { env: { ...process.env, XB_TUNNEL_SECRET: secret }, stdio: ['ignore', 'pipe', 'pipe'] });
  const log = [];
  const started = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`xb-tunnel ${args[0]}: no "${ready}" in:\n${log.join('')}`)), 15000);
    const on = (d) => {
      log.push(d);
      if (process.env.XB_RS_LOG) process.stdout.write(`[tunnel] ${d}`);
      const m = log.join('').match(ready);
      if (m) {
        clearTimeout(timer);
        resolve(m);
      }
    };
    child.stdout.setEncoding('utf8').on('data', on);
    child.stderr.setEncoding('utf8').on('data', on);
  });
  return { child, log, started };
}

const keys = {};
async function login(port, user, register = false) {
  keys[user] ??= new MemoryKeyStore();
  const c = new Client({ port, keyStore: keys[user] });
  c.on('error', () => {});
  await c.connect();
  await (register ? c.register(user, 'password1') : c.login(user, 'password1'));
  return c;
}

test('chat and photos through xb-tunnel; no secret, no tunnel; the client comes back', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-tunnel-'));
  const rs = await startRust({ dir });
  const [chatPort, webPort] = [await freePort(), await freePort()];
  const relay = tunnel(['relay', '--listen', '127.0.0.1:0', '--public-host', '127.0.0.1'], /waiting for the client on (\S+)/);
  const relayAddr = (await relay.started)[1];
  const maps = ['--map', `${chatPort}=127.0.0.1:${rs.port}`, '--map', `${webPort}=127.0.0.1:${rs.webPort}`];
  let client = tunnel(['client', '--relay', relayAddr, ...maps], /attached to/);
  await client.started;
  await new Promise((r) => setTimeout(r, 200)); // the relay opens the public ports

  // Chat, entirely through the tunnel.
  const alice = await login(chatPort, 'alice', true);
  const bob = await login(chatPort, 'bob', true);
  const got = new Promise((r) => bob.once('message', r));
  await alice.sendMessage('bob', 'through the tunnel');
  assert.equal((await got).text, 'through the tunnel');
  const back = new Promise((r) => alice.once('message', r));
  await bob.sendMessage('alice', 'and back');
  assert.equal((await back).text, 'and back');

  // A photo, fetched over the tunnelled web port (several hundred KB: many chunks).
  const photo = randomBytes(400_000);
  const m = new Promise((r) => bob.once('message', r));
  await alice.sendMedia('bob', photo, { mimetype: 'image/jpeg' });
  const sha = (await m).media.sha256;
  const viaTunnel = Buffer.from(await (await fetch(`http://127.0.0.1:${webPort}/m/${sha}`)).arrayBuffer());
  const direct = Buffer.from(await (await fetch(`${rs.base}/m/${sha}`)).arrayBuffer());
  assert.ok(viaTunnel.length > photo.length && viaTunnel.equals(direct), 'the same encrypted file, byte for byte');

  // A client with the wrong secret can't attach.
  const intruder = tunnel(['client', '--relay', relayAddr, '--map', `${await freePort()}=127.0.0.1:1`], /refused us|attached to/, 'wrong-secret-0123456789');
  assert.match((await intruder.started)[0], /refused us/);
  intruder.child.kill();
  assert.equal(relay.log.join('').match(/client \S+ attached/g).length, 1, 'only the real client attached');
  // And the real client's tunnel still works.
  const still = new Promise((r) => alice.once('message', r));
  await bob.sendMessage('alice', 'still here');
  assert.equal((await still).text, 'still here');

  // The client restarts: the public ports stay, traffic flows again.
  alice.close();
  bob.close();
  client.child.kill();
  await new Promise((r) => setTimeout(r, 300));
  client = tunnel(['client', '--relay', relayAddr, ...maps], /attached to/);
  await client.started;
  const alice2 = await login(chatPort, 'alice');
  const bob2 = await login(chatPort, 'bob');
  const again = new Promise((r) => bob2.once('message', r));
  await alice2.sendMessage('bob', 'after the client restarted');
  assert.equal((await again).text, 'after the client restarted');

  alice2.close();
  bob2.close();
  client.child.kill();
  relay.child.kill();
  await rs.close();
  await fs.rm(dir, { recursive: true, force: true });
});
