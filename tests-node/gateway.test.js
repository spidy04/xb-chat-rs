// Connection gateways (relay.rs, xb-gateway): two gateways in front of one
// core. Phones on different gateways talk; the core restarts, and the
// gateways tell their phones to come back later, then carry them to the
// new core.

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { promises as fs } from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';
import { MemoryKeyStore } from '../../xb-chat/clients/node/signal-node.js';

const here = import.meta.dirname;
const exe = process.platform === 'win32' ? 'xb-gateway.exe' : 'xb-gateway';
const GATEWAY = process.env.XB_RS_BIN ? path.join(path.dirname(process.env.XB_RS_BIN), exe) : path.resolve(here, '..', 'target-dev', 'debug', exe);

const freePort = () =>
  new Promise((resolve) => {
    const s = net.createServer().listen(0, '127.0.0.1', () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });

function gateway(core, metrics = 'none') {
  const child = spawn(GATEWAY, [], {
    env: { ...process.env, XB_CORE: core, XB_GATEWAY_SECRET: 'gw', XB_HOST: '127.0.0.1', XB_PORT: '0', XB_WEB_PORT: '0', XB_DRAIN_SPREAD_S: '2', XB_METRICS_ADDR: metrics },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const log = [];
  const ready = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`gateway did not start: ${log.join('')}`)), 15000);
    let port;
    const on = (d) => {
      log.push(d);
      const m = d.match(/chat on [^:]+:(\d+)/);
      if (m) port = Number(m[1]);
      if (port && /connected to the core/.test(log.join(''))) {
        clearTimeout(timer);
        resolve(port);
      }
    };
    child.stdout.setEncoding('utf8').on('data', on);
    child.stderr.setEncoding('utf8').on('data', on);
  });
  return { child, log, ready };
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

const next = (c, ev) => new Promise((r) => c.once(ev, r));

test('two gateways, one core: talk across them, survive a core restart', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-gw-'));
  const gwAddr = `127.0.0.1:${await freePort()}`;
  const env = { XB_GATEWAY_ADDR: gwAddr, XB_GATEWAY_SECRET: 'gw' };
  let core = await startRust({ dir, env });
  const g1 = gateway(gwAddr, '127.0.0.1:0');
  const g2 = gateway(gwAddr);
  const [p1, p2] = [await g1.ready, await g2.ready];

  const ana = await login(p1, 'ana', true);
  const ben = await login(p2, 'ben', true);
  const got = next(ben, 'message');
  await ana.sendMessage('ben', 'across two gateways');
  assert.equal((await got).text, 'across two gateways');
  // Gateway 1's own numbers, on /metrics.
  const metricsUrl = g1.log.join('').match(/metrics on (http:\/\/\S+)\/metrics/)[1];
  const metrics = await (await fetch(`${metricsUrl}/metrics`)).text();
  const value = (name) => Number(metrics.match(new RegExp(`^${name} (\\S+)$`, 'm'))?.[1]);
  assert.equal(value('xb_gateway_connections_open'), 1, 'ana');
  assert.equal(value('xb_gateway_core_connected'), 1);
  assert.ok(value('xb_gateway_stanzas_up_total') > 0);
  assert.ok(value('xb_gateway_stanzas_down_total') > 0);
  assert.equal((await fetch(`${metricsUrl}/ready`)).status, 200);
  const g = await ana.createGroup('Gw', ['ben']);
  const inGroup = next(ben, 'message');
  await ana.sendGroupMessage(g.id, 'group via gateways');
  assert.equal((await inGroup).text, 'group via gateways');

  // The core restarts: the gateways close their phones with "restart".
  const closed = [next(ana, 'close'), next(ben, 'close')];
  await core.close();
  for (const reason of await Promise.all(closed)) assert.equal(reason, 'restart');
  core = await startRust({ dir, env });
  // The gateways find the new core; the phones come back through them.
  await new Promise((r) => setTimeout(r, 1500));
  const ana2 = await login(p1, 'ana');
  const ben2 = await login(p2, 'ben');
  const again = next(ben2, 'message');
  await ana2.sendMessage('ben', 'after the restart');
  assert.equal((await again).text, 'after the restart');

  // Gateway 1 drains: ana is told to come back later, the gateway exits.
  const told = next(ana2, 'close');
  const exited = new Promise((r) => g1.child.once('exit', r));
  assert.equal((await fetch(`${metricsUrl}/drain`, { method: 'POST' })).status, 200);
  assert.equal(await told, 'restart');
  assert.equal(await exited, 0);

  for (const c of [ana, ben, ana2, ben2]) c.close();
  g2.child.kill();
  await core.close();
  await fs.rm(dir, { recursive: true, force: true });
});
