// /metrics, /health and /ready on the monitoring port.

import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';

test('metrics count what happens; health and readiness answer', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-metrics-'));
  const addr = '127.0.0.1:49591';
  const rs = await startRust({ dir, env: { XB_METRICS_ADDR: addr } });
  const get = async (p) => {
    const res = await fetch(`http://${addr}${p}`);
    return { status: res.status, text: await res.text() };
  };
  const value = (text, name) => Number(text.match(new RegExp(`^${name} (\\S+)$`, 'm'))?.[1]);

  assert.deepEqual(await get('/health'), { status: 200, text: 'ok' });
  assert.deepEqual(await get('/ready'), { status: 200, text: 'ready' });

  const c = new Client({ port: rs.port });
  c.on('error', () => {});
  await c.connect();
  await c.register('meter', 'password1');
  await new Promise((r) => setTimeout(r, 200));
  const m = (await get('/metrics')).text;
  assert.match(m, /# TYPE xb_connections_open gauge/);
  assert.equal(value(m, 'xb_connections_open'), 1);
  assert.equal(value(m, 'xb_users_online'), 1);
  assert.equal(value(m, 'xb_users_registered'), 1);
  assert.ok(value(m, 'xb_stanzas_in_total') >= 2);
  assert.ok(value(m, 'xb_saves_total') >= 1, 'the registration was saved');
  assert.equal(value(m, 'xb_panics_total'), 0);
  assert.match(m, /xb_build_info\{version="[^"]+"\} 1/);

  c.close();
  await rs.close();
  await fs.rm(dir, { recursive: true, force: true });
});
