// Runs the Rust server for the Node integration tests (copies of
// ../xb-chat/test/*.test.js), so the same clients and the same assertions
// check both servers.
//
//   const rs = await startRust({ dir });        // a fresh data folder
//   new Client({ port: rs.port, serverPublicKey: rs.publicKey })
//   await rs.store.pendingCount('bob')          // what the Node tests read from chat.store
//   await rs.close()
//
// The binary: XB_RS_BIN, else the newest of ../target{,-dev}/debug/xb-server(.exe)
// (build it first: cargo build).

import { spawn, spawnSync } from 'node:child_process';
import { promises as fs, statSync } from 'node:fs';
import path from 'node:path';
import { MediaServer } from '../../xb-chat/server/media.js';

const here = import.meta.dirname;
export const ROOT = path.resolve(here, '../../xb-chat');
const exe = process.platform === 'win32' ? 'xb-server.exe' : 'xb-server';
const BIN =
  process.env.XB_RS_BIN ||
  ['target', 'target-dev']
    .map((t) => path.resolve(here, '..', t, 'debug', exe))
    .map((f) => ({ f, t: statSync(f, { throwIfNoEntry: false })?.mtimeMs ?? -1 }))
    .sort((a, b) => b.t - a.t)[0].f;
const SECRET = 'test-secret';
const MEDIA_BIN = path.join(path.dirname(BIN), process.platform === 'win32' ? 'xb-media.exe' : 'xb-media');
const GATEWAY_BIN = path.join(path.dirname(BIN), process.platform === 'win32' ? 'xb-gateway.exe' : 'xb-gateway');

// A connection gateway (xb-gateway) in front of a core; -> { port, webPort, child }
async function startGateway(core, webUpstream) {
  const child = spawn(GATEWAY_BIN, [], {
    env: { ...process.env, XB_CORE: core, XB_GATEWAY_SECRET: 'gw-secret', XB_HOST: '127.0.0.1', XB_PORT: '0', XB_WEB_PORT: '0', XB_HTTP_UPSTREAM: webUpstream, XB_METRICS_ADDR: 'none' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  return new Promise((resolve, reject) => {
    let buf = '';
    const found = {};
    const timer = setTimeout(() => reject(new Error(`xb-gateway did not start: ${buf}`)), 15000);
    const onData = (d) => {
      buf += d;
      if (process.env.XB_RS_LOG) process.stdout.write(`[gw] ${d}`);
      const m = buf.match(/chat on [^:]+:(\d+), web on [^:]+:(\d+)/);
      if (m) [found.port, found.webPort] = [Number(m[1]), Number(m[2])];
      if (found.port && /connected to the core/.test(buf) && !found.done) {
        found.done = true;
        clearTimeout(timer);
        resolve({ ...found, child });
      }
    };
    child.stdout.setEncoding('utf8').on('data', onData);
    child.stderr.setEncoding('utf8').on('data', onData);
  });
}

// The separate media service (xb-media) on a free port; -> { port, child }
async function startMedia(dir) {
  const child = spawn(MEDIA_BIN, [], {
    env: { ...process.env, XB_DATA: dir, XB_MEDIA_HOST: '127.0.0.1', XB_MEDIA_PORT: '0', XB_SECRET: SECRET, XB_METRICS_ADDR: '127.0.0.1:0' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const port = await new Promise((resolve, reject) => {
    let buf = '';
    const timer = setTimeout(() => reject(new Error(`xb-media did not start: ${buf}`)), 15000);
    const onData = (d) => {
      buf += d;
      const m = buf.match(/xb-media on [^:]+:(\d+)/);
      const metrics = buf.match(/metrics on (http:\/\/\S+)\/metrics/);
      if (m && metrics) {
        clearTimeout(timer);
        resolve({ port: Number(m[1]), metrics: `${metrics[1]}/metrics` });
      }
    };
    child.stdout.setEncoding('utf8').on('data', onData);
    child.stderr.setEncoding('utf8').on('data', onData);
  });
  return { port: port.port, metrics: port.metrics, child };
}

// xb-server export-json / import-json on a data folder (to swap with the Node server).
export function rustCommand(dir, command) {
  const r = spawnSync(BIN, [command], { env: { ...process.env, XB_DATA: dir }, encoding: 'utf8' });
  if (r.status !== 0) throw new Error(`xb-server ${command} failed (${r.status}): ${r.stdout}${r.stderr}`);
  return r.stdout;
}

// splitMedia (or XB_RS_SPLIT_MEDIA=1 for every test): media in its own
// process (xb-media), the chat server pointed at it with XB_MEDIA_URL.
// viaGateway (or XB_RS_VIA_GATEWAY=1 for every test): the clients connect
// to a connection gateway (xb-gateway) in front of the server, not to the
// server itself; admin calls and media still go to the server.
export async function startRust({ dir, staticKeyPair = null, env = {}, appDir = 'none', splitMedia = !!process.env.XB_RS_SPLIT_MEDIA, viaGateway = !!process.env.XB_RS_VIA_GATEWAY } = {}) {
  if (viaGateway) env = { XB_GATEWAY_ADDR: '127.0.0.1:0', XB_GATEWAY_SECRET: 'gw-secret', ...env };
  await fs.mkdir(dir, { recursive: true });
  const mediaService = splitMedia ? await startMedia(dir) : null;
  if (mediaService) env = { XB_MEDIA_URL: `http://127.0.0.1:${mediaService.port}`, ...env };
  if (staticKeyPair) await fs.writeFile(path.join(dir, 'server_key'), Buffer.from(staticKeyPair.private).toString('base64'));
  const child = spawn(BIN, [], {
    env: {
      ...process.env,
      XB_DATA: dir,
      XB_HOST: '127.0.0.1',
      XB_PORT: '0',
      XB_WEB_PORT: '0',
      XB_SECRET: SECRET,
      XB_TEST_ADMIN: '1',
      XB_OTP_ECHO: '1',
      XB_LEGACY_PASSWORD: '1',
      XB_ROOT: ROOT,
      XB_APP_DIR: appDir,
      XB_METRICS_ADDR: 'none',
      ...env,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const log = [];
  const info = await new Promise((resolve, reject) => {
    const found = {};
    let buf = '';
    const timer = setTimeout(() => reject(new Error(`Rust server did not start:\n${log.join('\n')}`)), 15000);
    child.on('exit', (code) => reject(new Error(`Rust server exited (${code}):\n${log.join('\n')}`)));
    const onData = (d) => {
      buf += d;
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i).trimEnd();
        buf = buf.slice(i + 1);
        log.push(line);
        if (process.env.XB_RS_LOG) console.log('[rs]', line);
        let m;
        if ((m = line.match(/chat on [^:]+:(\d+)/))) found.port = Number(m[1]);
        if ((m = line.match(/web gateway on http:\/\/[^:]+:(\d+)/))) found.webPort = Number(m[1]);
        if ((m = line.match(/server key: (\S+)/))) found.key = m[1];
        if ((m = line.match(/gateways connect on (\S+)/))) found.gateways = m[1];
        if (found.port && found.webPort && found.key && !found.done) {
          found.done = true;
          clearTimeout(timer);
          resolve(found);
        }
      }
    };
    child.stdout.setEncoding('utf8').on('data', onData);
    child.stderr.setEncoding('utf8').on('data', onData);
  });
  child.removeAllListeners('exit');
  const base = `http://127.0.0.1:${info.webPort}`;

  const admin = async (fn, ...args) => {
    const res = await fetch(`${base}/__test`, { method: 'POST', body: JSON.stringify({ fn, args: args.map(String) }) });
    return res.json();
  };
  // chat.store, over HTTP: every method is async here.
  const store = new Proxy({}, { get: (_, fn) => (...args) => admin(fn, ...args) });
  // Same secret, same token format: the Node class issues tokens the Rust server accepts.
  const media = new MediaServer({ dir: path.join(dir, 'media'), secret: SECRET, publicUrl: base });

  const gateway = viaGateway ? await startGateway(info.gateways, base) : null;
  const killGateway = async () => {
    if (!gateway || gateway.child.exitCode !== null || gateway.child.signalCode !== null) return;
    const gone = new Promise((r) => gateway.child.once('exit', r));
    gateway.child.kill();
    await gone;
  };
  return {
    port: gateway ? gateway.port : info.port,
    webPort: gateway ? gateway.webPort : info.webPort,
    base,
    wsUrl: `ws://127.0.0.1:${gateway ? gateway.webPort : info.webPort}/ws`,
    corePort: info.port,
    gateways: info.gateways,
    publicKey: Buffer.from(info.key, 'base64'),
    store,
    media,
    log,
    // Like a power cut: no flush, no shutdown.
    async crash() {
      await killGateway();
      const exited = new Promise((r) => child.once('exit', r));
      child.kill('SIGKILL');
      await exited;
      mediaService?.child.kill('SIGKILL');
    },
    async close() {
      await killGateway();
      await admin('flush').catch(() => {});
      const exited = new Promise((r) => child.once('exit', r));
      child.kill();
      await exited;
      if (mediaService && mediaService.child.exitCode === null && mediaService.child.signalCode === null) {
        const gone = new Promise((r) => mediaService.child.once('exit', r));
        mediaService.child.kill();
        await gone;
      }
    },
    mediaPort: mediaService?.port,
    mediaMetrics: mediaService?.metrics,
    // Split media only: the media service dies (chat must not notice).
    async killMedia() {
      const gone = new Promise((r) => mediaService.child.once('exit', r));
      mediaService.child.kill('SIGKILL');
      await gone;
    },
  };
}
