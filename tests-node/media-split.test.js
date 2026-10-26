// Media as its own service (xb-media): files go to it directly or through
// the chat server's old paths, and when it dies, chat carries on.

import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomBytes } from 'node:crypto';
import { test } from 'node:test';
import { startRust } from './rust.js';
import { Client } from '../../xb-chat/clients/node/client.js';
import { MemoryKeyStore } from '../../xb-chat/clients/node/signal-node.js';

test('media on its own service; when it dies, chat carries on', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'xb-split-'));
  const rs = await startRust({ dir, splitMedia: true });
  const keys = { alice: new MemoryKeyStore(), bob: new MemoryKeyStore() };
  const login = async (u, register = false) => {
    const c = new Client({ port: rs.port, keyStore: keys[u] });
    c.on('error', () => {});
    await c.connect();
    await (register ? c.register(u, 'password1') : c.login(u, 'password1'));
    return c;
  };
  const alice = await login('alice', true);
  const bob = await login('bob', true);

  // A photo: uploaded straight to the media service (media_conn points there).
  const photo = randomBytes(100_000);
  const got = new Promise((r) => bob.once('message', r));
  await alice.sendMedia('bob', photo, { mimetype: 'image/jpeg' });
  const m = await got;
  assert.match(m.media.url, new RegExp(`:${rs.mediaPort}/m/`), 'the link is the media service');
  assert.ok((await bob.downloadMedia(m.media)).equals(photo));
  // The old path through the chat server's web port still works (installed apps).
  const viaChat = await fetch(`${rs.base}/m/${m.media.sha256}`);
  assert.equal(viaChat.status, 200);
  // The media service counts it all on /metrics.
  const metrics = await (await fetch(rs.mediaMetrics)).text();
  const value = (name) => Number(metrics.match(new RegExp(`^${name} (\\S+)$`, 'm'))?.[1]);
  assert.equal(value('xb_media_upload_requests_total'), 1);
  assert.ok(value('xb_media_upload_bytes_total') >= photo.length, 'the encrypted file is a bit bigger than the photo');
  assert.ok(value('xb_media_download_requests_total') >= 2);
  assert.equal(value('xb_media_upload_errors_total'), 0);

  // The media service dies.
  await rs.killMedia();
  const text = new Promise((r) => bob.once('message', r));
  await alice.sendMessage('bob', 'still here');
  assert.equal((await text).text, 'still here', 'chat is unaffected');
  await assert.rejects(alice.sendMedia('bob', photo, { mimetype: 'image/jpeg' }), 'uploads fail until it is back');
  assert.equal((await fetch(`${rs.base}/m/${m.media.sha256}`)).status, 502, 'the old path says so');

  alice.close();
  bob.close();
  await rs.close().catch(() => {});
  await fs.rm(dir, { recursive: true, force: true });
});
