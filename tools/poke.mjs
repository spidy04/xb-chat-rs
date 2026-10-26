// Connects to the Rust server the way a real xb-chat client does (using the
// Node client from ../xb-chat), so you can watch each lesson's progress.
//
//   node tools/poke.mjs            (server on 127.0.0.1:25222)
//   node tools/poke.mjs 15222      (try it against the Node server too)
import { Client } from '../../xb-chat/clients/node/client.js';

const port = Number(process.argv[2] || 25222);
const c = new Client({ host: '127.0.0.1', port });
c.on('error', () => {});
console.log(`connecting to 127.0.0.1:${port} …`);
const timeout = setTimeout(() => {
  console.log('no handshake reply yet (expected until lesson 3)');
  process.exit(0);
}, 3000);
try {
  await c.connect();
  clearTimeout(timeout);
  console.log('Noise handshake complete: server key', c.serverKey?.toString?.('base64') ?? '(pinned)');
  c.close();
  process.exit(0);
} catch (err) {
  clearTimeout(timeout);
  console.log('connection ended:', err.message);
  process.exit(0);
}
