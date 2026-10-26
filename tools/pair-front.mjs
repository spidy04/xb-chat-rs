// A front for a replica pair: one address for the clients, forwarding each
// connection to whichever node is serving. Only the primary listens on the
// chat and web ports (the standby doesn't), so "the first that accepts" is
// the primary. In production a load balancer does this (HAProxy, or a
// virtual IP with keepalived); this is the same idea in 40 lines, for
// development and tests.
//
//   node tools/pair-front.mjs <listen>=<target>,<target> ...
//   node tools/pair-front.mjs 15222=35222,35232 18080=38080,38090
//
// A target is a port on 127.0.0.1 or host:port. Listens on 0.0.0.0.
import net from 'node:net';

const parse = (t) => (t.includes(':') ? { host: t.split(':')[0], port: Number(t.split(':')[1]) } : { host: '127.0.0.1', port: Number(t) });

for (const arg of process.argv.slice(2)) {
  const [from, list] = arg.split('=');
  const targets = list.split(',').map(parse);
  let last = 0; // the one that worked last time is tried first

  // Connects to the first target that accepts; null if none does.
  const connect = (i = 0) =>
    new Promise((resolve) => {
      if (i >= targets.length) return resolve(null);
      const idx = (last + i) % targets.length;
      const s = net.connect(targets[idx]);
      s.once('connect', () => {
        if (idx !== last) console.log(`${from}: now forwarding to ${targets[idx].host}:${targets[idx].port}`);
        last = idx;
        resolve(s);
      });
      s.once('error', () => connect(i + 1).then(resolve));
    });

  net
    .createServer(async (a) => {
      a.pause();
      a.on('error', () => {});
      const b = await connect();
      if (!b) return a.destroy(); // nobody serving: the client backs off and retries
      a.setNoDelay(true);
      b.setNoDelay(true);
      a.pipe(b).pipe(a);
      a.resume();
      b.on('error', () => a.destroy());
      a.on('close', () => b.destroy());
      b.on('close', () => a.destroy());
    })
    .listen(Number(from), '0.0.0.0', () => console.log(`${from} -> ${targets.map((t) => `${t.host}:${t.port}`).join(' or ')}`));
}
