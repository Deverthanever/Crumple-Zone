/* Crumple Zone: static file server + party relay, with no dependencies.
   Serves index.html and relays each driver's state to everyone else in the same party code.
   Run: node server.js   (the PORT environment variable is respected). */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 8080, MAX_ROOM = 12, MAX_MSG = 16384, GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const page = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); res.end('ok'); return; }
  if (req.url === '/' || req.url.startsWith('/?') || req.url === '/index.html') {
    fs.readFile(page, (e, b) => { if (e) { res.writeHead(500); res.end('index.html missing'); return; } res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' }); res.end(b); });
    return;
  }
  res.writeHead(404); res.end('not found');
});
/* ---- minimal WebSocket (RFC 6455) ---- */
function frame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) h = Buffer.from([0x80 | op, len]); else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); } else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([h, payload]);
}
function sendText(c, s) { if (!c.dead && c.socket.writable) c.socket.write(frame(1, Buffer.from(s))); }
function parse(c, onText) {
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 15, masked = (b[1] & 0x80) !== 0; let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; } else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MSG || !masked) { kill(c); return; }
    if (b.length < off + 4 + len) return;
    const mask = b.slice(off, off + 4), data = Buffer.from(b.slice(off + 4, off + 4 + len)); for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    c.buf = b.slice(off + 4 + len);
    if (op === 8) { kill(c); return; }
    if (op === 9) { if (!c.dead) c.socket.write(frame(10, data)); continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op === 1 || op === 0) { c.part = c.part ? Buffer.concat([c.part, data]) : data; if (fin) { const t = c.part.toString(); c.part = null; onText(c, t); } }
  }
}
const rooms = new Map();   /* name -> { peers: Map(id -> conn), dirty } */
let nextId = 1;
function bcast(name) {
  const r = rooms.get(name); if (!r) return; r.dirty = false;
  const peers = []; r.peers.forEach((c, id) => peers.push({ peer: id, presence: c.presence }));
  const msg = JSON.stringify({ t: 'peers', peers }); r.peers.forEach(c => sendText(c, msg));
}
function mark(name) { const r = rooms.get(name); if (r && !r.dirty) { r.dirty = true; setTimeout(() => bcast(name), 50); } }
function kill(c) {
  if (c.dead) return; c.dead = true; try { c.socket.destroy(); } catch (e) {}
  if (c.room) { const r = rooms.get(c.room); if (r) { r.peers.delete(c.id); if (!r.peers.size) rooms.delete(c.room); else mark(c.room); } }
}
function onText(c, txt) {
  let m; try { m = JSON.parse(txt); } catch (e) { return; }
  if (m.t === 'join' && !c.room) {
    const name = String(m.room || '').toLowerCase().replace(/[^a-z0-9-]/g, '').slice(0, 40); if (!name) return;
    let r = rooms.get(name); if (!r) { r = { peers: new Map(), dirty: false }; rooms.set(name, r); }
    if (r.peers.size >= MAX_ROOM) { kill(c); return; }
    c.room = name; c.id = 'p' + (nextId++); c.presence = {}; r.peers.set(c.id, c); sendText(c, JSON.stringify({ t: 'hello', id: c.id })); mark(name);
  } else if (m.t === 'p' && c.room && m.d && typeof m.d === 'object') { Object.assign(c.presence, m.d); mark(c.room); }
}
const conns = new Set();
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key']; if (!key) { socket.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, buf: Buffer.alloc(0), part: null, room: null, id: null, presence: null, alive: true, dead: false }; conns.add(c);
  socket.on('data', d => { c.buf = Buffer.concat([c.buf, d]); parse(c, onText); });
  socket.on('close', () => { kill(c); conns.delete(c); }); socket.on('error', () => { kill(c); conns.delete(c); });
});
setInterval(() => conns.forEach(c => { if (!c.alive) { kill(c); conns.delete(c); return; } c.alive = false; if (!c.dead) c.socket.write(frame(9, Buffer.alloc(0))); }), 20000);
server.listen(PORT, () => console.log('Crumple Zone on port ' + PORT));
