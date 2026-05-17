import http from "node:http";
import net from "node:net";
import { WebSocketServer } from "ws";

const PORT = process.env.PORT || 8080;
const UPSTREAM_HOST = process.env.UPSTREAM_HOST;
const UPSTREAM_PORT = parseInt(process.env.UPSTREAM_PORT || "22", 10);
const AUTH_TOKEN = process.env.AUTH_TOKEN || "";
const WS_PATH = process.env.WS_PATH || "/r-change-me";

if (!UPSTREAM_HOST) {
  throw new Error("UPSTREAM_HOST is required");
}

const server = http.createServer((req, res) => {
  if (req.url === "/healthz") {
    res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
    res.end("ok\n");
    return;
  }

  res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  res.end("not found\n");
});

const wss = new WebSocketServer({
  noServer: true,
  perMessageDeflate: false,
  maxPayload: 256 * 1024
});

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://relay.local");

  if (url.pathname !== WS_PATH) {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }

  const token = url.searchParams.get("t");
  if (!AUTH_TOKEN || token !== AUTH_TOKEN) {
    socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws);
  });
});

wss.on("connection", (ws) => {
  const upstream = net.connect({
    host: UPSTREAM_HOST,
    port: UPSTREAM_PORT,
    noDelay: true,
    keepAlive: true
  });

  let closed = false;

  const closeBoth = () => {
    if (closed) return;
    closed = true;
    try { ws.close(); } catch {}
    try { upstream.destroy(); } catch {}
  };

  // Cloud Run max request timeout is 60 minutes; close before platform timeout.
  const sessionTtl = setTimeout(closeBoth, 55 * 60 * 1000);

  upstream.on("data", (chunk) => {
    if (ws.readyState === ws.OPEN) {
      ws.send(chunk, { binary: true });
    }
  });

  upstream.on("error", closeBoth);
  upstream.on("close", closeBoth);

  ws.on("message", (msg) => {
    if (Buffer.isBuffer(msg)) {
      upstream.write(msg);
      return;
    }

    if (msg instanceof ArrayBuffer) {
      upstream.write(Buffer.from(msg));
      return;
    }

    if (Array.isArray(msg)) {
      upstream.write(Buffer.concat(msg));
      return;
    }

    upstream.write(Buffer.from(String(msg), "utf8"));
  });

  ws.on("close", closeBoth);
  ws.on("error", closeBoth);

  ws.once("close", () => clearTimeout(sessionTtl));
});

server.listen(PORT, () => {
  console.log(`relay listening on :${PORT}`);
});
