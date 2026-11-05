/**
 * PC Live Screen Share Relay Server (Render Cloud + Local LAN Ready)
 * -------------------------------------------------------------------
 * Zero-dependency Node.js server (WebSocket RFC-6455 + HTTP POST + MJPEG + Mobile Browser Player).
 * Designed to deploy directly on Render.com (or Railway / VPS / Local LAN).
 *
 * Supported PC Push Endpoints (Target URLs for the Agent Card):
 *   1. WebSocket Push : wss://<your-app>.onrender.com/ws/push?room=pc-live
 *   2. HTTP POST Push : https://<your-app>.onrender.com/api/push?room=pc-live
 *
 * Supported Phone Browser & React Native Viewer Endpoints:
 *   1. Mobile Browser Live Player : https://<your-app>.onrender.com/watch/pc-live  (or /)
 *   2. React Native WebSocket URL : wss://<your-app>.onrender.com/ws/watch?room=pc-live
 *   3. MJPEG Live Stream URL      : https://<your-app>.onrender.com/stream/pc-live
 *   4. Latest Frame Snapshot JSON : https://<your-app>.onrender.com/api/frame/pc-live
 *   5. Health & Telemetry Status  : https://<your-app>.onrender.com/api/status
 */

const http = require("http");
const crypto = require("crypto");
const os = require("os");
const { URL } = require("url");

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function getLocalLanIPs() {
  const nets = os.networkInterfaces();
  const ips = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name] || []) {
      if (net.family === "IPv4" && !net.internal) {
        ips.push({ interface: name, address: net.address });
      }
    }
  }
  return ips;
}

/**
 * Encodes a string or Buffer into an RFC-6455 WebSocket frame.
 */
function encodeWsFrame(data, opcode = 0x01) {
  const payload = Buffer.isBuffer(data) ? data : Buffer.from(String(data), "utf8");
  const len = payload.length;
  let header;

  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }

  return Buffer.concat([header, payload]);
}

/**
 * Stateful parser for incoming RFC-6455 WebSocket frames on a socket.
 */
function attachWsFrameParser(socket, onMessage, onClose) {
  let buffer = Buffer.alloc(0);

  socket.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);

    while (buffer.length >= 2) {
      const firstByte = buffer[0];
      const secondByte = buffer[1];
      const opcode = firstByte & 0x0f;
      const isMasked = (secondByte & 0x80) !== 0;
      let payloadLen = secondByte & 0x7f;
      let offset = 2;

      if (payloadLen === 126) {
        if (buffer.length < 4) return;
        payloadLen = buffer.readUInt16BE(2);
        offset = 4;
      } else if (payloadLen === 127) {
        if (buffer.length < 10) return;
        payloadLen = Number(buffer.readBigUInt64BE(2));
        offset = 10;
      }

      const maskLen = isMasked ? 4 : 0;
      const totalFrameLen = offset + maskLen + payloadLen;
      if (buffer.length < totalFrameLen) return;

      let payload = buffer.subarray(offset + maskLen, totalFrameLen);
      if (isMasked) {
        const mask = buffer.subarray(offset, offset + 4);
        const unmasked = Buffer.allocUnsafe(payloadLen);
        for (let i = 0; i < payloadLen; i++) {
          unmasked[i] = payload[i] ^ mask[i & 3];
        }
        payload = unmasked;
      }

      buffer = buffer.subarray(totalFrameLen);

      if (opcode === 0x08) {
        try {
          socket.write(encodeWsFrame(Buffer.alloc(0), 0x08));
          socket.end();
        } catch {}
        if (onClose) onClose();
        return;
      } else if (opcode === 0x09) {
        // Ping -> Pong
        try {
          socket.write(encodeWsFrame(payload, 0x0a));
        } catch {}
      } else if (opcode === 0x0a) {
        // Pong received (keepalive)
      } else if (opcode === 0x01 || opcode === 0x02) {
        onMessage(payload, opcode);
      }
    }
  });

  socket.on("close", () => {
    if (onClose) onClose();
  });

  socket.on("error", () => {
    if (onClose) onClose();
  });
}

function createScreenRelayServer(options = {}) {
  const port = Number(options.port || process.env.PORT || process.env.RELAY_PORT || 8989);
  const authSecret = options.secret || process.env.RELAY_SECRET || "";

  const rooms = new Map();

  function getOrCreateRoom(roomId = "pc-live") {
    const cleanId = String(roomId || "pc-live").trim() || "pc-live";
    if (!rooms.has(cleanId)) {
      rooms.set(cleanId, {
        roomId: cleanId,
        lastFrameDataUrl: null,
        lastFrameBuffer: null,
        width: 0,
        height: 0,
        fps: 10,
        seq: 0,
        sourceName: "PC Screen",
        updatedAt: 0,
        startedAt: Date.now(),
        totalFrames: 0,
        bytesReceived: 0,
        publisherSocket: null,
        wsViewers: new Set(),
        mjpegResponses: new Set(),
      });
    }
    return rooms.get(cleanId);
  }

  function checkAuth(reqUrl, headers) {
    if (!authSecret) return true;
    const tokenFromQuery = reqUrl.searchParams.get("token") || reqUrl.searchParams.get("key") || "";
    const authHeader = headers["authorization"] || "";
    const streamKeyHeader = headers["x-stream-key"] || "";
    const bearer = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : authHeader.trim();
    return tokenFromQuery === authSecret || bearer === authSecret || streamKeyHeader === authSecret;
  }

  function broadcastFrameToRoom(room, framePayload) {
    const {
      frameData,
      width = 1280,
      height = 720,
      fps = 10,
      sourceName = "PC Screen",
      timestamp = Date.now(),
    } = framePayload;

    if (!frameData || typeof frameData !== "string") return false;

    const base64Clean = frameData.includes(",") ? frameData.split(",")[1] : frameData;
    const dataUrl = frameData.startsWith("data:") ? frameData : `data:image/jpeg;base64,${base64Clean}`;
    let rawJpegBuffer = null;
    try {
      rawJpegBuffer = Buffer.from(base64Clean, "base64");
    } catch {
      return false;
    }

    room.lastFrameDataUrl = dataUrl;
    room.lastFrameBuffer = rawJpegBuffer;
    room.width = Number(width) || room.width || 1280;
    room.height = Number(height) || room.height || 720;
    room.fps = Number(fps) || room.fps || 10;
    room.sourceName = sourceName || room.sourceName;
    room.updatedAt = Number(timestamp) || Date.now();
    room.seq += 1;
    room.totalFrames += 1;
    room.bytesReceived += rawJpegBuffer.length;

    // 1. Broadcast to WebSocket Watchers (Phone Browser / React Native App)
    if (room.wsViewers.size > 0) {
      const wsPacket = encodeWsFrame(
        JSON.stringify({
          type: "frame",
          roomId: room.roomId,
          frameData: dataUrl,
          width: room.width,
          height: room.height,
          fps: room.fps,
          sourceName: room.sourceName,
          timestamp: room.updatedAt,
          seq: room.seq,
          viewerCount: room.wsViewers.size + room.mjpegResponses.size,
        })
      );

      for (const viewer of room.wsViewers) {
        if (viewer.destroyed || !viewer.writable) {
          room.wsViewers.delete(viewer);
          continue;
        }
        try {
          viewer.write(wsPacket);
        } catch {
          room.wsViewers.delete(viewer);
        }
      }
    }

    // 2. Broadcast to MJPEG HTTP Stream Watchers
    if (room.mjpegResponses.size > 0 && rawJpegBuffer) {
      const mjpegHeader = Buffer.from(
        `--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${rawJpegBuffer.length}\r\n\r\n`,
        "utf8"
      );
      const mjpegTail = Buffer.from("\r\n", "utf8");
      for (const res of room.mjpegResponses) {
        if (res.destroyed || !res.writable) {
          room.mjpegResponses.delete(res);
          continue;
        }
        try {
          res.write(mjpegHeader);
          res.write(rawJpegBuffer);
          res.write(mjpegTail);
        } catch {
          room.mjpegResponses.delete(res);
        }
      }
    }

    return true;
  }

  // Keep-alive Ping every 25s so Render / Cloudflare load balancers never drop WebSocket connections
  const heartbeatInterval = setInterval(() => {
    const pingFrame = encodeWsFrame(Buffer.from("ping"), 0x09);
    for (const room of rooms.values()) {
      if (room.publisherSocket && !room.publisherSocket.destroyed && room.publisherSocket.writable) {
        try {
          room.publisherSocket.write(pingFrame);
        } catch {}
      }
      for (const viewer of room.wsViewers) {
        if (viewer.destroyed || !viewer.writable) {
          room.wsViewers.delete(viewer);
          continue;
        }
        try {
          viewer.write(pingFrame);
        } catch {
          room.wsViewers.delete(viewer);
        }
      }
    }
  }, 25000);

  function renderViewerHtml(roomId) {
    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, user-scalable=yes" />
  <title>Live PC Screen • ${roomId}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #07090e;
      color: #f8fafc;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      height: 100dvh;
      width: 100vw;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 8px;
      padding: 10px 14px;
      background: #111520;
      border-bottom: 1px solid rgba(255,255,255,0.09);
      flex-shrink: 0;
      z-index: 10;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 8px;
      font-weight: 800;
      font-size: 13px;
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: #ef4444;
      box-shadow: 0 0 10px #ef4444;
      animation: pulse 1.2s infinite;
      flex-shrink: 0;
    }
    .dot.offline {
      background: #64748b;
      box-shadow: none;
      animation: none;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.88); }
    }
    .stats {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 11px;
    }
    .badge {
      background: rgba(255,255,255,0.07);
      border: 1px solid rgba(255,255,255,0.12);
      padding: 4px 8px;
      border-radius: 6px;
      font-family: monospace;
      color: #e2e8f0;
      white-space: nowrap;
    }
    .btn {
      cursor: pointer;
      background: #0d9488;
      color: #fff;
      border: none;
      padding: 5px 10px;
      border-radius: 6px;
      font-weight: 700;
      font-size: 11px;
    }
    .stage {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      position: relative;
      background: #07090e;
      overflow: hidden;
    }
    #screenImg {
      width: 100%;
      height: 100%;
      object-fit: contain;
      display: none;
    }
    #screenImg.cover-mode {
      object-fit: cover;
    }
    .waiting {
      text-align: center;
      color: #94a3b8;
      padding: 24px;
      max-width: 420px;
    }
    .waiting h2 {
      color: #f1f5f9;
      font-size: 17px;
      margin-bottom: 8px;
    }
    .waiting p {
      font-size: 13px;
      line-height: 1.5;
      margin-bottom: 12px;
    }
    .url-box {
      background: #111520;
      border: 1px solid rgba(255,255,255,0.12);
      padding: 8px 10px;
      border-radius: 8px;
      font-family: monospace;
      font-size: 11px;
      color: #5eead4;
      word-break: break-all;
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div id="statusDot" class="dot offline"></div>
      <span id="statusText">WAITING FOR PC FEED (${roomId})</span>
    </div>
    <div class="stats">
      <span id="resBadge" class="badge">--×--</span>
      <span id="fpsBadge" class="badge">0 FPS</span>
      <button class="btn" onclick="toggleFit()" id="fitBtn">Fit</button>
      <button class="btn" onclick="toggleFull()">Fullscreen</button>
    </div>
  </header>
  <div class="stage" id="stage">
    <div class="waiting" id="waitingBox">
      <h2>📡 Ready for Live PC Screen</h2>
      <p>Room: <strong>${roomId}</strong><br/>Paste this Push URL in your PC Screen Share Agent Card:</p>
      <div class="url-box" id="pushUrlHint"></div>
    </div>
    <img id="screenImg" alt="Live PC Stream" />
  </div>
  <script>
    const roomId = ${JSON.stringify(roomId)};
    const img = document.getElementById("screenImg");
    const waitingBox = document.getElementById("waitingBox");
    const statusDot = document.getElementById("statusDot");
    const statusText = document.getElementById("statusText");
    const resBadge = document.getElementById("resBadge");
    const fpsBadge = document.getElementById("fpsBadge");
    const fitBtn = document.getElementById("fitBtn");
    const pushUrlHint = document.getElementById("pushUrlHint");

    const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
    pushUrlHint.textContent = wsProto + "//" + location.host + "/ws/push?room=" + encodeURIComponent(roomId);

    let frameCount = 0;
    let lastTick = performance.now();
    let lastFrameTime = 0;
    let lastSeq = -1;
    let wsConnected = false;

    setInterval(() => {
      const now = performance.now();
      const elapsed = (now - lastTick) / 1000;
      if (elapsed > 0) {
        fpsBadge.textContent = Math.round(frameCount / elapsed) + " FPS";
        frameCount = 0;
        lastTick = now;
      }
    }, 1000);

    function applyFrame(data) {
      if (!data || !data.frameData) return;
      if (data.seq && data.seq === lastSeq) return;
      lastSeq = data.seq || Date.now();
      lastFrameTime = Date.now();
      img.src = data.frameData;
      img.style.display = "block";
      waitingBox.style.display = "none";
      statusDot.classList.remove("offline");
      statusText.textContent = "LIVE • " + (data.sourceName || roomId);
      if (data.width && data.height) {
        resBadge.textContent = data.width + "×" + data.height;
      }
      frameCount++;
    }

    function connectWs() {
      const wsUrl = wsProto + "//" + location.host + "/ws/watch?room=" + encodeURIComponent(roomId) + (location.search ? "&" + location.search.slice(1) : "");
      let ws;
      try {
        ws = new WebSocket(wsUrl);
      } catch (e) {
        return;
      }

      ws.onopen = () => {
        wsConnected = true;
      };

      ws.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data);
          if (data.type === "frame") {
            applyFrame(data);
          } else if (data.type === "stream_stopped") {
            statusDot.classList.add("offline");
            statusText.textContent = "PAUSED (" + roomId + ")";
          }
        } catch (e) {}
      };

      ws.onclose = () => {
        wsConnected = false;
        statusDot.classList.add("offline");
        setTimeout(connectWs, 1500);
      };
    }

    // Automatic HTTP snapshot fallback if WebSocket frames haven't arrived in 2s
    setInterval(async () => {
      if (wsConnected && Date.now() - lastFrameTime < 2000) return;
      try {
        const res = await fetch("/api/frame/" + encodeURIComponent(roomId) + (location.search || ""), { cache: "no-store" });
        if (res.ok) {
          const data = await res.json();
          if (data && data.success && data.frameData) {
            applyFrame(data);
          }
        }
      } catch (e) {}
    }, 600);

    function toggleFit() {
      img.classList.toggle("cover-mode");
      fitBtn.textContent = img.classList.contains("cover-mode") ? "Cover" : "Fit";
    }

    function toggleFull() {
      const stage = document.getElementById("stage");
      if (!document.fullscreenElement) stage.requestFullscreen().catch(() => {});
      else document.exitFullscreen().catch(() => {});
    }

    connectWs();
  </script>
</body>
</html>`;
  }

  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, x-stream-key");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    const reqUrl = new URL(req.url || "/", `http://${req.headers.host || `localhost:${port}`}`);
    const pathname = reqUrl.pathname;

    // Render Health Check Endpoint
    if (req.method === "GET" && (pathname === "/health" || pathname === "/healthz")) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "ok", uptime: process.uptime() }));
      return;
    }

    if (!checkAuth(reqUrl, req.headers)) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "Unauthorized: Invalid stream token" }));
      return;
    }

    // 1. Server Status & Active Rooms Telemetry
    if (req.method === "GET" && pathname === "/api/status") {
      const activeRooms = [];
      for (const [id, r] of rooms.entries()) {
        const isLive = Date.now() - r.updatedAt < 5000;
        activeRooms.push({
          roomId: id,
          isLive,
          width: r.width,
          height: r.height,
          fps: r.fps,
          seq: r.seq,
          sourceName: r.sourceName,
          updatedAt: r.updatedAt,
          totalFrames: r.totalFrames,
          bytesReceived: r.bytesReceived,
          viewerCount: r.wsViewers.size + r.mjpegResponses.size,
        });
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          success: true,
          port,
          lanIPs: getLocalLanIPs(),
          rooms: activeRooms,
        })
      );
      return;
    }

    // 2. HTTP POST Frame Ingest (accepts POST on /, /api/push, /api/frame, /push/:roomId)
    if (
      req.method === "POST" &&
      (pathname === "/" ||
        pathname === "/api/push" ||
        pathname === "/api/frame" ||
        pathname.startsWith("/push/"))
    ) {
      const pathRoom = pathname.startsWith("/push/") ? decodeURIComponent(pathname.slice(6)) : null;
      let body = "";
      req.on("data", (chunk) => {
        body += chunk;
        if (body.length > 25 * 1024 * 1024) {
          req.destroy();
        }
      });
      req.on("end", () => {
        try {
          const payload = JSON.parse(body || "{}");
          const roomId = payload.roomId || pathRoom || reqUrl.searchParams.get("room") || "pc-live";
          const room = getOrCreateRoom(roomId);
          const ok = broadcastFrameToRoom(room, payload);
          res.writeHead(ok ? 200 : 400, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              success: ok,
              roomId: room.roomId,
              seq: room.seq,
              viewerCount: room.wsViewers.size + room.mjpegResponses.size,
            })
          );
        } catch (err) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: err.message }));
        }
      });
      return;
    }

    // 3. MJPEG Live Stream Endpoint (/stream or /stream/:roomId)
    if (req.method === "GET" && (pathname === "/stream" || pathname.startsWith("/stream/"))) {
      const roomId =
        pathname.startsWith("/stream/") && pathname.length > 8
          ? decodeURIComponent(pathname.slice(8))
          : reqUrl.searchParams.get("room") || "pc-live";
      const room = getOrCreateRoom(roomId);

      res.writeHead(200, {
        "Content-Type": "multipart/x-mixed-replace; boundary=frame",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        Pragma: "no-cache",
        Connection: "keep-alive",
      });

      room.mjpegResponses.add(res);

      if (room.lastFrameBuffer) {
        try {
          res.write(
            `--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${room.lastFrameBuffer.length}\r\n\r\n`
          );
          res.write(room.lastFrameBuffer);
          res.write("\r\n");
        } catch {}
      }

      req.on("close", () => {
        room.mjpegResponses.delete(res);
      });
      return;
    }

    // 4. Latest Frame Snapshot (/api/frame or /api/frame/:roomId)
    if (req.method === "GET" && (pathname === "/api/frame" || pathname.startsWith("/api/frame/"))) {
      const roomId =
        pathname.startsWith("/api/frame/") && pathname.length > 11
          ? decodeURIComponent(pathname.slice(11))
          : reqUrl.searchParams.get("room") || "pc-live";
      const room = getOrCreateRoom(roomId);

      if (!room.lastFrameDataUrl) {
        res.writeHead(404, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "No frame available yet for room: " + roomId }));
        return;
      }

      if (reqUrl.searchParams.get("raw") === "1" && room.lastFrameBuffer) {
        res.writeHead(200, {
          "Content-Type": "image/jpeg",
          "Content-Length": room.lastFrameBuffer.length,
          "Cache-Control": "no-cache",
        });
        res.end(room.lastFrameBuffer);
        return;
      }

      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-cache" });
      res.end(
        JSON.stringify({
          success: true,
          roomId: room.roomId,
          frameData: room.lastFrameDataUrl,
          width: room.width,
          height: room.height,
          fps: room.fps,
          seq: room.seq,
          sourceName: room.sourceName,
          timestamp: room.updatedAt,
          viewerCount: room.wsViewers.size + room.mjpegResponses.size,
        })
      );
      return;
    }

    // 5. Built-in Mobile/Desktop Live Web Player (/ or /watch or /watch/:roomId)
    if (req.method === "GET" && (pathname === "/" || pathname === "/watch" || pathname.startsWith("/watch/"))) {
      const roomId =
        pathname.startsWith("/watch/") && pathname.length > 7
          ? decodeURIComponent(pathname.slice(7))
          : reqUrl.searchParams.get("room") || "pc-live";
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end(renderViewerHtml(roomId));
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ success: false, error: "Endpoint not found" }));
  });

  // WebSocket Upgrade Handler (/ws/push, /push, /ws/watch, /watch, or /)
  server.on("upgrade", (req, socket) => {
    const reqUrl = new URL(req.url || "/", `http://${req.headers.host || `localhost:${port}`}`);
    const pathname = reqUrl.pathname;

    if (!checkAuth(reqUrl, req.headers)) {
      socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
      socket.destroy();
      return;
    }

    const key = req.headers["sec-websocket-key"];
    if (!key) {
      socket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
      socket.destroy();
      return;
    }

    const acceptKey = crypto
      .createHash("sha1")
      .update(key + WS_GUID)
      .digest("base64");

    socket.write(
      [
        "HTTP/1.1 101 Switching Protocols",
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Accept: ${acceptKey}`,
        "\r\n",
      ].join("\r\n")
    );

    const roomId = reqUrl.searchParams.get("room") || "pc-live";
    const roleParam = reqUrl.searchParams.get("role") || "";
    const room = getOrCreateRoom(roomId);

    // Role A: PC Publisher (/ws/push or /push or ?role=push)
    if (pathname === "/ws/push" || pathname === "/push" || roleParam === "push") {
      room.publisherSocket = socket;

      attachWsFrameParser(
        socket,
        (payload, opcode) => {
          try {
            if (opcode === 0x01) {
              const parsed = JSON.parse(payload.toString("utf8"));
              if (parsed.type === "frame" || parsed.frameData) {
                broadcastFrameToRoom(room, parsed);
              }
            } else if (opcode === 0x02) {
              const b64 = payload.toString("base64");
              broadcastFrameToRoom(room, {
                frameData: `data:image/jpeg;base64,${b64}`,
                timestamp: Date.now(),
              });
            }
          } catch {}
        },
        () => {
          if (room.publisherSocket === socket) {
            room.publisherSocket = null;
            const stopMsg = encodeWsFrame(
              JSON.stringify({ type: "stream_stopped", roomId: room.roomId, timestamp: Date.now() })
            );
            for (const viewer of room.wsViewers) {
              try {
                viewer.write(stopMsg);
              } catch {}
            }
          }
        }
      );
      return;
    }

    // Role B: Phone Browser / React Native Viewer (/ws/watch or /)
    room.wsViewers.add(socket);

    try {
      socket.write(
        encodeWsFrame(
          JSON.stringify({
            type: "welcome",
            roomId: room.roomId,
            hasActiveStream: Date.now() - room.updatedAt < 5000,
            viewerCount: room.wsViewers.size + room.mjpegResponses.size,
          })
        )
      );

      if (room.lastFrameDataUrl) {
        socket.write(
          encodeWsFrame(
            JSON.stringify({
              type: "frame",
              roomId: room.roomId,
              frameData: room.lastFrameDataUrl,
              width: room.width,
              height: room.height,
              fps: room.fps,
              sourceName: room.sourceName,
              timestamp: room.updatedAt,
              seq: room.seq,
              viewerCount: room.wsViewers.size + room.mjpegResponses.size,
            })
          )
        );
      }
    } catch {}

    attachWsFrameParser(
      socket,
      () => {},
      () => {
        room.wsViewers.delete(socket);
      }
    );
  });

  return {
    server,
    port,
    rooms,
    getLocalLanIPs,
    start: () =>
      new Promise((resolve, reject) => {
        server.once("error", (err) => reject(err));
        server.listen(port, "0.0.0.0", () => {
          resolve({
            port,
            lanIPs: getLocalLanIPs(),
          });
        });
      }),
    stop: () =>
      new Promise((resolve) => {
        clearInterval(heartbeatInterval);
        for (const room of rooms.values()) {
          if (room.publisherSocket) {
            try {
              room.publisherSocket.destroy();
            } catch {}
          }
          for (const v of room.wsViewers) {
            try {
              v.destroy();
            } catch {}
          }
          for (const r of room.mjpegResponses) {
            try {
              r.end();
            } catch {}
          }
        }
        server.close(() => resolve(true));
      }),
    getStatus: () => {
      const activeRooms = [];
      for (const [id, r] of rooms.entries()) {
        activeRooms.push({
          roomId: id,
          isLive: Date.now() - r.updatedAt < 5000,
          width: r.width,
          height: r.height,
          fps: r.fps,
          seq: r.seq,
          sourceName: r.sourceName,
          updatedAt: r.updatedAt,
          totalFrames: r.totalFrames,
          bytesReceived: r.bytesReceived,
          viewerCount: r.wsViewers.size + r.mjpegResponses.size,
        });
      }
      return {
        running: server.listening,
        port,
        lanIPs: getLocalLanIPs(),
        rooms: activeRooms,
      };
    },
  };
}

if (require.main === module) {
  const cliPort = Number(process.env.PORT || process.argv[2] || process.env.RELAY_PORT || 8989);
  const instance = createScreenRelayServer({ port: cliPort });
  instance
    .start()
    .then(({ port, lanIPs }) => {
      console.log("==============================================================");
      console.log(`📡 PC Live Screen Share Relay Server running on port ${port}`);
      console.log("==============================================================");
      const primaryIp = lanIPs[0]?.address || "localhost";
      console.log(`👉 PC Agent Push URL (WebSocket) : ws://${primaryIp}:${port}/ws/push?room=pc-live`);
      console.log(`👉 PC Agent Push URL (HTTP POST) : http://${primaryIp}:${port}/api/push`);
      console.log(`📱 Phone Browser Live Player     : http://${primaryIp}:${port}/watch/pc-live`);
      console.log(`📱 React Native Watch WebSocket  : ws://${primaryIp}:${port}/ws/watch?room=pc-live`);
      console.log(`📱 React Native MJPEG Stream URL : http://${primaryIp}:${port}/stream/pc-live`);
      console.log("==============================================================");
    })
    .catch((err) => {
      console.error("Failed to start Screen Relay Server:", err.message);
      process.exit(1);
    });
}

module.exports = { createScreenRelayServer, getLocalLanIPs };
