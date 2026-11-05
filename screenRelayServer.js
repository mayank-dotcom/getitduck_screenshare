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
        cursorX: 0.5,
        cursorY: 0.5,
        controlSeq: 0,
        controlQueue: [],
        publisherSocket: null,
        wsViewers: new Set(),
        mjpegResponses: new Set(),
      });
    }
    return rooms.get(cleanId);
  }

  function enqueueControlCommand(room, rawCmd) {
    if (!rawCmd || typeof rawCmd !== "object") return null;
    const action = String(rawCmd.action || "move").toLowerCase();

    if (action === "joystick_move") {
      const dx = Number(rawCmd.dx) || 0;
      const dy = Number(rawCmd.dy) || 0;
      room.cursorX = Math.max(0, Math.min(1, room.cursorX + dx));
      room.cursorY = Math.max(0, Math.min(1, room.cursorY + dy));
    } else if (typeof rawCmd.x === "number" && typeof rawCmd.y === "number") {
      room.cursorX = Math.max(0, Math.min(1, rawCmd.x));
      room.cursorY = Math.max(0, Math.min(1, rawCmd.y));
    }

    room.controlSeq += 1;
    const cmdObj = {
      id: room.controlSeq,
      type: "control",
      roomId: room.roomId,
      action,
      x: room.cursorX,
      y: room.cursorY,
      dx: Number(rawCmd.dx) || 0,
      dy: Number(rawCmd.dy) || 0,
      button: rawCmd.button || (action === "right_click" ? "right" : "left"),
      wheelDelta: Number(rawCmd.wheelDelta || rawCmd.scrollDelta) || 0,
      text: rawCmd.text !== undefined ? String(rawCmd.text) : "",
      key: rawCmd.key !== undefined ? String(rawCmd.key) : "",
      modifiers: Array.isArray(rawCmd.modifiers) ? rawCmd.modifiers : [],
      timestamp: Date.now(),
    };

    // Coalesce consecutive move/joystick_move commands so the queue stays zero-lag
    const lastIdx = room.controlQueue.length - 1;
    if (
      lastIdx >= 0 &&
      (action === "move" || action === "joystick_move") &&
      (room.controlQueue[lastIdx].action === "move" || room.controlQueue[lastIdx].action === "joystick_move")
    ) {
      room.controlQueue[lastIdx].x = cmdObj.x;
      room.controlQueue[lastIdx].y = cmdObj.y;
      room.controlQueue[lastIdx].dx += cmdObj.dx;
      room.controlQueue[lastIdx].dy += cmdObj.dy;
      room.controlQueue[lastIdx].id = cmdObj.id;
      room.controlQueue[lastIdx].timestamp = cmdObj.timestamp;
    } else {
      room.controlQueue.push(cmdObj);
      if (room.controlQueue.length > 120) {
        room.controlQueue.shift();
      }
    }

    const wsFrame = encodeWsFrame(JSON.stringify(cmdObj));

    // Forward immediately to PC Publisher WebSocket if connected
    if (room.publisherSocket && !room.publisherSocket.destroyed && room.publisherSocket.writable) {
      try {
        room.publisherSocket.write(wsFrame);
      } catch {}
    }

    // Also broadcast cursor/control state to any connected WS listeners (e.g. PC bridge or multi-viewers)
    for (const viewer of room.wsViewers) {
      if (viewer.destroyed || !viewer.writable) continue;
      try {
        viewer.write(wsFrame);
      } catch {}
    }

    return cmdObj;
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
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>Live PC Screen & Controller • ${roomId}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
    body {
      background: #07090e;
      color: #f8fafc;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      height: 100dvh;
      width: 100vw;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      user-select: none;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 6px;
      padding: 8px 12px;
      background: #0f1420;
      border-bottom: 1px solid rgba(255,255,255,0.09);
      flex-shrink: 0;
      z-index: 20;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 8px;
      font-weight: 800;
      font-size: 12px;
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: #10b981;
      box-shadow: 0 0 10px #10b981;
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
      flex-wrap: wrap;
      gap: 5px;
      font-size: 11px;
    }
    .badge {
      background: rgba(255,255,255,0.07);
      border: 1px solid rgba(255,255,255,0.12);
      padding: 4px 7px;
      border-radius: 6px;
      font-family: monospace;
      color: #e2e8f0;
      white-space: nowrap;
      font-size: 10px;
    }
    .btn {
      cursor: pointer;
      background: #1e293b;
      color: #f8fafc;
      border: 1px solid rgba(255,255,255,0.16);
      padding: 5px 9px;
      border-radius: 6px;
      font-weight: 700;
      font-size: 11px;
      transition: all 0.12s ease;
    }
    .btn.active {
      background: #0d9488;
      border-color: #14b8a6;
      color: #fff;
    }
    .btn:active {
      transform: scale(0.95);
    }
    .stage {
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
      position: relative;
      background: #05070b;
      overflow: hidden;
      min-height: 160px;
    }
    #screenWrap {
      position: relative;
      display: flex;
      align-items: center;
      justify-content: center;
      width: 100%;
      height: 100%;
    }
    #screenImg {
      max-width: 100%;
      max-height: 100%;
      width: 100%;
      height: 100%;
      object-fit: contain;
      display: none;
      pointer-events: auto;
    }
    #screenImg.cover-mode {
      object-fit: cover;
    }
    /* Virtual Cursor Pointer Overlay */
    #virtualCursor {
      position: absolute;
      width: 22px;
      height: 22px;
      left: 50%;
      top: 50%;
      transform: translate(-3px, -2px);
      pointer-events: none;
      z-index: 15;
      filter: drop-shadow(0 2px 6px rgba(0,0,0,0.85)) drop-shadow(0 0 8px rgba(16,185,129,0.75));
      transition: left 0.04s linear, top 0.04s linear;
    }
    .cursor-ring {
      position: absolute;
      left: -6px;
      top: -6px;
      width: 18px;
      height: 18px;
      border-radius: 50%;
      border: 2px solid #10b981;
      background: rgba(16, 185, 129, 0.18);
      transition: transform 0.12s ease, background 0.12s ease;
    }
    #virtualCursor.clicking .cursor-ring {
      transform: scale(1.65);
      background: rgba(244, 63, 94, 0.55);
      border-color: #f43f5e;
    }
    .waiting {
      text-align: center;
      color: #94a3b8;
      padding: 20px;
      max-width: 420px;
    }
    .waiting h2 {
      color: #f1f5f9;
      font-size: 16px;
      margin-bottom: 6px;
    }
    .waiting p {
      font-size: 12px;
      line-height: 1.45;
      margin-bottom: 10px;
    }
    .url-box {
      background: #111520;
      border: 1px solid rgba(255,255,255,0.12);
      padding: 7px 10px;
      border-radius: 8px;
      font-family: monospace;
      font-size: 10px;
      color: #5eead4;
      word-break: break-all;
    }

    /* ── CONTROLLER DOCK (JOYSTICK + MOUSE TRIGGERS + KEYBOARD) ── */
    #controllerDock {
      background: linear-gradient(180deg, #0d1322 0%, #090d16 100%);
      border-top: 1px solid rgba(255,255,255,0.12);
      padding: 10px 12px;
      display: flex;
      flex-direction: column;
      gap: 9px;
      flex-shrink: 0;
      z-index: 25;
      box-shadow: 0 -8px 28px rgba(0,0,0,0.6);
    }
    #controllerDock.hidden {
      display: none;
    }
    .gamepad-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    /* Analog Joystick Pad */
    .joystick-col {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    #joystickZone {
      width: 118px;
      height: 118px;
      border-radius: 50%;
      background: radial-gradient(circle at center, #1e293b 0%, #0f172a 70%, #090d16 100%);
      border: 2.5px solid rgba(20, 184, 166, 0.55);
      box-shadow: inset 0 4px 14px rgba(0,0,0,0.75), 0 0 16px rgba(13, 148, 136, 0.2);
      position: relative;
      touch-action: none;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
      cursor: grab;
    }
    #joystickZone::before, #joystickZone::after {
      content: "";
      position: absolute;
      background: rgba(255,255,255,0.06);
      pointer-events: none;
    }
    #joystickZone::before {
      width: 80%;
      height: 1px;
    }
    #joystickZone::after {
      width: 1px;
      height: 80%;
    }
    #joystickKnob {
      width: 48px;
      height: 48px;
      border-radius: 50%;
      background: radial-gradient(circle at 35% 35%, #2dd4bf 0%, #0d9488 65%, #115e59 100%);
      border: 2.5px solid #ccfbf1;
      box-shadow: 0 5px 14px rgba(0,0,0,0.65), 0 0 12px rgba(20, 184, 166, 0.5);
      position: absolute;
      left: 50%;
      top: 50%;
      transform: translate(-50%, -50%);
      pointer-events: none;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 9px;
      font-weight: 900;
      color: #042f2e;
      letter-spacing: 0.5px;
    }
    .speed-col {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .speed-label {
      font-size: 9px;
      font-weight: 800;
      color: #94a3b8;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .speed-pill {
      padding: 4px 8px;
      font-size: 10px;
      font-weight: 800;
      border-radius: 6px;
      border: 1px solid rgba(255,255,255,0.14);
      background: #161f30;
      color: #cbd5e1;
      cursor: pointer;
    }
    .speed-pill.active {
      background: #0d9488;
      color: #fff;
      border-color: #2dd4bf;
    }

    /* Mouse Action Buttons Grid */
    .mouse-pad {
      flex: 1;
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 6px;
      max-width: 360px;
    }
    .m-btn {
      padding: 11px 6px;
      border-radius: 10px;
      border: 1.5px solid rgba(255,255,255,0.15);
      background: #161f33;
      color: #f1f5f9;
      font-weight: 800;
      font-size: 11px;
      cursor: pointer;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 2px;
      box-shadow: 0 3px 8px rgba(0,0,0,0.35);
      touch-action: manipulation;
    }
    .m-btn span.sub {
      font-size: 8.5px;
      font-weight: 600;
      color: #94a3b8;
    }
    .m-btn.primary {
      background: linear-gradient(180deg, #0f766e 0%, #115e59 100%);
      border-color: #2dd4bf;
    }
    .m-btn.right-clk {
      background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
      border-color: #38bdf8;
    }
    .m-btn.hold-active {
      background: #e11d48 !important;
      border-color: #fda4af !important;
      color: #fff !important;
    }
    .m-btn:active {
      transform: scale(0.94);
      filter: brightness(1.2);
    }

    /* Keyboard Bar & Quick Keys */
    .kb-section {
      display: flex;
      flex-direction: column;
      gap: 6px;
      border-top: 1px solid rgba(255,255,255,0.08);
      padding-top: 8px;
    }
    .kb-input-row {
      display: flex;
      gap: 6px;
    }
    #kbTextInput {
      flex: 1;
      background: #111827;
      border: 1.5px solid rgba(255,255,255,0.18);
      border-radius: 8px;
      padding: 8px 11px;
      color: #f8fafc;
      font-size: 13px;
      outline: none;
    }
    #kbTextInput:focus {
      border-color: #14b8a6;
    }
    .kb-send-btn {
      background: #0d9488;
      color: #fff;
      border: 1px solid #2dd4bf;
      border-radius: 8px;
      padding: 0 14px;
      font-weight: 800;
      font-size: 12px;
      cursor: pointer;
      white-space: nowrap;
    }
    .quick-keys {
      display: flex;
      align-items: center;
      gap: 5px;
      overflow-x: auto;
      padding-bottom: 2px;
      scrollbar-width: none;
    }
    .quick-keys::-webkit-scrollbar { display: none; }
    .k-chip {
      flex-shrink: 0;
      padding: 6px 9px;
      border-radius: 7px;
      background: #172033;
      border: 1px solid rgba(255,255,255,0.14);
      color: #e2e8f0;
      font-size: 10.5px;
      font-weight: 700;
      font-family: monospace;
      cursor: pointer;
    }
    .k-chip:active {
      background: #0d9488;
      color: #fff;
      transform: scale(0.93);
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <div id="statusDot" class="dot offline"></div>
      <span id="statusText">WAITING FOR PC (${roomId})</span>
    </div>
    <div class="stats">
      <span id="resBadge" class="badge">--×--</span>
      <span id="fpsBadge" class="badge">0 FPS</span>
      <button class="btn active" onclick="toggleTapMode()" id="tapModeBtn" title="Tap video screen to click">Touch: Tap</button>
      <button class="btn active" onclick="toggleController()" id="ctrlToggleBtn">🎮 Controls</button>
      <button class="btn" onclick="toggleFit()" id="fitBtn">Fit</button>
      <button class="btn" onclick="toggleFull()">Full</button>
    </div>
  </header>

  <div class="stage" id="stage">
    <div class="waiting" id="waitingBox">
      <h2>📡 Ready for Live PC Screen</h2>
      <p>Room: <strong>${roomId}</strong><br/>Click Play (▶) on your PC Screen Share Agent Card:</p>
      <div class="url-box" id="pushUrlHint"></div>
    </div>
    <div id="screenWrap">
      <img id="screenImg" alt="Live PC Stream" />
      <div id="virtualCursor">
        <div class="cursor-ring"></div>
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
          <path d="M4 2L20 11L12 13L9 21L4 2Z" fill="#10b981" stroke="#ffffff" stroke-width="2" stroke-linejoin="round"/>
        </svg>
      </div>
    </div>
  </div>

  <!-- GAMEPAD JOYSTICK + MOUSE + KEYBOARD CONTROLLER DOCK -->
  <div id="controllerDock">
    <div class="gamepad-row">
      <!-- 1. Left Side: 360° Analog Mouse Joystick + Speed Selector -->
      <div class="joystick-col">
        <div id="joystickZone">
          <div id="joystickKnob">MOUSE</div>
        </div>
        <div class="speed-col">
          <span class="speed-label">Speed</span>
          <button class="speed-pill" onclick="setJoySpeed(0.5, this)">0.5x</button>
          <button class="speed-pill active" onclick="setJoySpeed(1.2, this)">1x</button>
          <button class="speed-pill" onclick="setJoySpeed(2.4, this)">2x</button>
          <button class="speed-pill" onclick="setJoySpeed(4.0, this)">3x</button>
        </div>
      </div>

      <!-- 2. Right Side: Tactile Mouse Triggers & Scroll -->
      <div class="mouse-pad">
        <button class="m-btn primary" onclick="sendMouseAction('click', 'left')">
          L-CLICK
          <span class="sub">Primary Tap</span>
        </button>
        <button class="m-btn right-clk" onclick="sendMouseAction('right_click', 'right')">
          R-CLICK
          <span class="sub">Context Menu</span>
        </button>
        <button class="m-btn" onclick="sendMouseAction('double_click', 'left')">
          DBL CLICK
          <span class="sub">Open / Select</span>
        </button>
        <button class="m-btn" id="dragHoldBtn" onclick="toggleDragHold()">
          DRAG HOLD
          <span class="sub" id="dragHoldSub">Release</span>
        </button>
        <button class="m-btn" onclick="sendScroll(120)">
          ▲ SCROLL
          <span class="sub">Wheel Up</span>
        </button>
        <button class="m-btn" onclick="sendScroll(-120)">
          ▼ SCROLL
          <span class="sub">Wheel Down</span>
        </button>
      </div>
    </div>

    <!-- 3. Bottom Row: Remote Keyboard Input + Special Function Keys -->
    <div class="kb-section">
      <div class="kb-input-row">
        <input
          id="kbTextInput"
          type="text"
          placeholder="Type text to send to PC keyboard..."
          autocomplete="off"
          autocorrect="off"
          autocapitalize="off"
        />
        <button class="kb-send-btn" onclick="sendTypedText(false)">Type</button>
        <button class="kb-send-btn" style="background:#2563eb;border-color:#60a5fa;" onclick="sendTypedText(true)">Type + ↵</button>
      </div>
      <div class="quick-keys">
        <button class="k-chip" onclick="sendKeyCombo('Enter', [])">Enter ↵</button>
        <button class="k-chip" onclick="sendKeyCombo('Backspace', [])">Bksp ⌫</button>
        <button class="k-chip" onclick="sendKeyCombo('Space', [])">Space ␣</button>
        <button class="k-chip" onclick="sendKeyCombo('Tab', [])">Tab ⇥</button>
        <button class="k-chip" onclick="sendKeyCombo('Escape', [])">Esc</button>
        <button class="k-chip" onclick="sendKeyCombo('Up', [])">↑</button>
        <button class="k-chip" onclick="sendKeyCombo('Down', [])">↓</button>
        <button class="k-chip" onclick="sendKeyCombo('Left', [])">←</button>
        <button class="k-chip" onclick="sendKeyCombo('Right', [])">→</button>
        <button class="k-chip" onclick="sendKeyCombo('C', ['Ctrl'])">Ctrl+C</button>
        <button class="k-chip" onclick="sendKeyCombo('V', ['Ctrl'])">Ctrl+V</button>
        <button class="k-chip" onclick="sendKeyCombo('Z', ['Ctrl'])">Ctrl+Z</button>
        <button class="k-chip" onclick="sendKeyCombo('A', ['Ctrl'])">Ctrl+A</button>
        <button class="k-chip" onclick="sendKeyCombo('Tab', ['Alt'])">Alt+Tab</button>
        <button class="k-chip" onclick="sendKeyCombo('Delete', [])">Del</button>
      </div>
    </div>
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
    const virtualCursor = document.getElementById("virtualCursor");
    const controllerDock = document.getElementById("controllerDock");
    const ctrlToggleBtn = document.getElementById("ctrlToggleBtn");
    const tapModeBtn = document.getElementById("tapModeBtn");
    const kbTextInput = document.getElementById("kbTextInput");

    const wsProto = location.protocol === "https:" ? "wss:" : "ws:";
    pushUrlHint.textContent = wsProto + "//" + location.host + "/ws/push?room=" + encodeURIComponent(roomId);

    let frameCount = 0;
    let lastTick = performance.now();
    let lastFrameTime = 0;
    let lastSeq = -1;
    let wsConnected = false;
    let activeWs = null;

    // Normalized cursor position (0..1)
    let cursorX = 0.5;
    let cursorY = 0.5;
    let joySpeedMultiplier = 1.2;
    let isDraggingHold = false;
    let directTapEnabled = true;

    setInterval(() => {
      const now = performance.now();
      const elapsed = (now - lastTick) / 1000;
      if (elapsed > 0) {
        fpsBadge.textContent = Math.round(frameCount / elapsed) + " FPS";
        frameCount = 0;
        lastTick = now;
      }
    }, 1000);

    function renderCursorOverlay() {
      const rect = img.getBoundingClientRect();
      const wrapRect = document.getElementById("screenWrap").getBoundingClientRect();
      if (!rect.width || !rect.height) {
        virtualCursor.style.left = (cursorX * 100) + "%";
        virtualCursor.style.top = (cursorY * 100) + "%";
        return;
      }
      // Compute actual rendered image box inside object-fit:contain
      const natW = img.naturalWidth || 1280;
      const natH = img.naturalHeight || 720;
      const imgAspect = natW / natH;
      const boxAspect = rect.width / rect.height;
      let drawW = rect.width;
      let drawH = rect.height;
      let offsetX = rect.left - wrapRect.left;
      let offsetY = rect.top - wrapRect.top;

      if (!img.classList.contains("cover-mode")) {
        if (boxAspect > imgAspect) {
          drawW = rect.height * imgAspect;
          offsetX += (rect.width - drawW) / 2;
        } else {
          drawH = rect.width / imgAspect;
          offsetY += (rect.height - drawH) / 2;
        }
      }

      const px = offsetX + cursorX * drawW;
      const py = offsetY + cursorY * drawH;
      virtualCursor.style.left = px + "px";
      virtualCursor.style.top = py + "px";
    }

    function flashCursorClick() {
      virtualCursor.classList.add("clicking");
      setTimeout(() => virtualCursor.classList.remove("clicking"), 180);
    }

    function sendControlPayload(payload) {
      const fullPayload = Object.assign({ type: "control", roomId }, payload);
      if (activeWs && activeWs.readyState === WebSocket.OPEN) {
        try {
          activeWs.send(JSON.stringify(fullPayload));
          return;
        } catch (e) {}
      }
      fetch("/api/control/" + encodeURIComponent(roomId) + (location.search || ""), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(fullPayload),
      }).catch(() => {});
    }

    // ── ANALOG JOYSTICK ENGINE ──
    const joystickZone = document.getElementById("joystickZone");
    const joystickKnob = document.getElementById("joystickKnob");
    let joyActive = false;
    let joyVecX = 0; // -1..1
    let joyVecY = 0; // -1..1
    const maxRadius = 38;

    function handleJoystickMove(clientX, clientY) {
      const rect = joystickZone.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const centerY = rect.top + rect.height / 2;
      let dx = clientX - centerX;
      let dy = clientY - centerY;
      const dist = Math.hypot(dx, dy);
      if (dist > maxRadius) {
        dx = (dx / dist) * maxRadius;
        dy = (dy / dist) * maxRadius;
      }
      joystickKnob.style.transform = "translate(calc(-50% + " + dx + "px), calc(-50% + " + dy + "px))";
      // Non-linear response curve for fine precision near center + fast sweep at edges
      const normX = dx / maxRadius;
      const normY = dy / maxRadius;
      joyVecX = Math.sign(normX) * Math.pow(Math.abs(normX), 1.35);
      joyVecY = Math.sign(normY) * Math.pow(Math.abs(normY), 1.35);
    }

    function resetJoystick() {
      joyActive = false;
      joyVecX = 0;
      joyVecY = 0;
      joystickKnob.style.transform = "translate(-50%, -50%)";
    }

    joystickZone.addEventListener("touchstart", (e) => {
      e.preventDefault();
      joyActive = true;
      const t = e.touches[0];
      handleJoystickMove(t.clientX, t.clientY);
    }, { passive: false });

    joystickZone.addEventListener("touchmove", (e) => {
      e.preventDefault();
      if (!joyActive) return;
      const t = e.touches[0];
      handleJoystickMove(t.clientX, t.clientY);
    }, { passive: false });

    joystickZone.addEventListener("touchend", (e) => {
      e.preventDefault();
      resetJoystick();
    });

    joystickZone.addEventListener("mousedown", (e) => {
      e.preventDefault();
      joyActive = true;
      handleJoystickMove(e.clientX, e.clientY);
    });

    window.addEventListener("mousemove", (e) => {
      if (!joyActive) return;
      handleJoystickMove(e.clientX, e.clientY);
    });

    window.addEventListener("mouseup", () => {
      if (joyActive) resetJoystick();
    });

    // Continuous 20Hz Joystick -> PC Cursor Updater
    let lastSentCursorX = cursorX;
    let lastSentCursorY = cursorY;
    setInterval(() => {
      if (!joyActive || (Math.abs(joyVecX) < 0.02 && Math.abs(joyVecY) < 0.02)) return;
      const step = 0.014 * joySpeedMultiplier;
      cursorX = Math.max(0, Math.min(1, cursorX + joyVecX * step));
      cursorY = Math.max(0, Math.min(1, cursorY + joyVecY * step));
      renderCursorOverlay();

      if (Math.abs(cursorX - lastSentCursorX) > 0.001 || Math.abs(cursorY - lastSentCursorY) > 0.001) {
        lastSentCursorX = cursorX;
        lastSentCursorY = cursorY;
        sendControlPayload({
          action: "move",
          x: Number(cursorX.toFixed(4)),
          y: Number(cursorY.toFixed(4)),
        });
      }
    }, 50);

    // Direct Tap on Video Screen to Move & Click
    img.addEventListener("click", (e) => {
      if (!directTapEnabled) return;
      const rect = img.getBoundingClientRect();
      const natW = img.naturalWidth || 1280;
      const natH = img.naturalHeight || 720;
      const imgAspect = natW / natH;
      const boxAspect = rect.width / rect.height;
      let drawW = rect.width;
      let drawH = rect.height;
      let offsetX = rect.left;
      let offsetY = rect.top;

      if (!img.classList.contains("cover-mode")) {
        if (boxAspect > imgAspect) {
          drawW = rect.height * imgAspect;
          offsetX += (rect.width - drawW) / 2;
        } else {
          drawH = rect.width / imgAspect;
          offsetY += (rect.height - drawH) / 2;
        }
      }

      const relX = (e.clientX - offsetX) / drawW;
      const relY = (e.clientY - offsetY) / drawH;
      if (relX >= 0 && relX <= 1 && relY >= 0 && relY <= 1) {
        cursorX = relX;
        cursorY = relY;
        renderCursorOverlay();
        flashCursorClick();
        sendControlPayload({
          action: "click",
          button: "left",
          x: Number(cursorX.toFixed(4)),
          y: Number(cursorY.toFixed(4)),
        });
      }
    });

    function setJoySpeed(mult, btnEl) {
      joySpeedMultiplier = mult;
      document.querySelectorAll(".speed-pill").forEach((el) => el.classList.remove("active"));
      if (btnEl) btnEl.classList.add("active");
    }

    function sendMouseAction(action, button) {
      flashCursorClick();
      sendControlPayload({
        action,
        button: button || "left",
        x: Number(cursorX.toFixed(4)),
        y: Number(cursorY.toFixed(4)),
      });
    }

    function toggleDragHold() {
      isDraggingHold = !isDraggingHold;
      const btn = document.getElementById("dragHoldBtn");
      const sub = document.getElementById("dragHoldSub");
      if (isDraggingHold) {
        btn.classList.add("hold-active");
        sub.textContent = "HOLDING...";
        sendMouseAction("down", "left");
      } else {
        btn.classList.remove("hold-active");
        sub.textContent = "Release";
        sendMouseAction("up", "left");
      }
    }

    function sendScroll(wheelDelta) {
      sendControlPayload({
        action: "scroll",
        wheelDelta,
        x: Number(cursorX.toFixed(4)),
        y: Number(cursorY.toFixed(4)),
      });
    }

    function sendTypedText(withEnter) {
      const val = kbTextInput.value;
      if (val) {
        sendControlPayload({
          action: "text",
          text: val,
        });
        kbTextInput.value = "";
      }
      if (withEnter) {
        setTimeout(() => {
          sendKeyCombo("Enter", []);
        }, 60);
      }
    }

    kbTextInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        sendTypedText(true);
      }
    });

    function sendKeyCombo(key, modifiers) {
      sendControlPayload({
        action: "key",
        key,
        modifiers: modifiers || [],
      });
    }

    function toggleController() {
      controllerDock.classList.toggle("hidden");
      ctrlToggleBtn.classList.toggle("active", !controllerDock.classList.contains("hidden"));
      setTimeout(renderCursorOverlay, 60);
    }

    function toggleTapMode() {
      directTapEnabled = !directTapEnabled;
      tapModeBtn.classList.toggle("active", directTapEnabled);
      tapModeBtn.textContent = directTapEnabled ? "Touch: Tap" : "Joystick Only";
    }

    function applyFrame(data) {
      if (!data || !data.frameData) return;
      if (data.seq && data.seq === lastSeq) return;
      lastSeq = data.seq || Date.now();
      lastFrameTime = Date.now();
      img.onload = () => renderCursorOverlay();
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
        activeWs = ws;
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
        activeWs = null;
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
      setTimeout(renderCursorOverlay, 50);
    }

    function toggleFull() {
      if (!document.fullscreenElement) document.body.requestFullscreen().catch(() => {});
      else document.exitFullscreen().catch(() => {});
    }

    window.addEventListener("resize", renderCursorOverlay);
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
          const pendingCommands = room.controlQueue.splice(0, room.controlQueue.length);
          res.writeHead(ok ? 200 : 400, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              success: ok,
              roomId: room.roomId,
              seq: room.seq,
              viewerCount: room.wsViewers.size + room.mjpegResponses.size,
              cursorX: room.cursorX,
              cursorY: room.cursorY,
              commands: pendingCommands,
            })
          );
        } catch (err) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: err.message }));
        }
      });
      return;
    }

    // 2.5. Remote Mouse/Joystick & Keyboard Control Endpoints (/api/control or /api/control/:roomId)
    if (pathname === "/api/control" || pathname.startsWith("/api/control/")) {
      const roomId =
        pathname.startsWith("/api/control/") && pathname.length > 13
          ? decodeURIComponent(pathname.slice(13))
          : reqUrl.searchParams.get("room") || "pc-live";
      const room = getOrCreateRoom(roomId);

      if (req.method === "GET") {
        const pendingCommands = room.controlQueue.splice(0, room.controlQueue.length);
        res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
        res.end(
          JSON.stringify({
            success: true,
            roomId: room.roomId,
            cursorX: room.cursorX,
            cursorY: room.cursorY,
            commands: pendingCommands,
          })
        );
        return;
      }

      if (req.method === "POST") {
        let body = "";
        req.on("data", (chunk) => {
          body += chunk;
          if (body.length > 512 * 1024) req.destroy();
        });
        req.on("end", () => {
          try {
            const payload = JSON.parse(body || "{}");
            const cmdObj = enqueueControlCommand(room, payload);
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(
              JSON.stringify({
                success: true,
                roomId: room.roomId,
                cursorX: room.cursorX,
                cursorY: room.cursorY,
                command: cmdObj,
              })
            );
          } catch (err) {
            res.writeHead(400, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ success: false, error: err.message }));
          }
        });
        return;
      }
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
      (payload, opcode) => {
        if (opcode !== 0x01) return;
        try {
          const parsed = JSON.parse(payload.toString("utf8"));
          if (parsed && (parsed.type === "control" || parsed.action)) {
            enqueueControlCommand(room, parsed);
          }
        } catch {}
      },
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
