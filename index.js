/**
 * Render.com Entry Point for PC Live Screen Share Relay Server
 */
const { createScreenRelayServer } = require("./screenRelayServer");

const port = Number(process.env.PORT || process.env.RELAY_PORT || 8989);
const secret = process.env.RELAY_SECRET || "";

const relay = createScreenRelayServer({ port, secret });

relay
  .start()
  .then(({ port, lanIPs }) => {
    const primaryIp = lanIPs[0]?.address || "localhost";
    console.log("==============================================================");
    console.log(`🚀 Screen Share Relay Server listening on 0.0.0.0:${port}`);
    console.log("==============================================================");
    console.log(`👉 PC Agent Push URL (WebSocket) : ws://${primaryIp}:${port}/ws/push?room=pc-live`);
    console.log(`👉 PC Agent Push URL (HTTP POST) : http://${primaryIp}:${port}/api/push`);
    console.log(`📱 Phone Browser Live Player     : http://${primaryIp}:${port}/watch/pc-live`);
    console.log(`📱 React Native Watch WebSocket  : ws://${primaryIp}:${port}/ws/watch?room=pc-live`);
    console.log("==============================================================");
  })
  .catch((err) => {
    console.error("Failed to start Screen Relay Server:", err);
    process.exit(1);
  });

