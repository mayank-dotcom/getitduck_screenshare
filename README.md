# PC Live Screen Share Relay Server (Render.com Ready)

Zero-dependency Node.js backend server that receives your PC's live screen feed from the **Live Screen Share Agent Card** and broadcasts it in real time to your **Phone Browser** and **React Native App**.

## How to Deploy on Render.com

### Option A: Deploy this `server` folder as a separate GitHub Repo
1. Push all files inside this `server/` folder (`package.json`, `index.js`, `screenRelayServer.js`, `render.yaml`) to a new GitHub repository.
2. On [Render Dashboard](https://dashboard.render.com), click **New +** → **Web Service**.
3. Connect your GitHub repository:
   - **Runtime:** `Node`
   - **Build Command:** `npm install`
   - **Start Command:** `npm start`
4. Click **Deploy Web Service**.

### Option B: Deploy from your main repo using Root Directory
1. On Render, select your main repository and set **Root Directory** to `server`.
2. **Build Command:** `npm install`
3. **Start Command:** `npm start`

---

## URLs After Deploying on Render

Suppose your Render URL is `https://my-screen-relay.onrender.com`:

1. **PC Agent Card — Target Push URL (WebSocket):**
   ```
   wss://my-screen-relay.onrender.com/ws/push?room=pc-live
   ```
   *(Or HTTP POST URL: `https://my-screen-relay.onrender.com/api/push?room=pc-live`)*

2. **Phone Browser Live Viewer URL:**
   ```
   https://my-screen-relay.onrender.com/watch/pc-live
   ```

3. **React Native App WebSocket URL:**
   ```
   wss://my-screen-relay.onrender.com/ws/watch?room=pc-live
   ```

