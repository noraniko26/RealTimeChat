const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

/* =========================================================
   REGISTRY USER (in-memory, cukup untuk demo)
   ========================================================= */
const users = new Map(); // username → { createdAt }
const USERNAME_RE = /^[a-z0-9_]{3,20}$/;

app.post("/api/register", (req, res) => {
  const username = String(req.body?.username || "")
    .trim()
    .toLowerCase();
  const room = String(req.body?.room || "")
    .trim()
    .toUpperCase();

  if (!USERNAME_RE.test(username)) {
    return res
      .status(400)
      .json({ error: "Username 3–20 karakter (a-z, 0-9, _)" });
  }
  const isNew = !users.has(username);
  if (isNew) users.set(username, { createdAt: Date.now() });

  const url =
    `/u/${username}` + (room ? `?join=${encodeURIComponent(room)}` : "");
  res.json({ ok: true, isNew, url });
});

/* =========================================================
   ⭐ DYNAMIC USER PAGE — dibuat otomatis, SATU template
   ========================================================= */
app.get("/u/:username", (req, res) => {
  const username = req.params.username.toLowerCase();
  if (!users.has(username)) return res.redirect("/?err=user-not-found");
  // SEMUA user dilayani oleh file user.html yang sama
  res.sendFile(path.join(__dirname, "public", "user.html"));
});

/* =========================================================
   ROOMS (WebSocket)
   ========================================================= */
const rooms = new Map();
const MAX_PER_ROOM = 2;
const CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

function uniqueCode() {
  for (let t = 0; t < 50; t++) {
    const bytes = crypto.randomBytes(6);
    let code = "";
    for (let i = 0; i < 6; i++)
      code += CODE_ALPHABET[bytes[i] % CODE_ALPHABET.length];
    if (!rooms.has(code)) return code;
  }
  throw new Error("Gagal generate code");
}

const send = (ws, obj) => {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
};
const broadcast = (room, obj, except) => {
  for (const c of room) if (c !== except) send(c, obj);
};

wss.on("connection", (ws) => {
  ws.roomCode = null;
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === "create") {
      if (ws.roomCode) return;
      const code = uniqueCode();
      rooms.set(code, new Set([ws]));
      ws.roomCode = code;
      send(ws, { type: "room", code, role: "host" });
      console.log(`[+] Room dibuat: ${code}`);
      return;
    }

    if (msg.type === "join") {
      if (ws.roomCode) return;
      const code = String(msg.code || "")
        .trim()
        .toUpperCase();
      const room = rooms.get(code);
      if (!room)
        return send(ws, {
          type: "error",
          message: "Room tidak ditemukan / sudah berakhir.",
        });
      if (room.size >= MAX_PER_ROOM)
        return send(ws, {
          type: "error",
          message: "Room sudah penuh (maks 2 orang).",
        });
      room.add(ws);
      ws.roomCode = code;
      send(ws, { type: "room", code, role: "guest" });
      broadcast(room, { type: "peer-joined" }, ws);
      send(ws, { type: "peer-joined" });
      console.log(`[+] Gabung room: ${code}`);
      return;
    }

    if (msg.type === "chat") {
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      const text = String(msg.text || "").slice(0, 4000);
      if (!text) return;
      broadcast(room, { type: "chat", text, time: Date.now() }, ws);
    }
  });

  ws.on("close", () => {
    const code = ws.roomCode;
    if (!code) return;
    const room = rooms.get(code);
    if (!room) return;
    room.delete(ws);
    broadcast(room, { type: "peer-left" });
    if (room.size === 0) {
      rooms.delete(code);
      console.log(`[-] Room dihapus: ${code}`);
    }
  });
});

// heartbeat
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log(`🚀 Server jalan di port ${PORT}`));
