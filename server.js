const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e4 });

app.set("trust proxy", true);
app.use(express.static("public"));

const MAX_MESSAGE_LENGTH = 500;
const RATE_LIMIT_COUNT = 5; // messages
const RATE_LIMIT_WINDOW_MS = 4000;
const REPORTS_BEFORE_BAN = 3;
const BAN_DURATION_MS = 24 * 60 * 60 * 1000;

let waiting = null; // the one socket waiting for a partner
const partners = new Map(); // socket.id -> partner socket.id
const reportCounts = new Map(); // ip -> number of reports
const bans = new Map(); // ip -> ban expiry timestamp

function ipOf(socket) {
  const forwarded = socket.handshake.headers["x-forwarded-for"];
  return forwarded ? forwarded.split(",")[0].trim() : socket.handshake.address;
}

function isBanned(ip) {
  const until = bans.get(ip);
  if (!until) return false;
  if (Date.now() > until) {
    bans.delete(ip);
    reportCounts.delete(ip);
    return false;
  }
  return true;
}

function broadcastOnline() {
  io.emit("online", io.engine.clientsCount);
}

function unpair(socket) {
  if (waiting === socket) waiting = null;
  const partnerId = partners.get(socket.id);
  if (partnerId) {
    partners.delete(socket.id);
    partners.delete(partnerId);
    const partner = io.sockets.sockets.get(partnerId);
    if (partner) partner.emit("partner_left");
  }
}

function findMatch(socket) {
  unpair(socket);
  if (waiting && waiting !== socket && waiting.connected) {
    const other = waiting;
    waiting = null;
    partners.set(socket.id, other.id);
    partners.set(other.id, socket.id);
    socket.emit("matched");
    other.emit("matched");
  } else {
    waiting = socket;
    socket.emit("waiting");
  }
}

io.use((socket, next) => {
  if (isBanned(ipOf(socket))) return next(new Error("banned"));
  next();
});

io.on("connection", (socket) => {
  socket.recentMessages = [];
  broadcastOnline();

  socket.on("find", () => findMatch(socket));

  socket.on("message", (text) => {
    if (typeof text !== "string") return;
    const clean = text.trim().slice(0, MAX_MESSAGE_LENGTH);
    if (!clean) return;

    const now = Date.now();
    socket.recentMessages = socket.recentMessages.filter(
      (t) => now - t < RATE_LIMIT_WINDOW_MS
    );
    if (socket.recentMessages.length >= RATE_LIMIT_COUNT) {
      socket.emit("slow_down");
      return;
    }
    socket.recentMessages.push(now);

    const partner = io.sockets.sockets.get(partners.get(socket.id));
    if (partner) partner.emit("message", clean);
  });

  socket.on("typing", (isTyping) => {
    const partner = io.sockets.sockets.get(partners.get(socket.id));
    if (partner) partner.emit("typing", !!isTyping);
  });

  socket.on("report", () => {
    const partner = io.sockets.sockets.get(partners.get(socket.id));
    if (!partner) return;
    const ip = ipOf(partner);
    const count = (reportCounts.get(ip) || 0) + 1;
    reportCounts.set(ip, count);
    if (count >= REPORTS_BEFORE_BAN) {
      bans.set(ip, Date.now() + BAN_DURATION_MS);
      partner.disconnect(true);
    }
    socket.emit("reported");
  });

  socket.on("disconnect", () => {
    unpair(socket);
    broadcastOnline();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Chat running on http://localhost:${PORT}`));
