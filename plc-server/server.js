"use strict";

require("dotenv").config();

const http = require("http");
const { Server } = require("socket.io");
const { startPlcPoller } = require("./plc/plcPoller");

const port = Number(process.env.PLC_SERVER_PORT || 3030);
const host = (process.env.PLC_SERVER_HOST || "0.0.0.0").trim();
const corsOrigin =
  process.env.PLC_SERVER_CORS_ORIGIN === "*"
    ? "*"
    : (process.env.PLC_SERVER_CORS_ORIGIN || true);

const httpServer = http.createServer((_, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Mills 1–4 PLC Socket.IO bridge — use a Socket.IO client.\n");
});

const io = new Server(httpServer, {
  cors: {
    origin: corsOrigin,
    methods: ["GET", "POST"],
  },
});

startPlcPoller(io);

httpServer.listen(port, host, () => {
  // eslint-disable-next-line no-console
  console.log(`plc-server listening on http://${host}:${port} (Socket.IO + Mills 1–4 S7)`);
});
