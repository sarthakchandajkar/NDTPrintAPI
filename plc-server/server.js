"use strict";

const http = require("http");
const { Server } = require("socket.io");
const { startPlcPoller } = require("./plc/plcPoller");

const port = Number(process.env.PLC_SERVER_PORT || 3030);
const corsOrigin =
  process.env.PLC_SERVER_CORS_ORIGIN === "*"
    ? "*"
    : (process.env.PLC_SERVER_CORS_ORIGIN || true);

const httpServer = http.createServer((_, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Mill-3 PLC Socket.IO bridge — use a Socket.IO client.\n");
});

const io = new Server(httpServer, {
  cors: {
    origin: corsOrigin,
    methods: ["GET", "POST"],
  },
});

startPlcPoller(io);

httpServer.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`plc-server listening on http://localhost:${port} (Socket.IO + Mill-3 S7 read)`);
});
