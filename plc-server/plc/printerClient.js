"use strict";

const net = require("net");

/**
 * Raw TCP send (e.g. ZPL on port 9100). Optional local bind (before connect), similar to NetworkPrinterSender.
 */
function sendTcpWithOptionalBind(host, port, data, bindAddress, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(new Error(`TCP timeout ${timeoutMs}ms to ${host}:${port}`));
    }, timeoutMs);

    const settleOk = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    };

    const settleErr = (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(err);
    };

    socket.once("error", settleErr);

    socket.once("close", () => {
      settleOk();
    });

    const connectAndWrite = () => {
      socket.connect(port, host, () => {
        socket.write(data, (writeErr) => {
          if (writeErr) {
            socket.destroy();
            settleErr(writeErr);
            return;
          }
          socket.end();
        });
      });
    };

    if (bindAddress && bindAddress.trim()) {
      socket.bind({ address: bindAddress.trim(), port: 0 }, (bindErr) => {
        if (bindErr) {
          socket.destroy();
          settleErr(bindErr);
          return;
        }
        connectAndWrite();
      });
    } else {
      connectAndWrite();
    }
  });
}

module.exports = { sendTcpWithOptionalBind };
