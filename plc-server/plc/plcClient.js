"use strict";

const nodes7 = require("nodes7");

/** DB251 INT reads + PO end coil read. M22.7 written only from poller (ack). */
const MILL3_VARIABLES = {
  ndtCount: "DB251,INT6",
  okCount: "DB251,INT2",
  nokCount: "DB251,INT4",
  poId: "DB251,INT8",
  slitId: "DB251,INT10",
  poEndSignal: "M20.6",
};

const ITEM_KEYS = Object.keys(MILL3_VARIABLES);

function initiateConnectionPromise(conn, options) {
  return new Promise((resolve, reject) => {
    try {
      conn.initiateConnection(options, (err) => {
        if (typeof err !== "undefined") reject(err);
        else resolve();
      });
    } catch (e) {
      reject(e);
    }
  });
}

function dropConnectionPromise(conn) {
  return new Promise((resolve) => {
    try {
      conn.dropConnection(() => resolve());
    } catch {
      resolve();
    }
  });
}

function readAllItemsPromise(conn) {
  return new Promise((resolve, reject) => {
    try {
      conn.readAllItems((anythingBad, values) => {
        if (anythingBad) reject(new Error("PLC read returned bad quality"));
        else resolve(values || {});
      });
    } catch (e) {
      reject(e);
    }
  });
}

/** Single-item write (e.g. M22.7). Callback: anythingBad is boolean. */
function writeItemsPromise(conn, item, value) {
  return new Promise((resolve, reject) => {
    try {
      conn.writeItems(item, value, (anythingBad) => {
        if (anythingBad) reject(new Error("PLC write reported bad quality"));
        else resolve();
      });
    } catch (e) {
      reject(e);
    }
  });
}

/**
 * Try rack/slot in order (e.g. slot 2 then 1 for S7-300).
 * @returns {{ conn: import('nodes7'), slot: number }}
 */
async function connectWithSlotFallback(host, port, rack, slots) {
  let lastErr;
  for (const slot of slots) {
    const conn = new nodes7();
    try {
      await initiateConnectionPromise(conn, {
        port,
        host,
        rack,
        slot,
        timeout: 8000,
        debug: false,
      });
      conn.setTranslationCB((tag) => MILL3_VARIABLES[tag] || tag);
      conn.addItems(ITEM_KEYS);
      return { conn, slot };
    } catch (err) {
      lastErr = err;
      try {
        await dropConnectionPromise(conn);
      } catch {
        /* ignore */
      }
    }
  }
  throw lastErr || new Error("PLC connection failed");
}

module.exports = {
  MILL3_VARIABLES,
  ITEM_KEYS,
  connectWithSlotFallback,
  readAllItemsPromise,
  writeItemsPromise,
  dropConnectionPromise,
};
