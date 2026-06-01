"use strict";

const nodes7 = require("nodes7");

/** DB251 INT reads shared by Mill 1–4 (L1L2_INTEGER_DB). PO-end coil is per-mill. */
function buildMillVariables(poEndSignalTag) {
  return {
    ndtCount: "DB251,INT6",
    okCount: "DB251,INT2",
    nokCount: "DB251,INT4",
    poId: "DB251,INT8",
    slitId: "DB251,INT10",
    poEndSignal: poEndSignalTag,
  };
}

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

/** Single-item write (e.g. M40.7 ack). Callback: anythingBad is boolean. */
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
 * @param {string} host
 * @param {number} port
 * @param {number} rack
 * @param {number[]} slots
 * @param {Record<string, string>} variableMap
 * @returns {Promise<{ conn: import('nodes7'), slot: number, itemKeys: string[] }>}
 */
async function connectWithSlotFallback(host, port, rack, slots, variableMap) {
  let lastErr;
  const itemKeys = Object.keys(variableMap);
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
      conn.setTranslationCB((tag) => variableMap[tag] || tag);
      conn.addItems(itemKeys);
      return { conn, slot, itemKeys };
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

/** @deprecated Use buildMillVariables — kept for tests */
const MILL3_VARIABLES = buildMillVariables("M20.6");
const ITEM_KEYS = Object.keys(MILL3_VARIABLES);

module.exports = {
  buildMillVariables,
  MILL3_VARIABLES,
  ITEM_KEYS,
  connectWithSlotFallback,
  readAllItemsPromise,
  writeItemsPromise,
  dropConnectionPromise,
};
