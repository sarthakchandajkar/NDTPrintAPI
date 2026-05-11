"use strict";

const LABEL_W = 800;
const LABEL_L = 1100;

function escape(value) {
  if (value == null || value === "") return "";
  return String(value)
    .replace(/\\/g, "\\\\")
    .replace(/\^/g, "\\^")
    .replace(/~/g, "\\~");
}

/**
 * Port of ZplNdtLabelBuilder.BuildNdtTagZpl (UTF-8 bytes for Honeywell / raw TCP).
 */
function buildNdtTagZpl({
  ndtBatchNo,
  millNo,
  poNumber,
  pipeGrade,
  pipeSize,
  pipeThickness,
  pipeLength,
  pipeWeightPerMeter,
  pipeType,
  date,
  pcsInBundle,
  isReprint,
  stationText,
}) {
  const zpl = [];
  zpl.push("^XA");
  zpl.push(`^PW${LABEL_W}^LL${LABEL_L}^LH0,0`);
  zpl.push("^CF0,32");

  const escapedBatch = escape(ndtBatchNo);
  const escapedPo = escape(poNumber);
  const escapedGrade = escape(pipeGrade);
  const escapedSize = escape(pipeSize);
  const escapedThickness = escape(pipeThickness);
  const escapedLength = escape(pipeLength);
  const escapedWeight = escape(pipeWeightPerMeter);
  const escapedType = escape(pipeType);
  const escapedStation = escape(stationText);

  let y = 40;
  const lineHeight = 34;

  zpl.push(`^FO80,${y}^BY3^BCN,100,Y,N,N^FD${escapedBatch}^FS`);
  y += 130;

  zpl.push(
    `^FO80,${y}^FB640,1,0,C,0^FDMill- ${millNo}  PO: ${escapedPo}  Bund: ${escapedBatch}^FS`
  );
  y += lineHeight;

  const gradePart = escapedGrade === "" ? "Gr- -" : `Gr- ${escapedGrade}`;
  zpl.push(
    `^FO80,${y}^FB640,1,0,C,0^FD${gradePart}  Size: ${
      escapedSize === "" ? "-" : escapedSize
    }  Len: ${escapedLength === "" ? "-" : escapedLength}  Wt: ${
      escapedWeight === "" ? "-" : escapedWeight
    }^FS`
  );
  y += lineHeight;

  if (escapedStation) {
    zpl.push(`^FO80,${y}^FB640,1,0,C,0^FDStation: ${escapedStation}^FS`);
    y += lineHeight;
  }

  const d = date instanceof Date ? date : new Date();
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yy = String(d.getFullYear()).slice(-2);
  const dateText = `${dd}/${mm}/${yy}`;
  const typeText = escapedType ? `  ${escapedType}` : "";
  const reprintText = isReprint ? "  Reprint" : "";
  zpl.push(
    `^FO80,${y}^FB640,1,0,C,0^FDDate: ${dateText}  Pcs. ${pcsInBundle}${typeText}${reprintText}^FS`
  );

  const bottomY1 = LABEL_L - 280;
  const bottomY2 = bottomY1 + 120;
  zpl.push(`^FO80,${bottomY1}^BY2^BCN,80,Y,N,N^FD${escapedBatch}^FS`);
  zpl.push(`^FO80,${bottomY2}^BY2^BCN,80,Y,N,N^FD${escapedBatch}^FS`);
  zpl.push("^XZ");
  return Buffer.from(zpl.join(""), "utf8");
}

module.exports = { buildNdtTagZpl };
