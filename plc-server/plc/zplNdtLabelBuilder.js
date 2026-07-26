"use strict";

const DotsPerMm = 8;

function escape(value) {
  if (value == null || value === "") return "";
  return String(value)
    .replace(/\\/g, "\\\\")
    .replace(/\^/g, "\\^")
    .replace(/~/g, "\\~");
}

function resolveLabelSize(widthMm = 100, lengthMm = 100) {
  return {
    widthDots: Math.max(DotsPerMm, widthMm * DotsPerMm),
    lengthDots: Math.max(DotsPerMm, lengthMm * DotsPerMm),
    isSquare: lengthMm >= 100,
  };
}

function buildCompactTagZpl(params, size) {
  const {
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
  } = params;

  const zpl = [];
  zpl.push("^XA");
  zpl.push(`^PW${size.widthDots}^LL${size.lengthDots}^LH0,0`);

  const escapedBatch = escape(ndtBatchNo);
  const escapedPo = escape(poNumber);
  const escapedGrade = escape(pipeGrade);
  const escapedSize = escape(pipeSize);
  const escapedThickness = escape(pipeThickness);
  const escapedLength = escape(pipeLength);
  const escapedWeight = escape(pipeWeightPerMeter);
  const escapedType = escape(pipeType);
  const escapedStation = escape(stationText);

  const left = 60;
  const fieldWidth = size.widthDots - left * 2;

  let y = 12;
  zpl.push(`^FO${left},${y}^BY2^BCN,55,Y,N,N^FD${escapedBatch}^FS`);
  y += 72;

  zpl.push("^CF0,22");
  zpl.push(
    `^FO${left},${y}^FB${fieldWidth},1,0,C,0^FDMill- ${millNo}  PO: ${escapedPo}  Bund: ${escapedBatch}^FS`
  );
  y += 24;

  const gradePart = escapedGrade === "" ? "Gr- -" : `Gr- ${escapedGrade}`;
  zpl.push(
    `^FO${left},${y}^FB${fieldWidth},1,0,C,0^FD${gradePart}  Size: ${
      escapedSize === "" ? "-" : escapedSize
    }  Thk: ${escapedThickness === "" ? "-" : escapedThickness}^FS`
  );
  y += 24;
  zpl.push(
    `^FO${left},${y}^FB${fieldWidth},1,0,C,0^FDLen: ${
      escapedLength === "" ? "-" : escapedLength
    }  Wt: ${escapedWeight === "" ? "-" : escapedWeight}^FS`
  );
  y += 24;

  if (escapedStation) {
    zpl.push(`^FO${left},${y}^FB${fieldWidth},1,0,C,0^FDStation: ${escapedStation}^FS`);
    y += 24;
  }

  const d = date instanceof Date ? date : new Date();
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yy = String(d.getFullYear()).slice(-2);
  const dateText = `${dd}/${mm}/${yy}`;
  const typeText = escapedType ? `  ${escapedType}` : "";
  const reprintText = isReprint ? "  Reprint" : "";
  zpl.push(
    `^FO${left},${y}^FB${fieldWidth},1,0,C,0^FDDate: ${dateText}  Pcs. ${pcsInBundle}${typeText}${reprintText}^FS`
  );

  const bottomY = size.lengthDots - 78;
  zpl.push(`^FO${left},${bottomY}^BY2^BCN,55,Y,N,N^FD${escapedBatch}^FS`);
  zpl.push("^XZ");
  return Buffer.from(zpl.join(""), "utf8");
}

function buildSquareTagZpl(params, size) {
  const {
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
  } = params;

  const zpl = [];
  zpl.push("^XA");
  zpl.push(`^PW${size.widthDots}^LL${size.lengthDots}^LH0,0`);
  zpl.push("^CF0,34");

  const escapedBatch = escape(ndtBatchNo);
  const escapedPo = escape(poNumber);
  const escapedGrade = escape(pipeGrade);
  const escapedSize = escape(pipeSize);
  const escapedThickness = escape(pipeThickness);
  const escapedLength = escape(pipeLength);
  const escapedWeight = escape(pipeWeightPerMeter);
  const escapedType = escape(pipeType);
  const escapedStation = escape(stationText);

  const left = 60;
  const fieldWidth = size.widthDots - left * 2;

  let y = 36;
  zpl.push(`^FO${left},${y}^BY3^BCN,110,Y,N,N^FD${escapedBatch}^FS`);
  y += 148;

  zpl.push("^CF0,30");
  zpl.push(
    `^FO${left},${y}^FB${fieldWidth},1,0,C,0^FDMill- ${millNo}  PO: ${escapedPo}  Bund: ${escapedBatch}^FS`
  );
  y += 38;

  const gradePart = escapedGrade === "" ? "Gr- -" : `Gr- ${escapedGrade}`;
  zpl.push("^CF0,28");
  zpl.push(
    `^FO${left},${y}^FB${fieldWidth},1,0,C,0^FD${gradePart}  Size: ${
      escapedSize === "" ? "-" : escapedSize
    }  Thk: ${escapedThickness === "" ? "-" : escapedThickness}^FS`
  );
  y += 36;
  zpl.push(
    `^FO${left},${y}^FB${fieldWidth},1,0,C,0^FDLen: ${
      escapedLength === "" ? "-" : escapedLength
    }  Wt: ${escapedWeight === "" ? "-" : escapedWeight}^FS`
  );
  y += 36;

  if (escapedStation) {
    zpl.push(`^FO${left},${y}^FB${fieldWidth},1,0,C,0^FDStation: ${escapedStation}^FS`);
    y += 36;
  }

  const d = date instanceof Date ? date : new Date();
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yy = String(d.getFullYear()).slice(-2);
  const dateText = `${dd}/${mm}/${yy}`;
  const typeText = escapedType ? `  ${escapedType}` : "";
  const reprintText = isReprint ? "  Reprint" : "";
  zpl.push("^CF0,32");
  zpl.push(
    `^FO${left},${y}^FB${fieldWidth},1,0,C,0^FDDate: ${dateText}  Pcs. ${pcsInBundle}${typeText}${reprintText}^FS`
  );

  const bottomY1 = size.lengthDots - 250;
  const bottomY2 = bottomY1 + 118;
  zpl.push(`^FO${left},${bottomY1}^BY2^BCN,88,Y,N,N^FD${escapedBatch}^FS`);
  zpl.push(`^FO${left},${bottomY2}^BY2^BCN,88,Y,N,N^FD${escapedBatch}^FS`);
  zpl.push("^XZ");
  return Buffer.from(zpl.join(""), "utf8");
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
  labelWidthMm = 100,
  labelLengthMm = 100,
}) {
  const size = resolveLabelSize(labelWidthMm, labelLengthMm);
  const params = {
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
  };

  return size.isSquare ? buildSquareTagZpl(params, size) : buildCompactTagZpl(params, size);
}

module.exports = { buildNdtTagZpl, resolveLabelSize, DotsPerMm };
