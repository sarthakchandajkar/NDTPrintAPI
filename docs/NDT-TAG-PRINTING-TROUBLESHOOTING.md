# NDT Tag Printing – Troubleshooting

## Flow summary

1. **Dashboard** → "Print dummy bundle" → `POST /api/Test/print-dummy-bundle`
2. **Backend** → `PdfNdtLabelPrinter.PrintLabelAsync` builds a PDF (QuestPDF + ZXing), saves it to `OutputBundleFolder`, then sends the same PDF bytes over TCP to `NdtTagPrinterAddress:NdtTagPrinterPort` (e.g. 192.168.0.125:9100).
3. **Send** → TCP connect, `WriteAsync` (full PDF), `FlushAsync`, short delay, then connection close.

The API now returns **`sentToPrinter`**: `true` if the TCP send completed without error, `false` if send failed or printer is not configured.

---

## Tag still not printing

### 1. Check `sentToPrinter` in the API response

- If **`sentToPrinter: false`**: the app could not send to the printer (connection refused, timeout, or address not set). Check backend logs for `"Printer send failed to ..."` or `"Could not send to printer at ..."`. Fix network/config and retry.
- If **`sentToPrinter: true`** but nothing comes out of the printer: data reached the printer; the issue is likely **printer format or settings** (see below).

### 2. Printer expects PDF vs ZPL

- The service sends **raw PDF** over port 9100 (raw/App Socket).
- Many **label printers** (e.g. Zebra, TSC) on port 9100 expect **ZPL** (Zebra) or **TSPL**, not PDF. They may accept the connection and bytes but not print, or print garbage.
- **What to do:**
  - If your printer supports **PDF** on port 9100: ensure it is set to “PDF” or “Auto” mode for that port; check the printer manual.
  - If it is **ZPL-only** (e.g. Zebra): the current stack does not generate ZPL. Options are (a) use a printer that supports PDF on 9100, or (b) add a ZPL-based label path (different implementation).

### 3. Network and config

- From the PC running the service, test: `Test-NetConnection -ComputerName 192.168.0.125 -Port 9100`. If `TcpTestSucceeded` is False, fix network/firewall or printer port.
- In `appsettings.json` / `appsettings.Development.json`: `NdtTagPrinterAddress` (e.g. `192.168.0.125`), `NdtTagPrinterPort` (e.g. `9100`). Optional: `NdtTagPrinterLocalBindAddress` (e.g. your PC’s IP) if you have multiple NICs.
- Backend logs: look for `"Connecting to printer at ..."` and then either `"Sent NDT label to printer at ..."` or `"Printer send failed to ..."`.

### 4. PDF saved but not sent

- PDFs are always written to **OutputBundleFolder** (when configured) before send. If send fails, you still have the file (e.g. `NDTLabel_0260100999_yyyyMMddHHmmss.pdf`). Open it to confirm the tag layout; the problem is then send or printer format.

---

## Quick checklist

| Check | Action |
|------|--------|
| API returns `sentToPrinter: false` | Check logs, `NdtTagPrinterAddress`/Port, and `Test-NetConnection` to printer:9100. |
| API returns `sentToPrinter: true`, no print | Printer may not support PDF on 9100; check manual or try a PDF-capable printer. |
| Request times out | Increase frontend timeout and/or fix backend/printer connectivity; check for “Printer send failed” in logs. |
