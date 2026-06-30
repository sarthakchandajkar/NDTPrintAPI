# PO End Rollout Checklists

Operational sign-off checklists for enabling PLC and TCP PO-end sources in production. Complete each section in order before advancing to the next mill or transport.

---

## Checklist A — Phases 1–5 on Mill-2 before Mills 1 and 3 PLC enable

Complete on **Mill-2** first while Mills 1 and 3 remain on File/Telemetry and Mill-4 remains on File.

### Phase 1 — Decoupled ack + queue

- [ ] Rising edge on PO-change trigger is detected in logs with a **CorrelationId**
- [ ] MES ack TRUE is written within **500 ms** of edge detection (not blocked by workflow)
- [ ] PO-end event is enqueued and drained by `PlcPoEndQueueWorker`
- [ ] Workflow completes (bundle close, print, hooter sync as configured)
- [ ] Duplicate edges for the same mill while processing are deduplicated (warning log, no double workflow)

### Phase 2 — Per-mill PoEndSource

- [ ] Mill-2 `PoEndSource=Plc` in appsettings; Mills 1/3 still File or Telemetry-only
- [ ] Settings API `/settings/plc` shows correct `poEndSource` per mill
- [ ] Mill-4 remains `File` (WIP filename PO end)

### Phase 3 — Mutual exclusion

- [ ] Concurrent slit bundle + PO end on the same mill are serialized (`IMillBundleStateLock`)
- [ ] WIP folder warnings appear for non-File mills when file watcher sees changes
- [ ] No double-fire from file-based PO end on Plc mills

### Phase 4 — WIP reconciliation (Mill-4 File today)

- [ ] `FileBasedPoEnd.ReconciliationEnabled` tested in dev/staging
- [ ] Missed WIP PO changes are caught within reconciliation interval
- [ ] Correlation IDs present on reconciled file-based PO-end events

### Phase 5 — Bundle print status in SQL

- [ ] `docs/NDT_Bundle_Alter_PrintStatus.sql` applied to target database
- [ ] New bundles record `PrintStatus=Pending` before print attempt
- [ ] Successful print → `Printed`; failure → `PrintFailed` with error text
- [ ] Stuck-print threshold monitoring reviewed

### Mill-2 sign-off

- [ ] Mill-2 PO-end stable for agreed soak period (operations + controls)
- [ ] **Only then** set Mills 1 and 3 to `PoEndSource=Plc`

---

## Checklist B — Phase 6 TCP before production `TcpOpen`

Complete before changing Mill-4 `PoEndSource` to `TcpOpen` in Production.

### Codec and CI

- [ ] `Mill4MessageCodec` unit tests pass in CI
- [ ] `TcpOpenCommSimulatorTests` pass (loopback ack + enqueue)
- [ ] Controls engineer confirms AG_SEND/AG_RECV byte layout; update **only** `Mill4MessageCodec.cs` if layout differs

### Dev / staging with simulator

- [ ] Dev appsettings: Mill-4 `PoEndSource=TcpOpen`, `TcpOpenCommHost`, `TcpOpenCommPort` pointed at simulator or staging PLC
- [ ] Service logs: TCP connect, PO-end frame received, ack sent, queue enqueue, workflow CorrelationId
- [ ] Disconnect simulator → reconnect Warning logs with increasing delay; no silent stall
- [ ] Revert to `File` → TCP worker idle; file-based PO end unchanged

### Staging with real CP connection

- [ ] Ladder AG_SEND/AG_RECV verified on staging PLC **before** Production config change
- [ ] End-to-end: PO end on floor → ack on wire → bundle workflow → print status
- [ ] `PlcPoEndQueueWorker` starts when TcpOpen mills configured (even if `TelemetryOnly=true` for S7)

### Production cutover (Mill-4)

- [ ] Mill-2 and Mills 1/3 Phases 1–5 signed off (Checklist A)
- [ ] Staging TCP sign-off complete
- [ ] Change Mill-4 `PoEndSource` to `TcpOpen` in Production appsettings
- [ ] Monitor first production PO ends: correlation IDs, ack timing, print status
- [ ] Rollback plan documented: revert Mill-4 to `File` without service code deploy

---

## Assumptions (review with controls)

| Item | Current assumption |
|------|-------------------|
| TCP role | **MES = client**, PLC = server (`TcpClient.Connect`) |
| Frame size | Fixed **3-byte** minimum (PO_Type_ID UInt16 BE + trigger byte) |
| Ack | Single byte `0x01` outbound |
| NDT count | `NdtCountFinal = 0` until codec exposes count bytes |
| PO mapping | `PO_Type_ID` → `PlcPoEndRequest.PoId` for PO number resolution |

If ladder expects MES as TCP server, change transport only (`TcpListener`); business path unchanged.
