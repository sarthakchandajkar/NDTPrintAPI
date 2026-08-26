# Dashboard behaviour under five-instance split (Phase 1 gaps)

Dashboard continues to call **Shared** (`:5000`) only. Mill instances listen on localhost `5001`–`5004` for ops/validation — not for the UI.

HTTP write proxy and `Mill_Instance_Status` SQL telemetry are **deferred**.

## Works on Shared (unchanged)

| Area | Notes |
|---|---|
| Reconcile (list / edit / print / SAP resubmit / PPC) | SQL + UNC CSV folders |
| ManualTags (Visual / Hydro / Revisual) | Must stay single Shared process |
| InputSlits list/read | Shared inbox UNC |
| Upload generate-now / scheduler | Shared only |
| Formation chart GET/PUT | Shared UNC file |
| Printers GET/PUT | Prefer Shared; mill files are per-instance — operator edits on Shared may not match mill-local printer JSON until paths are unified or proxied |
| ZPL generation toggle | SQL `App_Setting.ZplPhysicalPrintEnabled` — Shared + mills observe the same value |

## Empty / stale / wrong-process on Shared

| Endpoint / UI | Behaviour after split | Follow-up |
|---|---|---|
| `GET /api/Status/plc-live` | Empty or stale — Shared has no S7 mill loops | `Mill_Instance_Status` publish/read |
| Settings PLC connect / disconnect / test-po-change | No-op or empty registry on Shared | HTTP write proxy to mill |
| `POST /api/Test/po-end` | Runs against Shared's (empty) mill workers — **do not use for production mills** | Proxy to owned mill |
| `POST /api/Test/resume-wip/{n}` | Same — wrong process | Proxy |
| `GET /api/Test/live-mill-ndt` | Empty on Shared | Telemetry or proxy |
| Hooter / handshake live panels | Stale | Telemetry |

## Mill localhost APIs (non-prod / break-glass)

`Status`, `Settings`, `Test` remain registered on mill instances for validation against `http://127.0.0.1:500n`. Firewall should not expose these ports on the LAN.
