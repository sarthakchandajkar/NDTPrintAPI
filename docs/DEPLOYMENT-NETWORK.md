# Network deployment (VM hosting)

Deploy on the production VM so operators on the LAN can use the dashboard.

**This repo’s production VM:** `10.2.5.202`  
**User URL:** `http://10.2.5.202:3000`

---

## 1. Prerequisites on the VM

- .NET 10 runtime (or publish self-contained)
- Node.js 18+
- NSSM (`C:\Tools\nssm\nssm.exe`) for Node Windows Services
- `Z:\` mapped drive or UNC paths (service account must see SAP folders)
- SQL Server: `AJS-SOH-VM-PAS-\SQLEXPRESS` / `JazeeraMES_Prod`

---

## 2. Config files (before build)

| Component | File | Action |
|-----------|------|--------|
| Backend | `src/NdtBundleService/appsettings.Production.json` | Already set for Z:\ paths, SQL, PLCs. Listens on `http://*:5000`. |
| Dashboard | `ndtbundle-dashboard/.env.production` → `.env.local` | `copy .env.production .env.local` then `npm run build` |
| PLC bridge | `plc-server/.env.production` → `.env` | `copy .env.production .env` then `npm install` |

**Important:** `NEXT_PUBLIC_*` and dashboard `npm run build` must happen **after** `.env.local` is in place.

---

## 3. Publish and install

```powershell
cd C:\Users\Sarthak\source\repos\NDTPrintAPI

# Backend
dotnet publish .\src\NdtBundleService\NdtBundleService.csproj -c Release -o C:\Apps\NdtBundleService
copy .\src\NdtBundleService\appsettings.Production.json C:\Apps\NdtBundleService\

# PLC server
cd .\plc-server
copy .env.production .env
npm install

# Dashboard
cd ..\ndtbundle-dashboard
copy .env.production .env.local
npm install
npm run build
```

### Windows Services

See `docs/DEPLOYMENT-PRODUCTION.md` for `NdtBundleService` install.

**PLC server (NSSM):**

```powershell
$nssm = "C:\Tools\nssm\nssm.exe"
& $nssm install "NdtPlcServer" "C:\Program Files\nodejs\node.exe" "server.js"
& $nssm set "NdtPlcServer" AppDirectory "C:\Users\Sarthak\source\repos\NDTPrintAPI\plc-server"
& $nssm set "NdtPlcServer" Start SERVICE_AUTO_START
Start-Service NdtPlcServer
```

**Dashboard (NSSM):**

```powershell
& $nssm install "NdtDashboard" "C:\Program Files\nodejs\node.exe" ".\node_modules\next\dist\bin\next" "start -p 3000 -H 0.0.0.0"
& $nssm set "NdtDashboard" AppDirectory "C:\Users\Sarthak\source\repos\NDTPrintAPI\ndtbundle-dashboard"
& $nssm set "NdtDashboard" Start SERVICE_AUTO_START
Start-Service NdtDashboard
```

### Firewall

```powershell
New-NetFirewallRule -DisplayName "NDT Dashboard 3000" -Direction Inbound -Protocol TCP -LocalPort 3000 -Action Allow
New-NetFirewallRule -DisplayName "NDT API 5000"       -Direction Inbound -Protocol TCP -LocalPort 5000 -Action Allow
New-NetFirewallRule -DisplayName "NDT PLC 3030"       -Direction Inbound -Protocol TCP -LocalPort 3030 -Action Allow
```

---

## 4. Verify

| Check | URL |
|-------|-----|
| Dashboard | `http://10.2.5.202:3000` |
| API / Swagger | `http://10.2.5.202:5000/swagger` |
| Bundles API | `http://10.2.5.202:5000/api/Test/bundles` |

---

## 5. Deploy timing

Prefer deploying **NdtBundleService** when mills are off so slit CSVs and PO-end signals are not missed during restart.

---

## 6. Service account note

If `NdtBundleService` runs as **Local System**, mapped `Z:\` drives are not visible. Either:

- Run the service under a user that has `Z:\` mapped, or
- Use UNC paths in `appsettings.Production.json` (e.g. `\\server\share\...`).
