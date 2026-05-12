# Deploying NdtBundleService to Production (24/7)

This guide describes how to deploy **NdtBundleService** (API + slit monitoring worker) to a Windows server and keep it running 24/7 as a **Windows Service**.

---

## 1. Prerequisites

- **Windows Server** (or Windows 10/11) with .NET 8+ runtime (or use self-contained deploy).
- **Admin rights** on the server to install and manage Windows Services.
- **Folders** for input slit CSVs, output bundle CSVs, and optional config files (Pipe Size, Bundle Label, Formation Chart) with correct permissions for the service account.

---

## 2. Publish the application

From the solution root:

```powershell
cd c:\Users\dell\NDTPrintAPI

# Framework-dependent (requires .NET runtime on server)
dotnet publish src\NdtBundleService\NdtBundleService.csproj -c Release -o C:\Deploy\NdtBundleService

# Or self-contained (no runtime needed on server; larger size)
dotnet publish src\NdtBundleService\NdtBundleService.csproj -c Release -r win-x64 --self-contained true -o C:\Deploy\NdtBundleService
```

Copy the published folder (e.g. `C:\Deploy\NdtBundleService`) to the production server (e.g. `D:\Apps\NdtBundleService`). Use a path without spaces to simplify service configuration.

---

## 3. Configuration on the server

1. **appsettings.Production.json** (or environment variables / other config) on the server with production values, for example:

   ```json
   {
     "NdtBundle": {
       "InputSlitFolder": "D:\\Data\\NDT\\InputSlit",
       "OutputBundleFolder": "D:\\Data\\NDT\\BundleOutput",
       "PoPlanCsvPath": "D:\\Data\\NDT\\PO_Plan.csv",
       "FormationChartCsvPath": "",
       "PipeSizeCsvPath": "D:\\Data\\NDT\\PipeSize.csv",
       "BundleLabelCsvPath": "D:\\Data\\NDT\\BundleLabel.csv",
       "ShopId": "01",
       "NdtTagPrinterName": "",
       "PollIntervalSeconds": 5,
       "EnableNdtTagZplAndPrint": true,
       "NdtTagPrinterAddress": "192.168.0.125",
       "NdtTagPrinterPort": 9100,
       "UseSqlServerForBundles": true,
       "ConnectionString": "Server=YOUR_SQL;Database=JazeeraMES_Prod;Trusted_Connection=True;TrustServerCertificate=True;"
     },
     "Logging": {
       "LogLevel": {
         "Default": "Information",
         "Microsoft.AspNetCore": "Warning"
       }
     }
   }
   ```

   Set **`ASPNETCORE_ENVIRONMENT=Production`** so this file merges with `appsettings.json`. **Physical labels** require `EnableNdtTagZplAndPrint=true`, a real **`NdtTagPrinterAddress`** (Honeywell PD45S–style ZPL on TCP **9100**), and the service account must reach that IP (firewall). **SQL traceability and `NDT_Bundle`** require `UseSqlServerForBundles=true` and a valid **`ConnectionString`** to `JazeeraMES_Prod` (often set via environment variable `NdtBundle__ConnectionString` on the VM instead of storing secrets in JSON). If someone calls `POST /api/Status/zpl-generation` with `enabled:false`, printing stays off until set back to `true` or the service restarts (override vs config: see `ZplGenerationToggle`).

2. **Folders:** Create the paths used above and grant the service account **Read** on input/config and **Read/Write** on output.

3. **URLs:** By default the app listens on `http://localhost:5000`. To change it, set `ASPNETCORE_URLS` (e.g. `http://*:5000`) or add `urls` in config.

---

## 4. Install as a Windows Service (24/7)

Run **PowerShell as Administrator** on the server.

### Option A: Local System account (simplest)

```powershell
$serviceName   = "NdtBundleService"
$displayName   = "NDT Bundle Service"
$description   = "NDT bundle tag service: slit CSV monitoring, formation chart, bundle CSV export and optional tag printing."
$exePath       = "D:\Apps\NdtBundleService\NdtBundleService.exe"
$contentRoot   = "D:\Apps\NdtBundleService"

# Content root ensures appsettings.json and working directory are correct when the service runs
New-Service -Name $serviceName -BinaryPathName "`"$exePath`" --contentRoot `"$contentRoot`"" -DisplayName $displayName -Description $description -StartupType Automatic
Start-Service -Name $serviceName
```

### Option B: Dedicated service account (recommended for production)

1. Create a local user (e.g. `NdtBundleSvc`) and give it **Log on as a service** (Local Security Policy → User Rights Assignment).
2. Grant that user **Read/Write** on the deploy folder and **Read** on input/config, **Read/Write** on output folders.
3. Create the service with that account:

   ```powershell
   $serviceName   = "NdtBundleService"
   $displayName   = "NDT Bundle Service"
   $description   = "NDT bundle tag service."
   $exePath       = "D:\Apps\NdtBundleService\NdtBundleService.exe"
   $contentRoot   = "D:\Apps\NdtBundleService"
   $credential    = Get-Credential   # e.g. .\NdtBundleSvc and password

   New-Service -Name $serviceName -BinaryPathName "`"$exePath`" --contentRoot `"$contentRoot`"" -DisplayName $displayName -Description $description -StartupType Automatic -Credential $credential
   Start-Service -Name $serviceName
   ```

`StartupType Automatic` makes the service start after reboot so it runs 24/7.

---

## 5. Automatic restart on failure (24/7 resilience)

Configure the service to restart if it crashes:

```powershell
# Restart after 1 minute, up to 3 times; reset failure count after 1 day
sc.exe failure NdtBundleService reset= 86400 actions= restart/60000/60000/60000
```

Or via **Services** (services.msc) → **NdtBundle Service** → **Properties** → **Recovery** tab:

- First failure: **Restart the service** (e.g. after 1 minute).
- Second failure: **Restart the service**.
- Subsequent failure: **Restart the service**.
- **Reset fail count after:** 1 day.

---

## 6. Manage the service

```powershell
Start-Service   -Name NdtBundleService
Stop-Service    -Name NdtBundleService
Restart-Service -Name NdtBundleService
Get-Service     -Name NdtBundleService
```

To remove the service (stop first):

```powershell
Stop-Service -Name NdtBundleService -Force
sc.exe delete NdtBundleService
```

---

## 7. Firewall and access

- If other machines need to call the API (e.g. NdtBundleApp or PLC integration), open **TCP 5000** (or the port in `ASPNETCORE_URLS`) in Windows Firewall for the app.
- For local-only access, leave the default `http://localhost:5000` and do not open the port.

---

## 8. Monitoring and logging

- **Event Log:** With `AddWindowsService()`, the app can log to **Windows Event Log** (Application source). If the event source cannot be created (e.g. non-admin install), a warning is logged and event log logging is skipped; file/console logging still apply.
- **Log level:** Control level in `appsettings.Production.json` under `Logging:LogLevel` (e.g. `Default: Information`).
- **Health:** Use the HTTP API (e.g. `GET /api/Test/bundles` or a dedicated health endpoint if you add one) to confirm the app is responding.
- **Output folder:** Monitor the bundle CSV output folder to verify bundles are being written.

---

## 9. Updating the application

1. Stop the service: `Stop-Service -Name NdtBundleService`
2. Replace the files in the deploy folder (e.g. `D:\Apps\NdtBundleService`) with the new publish output, keeping or merging `appsettings.Production.json`.
3. Start the service: `Start-Service -Name NdtBundleService`

---

## 10. Summary checklist

| Step | Action |
|------|--------|
| 1 | Publish with `dotnet publish` (FDD or SCD) |
| 2 | Copy publish output to server (e.g. `D:\Apps\NdtBundleService`) |
| 3 | Add/update `appsettings.Production.json` and create data folders with correct permissions |
| 4 | Install Windows Service with `New-Service` and `--contentRoot` |
| 5 | Set **StartupType** to **Automatic** |
| 6 | Configure **Recovery** (restart on failure) |
| 7 | Open firewall for the API port if needed |
| 8 | Use Event Log and API/health checks for monitoring |

With this, NdtBundleService runs 24/7 as a Windows Service and restarts automatically after reboots and after failures (when recovery is configured).
