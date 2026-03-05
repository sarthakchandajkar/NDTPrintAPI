# Testing the NDT Bundle Flow

This guide helps you verify that the bundle flow works as expected: **Input Slit CSVs** → **Formation Chart** (pcs per bundle by pipe size) → **bundle close** (size threshold or PO end) → **export CSV** (same structure as input + `NDT_Batch_No`).

---

## Prerequisites

1. **Sample Input Slit CSVs** in `C:\Users\dell\Documents\Mill-1 Sample NDT Files`  
   - Headers must include: `PO Number`, `Slit No`, `NDT Pipes`, `Rejected P`, `Slit Start Time`, `Slit Finish Time`, `Mill No`, `NDT Short Length Pipe`, `Rejected Short Length Pipe`.
2. **NdtBundleService** running (e.g. `dotnet run --project src/NdtBundleService/NdtBundleService.csproj`).
3. **Output folder** writable: `C:\Users\dell\Documents\NDT Bundle Output` (from appsettings).

---

## 1. Get PO and Mill from your sample files

Your sample filenames look like: `2511743_01_260228_1000055673.csv`.  
Open one CSV and note:

- **PO Number** (e.g. `1000055673`)
- **Mill No** (e.g. `260228`)

You’ll use these when simulating PO end.

---

## 2. Run the service and let the worker load input

1. Start **NdtBundleService**.
2. Watch the console: you should see:
   - `Using built-in NDT Bundle Formation Chart (no CSV configured).`
   - `SlitMonitoringWorker started. Watching folder ...`
   - `Processing Input Slit CSV file ...` for each CSV in the input folder.

3. **During this step** the engine may already close some bundles (if any size reaches the Formation Chart threshold, e.g. 80 for size `2`). If so, bundle CSVs will appear in **Output Bundle Folder** immediately.

---

## 3. Simulate PO End (close remaining bundles)

Use either **Swagger** or the **Desktop UI**.

### Option A – Swagger

1. Open **http://localhost:5000/swagger**.
2. **POST /api/Test/po-end**  
   Body (JSON):
   ```json
   {
     "poNumber": "1000055673",
     "millNo": 260228
   }
   ```
   Use the PO and Mill from your sample CSVs.
3. Expect **200 OK** and log lines like:  
   `SIMULATED PRINT (CSV exported): PO ... Mill ... Batch ... NdtPcs ...`

### Option B – Desktop UI (NdtBundleApp)

1. Run NdtBundleApp; set Service URL to `http://localhost:5000`.
2. Enter **PO Number** and **Mill No** (same as above).
3. Click **Simulate PO End**.
4. Check the UI message and/or service logs.

---

## 4. Verify output CSVs

1. **List bundle files**  
   - **GET http://localhost:5000/api/Test/bundles**  
   - Or open folder: `C:\Users\dell\Documents\NDT Bundle Output`

2. **Check each bundle CSV**  
   - **Header**: same as input + last column **`NDT_Batch_No`**  
     Example:  
     `PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT_Batch_No`
   - **Data row**: one row per bundle; `NDT Pipes` column = total NDT pcs in that bundle; `NDT_Batch_No` = batch number.

3. **Logic checks**  
   - **Size-based bundles** (closed while processing slit files): for a given pipe size, `NDT Pipes` in the bundle CSV should match the Formation Chart (e.g. size `2` → 80, size `6` → 20, unknown size → 20 Default).
   - **PO-end bundles**: remaining accumulated count for that PO/Mill is closed; `NDT Pipes` can be less than a full formation-chart bundle (partial bundle at PO end).
   - Batch numbers should increase per PO/Mill (1, 2, 3, …).

---

## 5. Quick API check (PowerShell)

```powershell
# List current bundle CSVs
Invoke-RestMethod -Uri "http://localhost:5000/api/Test/bundles" -Method Get

# Simulate PO end (replace PO and Mill with your sample values)
$body = @{ poNumber = "1000055673"; millNo = 260228 } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:5000/api/Test/po-end" -Method Post -Body $body -ContentType "application/json"
```

---

## Expected behavior summary

| Step | What happens |
|------|----------------|
| Service starts | Worker scans input folder; Formation Chart loaded (built-in or CSV). |
| Slit CSVs processed | Each row with `NDT Pipes` > 0 is accumulated per (PO, Mill) and per pipe size (`NDT Short Length Pipe`). |
| Size threshold reached | When count for a size ≥ Formation Chart value (e.g. 80 for size 2), a bundle is closed and a CSV is written (worker). |
| Simulate PO End | Remaining global count and any remaining size counts for that PO/Mill are closed; one CSV per closed bundle (Test controller + output writer). |
| Output CSV | Same columns as input, plus `NDT_Batch_No`; one data row per bundle. |

If any of these steps or checks fail, inspect the service logs (bundle close messages, errors) and the actual CSV contents (headers, row count, `NDT Pipes`, `NDT_Batch_No`).
