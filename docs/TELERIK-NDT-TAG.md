# Telerik NDT Tag Printing

The NDT bundle tag uses the **Telerik report design** (Rpt_NDTLabel): 100mm x 100mm label with Specification, Type, Size, Length, Pcs/Bnd, Slit Number, Bundle Number, and Code128 barcodes.

## Data flow

- **From bundle flow:** NDT_Batch_No (formatted), total NDT pcs, Slit No (from context record).
- **From bundle label file:** Specification, Type, Pipe Size, Length — loaded from a CSV by (PO Number, Mill No).

## Enabling Telerik printing

1. **Add Telerik NuGet feed credentials**  
   The Telerik.Reporting package is on a private feed and requires authentication. In the repo root `nuget.config`:
   - Open `nuget.config` and find the `<packageSourceCredentials><telerik>` section.
   - Replace `YOUR_TELERIK_EMAIL` with your Telerik.com account email.
   - Replace `YOUR_TELERIK_PASSWORD` with your Telerik password (or API token if you use one).
   - Save the file, then run `dotnet restore` from the solution or `src/NdtBundleService` folder.  
   If you don’t have a Telerik account, sign up at [telerik.com](https://www.telerik.com) (trial available).

2. **Reference the Reports project**  
   In `src/NdtBundleService/NdtBundleService.csproj`, the `ProjectReference` to `NdtBundleService.Reports` and the `Telerik.Reporting` package are enabled. Restore will succeed once the Telerik feed credentials in `nuget.config` are set.

3. **Label printer registration**  
   In `src/NdtBundleService/Program.cs`, **TelerikNdtLabelPrinter** is registered. To run without Telerik (e.g. if the feed is not configured), switch back to **StubNdtLabelPrinter** in `Program.cs` and revert the csproj changes (see “Without Telerik” below).

4. **Configure the bundle label file**  
   In `appsettings.json` (or environment), set:
   - **BundleLabelCsvPath:** Full path to the CSV with columns: `PO Number`, `Mill No`, `Specification`, `Type`, `Pipe Size`, `Length` (keyed by PO Number and Mill No).

5. **Printer: by name or by IP**  
   - **NdtTagPrinterName:** Windows printer name (e.g. a shared printer `\\server\NDTPrinter`). If set, the service uses this for print (PDF is also saved to OutputBundleFolder).  
   - **NdtTagPrinterAddress:** IP or hostname for direct network printing. Use `0.0.0.0` as placeholder until the real printer IP is specified. When set to a valid address (e.g. `192.168.1.100`), the rendered PDF is sent over TCP to this host.  
   - **NdtTagPrinterPort:** Port for direct IP printing (default `9100`, common for many label printers).  
   If neither is set, the report is only rendered to PDF in **OutputBundleFolder** (file name: `NDTLabel_{BundleNo}_{timestamp}.pdf`).

## Bundle label CSV example

```csv
PO Number,Mill No,Specification,Type,Pipe Size,Length
1000055673,1,SPEC-XXX,TypeA,6,40
1000055674,1,SPEC-YYY,TypeB,4,20
```

## Without Telerik

If the Telerik NuGet feed is not configured, restore/build will fail (401 on Telerik feed). To build without Telerik, in `Program.cs` register **StubNdtLabelPrinter** instead of **TelerikNdtLabelPrinter**, and remove or comment out the `ProjectReference` to `NdtBundleService.Reports` and the `Telerik.Reporting` package reference in `NdtBundleService.csproj`. The stub does not render or print tags; CSV export and the rest of the bundle flow still run.
