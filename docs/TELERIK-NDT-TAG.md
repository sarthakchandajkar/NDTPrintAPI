# Telerik NDT Tag Printing

The NDT bundle tag uses the **Telerik report design** (Rpt_NDTLabel): 100mm x 100mm label with Specification, Type, Size, Length, Pcs/Bnd, Slit Number, Bundle Number, and Code128 barcodes.

## Data flow

- **From bundle flow:** NDT_Batch_No (formatted), total NDT pcs, Slit No (from context record).
- **From bundle label file:** Specification, Type, Pipe Size, Length — loaded from a CSV by (PO Number, Mill No).

## Enabling Telerik printing

1. **Add Telerik NuGet feed**  
   The Telerik.Reporting package is on a private feed. In `nuget.config` (or your user NuGet config), add a source with your credentials:
   - Source: `https://nuget.telerik.com/v3/index.json`
   - Use your Telerik account or license credentials as required by Telerik.

2. **Reference the Reports project**  
   In `src/NdtBundleService/NdtBundleService.csproj`, uncomment the `ProjectReference` to `NdtBundleService.Reports`.

3. **Register the Telerik printer**  
   In `src/NdtBundleService/Program.cs`, replace:
   - `builder.Services.AddSingleton<INdtLabelPrinter, StubNdtLabelPrinter>();`  
   with:
   - `builder.Services.AddSingleton<INdtLabelPrinter, TelerikNdtLabelPrinter>();`

4. **Configure the bundle label file**  
   In `appsettings.json` (or environment), set:
   - **BundleLabelCsvPath:** Full path to the CSV with columns: `PO Number`, `Mill No`, `Specification`, `Type`, `Pipe Size`, `Length` (keyed by PO Number and Mill No).

5. **Optional: printer name**  
   Set **NdtTagPrinterName** to send the tag to a specific printer; if empty, the report is rendered to PDF in **OutputBundleFolder** (file name: `NDT_Tag_{NDT_Batch_No}.pdf`).

## Bundle label CSV example

```csv
PO Number,Mill No,Specification,Type,Pipe Size,Length
1000055673,1,SPEC-XXX,TypeA,6,40
1000055674,1,SPEC-YYY,TypeB,4,20
```

## Without Telerik

If the Telerik feed is not configured, the solution builds with **StubNdtLabelPrinter**: no tag is rendered or printed, but CSV export and the rest of the bundle flow still run.
