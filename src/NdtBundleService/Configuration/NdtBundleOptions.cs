namespace NdtBundleService.Configuration;

public class NdtBundleOptions
{
    /// <summary>Folder where Input Slit CSV files are dropped.</summary>
    public string InputSlitFolder { get; set; } = string.Empty;

  /// <summary>
  /// Optional folder where Input Slit CSV files are moved after acceptance (e.g. SAP). When set, <c>ndt-summary</c> and
  /// current-PO-per-mill on the dashboard sum/read from both this folder and <see cref="InputSlitFolder"/>; files are never modified.
  /// </summary>
  public string InputSlitAcceptedFolder { get; set; } = string.Empty;

  /// <summary>
  /// When true (default), running PO per mill on the Summary dashboard is resolved from the latest rows in
  /// <see cref="InputSlitFolder"/> / <see cref="InputSlitAcceptedFolder"/> before SQL <c>Input_Slit_Row</c>.
  /// This matches the floor truth in the Input Slit inbox (e.g. <c>Z:\To SAP\TM\Input Slit</c>).
  /// </summary>
  public bool PreferInputSlitFilesForRunningPo { get; set; } = true;

  /// <summary>
  /// Folder for NDT Input Slit output CSVs: same columns as input plus <c>NDT Batch No</c> (written by <c>SlitMonitoringWorker</c>).
    /// Production example: <c>Z:\To SAP\TM\NDT\NDT Input Slit\Input Slit</c>. When empty, those files are not written.
    /// </summary>
    public string OutputBundleFolder { get; set; } = string.Empty;

    /// <summary>
    /// Folder for the single consolidated NDT process CSV (written after Revisual completes) and optional ZPL previews.
    /// Also scanned as input when generating Upload NDT Bundle CSVs. Production example:
    /// <c>Z:\To SAP\TM\NDT\NDT Final Output\Bundle</c> (folder must exist for the upload scheduler).
    /// </summary>
    public string NdtProcessOutputFolder { get; set; } = @"Z:\To SAP\TM\NDT\NDT Final Output\Bundle";

    /// <summary>Folder where PO Plan (WIP) CSV files are dropped from SAP (e.g. D:\NDT\From SAP\TM). When set, one file is used as current PO plan; advance to next file only on PO End. If empty, single-file paths below are used.</summary>
    public string PoPlanFolder { get; set; } = string.Empty;

    /// <summary>
    /// When greater than 0, only PO plan CSVs in <see cref="PoPlanFolder"/> whose <c>LastWriteTimeUtc</c> is on or after
    /// <c>UtcNow - PoPlanFolderRollingDays</c> are scanned (merged WIP, pipe size, current plan list). Combined with
    /// <see cref="MinSourceFileLastWriteUtc"/> using the later (stricter) cutoff. Set to <c>0</c> to disable the rolling window
    /// and use only <see cref="MinSourceFileLastWriteUtc"/> (or no date filter if that is empty). Default <c>90</c>.
    /// </summary>
    public int PoPlanFolderRollingDays { get; set; } = 90;

    /// <summary>Path to the PO_Plan CSV file that includes NDTPcsPerBundle per PO. Used when PoPlanFolder is not set.</summary>
    public string PoPlanCsvPath { get; set; } = string.Empty;

    /// <summary>Path to the NDT Bundle Formation Chart CSV. When empty, built-in chart is used. Chart defines RequiredNdtPcs (pieces per bundle) per Pipe Size.</summary>
    public string FormationChartCsvPath { get; set; } = string.Empty;

    /// <summary>Path to the CSV that contains PO Number and Pipe Size. When PoPlanFolder is set, the current file from that folder (e.g. D:\NDT\From SAP\TM) is used instead. Pipe Size from here is looked up in the NDT Bundle Formation Chart to determine pieces per bundle.</summary>
    public string PipeSizeCsvPath { get; set; } = string.Empty;

    /// <summary>Polling interval in seconds for scanning input folders and PLC signals.</summary>
    public int PollIntervalSeconds { get; set; } = 5;

    /// <summary>Shop ID for NDT_Batch_No format (2 digits, e.g. 01, 02, 03, 04).</summary>
    public string ShopId { get; set; } = "01";

    /// <summary>Path to the CSV file that contains label fields per (PO Number, Mill No): Specification, Type, Pipe Size, Length. Used for the Telerik NDT tag report.</summary>
    public string BundleLabelCsvPath { get; set; } = string.Empty;

    /// <summary>Optional printer name for NDT tags (e.g. Windows printer name or "\\192.168.1.100\ShareName"). If empty, report is rendered to PDF in OutputBundleFolder only.</summary>
    public string NdtTagPrinterName { get; set; } = string.Empty;

    /// <summary>IP address (or hostname) of the NDT tag printer for direct network printing. Placeholder: use "0.0.0.0" until the real IP is specified. When set to a valid IP/host, the service will send the rendered tag to this address (e.g. port 9100 for raw printing).</summary>
    public string NdtTagPrinterAddress { get; set; } = "0.0.0.0";

    /// <summary>Port for direct network printing when NdtTagPrinterAddress is set (e.g. 9100 for many label printers). Ignored if using NdtTagPrinterName.</summary>
    public int NdtTagPrinterPort { get; set; } = 9100;

    /// <summary>When true, the service writes ZPL preview files to output folders and sends tags to NdtTagPrinterAddress. When false, only CSV outputs are produced (bundle CSV, slit CSV, manual station CSV); no ZPL files and no network print.</summary>
    public bool EnableNdtTagZplAndPrint { get; set; }

    /// <summary>When true, manual station state is persisted as JSON files under OutputBundleFolder\ManualStationState. When false, state is kept in memory only and no files are generated in that folder.</summary>
    public bool EnableManualStationStateFiles { get; set; }

    /// <summary>
    /// When true, persists per-(PO, mill) bundle sequence, running NDT total, and engine size counts to
    /// <see cref="NdtBundleRuntimeStateFile"/> (or OutputBundleFolder\NdtBundleRuntimeState.json) so tag numbering continues after restart.
    /// </summary>
    public bool EnableNdtBundleRuntimeStatePersistence { get; set; } = true;

    /// <summary>Optional full path for runtime state JSON. When empty, uses OutputBundleFolder\NdtBundleRuntimeState.json.</summary>
    public string? NdtBundleRuntimeStateFile { get; set; }

    /// <summary>
    /// Optional last-known NDT batch number per mill (keys <c>"1"</c>–<c>"4"</c>, values e.g. <c>1226100029</c>).
    /// On startup the service never assigns a lower sequence for that mill than the maximum of these seeds,
    /// bundles already in SQL/CSV, and persisted runtime state. Used for first deploy and as a safety floor after restart.
    /// </summary>
    public Dictionary<string, string> InitialMillBatchNumbers { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// When true (default), startup and manual sequence sync ignore placeholder <c>NDT_Bundle</c> rows
    /// (<c>Total_NDT_Pcs = 0</c>, never printed) so restarts continue from the last real tag print.
    /// </summary>
    public bool SyncRuntimeStateFromPrintedBundlesOnly { get; set; } = true;

    /// <summary>When true, writes <c>NDT_Bundle_{batchNo}.csv</c> to <see cref="BundleSummaryOutputFolder"/>. When false, bundle summary CSV is skipped (ZPL may still be written on print).</summary>
    public bool EnableBundleSummaryCsvFiles { get; set; } = true;

    /// <summary>
    /// Folder for completed bundle artifacts: <c>NDT_Bundle_{batchNo}.csv</c> and <c>NDT_Bundle_{batchNo}.zpl</c>.
    /// Production example: <c>Z:\To SAP\TM\NDT\NDT Bundles</c>. Per-slit input-slit CSVs stay in <see cref="OutputBundleFolder"/>.
    /// When empty, falls back to <see cref="OutputBundleFolder"/>.
    /// </summary>
    public string BundleSummaryOutputFolder { get; set; } = string.Empty;

    /// <summary>
    /// Folder where scheduled/manual <c>UploadNdtBundle__PO__…</c> CSV files are written.
    /// Production example: <c>Z:\To SAP\TM\NDT\MES PAS NDT\Bundle</c>.
    /// </summary>
    public string UploadNdtBundleFilesFolder { get; set; } = @"Z:\To SAP\TM\NDT\MES PAS NDT\Bundle";

    /// <summary>Folder where slitting accepted CSVs are read for Slit Width on upload bundle files. Production: <c>Z:\To SAP\TM\Slitting\Slit Accepted</c> (not Input Slit Accepted).</summary>
    public string SlitAcceptedFolder { get; set; } = @"Z:\To SAP\TM\Slitting\Slit Accepted";

    /// <summary>
    /// When true, after PO end each mill waits for a new <c>WIP_MM_PO_…</c> file in the TM Bundle folder before slit bundling
    /// resumes for that mill. The WIP filename and CSV provide the next running PO and pipe details.
    /// </summary>
    public bool WaitForWipBundleAfterPoEnd { get; set; } = true;

    /// <summary>
    /// Mills for which <c>SlitMonitoringWorker</c> creates NDT Input Slit output CSVs, updates bundle state, and prints tags.
    /// Empty or null = all mills 1–4. Example: <c>[1]</c> for Mill-1-only commissioning.
    /// Input inbox files for other mills are acknowledged without writing NDT output.
    /// </summary>
    public int[]? InputSlitProcessMills { get; set; }

    /// <summary>TM FG bundle folder (<c>FG_{mill}_{po}_….csv</c>) for Pipe Grade → Slit Grade. When empty, uses <see cref="MillSlitLiveOptions.WipBundleFolder"/>.</summary>
    public string FgBundleFolder { get; set; } = @"Z:\To SAP\TM\Bundle";

    /// <summary>Accepted FG bundle folder. When empty, uses <see cref="MillSlitLiveOptions.WipBundleAcceptedFolder"/>.</summary>
    public string FgBundleAcceptedFolder { get; set; } = @"Z:\To SAP\TM\Bundle Accepted";

    /// <summary>When true, the upload bundle CSV generator runs on a timer.</summary>
    public bool EnableUploadNdtBundleScheduler { get; set; } = true;

    /// <summary>Timer interval in hours for generating Upload NDT Bundle Files CSV.</summary>
    public int UploadNdtBundleIntervalHours { get; set; } = 12;

    /// <summary>Optional local IP to bind to when connecting to the printer (e.g. 192.168.0.14). Use when the PC has multiple NICs and you want to force the same interface that can reach the printer. Leave empty to let the OS choose.</summary>
    public string NdtTagPrinterLocalBindAddress { get; set; } = string.Empty;

    /// <summary>When false, SQL Server is never used for bundles (reads/writes use CSV folders only), even if <see cref="ConnectionString"/> is set (e.g. env override).</summary>
    public bool UseSqlServerForBundles { get; set; }

    /// <summary>Default age threshold in minutes for <see cref="INdtBundleRepository.GetStuckPrintsAsync"/>.</summary>
    public int StuckPrintThresholdMinutes { get; set; } = 10;

    /// <summary>
    /// When true and <see cref="UseSqlServerForBundles"/> is enabled, reconcile/printed-tags list endpoints read bundles and slits from SQL first.
    /// CSV folder scan is used only when SQL is disabled or the query fails (avoids slow Z:\ scans on dashboard load).
    /// </summary>
    public bool PreferSqlForReconcileReads { get; set; } = true;

    /// <summary>
    /// When true and SQL is configured, pipe size and PO plan WIP enrichment read from <c>dbo.PO_Plan_WIP</c>
    /// instead of scanning PO plan CSV folders on UNC (falls back to CSV when SQL is unavailable or empty).
    /// </summary>
    public bool PreferSqlForPoPlanWip { get; set; } = true;

    /// <summary>
    /// When <see cref="PreferSqlForPoPlanWip"/> is true, optionally merge pipe sizes from TM WIP bundle CSV folders
    /// for POs missing in SQL. Default false to avoid slow UNC scans on every cache refresh.
    /// </summary>
    public bool MergeWipBundlePipeSizesWhenUsingSqlPoPlan { get; set; }

    /// <summary>
    /// When true and SQL is configured, import eligible files from <see cref="PoPlanFolder"/> (PO Accepted)
    /// into <c>dbo.PO_Plan_WIP</c> on startup and periodically. Skips files already imported at the same last-write time.
    /// </summary>
    public bool ImportPoPlanWipFromFolder { get; set; } = true;

    /// <summary>
    /// Minutes between PO Accepted folder scans for new/changed files. 0 = startup import only.
    /// </summary>
    public int ImportPoPlanWipPollMinutes { get; set; } = 5;

    /// <summary>
    /// UTC cutoff for PO Accepted import into <c>PO_Plan_WIP</c> (ISO-8601, e.g. <c>2026-06-01T00:00:00Z</c>).
    /// When empty, falls back to <see cref="MinSourceFileLastWriteUtc"/> (no rolling window for import).
    /// </summary>
    public string? PoPlanImportMinLastWriteUtc { get; set; } = "2026-06-01T00:00:00Z";

    /// <summary>
    /// When false and SQL bundle list fails, returns an empty list instead of scanning all output CSV files (prevents long hangs / timeouts on the dashboard).
    /// </summary>
    public bool AllowCsvFallbackForBundleReads { get; set; } = true;

    /// <summary>SQL Server connection string for NDT_Bundle and reconciliation. Ignored unless <see cref="UseSqlServerForBundles"/> is true. If empty, bundle list comes from output CSVs.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>When <see cref="ConnectionString"/> is empty, builds Trusted_Connection string from Server + Database. Production VM NetBIOS: <c>AJS-SOH-VM-PAS-\SQLEXPRESS</c> (normalized to localhost when the app runs on that host).</summary>
    public string SqlServer { get; set; } = @"AJS-SOH-VM-PAS-\SQLEXPRESS";

    /// <summary>Database name when using <see cref="SqlServer"/> instead of <see cref="ConnectionString"/> (default target: JazeeraMES_Prod).</summary>
    public string SqlDatabase { get; set; } = "JazeeraMES_Prod";

    /// <summary>
    /// Optional UTC cutoff (ISO-8601, e.g. <c>2026-04-05T00:00:00Z</c>). When set, only CSV files whose last write time (UTC)
    /// is on or after this instant are read for input slits, WIP merge, PO plan rotation, Input Slit Accepted, and bundle-output CSVs used for ndt-summary.
    /// Leave empty to include all files.
    /// </summary>
    public string? MinSourceFileLastWriteUtc { get; set; }

    /// <summary>Optional per-mill PLC PO-end signals (Modbus TCP, etc.). Disabled when <see cref="PlcHandshake"/> is enabled.</summary>
    public PlcPoEndOptions PlcPoEnd { get; set; } = new();

    /// <summary>
    /// Persistent per-mill S7 PO-change handshake (trigger/ack M-bits). Preferred over legacy <see cref="PlcPoEnd"/> polling.
    /// </summary>
    public PlcHandshakeOptions PlcHandshake { get; set; } = new();

    /// <summary>PO end detection from TM Bundle WIP filenames instead of the PLC PO-change trigger.</summary>
    public FileBasedPoEndOptions FileBasedPoEnd { get; set; } = new();

    /// <summary>Optional live NDT count + running PO from WIP bundle folder for one mill (e.g. Mill-3 S7).</summary>
    public MillSlitLiveOptions MillSlitLive { get; set; } = new();

    /// <summary>Password-protected dashboard settings (formation chart thresholds, per-mill printers).</summary>
    public DashboardSettingsOptions DashboardSettings { get; set; } = new();

    /// <summary>Prunes idle completed PO/mill slots from persisted runtime state (see <see cref="RuntimeStatePruningOptions"/>).</summary>
    public RuntimeStatePruningOptions RuntimeStatePruning { get; set; } = new();
}

