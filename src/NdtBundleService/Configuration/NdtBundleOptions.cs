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

    /// <summary>When true, the service writes NDT_Bundle_*.csv summary files in OutputBundleFolder. When false, these summary files are skipped while other CSV flows continue.</summary>
    public bool EnableBundleSummaryCsvFiles { get; set; } = true;

    /// <summary>When <see cref="EnableBundleSummaryCsvFiles"/> is true, folder for NDT_Bundle_*.csv summaries; when empty, falls back to <see cref="OutputBundleFolder"/>. Also used when scanning CSVs for bundle list if not using SQL.</summary>
    public string BundleSummaryOutputFolder { get; set; } = string.Empty;

    /// <summary>
    /// Folder where scheduled/manual <c>UploadNdtBundle__PO__…</c> CSV files are written.
    /// Production example: <c>Z:\To SAP\TM\NDT\MES PAS NDT\Bundle</c>.
    /// </summary>
    public string UploadNdtBundleFilesFolder { get; set; } = @"Z:\To SAP\TM\NDT\MES PAS NDT\Bundle";

    /// <summary>Folder where Slit Accepted files are written (used to map slit width by slit batch number). Production example: <c>Z:\To SAP\TM\Slitting\Slit Accepted</c>.</summary>
    public string SlitAcceptedFolder { get; set; } = @"Z:\To SAP\TM\Slitting\Slit Accepted";

    /// <summary>When true, the upload bundle CSV generator runs on a timer.</summary>
    public bool EnableUploadNdtBundleScheduler { get; set; } = true;

    /// <summary>Timer interval in hours for generating Upload NDT Bundle Files CSV.</summary>
    public int UploadNdtBundleIntervalHours { get; set; } = 12;

    /// <summary>Optional local IP to bind to when connecting to the printer (e.g. 192.168.0.14). Use when the PC has multiple NICs and you want to force the same interface that can reach the printer. Leave empty to let the OS choose.</summary>
    public string NdtTagPrinterLocalBindAddress { get; set; } = string.Empty;

    /// <summary>When false, SQL Server is never used for bundles (reads/writes use CSV folders only), even if <see cref="ConnectionString"/> is set (e.g. env override).</summary>
    public bool UseSqlServerForBundles { get; set; } = true;

    /// <summary>SQL Server connection string for NDT_Bundle and reconciliation. Ignored unless <see cref="UseSqlServerForBundles"/> is true. If empty, bundle list comes from output CSVs.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Optional UTC cutoff (ISO-8601, e.g. <c>2026-04-05T00:00:00Z</c>). When set, only CSV files whose last write time (UTC)
    /// is on or after this instant are read for input slits, WIP merge, PO plan rotation, Input Slit Accepted, and bundle-output CSVs used for ndt-summary.
    /// Leave empty to include all files.
    /// </summary>
    public string? MinSourceFileLastWriteUtc { get; set; }

    /// <summary>Optional per-mill PLC PO-end signals (Modbus TCP, etc.).</summary>
    public PlcPoEndOptions PlcPoEnd { get; set; } = new();

    /// <summary>Optional live NDT count + running PO from WIP bundle folder for one mill (e.g. Mill-3 S7).</summary>
    public MillSlitLiveOptions MillSlitLive { get; set; } = new();
}

