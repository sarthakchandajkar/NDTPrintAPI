namespace NdtBundleService.Configuration;

public class NdtBundleOptions
{
    /// <summary>Folder where Input Slit CSV files are dropped.</summary>
    public string InputSlitFolder { get; set; } = string.Empty;

    /// <summary>Folder where output bundle CSV files (with NDT_Batch_No) are written.</summary>
    public string OutputBundleFolder { get; set; } = string.Empty;

    /// <summary>Folder where PO Plan (WIP) CSV files are dropped from SAP (e.g. D:\NDT\From SAP\TM). When set, one file is used as current PO plan; advance to next file only on PO End. If empty, single-file paths below are used.</summary>
    public string PoPlanFolder { get; set; } = string.Empty;

    /// <summary>Path to the PO_Plan CSV file that includes NDTPcsPerBundle per PO. Used when PoPlanFolder is not set.</summary>
    public string PoPlanCsvPath { get; set; } = string.Empty;

    /// <summary>Path to the NDT Bundle Formation Chart CSV.</summary>
    public string FormationChartCsvPath { get; set; } = string.Empty;

    /// <summary>Path to the CSV file that contains PO Number and Pipe Size (linked by PO Number to Input Slit data). Used for Formation Chart and size-based bundle logic; NDT Short Length Pipe from input is not used.</summary>
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

    /// <summary>Optional local IP to bind to when connecting to the printer (e.g. 192.168.0.14). Use when the PC has multiple NICs and you want to force the same interface that can reach the printer. Leave empty to let the OS choose.</summary>
    public string NdtTagPrinterLocalBindAddress { get; set; } = string.Empty;

    /// <summary>SQL Server connection string for NDT_Bundle and reconciliation. If empty, bundle list comes from output CSVs and only CSV + reprint are updated on reconcile.</summary>
    public string ConnectionString { get; set; } = string.Empty;
}

