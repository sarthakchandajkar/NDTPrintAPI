namespace NdtBundleService.Configuration;

public class NdtBundleOptions
{
    /// <summary>Folder where Input Slit CSV files are dropped.</summary>
    public string InputSlitFolder { get; set; } = string.Empty;

    /// <summary>Folder where output bundle CSV files (with NDT_Batch_No) are written.</summary>
    public string OutputBundleFolder { get; set; } = string.Empty;

    /// <summary>Path to the PO_Plan CSV file that includes NDTPcsPerBundle per PO.</summary>
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

    /// <summary>Optional printer name for NDT tags. If empty, report is rendered to PDF in OutputBundleFolder only.</summary>
    public string NdtTagPrinterName { get; set; } = string.Empty;
}

