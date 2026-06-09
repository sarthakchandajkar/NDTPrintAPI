namespace NdtBundleService.Configuration;

/// <summary>Rolling file logs for 24/7 Windows Service troubleshooting. Bound from <c>Logging:File</c>.</summary>
public sealed class FileLoggingOptions
{
    /// <summary>When false, only the default console / event log providers apply (not recommended for services).</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Log directory. Empty = <c>{ContentRoot}/Logs</c> next to the deployed exe.</summary>
    public string Folder { get; set; } = string.Empty;

    /// <summary>File name prefix; daily files are <c>{prefix}-YYYYMMDD.log</c>.</summary>
    public string FileNamePrefix { get; set; } = "ndtbundle";

    /// <summary>Number of daily log files to keep (older files deleted automatically).</summary>
    public int RetainFileCount { get; set; } = 31;

    /// <summary>Also write Warning+ to Windows Event Log (Application log, source NdtBundleService).</summary>
    public bool WriteToEventLog { get; set; } = true;
}
