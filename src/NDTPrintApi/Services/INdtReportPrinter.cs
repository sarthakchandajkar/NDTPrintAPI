namespace NDTPrintApi.Services;

/// <summary>
/// Abstraction for printing the NDT tag report (e.g. via Telerik). Register a real implementation when Telerik.Reporting is available.
/// </summary>
public interface INdtReportPrinter
{
    /// <summary>
    /// Prints the NDT bundle tag report to the given printer.
    /// </summary>
    Task PrintAsync(string connectionString, int ndtBundleId, int millNo, bool isReprint, string printerName, CancellationToken cancellationToken = default);
}
