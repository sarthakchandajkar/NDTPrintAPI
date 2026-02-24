namespace NDTPrintApi.Services;

/// <summary>
/// Stub implementation that throws until Telerik.Reporting is added and a real printer is registered.
/// To enable printing: add Telerik NuGet feed (https://nuget.telerik.com/v3/index.json), install Telerik.Reporting,
/// implement INdtReportPrinter using ReportProcessor, and register it in Program.cs instead of this stub.
/// </summary>
public class StubNdtReportPrinter : INdtReportPrinter
{
    public Task PrintAsync(string connectionString, int ndtBundleId, int millNo, bool isReprint, string printerName, CancellationToken cancellationToken = default)
    {
        throw new InvalidOperationException(
            "NDT report printing is not configured. Add the Telerik.Reporting package (from https://nuget.telerik.com/v3/index.json) " +
            "and register an INdtReportPrinter implementation that uses Telerik ReportProcessor.");
    }
}
