using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface IPoPlanProvider
{
    Task<IReadOnlyDictionary<string, PoPlanEntry>> GetPoPlansAsync(CancellationToken cancellationToken);
}

public interface IFormationChartProvider
{
    Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Provides pipe size per PO Number from an external file (e.g. CSV with PO Number and Pipe Size columns).
/// Used for Formation Chart lookup and size-based bundle logic; Input Slit CSV no longer supplies pipe size.
/// </summary>
public interface IPipeSizeProvider
{
    /// <summary>Returns PO Number -> Pipe Size. Empty or missing PO means no size (Default formation used for threshold; no per-size count).</summary>
    Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Provides label fields (Specification, Type, Pipe Size, Length) per (PO Number, Mill No) from the bundle label CSV file.
/// </summary>
public interface IBundleLabelInfoProvider
{
    Task<IReadOnlyDictionary<(string PoNumber, int MillNo), BundleLabelInfo>> GetBundleLabelInfoAsync(CancellationToken cancellationToken);
}

public interface IBundleOutputWriter
{
    Task WriteBundleAsync(InputSlitRecord contextRecord, int ndtBatchNo, int totalNdtPcs, CancellationToken cancellationToken);
}

/// <summary>
/// Renders and optionally prints the NDT bundle tag using the Telerik report design. Data comes from context + bundle label file.
/// </summary>
public interface INdtLabelPrinter
{
    Task PrintLabelAsync(InputSlitRecord contextRecord, string ndtBatchNoFormatted, int totalNdtPcs, bool isReprint, CancellationToken cancellationToken);
}

public interface IBundleEngine
{
    /// <summary>
    /// Processes a new slit record and determines whether one or more bundles are ready to be closed.
    /// When bundles are closed, the engine calls the provided callback with context record, batch number, and total NDT pcs.
    /// </summary>
    Task ProcessSlitRecordAsync(
        InputSlitRecord record,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken);

    /// <summary>
    /// Handles PO end signal for the current PO and closes any pending partial bundles.
    /// Callback receives a context record (last seen or synthetic), batch number, and total NDT pcs.
    /// </summary>
    Task HandlePoEndAsync(
        string poNumber,
        int millNo,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken);
}

public interface IPlcClient
{
    /// <summary>
    /// Returns true when the PO-end signal from the PLC is active.
    /// </summary>
    Task<bool> GetPoEndAsync(CancellationToken cancellationToken);
}

