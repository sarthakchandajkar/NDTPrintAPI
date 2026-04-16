using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>Identifies a removed line in a per-slit output CSV for Output_Slit_Row traceability cleanup.</summary>
public sealed record RemovedSlitRowTraceRef(string FileBaseName, int SourceRowNumber1Based, string PoNumber);

/// <summary>
/// Persists and queries NDT bundles for listing and reconciliation. When UseSqlServerForBundles and ConnectionString are set, uses SQL Server; otherwise uses output CSV folder.
/// </summary>
public interface INdtBundleRepository
{
    Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken);
    Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken);
    Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken);
    Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken);
    /// <summary>Finds all output CSVs containing the given NDT Batch No, updates the NDT Pipes column to newPipes, and overwrites the files. Returns the number of files updated.</summary>
    Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken);
    /// <summary>Returns slit details (slitNo -> ndtPipes sum) for the given NDT Batch No from per-slit output CSVs (excludes NDT_Bundle_*.csv summary files).</summary>
    Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken);
    /// <summary>Updates the NDT Pipes value for rows matching (batchNo, slitNo) in per-slit output CSVs and overwrites the affected files. Returns the number of files updated.</summary>
    Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken);
    /// <summary>Updates the bundle total in the database only (no output CSV updates). No-op if DB not configured.</summary>
    Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken);
    /// <summary>Updates the bundle summary CSV (NDT_Bundle_{batchNo}.csv) if present. Returns true if updated.</summary>
    Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken);

    /// <summary>
    /// Removes data rows for the given NDT batch and slit numbers from per-slit output CSVs (excludes NDT_Bundle_*).
    /// Deletes a file when only the header row would remain. Returns rows removed and refs for SQL traceability cleanup.
    /// </summary>
    Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
        string batchNo,
        IReadOnlyList<string> slitNos,
        CancellationToken cancellationToken);
}

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
/// Renders and optionally prints the NDT bundle tag. Data comes from context + bundle label file or from NDTBundlePrintData.
/// </summary>
public interface INdtLabelPrinter
{
    /// <summary>Renders and sends the NDT tag. Returns true if sent to printer (TCP or Windows), false if only saved to folder or send failed.</summary>
    Task<bool> PrintLabelAsync(InputSlitRecord contextRecord, string ndtBatchNoFormatted, int totalNdtPcs, bool isReprint, CancellationToken cancellationToken);

    /// <summary>Print tag from POC-style print data (layout matches Rpt_NDTLabel / NDT_Bundle_Printing_POC). Returns true if sent to printer.</summary>
    Task<bool> PrintLabelFromDataAsync(NDTBundlePrintData printData, CancellationToken cancellationToken);
}

/// <summary>
/// Single entry point for printing an NDT bundle tag from print data (POC flow). Returns true if print/send succeeded.
/// </summary>
public interface INdtBundleTagPrinter
{
    /// <summary>Renders the tag from printData (layout as per Rpt_NDTLabel), sends to configured printer or saves to file. Returns true on success.</summary>
    Task<bool> PrintNDTBundleTagAsync(NDTBundlePrintData printData, CancellationToken cancellationToken = default);
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
    /// Current PO-end signal level per mill (1–4). Mills without a configured endpoint read as false.
    /// </summary>
    Task<IReadOnlyDictionary<int, bool>> GetPoEndSignalsByMillAsync(CancellationToken cancellationToken);

    /// <summary>
    /// True when any mill reports PO-end active (convenience for dashboard).
    /// </summary>
    Task<bool> GetPoEndAsync(CancellationToken cancellationToken);

    /// <summary>
    /// When <see cref="PlcPoEndOptions.DetectionMode"/> is Modbus PO_Id transition, returns fresh per-mill snapshots; otherwise null (no extra I/O).
    /// </summary>
    Task<IReadOnlyDictionary<int, MillPoPlcSnapshot>?> ReadMillPoSnapshotsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Writes the MES acknowledgment coil for the mill (PLC RESETMESTOPLC), if configured. No-op for Stub or when the mill has no ack address.
    /// </summary>
    Task AcknowledgeMesPoChangeAsync(int millNo, CancellationToken cancellationToken);
}

/// <summary>
/// Runs the same steps as <c>POST /api/Test/po-end</c>: close partial bundles, advance batch state, optional PO plan file advance.
/// </summary>
public interface IPoEndWorkflowService
{
    Task ExecuteAsync(string poNumber, int millNo, bool advancePoPlanFile, CancellationToken cancellationToken);
}

/// <summary>
/// Resolves the active PO number per mill from Input Slit (+ Accepted) CSVs and optional SQL traceability.
/// </summary>
public interface IActivePoPerMillService
{
    Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken);

    IReadOnlyList<string> GetInputSlitReadFolderPaths();
}

/// <summary>
/// Shared state for NDT batch number and running total per (PO, Mill).
/// Batch increments when count reaches formation chart threshold or on PO End.
/// </summary>
public interface INdtBatchStateService
{
    /// <summary>
    /// Updates running total for (poNumber, millNo) by ndtPipes and returns the batch number for this record,
    /// the new total so far, and the threshold used (from formation chart by pipe size).
    /// </summary>
    Task<(int BatchNumber, int TotalSoFar, int Threshold)> GetBatchForRecordAsync(string poNumber, int millNo, int ndtPipes, CancellationToken cancellationToken);

    /// <summary>
    /// Call when PO End is triggered (e.g. Simulate PO End). Resets total and advances batch for the next bundle.
    /// </summary>
    Task IncrementBatchOnPoEndAsync(string poNumber, int millNo, CancellationToken cancellationToken);
}

/// <summary>
/// When using PoPlanFolder: tracks the "current" PO plan file (one file at a time). Advance to next file only on PO End.
/// PO End can be triggered by Simulate PO End button or eventually by PLC signal.
/// </summary>
public interface ICurrentPoPlanService
{
    /// <summary>Full path to the current PO plan CSV file, or null if folder is empty or not configured.</summary>
    Task<string?> GetCurrentPoPlanPathAsync(CancellationToken cancellationToken);

    /// <summary>PO Number from the first data row of the current plan file, or null if none.</summary>
    Task<string?> GetCurrentPoNumberAsync(CancellationToken cancellationToken);

    /// <summary>Call when PO End is triggered. Advances to the next file in the folder so the next PO is used for the next batch.</summary>
    Task AdvanceToNextPoAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Sends raw bytes to a network printer over TCP (e.g. port 9100 for ZPL). Used for ZPL-based label printers like Honeywell PD45S.
/// </summary>
public interface INetworkPrinterSender
{
    Task<bool> SendAsync(string host, int port, byte[] data, CancellationToken cancellationToken = default);
}

/// <summary>
/// Provides WIP label fields (Pipe Grade, Size, Thickness, Length, Weight, Type) from the current PO plan file for tag printing.
/// </summary>
public interface IWipLabelProvider
{
    Task<WipLabelInfo?> GetWipLabelAsync(string poNumber, int millNo, CancellationToken cancellationToken = default);
}

/// <summary>
/// Prints an NDT bundle tag (e.g. via ZPL to Honeywell PD45S). Used when a bundle is closed or reprinted after reconcile.
/// </summary>
public interface INdtTagPrinter
{
    Task<bool> PrintBundleTagAsync(InputSlitRecord record, int batchNumber, int totalNdtPcs, bool isReprint, CancellationToken cancellationToken = default);
}

/// <summary>
/// Runtime switch for enabling/disabling ZPL generation and print behavior without changing appsettings.
/// </summary>
public interface IZplGenerationToggle
{
    bool IsEnabled { get; }
    bool SetEnabled(bool enabled);
}

