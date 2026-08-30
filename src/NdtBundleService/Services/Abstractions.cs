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

    /// <summary>Upserts bundle row with <see cref="BundlePrintStatus.Pending"/> before ZPL print attempt.</summary>
    Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken);

    /// <summary>Same as pending-print upsert on an existing connection/transaction (allocate+insert TX).</summary>
    Task RecordBundlePendingPrintInTxAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        Microsoft.Data.SqlClient.SqlTransaction tx,
        NdtBundleRecord record,
        CancellationToken cancellationToken) =>
        RecordBundlePendingPrintAsync(record, cancellationToken);

    Task<IReadOnlyList<string>> GetOutputSourceFilesForBatchAsync(
        string batchNo,
        CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

    Task<int> RewriteNdtBatchNoInOutputCsvsAsync(
        string oldBatchNo,
        string newBatchNo,
        CancellationToken cancellationToken) =>
        Task.FromResult(0);

    /// <summary>Updates <c>Print_Status</c> and optional <c>Print_Error</c> after a print attempt.</summary>
    Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken);

    /// <summary>Bundles with <c>Print_Status</c> Pending or PrintFailed older than the threshold.</summary>
    Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken);

    Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Batches present in <c>Output_Slit_Row</c> but not yet in <c>NDT_Bundle</c> (accumulating toward print).
    /// </summary>
    Task<IReadOnlyList<NdtBundleRecord>> GetFormingBundlesFromSqlAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());

    /// <summary>Sum of <c>Output_Slit_Row.NDT_Pipes</c> per batch (empty when SQL disabled).</summary>
    Task<IReadOnlyDictionary<string, int>> GetSlitTotalsByBatchAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyDictionary<string, int>>(new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase));

    /// <summary>
    /// <see cref="GetByBatchNoAsync"/> or, when missing, a synthetic row from <c>Output_Slit_Row</c> slit sum.
    /// </summary>
    Task<NdtBundleRecord?> TryGetBundleForReconcileAsync(string batchNo, CancellationToken cancellationToken) =>
        GetByBatchNoAsync(batchNo, cancellationToken);

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
    /// Aligns <c>NDT_Bundle.Total_NDT_Pcs</c> (and bundle summary CSV when present) with the sum of per-slit output rows.
    /// When <paramref name="forceFromSlits"/> is false, only updates rows whose stored total is zero.
    /// Returns the effective total after sync (or the unchanged stored total).
    /// </summary>
    Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken);

    /// <summary>
    /// Removes data rows for the given NDT batch and slit numbers from per-slit output CSVs (excludes NDT_Bundle_*).
    /// Deletes a file when only the header row would remain. Returns rows removed and refs for SQL traceability cleanup.
    /// </summary>
    Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
        string batchNo,
        IReadOnlyList<string> slitNos,
        CancellationToken cancellationToken);

    /// <summary>Latest printed bundle row for a mill (<c>Total_NDT_Pcs &gt; 0</c>), or null when SQL is disabled or no row exists.</summary>
    Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken);

    /// <summary>True when a printed bundle exists for the PO on the given mill.</summary>
    Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken);

    /// <summary>
    /// Best existing bundle for Closed-PO traceability attach (manual-recon locked preferred, then latest printed).
    /// Scoped to PO+mill+pipe size via <c>Bundle_Label</c> when <paramref name="pipeSize"/> is known.
    /// Returns formatted <c>Bundle_No</c> or null when SQL is disabled or no row exists.
    /// </summary>
    Task<string?> TryFindTraceabilityBundleForPoMillAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        CancellationToken cancellationToken) =>
        Task.FromResult<string?>(null);

    /// <summary>
    /// Sets <c>Manual_Review=1</c> on existing <c>NDT_Bundle</c> rows for the PO/mill (additive column).
    /// Returns rows updated; 0 when SQL disabled, column missing, or no matching rows.
    /// </summary>
    Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken);

    /// <summary>
    /// After a PLC-driven close/print, stamp <c>Close_Source='Plc'</c> and ensure fill-to-target
    /// columns are initialized. Never sets <c>Awaiting_Csv_Recon=1</c>.
    /// </summary>
    Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken);

    /// <summary>Legacy FIFO API ? always null after fill-to-target cutover.</summary>
    Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken);

    /// <summary>Legacy FIFO API ? always empty after fill-to-target cutover.</summary>
    Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken);

    /// <summary>Legacy FIFO API ? always null after fill-to-target cutover.</summary>
    Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(
        string poNumber,
        int millNo,
        int slitSum,
        CancellationToken cancellationToken);

    /// <summary>Legacy FIFO API ? always null after fill-to-target cutover.</summary>
    Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(
        string bundleNo,
        int slitSum,
        int reconWindowMinutes,
        DateTime utcNow,
        bool force,
        CancellationToken cancellationToken);

    /// <summary>Legacy FIFO API ? always empty after fill-to-target cutover.</summary>
    Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(
        string poNumber,
        int millNo,
        int reconWindowMinutes,
        DateTime utcNow,
        CancellationToken cancellationToken);

    /// <summary>Legacy FIFO API ? always null after fill-to-target cutover.</summary>
    Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken);

    /// <summary>
    /// True when <c>Manual_Recon=1</c> for the bundle. When SQL is disabled, returns false.
    /// </summary>
    Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult(false);

    /// <summary>
    /// True when <c>Manual_Review=1</c> for the bundle (Closed-PO attach flag). False when SQL is
    /// disabled or the column has not been migrated.
    /// </summary>
    Task<bool> IsManualReviewFlaggedAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult(false);

    /// <summary>
    /// Distinct (normalized slit key, source file basename) pairs from <c>Output_Slit_Row</c> for the
    /// batch. Slit keys use the same normalization as <see cref="GetSlitsForBatchAsync"/> (empty ? "?").
    /// Empty when SQL is disabled ? SAP status enrichment is SQL-only.
    /// </summary>
    Task<IReadOnlyList<(string SlitNo, string SourceFileName)>> GetSlitSourceFileNamesForBatchAsync(
        string batchNo,
        CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<(string, string)>>(Array.Empty<(string, string)>());

    /// <summary>
    /// <c>Output_Slit_Row.NDT_Pipes</c> sums per (batch, normalized slit key) for one source file
    /// basename (Phase 4 resubmit drift detection). Empty when SQL is disabled.
    /// </summary>
    Task<IReadOnlyList<(string BatchNo, string SlitNo, int NdtPipes)>> GetOutputSlitRowSumsForSourceFileAsync(
        string sourceFileBaseName,
        CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<(string, string, int)>>(Array.Empty<(string, string, int)>());

    /// <summary>
    /// Atomically force-finalizes PLC awaiting recon (when applicable), sets manual lock + audit fields,
    /// and writes <c>Total_NDT_Pcs</c>. Returns null when bundle not found or SQL disabled.
    /// </summary>
    Task<ManualBundleReconcileResult?> ManualReconcileBundleAsync(
        string batchNo,
        int correctedTotal,
        string reason,
        string reconciledBy,
        CancellationToken cancellationToken) =>
        Task.FromResult<ManualBundleReconcileResult?>(null);

    /// <summary>
    /// Full recompute of current slit-row sum into <c>Post_Recon_Csv_Sum</c> for locked bundles only.
    /// Never mutates <c>Total_NDT_Pcs</c> or print status.
    /// </summary>
    Task<int> TryUpdatePostReconCsvSumAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult(0);

    /// <summary>
    /// Bundle numbers whose <c>Output_Slit_Row</c> rows span multiple POs or a PO different from the
    /// bundle row ? indicates provisional-stamp collision contamination; operators should verify
    /// before reconciling such bundles.
    /// </summary>
    Task<IReadOnlyList<string>> GetBatchesWithPoMismatchedSlitRowsAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
}

/// <summary>Result of operator manual bundle reconcile (count correction + lock).</summary>
public sealed class ManualBundleReconcileResult
{
    public required NdtBundleRecord Bundle { get; init; }
    public int OriginalTotal { get; init; }
    public int CorrectedTotal { get; init; }
    public bool ForceFinalized { get; init; }
    public bool CountDiscrepancyLogged { get; init; }
    public int SlitSumAtFinalize { get; init; }

    /// <summary><c>Csv_Filled</c> at reconcile time (before/after unchanged by target revision).</summary>
    public int CsvFilledAtReconcile { get; init; }

    /// <summary>True when mid-fill corrected target is below already-stamped CSV count (plan case c).</summary>
    public bool FillOvershootVsCorrectedTarget => CsvFilledAtReconcile > CorrectedTotal;
}

/// <summary>Result of reconciling a late Input Slit sum against a PLC-closed bundle.</summary>
public sealed class PlcCsvReconResult
{
    public string BundleNo { get; init; } = string.Empty;
    public int PlcTotal { get; init; }
    public int SlitSum { get; init; }
    public bool CountDiscrepancy => SlitSum != PlcTotal;
    public bool ManualReviewEscalated { get; init; }
}

public interface IPoPlanProvider
{
    Task<IReadOnlyDictionary<string, PoPlanEntry>> GetPoPlansAsync(CancellationToken cancellationToken);
}

public interface IFormationChartProvider
{
    Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken);

    /// <summary>Clears cached chart after settings save.</summary>
    void InvalidateCache();
}

/// <summary>
/// Provides pipe size per PO Number from an external file (e.g. CSV with PO Number and Pipe Size columns).
/// Used for Formation Chart lookup and size-based bundle logic; Input Slit CSV no longer supplies pipe size.
/// </summary>
public interface IPoPlanWipRepository
{
    Task<bool> IsAvailableAsync(CancellationToken cancellationToken);

    Task<string?> TryGetDataSignatureAsync(CancellationToken cancellationToken);

    Task<IReadOnlyDictionary<string, string>> GetLatestPipeSizeByPoAsync(CancellationToken cancellationToken);

    Task<PoPlanWipSqlSnapshot> GetLatestEnrichmentAsync(CancellationToken cancellationToken);

    Task<PoPlanWipRow?> TryGetLatestByPoAsync(string poNumber, CancellationToken cancellationToken);

    /// <summary>Verifies SQL and <c>dbo.PO_Plan_WIP</c> exist for folder import (does not require <see cref="NdtBundleOptions.PreferSqlForPoPlanWip"/>).</summary>
    Task<bool> EnsureImportReadyAsync(CancellationToken cancellationToken);

    /// <summary>Returns true when this exact file version (<c>path|w:ticks</c>) was already imported.</summary>
    Task<bool> IsImportSourceFilePresentAsync(string sourceFileKey, CancellationToken cancellationToken);

    /// <summary>Inserts one row per PO from a PO Accepted CSV import batch.</summary>
    Task<int> InsertImportRowsAsync(
        string sourceFileKey,
        IEnumerable<PoPlanWipRow> rows,
        CancellationToken cancellationToken);
}

public sealed class PoPlanWipSqlSnapshot
{
    public IReadOnlyDictionary<int, PoPlanWipRow> ByMill { get; init; } =
        new Dictionary<int, PoPlanWipRow>();

    public IReadOnlyDictionary<string, PoPlanWipRow> ByPo { get; init; } =
        new Dictionary<string, PoPlanWipRow>(StringComparer.OrdinalIgnoreCase);

    public string SourceDescription { get; init; } = string.Empty;
}

public interface IPipeSizeProvider
{
    /// <summary>Returns PO Number -> Pipe Size. Empty or missing PO means no size (Default formation used for threshold; no per-size count).</summary>
    Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken);

    /// <summary>Returns the cached map when available; null when the cache has not been built yet.</summary>
    IReadOnlyDictionary<string, string>? TryGetCachedPipeSizes();

    /// <summary>Resolves pipe size for one PO without forcing a full PO-plan folder rescan when SQL cache is warm.</summary>
    Task<string?> TryGetPipeSizeForPoAsync(string poNumber, CancellationToken cancellationToken);
}

/// <summary>Cached PO plan WIP rows for dashboard enrichment (pipe grade, SAP pieces/bundle, etc.).</summary>
public interface IPoPlanWipEnrichmentProvider
{
    Task<PoPlanWipEnrichmentSnapshot> GetEnrichmentAsync(CancellationToken cancellationToken);

    /// <summary>Returns the cached snapshot when available; null when the cache has not been built yet.</summary>
    PoPlanWipEnrichmentSnapshot? TryGetCachedEnrichment();
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
    /// <summary>Returns the allocated mill sequence (0 when skipped for zero pcs).</summary>
    Task<int> WriteBundleAsync(InputSlitRecord contextRecord, int ndtBatchNo, int totalNdtPcs, CancellationToken cancellationToken, Guid? correlationId = null);
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
        CancellationToken cancellationToken,
        string? knownPipeSize = null);

    /// <summary>
    /// Handles PO end signal for the current PO and closes any pending partial bundles.
    /// Callback receives a context record (last seen or synthetic), batch number, and total NDT pcs.
    /// </summary>
    Task HandlePoEndAsync(
        string poNumber,
        int millNo,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken,
        Guid? correlationId = null);

    /// <summary>
    /// PLC close: allocate bundle from live count, zero size accumulation, invoke print callback.
    /// When <paramref name="allowPartial"/> is false (slit-end), skips if count &lt; formation-chart threshold.
    /// When true (PO-end remainder), closes any count &gt; 0.
    /// Caller stamps <c>Close_Source=Plc</c> / <c>Awaiting_Csv_Recon=1</c> after print.
    /// </summary>
    Task CloseBundleFromPlcAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        int plcCount,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken,
        bool allowPartial = false);
}

public interface IPlcClient
{
    /// <summary>
    /// Current PO-end signal level per mill (1?4). Mills without a configured endpoint read as false.
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

/// <summary>Outcome of <see cref="IPoEndWorkflowService.ExecuteAsync"/>.</summary>
public sealed class PoEndWorkflowResult
{
    public string PoNumber { get; init; } = string.Empty;
    public int MillNo { get; init; }
    public int BundlesClosed { get; init; }
    public int TotalNdtPcsClosed { get; init; }
    public bool WaitingForNewWip { get; init; }
    public bool AdvancedPoPlanFile { get; init; }

    /// <summary>True when Plc <c>AfterDrain</c> deferred the partial flush to the drain window.</summary>
    public bool FlushDeferred { get; init; }
}

/// <summary>
/// Runs the same steps as <c>POST /api/Test/po-end</c>: close partial bundles, advance batch state, optional PO plan file advance.
/// </summary>
public interface IPoEndWorkflowService
{
    Task<PoEndWorkflowResult> ExecuteAsync(
        string poNumber,
        int millNo,
        bool advancePoPlanFile,
        CancellationToken cancellationToken,
        Guid? correlationId = null);

    /// <param name="plcNdtCountFinal">DB251 NDT at M40.6 edge ? fallback when sizeCounts/MW56 are empty.</param>
    Task<PoEndWorkflowResult> ExecuteAsync(
        string poNumber,
        int millNo,
        bool advancePoPlanFile,
        CancellationToken cancellationToken,
        Guid? correlationId,
        int? plcNdtCountFinal);
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
/// Running PO for a mill from the latest <c>WIP_MM_?</c> file in the TM Bundle folder.
/// After PO end, waits for a newer WIP file for that mill before returning the next PO.
/// </summary>
public interface IWipBundleRunningPoProvider
{
    Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken);

    /// <summary>Call after PO end workflow completes; mill waits for a new WIP bundle file before the next PO is active.</summary>
    void NotifyPoEndForMill(int millNo, string endedPo);

    /// <summary>True when PO end completed but no qualifying new WIP bundle file has arrived yet for this mill.</summary>
    bool IsWaitingForNewWipAfterPoEnd(int millNo);

    /// <summary>Post?PO-end wait context for slit-file adoption and orphan guards.</summary>
    bool TryGetPoEndWaitContext(int millNo, out bool waitingForNewWip, out string? endedPo);

    /// <summary>
    /// Clears post?PO-end WIP wait for a mill and re-seeds running PO from the latest WIP bundle file.
    /// Use when PO end was triggered in error (e.g. stale PLC latch at service startup).
    /// </summary>
    /// <returns>True when the mill was waiting and has been resumed.</returns>
    bool ResumeRunningWipForMill(int millNo);

    /// <summary>
    /// After file-based PO end workflow, set the WIP file that triggered the PO change as the new running PO.
    /// </summary>
    bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName);
}

/// <summary>Live NDT pipe counter from the mill Siemens PLC (data block INT).</summary>
public interface IMillNdtCountReader
{
    /// <summary>Null when disabled, misconfigured, or read failed.</summary>
    Task<int?> TryReadNdtPipesCountAsync(CancellationToken cancellationToken);
}

/// <summary>
/// MillSlitLive PLC counter tracking (legacy / diagnostics). Bundle CSV and tags use Input Slit CSV slit totals per
/// <see cref="SlitMonitoringWorker"/>; PO end clears any cached PLC baseline via <see cref="OnPoEndForMill"/>.
/// </summary>
public interface IMillSlitLiveNdtAccumulator
{
    /// <summary>
    /// Legacy hook: PLC deltas for bundle splitting (no longer used by <see cref="SlitMonitoringWorker"/> for tags/CSV batch).
    /// </summary>
    IReadOnlyList<int>? TryConsumeRawForBundleIncrements(string normalizedPoNumber, int millNo, int plcRawNdt);

    /// <summary>Clears the PLC baseline for this PO/mill after PO end so counts do not carry incorrectly.</summary>
    void OnPoEndForMill(string normalizedPoNumber, int millNo);
}

/// <summary>
/// Running NDT total per (PO, Mill) for file-close bookkeeping. Does not allocate batch numbers ?
/// close + fill-to-target own numbering.
/// </summary>
public interface INdtBatchStateService
{
    /// <summary>
    /// Updates running total for (poNumber, millNo) by ndtPipes. Returns batch number 0 (unused under
    /// fill-to-target), the new total so far, and the formation-chart threshold.
    /// </summary>
    Task<(int BatchNumber, int TotalSoFar, int Threshold)> GetBatchForRecordAsync(
        string poNumber,
        int millNo,
        int ndtPipes,
        CancellationToken cancellationToken,
        string? knownPipeSize = null);

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

/// <summary>Outcome of sending raw bytes to a network printer.</summary>
public readonly record struct PrinterSendResult(bool Success, string? ErrorDetail = null);

/// <summary>
/// Sends raw bytes to a network printer over TCP (e.g. port 9100 for ZPL). Used for ZPL-based label printers like Honeywell PD45S.
/// </summary>
public interface INetworkPrinterSender
{
    Task<PrinterSendResult> SendAsync(string host, int port, byte[] data, CancellationToken cancellationToken = default);
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

/// <summary>Reprints an NDT bundle tag with the existing "Reprint" ZPL marker and updates print status.</summary>
public interface IReconcileBundleTagService
{
    Task<ReconcileBundleTagPrintResult> ReprintAsync(NdtBundleRecord bundle, CancellationToken cancellationToken);
}

public sealed class ReconcileBundleTagPrintResult
{
    public bool Success { get; init; }
    public string? Message { get; init; }
    public string? ErrorDetail { get; init; }
}

/// <summary>
/// Runtime switch for enabling/disabling ZPL generation and print behavior without changing appsettings.
/// </summary>
public interface IZplGenerationToggle
{
    bool IsEnabled { get; }
    bool SetEnabled(bool enabled);
}

