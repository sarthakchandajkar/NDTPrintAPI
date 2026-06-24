using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public interface IReconcileSyncService
{
    /// <summary>After bundle total reconcile: NDT process CSV, consolidated SQL, and per-slit output SQL.</summary>
    Task SyncAfterBundleTotalReconcileAsync(
        string ndtBatchNo,
        string poNumber,
        int newBundleTotalPcs,
        CancellationToken cancellationToken);

    /// <summary>After per-slit reconcile: align Output_Slit_Row for that slit.</summary>
    Task<int> SyncAfterSlitReconcileAsync(
        string ndtBatchNo,
        string slitNo,
        int newNdtPipes,
        CancellationToken cancellationToken);

    /// <summary>After Visual/Hydro/Revisual manual reconcile: SQL station row, consolidated SQL, bundle total on Revisual.</summary>
    Task SyncAfterManualStationReconcileAsync(
        ManualTagStation station,
        ManualStationReconcileSnapshot snapshot,
        string? ndtProcessCsvPath,
        CancellationToken cancellationToken);
}

public sealed class ManualStationReconcileSnapshot
{
    public string PoNumber { get; init; } = string.Empty;
    public string NdtBatchNo { get; init; } = string.Empty;
    public int InitialPcs { get; init; }
    public int VisualRejected { get; init; }
    public int HydroRejected { get; init; }
    public int RevisualOk { get; init; }
    public int RevisualRejected { get; init; }
    public bool RevisualInvalidated { get; init; }
    public DateTime? VisualStart { get; init; }
    public DateTime? RevisualEnd { get; init; }
    public bool HasVisual { get; init; }
    public bool HasHydro { get; init; }
    public bool HasRevisual { get; init; }
}

public sealed class ReconcileSyncService : IReconcileSyncService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ITraceabilityRepository _traceability;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly ILogger<ReconcileSyncService> _logger;

    public ReconcileSyncService(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        ITraceabilityRepository traceability,
        INdtBundleRepository bundleRepository,
        ILogger<ReconcileSyncService> logger)
    {
        _optionsMonitor = optionsMonitor;
        _traceability = traceability;
        _bundleRepository = bundleRepository;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;

    public async Task SyncAfterBundleTotalReconcileAsync(
        string ndtBatchNo,
        string poNumber,
        int newBundleTotalPcs,
        CancellationToken cancellationToken)
    {
        var batch = ndtBatchNo.Trim();
        var po = poNumber.Trim();

        var csvPath = await NdtProcessCsvReconcileHelper.TryUpdateOkForBatchAsync(
            Opt,
            batch,
            newBundleTotalPcs,
            visualReject: null,
            hydroReject: null,
            revisualReject: null,
            _logger,
            cancellationToken).ConfigureAwait(false);

        var metrics = NdtProcessCsvReconcileHelper.TryReadMetricsForBatch(Opt, batch);
        var visualRej = metrics?.VisualReject ?? 0;
        var hydroRej = metrics?.HydroReject ?? 0;
        var revisualRej = metrics?.RevisualReject ?? 0;
        var ndtPcs = metrics?.NdtPcs ?? newBundleTotalPcs;
        if (string.IsNullOrWhiteSpace(po) && metrics.HasValue && !string.IsNullOrWhiteSpace(metrics.Value.Po))
            po = metrics.Value.Po!;

        await _traceability.UpdateNdtProcessConsolidatedFromStationsAsync(
            po,
            batch,
            ndtPcs,
            okPcs: newBundleTotalPcs,
            visualRej,
            hydroRej,
            revisualRej,
            bundleStart: null,
            bundleEnd: null,
            outputFilePath: csvPath,
            cancellationToken).ConfigureAwait(false);

        await _traceability.SyncOutputSlitRowsFromPerSlitCsvForBatchAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> SyncAfterSlitReconcileAsync(
        string ndtBatchNo,
        string slitNo,
        int newNdtPipes,
        CancellationToken cancellationToken)
    {
        var sqlRows = await _traceability.UpdateOutputSlitRowNdtPipesByBatchAndSlitAsync(
            ndtBatchNo.Trim(),
            slitNo,
            newNdtPipes,
            cancellationToken).ConfigureAwait(false);

        await _traceability.SyncOutputSlitRowsFromPerSlitCsvForBatchAsync(ndtBatchNo.Trim(), cancellationToken)
            .ConfigureAwait(false);

        return sqlRows;
    }

    public async Task SyncAfterManualStationReconcileAsync(
        ManualTagStation station,
        ManualStationReconcileSnapshot snapshot,
        string? ndtProcessCsvPath,
        CancellationToken cancellationToken)
    {
        var batch = snapshot.NdtBatchNo.Trim();
        var po = snapshot.PoNumber.Trim();

        var visualRej = snapshot.HasVisual ? snapshot.VisualRejected : 0;
        var hydroRej = snapshot.HasHydro ? snapshot.HydroRejected : 0;
        var revisualRej = snapshot.HasRevisual && !snapshot.RevisualInvalidated ? snapshot.RevisualRejected : 0;
        var okPcs = snapshot.HasRevisual && !snapshot.RevisualInvalidated
            ? snapshot.RevisualOk
            : 0;

        if (station == ManualTagStation.Revisual && snapshot.HasRevisual)
        {
            await _bundleRepository.UpdateBundlePipesAsync(batch, snapshot.RevisualOk, cancellationToken).ConfigureAwait(false);
            await _bundleRepository.UpdateBundleSummaryCsvAsync(batch, snapshot.RevisualOk, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (station is ManualTagStation.Visual or ManualTagStation.Hydrotesting or ManualTagStation.FourHeadHydrotesting or ManualTagStation.BigHydrotesting)
        {
            await NdtProcessCsvReconcileHelper.TryUpdateOkForBatchAsync(
                Opt,
                batch,
                okPcs,
                visualReject: snapshot.HasVisual ? visualRej : null,
                hydroReject: snapshot.HasHydro ? hydroRej : null,
                revisualReject: snapshot.HasRevisual && !snapshot.RevisualInvalidated ? revisualRej : null,
                _logger,
                cancellationToken).ConfigureAwait(false);
        }

        await _traceability.UpdateNdtProcessConsolidatedFromStationsAsync(
            po,
            batch,
            snapshot.InitialPcs,
            okPcs,
            visualRej,
            hydroRej,
            revisualRej,
            snapshot.VisualStart,
            snapshot.RevisualEnd,
            ndtProcessCsvPath,
            cancellationToken).ConfigureAwait(false);
    }
}
