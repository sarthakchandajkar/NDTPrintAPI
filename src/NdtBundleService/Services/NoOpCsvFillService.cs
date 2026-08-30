namespace NdtBundleService.Services;

/// <summary>No-op fill service for unit tests that do not exercise SQL fill.</summary>
public class NoOpCsvFillService : ICsvFillService
{
    public static readonly NoOpCsvFillService Instance = new();

    public virtual Task TryInitializeFillTargetAsync(
        string bundleNo,
        int targetNdtPcs,
        string? closeSource,
        CancellationToken cancellationToken) =>
        Task.CompletedTask;

    public virtual Task<CsvFillIncompleteBundle?> TryGetOldestIncompleteAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        CancellationToken cancellationToken) =>
        Task.FromResult<CsvFillIncompleteBundle?>(null);

    public virtual Task<bool> HasTerminalFillRowAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken) =>
        Task.FromResult(false);

    public virtual Task<CsvFillStampResult?> TryStampFileAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        int fileNdtPipes,
        CancellationToken cancellationToken) =>
        Task.FromResult<CsvFillStampResult?>(null);

    public virtual Task<int> AdvanceQuietShortAsync(
        string? poNumber,
        int? millNo,
        int quietMinutes,
        DateTime utcNow,
        bool forcePoEnd,
        CancellationToken cancellationToken) =>
        Task.FromResult(0);

    public virtual Task UpsertHoldAsync(
        string sourceFileName,
        string poNumber,
        int millNo,
        string? pipeSize,
        string reasonCode,
        CancellationToken cancellationToken) =>
        Task.CompletedTask;

    public virtual Task<int> EscalateExpiredHoldsAsync(
        int quietMinutes,
        DateTime utcNow,
        CancellationToken cancellationToken,
        int? millNo = null) =>
        Task.FromResult(0);

    public virtual Task ApplyCountRevisionAsync(
        string sourceFileName,
        string batchNo,
        int oldNdtPipes,
        int newNdtPipes,
        CancellationToken cancellationToken) =>
        Task.CompletedTask;

    public virtual Task<Guid> ApplyBatchMoveAsync(
        string sourceFileName,
        string oldBatchNo,
        string newBatchNo,
        int ndtPipes,
        CancellationToken cancellationToken) =>
        Task.FromResult(Guid.Empty);

    public virtual Task<bool> HasAwaitingCsvReconRowsAsync(CancellationToken cancellationToken, int? millNo = null) =>
        Task.FromResult(false);

    public virtual Task<bool> HasBundlesMissingFillTargetAsync(CancellationToken cancellationToken, int? millNo = null) =>
        Task.FromResult(false);
}
