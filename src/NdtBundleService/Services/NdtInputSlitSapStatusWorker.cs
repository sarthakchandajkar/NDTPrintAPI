using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Polls the NDT Input Slit pending/Accepted/Rejected folders and records per-file SAP status
/// (Pending → Accepted | Rejected; Rejected → Pending on operator resubmit) in
/// <c>Output_Slit_Sap_Status</c>. Observation never moves, deletes, or gates anything. On a
/// Resubmit transition (Phase 4) it additionally runs <see cref="IResubmitDriftService"/> to
/// re-sync <c>Output_Slit_Row</c> and bundle totals with the operator-edited file. Polling (not
/// FileSystemWatcher) for UNC reliability, mirroring <see cref="SlitMonitoringWorker"/>.
/// See docs/NDT_Input_Slit_SAP_Status_Design.md.
/// </summary>
public sealed class NdtInputSlitSapStatusWorker : BackgroundService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly IOutputSlitSapStatusRepository _repository;
    private readonly IResubmitDriftService _resubmitDrift;
    private readonly ILogger<NdtInputSlitSapStatusWorker> _logger;

    private bool _loggedNotConfigured;
    private bool _loggedStarted;
    private bool _loggedFolderOverlap;

    public NdtInputSlitSapStatusWorker(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IOutputSlitSapStatusRepository repository,
        IResubmitDriftService resubmitDrift,
        ILogger<NdtInputSlitSapStatusWorker> logger)
    {
        _optionsMonitor = optionsMonitor;
        _repository = repository;
        _resubmitDrift = resubmitDrift;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PollOnceAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NDT Input Slit SAP status poll failed.");
            }

            var o = _optionsMonitor.CurrentValue;
            var delaySeconds = Math.Max(
                1,
                o.NdtInputSlitSapStatusPollSeconds > 0 ? o.NdtInputSlitSapStatusPollSeconds : o.PollIntervalSeconds);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task PollOnceAsync(CancellationToken cancellationToken)
    {
        var o = _optionsMonitor.CurrentValue;
        var pendingFolder = (o.OutputBundleFolder ?? string.Empty).Trim();
        var acceptedFolder = (o.NdtInputSlitAcceptedFolder ?? string.Empty).Trim();
        var rejectedFolder = (o.NdtInputSlitRejectedFolder ?? string.Empty).Trim();

        if (!_repository.Enabled
            || (string.IsNullOrEmpty(acceptedFolder) && string.IsNullOrEmpty(rejectedFolder)))
        {
            if (!_loggedNotConfigured)
            {
                _loggedNotConfigured = true;
                _logger.LogInformation(
                    "NDT Input Slit SAP status watcher idle: {Reason}. Set NdtBundle:NdtInputSlitAcceptedFolder / NdtInputSlitRejectedFolder and enable SQL traceability to activate.",
                    !_repository.Enabled ? "SQL traceability disabled" : "no Accepted/Rejected folder configured");
            }

            return;
        }

        // Never-write invariant: this system only ever writes into the pending folder. If the
        // Accepted/Rejected paths alias the pending folder (misconfiguration), every derived status
        // would be wrong and seed-on-write would touch a "SAP-owned" folder — refuse to observe.
        if (FoldersOverlap(pendingFolder, acceptedFolder)
            || FoldersOverlap(pendingFolder, rejectedFolder)
            || FoldersOverlap(acceptedFolder, rejectedFolder))
        {
            if (!_loggedFolderOverlap)
            {
                _loggedFolderOverlap = true;
                _logger.LogError(
                    "NDT Input Slit SAP status watcher disabled: pending/Accepted/Rejected folders must be distinct. "
                    + "Pending: {Pending}; Accepted: {Accepted}; Rejected: {Rejected}.",
                    pendingFolder,
                    acceptedFolder,
                    rejectedFolder);
            }

            return;
        }

        _loggedFolderOverlap = false;

        if (!_loggedStarted)
        {
            _loggedStarted = true;
            _logger.LogInformation(
                "NDT Input Slit SAP status watcher active. Pending: {Pending}; Accepted: {Accepted}; Rejected: {Rejected}.",
                string.IsNullOrEmpty(pendingFolder) ? "(not set)" : pendingFolder,
                string.IsNullOrEmpty(acceptedFolder) ? "(not set)" : acceptedFolder,
                string.IsNullOrEmpty(rejectedFolder) ? "(not set)" : rejectedFolder);
        }

        var minUtc = SourceFileEligibility.ParseMinUtc(o);
        var presence = new Dictionary<string, FolderPresence>(StringComparer.OrdinalIgnoreCase);

        // A cycle is applied only when every configured folder enumerates successfully; a partial
        // view (e.g. Accepted share briefly unreachable) must not produce misleading observations.
        if (!TryScanFolder(pendingFolder, minUtc, presence, static (p, lw) => { p.InPending = true; p.PendingLastWriteUtc = lw; })
            || !TryScanFolder(acceptedFolder, minUtc, presence, static (p, lw) => { p.InAccepted = true; p.AcceptedLastWriteUtc = lw; })
            || !TryScanFolder(rejectedFolder, minUtc, presence, static (p, lw) => { p.InRejected = true; p.RejectedLastWriteUtc = lw; }))
        {
            return;
        }

        if (presence.Count == 0)
            return;

        var observations = new List<OutputSlitSapStatusObservation>(presence.Count);
        foreach (var (fileName, p) in presence)
        {
            var status = OutputSlitSapStatusPolicy.DeriveObservedStatus(p.InPending, p.InAccepted, p.InRejected);
            var (folder, lw) = status switch
            {
                OutputSlitSapStatus.Accepted => (acceptedFolder, p.AcceptedLastWriteUtc),
                OutputSlitSapStatus.Rejected => (rejectedFolder, p.RejectedLastWriteUtc),
                _ => (pendingFolder, p.PendingLastWriteUtc)
            };

            observations.Add(new OutputSlitSapStatusObservation(
                fileName,
                status,
                folder,
                lw,
                OutputSlitSapStatusObservation.SourceWatcher));
        }

        var result = await _repository.ApplyObservationsAsync(observations, cancellationToken).ConfigureAwait(false);
        if (result.Changed > 0)
        {
            _logger.LogInformation(
                "NDT Input Slit SAP status: {Changed} status change(s) across {Files} observed file(s).",
                result.Changed,
                observations.Count);
        }
        else
        {
            _logger.LogDebug(
                "NDT Input Slit SAP status: no changes across {Files} observed file(s).",
                observations.Count);
        }

        // Phase 4: a Rejected → Pending transition means an operator dropped an edited copy back
        // into the pending folder. That file is authoritative for what SAP will post — diff it
        // against Output_Slit_Row and re-sync SQL/bundle totals (Manual_Recon lock respected).
        if (!string.IsNullOrEmpty(pendingFolder))
        {
            foreach (var fileName in result.ResubmittedFiles)
            {
                try
                {
                    await _resubmitDrift
                        .DetectAndReconcileAsync(pendingFolder, fileName, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Resubmit drift detection failed for {File}.", fileName);
                }
            }
        }
    }

    /// <summary>
    /// Enumerates one folder into <paramref name="presence"/>. Empty/missing folders are fine
    /// (returns true); an enumeration failure on a configured, existing path aborts the cycle.
    /// </summary>
    private bool TryScanFolder(
        string folder,
        DateTime? minUtc,
        Dictionary<string, FolderPresence> presence,
        Action<FolderPresence, DateTime> apply)
    {
        if (string.IsNullOrEmpty(folder))
            return true;

        try
        {
            if (!Directory.Exists(folder))
                return true;

            foreach (var path in InputSlitInboxEnumeration.EnumerateFiles(folder))
            {
                DateTime lwUtc;
                try
                {
                    lwUtc = File.GetLastWriteTimeUtc(path);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "SAP status scan: could not read LastWriteTimeUtc for {File}.", path);
                    continue;
                }

                if (!SourceFileEligibility.IncludeFileUtc(lwUtc, minUtc))
                    continue;

                var name = Path.GetFileName(path);
                if (string.IsNullOrEmpty(name))
                    continue;

                if (!presence.TryGetValue(name, out var p))
                {
                    p = new FolderPresence();
                    presence[name] = p;
                }

                apply(p, lwUtc);
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SAP status scan failed for folder {Folder}; skipping this cycle.", folder);
            return false;
        }
    }

    /// <summary>Case-insensitive path equality ignoring trailing separators; empty paths never overlap.</summary>
    internal static bool FoldersOverlap(string? a, string? b)
    {
        if (string.IsNullOrWhiteSpace(a) || string.IsNullOrWhiteSpace(b))
            return false;

        static string Normalize(string p) =>
            p.Trim().TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        return string.Equals(Normalize(a), Normalize(b), StringComparison.OrdinalIgnoreCase);
    }

    private sealed class FolderPresence
    {
        public bool InPending;
        public DateTime PendingLastWriteUtc;
        public bool InAccepted;
        public DateTime AcceptedLastWriteUtc;
        public bool InRejected;
        public DateTime RejectedLastWriteUtc;
    }
}
