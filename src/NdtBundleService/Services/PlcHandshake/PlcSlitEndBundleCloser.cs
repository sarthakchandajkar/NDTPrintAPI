using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.S7;
using S7.Net;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Detects slit-end and closes bundles from live PLC NDT count when <c>CloseTrigger</c> allows.
/// Default: Slit ID change (DB251.DBW10, same value shown on Mills PLC). Optional merker override via config.
/// </summary>
public interface IPlcSlitEndBundleCloser
{
    /// <param name="liveNdtCount">Current DB251 NDT INT from this poll.</param>
    /// <param name="liveSlitId">Current DB251 Slit ID INT (DBW10) from this poll.</param>
    Task TryCloseOnSlitEndAsync(
        int millNo,
        MillConfig mill,
        IS7ConnectionProvider s7,
        int liveNdtCount,
        int liveSlitId,
        CancellationToken cancellationToken);
}

public sealed class PlcSlitEndBundleCloser : IPlcSlitEndBundleCloser
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly IActivePoPerMillService _activePo;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IFormationChartProvider _formationChart;
    private readonly IMillBundleStateLock _millLock;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly ILogger<PlcSlitEndBundleCloser> _logger;

    private readonly Dictionary<int, int> _prevNdtByMill = new();
    private readonly Dictionary<int, int> _prevSlitIdByMill = new();
    private readonly HashSet<int> _slitIdPrimedByMill = new();
    private readonly Dictionary<int, bool> _prevSlitEndBitByMill = new();
    private readonly HashSet<int> _closedThisSlitByMill = new();

    public PlcSlitEndBundleCloser(
        IOptions<NdtBundleOptions> options,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        IActivePoPerMillService activePo,
        IPipeSizeProvider pipeSizeProvider,
        IFormationChartProvider formationChart,
        IMillBundleStateLock millLock,
        INdtBundleRepository bundleRepository,
        ILogger<PlcSlitEndBundleCloser> logger)
    {
        _options = options;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _activePo = activePo;
        _pipeSizeProvider = pipeSizeProvider;
        _formationChart = formationChart;
        _millLock = millLock;
        _bundleRepository = bundleRepository;
        _logger = logger;
    }

    public async Task TryCloseOnSlitEndAsync(
        int millNo,
        MillConfig mill,
        IS7ConnectionProvider s7,
        int liveNdtCount,
        int liveSlitId,
        CancellationToken cancellationToken)
    {
        var bundleOpts = _options.Value;
        var handshake = bundleOpts.PlcHandshake ?? new PlcHandshakeOptions();
        var trigger = BundleCloseTriggerParser.Parse(bundleOpts.CloseTrigger);
        if (!BundleClosePolicy.AllowPlcClose(trigger, s7.IsHealthy))
            return;

        if (!TryDetectSlitEnd(millNo, handshake, s7, liveNdtCount, liveSlitId, out var edgeReason, out var plcCountForClose))
            return;

        if (_closedThisSlitByMill.Contains(millNo))
            return;

        var formation = await _formationChart.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var poByMill = await _activePo.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        if (!poByMill.TryGetValue(millNo, out var po) || string.IsNullOrWhiteSpace(po))
        {
            _logger.LogDebug("{MillName}: slit-end detected ({Reason}) but no active PO; skip PLC close.", mill.Name, edgeReason);
            return;
        }

        var poNorm = InputSlitCsvParsing.NormalizePo(po.Trim());
        string? pipeSize = null;
        try
        {
            pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(poNorm, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            /* use default threshold */
        }

        var threshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);
        if (plcCountForClose < threshold)
        {
            _logger.LogDebug(
                "{MillName}: slit-end ({Reason}) plcCount={Count} < threshold={Threshold}; skip close.",
                mill.Name,
                edgeReason,
                plcCountForClose,
                threshold);
            return;
        }

        using (await _millLock.AcquireAsync(millNo, cancellationToken).ConfigureAwait(false))
        {
            if (_closedThisSlitByMill.Contains(millNo))
                return;

            await _bundleEngine.CloseBundleFromPlcAsync(
                poNorm,
                millNo,
                pipeSize,
                plcCountForClose,
                async (contextRecord, batchNo, totalNdtPcs) =>
                {
                    if (totalNdtPcs <= 0)
                        return;

                    await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken)
                        .ConfigureAwait(false);
                    await _bundleRepository.TrySetPlcCloseMetadataAsync(batchNo, contextRecord.MillNo, cancellationToken)
                        .ConfigureAwait(false);

                    _logger.LogInformation(
                        "{MillName}: PLC slit-end close printed bundle sequence {BatchNo} ({Total} pcs) — {Reason}.",
                        mill.Name,
                        batchNo,
                        totalNdtPcs,
                        edgeReason);
                },
                cancellationToken).ConfigureAwait(false);

            _closedThisSlitByMill.Add(millNo);
        }
    }

    /// <summary>
    /// Detects slit end. Prefer optional merker rising edge when configured;
    /// otherwise Slit ID (DB251.DBW10) change means the previous slit finished.
    /// </summary>
    internal bool TryDetectSlitEnd(
        int millNo,
        PlcHandshakeOptions handshake,
        IS7ConnectionProvider s7,
        int liveNdtCount,
        int liveSlitId,
        out string reason,
        out int plcCountForClose)
    {
        reason = "";
        plcCountForClose = liveNdtCount;

        if (handshake.SlitEndTriggerByte >= 0)
        {
            var bit = Math.Clamp(handshake.SlitEndTriggerBit, 0, 7);
            var active = s7.Read(ops =>
            {
                var value = ops.Read(DataType.Memory, 0, handshake.SlitEndTriggerByte, VarType.Bit, 1, (byte)bit);
                return value is true;
            });

            _prevSlitEndBitByMill.TryGetValue(millNo, out var prev);
            _prevSlitEndBitByMill[millNo] = active;
            if (active && !prev)
            {
                _closedThisSlitByMill.Remove(millNo);
                reason = $"M{handshake.SlitEndTriggerByte}.{bit} rising edge";
                plcCountForClose = liveNdtCount;
                return true;
            }

            if (!active)
                _closedThisSlitByMill.Remove(millNo);
            return false;
        }

        // Default: Mills PLC Slit ID (DB251.DBW10) changed → previous slit finished.
        // Use the NDT count from the previous poll (before the PLC resets/rolls to the new slit).
        _prevSlitIdByMill.TryGetValue(millNo, out var prevSlitId);
        _prevNdtByMill.TryGetValue(millNo, out var prevNdt);

        if (!_slitIdPrimedByMill.Contains(millNo))
        {
            _prevSlitIdByMill[millNo] = liveSlitId;
            _prevNdtByMill[millNo] = liveNdtCount;
            _slitIdPrimedByMill.Add(millNo);
            return false;
        }

        if (liveSlitId != prevSlitId)
        {
            _closedThisSlitByMill.Remove(millNo);
            reason = $"Slit ID change ({prevSlitId}→{liveSlitId})";
            plcCountForClose = prevNdt;
            _prevSlitIdByMill[millNo] = liveSlitId;
            _prevNdtByMill[millNo] = liveNdtCount;
            return true;
        }

        _prevNdtByMill[millNo] = liveNdtCount;
        return false;
    }
}
