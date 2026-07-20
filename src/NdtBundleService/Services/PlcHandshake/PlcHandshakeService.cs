using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using NdtBundleService.Services.PlcHandshake.S7;
using S7.Net;
using S7.Net.Types;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Persistent S7 connection and PO-change handshake for one mill:
/// trigger rising edge (FALSE→TRUE) → ack TRUE → wait trigger FALSE (N80: rising edge of ack clears trigger) → ack FALSE.
/// Uses rising-edge detection (same as plc-server) so a held or quickly re-pulsed M-bit does not
/// repeat the PO-end workflow until trigger has been false for several poll cycles.
/// All S7 I/O goes through the shared <see cref="IS7ConnectionProvider"/> for this mill.
/// </summary>
public sealed class PlcHandshakeService
{
    private readonly MillConfig _mill;
    private readonly PlcHandshakeOptions _options;
    private readonly NdtBundleOptions _bundleOptions;
    private readonly IPoChangeHandler _poChangeHandler;
    private readonly PlcPoEndQueue _plcPoEndQueue;
    private readonly PlcHandshakeStatusRegistry _statusRegistry;
    private readonly PlcConnectionHealth _connectionHealth;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IMillHooterPlcValuesService? _hooterValues;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IS7ConnectionProvider _s7;
    private readonly IPlcSlitEndBundleCloser? _slitEndCloser;
    private readonly IHandshakeEventRepository _handshakeEvents;
    private readonly PlcHandshakeEdgeTracker _edge;
    private readonly ILogger<PlcHandshakeService> _logger;

    private bool _handshakeInProgress;
    private bool _ackAwaitingTriggerClear;
    private DateTimeOffset? _idleTriggerTrueSinceUtc;
    private System.DateTime _ackClearDeadlineUtc;
    private volatile bool _plcConnectionEnabled;
    private bool _settingsTestInProgress;
    private bool _suppressNdtUntilPoChange;
    private int _poIdWhenSuppressed;
    private HandshakeAuditSession? _audit;
    private bool _auditStartupRecovery;

    private bool _hooterPulseActive;
    private DateTimeOffset _hooterResetUtc;
    private string? _hooterTrackedPo;
    private int _hooterLastWrittenThreshold = -1;
    private int _hooterLastWrittenAccumulated = -1;
    private bool _hooterAboveThreshold;
    private bool _hooterLoggedPasBlocked;
    private DateTimeOffset _hooterLastPeriodicLogUtc;

    private static readonly TimeSpan HooterStatusLogInterval = TimeSpan.FromSeconds(60);

    public PlcHandshakeService(
        MillConfig mill,
        PlcHandshakeOptions options,
        NdtBundleOptions bundleOptions,
        IPoChangeHandler poChangeHandler,
        PlcPoEndQueue plcPoEndQueue,
        PlcHandshakeStatusRegistry statusRegistry,
        PlcConnectionHealth connectionHealth,
        IActivePoPerMillService activePoPerMill,
        IMillHooterPlcValuesService? hooterValues,
        IWipBundleRunningPoProvider wipRunningPo,
        IS7ConnectionProvider s7,
        ILogger<PlcHandshakeService> logger,
        IPlcSlitEndBundleCloser? slitEndCloser = null,
        IHandshakeEventRepository? handshakeEvents = null)
    {
        _mill = mill;
        _options = options;
        _bundleOptions = bundleOptions;
        _poChangeHandler = poChangeHandler;
        _plcPoEndQueue = plcPoEndQueue;
        _statusRegistry = statusRegistry;
        _connectionHealth = connectionHealth;
        _activePoPerMill = activePoPerMill;
        _hooterValues = hooterValues;
        _wipRunningPo = wipRunningPo;
        _s7 = s7;
        _slitEndCloser = slitEndCloser;
        _handshakeEvents = handshakeEvents ?? NullHandshakeEventRepository.Instance;
        _edge = new PlcHandshakeEdgeTracker(options);
        _logger = logger;
        _plcConnectionEnabled = mill.PlcHandshakeEnabled;

        var millNo = mill.ResolveMillNo();
        _statusRegistry.RegisterMill(millNo, new PlcHandshakeMillStatus
        {
            MillName = mill.Name,
            MillNo = millNo,
            IpAddress = mill.IpAddress.Trim(),
            PlcConnectionEnabled = _plcConnectionEnabled,
            HandshakeState = _plcConnectionEnabled ? "Starting" : "Disconnected (manual)"
        });
    }

    private bool IsS7PoEndHandshakeDisabled() =>
        _options.TelemetryOnly || !_mill.UsesPlcHandshakeForPoEnd(_bundleOptions);

    private string NonPlcPoEndIdleState()
    {
        var source = _mill.ResolvePoEndSource(_bundleOptions);
        return $"Idle (PoEndSource={MillPoEndSourceResolver.ToConfigValue(source)})";
    }

    public bool IsPlcConnectionEnabled => _plcConnectionEnabled;

    /// <summary>Enable or disable S7 connect/reconnect for this mill (manual maintenance).</summary>
    public MillPlcConnectionResult SetPlcConnectionEnabled(bool enabled)
    {
        var millNo = _mill.ResolveMillNo();
        var wasEnabled = _plcConnectionEnabled;
        _plcConnectionEnabled = enabled;

        if (!enabled)
        {
            DisconnectPlc();
            _edge.ResetTriggerEdgeState(reprime: true);
            _ackAwaitingTriggerClear = false;
            _s7.ResetReconnectBackoff();
            UpdateStatus(millNo, s =>
            {
                s.PlcConnectionEnabled = false;
                s.Connected = false;
                s.TriggerActive = false;
                s.AckActive = false;
                s.HandshakeState = "Disconnected (manual)";
                s.LastError = null;
            });
            _logger.LogInformation(
                "{MillName}: PLC handshake connection manually disconnected (S7 slot released).",
                _mill.Name);

            return new MillPlcConnectionResult
            {
                Success = true,
                MillNo = millNo,
                MillName = _mill.Name,
                PlcConnectionEnabled = false,
                Connected = false,
                Message = wasEnabled
                    ? "Mill PLC disconnected. Slit processing and other mills are unaffected."
                    : "Mill PLC was already disconnected."
            };
        }

        UpdateStatus(millNo, s =>
        {
            s.PlcConnectionEnabled = true;
            s.HandshakeState = "Reconnecting";
            s.LastError = null;
        });
        _logger.LogInformation(
            "{MillName}: PLC handshake connection manually enabled; reconnecting.",
            _mill.Name);

        return new MillPlcConnectionResult
        {
            Success = true,
            MillNo = millNo,
            MillName = _mill.Name,
            PlcConnectionEnabled = true,
            Connected = IsPlcConnected(),
            Message = wasEnabled
                ? "Mill PLC connection was already enabled."
                : "Mill PLC connection enabled; reconnecting on next poll cycle."
        };
    }

    private bool IsPlcConnected() => _s7.IsConnected;

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        var millNo = _mill.ResolveMillNo();
        if (IsS7PoEndHandshakeDisabled())
        {
            var source = _mill.ResolvePoEndSource(_bundleOptions);
            _logger.LogInformation(
                "PlcHandshakeService starting for {MillName} at {Host} (S7 telemetry-only: PoEndSource={PoEndSource}, OK/NOK/NDT + line running{Hooter}, poll {Poll}ms).",
                _mill.Name,
                _mill.IpAddress,
                MillPoEndSourceResolver.ToConfigValue(source),
                _mill.Hooter?.Enabled == true ? " + hooter" : string.Empty,
                ResolvePollIntervalMs());
        }
        else
        {
            _logger.LogInformation(
                "PlcHandshakeService starting for {MillName} at {Host} trigger {Trigger} ack {Ack} poll {Poll}ms.",
                _mill.Name,
                _mill.IpAddress,
                _mill.TriggerAddress,
                _mill.AckAddress,
                ResolvePollIntervalMs());
        }

        if (_mill.Hooter?.Enabled == true)
        {
            var h = _mill.Hooter;
            _logger.LogInformation(
                "{MillName}: NDT bundle hooter enabled — MW{AccumWord}/MW{ThresholdWord} compare, Q{Byte}.{Bit} pulse {DurationMs}ms, PAS enable DB{Db}.DBX{Byte}.{Bit}.",
                _mill.Name,
                h.AccumulatedWordOffset,
                h.ThresholdWordOffset,
                h.OutputByte,
                h.OutputBit,
                h.DurationMs,
                h.PasEnableDbNumber,
                h.PasEnableByteOffset,
                h.PasEnableBit);
        }

        _s7.ResetReconnectBackoff();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!_plcConnectionEnabled)
                {
                    DisconnectPlc();
                    UpdateStatus(millNo, s =>
                    {
                        s.PlcConnectionEnabled = false;
                        s.Connected = false;
                        s.HandshakeState = "Disconnected (manual)";
                    });
                    await PollDelayAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                if (!await EnsureConnectedAsync(stoppingToken).ConfigureAwait(false))
                {
                    await DelayReconnectAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await ProcessConnectedPollAsync(millNo, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "{MillName}: handshake poll error; reconnecting.", _mill.Name);
                MarkDisconnected(millNo, ex.Message);
                DisconnectPlc();
                _edge.ResetTriggerEdgeState(reprime: true);
                _ackAwaitingTriggerClear = false;
                _audit = null;
                _auditStartupRecovery = false;
                await DelayReconnectAsync(stoppingToken).ConfigureAwait(false);
                continue;
            }

            await PollDelayAsync(stoppingToken).ConfigureAwait(false);
        }

        DisconnectPlc();
        UpdateStatus(millNo, s =>
        {
            s.Connected = false;
            s.HandshakeState = "Stopped";
        });
        _logger.LogInformation("PlcHandshakeService stopped for {MillName}.", _mill.Name);
    }

    /// <summary>
    /// One connected poll iteration without reconnect/delay. Used by sequence pin tests
    /// (N80 handshake: M40.7 rising edge clears M40.6) and by <see cref="RunAsync"/>.
    /// </summary>
    internal Task ExecuteHandshakePollForTestsAsync(CancellationToken cancellationToken)
    {
        var millNo = _mill.ResolveMillNo();
        return ProcessConnectedPollAsync(millNo, cancellationToken);
    }

    private async Task ProcessConnectedPollAsync(int millNo, CancellationToken stoppingToken)
    {
        if (IsS7PoEndHandshakeDisabled())
        {
            UpdateStatus(millNo, s =>
            {
                s.PlcConnectionEnabled = true;
                s.Connected = true;
                s.LastError = null;
            });

            var telemetryCounts = TryUpdateDb251Counts(millNo);
            await TryCloseOnSlitEndSafeAsync(millNo, telemetryCounts.Ndt, telemetryCounts.SlitId, stoppingToken)
                .ConfigureAwait(false);
            await TryUpdateMillSignalsAsync(millNo, stoppingToken).ConfigureAwait(false);

            _connectionHealth.RecordModbusPoll(true, _statusRegistry.AllConnected());
            SetState(millNo, "Telemetry");
            _edge.MarkPrimedForTelemetry();
            return;
        }

        var trigger = ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit);
        var ack = ReadMerkerBit(_mill.AckByte, _mill.AckBit);

        UpdateStatus(millNo, s =>
        {
            s.PlcConnectionEnabled = true;
            s.Connected = true;
            s.TriggerActive = trigger;
            s.AckActive = ack;
            s.LastError = null;
        });

        var liveCounts = TryUpdateDb251Counts(millNo);
        await TryCloseOnSlitEndSafeAsync(millNo, liveCounts.Ndt, liveCounts.SlitId, stoppingToken)
            .ConfigureAwait(false);

        // Handshake (ack + PO-change edge) before slow hooter/MES resolve so Mill-1 polls stay timely.
        if (_ackAwaitingTriggerClear)
        {
            TryCompleteMesAckSequence(millNo, trigger);
            if (!_ackAwaitingTriggerClear)
                RefreshTriggerAndAck(millNo, out trigger, out ack);
        }

        TryRaiseStuckTriggerAlarm(millNo, trigger);

        if (!_edge.Primed)
        {
            if (!trigger)
            {
                ResetStuckAckIfNeeded(millNo, ack);
                _edge.ArmAfterTriggerClear(isStartup: true);
                SetState(millNo, "Idle");
                _logger.LogDebug(
                    "{MillName}: handshake primed (trigger {Trigger} is false).",
                    _mill.Name,
                    _mill.TriggerAddress);
            }
            else if (_options.RecoverLatchedTriggerAtStartup && !_edge.StartupRecoveryAttempted)
            {
                if (IsS7PoEndHandshakeDisabled())
                {
                    _edge.StartupRecoveryAttempted = true;
                    _edge.LoggedStartupTriggerWait = false;
                    _edge.SetPrevTriggerActive(trigger);
                    _edge.MarkPulseHandled();
                    _edge.ArmAfterTriggerClear(isStartup: false);
                    SetState(millNo, NonPlcPoEndIdleState());
                    _logger.LogInformation(
                        "{MillName}: latched trigger {Trigger} at startup ignored — PoEndSource={PoEndSource}.",
                        _mill.Name,
                        _mill.TriggerAddress,
                        MillPoEndSourceResolver.ToConfigValue(_mill.ResolvePoEndSource(_bundleOptions)));
                }
                else
                {
                    await RunStartupRecoveryAsync(millNo, stoppingToken).ConfigureAwait(false);
                    trigger = ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit);
                }
            }
            else
            {
                SetState(millNo, "WaitingTriggerClear (startup)");
                if (!_edge.LoggedStartupTriggerWait)
                {
                    _edge.LoggedStartupTriggerWait = true;
                    _logger.LogInformation(
                        "{MillName}: trigger {Trigger} already set at startup; waiting for PLC to clear before arming.",
                        _mill.Name,
                        _mill.TriggerAddress);
                }
                else
                {
                    _logger.LogDebug(
                        "{MillName}: trigger {Trigger} still set; waiting for PLC to clear before arming.",
                        _mill.Name,
                        _mill.TriggerAddress);
                }
            }

            _edge.SetPrevTriggerActive(trigger);
            await TryUpdateMillSignalsAsync(
                    millNo,
                    stoppingToken,
                    deferHooterResolve: ShouldDeferHooterResolve())
                .ConfigureAwait(false);
            _connectionHealth.RecordModbusPoll(true, _statusRegistry.AllConnected());
            return;
        }

        var risingEdge = _edge.TryDetectPoChangeRisingEdge(trigger);
        var rearmedBeforePoll = _edge.IsRearmedForNextPoChange();
        _edge.UpdateTriggerEdgeTracking(trigger);

        if (risingEdge &&
            !_handshakeInProgress &&
            !_settingsTestInProgress)
        {
            _edge.MarkPulseHandled();
            if (IsS7PoEndHandshakeDisabled())
            {
                SetState(millNo, NonPlcPoEndIdleState());
                _logger.LogInformation(
                    "{MillName}: PLC PO-change trigger {Trigger} ignored — PoEndSource={PoEndSource}.",
                    _mill.Name,
                    _mill.TriggerAddress,
                    MillPoEndSourceResolver.ToConfigValue(_mill.ResolvePoEndSource(_bundleOptions)));
            }
            else
            {
                await RunHandshakeAsync(millNo, stoppingToken).ConfigureAwait(false);
                RefreshTriggerAndAck(millNo, out trigger, out ack);
            }
        }
        else if (!_handshakeInProgress && !_settingsTestInProgress)
        {
            if (_ackAwaitingTriggerClear)
                SetState(millNo, "WaitingTriggerClear");
            else if (trigger && _edge.PoChangePulseHandled)
                SetState(millNo, "Idle (pulse handled, waiting for trigger clear)");
            else if (trigger && !_edge.PrevTriggerActive && !rearmedBeforePoll)
                SetState(millNo, "Idle (re-arming after last handshake)");
            else
                SetState(millNo, "Idle");
        }

        _edge.SetPrevTriggerActive(trigger);

        if (!risingEdge)
        {
            await TryRecoverIdleLatchedTriggerAsync(millNo, trigger, stoppingToken).ConfigureAwait(false);
            RefreshTriggerAndAck(millNo, out trigger, out ack);
        }

        await TryUpdateMillSignalsAsync(
                millNo,
                stoppingToken,
                deferHooterResolve: ShouldDeferHooterResolve())
            .ConfigureAwait(false);
        _connectionHealth.RecordModbusPoll(true, _statusRegistry.AllConnected());
    }

    private async Task RunHandshakeAsync(int millNo, CancellationToken stoppingToken, bool startupRecovery = false)
    {
        _handshakeInProgress = true;
        try
        {
            await RunHandshakeCoreAsync(millNo, stoppingToken, startupRecovery).ConfigureAwait(false);
        }
        finally
        {
            _handshakeInProgress = false;
        }
    }

    /// <summary>
    /// PO-end trigger was latched before the handshake armed (e.g. service restart). Run workflow + ack once.
    /// </summary>
    private async Task RunStartupRecoveryAsync(int millNo, CancellationToken stoppingToken)
    {
        _edge.BeginStartupRecovery();

        _logger.LogInformation(
            _options.RunPoEndWorkflowOnStartupRecovery
                ? "{MillName}: trigger {Trigger} latched at startup; running recovery handshake (PO end workflow + MES ack)."
                : "{MillName}: trigger {Trigger} latched at startup; clearing PLC latch with MES ack only (PO end workflow skipped).",
            _mill.Name,
            _mill.TriggerAddress);

        SetState(millNo, "StartupRecovery");
        await RunHandshakeAsync(millNo, stoppingToken, startupRecovery: true).ConfigureAwait(false);

        var triggerAfter = ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit);
        var ackAfter = ReadMerkerBit(_mill.AckByte, _mill.AckBit);
        UpdateStatus(millNo, s =>
        {
            s.TriggerActive = triggerAfter;
            s.AckActive = ackAfter;
        });

        if (!triggerAfter)
        {
            _edge.ArmAfterTriggerClear(isStartup: false);
            SetState(millNo, "Idle");
            _logger.LogInformation(
                "{MillName}: startup recovery complete; handshake armed for next PO change.",
                _mill.Name);
        }
        else
        {
            SetState(millNo, "WaitingTriggerClear (startup recovery)");
            _logger.LogWarning(
                "{MillName}: startup recovery finished but trigger {Trigger} is still TRUE; waiting for PLC to clear before arming.",
                _mill.Name,
                _mill.TriggerAddress);
        }
    }

    private void ResetStuckAckIfNeeded(int millNo, bool ack)
    {
        if (!ack)
            return;

        _logger.LogInformation(
            "{MillName}: ack {Ack} was TRUE at startup while trigger {Trigger} is clear; resetting ack to FALSE.",
            _mill.Name,
            _mill.AckAddress,
            _mill.TriggerAddress);

        WriteMerkerBit(_mill.AckByte, _mill.AckBit, false);
        UpdateStatus(millNo, s => s.AckActive = false);
    }

    private Task RunHandshakeCoreAsync(int millNo, CancellationToken stoppingToken, bool startupRecovery = false)
    {
        SetState(millNo, startupRecovery ? "StartupRecovery" : "ProcessingPoChange");

        if (startupRecovery)
        {
            if (_options.RunPoEndWorkflowOnStartupRecovery)
            {
                _logger.LogInformation(
                    "{MillName}: startup recovery handshake on latched {Trigger} (including PO end workflow).",
                    _mill.Name,
                    _mill.TriggerAddress);
            }
            else
            {
                _logger.LogInformation(
                    "{MillName}: startup recovery on latched {Trigger} — MES ack only; PO end workflow not run.",
                    _mill.Name,
                    _mill.TriggerAddress);
            }
        }

        var runPoEndWorkflow = !startupRecovery || _options.RunPoEndWorkflowOnStartupRecovery;
        var correlationId = Guid.NewGuid();
        var detectedAt = DateTimeOffset.UtcNow;

        var poIdVal = 0;
        var ndtFinal = 0;
        if (runPoEndWorkflow)
        {
            try
            {
                poIdVal = ReadDbInt(_options.PoIdByteOffset);
                ndtFinal = ReadNdtCountInt();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "{MillName}: could not read PO/NDT from DB{Db} at PO change start.", _mill.Name, _options.CountsDbNumber);
            }

            _suppressNdtUntilPoChange = true;
            _poIdWhenSuppressed = poIdVal;
            UpdateStatus(millNo, s =>
            {
                s.LastPoEnd = new PlcHandshakeLastPoEnd
                {
                    PoId = poIdVal,
                    NdtCountFinal = ndtFinal,
                    TimestampUtc = detectedAt
                };
                s.NdtCount = 0;
                s.AckWriteFailedAlarm = false;
                s.StuckTriggerAlarm = false;
            });

            BeginHandshakeAudit(millNo, correlationId, detectedAt, poIdVal, ndtFinal, startupRecovery);

            PlcPoEndEdgeProcessor.ProcessDecoupledEdge(
                new PlcPoEndEdgeProcessor.EdgeProcessInput(
                    millNo,
                    _mill.Name,
                    poIdVal,
                    ndtFinal,
                    correlationId,
                    detectedAt,
                    startupRecovery,
                    _mill.TriggerAddress),
                beginAckTrue: () => BeginMesAckSequence(millNo),
                tryEnqueue: _plcPoEndQueue.TryEnqueue,
                _logger);
        }
        else
        {
            BeginHandshakeAudit(millNo, correlationId, detectedAt, poIdVal: 0, ndtFinal: 0, startupRecovery);
            BeginMesAckSequence(millNo);
        }

        return Task.CompletedTask;
    }

    private void BeginMesAckSequence(int millNo)
    {
        SetState(millNo, "AckSent");
        if (!TryWriteMerkerBitWithRetry(_mill.AckByte, _mill.AckBit, value: true, out var err))
        {
            _logger.LogError(
                "{MillName}: ack write TRUE failed after {Retries} attempts — {Error}. CorrelationId {CorrelationId}",
                _mill.Name,
                Math.Max(1, _options.AckWriteRetryCount),
                err,
                _audit?.CorrelationId);
            UpdateStatus(millNo, s =>
            {
                s.AckWriteFailedAlarm = true;
                s.LastError = $"Ack write TRUE failed: {err}";
            });
            FinalizeAudit(HandshakeOutcome.AckWriteFailed, err);
            return;
        }

        UpdateStatus(millNo, s =>
        {
            s.AckActive = true;
            s.AckWriteFailedAlarm = false;
        });
        if (_audit is not null)
        {
            _audit.AckAtUtc = DateTimeOffset.UtcNow;
            PersistAudit();
        }

        _ackAwaitingTriggerClear = true;
        _ackClearDeadlineUtc = System.DateTime.UtcNow.AddMilliseconds(Math.Max(1000, _mill.TriggerClearTimeoutMs));
        SetState(millNo, "WaitingTriggerClear");
    }

    private void TryCompleteMesAckSequence(int millNo, bool trigger)
    {
        if (!_ackAwaitingTriggerClear)
            return;

        if (trigger && System.DateTime.UtcNow < _ackClearDeadlineUtc)
            return;

        var forceDrop = trigger;
        if (forceDrop)
        {
            _logger.LogWarning(
                "{MillName}: trigger {Trigger} still TRUE after {Timeout}ms; resetting ack anyway.",
                _mill.Name,
                _mill.TriggerAddress,
                _mill.TriggerClearTimeoutMs);
        }
        else
        {
            _logger.LogInformation(
                "{MillName}: trigger {Trigger} cleared by PLC.",
                _mill.Name,
                _mill.TriggerAddress);
            if (_audit is not null)
                _audit.ClearedAtUtc = DateTimeOffset.UtcNow;
        }

        SetState(millNo, "AckReset");
        if (!TryWriteMerkerBitWithRetry(_mill.AckByte, _mill.AckBit, value: false, out var err))
        {
            _logger.LogError(
                "{MillName}: ack write FALSE failed after {Retries} attempts — {Error}. CorrelationId {CorrelationId}",
                _mill.Name,
                Math.Max(1, _options.AckWriteRetryCount),
                err,
                _audit?.CorrelationId);
            UpdateStatus(millNo, s =>
            {
                s.AckWriteFailedAlarm = true;
                s.LastError = $"Ack write FALSE failed: {err}";
            });
            FinalizeAudit(HandshakeOutcome.AckWriteFailed, err);
            // Leave _ackAwaitingTriggerClear so next poll retries drop.
            return;
        }

        _ackAwaitingTriggerClear = false;
        UpdateStatus(millNo, s =>
        {
            s.AckActive = false;
            s.TriggerActive = trigger;
            s.StuckTriggerAlarm = false;
            s.AckWriteFailedAlarm = false;
        });

        if (_audit is not null)
            _audit.AckDroppedAtUtc = DateTimeOffset.UtcNow;

        _logger.LogInformation(
            "{MillName}: wrote ack {Ack}=FALSE — handshake complete.",
            _mill.Name,
            _mill.AckAddress);

        var outcome = forceDrop
            ? HandshakeOutcome.TriggerTimeoutForceAckDrop
            : _auditStartupRecovery
                ? HandshakeOutcome.StartupRecoveryCompleted
                : HandshakeOutcome.Completed;
        FinalizeAudit(outcome, error: null);
        // One confirmed FALSE poll completing the handshake is enough to re-arm for the next edge.
        if (!forceDrop)
            _edge.ArmAfterTriggerClear(isStartup: false);
        SetState(millNo, "Idle");
    }

    private bool ShouldDeferHooterResolve() =>
        PlcHandshakeHooterDeferral.ShouldDeferHooterResolve(_ackAwaitingTriggerClear, _handshakeInProgress);

    private async Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken) =>
        await _s7.EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);

    private void DisconnectPlc() => _s7.Disconnect();

    private bool ReadMerkerBit(int byteOffset, int bit) =>
        _s7.Read(ops =>
        {
            var value = ops.Read(DataType.Memory, 0, byteOffset, VarType.Bit, 1, (byte)bit);
            return value is true;
        });

    private void WriteMerkerBit(int byteOffset, int bit, bool value) =>
        _s7.Write(ops => ops.Write(DataType.Memory, 0, byteOffset, value, bit));

    private bool TryWriteMerkerBitWithRetry(int byteOffset, int bit, bool value, out string? error)
    {
        error = null;
        var retries = Math.Max(1, _options.AckWriteRetryCount);
        var backoffMs = Math.Max(0, _options.AckWriteRetryInitialBackoffMs);
        for (var attempt = 1; attempt <= retries; attempt++)
        {
            try
            {
                WriteMerkerBit(byteOffset, bit, value);
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
                if (attempt >= retries)
                    break;
                if (backoffMs > 0)
                    Thread.Sleep(backoffMs * attempt);
            }
        }

        return false;
    }

    private void BeginHandshakeAudit(
        int millNo,
        Guid correlationId,
        DateTimeOffset edgeAt,
        int poIdVal,
        int ndtFinal,
        bool startupRecovery)
    {
        if (!_options.HandshakeAuditEnabled)
        {
            _audit = null;
            _auditStartupRecovery = false;
            return;
        }

        _auditStartupRecovery = startupRecovery;
        _audit = new HandshakeAuditSession
        {
            MillNo = millNo,
            CorrelationId = correlationId,
            EdgeAtUtc = edgeAt,
            PlcPoId = poIdVal,
            PlcNdtCount = ndtFinal,
            Outcome = HandshakeOutcome.InProgress
        };
        PersistAudit();
    }

    private void PersistAudit()
    {
        if (_audit is null || !_options.HandshakeAuditEnabled)
            return;
        _ = _handshakeEvents.UpsertAsync(_audit.ToRecord());
    }

    private void FinalizeAudit(HandshakeOutcome outcome, string? error)
    {
        if (_audit is null)
            return;
        _audit.Outcome = outcome;
        _audit.ErrorMessage = error;
        PersistAudit();
        _audit = null;
        _auditStartupRecovery = false;
    }

    private void TryRaiseStuckTriggerAlarm(int millNo, bool trigger)
    {
        if (_audit is null || !trigger)
            return;

        var limit = Math.Max(0, _options.StuckTriggerAlarmSeconds);
        if ((DateTimeOffset.UtcNow - _audit.EdgeAtUtc).TotalSeconds < limit)
            return;
        if (_audit.StuckTriggerLogged)
            return;

        _audit.StuckTriggerLogged = true;
        _audit.Outcome = HandshakeOutcome.StuckTrigger;
        PersistAudit();
        UpdateStatus(millNo, s => s.StuckTriggerAlarm = true);
        _logger.LogWarning(
            "{MillName}: stuck trigger alarm — {Trigger} TRUE for >{Seconds}s without completed handshake. CorrelationId {CorrelationId}",
            _mill.Name,
            _mill.TriggerAddress,
            limit,
            _audit.CorrelationId);
    }

    /// <summary>
    /// F-6.3 level recovery: M40.6 TRUE while idle (no handshake in progress) longer than
    /// <see cref="PlcHandshakeOptions.StuckTriggerAlarmSeconds"/> is treated like a new edge.
    /// </summary>
    private async Task TryRecoverIdleLatchedTriggerAsync(int millNo, bool trigger, CancellationToken stoppingToken)
    {
        if (!_edge.Primed
            || _handshakeInProgress
            || _settingsTestInProgress
            || _ackAwaitingTriggerClear
            || _audit is not null
            || IsS7PoEndHandshakeDisabled())
        {
            _idleTriggerTrueSinceUtc = null;
            return;
        }

        if (!trigger)
        {
            _idleTriggerTrueSinceUtc = null;
            return;
        }

        _idleTriggerTrueSinceUtc ??= DateTimeOffset.UtcNow;
        var limit = Math.Max(0, _options.StuckTriggerAlarmSeconds);
        if ((DateTimeOffset.UtcNow - _idleTriggerTrueSinceUtc.Value).TotalSeconds < limit)
            return;

        UpdateStatus(millNo, s => s.StuckTriggerAlarm = true);
        _logger.LogWarning(
            "{MillName}: idle stuck trigger recovery — {Trigger} TRUE for >{Seconds}s with no handshake in progress; treating as new PO-end edge.",
            _mill.Name,
            _mill.TriggerAddress,
            limit);

        _idleTriggerTrueSinceUtc = null;
        _edge.MarkPulseHandled();
        await RunHandshakeAsync(millNo, stoppingToken).ConfigureAwait(false);
    }

    private async Task TryCloseOnSlitEndSafeAsync(
        int millNo,
        int liveNdt,
        int liveSlitId,
        CancellationToken stoppingToken)
    {
        if (_slitEndCloser is null)
            return;

        try
        {
            await _slitEndCloser.TryCloseOnSlitEndAsync(millNo, _mill, _s7, liveNdt, liveSlitId, stoppingToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "{MillName}: PLC slit-end close attempt failed.", _mill.Name);
        }
    }

    private sealed class NullHandshakeEventRepository : IHandshakeEventRepository
    {
        public static readonly NullHandshakeEventRepository Instance = new();
        public Task UpsertAsync(HandshakeEventRecord record, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private (int Ndt, int SlitId) TryUpdateDb251Counts(int millNo)
    {
        try
        {
            // Each ReadDbInt is a separate gate entry (no nested provider calls).
            var ok = ReadDbInt(_options.OkCountByteOffset);
            var nok = ReadDbInt(_options.NokCountByteOffset);
            var ndtRaw = ReadNdtCountInt();
            var poId = ReadDbInt(_options.PoIdByteOffset);
            var slitId = ReadDbInt(_options.SlitIdByteOffset);

            var ndtDisplay = ndtRaw;
            if (!IsS7PoEndHandshakeDisabled() && _suppressNdtUntilPoChange)
            {
                if (poId != _poIdWhenSuppressed)
                    _suppressNdtUntilPoChange = false;
                else
                    ndtDisplay = 0;
            }

            UpdateStatus(millNo, s =>
            {
                s.OkCount = ok;
                s.NokCount = nok;
                s.NdtCount = ndtDisplay;
                s.PoId = poId;
                s.SlitId = slitId;
                s.CountsUpdatedUtc = DateTimeOffset.UtcNow;
            });
            return (ndtRaw, slitId);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                ex,
                "{MillName}: DB{Db} count read failed.",
                _mill.Name,
                _options.CountsDbNumber);
            return (0, 0);
        }
    }

    private int ReadDbInt(int byteOffset) =>
        ReadDbInt(_options.CountsDbNumber, byteOffset);

    private int ReadNdtCountInt() =>
        ReadDbInt(_options.NdtCountDb > 0 ? _options.NdtCountDb : _options.CountsDbNumber, _options.NdtCountByteOffset);

    private int ReadDbInt(int dbNumber, int byteOffset) =>
        _s7.Read(ops =>
        {
            var raw = ops.Read(DataType.DataBlock, dbNumber, byteOffset, VarType.Int, 1);
            if (raw is null)
                return 0;
            var v = Convert.ToInt32(raw);
            return v < 0 ? 0 : v;
        });

    private void RefreshTriggerAndAck(int millNo, out bool trigger, out bool ack)
    {
        trigger = ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit);
        ack = ReadMerkerBit(_mill.AckByte, _mill.AckBit);
        var capturedTrigger = trigger;
        var capturedAck = ack;
        UpdateStatus(millNo, s =>
        {
            s.TriggerActive = capturedTrigger;
            s.AckActive = capturedAck;
        });
    }

    private async Task TryUpdateMillSignalsAsync(
        int millNo,
        CancellationToken cancellationToken,
        bool deferHooterResolve = false)
    {
        bool? lineRunning = null;
        int? accumulated = null;
        int? threshold = null;
        var hooterActive = _hooterPulseActive;

        try
        {
            if (_options.ReadLineRunning)
                lineRunning = ReadDbBit(_options.LineRunningDbNumber, _options.LineRunningByteOffset, _options.LineRunningBit);

            var hooterCfg = _mill.Hooter;
            if (hooterCfg?.Enabled == true && _hooterValues != null && !deferHooterResolve)
            {
                var resolved = await _hooterValues.ResolveAsync(millNo, cancellationToken).ConfigureAwait(false);
                SyncHooterMemoryWords(hooterCfg, resolved, forcePoResync: false);
                accumulated = resolved.Accumulated;
                threshold = resolved.Threshold;
                hooterActive = ProcessHooterPulse(hooterCfg, accumulated.Value, threshold.Value);

                var pasEnable = ReadDbBit(
                    hooterCfg.PasEnableDbNumber,
                    hooterCfg.PasEnableByteOffset,
                    hooterCfg.PasEnableBit);
                var qOn = hooterActive || ReadOutputBit(hooterCfg.OutputByte, hooterCfg.OutputBit);
                LogHooterPeriodicStatusIfDue(hooterCfg, accumulated.Value, threshold.Value, pasEnable, qOn, resolved);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "{MillName}: mill signal read/hooter failed.", _mill.Name);
        }

        var capturedLineRunning = lineRunning;
        var capturedAccumulated = accumulated;
        var capturedThreshold = threshold;
        var capturedHooterActive = hooterActive;
        UpdateStatus(millNo, s =>
        {
            if (capturedLineRunning.HasValue)
                s.LineRunning = capturedLineRunning.Value;
            s.AccumulatedValue = capturedAccumulated;
            s.ThresholdValue = capturedThreshold;
            s.HooterActive = capturedHooterActive;
        });
    }

    internal async Task SyncHooterMemoryAfterPoEndAsync(int millNo, CancellationToken cancellationToken)
    {
        var hooterCfg = _mill.Hooter;
        if (hooterCfg?.Enabled != true || _hooterValues == null)
            return;

        try
        {
            if (!_s7.IsConnected)
                return;

            WriteMemoryInt(hooterCfg.AccumulatedWordOffset, 0);
            _hooterLastWrittenAccumulated = 0;
            _hooterTrackedPo = null;

            if (_wipRunningPo.IsWaitingForNewWipAfterPoEnd(millNo))
            {
                WriteMemoryInt(hooterCfg.ThresholdWordOffset, 0);
                _hooterLastWrittenThreshold = 0;
                _logger.LogInformation(
                    "{MillName}: PO end — cleared MW{AccumWord}/MW{ThresholdWord}; waiting for new WIP bundle PO.",
                    _mill.Name,
                    hooterCfg.AccumulatedWordOffset,
                    hooterCfg.ThresholdWordOffset);
                return;
            }

            var resolved = await _hooterValues.ResolveAsync(millNo, cancellationToken).ConfigureAwait(false);
            SyncHooterMemoryWords(hooterCfg, resolved, forcePoResync: true);
            _logger.LogInformation(
                "{MillName}: PO end — rewrote MW{ThresholdWord}={Threshold} (formation chart, PO {PO}, size {Size}) and MW{AccumWord}={Accumulated}.",
                _mill.Name,
                hooterCfg.ThresholdWordOffset,
                resolved.Threshold,
                resolved.PoNumber ?? "(none)",
                resolved.PipeSize ?? "—",
                hooterCfg.AccumulatedWordOffset,
                resolved.Accumulated);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "{MillName}: failed to rewrite MW56/MW58 after PO end.", _mill.Name);
        }
    }

    private void SyncHooterMemoryWords(MillHooterOptions cfg, MillHooterResolvedValues values, bool forcePoResync)
    {
        if (!values.HasPo)
        {
            if (_hooterTrackedPo == null && !forcePoResync)
                return;

            WriteMemoryInt(cfg.ThresholdWordOffset, 0);
            WriteMemoryInt(cfg.AccumulatedWordOffset, 0);
            _hooterTrackedPo = null;
            _hooterLastWrittenThreshold = 0;
            _hooterLastWrittenAccumulated = 0;
            _logger.LogDebug("{MillName}: no running PO; cleared MW{ThresholdWord} and MW{AccumWord}.", _mill.Name, cfg.ThresholdWordOffset, cfg.AccumulatedWordOffset);
            return;
        }

        var poChanged = forcePoResync ||
            !string.Equals(_hooterTrackedPo, values.PoNumber, StringComparison.OrdinalIgnoreCase);

        if (poChanged)
        {
            WriteMemoryInt(cfg.ThresholdWordOffset, values.Threshold);
            WriteMemoryInt(cfg.AccumulatedWordOffset, values.Accumulated);
            _hooterTrackedPo = values.PoNumber;
            _hooterLastWrittenThreshold = values.Threshold;
            _hooterLastWrittenAccumulated = values.Accumulated;
            _logger.LogInformation(
                "{MillName}: wrote MW{ThresholdWord}={Threshold} MW{AccumWord}={Accumulated} for PO {PO} (size {Size}).",
                _mill.Name,
                cfg.ThresholdWordOffset,
                values.Threshold,
                cfg.AccumulatedWordOffset,
                values.Accumulated,
                values.PoNumber,
                values.PipeSize ?? "—");
            return;
        }

        if (values.Threshold != _hooterLastWrittenThreshold)
        {
            WriteMemoryInt(cfg.ThresholdWordOffset, values.Threshold);
            _hooterLastWrittenThreshold = values.Threshold;
            _logger.LogInformation(
                "{MillName}: hooter MW{Word}={Threshold} (formation chart, PO {PO}, size {Size}).",
                _mill.Name,
                cfg.ThresholdWordOffset,
                values.Threshold,
                values.PoNumber,
                values.PipeSize ?? "—");
        }

        if (values.Accumulated != _hooterLastWrittenAccumulated)
        {
            WriteMemoryInt(cfg.AccumulatedWordOffset, values.Accumulated);
            _hooterLastWrittenAccumulated = values.Accumulated;
            _logger.LogInformation(
                "{MillName}: hooter MW{Word}={Accumulated} toward next bundle (PO {PO}, MW{ThresholdWord}={Threshold}).",
                _mill.Name,
                cfg.AccumulatedWordOffset,
                values.Accumulated,
                values.PoNumber,
                cfg.ThresholdWordOffset,
                values.Threshold);
        }
    }

    private void WriteMemoryInt(int wordOffset, int value)
    {
        var clamped = Math.Clamp(value, 0, short.MaxValue);
        _s7.Write(ops => ops.Write(DataType.Memory, 0, wordOffset, (short)clamped));
    }

    /// <summary>
    /// Replicates PLC networks 90–92: SET Q when PAS enable and MW56 &gt; MW58 (and hooter off);
    /// RESET Q after <see cref="MillHooterOptions.DurationMs"/>.
    /// </summary>
    private bool ProcessHooterPulse(MillHooterOptions cfg, int accumulated, int threshold)
    {
        var now = DateTimeOffset.UtcNow;
        var aboveThreshold = accumulated > threshold;
        if (aboveThreshold && !_hooterAboveThreshold)
        {
            _logger.LogInformation(
                "{MillName}: hooter MW{AccumWord}={Accumulated} exceeded MW{ThresholdWord}={Threshold}.",
                _mill.Name,
                cfg.AccumulatedWordOffset,
                accumulated,
                cfg.ThresholdWordOffset,
                threshold);
        }
        else if (!aboveThreshold && _hooterAboveThreshold)
        {
            _logger.LogInformation(
                "{MillName}: hooter MW{AccumWord}={Accumulated} at or below MW{ThresholdWord}={Threshold}.",
                _mill.Name,
                cfg.AccumulatedWordOffset,
                accumulated,
                cfg.ThresholdWordOffset,
                threshold);
        }

        _hooterAboveThreshold = aboveThreshold;

        if (_hooterPulseActive)
        {
            if (now >= _hooterResetUtc)
            {
                WriteOutputBit(cfg.OutputByte, cfg.OutputBit, false);
                _hooterPulseActive = false;
                _logger.LogInformation(
                    "{MillName}: NDT bundle hooter Q{Byte}.{Bit} OFF after {DurationMs}ms pulse (MW{AccumWord}={Accumulated}, MW{ThresholdWord}={Threshold}).",
                    _mill.Name,
                    cfg.OutputByte,
                    cfg.OutputBit,
                    cfg.DurationMs,
                    cfg.AccumulatedWordOffset,
                    accumulated,
                    cfg.ThresholdWordOffset,
                    threshold);
            }

            return _hooterPulseActive;
        }

        if (!aboveThreshold)
        {
            _hooterLoggedPasBlocked = false;
            return false;
        }

        var pasEnable = ReadDbBit(cfg.PasEnableDbNumber, cfg.PasEnableByteOffset, cfg.PasEnableBit);
        if (!pasEnable)
        {
            if (!_hooterLoggedPasBlocked)
            {
                _hooterLoggedPasBlocked = true;
                _logger.LogInformation(
                    "{MillName}: hooter armed (MW{AccumWord}={Accumulated} &gt; MW{ThresholdWord}={Threshold}) but PAS enable DB{Db}.DBX{Byte}.{Bit} is OFF — Q{OutByte}.{OutBit} not pulsed.",
                    _mill.Name,
                    cfg.AccumulatedWordOffset,
                    accumulated,
                    cfg.ThresholdWordOffset,
                    threshold,
                    cfg.PasEnableDbNumber,
                    cfg.PasEnableByteOffset,
                    cfg.PasEnableBit,
                    cfg.OutputByte,
                    cfg.OutputBit);
            }

            return false;
        }

        _hooterLoggedPasBlocked = false;

        if (ReadOutputBit(cfg.OutputByte, cfg.OutputBit))
            return false;

        WriteOutputBit(cfg.OutputByte, cfg.OutputBit, true);
        _hooterPulseActive = true;
        _hooterResetUtc = now.AddMilliseconds(Math.Max(1000, cfg.DurationMs));
        _logger.LogInformation(
            "{MillName}: NDT bundle hooter Q{Byte}.{Bit} ON — MW{AccumWord}={Accumulated} &gt; MW{ThresholdWord}={Threshold} (PAS enable on).",
            _mill.Name,
            cfg.OutputByte,
            cfg.OutputBit,
            cfg.AccumulatedWordOffset,
            accumulated,
            cfg.ThresholdWordOffset,
            threshold);
        return true;
    }

    private void LogHooterPeriodicStatusIfDue(
        MillHooterOptions cfg,
        int accumulated,
        int threshold,
        bool pasEnable,
        bool qOn,
        MillHooterResolvedValues resolved)
    {
        var now = DateTimeOffset.UtcNow;
        if (_hooterLastPeriodicLogUtc != default &&
            now - _hooterLastPeriodicLogUtc < HooterStatusLogInterval)
            return;

        _hooterLastPeriodicLogUtc = now;

        var armed = accumulated > threshold;
        var readyToPulse = armed && pasEnable && !_hooterPulseActive && !qOn;
        _logger.LogInformation(
            "{MillName}: hooter status — PO {PO} size {Size} MW{AccumWord}={Accumulated} MW{ThresholdWord}={Threshold} PAS={PasEnable} Q{Byte}.{Bit}={QOn} armed={Armed} ready={Ready}.",
            _mill.Name,
            resolved.PoNumber ?? "(none)",
            resolved.PipeSize ?? "—",
            cfg.AccumulatedWordOffset,
            accumulated,
            cfg.ThresholdWordOffset,
            threshold,
            pasEnable ? "ON" : "OFF",
            cfg.OutputByte,
            cfg.OutputBit,
            qOn ? "ON" : "OFF",
            armed,
            readyToPulse);
    }

    private bool ReadDbBit(int dbNumber, int byteOffset, int bit) =>
        _s7.Read(ops =>
        {
            var value = ops.Read(DataType.DataBlock, dbNumber, byteOffset, VarType.Bit, 1, (byte)bit);
            return value is true;
        });

    private bool ReadOutputBit(int byteOffset, int bit) =>
        _s7.Read(ops =>
        {
            var value = ops.Read(DataType.Output, 0, byteOffset, VarType.Bit, 1, (byte)bit);
            return value is true;
        });

    private void WriteOutputBit(int byteOffset, int bit, bool value) =>
        _s7.Write(ops => ops.Write(DataType.Output, 0, byteOffset, value, bit));

    private int ResolvePollIntervalMs() =>
        PlcHandshakePollTiming.ResolvePollIntervalMs(_mill, _options);

    private Task PollDelayAsync(CancellationToken cancellationToken) =>
        Task.Delay(ResolvePollIntervalMs(), cancellationToken);

    private async Task DelayReconnectAsync(CancellationToken cancellationToken)
    {
        var millNo = _mill.ResolveMillNo();
        MarkDisconnected(millNo, "S7 connection failed; retrying.");
        _connectionHealth.RecordModbusPoll(true, false, "One or more handshake mills unreachable.");

        var reconnectDelayMs = _s7.TakeReconnectDelayMs();
        _logger.LogWarning(
            "{MillName}: reconnect in {Delay}ms.",
            _mill.Name,
            reconnectDelayMs);

        await Task.Delay(reconnectDelayMs, cancellationToken).ConfigureAwait(false);
    }

    private void MarkDisconnected(int millNo, string? error)
    {
        UpdateStatus(millNo, s =>
        {
            s.Connected = false;
            s.LastError = error;
        });
    }

    private void SetState(int millNo, string state) => UpdateStatus(millNo, s => s.HandshakeState = state);

    private void UpdateStatus(int millNo, Action<PlcHandshakeMillStatus> apply) =>
        _statusRegistry.UpdateMill(millNo, apply);

    /// <summary>
    /// Settings test: read trigger, run PO end workflow, pulse ack on the mill's existing S7 connection.
    /// Does not require a live PLC PO-change event.
    /// </summary>
    public async Task<PlcPoChangeTestResult> RunSettingsTestAsync(CancellationToken cancellationToken)
    {
        var millNo = _mill.ResolveMillNo();
        var steps = new List<string>();

        if (IsS7PoEndHandshakeDisabled())
        {
            var source = _mill.ResolvePoEndSource(_bundleOptions);
            var reason = _options.TelemetryOnly
                ? "PlcHandshake.TelemetryOnly is true (global kill-switch)."
                : $"Mill PoEndSource is {MillPoEndSourceResolver.ToConfigValue(source)} (not Plc).";
            return new PlcPoChangeTestResult
            {
                Success = false,
                MillNo = millNo,
                MillName = _mill.Name,
                Message =
                    $"PO-change handshake is disabled for this mill ({reason}) " +
                    "Use POST /api/Test/po-end for workflow-only simulation.",
                Steps = steps
            };
        }

        if (_handshakeInProgress || _settingsTestInProgress)
        {
            return new PlcPoChangeTestResult
            {
                Success = false,
                MillNo = millNo,
                MillName = _mill.Name,
                Message = "Mill is busy with a PO-change handshake. Try again in a few seconds.",
                Steps = steps
            };
        }

        if (!_plcConnectionEnabled)
        {
            return new PlcPoChangeTestResult
            {
                Success = false,
                MillNo = millNo,
                MillName = _mill.Name,
                Message =
                    "Mill PLC connection is manually disconnected. " +
                    "Use POST /api/Settings/plc/mill/{millNo}/connect before running a PO-change test.",
                Steps = steps
            };
        }

        _settingsTestInProgress = true;
        SetState(millNo, "SettingsTest");

        try
        {
            _logger.LogInformation(
                "[Settings test] {MillName}: PO change test started (trigger {Trigger}, ack {Ack}).",
                _mill.Name,
                _mill.TriggerAddress,
                _mill.AckAddress);
            steps.Add("Test started.");

            if (!await EnsureConnectedAsync(cancellationToken).ConfigureAwait(false))
            {
                steps.Add("S7 connection failed.");
                return new PlcPoChangeTestResult
                {
                    Success = false,
                    MillNo = millNo,
                    MillName = _mill.Name,
                    PlcConnected = false,
                    Message = "Could not connect to the mill PLC over S7. Check IP, rack/slot, and that no other app is blocking connections.",
                    Steps = steps
                };
            }

            steps.Add("S7 connected.");

            var triggerBefore = ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit);
            steps.Add($"Trigger { _mill.TriggerAddress} before test: {(triggerBefore ? "ON" : "OFF")}.");
            _logger.LogInformation(
                "[Settings test] {MillName}: trigger {Trigger} is {State} before test.",
                _mill.Name,
                _mill.TriggerAddress,
                triggerBefore ? "ON" : "OFF");

            string? poNumber = null;
            var poByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
            if (poByMill.TryGetValue(millNo, out var po) && !string.IsNullOrWhiteSpace(po))
            {
                poNumber = po;
                steps.Add($"Resolved PO {po} from latest Input Slit CSV.");
            }
            else
            {
                steps.Add("No PO found in latest Input Slit CSV for this mill; workflow may skip bundle close.");
            }

            steps.Add("Running PO end workflow (HandlePoChangeAsync)…");
            await _poChangeHandler.HandlePoChangeAsync(_mill, cancellationToken).ConfigureAwait(false);
            steps.Add("PO end workflow completed (see logs for bundle/tag details).");

            var pulseMs = Math.Clamp(_mill.PollIntervalMs > 0 ? _mill.PollIntervalMs : _options.PollIntervalMs, 100, 5000);
            steps.Add($"Writing ack { _mill.AckAddress}=TRUE…");
            WriteMerkerBit(_mill.AckByte, _mill.AckBit, true);
            UpdateStatus(millNo, s => s.AckActive = true);
            _logger.LogInformation("[Settings test] {MillName}: wrote ack {Ack}=TRUE.", _mill.Name, _mill.AckAddress);

            await Task.Delay(pulseMs, cancellationToken).ConfigureAwait(false);

            steps.Add($"Writing ack {_mill.AckAddress}=FALSE…");
            WriteMerkerBit(_mill.AckByte, _mill.AckBit, false);
            UpdateStatus(millNo, s => s.AckActive = false);
            _logger.LogInformation("[Settings test] {MillName}: wrote ack {Ack}=FALSE.", _mill.Name, _mill.AckAddress);

            var triggerAfter = ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit);
            steps.Add($"Trigger {_mill.TriggerAddress} after test: {(triggerAfter ? "ON" : "OFF")}.");

            UpdateStatus(millNo, s =>
            {
                s.TriggerActive = triggerAfter;
                s.LastPoChangeUtc = DateTimeOffset.UtcNow;
            });

            _logger.LogInformation("[Settings test] {MillName}: PO change test finished successfully.", _mill.Name);

            return new PlcPoChangeTestResult
            {
                Success = true,
                MillNo = millNo,
                MillName = _mill.Name,
                PlcConnected = true,
                TriggerBefore = triggerBefore,
                TriggerAfter = triggerAfter,
                AckPulsed = true,
                WorkflowInvoked = true,
                PoNumber = poNumber,
                Message = "PO change test completed. Check application logs for workflow and ack details.",
                Steps = steps
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "[Settings test] {MillName}: PO change test failed.", _mill.Name);
            steps.Add($"Error: {ex.Message}");
            return new PlcPoChangeTestResult
            {
                Success = false,
                MillNo = millNo,
                MillName = _mill.Name,
                Message = ex.Message,
                Steps = steps
            };
        }
        finally
        {
            _settingsTestInProgress = false;
            SetState(millNo, "Idle");
        }
    }
}
