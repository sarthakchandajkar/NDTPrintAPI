using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using S7.Net;
using S7.Net.Types;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Persistent S7 connection and PO-change handshake for one mill:
/// trigger rising edge (FALSE→TRUE) → handle PO change → ack TRUE → wait trigger FALSE → ack FALSE.
/// Uses rising-edge detection (same as plc-server) so a held or quickly re-pulsed M-bit does not
/// repeat the PO-end workflow until trigger has been false for several poll cycles.
/// </summary>
public sealed class PlcHandshakeService
{
    private readonly MillConfig _mill;
    private readonly PlcHandshakeOptions _options;
    private readonly NdtBundleOptions _bundleOptions;
    private readonly IPoChangeHandler _poChangeHandler;
    private readonly PlcHandshakeStatusRegistry _statusRegistry;
    private readonly PlcConnectionHealth _connectionHealth;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IMillHooterPlcValuesService? _hooterValues;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly ILogger<PlcHandshakeService> _logger;

    private readonly object _plcLock = new();
    private Plc? _plc;
    private short _connectedSlot;
    private int _reconnectDelayMs;

    private bool _primed;
    private bool _loggedStartupTriggerWait;
    private bool _startupRecoveryAttempted;
    private bool _prevTriggerActive;
    private bool _poChangePulseHandled;
    private int _consecutiveTriggerFalsePolls;
    private bool _handshakeInProgress;
    private volatile bool _plcConnectionEnabled;
    private bool _settingsTestInProgress;
    private bool _suppressNdtUntilPoChange;
    private int _poIdWhenSuppressed;

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
        PlcHandshakeStatusRegistry statusRegistry,
        PlcConnectionHealth connectionHealth,
        IActivePoPerMillService activePoPerMill,
        IMillHooterPlcValuesService? hooterValues,
        IWipBundleRunningPoProvider wipRunningPo,
        ILogger<PlcHandshakeService> logger)
    {
        _mill = mill;
        _options = options;
        _bundleOptions = bundleOptions;
        _poChangeHandler = poChangeHandler;
        _statusRegistry = statusRegistry;
        _connectionHealth = connectionHealth;
        _activePoPerMill = activePoPerMill;
        _hooterValues = hooterValues;
        _wipRunningPo = wipRunningPo;
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

    private bool IsFileBasedPoEndEnabled() =>
        _bundleOptions.FileBasedPoEnd?.Enabled == true;

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
            ResetTriggerEdgeState(reprime: true);
            _reconnectDelayMs = Math.Max(250, _options.InitialReconnectDelayMs);
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

    private bool IsPlcConnected()
    {
        lock (_plcLock)
            return _plc is not null && _plc.IsConnected;
    }

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        var millNo = _mill.ResolveMillNo();
        _logger.LogInformation(
            "PlcHandshakeService starting for {MillName} at {Host} trigger {Trigger} ack {Ack} poll {Poll}ms.",
            _mill.Name,
            _mill.IpAddress,
            _mill.TriggerAddress,
            _mill.AckAddress,
            ResolvePollIntervalMs());

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

        _reconnectDelayMs = Math.Max(250, _options.InitialReconnectDelayMs);

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

                TryUpdateDb251Counts(millNo);
                await TryUpdateMillSignalsAsync(millNo, stoppingToken).ConfigureAwait(false);

                _connectionHealth.RecordModbusPoll(true, _statusRegistry.AllConnected());

                if (!_primed)
                {
                    if (!trigger)
                    {
                        ResetStuckAckIfNeeded(millNo, ack);
                        ArmAfterTriggerClear(isStartup: true);
                        SetState(millNo, "Idle");
                        _logger.LogDebug(
                            "{MillName}: handshake primed (trigger {Trigger} is false).",
                            _mill.Name,
                            _mill.TriggerAddress);
                    }
                    else if (_options.RecoverLatchedTriggerAtStartup && !_startupRecoveryAttempted)
                    {
                        if (IsFileBasedPoEndEnabled())
                        {
                            _startupRecoveryAttempted = true;
                            _loggedStartupTriggerWait = false;
                            _prevTriggerActive = trigger;
                            _poChangePulseHandled = true;
                            ArmAfterTriggerClear(isStartup: false);
                            SetState(millNo, "Idle (file-based PO end)");
                            _logger.LogInformation(
                                "{MillName}: latched trigger {Trigger} at startup ignored — PO end uses TM Bundle WIP files.",
                                _mill.Name,
                                _mill.TriggerAddress);
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
                        if (!_loggedStartupTriggerWait)
                        {
                            _loggedStartupTriggerWait = true;
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

                    _prevTriggerActive = trigger;
                    await PollDelayAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                var risingEdge = TryDetectPoChangeRisingEdge(trigger);
                var rearmedBeforePoll = IsRearmedForNextPoChange();
                UpdateTriggerEdgeTracking(trigger);

                if (risingEdge &&
                    !_handshakeInProgress &&
                    !_settingsTestInProgress)
                {
                    _poChangePulseHandled = true;
                    if (IsFileBasedPoEndEnabled())
                    {
                        SetState(millNo, "Idle (file-based PO end)");
                        _logger.LogInformation(
                            "{MillName}: PLC PO-change trigger {Trigger} ignored — PO end uses TM Bundle WIP filename changes.",
                            _mill.Name,
                            _mill.TriggerAddress);
                    }
                    else
                    {
                        await RunHandshakeAsync(millNo, stoppingToken).ConfigureAwait(false);
                    }
                }
                else if (!_handshakeInProgress && !_settingsTestInProgress)
                {
                    if (trigger && _poChangePulseHandled)
                        SetState(millNo, "Idle (pulse handled, waiting for trigger clear)");
                    else if (trigger && !_prevTriggerActive && !rearmedBeforePoll)
                        SetState(millNo, "Idle (re-arming after last handshake)");
                    else
                        SetState(millNo, "Idle");
                }

                _prevTriggerActive = trigger;
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
                ResetTriggerEdgeState(reprime: true);
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
        _startupRecoveryAttempted = true;
        _loggedStartupTriggerWait = false;
        _prevTriggerActive = true;
        _poChangePulseHandled = true;

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
            ArmAfterTriggerClear(isStartup: false);
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

    private async Task RunHandshakeCoreAsync(int millNo, CancellationToken stoppingToken, bool startupRecovery = false)
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
        else
        {
            _logger.LogInformation(
                "{MillName}: PO change rising edge on {Trigger} — handshake started.",
                _mill.Name,
                _mill.TriggerAddress);
        }

        var runPoEndWorkflow = !startupRecovery || _options.RunPoEndWorkflowOnStartupRecovery;

        var poIdVal = 0;
        var ndtFinal = 0;
        if (runPoEndWorkflow)
        {
            try
            {
                poIdVal = ReadDbInt(_options.PoIdByteOffset);
                ndtFinal = ReadDbInt(_options.NdtCountByteOffset);
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
                    TimestampUtc = DateTimeOffset.UtcNow
                };
                s.NdtCount = 0;
            });

            try
            {
                await _poChangeHandler.HandlePoChangeAsync(_mill, stoppingToken).ConfigureAwait(false);
                UpdateStatus(millNo, s => s.LastPoChangeUtc = DateTimeOffset.UtcNow);
                await SyncHooterMemoryAfterPoEndAsync(millNo, stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{MillName}: PO change handler failed; continuing handshake ack sequence.", _mill.Name);
            }
        }

        await RunMesAckSequenceAsync(millNo, stoppingToken).ConfigureAwait(false);

        SetState(millNo, "Idle");
    }

    private async Task RunMesAckSequenceAsync(int millNo, CancellationToken stoppingToken)
    {
        SetState(millNo, "AckSent");
        WriteMerkerBit(_mill.AckByte, _mill.AckBit, true);
        UpdateStatus(millNo, s => s.AckActive = true);

        _logger.LogInformation(
            "{MillName}: wrote ack {Ack}=TRUE.",
            _mill.Name,
            _mill.AckAddress);

        SetState(millNo, "WaitingTriggerClear");
        var clearDeadline = System.DateTime.UtcNow.AddMilliseconds(Math.Max(1000, _mill.TriggerClearTimeoutMs));
        while (!stoppingToken.IsCancellationRequested && System.DateTime.UtcNow < clearDeadline)
        {
            if (!ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit))
            {
                _logger.LogInformation(
                    "{MillName}: trigger {Trigger} cleared by PLC.",
                    _mill.Name,
                    _mill.TriggerAddress);
                break;
            }

            await Task.Delay(ResolvePollIntervalMs(), stoppingToken).ConfigureAwait(false);
        }

        if (ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit))
        {
            _logger.LogWarning(
                "{MillName}: trigger {Trigger} still TRUE after {Timeout}ms; resetting ack anyway.",
                _mill.Name,
                _mill.TriggerAddress,
                _mill.TriggerClearTimeoutMs);
        }

        SetState(millNo, "AckReset");
        WriteMerkerBit(_mill.AckByte, _mill.AckBit, false);
        UpdateStatus(millNo, s =>
        {
            s.AckActive = false;
            s.TriggerActive = ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit);
        });

        _logger.LogInformation(
            "{MillName}: wrote ack {Ack}=FALSE — handshake complete.",
            _mill.Name,
            _mill.AckAddress);
    }

    private void UpdateTriggerEdgeTracking(bool trigger)
    {
        if (!trigger)
        {
            _poChangePulseHandled = false;
            _consecutiveTriggerFalsePolls++;
        }
        else
        {
            _consecutiveTriggerFalsePolls = 0;
        }
    }

    private bool TryDetectPoChangeRisingEdge(bool trigger) =>
        trigger &&
        !_prevTriggerActive &&
        !_poChangePulseHandled &&
        IsRearmedForNextPoChange();

    private bool IsRearmedForNextPoChange()
    {
        var required = Math.Max(1, _options.MinimumTriggerFalsePollsBeforeRearm);
        return _consecutiveTriggerFalsePolls >= required;
    }

    private void ArmAfterTriggerClear(bool isStartup)
    {
        _primed = true;
        _loggedStartupTriggerWait = false;
        _prevTriggerActive = false;
        _poChangePulseHandled = false;
        _consecutiveTriggerFalsePolls = isStartup ? 1 : Math.Max(1, _options.MinimumTriggerFalsePollsBeforeRearm);
    }

    private void ResetTriggerEdgeState(bool reprime)
    {
        _primed = !reprime;
        if (reprime)
        {
            _loggedStartupTriggerWait = false;
            _startupRecoveryAttempted = false;
        }

        _prevTriggerActive = false;
        _poChangePulseHandled = false;
        _consecutiveTriggerFalsePolls = 0;
    }

    private async Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        lock (_plcLock)
        {
            if (_plc is not null && _plc.IsConnected)
                return true;
        }

        DisconnectPlc();

        foreach (var slot in ResolveSlots())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var cpu = ParseCpu(_mill.CpuType);
            var plc = new Plc(cpu, _mill.IpAddress.Trim(), _mill.Rack, slot);
            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(Math.Clamp(_mill.ConnectTimeoutMs, 500, 60_000));
                await Task.Run(() => plc.Open(), timeoutCts.Token).ConfigureAwait(false);

                lock (_plcLock)
                {
                    _plc = plc;
                    _connectedSlot = slot;
                }

                _reconnectDelayMs = Math.Max(250, _options.InitialReconnectDelayMs);
                _logger.LogInformation(
                    "{MillName}: S7 connected to {Host} rack {Rack} slot {Slot}.",
                    _mill.Name,
                    _mill.IpAddress,
                    _mill.Rack,
                    slot);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    ex,
                    "{MillName}: S7 connect failed at slot {Slot}.",
                    _mill.Name,
                    slot);
                try
                {
                    plc.Close();
                }
                catch
                {
                    /* ignore */
                }
            }
        }

        return false;
    }

    private void DisconnectPlc()
    {
        lock (_plcLock)
        {
            if (_plc is null)
                return;
            try
            {
                _plc.Close();
            }
            catch
            {
                /* ignore */
            }

            _plc = null;
        }
    }

    private bool ReadMerkerBit(int byteOffset, int bit)
    {
        lock (_plcLock)
        {
            if (_plc is null || !_plc.IsConnected)
                throw new InvalidOperationException("PLC not connected.");

            var value = _plc.Read(DataType.Memory, 0, byteOffset, VarType.Bit, 1, (byte)bit);
            return value is true;
        }
    }

    private void WriteMerkerBit(int byteOffset, int bit, bool value)
    {
        lock (_plcLock)
        {
            if (_plc is null || !_plc.IsConnected)
                throw new InvalidOperationException("PLC not connected.");

            _plc.Write(DataType.Memory, 0, byteOffset, value, bit);
        }
    }

    private void TryUpdateDb251Counts(int millNo)
    {
        try
        {
            var ok = ReadDbInt(_options.OkCountByteOffset);
            var nok = ReadDbInt(_options.NokCountByteOffset);
            var ndtRaw = ReadDbInt(_options.NdtCountByteOffset);
            var poId = ReadDbInt(_options.PoIdByteOffset);
            var slitId = ReadDbInt(_options.SlitIdByteOffset);

            var ndtDisplay = ndtRaw;
            if (_suppressNdtUntilPoChange)
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
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                ex,
                "{MillName}: DB{Db} count read failed.",
                _mill.Name,
                _options.CountsDbNumber);
        }
    }

    private int ReadDbInt(int byteOffset)
    {
        lock (_plcLock)
        {
            if (_plc is null || !_plc.IsConnected)
                throw new InvalidOperationException("PLC not connected.");

            var raw = _plc.Read(DataType.DataBlock, _options.CountsDbNumber, byteOffset, VarType.Int, 1);
            if (raw is null)
                return 0;
            var v = Convert.ToInt32(raw);
            return v < 0 ? 0 : v;
        }
    }

    private async Task TryUpdateMillSignalsAsync(int millNo, CancellationToken cancellationToken)
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
            if (hooterCfg?.Enabled == true && _hooterValues != null)
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

    private async Task SyncHooterMemoryAfterPoEndAsync(int millNo, CancellationToken cancellationToken)
    {
        var hooterCfg = _mill.Hooter;
        if (hooterCfg?.Enabled != true || _hooterValues == null)
            return;

        try
        {
            if (_plc is null || !_plc.IsConnected)
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
        lock (_plcLock)
        {
            if (_plc is null || !_plc.IsConnected)
                throw new InvalidOperationException("PLC not connected.");

            _plc.Write(DataType.Memory, 0, wordOffset, (short)clamped);
        }
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

    private bool ReadDbBit(int dbNumber, int byteOffset, int bit)
    {
        lock (_plcLock)
        {
            if (_plc is null || !_plc.IsConnected)
                throw new InvalidOperationException("PLC not connected.");

            var value = _plc.Read(DataType.DataBlock, dbNumber, byteOffset, VarType.Bit, 1, (byte)bit);
            return value is true;
        }
    }

    private bool ReadOutputBit(int byteOffset, int bit)
    {
        lock (_plcLock)
        {
            if (_plc is null || !_plc.IsConnected)
                throw new InvalidOperationException("PLC not connected.");

            var value = _plc.Read(DataType.Output, 0, byteOffset, VarType.Bit, 1, (byte)bit);
            return value is true;
        }
    }

    private void WriteOutputBit(int byteOffset, int bit, bool value)
    {
        lock (_plcLock)
        {
            if (_plc is null || !_plc.IsConnected)
                throw new InvalidOperationException("PLC not connected.");

            _plc.Write(DataType.Output, 0, byteOffset, value, bit);
        }
    }

    private IEnumerable<short> ResolveSlots()
    {
        yield return _mill.Slot;
        foreach (var s in _mill.SlotFallback ?? new List<short>())
        {
            if (s != _mill.Slot)
                yield return s;
        }

        if (_mill.Slot != 1 && !(_mill.SlotFallback?.Contains((short)1) ?? false))
            yield return 1;
    }

    private int ResolvePollIntervalMs()
    {
        if (_mill.PollIntervalMs > 0)
            return _mill.PollIntervalMs;
        return Math.Max(100, _options.PollIntervalMs);
    }

    private Task PollDelayAsync(CancellationToken cancellationToken) =>
        Task.Delay(ResolvePollIntervalMs(), cancellationToken);

    private async Task DelayReconnectAsync(CancellationToken cancellationToken)
    {
        var millNo = _mill.ResolveMillNo();
        MarkDisconnected(millNo, "S7 connection failed; retrying.");
        _connectionHealth.RecordModbusPoll(true, false, "One or more handshake mills unreachable.");

        _logger.LogWarning(
            "{MillName}: reconnect in {Delay}ms.",
            _mill.Name,
            _reconnectDelayMs);

        await Task.Delay(_reconnectDelayMs, cancellationToken).ConfigureAwait(false);
        _reconnectDelayMs = Math.Min(_reconnectDelayMs * 2, Math.Max(1000, _options.MaxReconnectDelayMs));
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

    private static CpuType ParseCpu(string? raw) =>
        raw?.Trim().ToUpperInvariant() switch
        {
            "S71200" => CpuType.S71200,
            "S71500" => CpuType.S71500,
            "S7400" => CpuType.S7400,
            _ => CpuType.S7300
        };

    /// <summary>
    /// Settings test: read trigger, run PO end workflow, pulse ack on the mill's existing S7 connection.
    /// Does not require a live PLC PO-change event.
    /// </summary>
    public async Task<PlcPoChangeTestResult> RunSettingsTestAsync(CancellationToken cancellationToken)
    {
        var millNo = _mill.ResolveMillNo();
        var steps = new List<string>();

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
