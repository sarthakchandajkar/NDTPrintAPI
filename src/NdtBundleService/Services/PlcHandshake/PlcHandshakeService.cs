using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using S7.Net;
using S7.Net.Types;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Persistent S7 connection and PO-change handshake for one mill:
/// trigger TRUE → handle PO change → ack TRUE → wait trigger FALSE → ack FALSE.
/// </summary>
public sealed class PlcHandshakeService
{
    private readonly MillConfig _mill;
    private readonly PlcHandshakeOptions _options;
    private readonly IPoChangeHandler _poChangeHandler;
    private readonly PlcHandshakeStatusRegistry _statusRegistry;
    private readonly PlcConnectionHealth _connectionHealth;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly ILogger<PlcHandshakeService> _logger;

    private readonly object _plcLock = new();
    private Plc? _plc;
    private short _connectedSlot;
    private int _reconnectDelayMs;

    private bool _primed;
    private bool _handshakeInProgress;
    private volatile bool _settingsTestInProgress;
    private bool _suppressNdtUntilPoChange;
    private int _poIdWhenSuppressed;

    public PlcHandshakeService(
        MillConfig mill,
        PlcHandshakeOptions options,
        IPoChangeHandler poChangeHandler,
        PlcHandshakeStatusRegistry statusRegistry,
        PlcConnectionHealth connectionHealth,
        IActivePoPerMillService activePoPerMill,
        ILogger<PlcHandshakeService> logger)
    {
        _mill = mill;
        _options = options;
        _poChangeHandler = poChangeHandler;
        _statusRegistry = statusRegistry;
        _connectionHealth = connectionHealth;
        _activePoPerMill = activePoPerMill;
        _logger = logger;

        var millNo = mill.ResolveMillNo();
        _statusRegistry.RegisterMill(millNo, new PlcHandshakeMillStatus
        {
            MillName = mill.Name,
            MillNo = millNo,
            IpAddress = mill.IpAddress.Trim(),
            HandshakeState = "Starting"
        });
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

        _reconnectDelayMs = Math.Max(250, _options.InitialReconnectDelayMs);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!await EnsureConnectedAsync(stoppingToken).ConfigureAwait(false))
                {
                    await DelayReconnectAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                var trigger = ReadMerkerBit(_mill.TriggerByte, _mill.TriggerBit);
                var ack = ReadMerkerBit(_mill.AckByte, _mill.AckBit);

                UpdateStatus(millNo, s =>
                {
                    s.Connected = true;
                    s.TriggerActive = trigger;
                    s.AckActive = ack;
                    s.LastError = null;
                });

                TryUpdateDb251Counts(millNo);

                _connectionHealth.RecordModbusPoll(true, _statusRegistry.AllConnected());

                if (!_primed)
                {
                    if (!trigger)
                    {
                        _primed = true;
                        SetState(millNo, "Idle");
                        _logger.LogDebug(
                            "{MillName}: handshake primed (trigger {Trigger} is false).",
                            _mill.Name,
                            _mill.TriggerAddress);
                    }
                    else
                    {
                        SetState(millNo, "WaitingTriggerClear (startup)");
                        _logger.LogInformation(
                            "{MillName}: trigger {Trigger} already set at startup; waiting for PLC to clear before arming.",
                            _mill.Name,
                            _mill.TriggerAddress);
                    }

                    await PollDelayAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                if (!_handshakeInProgress && !_settingsTestInProgress && trigger)
                {
                    await RunHandshakeAsync(millNo, stoppingToken).ConfigureAwait(false);
                }
                else if (!_handshakeInProgress && !_settingsTestInProgress)
                {
                    SetState(millNo, "Idle");
                }
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

    private async Task RunHandshakeAsync(int millNo, CancellationToken stoppingToken)
    {
        _handshakeInProgress = true;
        try
        {
            await RunHandshakeCoreAsync(millNo, stoppingToken).ConfigureAwait(false);
        }
        finally
        {
            _handshakeInProgress = false;
        }
    }

    private async Task RunHandshakeCoreAsync(int millNo, CancellationToken stoppingToken)
    {
        SetState(millNo, "ProcessingPoChange");

        _logger.LogInformation(
            "{MillName}: trigger {Trigger} TRUE — PO change handshake started.",
            _mill.Name,
            _mill.TriggerAddress);

        var poIdVal = 0;
        var ndtFinal = 0;
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
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{MillName}: PO change handler failed; continuing handshake ack sequence.", _mill.Name);
        }

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

        SetState(millNo, "Idle");
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
