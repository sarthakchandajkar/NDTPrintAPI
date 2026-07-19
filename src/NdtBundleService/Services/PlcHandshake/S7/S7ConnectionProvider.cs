using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using S7.Net;

namespace NdtBundleService.Services.PlcHandshake.S7;

/// <summary>
/// Owns the single <see cref="Plc"/> instance for one mill. Reconnect backoff uses
/// <see cref="PlcHandshakeOptions.InitialReconnectDelayMs"/> / <see cref="PlcHandshakeOptions.MaxReconnectDelayMs"/>.
/// </summary>
public sealed class S7ConnectionProvider : IS7ConnectionProvider
{
    private readonly MillConfig _mill;
    private readonly PlcHandshakeOptions _options;
    private readonly ILogger _logger;
    private readonly S7ConnectionGate _gate = new();
    private readonly object _plcLock = new();
    private readonly object _healthLock = new();

    private Plc? _plc;
    private short _connectedSlot;
    private int _reconnectDelayMs;
    private bool _healthy;
    private bool _everConnected;
    private DateTimeOffset _lastHealthSummaryUtc;
    private int _failedConnectAttemptsSinceSummary;
    private int _successfulIoSinceSummary;
    private int _failedIoSinceSummary;

    private static readonly TimeSpan HealthSummaryInterval = TimeSpan.FromMinutes(5);

    public S7ConnectionProvider(
        MillConfig mill,
        PlcHandshakeOptions options,
        ILogger logger)
    {
        _mill = mill;
        _options = options;
        _logger = logger;
        _reconnectDelayMs = Math.Max(250, options.InitialReconnectDelayMs);
        MillNo = mill.ResolveMillNo();
        MillName = mill.Name;
    }

    public int MillNo { get; }

    public string MillName { get; }

    public bool IsConnected
    {
        get
        {
            lock (_plcLock)
                return _plc is not null && _plc.IsConnected;
        }
    }

    public bool IsHealthy
    {
        get
        {
            lock (_healthLock)
                return _healthy && IsConnected;
        }
    }

    public event Action<bool>? HealthChanged;

    public async Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        lock (_plcLock)
        {
            if (_plc is not null && _plc.IsConnected)
            {
                SetHealthy(true);
                return true;
            }
        }

        Disconnect();

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

                ResetReconnectBackoff();
                _everConnected = true;
                SetHealthy(true);
                _logger.LogInformation(
                    "{MillName}: S7 connected to {Host} rack {Rack} slot {Slot}.",
                    MillName,
                    _mill.IpAddress,
                    _mill.Rack,
                    slot);
                return true;
            }
            catch (Exception ex)
            {
                _failedConnectAttemptsSinceSummary++;
                _logger.LogDebug(
                    ex,
                    "{MillName}: S7 connect failed at slot {Slot}.",
                    MillName,
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

        SetHealthy(false);
        MaybeLogHealthSummary();
        return false;
    }

    public void Disconnect()
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

        SetHealthy(false);
    }

    public T Read<T>(Func<IS7PlcOperations, T> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        try
        {
            var result = _gate.Execute(() =>
            {
                var plc = RequireConnectedPlc();
                return operation(new S7NetPlcOperations(plc));
            });
            RecordIoSuccess();
            return result;
        }
        catch (S7ConnectionReentrancyException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            RecordIoFailure();
            throw;
        }
    }

    public void Write(Action<IS7PlcOperations> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        Read<object?>(ops =>
        {
            operation(ops);
            return null;
        });
    }

    public async Task<T> ReadAsync<T>(Func<IS7PlcOperations, T> operation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        try
        {
            var result = await _gate.ExecuteAsync(() =>
            {
                var plc = RequireConnectedPlc();
                return operation(new S7NetPlcOperations(plc));
            }, cancellationToken).ConfigureAwait(false);
            RecordIoSuccess();
            return result;
        }
        catch (S7ConnectionReentrancyException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            RecordIoFailure();
            throw;
        }
    }

    public Task WriteAsync(Action<IS7PlcOperations> operation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return ReadAsync<object?>(ops =>
        {
            operation(ops);
            return null;
        }, cancellationToken);
    }

    public int TakeReconnectDelayMs()
    {
        var delay = _reconnectDelayMs;
        _reconnectDelayMs = Math.Min(_reconnectDelayMs * 2, Math.Max(1000, _options.MaxReconnectDelayMs));
        return delay;
    }

    public void ResetReconnectBackoff()
    {
        _reconnectDelayMs = Math.Max(250, _options.InitialReconnectDelayMs);
    }

    public ValueTask DisposeAsync()
    {
        Disconnect();
        _gate.Dispose();
        return ValueTask.CompletedTask;
    }

    private Plc RequireConnectedPlc()
    {
        lock (_plcLock)
        {
            if (_plc is null || !_plc.IsConnected)
                throw new InvalidOperationException("PLC not connected.");
            return _plc;
        }
    }

    private void RecordIoSuccess()
    {
        _successfulIoSinceSummary++;
        SetHealthy(true);
        MaybeLogHealthSummary();
    }

    private void RecordIoFailure()
    {
        _failedIoSinceSummary++;
        SetHealthy(false);
        MaybeLogHealthSummary();
    }

    private void SetHealthy(bool healthy)
    {
        bool changed;
        lock (_healthLock)
        {
            changed = _healthy != healthy;
            _healthy = healthy;
        }

        if (!changed)
            return;

        if (healthy)
        {
            _logger.LogInformation(
                "{MillName}: S7 connection healthy (host {Host}).",
                MillName,
                _mill.IpAddress);
        }
        else if (_everConnected)
        {
            _logger.LogWarning(
                "{MillName}: S7 connection unhealthy (host {Host}).",
                MillName,
                _mill.IpAddress);
        }

        try
        {
            HealthChanged?.Invoke(healthy);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "{MillName}: HealthChanged subscriber threw.", MillName);
        }
    }

    private void MaybeLogHealthSummary()
    {
        var now = DateTimeOffset.UtcNow;
        if (_lastHealthSummaryUtc != default && now - _lastHealthSummaryUtc < HealthSummaryInterval)
            return;

        _lastHealthSummaryUtc = now;
        var failedConnect = _failedConnectAttemptsSinceSummary;
        var okIo = _successfulIoSinceSummary;
        var failIo = _failedIoSinceSummary;
        _failedConnectAttemptsSinceSummary = 0;
        _successfulIoSinceSummary = 0;
        _failedIoSinceSummary = 0;

        _logger.LogInformation(
            "{MillName}: S7 health summary — healthy={Healthy} connected={Connected} okIo={OkIo} failIo={FailIo} failedConnectAttempts={FailedConnect}.",
            MillName,
            IsHealthy,
            IsConnected,
            okIo,
            failIo,
            failedConnect);
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

    private static CpuType ParseCpu(string? raw) =>
        raw?.Trim().ToUpperInvariant() switch
        {
            "S71200" => CpuType.S71200,
            "S71500" => CpuType.S71500,
            "S7400" => CpuType.S7400,
            _ => CpuType.S7300
        };
}
