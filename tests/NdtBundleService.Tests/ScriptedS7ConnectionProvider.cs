using S7.Net;
using NdtBundleService.Services.PlcHandshake.S7;

namespace NdtBundleService.Tests;

/// <summary>
/// Scripted shared S7 provider for handshake sequence pin tests.
/// Records every read/write address; merker/DB values are supplied by the script.
/// </summary>
internal sealed class ScriptedS7ConnectionProvider : IS7ConnectionProvider
{
    private readonly object _gate = new();
    private bool _trigger;
    private bool _ack;
    private readonly Dictionary<int, int> _db251 = new()
    {
        [2] = 0,
        [4] = 0,
        [6] = 5,
        [8] = 6,
        [10] = 1
    };
    private bool _lineRunning = true;

    public List<string> Operations { get; } = new();

    public int MillNo => 1;
    public string MillName => "Mill-1";
    public bool IsConnected { get; private set; } = true;
    public bool IsHealthy => IsConnected;
#pragma warning disable CS0067
    public event Action<bool>? HealthChanged;
#pragma warning restore CS0067

    public void SetTrigger(bool value) => _trigger = value;
    public void SetAck(bool value) => _ack = value;
    public void SetDb251Int(int byteOffset, int value) => _db251[byteOffset] = value;
    public void ClearOperations() => Operations.Clear();

    /// <summary>Next N Write calls throw (for ack-retry pin tests).</summary>
    public int FailNextWrites { get; set; }

    public Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        IsConnected = true;
        return Task.FromResult(true);
    }

    public void Disconnect() => IsConnected = false;

    public T Read<T>(Func<IS7PlcOperations, T> operation)
    {
        lock (_gate)
            return operation(new RecordingOps(this));
    }

    public void Write(Action<IS7PlcOperations> operation)
    {
        lock (_gate)
        {
            if (FailNextWrites > 0)
            {
                FailNextWrites--;
                throw new InvalidOperationException("scripted S7 write failure");
            }

            operation(new RecordingOps(this));
        }
    }

    public Task<T> ReadAsync<T>(Func<IS7PlcOperations, T> operation, CancellationToken cancellationToken = default) =>
        Task.FromResult(Read(operation));

    public Task WriteAsync(Action<IS7PlcOperations> operation, CancellationToken cancellationToken = default)
    {
        Write(operation);
        return Task.CompletedTask;
    }

    public int TakeReconnectDelayMs() => 1000;
    public void ResetReconnectBackoff() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private sealed class RecordingOps : IS7PlcOperations
    {
        private readonly ScriptedS7ConnectionProvider _owner;

        public RecordingOps(ScriptedS7ConnectionProvider owner) => _owner = owner;

        public object? Read(DataType dataType, int db, int startByteAdr, VarType varType, int varCount, byte bitAdr = 0)
        {
            if (dataType == DataType.Memory && varType == VarType.Bit)
            {
                var addr = $"M{startByteAdr}.{bitAdr}";
                _owner.Operations.Add($"R {addr}");
                if (startByteAdr == 40 && bitAdr == 6)
                    return _owner._trigger;
                if (startByteAdr == 40 && bitAdr == 7)
                    return _owner._ack;
                return false;
            }

            if (dataType == DataType.DataBlock && varType == VarType.Int && db == 251)
            {
                _owner.Operations.Add($"R DB251@{startByteAdr}");
                return _owner._db251.TryGetValue(startByteAdr, out var v) ? v : 0;
            }

            if (dataType == DataType.DataBlock && varType == VarType.Bit && db == 250)
            {
                _owner.Operations.Add($"R DB250.DBX{startByteAdr}.{bitAdr}");
                return _owner._lineRunning;
            }

            _owner.Operations.Add($"R {dataType} db={db} @{startByteAdr} {varType} bit={bitAdr}");
            return varType == VarType.Bit ? false : 0;
        }

        public void Write(DataType dataType, int db, int startByteAdr, object value, int bitAdr = -1)
        {
            if (dataType == DataType.Memory && bitAdr >= 0)
            {
                var on = value is true || value is bool b && b;
                var addr = $"M{startByteAdr}.{bitAdr}";
                _owner.Operations.Add($"W {addr}={(on ? "TRUE" : "FALSE")}");
                if (startByteAdr == 40 && bitAdr == 7)
                    _owner._ack = on;
                return;
            }

            _owner.Operations.Add($"W {dataType} db={db} @{startByteAdr} bit={bitAdr}={value}");
        }
    }
}
