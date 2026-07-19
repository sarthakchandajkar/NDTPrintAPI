using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake.S7;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class S7MillNdtCountReaderSharedProviderTests
{
    [Fact]
    public async Task Returns_null_when_no_shared_provider_without_opening_plc()
    {
        var options = Options.Create(new NdtBundleOptions
        {
            MillSlitLive = new MillSlitLiveOptions
            {
                ApplyToMillNo = 1,
                S7 = new MillS7NdtOptions
                {
                    Host = "192.168.0.13",
                    DbNumber = 251,
                    NdtCountByteOffset = 6
                }
            }
        });

        var reader = new S7MillNdtCountReader(
            options,
            new EmptyS7Registry(),
            NullLogger<S7MillNdtCountReader>.Instance);

        var result = await reader.TryReadNdtPipesCountAsync(CancellationToken.None);
        Assert.Null(result);
    }

    [Fact]
    public async Task Reads_via_shared_provider_ReadAsync()
    {
        var options = Options.Create(new NdtBundleOptions
        {
            MillSlitLive = new MillSlitLiveOptions
            {
                ApplyToMillNo = 1,
                S7 = new MillS7NdtOptions
                {
                    Host = "192.168.0.13",
                    DbNumber = 251,
                    NdtCountByteOffset = 6
                }
            }
        });

        var provider = new FakeHealthyS7Provider(ndtValue: 11);
        var reader = new S7MillNdtCountReader(
            options,
            new FixedS7Registry(provider),
            NullLogger<S7MillNdtCountReader>.Instance);

        var result = await reader.TryReadNdtPipesCountAsync(CancellationToken.None);
        Assert.Equal(11, result);
        Assert.Equal(1, provider.ReadAsyncCallCount);
    }

    [Fact]
    public async Task Returns_null_when_provider_read_fails_without_throwing()
    {
        var options = Options.Create(new NdtBundleOptions
        {
            MillSlitLive = new MillSlitLiveOptions
            {
                ApplyToMillNo = 1,
                S7 = new MillS7NdtOptions
                {
                    Host = "192.168.0.13",
                    DbNumber = 251,
                    NdtCountByteOffset = 6
                }
            }
        });

        var provider = new FakeHealthyS7Provider(ndtValue: 11, throwOnRead: true);
        var reader = new S7MillNdtCountReader(
            options,
            new FixedS7Registry(provider),
            NullLogger<S7MillNdtCountReader>.Instance);

        var result = await reader.TryReadNdtPipesCountAsync(CancellationToken.None);
        Assert.Null(result);
        Assert.Equal(1, provider.ReadAsyncCallCount);
    }

    private sealed class EmptyS7Registry : IS7ConnectionProviderRegistry
    {
        public IS7ConnectionProvider GetOrCreate(MillConfig mill, PlcHandshakeOptions options) =>
            throw new NotSupportedException();

        public IS7ConnectionProvider? TryGet(int millNo) => null;
    }

    private sealed class FixedS7Registry : IS7ConnectionProviderRegistry
    {
        private readonly IS7ConnectionProvider _provider;

        public FixedS7Registry(IS7ConnectionProvider provider) => _provider = provider;

        public IS7ConnectionProvider GetOrCreate(MillConfig mill, PlcHandshakeOptions options) => _provider;

        public IS7ConnectionProvider? TryGet(int millNo) => millNo == 1 ? _provider : null;
    }

    /// <summary>
    /// Test double: proves the reader uses <see cref="IS7ConnectionProvider.ReadAsync{T}"/>
    /// instead of opening its own S7 client. Does not invoke the Plc delegate (no live PLC in unit tests).
    /// </summary>
    private sealed class FakeHealthyS7Provider : IS7ConnectionProvider
    {
        private readonly int _ndtValue;
        private readonly bool _throwOnRead;

        public FakeHealthyS7Provider(int ndtValue, bool throwOnRead = false)
        {
            _ndtValue = ndtValue;
            _throwOnRead = throwOnRead;
        }

        public int ReadAsyncCallCount { get; private set; }

        public int MillNo => 1;
        public string MillName => "Mill-1";
        public bool IsConnected => true;
        public bool IsHealthy => true;
#pragma warning disable CS0067 // Test double; event required by interface
        public event Action<bool>? HealthChanged;
#pragma warning restore CS0067

        public Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken) => Task.FromResult(true);

        public void Disconnect()
        {
        }

        public T Read<T>(Func<IS7PlcOperations, T> operation) => throw new NotSupportedException();

        public void Write(Action<IS7PlcOperations> operation) => throw new NotSupportedException();

        public Task<T> ReadAsync<T>(Func<IS7PlcOperations, T> operation, CancellationToken cancellationToken = default)
        {
            ReadAsyncCallCount++;
            if (_throwOnRead)
                throw new InvalidOperationException("simulated S7 failure");
            if (typeof(T) == typeof(int?))
                return Task.FromResult((T)(object)(int?)_ndtValue);
            throw new NotSupportedException($"Unexpected ReadAsync type {typeof(T)}");
        }

        public Task WriteAsync(Action<IS7PlcOperations> operation, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public int TakeReconnectDelayMs() => 1000;

        public void ResetReconnectBackoff()
        {
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
