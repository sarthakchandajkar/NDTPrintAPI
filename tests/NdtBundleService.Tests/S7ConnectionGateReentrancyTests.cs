using NdtBundleService.Services.PlcHandshake.S7;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class S7ConnectionGateReentrancyTests
{
    [Fact]
    public void Nested_Execute_throws_S7ConnectionReentrancyException()
    {
        using var gate = new S7ConnectionGate();

        var ex = Assert.Throws<S7ConnectionReentrancyException>(() =>
            gate.Execute(() =>
            {
                gate.Execute(() => 1);
                return 0;
            }));

        Assert.Contains("Nested IS7ConnectionProvider", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Sequential_Execute_succeeds()
    {
        using var gate = new S7ConnectionGate();
        Assert.Equal(1, gate.Execute(() => 1));
        Assert.Equal(2, gate.Execute(() => 2));
    }

    [Fact]
    public async Task Nested_ExecuteAsync_throws_S7ConnectionReentrancyException()
    {
        using var gate = new S7ConnectionGate();

        await Assert.ThrowsAsync<S7ConnectionReentrancyException>(async () =>
            await gate.ExecuteAsync(() =>
            {
                gate.Execute(() => 1);
                return 0;
            }, CancellationToken.None));
    }
}
