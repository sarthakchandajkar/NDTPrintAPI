namespace NdtBundleService.Services.PlcHandshake.S7;

/// <summary>Thrown in DEBUG when a nested <see cref="IS7ConnectionProvider"/> call is detected.</summary>
public sealed class S7ConnectionReentrancyException : InvalidOperationException
{
    public S7ConnectionReentrancyException(string message) : base(message)
    {
    }
}
