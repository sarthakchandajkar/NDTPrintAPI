using S7.Net;

namespace NdtBundleService.Services.PlcHandshake.S7;

/// <summary>
/// Pure PLC session operations used inside <see cref="IS7ConnectionProvider"/> delegates.
/// Must not call back into <see cref="IS7ConnectionProvider"/>.
/// </summary>
public interface IS7PlcOperations
{
    object? Read(DataType dataType, int db, int startByteAdr, VarType varType, int varCount, byte bitAdr = 0);

    void Write(DataType dataType, int db, int startByteAdr, object value, int bitAdr = -1);
}
