using S7.Net;

namespace NdtBundleService.Services.PlcHandshake.S7;

/// <summary>Adapts <see cref="Plc"/> to <see cref="IS7PlcOperations"/>.</summary>
internal sealed class S7NetPlcOperations : IS7PlcOperations
{
    private readonly Plc _plc;

    public S7NetPlcOperations(Plc plc) => _plc = plc;

    public object? Read(DataType dataType, int db, int startByteAdr, VarType varType, int varCount, byte bitAdr = 0) =>
        _plc.Read(dataType, db, startByteAdr, varType, varCount, bitAdr);

    public void Write(DataType dataType, int db, int startByteAdr, object value, int bitAdr = -1) =>
        _plc.Write(dataType, db, startByteAdr, value, bitAdr);
}
