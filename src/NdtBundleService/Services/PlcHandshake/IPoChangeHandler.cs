using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>Invoked when a mill PLC raises the PO-change trigger bit (M_x.6).</summary>
public interface IPoChangeHandler
{
    Task HandlePoChangeAsync(MillConfig mill, CancellationToken cancellationToken);
}
