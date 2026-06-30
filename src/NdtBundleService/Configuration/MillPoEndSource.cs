namespace NdtBundleService.Configuration;

/// <summary>Per-mill PO-end trigger source (see <see cref="MillConfig.PoEndSource"/>).</summary>
public enum MillPoEndSource
{
  File,
  Plc,
  TcpOpen
}
