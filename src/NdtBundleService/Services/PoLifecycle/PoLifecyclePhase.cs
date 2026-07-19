namespace NdtBundleService.Services.PoLifecycle;

/// <summary>Per mill+PO lifecycle for Plc-sourced PO end (Phase 1 F-4).</summary>
public enum PoLifecyclePhase
{
    Running = 0,
    Draining = 1,
    Closed = 2
}
