using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.FileBasedPoChange;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using NdtBundleService.Services.PlcHandshake.S7;
using NdtBundleService.Services.PoLifecycle;
using NdtBundleService.Services.TcpOpenComm;

namespace NdtBundleService.DependencyInjection;

/// <summary>Central DI registration for NdtBundleService (shared by Program and composition-root tests).</summary>
public static class NdtBundleServiceCollectionExtensions
{
    public static IServiceCollection AddNdtBundleServices(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<NdtBundleOptions>(configuration.GetSection("NdtBundle"));
        services.Configure<FileLoggingOptions>(configuration.GetSection("Logging:File"));

        services.AddSingleton<AppLogReader>();
        services.AddSingleton<IPoPlanProvider, PoPlanCsvProvider>();
        services.AddSingleton<IPoPlanWipRepository, PoPlanWipRepository>();
        services.AddSingleton<IPoPlanWipImporter, PoPlanWipImporter>();
        services.AddSingleton<IFormationChartProvider, FormationChartCsvProvider>();
        services.AddSingleton<IPipeSizeProvider, PipeSizeCsvProvider>();
        services.AddSingleton<IPoPlanWipEnrichmentProvider, PoPlanWipEnrichmentProvider>();
        services.AddSingleton<IBundleLabelInfoProvider, BundleLabelCsvProvider>();
        services.AddSingleton<ICurrentPoPlanService, CurrentPoPlanService>();
        services.AddSingleton<INdtBundleRepository, NdtBundleRepository>();
        services.AddSingleton<INdtBundleRuntimeStateStore, NdtBundleRuntimeStateStore>();
        services.AddSingleton<IBundleProvisionalStampCorrector, BundleProvisionalStampCorrector>();
        services.AddSingleton<IBundleEngine, NdtBundleEngine>();
        services.AddSingleton<IBundleOutputWriter, CsvBundleOutputWriter>();
        services.AddSingleton<INdtBatchStateService, NdtBatchStateService>();
        services.AddSingleton<INdtLabelPrinter, PdfNdtLabelPrinter>();
        services.AddSingleton<INdtBundleTagPrinter, NdtBundleTagPrintService>();
        services.AddSingleton<INetworkPrinterSender, NetworkPrinterSender>();
        services.AddSingleton<IWipLabelProvider, WipLabelProvider>();
        services.AddSingleton<INdtTagPrinter, NdtZplTagPrinter>();
        services.AddSingleton<IZplGenerationToggle, ZplGenerationToggle>();
        services.AddSingleton<SettingsAuthService>();
        services.AddSingleton<IMillPrinterSettingsService, MillPrinterSettingsService>();
        services.AddSingleton<IFormationChartSettingsService, FormationChartSettingsService>();
        services.AddSingleton<WipConfirmedRunningPoNotifier>();
        services.AddSingleton<IWipConfirmedRunningPoNotifier>(sp => sp.GetRequiredService<WipConfirmedRunningPoNotifier>());
        services.AddSingleton<IWipConfirmedRunningPoNotifierRegistration>(sp => sp.GetRequiredService<WipConfirmedRunningPoNotifier>());
        services.AddSingleton<IActivePoPerMillService, ActivePoPerMillService>();
        services.AddSingleton<IWipBundleRunningPoProvider, WipBundleRunningPoProvider>();
        services.AddSingleton<FileBasedPoChangeQueue>();
        services.AddSingleton<IWipBundleReconciliationService, WipBundleReconciliationService>();
        services.AddSingleton<PlcPoEndQueue>();
        services.AddSingleton<IS7ConnectionProviderRegistry, S7ConnectionProviderRegistry>();
        services.AddSingleton<IPlcSlitEndBundleCloser, PlcSlitEndBundleCloser>();
        services.AddSingleton<IHandshakeEventRepository, HandshakeEventRepository>();
        services.AddSingleton<IMillNdtCountReader, S7MillNdtCountReader>();
        services.AddSingleton<IMillSlitLiveNdtAccumulator, MillSlitLiveNdtAccumulator>();
        services.AddSingleton<IMillBundleStateLock, MillBundleStateLock>();
        services.AddSingleton<IPoLifecycleService, PoLifecycleService>();
        services.AddSingleton<PoReopenService>();
        services.AddSingleton<PoEndWorkflowService>();
        services.AddSingleton<IPoEndWorkflowService>(sp => sp.GetRequiredService<PoEndWorkflowService>());
        services.AddSingleton<MillPoEndTransitionDetector>();
        services.AddSingleton<PoEndDetectionDiagnostics>();
        services.AddSingleton<PlcPoEndPollHandler>();
        services.AddSingleton<PlcConnectionHealth>();
        services.AddSingleton<PlcHandshakeStatusRegistry>();
        services.AddSingleton<PlcHandshakeCoordinator>();
        services.AddSingleton<IPoChangeHandler, PoChangeHandler>();
        services.AddSingleton<IMillHooterPlcValuesService, MillHooterPlcValuesService>();
        services.AddSingleton<IPlcClient>(sp =>
        {
            var bundleOptions = sp.GetRequiredService<IOptions<NdtBundleOptions>>().Value;
            var handshake = bundleOptions.PlcHandshake ?? new PlcHandshakeOptions();
            if (handshake.Enabled)
            {
                return new PlcHandshakeMirrorPlcClient(sp.GetRequiredService<PlcHandshakeStatusRegistry>());
            }

            var plc = bundleOptions.PlcPoEnd ?? new PlcPoEndOptions();
            if (plc.Enabled && PlcPoEndOptions.IsS7Driver(plc))
            {
                return new S7MillPoEndPlcClient(
                    sp.GetRequiredService<IOptions<NdtBundleOptions>>(),
                    sp.GetRequiredService<PlcConnectionHealth>(),
                    sp.GetRequiredService<ILogger<S7MillPoEndPlcClient>>());
            }

            if (plc.Enabled && string.Equals(plc.Driver, "ModbusTcp", StringComparison.OrdinalIgnoreCase))
            {
                return new ModbusTcpMillPoEndPlcClient(
                    sp.GetRequiredService<IOptions<NdtBundleOptions>>(),
                    sp.GetRequiredService<PlcConnectionHealth>(),
                    sp.GetRequiredService<ILogger<ModbusTcpMillPoEndPlcClient>>());
            }

            return new StubPlcClient(sp.GetRequiredService<ILogger<StubPlcClient>>());
        });
        services.AddSingleton<IManualNdtTagService, ManualNdtTagService>();
        services.AddSingleton<IUploadNdtBundleFileService, UploadNdtBundleFileService>();
        services.AddSingleton<ITraceabilityRepository, TraceabilityRepository>();
        services.AddSingleton<IReconcileSyncService, ReconcileSyncService>();
        services.AddSingleton<IReconcileBundleTagService, ReconcileBundleTagService>();
        services.AddSingleton<ISqlTraceabilityWriteTracker, SqlTraceabilityWriteTracker>();
        services.AddSingleton<ISqlTraceabilityHealth, SqlTraceabilityHealth>();

        services.AddHostedService<SqlTraceabilityStartupCheck>();
        services.AddHostedService<PoPlanWipImportHostedService>();
        services.AddHostedService<PoPlanCacheWarmupService>();
        services.AddHostedService<PoReopenWipConfirmationBridge>();
        services.AddHostedService<PlcHandshakeWorker>();
        services.AddHostedService<PlcPoEndQueueWorker>();
        services.AddHostedService<MillTcpOpenCommWorker>();
        services.AddHostedService<FileBasedPoChangeWorker>();
        services.AddHostedService<WipBundleFileReconciliationWorker>();
        services.AddHostedService<PoLifecycleSweepWorker>();
        services.AddHostedService<SlitMonitoringWorker>();
        services.AddHostedService<UploadNdtBundleSchedulerWorker>();

        return services;
    }
}
