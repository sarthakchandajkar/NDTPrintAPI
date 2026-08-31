using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.FileBasedPoChange;
using NdtBundleService.Services.InstanceLease;
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
        var roleSection = configuration.GetSection(InstanceRoleOptions.SectionName);
        services.AddSingleton<IValidateOptions<InstanceRoleOptions>, InstanceRoleOptionsValidator>();
        services.AddOptions<InstanceRoleOptions>()
            .Bind(roleSection)
            .ValidateOnStart();

        var role = roleSection.Get<InstanceRoleOptions>() ?? new InstanceRoleOptions();

        services.AddSingleton<IValidateOptions<NdtBundleOptions>, NdtBundleOptionsValidator>();
        services.AddOptions<NdtBundleOptions>()
            .Bind(configuration.GetSection("NdtBundle"))
            .ValidateOnStart();
        services.Configure<FileLoggingOptions>(configuration.GetSection("Logging:File"));

        services.AddSingleton<IMillOwnership, MillOwnership>();

        AddCoreServices(services, role);
        AddZplToggle(services, role);

        // Mill singletons (PLC registry, PO lifecycle, runtime state) are required by dashboard Test/Settings
        // endpoints even on Shared; hosted mill workers are registered separately.
        if (role.IsMonolith || role.EnableMillWorkers || role.EnableDashboardApi)
            AddMillSingletonServices(services);

        AddHostedServices(services, role, configuration);

        return services;
    }

    private static void AddCoreServices(IServiceCollection services, InstanceRoleOptions role)
    {
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
        services.AddSingleton<ICsvFillService, CsvFillService>();
        services.AddSingleton<SlitCsvFillAssigner>();
        services.AddSingleton<ITraceabilityRepository, TraceabilityRepository>();
        services.AddSingleton<IOutputSlitSapStatusRepository, OutputSlitSapStatusRepository>();
        services.AddSingleton<IPpcCorrectionRepository, PpcCorrectionRepository>();
        services.AddSingleton<IMillSequenceService, MillSequenceService>();
        services.AddSingleton<INdtBundleRuntimeStateStore, NdtBundleRuntimeStateStore>();
        services.AddSingleton<IResubmitDriftService, ResubmitDriftService>();
        services.AddSingleton<IReconcileSyncService, ReconcileSyncService>();
        services.AddSingleton<ISqlTraceabilityWriteTracker, SqlTraceabilityWriteTracker>();
        services.AddSingleton<ISqlTraceabilityHealth, SqlTraceabilityHealth>();
        services.AddSingleton<SettingsAuthService>();
        services.AddSingleton<IMillPrinterSettingsService, MillPrinterSettingsService>();
        services.AddSingleton<INetworkPrinterSender, NetworkPrinterSender>();
        services.AddSingleton<IWipLabelProvider, WipLabelProvider>();
        services.AddSingleton<INdtTagPrinter, NdtZplTagPrinter>();
        services.AddSingleton<INdtLabelPrinter, PdfNdtLabelPrinter>();
        services.AddSingleton<INdtBundleTagPrinter, NdtBundleTagPrintService>();
        services.AddSingleton<IUploadNdtBundleFileService, UploadNdtBundleFileService>();
        services.AddSingleton<IAppSettingRepository, AppSettingRepository>();
        services.AddSingleton<IMillInstanceLeaseService, MillInstanceLeaseService>();

        // Settings (printers / formation / ZPL) live on Shared and Mill instances.
        if (role.IsMonolith || role.EnableDashboardApi || role.IsMill)
        {
            services.AddSingleton<IFormationChartSettingsService, FormationChartSettingsService>();
        }

        if (role.IsMonolith || role.EnableDashboardApi)
        {
            services.AddSingleton<IManualNdtTagService, ManualNdtTagService>();
            services.AddSingleton<IReconcileBundleTagService, ReconcileBundleTagService>();
            services.AddSingleton<IBundleMergeService, BundleMergeService>();
        }

        services.AddHostedService<SqlTraceabilityStartupCheck>();
        services.AddHostedService<LegacyJsonStateStartupCheck>();
        services.AddHostedService<SourceFileEligibilityStartupLog>();
        services.AddHostedService<PoPlanCacheWarmupService>();
    }

    private static void AddMillSingletonServices(IServiceCollection services)
    {
        services.AddSingleton<IBundleEngine, NdtBundleEngine>();
        services.AddSingleton<IBundleOutputWriter, CsvBundleOutputWriter>();
        services.AddSingleton<INdtBatchStateService, NdtBatchStateService>();
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
        services.AddSingleton<OpenAccumulationOverrideService>();
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
    }

    private static void AddZplToggle(IServiceCollection services, InstanceRoleOptions role)
    {
        if (role.IsMonolith)
            services.AddSingleton<IZplGenerationToggle, ZplGenerationToggle>();
        else
            services.AddSingleton<IZplGenerationToggle, SqlZplGenerationToggle>();
    }

    private static void AddHostedServices(
        IServiceCollection services,
        InstanceRoleOptions role,
        IConfiguration configuration)
    {
        var bundleOptions = configuration.GetSection("NdtBundle").Get<NdtBundleOptions>() ?? new NdtBundleOptions();

        if (role.IsMonolith || role.IsMill)
        {
            // Seed/guard Mill_Sequence before FillCutover (open Bundle_Accumulation EXISTS).
            services.AddHostedService<MillSequenceStartupGuard>();
            services.AddHostedService<FillCutoverStartupCheck>();
            // Lease claim must complete before mill workers StartAsync (registration order = start order).
            services.AddHostedService<MillInstanceLeaseHostedService>();
        }

        if (role.IsMonolith || role.EnableMillWorkers)
        {
            services.AddHostedService<PoReopenWipConfirmationBridge>();
            services.AddHostedService<PlcHandshakeWorker>();
            services.AddHostedService<PlcPoEndQueueWorker>();
            services.AddHostedService<MillTcpOpenCommWorker>();
            services.AddHostedService<FileBasedPoChangeWorker>();
            services.AddHostedService<WipBundleFileReconciliationWorker>();
            services.AddHostedService<PoLifecycleSweepWorker>();
            services.AddHostedService<SlitMonitoringWorker>();
        }

        if (role.IsMonolith || role.IsShared)
            services.AddHostedService<NdtInputSlitSapStatusWorker>();

        if (role.IsMonolith || role.EnablePoPlanWipImport)
        {
            if (PoPlanWipImportSettings.IsEnabled(bundleOptions))
                services.AddHostedService<PoPlanWipImportHostedService>();
        }
    }
}
