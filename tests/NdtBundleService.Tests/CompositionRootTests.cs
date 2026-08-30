using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using NdtBundleService.Configuration;
using NdtBundleService.DependencyInjection;
using NdtBundleService.Services;
using NdtBundleService.Services.InstanceLease;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>Guards the production DI graph against cycles and missing registrations per InstanceRole.</summary>
public sealed class CompositionRootTests : IDisposable
{
    private readonly string _tempRoot;

    public CompositionRootTests()
    {
        _tempRoot = Path.Combine(Path.GetTempPath(), "ndt-composition-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempRoot);
        foreach (var sub in new[]
                 {
                     "Input Slit", "Input Slit Accepted", "Bundle", "Bundle Accepted",
                     "PO Accepted", "Slit Accepted", "NDT Output", "NDT Process", "Upload"
                 })
        {
            Directory.CreateDirectory(Path.Combine(_tempRoot, sub));
        }
    }

    [Fact]
    public async Task AddNdtBundleServices_Monolith_absent_InstanceRole_validates_on_build()
    {
        var provider = await BuildProviderAsync(BuildCompositionConfiguration(_tempRoot, roleOverrides: null));
        AssertMonolithHostedServices(provider);
    }

    [Fact]
    public async Task AddNdtBundleServices_Monolith_explicit_matches_absent_section()
    {
        var absent = await BuildProviderAsync(BuildCompositionConfiguration(_tempRoot, roleOverrides: null));
        var explicitMonolith = await BuildProviderAsync(BuildCompositionConfiguration(_tempRoot, new Dictionary<string, string?>
        {
            ["InstanceRole:Mode"] = InstanceRoleModes.Monolith,
        }));

        Assert.Equal(GetHostedServiceTypes(absent), GetHostedServiceTypes(explicitMonolith));
    }

    [Fact]
    public async Task AddNdtBundleServices_Shared_mode_validates_on_build()
    {
        var provider = await BuildProviderAsync(BuildCompositionConfiguration(_tempRoot, SharedRoleOverrides()));

        var types = GetHostedServiceTypes(provider);
        Assert.Contains(typeof(SqlTraceabilityStartupCheck), types);
        Assert.Contains(typeof(PoPlanCacheWarmupService), types);
        Assert.Contains(typeof(NdtInputSlitSapStatusWorker), types);
        Assert.DoesNotContain(typeof(PlcHandshakeWorker), types);
        Assert.DoesNotContain(typeof(SlitMonitoringWorker), types);
        Assert.DoesNotContain(typeof(PoReopenWipConfirmationBridge), types);
        Assert.DoesNotContain(typeof(MillInstanceLeaseHostedService), types);
        Assert.DoesNotContain(typeof(FillCutoverStartupCheck), types);
        Assert.DoesNotContain(typeof(MillSequenceStartupGuard), types);
        Assert.IsType<SqlZplGenerationToggle>(provider.GetRequiredService<IZplGenerationToggle>());
        Assert.NotNull(provider.GetRequiredService<IMillSequenceService>());
        Assert.NotNull(provider.GetRequiredService<IBundleMergeService>());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task AddNdtBundleServices_Mill_mode_validates_on_build(int millNo)
    {
        var provider = await BuildProviderAsync(BuildCompositionConfiguration(_tempRoot, MillRoleOverrides(millNo)));

        var types = GetHostedServiceTypes(provider);
        Assert.Contains(typeof(PlcHandshakeWorker), types);
        Assert.Contains(typeof(SlitMonitoringWorker), types);
        Assert.Contains(typeof(PoReopenWipConfirmationBridge), types);
        Assert.Contains(typeof(MillInstanceLeaseHostedService), types);
        Assert.Contains(typeof(FillCutoverStartupCheck), types);
        Assert.Contains(typeof(MillSequenceStartupGuard), types);
        Assert.DoesNotContain(typeof(PoPlanWipImportHostedService), types);
        Assert.DoesNotContain(typeof(UploadNdtBundleSchedulerWorker), types);
        Assert.DoesNotContain(typeof(NdtInputSlitSapStatusWorker), types);

        Assert.IsType<SqlZplGenerationToggle>(provider.GetRequiredService<IZplGenerationToggle>());
        Assert.Equal(millNo, provider.GetRequiredService<IMillOwnership>().SingleOwnedMill);
        Assert.NotNull(provider.GetRequiredService<IMillSequenceService>());
        Assert.Null(provider.GetService<IBundleMergeService>());

        var hosted = provider.GetServices<IHostedService>().Select(s => s.GetType()).ToList();
        Assert.True(
            hosted.IndexOf(typeof(MillInstanceLeaseHostedService))
            < hosted.IndexOf(typeof(SlitMonitoringWorker)));
    }

    [Fact]
    public void InstanceRole_absent_defaults_to_Monolith()
    {
        var config = BuildCompositionConfiguration(_tempRoot, roleOverrides: null);
        var role = config.GetSection(InstanceRoleOptions.SectionName).Get<InstanceRoleOptions>() ?? new InstanceRoleOptions();
        Assert.Equal(InstanceRoleModes.Monolith, role.Mode);
        Assert.True(role.EnableMillWorkers);
        Assert.True(role.EnableDashboardApi);
    }

    private static Dictionary<string, string?> SharedRoleOverrides() => new()
    {
        ["InstanceRole:Mode"] = InstanceRoleModes.Shared,
        ["InstanceRole:EnableDashboardApi"] = "true",
        ["InstanceRole:EnableMillWorkers"] = "false",
        ["InstanceRole:EnablePoPlanWipImport"] = "true",
        ["InstanceRole:EnableUploadScheduler"] = "true",
    };

    private static Dictionary<string, string?> MillRoleOverrides(int millNo) => new()
    {
        ["InstanceRole:Mode"] = InstanceRoleModes.Mill,
        ["InstanceRole:OwnedMillNos:0"] = millNo.ToString(),
        ["InstanceRole:EnableDashboardApi"] = "false",
        ["InstanceRole:EnableMillWorkers"] = "true",
        ["InstanceRole:EnablePoPlanWipImport"] = "false",
        ["InstanceRole:EnableUploadScheduler"] = "false",
    };

    private static void AssertMonolithHostedServices(ServiceProvider provider)
    {
        var types = GetHostedServiceTypes(provider);
        Assert.Contains(typeof(PoReopenWipConfirmationBridge), types);
        Assert.Contains(typeof(PlcHandshakeWorker), types);
        Assert.Contains(typeof(SlitMonitoringWorker), types);
        Assert.Contains(typeof(NdtInputSlitSapStatusWorker), types);
        Assert.Contains(typeof(MillInstanceLeaseHostedService), types);
        Assert.Contains(typeof(MillSequenceStartupGuard), types);
        Assert.IsType<ZplGenerationToggle>(provider.GetRequiredService<IZplGenerationToggle>());
        Assert.NotNull(provider.GetRequiredService<IMillSequenceService>());
        Assert.NotNull(provider.GetRequiredService<IBundleMergeService>());

        var hosted = provider.GetServices<IHostedService>().Select(s => s.GetType()).ToList();
        var lease = hosted.IndexOf(typeof(MillInstanceLeaseHostedService));
        var worker = hosted.IndexOf(typeof(SlitMonitoringWorker));
        var guard = hosted.IndexOf(typeof(MillSequenceStartupGuard));
        var fill = hosted.IndexOf(typeof(FillCutoverStartupCheck));
        Assert.True(guard >= 0 && fill >= 0 && lease >= 0 && worker >= 0);
        Assert.True(guard < fill, "MillSequenceStartupGuard must start before FillCutover (leftover JSON millMaxSequence).");
        Assert.True(fill < lease, "FillCutover must start after sequence seed.");
        Assert.True(lease < worker, "MillInstanceLeaseHostedService must start before mill workers.");

        foreach (var hostedService in provider.GetServices<IHostedService>())
            Assert.NotNull(hostedService);
    }

    private static async Task<ServiceProvider> BuildProviderAsync(IConfiguration configuration)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IHostEnvironment>(new CompositionHostEnvironment(
            configuration["NdtBundle:OutputBundleFolder"] ?? Path.GetTempPath()));
        services.AddSingleton<IHostApplicationLifetime, CompositionHostLifetime>();

        services.AddNdtBundleServices(configuration);

        var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true
        });

        foreach (var hostedService in provider.GetServices<IHostedService>())
            _ = hostedService;

        await Task.CompletedTask;
        return provider;
    }

    private static HashSet<Type> GetHostedServiceTypes(IServiceProvider provider) =>
        provider.GetServices<IHostedService>().Select(s => s.GetType()).ToHashSet();

    private static IConfiguration BuildCompositionConfiguration(
        string tempRoot,
        Dictionary<string, string?>? roleOverrides)
    {
        var serviceProjectDir = Path.Combine(FindRepositoryRoot(), "src", "NdtBundleService");
        var formationChart = Path.Combine(serviceProjectDir, "FormationChart.csv");

        var builder = new ConfigurationBuilder()
            .SetBasePath(serviceProjectDir)
            .AddJsonFile("appsettings.json", optional: false)
            .AddJsonFile("appsettings.Development.json", optional: true)
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["NdtBundle:UseSqlServerForBundles"] = "false",
                ["NdtBundle:ConnectionString"] = "",
                ["NdtBundle:PreferSqlForPoPlanWip"] = "false",
                ["NdtBundle:ImportPoPlanWipFromFolder"] = "false",
                ["NdtBundle:EnableUploadNdtBundleScheduler"] = "false",
                ["NdtBundle:EnableNdtBundleRuntimeStatePersistence"] = "false",
                ["NdtBundle:RuntimeStatePruning:RunOnStartup"] = "false",
                ["NdtBundle:BackfillReconciliationEnabled"] = "false",
                ["NdtBundle:RequireCleanFillCutover"] = "false",
                ["NdtBundle:InputSlitFolder"] = Path.Combine(tempRoot, "Input Slit"),
                ["NdtBundle:InputSlitAcceptedFolder"] = Path.Combine(tempRoot, "Input Slit Accepted"),
                ["NdtBundle:OutputBundleFolder"] = Path.Combine(tempRoot, "NDT Output"),
                ["NdtBundle:NdtProcessOutputFolder"] = Path.Combine(tempRoot, "NDT Process"),
                ["NdtBundle:PoPlanFolder"] = Path.Combine(tempRoot, "PO Accepted"),
                ["NdtBundle:SlitAcceptedFolder"] = Path.Combine(tempRoot, "Slit Accepted"),
                ["NdtBundle:UploadNdtBundleFilesFolder"] = Path.Combine(tempRoot, "Upload"),
                ["NdtBundle:FgBundleFolder"] = Path.Combine(tempRoot, "Bundle"),
                ["NdtBundle:FgBundleAcceptedFolder"] = Path.Combine(tempRoot, "Bundle Accepted"),
                ["NdtBundle:MillSlitLive:WipBundleFolder"] = Path.Combine(tempRoot, "Bundle"),
                ["NdtBundle:MillSlitLive:WipBundleAcceptedFolder"] = Path.Combine(tempRoot, "Bundle Accepted"),
                ["NdtBundle:FormationChartCsvPath"] = formationChart,
            });

        if (roleOverrides is not null)
            builder.AddInMemoryCollection(roleOverrides);

        return builder.Build();
    }

    private static string FindRepositoryRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "NdtBundleService.sln"))
                || Directory.Exists(Path.Combine(dir.FullName, ".git")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException("Could not locate repository root from test output directory.");
    }

    private sealed class CompositionHostEnvironment : IHostEnvironment
    {
        public CompositionHostEnvironment(string contentRoot)
        {
            ContentRootPath = contentRoot;
            ContentRootFileProvider = new PhysicalFileProvider(contentRoot);
        }

        public string EnvironmentName { get; set; } = Environments.Development;
        public string ApplicationName { get; set; } = "NdtBundleService.Tests";
        public string ContentRootPath { get; set; }
        public IFileProvider ContentRootFileProvider { get; set; }
    }

    private sealed class CompositionHostLifetime : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() { }
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempRoot))
                Directory.Delete(_tempRoot, recursive: true);
        }
        catch
        {
            // Best-effort cleanup for temp composition folders.
        }
    }
}
