using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using NdtBundleService.DependencyInjection;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>Guards the production DI graph against cycles and missing registrations.</summary>
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
    public async Task AddNdtBundleServices_validates_on_build_and_resolves_all_hosted_services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IHostEnvironment>(new CompositionHostEnvironment(_tempRoot));

        var configuration = BuildCompositionConfiguration(_tempRoot);
        services.AddNdtBundleServices(configuration);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions
        {
            ValidateOnBuild = true,
            ValidateScopes = true
        });

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        Assert.NotEmpty(hostedServices);
        Assert.Contains(hostedServices, s => s is PoReopenWipConfirmationBridge);

        foreach (var hostedService in hostedServices)
            Assert.NotNull(hostedService);
    }

    private static IConfiguration BuildCompositionConfiguration(string tempRoot)
    {
        var serviceProjectDir = Path.Combine(FindRepositoryRoot(), "src", "NdtBundleService");
        var formationChart = Path.Combine(serviceProjectDir, "FormationChart.csv");

        return new ConfigurationBuilder()
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
            })
            .Build();
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
