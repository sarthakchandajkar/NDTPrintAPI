using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using QuestPDF.Infrastructure;
using Serilog;
using Serilog.Events;

QuestPDF.Settings.License = LicenseType.Community;
QuestPDF.Settings.EnableDebugging = true; // Better error location when layout constraints conflict

// Production hosting loads appsettings.Production.json (e.g. Z:\ Input Slit) even when debugging from the repo.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    EnvironmentName = Environments.Production
});

ConfigureSerilog(builder);

builder.Services.AddWindowsService();

// Bind options from configuration (appsettings.json, environment, etc.)
builder.Services.Configure<NdtBundleOptions>(builder.Configuration.GetSection("NdtBundle"));
builder.Services.Configure<FileLoggingOptions>(builder.Configuration.GetSection("Logging:File"));
builder.Services.AddSingleton<AppLogReader>();

// Core services
builder.Services.AddSingleton<IPoPlanProvider, PoPlanCsvProvider>();
builder.Services.AddSingleton<IFormationChartProvider, FormationChartCsvProvider>();
builder.Services.AddSingleton<IPipeSizeProvider, PipeSizeCsvProvider>();
builder.Services.AddSingleton<IBundleLabelInfoProvider, BundleLabelCsvProvider>();
builder.Services.AddSingleton<ICurrentPoPlanService, CurrentPoPlanService>();
builder.Services.AddSingleton<INdtBundleRepository, NdtBundleRepository>();
builder.Services.AddSingleton<INdtBundleRuntimeStateStore, NdtBundleRuntimeStateStore>();
builder.Services.AddSingleton<IBundleEngine, NdtBundleEngine>();
builder.Services.AddSingleton<IBundleOutputWriter, CsvBundleOutputWriter>();
builder.Services.AddSingleton<INdtBatchStateService, NdtBatchStateService>();
builder.Services.AddSingleton<INdtLabelPrinter, PdfNdtLabelPrinter>();
builder.Services.AddSingleton<INdtBundleTagPrinter, NdtBundleTagPrintService>();
builder.Services.AddSingleton<INetworkPrinterSender, NetworkPrinterSender>();
builder.Services.AddSingleton<IWipLabelProvider, WipLabelProvider>();
builder.Services.AddSingleton<INdtTagPrinter, NdtZplTagPrinter>();
builder.Services.AddSingleton<IZplGenerationToggle, ZplGenerationToggle>();
builder.Services.AddSingleton<SettingsAuthService>();
builder.Services.AddSingleton<IMillPrinterSettingsService, MillPrinterSettingsService>();
builder.Services.AddSingleton<IFormationChartSettingsService, FormationChartSettingsService>();
builder.Services.AddSingleton<IActivePoPerMillService, ActivePoPerMillService>();
builder.Services.AddSingleton<IWipBundleRunningPoProvider, WipBundleRunningPoProvider>();
builder.Services.AddSingleton<IMillNdtCountReader, S7MillNdtCountReader>();
builder.Services.AddSingleton<IMillSlitLiveNdtAccumulator, MillSlitLiveNdtAccumulator>();
builder.Services.AddSingleton<IPoEndWorkflowService, PoEndWorkflowService>();
builder.Services.AddSingleton<MillPoEndTransitionDetector>();
builder.Services.AddSingleton<PoEndDetectionDiagnostics>();
builder.Services.AddSingleton<PlcPoEndPollHandler>();
builder.Services.AddSingleton<PlcConnectionHealth>();
builder.Services.AddSingleton<IPlcClient>(sp =>
{
    var bundleOptions = sp.GetRequiredService<IOptions<NdtBundleOptions>>().Value;
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
builder.Services.AddSingleton<IManualNdtTagService, ManualNdtTagService>();
builder.Services.AddSingleton<IUploadNdtBundleFileService, UploadNdtBundleFileService>();
builder.Services.AddSingleton<ITraceabilityRepository, TraceabilityRepository>();
builder.Services.AddSingleton<IReconcileSyncService, ReconcileSyncService>();
builder.Services.AddSingleton<ISqlTraceabilityWriteTracker, SqlTraceabilityWriteTracker>();
builder.Services.AddSingleton<ISqlTraceabilityHealth, SqlTraceabilityHealth>();

builder.Services.AddHostedService<SqlTraceabilityStartupCheck>();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
});

// Controllers & Swagger UI for testing
builder.Services.AddControllers().AddJsonOptions(o =>
{
    // Match ndtbundle-dashboard (expects mills, sourcePath, liveMillNdt).
    o.JsonSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    o.JsonSerializerOptions.DictionaryKeyPolicy = JsonNamingPolicy.CamelCase;
});
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background worker that orchestrates the flow
builder.Services.AddHostedService<SlitMonitoringWorker>();
builder.Services.AddHostedService<UploadNdtBundleSchedulerWorker>();

try
{
    var app = builder.Build();

    if (app.Environment.IsDevelopment() || app.Configuration.GetValue<bool>("ShowSwagger"))
    {
        app.UseSwagger();
        app.UseSwaggerUI();
    }

    app.UseCors();

    app.MapControllers();

    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "NdtBundleService terminated unexpectedly.");
    throw;
}
finally
{
    Log.CloseAndFlush();
}

static void ConfigureSerilog(WebApplicationBuilder builder)
{
    var fileLogging = builder.Configuration.GetSection("Logging:File").Get<FileLoggingOptions>() ?? new FileLoggingOptions();
    var defaultLevel = builder.Configuration["Logging:LogLevel:Default"];
    var minLevel = Enum.TryParse<LogEventLevel>(defaultLevel, ignoreCase: true, out var parsed)
        ? parsed
        : LogEventLevel.Information;

    var loggerConfig = new LoggerConfiguration()
        .MinimumLevel.Is(minLevel)
        .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
        .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Information)
        .Enrich.FromLogContext();

    if (fileLogging.Enabled)
    {
        var logFolder = string.IsNullOrWhiteSpace(fileLogging.Folder)
            ? Path.Combine(builder.Environment.ContentRootPath, "Logs")
            : fileLogging.Folder.Trim();
        Directory.CreateDirectory(logFolder);

        var prefix = string.IsNullOrWhiteSpace(fileLogging.FileNamePrefix) ? "ndtbundle" : fileLogging.FileNamePrefix.Trim();
        var logPath = Path.Combine(logFolder, $"{prefix}-.log");
        var retain = fileLogging.RetainFileCount > 0 ? fileLogging.RetainFileCount : 31;

        loggerConfig = loggerConfig.WriteTo.File(
            logPath,
            rollingInterval: RollingInterval.Day,
            retainedFileCountLimit: retain,
            shared: true,
            outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}");
    }

    if (OperatingSystem.IsWindows() && fileLogging.WriteToEventLog)
    {
        loggerConfig = loggerConfig.WriteTo.EventLog(
            "NdtBundleService",
            manageEventSource: false,
            restrictedToMinimumLevel: LogEventLevel.Warning);
    }

    Log.Logger = loggerConfig.CreateLogger();
    builder.Host.UseSerilog();
}

