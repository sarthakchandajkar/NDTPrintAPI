using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using QuestPDF.Infrastructure;

QuestPDF.Settings.License = LicenseType.Community;
QuestPDF.Settings.EnableDebugging = true; // Better error location when layout constraints conflict

// Production hosting loads appsettings.Production.json (e.g. Z:\ Input Slit) even when debugging from the repo.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = args,
    EnvironmentName = Environments.Production
});

builder.Services.AddWindowsService();

// Bind options from configuration (appsettings.json, environment, etc.)
builder.Services.Configure<NdtBundleOptions>(builder.Configuration.GetSection("NdtBundle"));

// Core services
builder.Services.AddSingleton<IPoPlanProvider, PoPlanCsvProvider>();
builder.Services.AddSingleton<IFormationChartProvider, FormationChartCsvProvider>();
builder.Services.AddSingleton<IPipeSizeProvider, PipeSizeCsvProvider>();
builder.Services.AddSingleton<IBundleLabelInfoProvider, BundleLabelCsvProvider>();
builder.Services.AddSingleton<ICurrentPoPlanService, CurrentPoPlanService>();
builder.Services.AddSingleton<INdtBundleRepository, NdtBundleRepository>();
builder.Services.AddSingleton<IBundleEngine, NdtBundleEngine>();
builder.Services.AddSingleton<IBundleOutputWriter, CsvBundleOutputWriter>();
builder.Services.AddSingleton<INdtBatchStateService, NdtBatchStateService>();
builder.Services.AddSingleton<INdtLabelPrinter, PdfNdtLabelPrinter>();
builder.Services.AddSingleton<INdtBundleTagPrinter, NdtBundleTagPrintService>();
builder.Services.AddSingleton<INetworkPrinterSender, NetworkPrinterSender>();
builder.Services.AddSingleton<IWipLabelProvider, WipLabelProvider>();
builder.Services.AddSingleton<INdtTagPrinter, NdtZplTagPrinter>();
builder.Services.AddSingleton<IZplGenerationToggle, ZplGenerationToggle>();
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
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Background worker that orchestrates the flow
builder.Services.AddHostedService<SlitMonitoringWorker>();
builder.Services.AddHostedService<UploadNdtBundleSchedulerWorker>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors();

app.MapControllers();

app.Run();

