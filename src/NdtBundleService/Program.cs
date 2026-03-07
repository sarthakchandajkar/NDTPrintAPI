using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using QuestPDF.Infrastructure;

QuestPDF.Settings.License = LicenseType.Community;
QuestPDF.Settings.EnableDebugging = true; // Better error location when layout constraints conflict

var builder = WebApplication.CreateBuilder(args);

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
builder.Services.AddSingleton<IPlcClient, StubPlcClient>();

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

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors();

app.MapControllers();

app.Run();

