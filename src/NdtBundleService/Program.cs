using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NdtBundleService.Configuration;
using NdtBundleService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddWindowsService();

// Bind options from configuration (appsettings.json, environment, etc.)
builder.Services.Configure<NdtBundleOptions>(builder.Configuration.GetSection("NdtBundle"));

// Core services
builder.Services.AddSingleton<IPoPlanProvider, PoPlanCsvProvider>();
builder.Services.AddSingleton<IFormationChartProvider, FormationChartCsvProvider>();
builder.Services.AddSingleton<IPipeSizeProvider, PipeSizeCsvProvider>();
builder.Services.AddSingleton<IBundleLabelInfoProvider, BundleLabelCsvProvider>();
builder.Services.AddSingleton<IBundleEngine, NdtBundleEngine>();
builder.Services.AddSingleton<IBundleOutputWriter, CsvBundleOutputWriter>();
builder.Services.AddSingleton<INdtLabelPrinter, StubNdtLabelPrinter>();
builder.Services.AddSingleton<IPlcClient, StubPlcClient>();

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

app.MapControllers();

app.Run();

