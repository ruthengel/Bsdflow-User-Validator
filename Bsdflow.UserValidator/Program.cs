using Bsdflow.UserValidator;
using Bsdflow.UserValidator.Infrastructure;
using Bsdflow.UserValidator.Messaging;
using Bsdflow.UserValidator.Validation;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Configuration.AddEnvironmentVariables();

var kafkaOpts = KafkaOptions.Load(builder.Configuration);
builder.Services.AddSingleton(kafkaOpts);
builder.Services.AddSingleton<IOptions<KafkaOptions>>(sp => Options.Create(kafkaOpts));
builder.Services.AddSingleton<IKafkaPublisher, KafkaPublisher>();
builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
builder.Services.AddSingleton<IUserValidator, UserValidator>();

builder.Services.AddHealthChecks()
    .AddCheck("self", () => HealthCheckResult.Healthy())
    .AddCheck<KafkaBrokerHealthCheck>("kafka");


builder.Services.AddHostedService<Worker>();

builder.Logging.ClearProviders();
builder.Logging.AddConsole();

var app = builder.Build();

app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = r => r.Name == "self" });
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = r => r.Name == "kafka" });

if (!app.Urls.Any())
{
    app.Urls.Add("http://0.0.0.0:8088");
}

app.Run();
