using Bsdflow.UserValidator;
using Bsdflow.UserValidator.Messaging;
using Bsdflow.UserValidator.Validation;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration.AddEnvironmentVariables();

builder.Services.AddSingleton(KafkaOptions.Load(builder.Configuration));
builder.Services.AddSingleton<IKafkaPublisher, KafkaPublisher>();
builder.Services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
builder.Services.AddSingleton<IUserValidator, UserValidator>();
builder.Services.AddHostedService<Worker>();

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.TimestampFormat = "HH:mm:ss ";
    o.SingleLine = true;
});

var app = builder.Build();
await app.RunAsync();



