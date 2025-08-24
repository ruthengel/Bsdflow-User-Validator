//using Bsdflow.UserValidator;

//var builder = Host.CreateApplicationBuilder(args);
//builder.Services.AddHostedService<Worker>();

//var host = builder.Build();
//host.Run();

using Bsdflow.UserValidator;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((ctx, cfg) =>
    {
        cfg.AddEnvironmentVariables(); 
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddSimpleConsole(o =>
        {
            o.TimestampFormat = "HH:mm:ss ";
            o.SingleLine = true;
        });
    })
    .ConfigureServices((ctx, services) =>
    {
        var kafka = KafkaOptions.Load(ctx.Configuration);
        services.AddSingleton(kafka);
        services.AddHostedService<Worker>(); 
    })
    .Build();

await host.RunAsync();

