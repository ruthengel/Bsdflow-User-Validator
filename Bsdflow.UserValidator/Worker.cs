using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Bsdflow.UserValidator;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _log;
    private readonly KafkaOptions _kafka;

    public Worker(ILogger<Worker> log, KafkaOptions kafka)
    {
        _log = log;
        _kafka = kafka;
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _log.LogInformation("Startup | Kafka Config: {@Kafka}", new
        {
            _kafka.BootstrapServers,
            _kafka.GroupId,
            _kafka.InputTopic,
            _kafka.ValidTopic,
            _kafka.ErrorTopic,
            _kafka.AutoOffsetReset,
            _kafka.EnableAutoCommit
        });
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("Worker running (Setup phase). Kafka integration disabled in Task 1.");
        while (!stoppingToken.IsCancellationRequested)
        {
            _log.LogDebug("Heartbeat…");
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _log.LogInformation("Worker stopping gracefully.");
        return base.StopAsync(cancellationToken);
    }
}
