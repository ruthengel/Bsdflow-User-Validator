using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Bsdflow.UserValidator.Infrastructure;

public sealed class KafkaBrokerHealthCheck : IHealthCheck
{
    private readonly KafkaOptions _opts;
    public KafkaBrokerHealthCheck(IOptions<KafkaOptions> opts) => _opts = opts.Value;

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var cfg = new AdminClientConfig { BootstrapServers = _opts.BootstrapServers };
            using var admin = new AdminClientBuilder(cfg).Build();
            var md = admin.GetMetadata(TimeSpan.FromSeconds(2));

            var required = new[] { _opts.InputTopic, _opts.ValidTopic, _opts.ErrorTopic }
                           .Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();

            var missing = required.Where(t => !md.Topics.Any(x => x.Topic == t)).ToArray();

            if (missing.Length == 0)
                return Task.FromResult(HealthCheckResult.Healthy("Kafka reachable & topics exist"));

            return Task.FromResult(new HealthCheckResult(
                status: HealthStatus.Degraded,
                description: $"Kafka reachable, but missing topics: {string.Join(", ", missing)}"));
        }
        catch (Exception ex)
        {
            return Task.FromResult(new HealthCheckResult(
                status: HealthStatus.Unhealthy,
                description: "Kafka unreachable",
                exception: ex));
        }
    }
}
