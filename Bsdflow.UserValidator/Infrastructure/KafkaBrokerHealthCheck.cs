using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
namespace Bsdflow.UserValidator.Infrastructure;
public sealed class KafkaBrokerHealthCheck : IHealthCheck
{
    private readonly KafkaOptions _opts;
    private readonly Func<CancellationToken, IEnumerable<string>> _listTopics;
    public KafkaBrokerHealthCheck(IOptions<KafkaOptions> opts)
        : this(opts, listTopics: null) { }
    public KafkaBrokerHealthCheck(
        IOptions<KafkaOptions> opts,
        Func<CancellationToken, IEnumerable<string>>? listTopics)
    {
        _opts = opts.Value;
        _listTopics = listTopics ?? (_ =>
        {
            var cfg = new AdminClientConfig { BootstrapServers = _opts.BootstrapServers };
            using var admin = new AdminClientBuilder(cfg).Build();
            var md = admin.GetMetadata(TimeSpan.FromSeconds(2));
            return md.Topics.Select(t => t.Topic);
        });
    }
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var existing = _listTopics(cancellationToken).ToHashSet(StringComparer.Ordinal);
            var required = new[] { _opts.InputTopic, _opts.ValidTopic, _opts.ErrorTopic }
                           .Where(s => !string.IsNullOrWhiteSpace(s));
            var missing = required.Where(t => !existing.Contains(t!)).ToArray();
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
