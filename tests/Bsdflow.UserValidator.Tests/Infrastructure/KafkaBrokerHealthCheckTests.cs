using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Bsdflow.UserValidator.Infrastructure;
using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Xunit;
namespace Bsdflow.UserValidator.Tests.Infrastructure;
public class KafkaBrokerHealthCheckTests
{
    private static IOptions<KafkaOptions> Opts(string input, string valid, string error, string bootstrap = "any")
        => Options.Create(new KafkaOptions
        {
            BootstrapServers = bootstrap,
            InputTopic = input,
            ValidTopic = valid,
            ErrorTopic = error
        });
    [Fact]
    public async Task Healthy_When_AllRequiredTopicsExist()
    {
        var opts = Opts("users.in", "users.ok", "users.err");
        Func<CancellationToken, IEnumerable<string>> listTopics =
            _ => new[] { "users.in", "users.ok", "users.err" };
        var hc = new KafkaBrokerHealthCheck(opts, listTopics);
        var result = await hc.CheckHealthAsync(new HealthCheckContext());
        result.Status.Should().Be(HealthStatus.Healthy);
        result.Description.Should().Contain("Kafka reachable & topics exist");
    }
    [Fact]
    public async Task Degraded_When_Missing_Topics()
    {
        var opts = Opts("users.in", "users.ok", "users.err");
        Func<CancellationToken, IEnumerable<string>> listTopics =
            _ => new[] { "users.in", "users.ok" };
        var hc = new KafkaBrokerHealthCheck(opts, listTopics);
        var result = await hc.CheckHealthAsync(new HealthCheckContext());
        result.Status.Should().Be(HealthStatus.Degraded);
        result.Description.Should().Contain("missing topics").And.Contain("users.err");
    }
    [Fact]
    public async Task Ignores_Blank_Topics()
    {
        var opts = Opts("users.in", "users.ok", "");
        Func<CancellationToken, IEnumerable<string>> listTopics =
            _ => new[] { "users.in", "users.ok" };
        var hc = new KafkaBrokerHealthCheck(opts, listTopics);
        var result = await hc.CheckHealthAsync(new HealthCheckContext());
        result.Status.Should().Be(HealthStatus.Healthy);
    }
    [Fact]
    public async Task Unhealthy_When_Admin_Throws()
    {
        var opts = Opts("users.in", "users.ok", "users.err");
        Func<CancellationToken, IEnumerable<string>> listTopics =
            _ => throw new TimeoutException("boom");
        var hc = new KafkaBrokerHealthCheck(opts, listTopics);
        var result = await hc.CheckHealthAsync(new HealthCheckContext());
        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Exception.Should().BeOfType<TimeoutException>();
        result.Description.Should().Contain("Kafka unreachable");
    }
}