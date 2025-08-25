
using System.Text.Json;
using Bsdflow.UserValidator.Messaging;

namespace Bsdflow.UserValidator;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _log;
    private readonly KafkaOptions _opts;
    private readonly IKafkaConsumer _consumer;
    private readonly IKafkaPublisher _publisher;

    public Worker(ILogger<Worker> log, KafkaOptions opts, IKafkaConsumer consumer, IKafkaPublisher publisher)
    {
        _log = log;
        _opts = opts;
        _consumer = consumer;
        _publisher = publisher;
    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_opts.BootstrapServers) ||
            string.Equals(_opts.BootstrapServers, "placeholder:9092", StringComparison.OrdinalIgnoreCase))
        {
            _log.LogInformation("Worker running (Setup phase). Kafka integration disabled in Task 2 until real broker.");
            while (!stoppingToken.IsCancellationRequested)
            {
                _log.LogDebug("Heartbeat…");
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            return;
        }

        _log.LogInformation("Starting Kafka pipeline | input={Input} validOut={Valid} errorOut={Error}",
            _opts.InputTopic, _opts.ValidTopic, _opts.ErrorTopic);

        await _consumer.RunAsync(_opts.InputTopic!, HandlerAsync, stoppingToken);
    }

    private async Task<ProcessingResult> HandlerAsync(KafkaEnvelope env, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            using var doc = JsonDocument.Parse(env.Value); 

            var outputJson = env.Value;

            await _publisher.PublishAsync(_opts.ValidTopic!, env.Key, outputJson,
                                          env.MessageId, env.CorrelationId, ct);

            _log.LogInformation("Processed OK | msgId={MsgId} durationMs={Ms}", env.MessageId, sw.ElapsedMilliseconds);
            return new ProcessingResult(IsValid: true, OutputJson: outputJson, OutputKey: env.Key);
        }
        catch (JsonException jx)
        {
            var errorPayload = JsonSerializer.Serialize(new
            {
                messageId = env.MessageId,
                correlationId = env.CorrelationId,
                reason = "invalid_json",
                details = jx.Message,
                original = env.Value
            });

            await _publisher.PublishAsync(_opts.ErrorTopic!, env.Key, errorPayload,
                                          env.MessageId, env.CorrelationId, ct);

            _log.LogWarning("Processed ERROR (invalid json) | msgId={MsgId} durationMs={Ms}", env.MessageId, sw.ElapsedMilliseconds);
            return new ProcessingResult(IsValid: false, OutputJson: errorPayload, OutputKey: env.Key);
        }
    }
}
