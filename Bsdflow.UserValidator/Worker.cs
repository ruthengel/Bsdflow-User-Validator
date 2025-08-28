
using System.Text.Json;
using Bsdflow.UserValidator.Messaging;
using Bsdflow.UserValidator.Domain;
using Bsdflow.UserValidator.Validation;


namespace Bsdflow.UserValidator;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _log;
    private readonly KafkaOptions _opts;
    private readonly IKafkaConsumer _consumer;
    private readonly IKafkaPublisher _publisher;
    private readonly IUserValidator _validator;
    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);


    public Worker(ILogger<Worker> log, KafkaOptions opts, IKafkaConsumer consumer, IKafkaPublisher publisher, IUserValidator validator)
    {
        _log = log;
        _opts = opts;
        _consumer = consumer;
        _publisher = publisher;
        _validator = validator;
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

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _log.LogInformation("Stopping service - draining...");
        try
        {
            await _publisher.FlushAsync(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "Flush failed on shutdown");
        }
        await base.StopAsync(cancellationToken);
    }


    private async Task<ProcessingResult> HandlerAsync(KafkaEnvelope env, CancellationToken ct)
    {
        using var _ = _log.BeginScope(new Dictionary<string, object?>
        {
            ["msgId"] = env.MessageId,
            ["corrId"] = env.CorrelationId,
            ["topicIn"] = env.Topic,
            ["partition"] = env.Partition,
            ["offset"] = env.Offset
        });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var input = JsonSerializer.Deserialize<UserInput>(env.Value, _jsonOptions)
                        ?? new UserInput(null, null, null, null);

            _log.LogDebug("Parsed JSON to UserInput");

            var result = _validator.Validate(input);
            _log.LogInformation("Validation done | isValid={IsValid} errors={ErrCount}",
                                result.IsValid, result.Errors.Count);

            if (result.IsValid)
            {
                var payload = JsonSerializer.Serialize(new
                {
                    messageId = env.MessageId,
                    correlationId = env.CorrelationId,
                    user = result.User
                });

                await _publisher.PublishAsync(_opts.ValidTopic!, env.Key, payload,
                                              env.MessageId, env.CorrelationId, ct);

                _log.LogInformation("Published to {Topic} | durationMs={Ms}",
                                    _opts.ValidTopic, sw.ElapsedMilliseconds);

                return new ProcessingResult(true, payload, env.Key);
            }
            else
            {
                var payload = JsonSerializer.Serialize(new
                {
                    messageId = env.MessageId,
                    correlationId = env.CorrelationId,
                    errors = result.Errors,
                    original = input
                });

                await _publisher.PublishAsync(_opts.ErrorTopic!, env.Key, payload,
                                              env.MessageId, env.CorrelationId, ct);

                _log.LogWarning("Published to {Topic} (INVALID) | errors={ErrCount} durationMs={Ms}",
                                _opts.ErrorTopic, result.Errors.Count, sw.ElapsedMilliseconds);

                return new ProcessingResult(false, payload, env.Key);
            }
        }
        catch (JsonException jx)
        {
            var payload = JsonSerializer.Serialize(new
            {
                messageId = env.MessageId,
                correlationId = env.CorrelationId,
                reason = "invalid_json",
                details = jx.Message,
                original = env.Value
            });

            await _publisher.PublishAsync(_opts.ErrorTopic!, env.Key, payload,
                                          env.MessageId, env.CorrelationId, ct);

            _log.LogWarning(jx, "JSON parse failed | durationMs={Ms}", sw.ElapsedMilliseconds);
            return new ProcessingResult(false, payload, env.Key);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Unexpected failure | durationMs={Ms}", sw.ElapsedMilliseconds);
            throw; 
        }
    }

}
