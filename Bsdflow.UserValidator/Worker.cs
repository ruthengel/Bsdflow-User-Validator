
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

    private async Task<ProcessingResult> HandlerAsync(KafkaEnvelope env, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            var input = JsonSerializer.Deserialize<UserInput>(env.Value, _jsonOptions)
                        ?? new UserInput(null, null, null, null);

            var result = _validator.Validate(input);

            if (result.IsValid && result.User is not null)
            {
                var payload = new
                {
                    messageId = env.MessageId,
                    correlationId = env.CorrelationId,
                    user = result.User
                };

                var outputJson = JsonSerializer.Serialize(payload, _jsonOptions);

                await _publisher.PublishAsync(
                    _opts.ValidTopic!, env.Key, outputJson,
                    env.MessageId, env.CorrelationId, ct);

                _log.LogInformation(
                    "Processed OK | msgId={MsgId} durationMs={Ms}",
                    env.MessageId, sw.ElapsedMilliseconds);

                return new ProcessingResult(
                    IsValid: true, OutputJson: outputJson, OutputKey: env.Key);
            }
            else
            {
                var errorPayload = new
                {
                    messageId = env.MessageId,
                    correlationId = env.CorrelationId,
                    reason = "validation_failed",
                    errors = result.Errors,  
                    original = input      
                };

                var errorJson = JsonSerializer.Serialize(errorPayload, _jsonOptions);

                await _publisher.PublishAsync(
                    _opts.ErrorTopic!, env.Key, errorJson,
                    env.MessageId, env.CorrelationId, ct);

                _log.LogWarning(
                    "Processed ERROR (validation) | msgId={MsgId} errors={Count} durationMs={Ms}",
                    env.MessageId, result.Errors.Count, sw.ElapsedMilliseconds);

                return new ProcessingResult(
                    IsValid: false, OutputJson: errorJson, OutputKey: env.Key);
            }
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
            }, _jsonOptions);

            await _publisher.PublishAsync(
                _opts.ErrorTopic!, env.Key, errorPayload,
                env.MessageId, env.CorrelationId, ct);

            _log.LogWarning(
                "Processed ERROR (invalid json) | msgId={MsgId} durationMs={Ms}",
                env.MessageId, sw.ElapsedMilliseconds);

            return new ProcessingResult(
                IsValid: false, OutputJson: errorPayload, OutputKey: env.Key);
        }
    }

    //private async Task<ProcessingResult> HandlerAsync(KafkaEnvelope env, CancellationToken ct)
    //{
    //    var sw = System.Diagnostics.Stopwatch.StartNew();
    //    try
    //    {
    //        using var doc = JsonDocument.Parse(env.Value); 

    //        var outputJson = env.Value;

    //        await _publisher.PublishAsync(_opts.ValidTopic!, env.Key, outputJson,
    //                                      env.MessageId, env.CorrelationId, ct);

    //        _log.LogInformation("Processed OK | msgId={MsgId} durationMs={Ms}", env.MessageId, sw.ElapsedMilliseconds);
    //        return new ProcessingResult(IsValid: true, OutputJson: outputJson, OutputKey: env.Key);
    //    }
    //    catch (JsonException jx)
    //    {
    //        var errorPayload = JsonSerializer.Serialize(new
    //        {
    //            messageId = env.MessageId,
    //            correlationId = env.CorrelationId,
    //            reason = "invalid_json",
    //            details = jx.Message,
    //            original = env.Value
    //        });

    //        await _publisher.PublishAsync(_opts.ErrorTopic!, env.Key, errorPayload,
    //                                      env.MessageId, env.CorrelationId, ct);

    //        _log.LogWarning("Processed ERROR (invalid json) | msgId={MsgId} durationMs={Ms}", env.MessageId, sw.ElapsedMilliseconds);
    //        return new ProcessingResult(IsValid: false, OutputJson: errorPayload, OutputKey: env.Key);
    //    }
    //}
}
