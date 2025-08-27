using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text;

namespace Bsdflow.UserValidator.Messaging;

public class KafkaConsumer : IKafkaConsumer, IDisposable
{
    private readonly KafkaOptions _opts;
    private readonly ILogger<KafkaConsumer> _log;
    private readonly IConsumer<string, string> _consumer;

    public KafkaConsumer(KafkaOptions opts, ILogger<KafkaConsumer> log)
    {
        _opts = opts;
        _log = log;

        var cfg = new ConsumerConfig
        {
            BootstrapServers = _opts.BootstrapServers,
            GroupId = _opts.GroupId,
            EnableAutoCommit = false, 
            AutoOffsetReset = Enum.TryParse<AutoOffsetReset>(_opts.AutoOffsetReset, true, out var v)
                                ? v : AutoOffsetReset.Earliest
        };

        _consumer = new ConsumerBuilder<string, string>(cfg)
            .SetErrorHandler((_, e) => _log.LogError("Kafka error: {Error}", e))
            .Build();
    }

    //public Task RunAsync(
    //    string topic,
    //    Func<KafkaEnvelope, CancellationToken, Task<ProcessingResult>> handler,
    //    CancellationToken ct)
    //{
    //    _consumer.Subscribe(topic);

    //    return Task.Run(async () =>
    //    {
    //        var backoff = TimeSpan.FromSeconds(1);

    //        while (!ct.IsCancellationRequested)
    //        {
    //            try
    //            {
    //                var cr = _consumer.Consume(ct); 
    //                if (cr == null || cr.IsPartitionEOF) continue;

    //                var headers = cr.Message.Headers;
    //                string GetHeader(string name)
    //                {
    //                    var h = headers?.FirstOrDefault(x => x.Key == name);
    //                    return h is null ? "" : Encoding.UTF8.GetString(h.GetValueBytes());
    //                }

    //                var messageId     = string.IsNullOrWhiteSpace(GetHeader("messageId"))     ? Guid.NewGuid().ToString("N") : GetHeader("messageId");
    //                var correlationId = string.IsNullOrWhiteSpace(GetHeader("correlationId")) ? messageId                    : GetHeader("correlationId");

    //                var env = new KafkaEnvelope(
    //                    Topic: cr.Topic,
    //                    Key: cr.Message.Key,
    //                    Value: cr.Message.Value,
    //                    MessageId: messageId,
    //                    CorrelationId: correlationId,
    //                    Partition: cr.Partition.Value,
    //                    Offset: cr.Offset.Value
    //                );



    //                var result = await handler(env, ct);
    //                _consumer.Commit(cr);

    //                backoff = TimeSpan.FromSeconds(1); 
    //            }
    //            catch (ConsumeException ex)
    //            {
    //                _log.LogError(ex, "Consume failed: {Reason}", ex.Error.Reason);
    //                await Task.Delay(backoff, ct);
    //                backoff = TimeSpan.FromMilliseconds(Math.Min(backoff.TotalMilliseconds * 2, 5000));
    //            }
    //            catch (OperationCanceledException)
    //            {
    //                break;
    //            }
    //            catch (Exception ex)
    //            {
    //                _log.LogError(ex, "Unhandled processing error");
    //                await Task.Delay(backoff, ct);
    //                backoff = TimeSpan.FromMilliseconds(Math.Min(backoff.TotalMilliseconds * 2, 5000));
    //            }
    //        }
    //    }, ct);
    //}

    public Task RunAsync(string topic, Func<KafkaEnvelope, CancellationToken, Task<ProcessingResult>> handler, CancellationToken ct){
       
        _consumer.Subscribe(topic);

        return Task.Run(async () =>
        {
            var backoff = TimeSpan.FromSeconds(1);

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(ct);
                    if (cr == null || cr.IsPartitionEOF) continue;

                    var headers = cr.Message.Headers;
                    string GetHeader(string name)
                    {
                        var h = headers?.FirstOrDefault(x => x.Key == name);
                        return h is null ? "" : Encoding.UTF8.GetString(h.GetValueBytes());
                    }
                    var messageId = string.IsNullOrWhiteSpace(GetHeader("messageId")) ? Guid.NewGuid().ToString("N") : GetHeader("messageId");
                    var correlationId = string.IsNullOrWhiteSpace(GetHeader("correlationId")) ? messageId : GetHeader("correlationId");

                    var env = new KafkaEnvelope(
                        Topic: cr.Topic,
                        Key: cr.Message.Key,
                        Value: cr.Message.Value,
                        MessageId: messageId,
                        CorrelationId: correlationId,
                        Partition: cr.Partition.Value,
                        Offset: cr.Offset.Value
                    );

                    var result = await handler(env, ct);

                    _consumer.Commit(cr);
                    _log.LogInformation("Committed | partition={P} offset={O}", cr.Partition.Value, cr.Offset.Value);

                    backoff = TimeSpan.FromSeconds(1);
                }
                catch (ConsumeException ex)
                {
                    _log.LogError(ex, "Consume failed: {Reason}", ex.Error.Reason);
                    await Task.Delay(backoff, ct);
                    backoff = TimeSpan.FromMilliseconds(Math.Min(backoff.TotalMilliseconds * 2, 5000));
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Processing failed; will not commit offset");
                    await Task.Delay(backoff, ct);
                    backoff = TimeSpan.FromMilliseconds(Math.Min(backoff.TotalMilliseconds * 2, 5000));
                }
            }
        }, ct);
    }

    public void Dispose() => _consumer.Close();


}
