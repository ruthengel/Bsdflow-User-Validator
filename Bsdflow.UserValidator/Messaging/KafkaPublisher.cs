using Confluent.Kafka;
using Microsoft.Extensions.Logging;
namespace Bsdflow.UserValidator.Messaging;
public class KafkaPublisher : IKafkaPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaPublisher> _log;
    public KafkaPublisher(KafkaOptions opts, ILogger<KafkaPublisher> log)
    {
        _log = log;
        var cfg = new ProducerConfig
        {
            BootstrapServers = opts.BootstrapServers,
        };
        _producer = new ProducerBuilder<string, string>(cfg).Build();
    }
    public KafkaPublisher(IProducer<string, string> producer, ILogger<KafkaPublisher> log)
    {
        _producer = producer;
        _log = log;
    }
    public async Task PublishAsync(string topic, string? key, string value,
                                   string messageId, string correlationId,
                                   CancellationToken ct)
    {
        var msg = new Message<string, string>
        {
            Key = key,
            Value = value,
            Headers = new Headers
            {
                { "messageId", System.Text.Encoding.UTF8.GetBytes(messageId) },
                { "correlationId", System.Text.Encoding.UTF8.GetBytes(correlationId) }
            }
        };
        var dr = await _producer.ProduceAsync(topic, msg, ct);
        _log.LogInformation("Published | topic={Topic} key={Key} partition={Partition} offset={Offset}",
            dr.Topic, key, dr.Partition.Value, dr.Offset.Value);
    }
    public Task FlushAsync(TimeSpan timeout)
    {
        _producer.Flush(timeout);
        return Task.CompletedTask;
    }
    public void Dispose()
    {
        try { _producer.Flush(TimeSpan.FromSeconds(5)); }
        finally { (_producer as IDisposable)?.Dispose(); }
    }
}
