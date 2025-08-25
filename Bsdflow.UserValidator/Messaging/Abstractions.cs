using System.Threading;
using System.Threading.Tasks;

namespace Bsdflow.UserValidator.Messaging;

public interface IKafkaPublisher
{
    Task PublishAsync(string topic, string? key, string value,
                      string messageId, string correlationId,
                      CancellationToken ct);
}

public interface IKafkaConsumer
{
    Task RunAsync(string topic,
                  Func<KafkaEnvelope, CancellationToken, Task<ProcessingResult>> handler,
                  CancellationToken ct);
}
