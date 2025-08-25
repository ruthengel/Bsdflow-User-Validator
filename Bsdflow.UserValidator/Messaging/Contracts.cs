namespace Bsdflow.UserValidator.Messaging;

public record KafkaEnvelope(
    string Topic,
    string? Key,
    string Value,
    string MessageId,
    string CorrelationId,
    int Partition,
    long Offset
);

public record ProcessingResult(
    bool IsValid,
    string OutputJson,             
    string? OutputKey = null
);
