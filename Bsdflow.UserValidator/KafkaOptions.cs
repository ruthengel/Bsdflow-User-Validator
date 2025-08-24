using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace Bsdflow.UserValidator
{
    public record KafkaOptions
    {
        public string? BootstrapServers { get; init; }
        public string? GroupId { get; init; }
        public string? InputTopic { get; init; }
        public string? ValidTopic { get; init; }
        public string? ErrorTopic { get; init; }
        public string AutoOffsetReset { get; init; } = "earliest";
        public bool EnableAutoCommit { get; init; } = false;

        public static KafkaOptions Load(IConfiguration cfg)
        {
            static string? Get(IConfiguration c, string a, string b) =>
                !string.IsNullOrWhiteSpace(c[a]) ? c[a] :
                !string.IsNullOrWhiteSpace(c[b]) ? c[b] : null;

            return new KafkaOptions
            {
                BootstrapServers = Get(cfg, "Kafka:BootstrapServers", "BOOTSTRAP_SERVERS"),
                GroupId = Get(cfg, "Kafka:GroupId", "GROUP_ID"),
                InputTopic = Get(cfg, "Kafka:InputTopic", "INPUT_TOPIC"),
                ValidTopic = Get(cfg, "Kafka:ValidTopic", "VALID_TOPIC"),
                ErrorTopic = Get(cfg, "Kafka:ErrorTopic", "ERROR_TOPIC"),
                AutoOffsetReset = Get(cfg, "Kafka:AutoOffsetReset", "AUTO_OFFSET_RESET") ?? "earliest",
                EnableAutoCommit = bool.TryParse(Get(cfg, "Kafka:EnableAutoCommit", "ENABLE_AUTO_COMMIT"), out var v) && v
            };
        }
    }
}
