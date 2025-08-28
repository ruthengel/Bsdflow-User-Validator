using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bsdflow.UserValidator.Messaging;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Bsdflow.UserValidator.Tests.Messaging
{
    public class KafkaConsumer_SampleStyleTests
    {
        private static KafkaOptions Opts() => new KafkaOptions
        {
            BootstrapServers = "ignored-in-unit",
            GroupId = "tests",
            AutoOffsetReset = "earliest"
        };
        private static ConsumeResult<string, string> MakeCr(
            string topic = "users.intake",
            string key = "k1",
            string value = "{\"idNumber\":\"100000009\"}",
            Headers? headers = null,
            int partition = 0,
            long offset = 42)
        {
            return new ConsumeResult<string, string>
            {
                Topic = topic,
                Partition = new Partition(partition),
                Offset = new Offset(offset),
                Message = new Message<string, string>
                {
                    Key = key,
                    Value = value,
                    Headers = headers ?? new Headers()
                }
            };
        }
        private static Task NoDelay(TimeSpan _, CancellationToken __) => Task.CompletedTask;
        [Fact]
        public async Task RunAsync_WhenMessageIsValid_BuildsEnvelope_InvokesHandler_AndCommits()
        {
            var headers = new Headers { new Header("messageId", Encoding.UTF8.GetBytes("abc123")) };
            var cr = MakeCr(headers: headers);
            var consumerMock = new Mock<IConsumer<string, string>>(MockBehavior.Strict);
            consumerMock.Setup(c => c.Subscribe("users.intake"));
            consumerMock.SetupSequence(c => c.Consume(It.IsAny<CancellationToken>()))
                        .Returns(cr)
                        .Throws(new OperationCanceledException());
            consumerMock.Setup(c => c.Commit(It.Is<ConsumeResult<string, string>>(x => x.Offset == new Offset(42))));
            consumerMock.Setup(c => c.Close());
            using var sut = new KafkaConsumer(
                Opts(),
                Mock.Of<ILogger<KafkaConsumer>>(),
                consumerMock.Object,
                NoDelay
            );
            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                sut.RunAsync("users.intake", (env, ct) =>
                {
                    env.Topic.Should().Be("users.intake");
                    env.Key.Should().Be("k1");
                    env.Value.Should().Contain("100000009");
                    env.MessageId.Should().Be("abc123");
                    env.CorrelationId.Should().Be("abc123");
                    return Task.FromResult(default(ProcessingResult));
                }, default));
            consumerMock.Verify(c => c.Subscribe("users.intake"), Times.Once);
            consumerMock.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, string>>()), Times.Once);
        }
    }
}
