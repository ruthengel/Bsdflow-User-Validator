using Bsdflow.UserValidator.Messaging;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Xunit;
namespace Bsdflow.UserValidator.Tests.Messaging
{
    public class KafkaPublisherTests
    {
        [Fact]
        public async Task PublishAsync_Sends_KeyValue_And_Headers()
        {
            var topic = "users.valid";
            var key = "k1";
            var value = "{\"idNumber\":\"100000009\"}";
            var messageId = "m-123";
            var correlationId = "c-123";
            Message<string, string>? captured = null;
            var producerMock = new Mock<IProducer<string, string>>(MockBehavior.Strict);
            producerMock
                .Setup(p => p.ProduceAsync(
                    topic,
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .Callback<string, Message<string, string>, CancellationToken>((_, msg, __) => captured = msg)
                .ReturnsAsync(new DeliveryResult<string, string>
                {
                    TopicPartitionOffset = new TopicPartitionOffset(
                        new TopicPartition(topic, new Partition(0)),
                        new Offset(1)),
                    Status = PersistenceStatus.Persisted
                });
            var sut = new KafkaPublisher(producerMock.Object, NullLogger<KafkaPublisher>.Instance);
            await sut.PublishAsync(topic, key, value, messageId, correlationId, CancellationToken.None);
            captured.Should().NotBeNull();
            captured!.Key.Should().Be(key);
            captured.Value.Should().Be(value);
            var hdrs = captured.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes()));
            hdrs.Should().ContainKey("messageId");
            hdrs.Should().ContainKey("correlationId");
            hdrs["messageId"].Should().Be(messageId);
            hdrs["correlationId"].Should().Be(correlationId);
            producerMock.VerifyAll();
        }
        [Fact]
        public async Task PublishAsync_Throws_WhenProducerFails()
        {
            var topic = "users.errors";
            var producerMock = new Mock<IProducer<string, string>>();
            producerMock
                .Setup(p => p.ProduceAsync(
                    topic,
                    It.IsAny<Message<string, string>>(),
                    It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ProduceException<string, string>(new Error(ErrorCode.Local_Fail), null));
            var sut = new KafkaPublisher(producerMock.Object, NullLogger<KafkaPublisher>.Instance);
            await Assert.ThrowsAsync<ProduceException<string, string>>(() =>
                sut.PublishAsync(topic, null, "{}", "m1", "c1", CancellationToken.None));
        }
        [Fact]
        public void FlushAsync_Calls_Flush_On_Producer()
        {
            var timeout = TimeSpan.FromSeconds(2);
            var producerMock = new Mock<IProducer<string, string>>();
            producerMock.Setup(p => p.Flush(timeout));
            var sut = new KafkaPublisher(producerMock.Object, NullLogger<KafkaPublisher>.Instance);
            sut.FlushAsync(timeout).GetAwaiter().GetResult();
            producerMock.Verify(p => p.Flush(timeout), Times.Once);
        }
        [Fact]
        public void Dispose_Flushes_And_Disposes_Producer()
        {
            var producerMock = new Mock<IProducer<string, string>>();
            producerMock.Setup(p => p.Flush(It.IsAny<TimeSpan>()));
            producerMock.Setup(p => p.Dispose());
            var sut = new KafkaPublisher(producerMock.Object, NullLogger<KafkaPublisher>.Instance);
            sut.Dispose();
            producerMock.Verify(p => p.Flush(It.IsAny<TimeSpan>()), Times.Once);
            producerMock.Verify(p => p.Dispose(), Times.Once);
        }
    }
}