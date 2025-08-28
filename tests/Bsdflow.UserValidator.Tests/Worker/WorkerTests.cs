using System.Reflection;
using System.Text.Json;
using FluentAssertions;
using Xunit;

using Bsdflow.UserValidator;
using Bsdflow.UserValidator.Domain;
using Bsdflow.UserValidator.Validation;
using Bsdflow.UserValidator.Messaging;
using Microsoft.Extensions.Logging;

namespace Bsdflow.UserValidator.Tests.Worker
{
    public class WorkerTests
    {
        private sealed class NullConsumer : IKafkaConsumer
        {
            public Task RunAsync(
                string topic,
                Func<KafkaEnvelope, CancellationToken, Task<ProcessingResult>> handler,
                CancellationToken ct) => Task.CompletedTask;
        }

        private sealed class CapturingPublisher : IKafkaPublisher
        {
            public readonly List<(string Topic, string? Key, string Payload, string MsgId, string CorrId)> Published
                = new();

            public Task PublishAsync(string topic, string? key, string value, string messageId, string correlationId, CancellationToken ct)
            {
                Published.Add((topic, key, value, messageId, correlationId));
                return Task.CompletedTask;
            }

            public bool Flushed { get; private set; }

            public Task FlushAsync(TimeSpan timeout)
            {
                Flushed = true;
                return Task.CompletedTask;
            }
        }

        private sealed class StubValidator : IUserValidator
        {
            private readonly Func<UserInput, ValidationResult> _impl;
            public StubValidator(Func<UserInput, ValidationResult> impl) => _impl = impl;
            public ValidationResult Validate(UserInput input) => _impl(input);
        }

        private static KafkaOptions MakeOpts() => new KafkaOptions
        {
            BootstrapServers = "dev:9092",
            GroupId = "g1",
            InputTopic = "users.intake",
            ValidTopic = "users.valid",
            ErrorTopic = "users.errors",
            EnableAutoCommit = false,
            AutoOffsetReset = "earliest",
        };

        private static Task<ProcessingResult> InvokeHandlerAsync(Bsdflow.UserValidator.Worker worker, KafkaEnvelope env, CancellationToken ct)
        {
            var mi = typeof(Bsdflow.UserValidator.Worker)
                .GetMethod("HandlerAsync", BindingFlags.Instance | BindingFlags.NonPublic);

            mi.Should().NotBeNull("HandlerAsync must exist on Worker");

            var task = (Task<ProcessingResult>)mi!.Invoke(worker, new object[] { env, ct })!;
            return task;
        }

        private static KafkaEnvelope MakeEnvelope(
            string json,
            string? key = "user-1",
            string msgId = "m-123",
            string corrId = "c-123") =>
            new KafkaEnvelope(
                Topic: "users.intake",
                Key: key,
                Value: json,
                MessageId: msgId,
                CorrelationId: corrId,
                Partition: 0,
                Offset: 1
            );


        [Fact]
        public async Task Handler_ValidMessage_PublishesToValidTopic()
        {
            var opts = MakeOpts();
            var publisher = new CapturingPublisher();
            var consumer = new NullConsumer();

            var validator = new StubValidator(_ =>
                new ValidationResult(
                    IsValid: true,
                    User: new CanonicalizedUser(
                        IdNumber: "123456782",
                        Email: "test@example.com",
                        PhoneE164: "+972541234567",
                        BirthYear: 1990,
                        FirstName: "Sara",
                        LastName: "Levi"
                    ),
                    Errors: Array.Empty<ValidationError>()));

            using var loggerFactory = LoggerFactory.Create(b => b.AddSimpleConsole().SetMinimumLevel(LogLevel.Debug));
            var logger = loggerFactory.CreateLogger<Bsdflow.UserValidator.Worker>();

            var worker = new Bsdflow.UserValidator.Worker(logger, opts, consumer, publisher, validator);

            var inputJson = JsonSerializer.Serialize(new
            {
                idNumber = "123456782",
                email = "Test@Example.com",
                phone = "054-1234567",
                birthYear = 1990,
                firstName = "Sara",
                lastName = "Levi"
            }, new JsonSerializerOptions(JsonSerializerDefaults.Web));

            var env = MakeEnvelope(inputJson);

            var res = await InvokeHandlerAsync(worker, env, CancellationToken.None);

            res.IsValid.Should().BeTrue();
            publisher.Published.Should().HaveCount(1);
            var p = publisher.Published.Single();
            p.Topic.Should().Be(opts.ValidTopic);
            p.MsgId.Should().Be(env.MessageId);
            p.CorrId.Should().Be(env.CorrelationId);

            using var doc = JsonDocument.Parse(p.Payload);
            doc.RootElement.TryGetProperty("user", out _).Should().BeTrue();
        }

        [Fact]
        public async Task Handler_InvalidValidation_PublishesToErrorTopic()
        {
            var opts = MakeOpts();
            var publisher = new CapturingPublisher();
            var consumer = new NullConsumer();

            var errors = new List<ValidationError>
            {
                new ValidationError(ErrorCode.InvalidEmail, "Email", "Email format is invalid.")
            };

            var validator = new StubValidator(_ =>
                new ValidationResult(IsValid: false, User: null, Errors: errors));

            using var loggerFactory = LoggerFactory.Create(b => b.AddSimpleConsole().SetMinimumLevel(LogLevel.Debug));
            var logger = loggerFactory.CreateLogger<Bsdflow.UserValidator.Worker>();

            var worker = new Bsdflow.UserValidator.Worker(logger, opts, consumer, publisher, validator);

            var inputJson = JsonSerializer.Serialize(new { idNumber = "123", email = "bad@", phone = "000", birthYear = 2010 },
                new JsonSerializerOptions(JsonSerializerDefaults.Web));

            var env = MakeEnvelope(inputJson);

            var res = await InvokeHandlerAsync(worker, env, CancellationToken.None);

            res.IsValid.Should().BeFalse();
            publisher.Published.Should().HaveCount(1);
            var p = publisher.Published.Single();
            p.Topic.Should().Be(opts.ErrorTopic);

            using var doc = JsonDocument.Parse(p.Payload);
            doc.RootElement.TryGetProperty("errors", out var errs).Should().BeTrue();
            errs.ValueKind.Should().Be(JsonValueKind.Array);
            errs.GetArrayLength().Should().Be(1);
        }

        [Fact]
        public async Task Handler_InvalidJson_PublishesInvalidJsonError()
        {
            var opts = MakeOpts();
            var publisher = new CapturingPublisher();
            var consumer = new NullConsumer();

            var validator = new StubValidator(_ => throw new Exception("Should not be called"));

            using var loggerFactory = LoggerFactory.Create(b => b.AddSimpleConsole().SetMinimumLevel(LogLevel.Debug));
            var logger = loggerFactory.CreateLogger<Bsdflow.UserValidator.Worker>();

            var worker = new Bsdflow.UserValidator.Worker(logger, opts, consumer, publisher, validator);

            var badJson = "{ idNumber: 123456 ";
            var env = MakeEnvelope(badJson);

            var res = await InvokeHandlerAsync(worker, env, CancellationToken.None);

            res.IsValid.Should().BeFalse();
            publisher.Published.Should().HaveCount(1);
            var p = publisher.Published.Single();
            p.Topic.Should().Be(opts.ErrorTopic);

            using var doc = JsonDocument.Parse(p.Payload);
            doc.RootElement.GetProperty("reason").GetString().Should().Be("invalid_json");
            doc.RootElement.TryGetProperty("original", out _).Should().BeTrue();
        }

        [Fact]
        public async Task StopAsync_FlushesPublisher()
        {
            var opts = MakeOpts();
            var publisher = new CapturingPublisher();
            var consumer = new NullConsumer();

            var validator = new StubValidator(_ =>
                new ValidationResult(true, null, Array.Empty<ValidationError>()));

            using var loggerFactory = LoggerFactory.Create(b => b.AddSimpleConsole().SetMinimumLevel(LogLevel.Debug));
            var logger = loggerFactory.CreateLogger<Bsdflow.UserValidator.Worker>();

            var worker = new Bsdflow.UserValidator.Worker(logger, opts, consumer, publisher, validator);

            await worker.StopAsync(CancellationToken.None);

            publisher.Flushed.Should().BeTrue();
        }
    }
}
