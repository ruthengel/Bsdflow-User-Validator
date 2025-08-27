using Bsdflow.UserValidator.Validation;
using FluentAssertions;
using Xunit;

namespace Bsdflow.UserValidator.Tests.Validation;

public class PhoneRuleTests
{
    [Theory]
    [InlineData("054-1234567", "+972541234567")]
    [InlineData("0501234567", "+972501234567")]
    [InlineData("+972-54-1234567", "+972541234567")]
    [InlineData("02-1234567", "+97221234567")]
    public void TryNormalize_ValidPhones_E164(string raw, string expectedE164)
    {
        var ok = PhoneRule.TryNormalize(raw, out var e164);

        ok.Should().BeTrue();
        e164.Should().Be(expectedE164);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("1234")]
    [InlineData("972541234567")] 
    public void TryNormalize_InvalidPhones_ReturnsFalse(string? raw)
    {
        var ok = PhoneRule.TryNormalize(raw, out var _);

        ok.Should().BeFalse();
    }
}
