using Bsdflow.UserValidator.Validation;
using FluentAssertions;
using Xunit;

namespace Bsdflow.UserValidator.Tests.Validation;

public class EmailRuleTests
{
    [Theory]
    [InlineData("Test@Example.com", "test@example.com")]
    [InlineData("  user+tag@domain.co.il ", "user+tag@domain.co.il")]
    public void TryNormalize_ValidEmails_ToLowerTrimmed(string raw, string expected)
    {
        var ok = EmailRule.TryNormalize(raw, out var norm);

        ok.Should().BeTrue();
        norm.Should().Be(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("a")]
    [InlineData("user@")]
    [InlineData("@domain.com")]
    public void TryNormalize_InvalidEmails_ReturnsFalse(string? raw)
    {
        var ok = EmailRule.TryNormalize(raw, out var _);

        ok.Should().BeFalse();
    }
}
