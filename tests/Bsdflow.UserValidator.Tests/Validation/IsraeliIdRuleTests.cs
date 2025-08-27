using Bsdflow.UserValidator.Validation;
using FluentAssertions;
using Xunit;

namespace Bsdflow.UserValidator.Tests.Validation;

public class IsraeliIdRuleTests
{
    [Theory]
    [InlineData("123456782")]
    [InlineData("123-456-782")]
    [InlineData("100000009")]
    [InlineData("100008")]
    public void TryNormalize_ValidIds_ReturnsTrueAndNineDigits(string raw)
    {
        var ok = IsraeliIdRule.TryNormalize(raw, out var norm);
        ok.Should().BeTrue();
        norm.Should().HaveLength(9);
        norm.Should().MatchRegex(@"^\d{9}$");
    }

    [Theory]
    [InlineData("abcdefghi")]
    [InlineData("456782")]
    [InlineData("123456780")]
    [InlineData("1234-56x-782")]
    public void TryNormalize_InvalidIds_ReturnsFalse(string raw)
    {
        var ok = IsraeliIdRule.TryNormalize(raw, out var _);
        ok.Should().BeFalse();
    }

}
