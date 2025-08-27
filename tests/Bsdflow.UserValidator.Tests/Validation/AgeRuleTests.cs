using Bsdflow.UserValidator.Validation;
using FluentAssertions;
using Xunit;

namespace Bsdflow.UserValidator.Tests.Validation;

public class AgeRuleTests
{
    [Fact]
    public void IsAdult_WhenBirthYearNull_ReturnsFalse()
    {
        AgeRule.IsAdult(null, nowYear: 2025).Should().BeFalse();
    }

    [Fact]
    public void IsAdult_WhenAge18OrMore_IsTrue()
    {
        var now = DateTime.UtcNow.Year;
        AgeRule.IsAdult(now - 18, now).Should().BeTrue();
        AgeRule.IsAdult(now - 30, now).Should().BeTrue();
    }

    [Fact]
    public void IsAdult_WhenAgeLessThan18_IsFalse()
    {
        var now = DateTime.UtcNow.Year;
        AgeRule.IsAdult(now - 17, now).Should().BeFalse();
    }
}
