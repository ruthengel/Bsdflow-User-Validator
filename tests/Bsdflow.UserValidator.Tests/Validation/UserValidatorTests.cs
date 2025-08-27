using Bsdflow.UserValidator.Domain;
using core =  Bsdflow.UserValidator.Validation;
using FluentAssertions;
using Xunit;

namespace Bsdflow.UserValidator.Tests.Validation;

public class UserValidatorTests
{
    private readonly core.IUserValidator _validator = new core.UserValidator();

    [Fact]
    public void Validate_ValidInput_ReturnsValidAndCanonicalizedUser()
    {
        var now = DateTime.UtcNow.Year;

        var input = new UserInput(
            IdNumber: "123456782",
            Email: "Test@Example.com",
            Phone: "054-1234567",
            BirthYear: now - 25,
            FirstName: "  Sara ",
            LastName: " Levi  "
        );

        var res = _validator.Validate(input);

        res.IsValid.Should().BeTrue();
        res.Errors.Should().BeEmpty();
        res.User.Should().NotBeNull();
        res.User!.IdNumber.Should().Be("123456782");
        res.User.Email.Should().Be("test@example.com");
        res.User.PhoneE164.Should().Be("+972541234567");
        res.User.BirthYear.Should().Be(input.BirthYear!.Value);
        res.User.FirstName.Should().Be("Sara");
        res.User.LastName.Should().Be("Levi");
    }

    [Fact]
    public void Validate_InvalidAllFields_ReturnsErrors()
    {
        var input = new UserInput(
            IdNumber: "123456780", // checksum שגוי
            Email: "bademail",
            Phone: "1234",
            BirthYear: DateTime.UtcNow.Year - 10, // צעיר מדי
            FirstName: null,
            LastName: null
        );

        var res = _validator.Validate(input);

        res.IsValid.Should().BeFalse();
        res.User.Should().BeNull();
        res.Errors.Should().NotBeEmpty();

        res.Errors.Should().Contain(e => e.Code == ErrorCode.InvalidId && e.Field == "IdNumber");
        res.Errors.Should().Contain(e => e.Code == ErrorCode.InvalidEmail && e.Field == "Email");
        res.Errors.Should().Contain(e => e.Code == ErrorCode.InvalidPhone && e.Field == "Phone");
        res.Errors.Should().Contain(e => e.Code == ErrorCode.UnderAge && e.Field == "BirthYear");
    }
}
