using Bsdflow.UserValidator.Domain;

namespace Bsdflow.UserValidator.Validation;

public interface IUserValidator
{
    ValidationResult Validate(UserInput input);
}

public sealed class UserValidator : IUserValidator
{
    public ValidationResult Validate(UserInput input)
    {
        var errors = new List<ValidationError>();

        if (!IsraeliIdRule.TryNormalize(input.IdNumber, out var idNorm))
            errors.Add(new ValidationError(ErrorCode.InvalidId, "IdNumber", "ID number is invalid."));

        if (!EmailRule.TryNormalize(input.Email, out var emailNorm))
            errors.Add(new ValidationError(ErrorCode.InvalidEmail, "Email", "Email format is invalid."));

        if (!PhoneRule.TryNormalize(input.Phone, out var phoneE164))
            errors.Add(new ValidationError(ErrorCode.InvalidPhone, "Phone", "Phone format is invalid."));

        if (!AgeRule.IsAdult(input.BirthYear, DateTime.UtcNow.Year))
            errors.Add(new ValidationError(ErrorCode.UnderAge, "BirthYear", "Age must be 18 or above."));

        if (errors.Count > 0)
            return new ValidationResult(false, null, errors);

        var canon = new CanonicalizedUser(
            IdNumber: idNorm,
            Email: emailNorm,
            PhoneE164: phoneE164,
            BirthYear: input.BirthYear!.Value,
            FirstName: input.FirstName?.Trim(),
            LastName: input.LastName?.Trim()
        );

        return new ValidationResult(true, canon, Array.Empty<ValidationError>());
    }
}
