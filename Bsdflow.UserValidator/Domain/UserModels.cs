namespace Bsdflow.UserValidator.Domain;

public record UserInput(
    string? IdNumber,
    string? Email,
    string? Phone,
    int? BirthYear,
    string? FirstName = null,
    string? LastName = null
);

public enum ErrorCode
{
    MissingField,
    InvalidId,
    InvalidEmail,
    InvalidPhone,
    UnderAge
}

public record ValidationError(ErrorCode Code, string Field, string Message);

public record CanonicalizedUser(
    string IdNumber,  
    string Email,   
    string PhoneE164,
    int BirthYear,
    string? FirstName = null,
    string? LastName = null
);

public record ValidationResult(
    bool IsValid,
    CanonicalizedUser? User,
    IReadOnlyList<ValidationError> Errors
);
