using System.Net.Mail;
using System.Text.RegularExpressions;

namespace Bsdflow.UserValidator.Validation;

public static class IsraeliIdRule
{
    public static bool TryNormalize(string? raw, out string normalized)
    {
        normalized = "";
        if (string.IsNullOrWhiteSpace(raw)) return false;

        if (raw.Any(c => !(char.IsDigit(c) || c == ' ' || c == '-')))
            return false;

        var digits = new string(raw.Where(char.IsDigit).ToArray());
        if (digits.Length == 0 || digits.Length > 9) return false;

        digits = digits.PadLeft(9, '0');

        int sum = 0;
        for (int i = 0; i < 9; i++)
        {
            int d = digits[i] - '0';
            int mult = (i % 2 == 0) ? 1 : 2;
            int t = d * mult;
            if (t > 9) t -= 9;
            sum += t;
        }

        if (sum % 10 != 0) return false;
        normalized = digits;
        return true;
    }
}

public static class EmailRule
{
    public static bool TryNormalize(string? raw, out string normalized)
    {
        normalized = "";
        if (string.IsNullOrWhiteSpace(raw)) return false;
        var e = raw.Trim();

        try
        {
            var addr = new MailAddress(e);
            normalized = addr.Address.ToLowerInvariant();
            return true;
        }
        catch { return false; }
    }
}

public static class PhoneRule
{
    public static bool TryNormalize(string? raw, out string e164)
    {
        e164 = "";
        if (string.IsNullOrWhiteSpace(raw)) return false;

        var s = Regex.Replace(raw, @"[\s\-]", "");

        if (s.StartsWith("+972"))
        {
            var rest = s.Substring(4);
            if (rest.StartsWith("0")) rest = rest.Substring(1);
            var candidate = "+972" + rest;
            if (Regex.IsMatch(candidate, @"^\+972\d{8,9}$"))
            {
                e164 = candidate;
                return true;
            }
            return false;
        }

        if (s.StartsWith("0"))
        {
            var digits = new string(s.Where(char.IsDigit).ToArray());
            if (digits.Length is >= 9 and <= 10)
            {
                var without0 = digits.Substring(1);
                var candidate = "+972" + without0;
                if (Regex.IsMatch(candidate, @"^\+972\d{8,9}$"))
                {
                    e164 = candidate;
                    return true;
                }
            }
        }

        return false;
    }
}

public static class AgeRule
{
    public static bool IsAdult(int? birthYear, int nowYear)
    {
        if (birthYear is null) return false;
        return (nowYear - birthYear.Value) >= 18;
    }
}
