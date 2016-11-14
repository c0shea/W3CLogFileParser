namespace W3CLogFileParser
{
    public static class StringExtensions
    {
        public static string Truncate(this string stringToTruncate, int maxLength)
        {
            if (stringToTruncate == null || stringToTruncate.Length <= maxLength)
            {
                return stringToTruncate;
            }

            return stringToTruncate.Substring(0, maxLength);
        }
    }
}