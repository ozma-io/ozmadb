using System;
using System.Text;
using System.Globalization;
using NodaTime;

// Extracted from `NpgsqlTimeSpan` source code.
namespace OzmaDBSchema.Npgsql
{
    public static class TimeSpanStrings
    {
        public static Period Parse(string str)
        {
            if (str == null)
            {
                throw new ArgumentNullException(nameof(str));
            }
            str = str.Replace('s', ' '); //Quick and easy way to catch plurals.
            try
            {
                var years = 0;
                var months = 0;
                var days = 0;
                var hours = 0;
                var minutes = 0;
                var seconds = 0m;
                var idx = str.IndexOf("year", StringComparison.Ordinal);
                if (idx > 0)
                {
                    years = int.Parse(str.Substring(0, idx));
                    str = SafeSubstring(str, idx + 5);
                }
                idx = str.IndexOf("mon", StringComparison.Ordinal);
                if (idx > 0)
                {
                    months = int.Parse(str.Substring(0, idx));
                    str = SafeSubstring(str, idx + 4);
                }
                idx = str.IndexOf("day", StringComparison.Ordinal);
                if (idx > 0)
                {
                    days = int.Parse(str.Substring(0, idx));
                    str = SafeSubstring(str, idx + 4).Trim();
                }
                if (str.Length > 0)
                {
                    var isNegative = str[0] == '-';
                    var parts = str.Split(':');
                    switch (parts.Length) //One of those times that fall-through would actually be good.
                    {
                        case 1:
                            hours = int.Parse(parts[0]);
                            break;
                        case 2:
                            hours = int.Parse(parts[0]);
                            minutes = int.Parse(parts[1]);
                            break;
                        default:
                            hours = int.Parse(parts[0]);
                            minutes = int.Parse(parts[1]);
                            seconds = decimal.Parse(parts[2], CultureInfo.InvariantCulture.NumberFormat);
                            break;
                    }
                    if (isNegative)
                    {
                        minutes *= -1;
                        seconds *= -1;
                    }
                }

                var ticks = hours * TimeSpan.TicksPerHour + minutes * TimeSpan.TicksPerMinute + (long)(seconds * TimeSpan.TicksPerSecond);
                var builder = new PeriodBuilder
                {
                    Months = months,
                    Days = days,
                    Ticks = ticks
                };
                return builder.Build();
            }
            catch (OverflowException)
            {
                throw;
            }
            catch (Exception)
            {
                throw new FormatException();
            }
        }

        private static string SafeSubstring(string s, int startIndex)
        {
            if (startIndex >= s.Length)
                return string.Empty;
            else
                return s.Substring(startIndex);
        }

        public static bool TryParse(string str, out Period result)
        {
            try
            {
                result = Parse(str);
                return true;
            }
            catch (Exception)
            {
                result = Period.Zero;
                return false;
            }
        }

        public static string ToString(Period period)
        {
            var sb = new StringBuilder();
            bool isNegative = period.Months < 0;
            if (period.Months != 0)
            {
                sb.Append(period.Months).Append(Math.Abs(period.Months) == 1 ? " mon " : " mons ");
            }
            if (period.Days != 0)
            {
                if (period.Months < 0 && period.Days > 0)
                {
                    sb.Append('+');
                }
                sb.Append(period.Days).Append(Math.Abs(period.Days) == 1 ? " day " : " days ");
            }
            if (period.HasTimeComponent || sb.Length == 0)
            {
                var totalTicks =
                    period.Hours * TimeSpan.TicksPerHour +
                    period.Minutes * TimeSpan.TicksPerMinute +
                    period.Seconds * TimeSpan.TicksPerSecond +
                    period.Milliseconds * TimeSpan.TicksPerMillisecond +
                    period.Ticks +
                    period.Nanoseconds / 100L;
                if (totalTicks < 0)
                {
                    sb.Append('-');
                }
                else if (period.Days < 0 || (period.Days == 0 && period.Months < 0))
                {
                    sb.Append('+');
                }
                totalTicks = Math.Abs(totalTicks);
                // calculate total seconds and then subtract total whole minutes in seconds to get just the seconds and fractional part
                var totalSeconds = totalTicks / TimeSpan.TicksPerSecond;
                var totalMinutes = totalSeconds / 60;
                var totalHours = totalMinutes / 60;
                var minutes = totalMinutes % 60;
                var seconds = totalSeconds % 60;
                var microseconds = totalTicks % TimeSpan.TicksPerSecond / 10;
                sb.Append(totalHours.ToString("D2")).Append(':').Append(minutes.ToString("D2")).Append(':').Append(seconds.ToString("D2")).Append('.').Append(microseconds.ToString("D6"));
            }
            return sb.ToString();
        }
    }
}
