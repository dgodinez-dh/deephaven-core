/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.clock.TimeConstants;
import io.deephaven.configuration.Configuration;
import io.deephaven.function.Numeric;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.*;
import java.time.format.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * Functions for working with time.
 */
@SuppressWarnings({"RegExpRedundantEscape"})
public class DateTimeUtils {

    // region Format Patterns

    /**
     * Very permissive formatter / parser for local dates.
     */
    private static final DateTimeFormatter FORMATTER_ISO_LOCAL_DATE = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL)
            .toFormatter();

    /**
     * Very permissive formatter / parser for local times.
     */
    private static final DateTimeFormatter FORMATTER_ISO_LOCAL_TIME = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NORMAL)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    /**
     * Very permissive formatter / parser for local date times.
     */
    private static final DateTimeFormatter FORMATTER_ISO_LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(FORMATTER_ISO_LOCAL_DATE)
            .appendLiteral('T')
            .append(FORMATTER_ISO_LOCAL_TIME)
            .toFormatter();

    // The following 4 patterns support LocalDate literals. Note all LocalDate patterns must not have characters after
    // the date, to avoid confusion with date time literals.

    /** Matches yyyy-MM-dd. */
    private static final Pattern STD_DATE_PATTERN =
            Pattern.compile("^(?<year>[0-9][0-9][0-9][0-9])-(?<month>[0-9][0-9])-(?<day>[0-9][0-9])$");

    /** Matches yyyyMMdd (consistent with ISO dates). */
    private static final Pattern NOSEP_DATE_PATTERN =
            Pattern.compile("^(?<year>[0-9][0-9][0-9][0-9])(?<month>[0-9][0-9])(?<day>[0-9][0-9])$");

    /**
     * Matches variations of month/day/year or day/month/year or year/month/day - how this is interpreted depends on the
     * DateTimeUtils.dateStyle system property.
     */
    private static final Pattern SLASH_DATE_PATTERN =
            Pattern.compile(
                    "^(?<part1>[0-9]?[0-9](?<part1sub2>[0-9][0-9])?)\\/(?<part2>[0-9]?[0-9])\\/(?<part3>[0-9]?[0-9](?<part3sub2>[0-9][0-9])?)$");

    /**
     * Matches variations of month-day-year or day-month-year or year-month-day - how this is interpreted depends on the
     * DateTimeUtils.dateStyle system property.
     */
    private static final Pattern DASH_DATE_PATTERN =
            Pattern.compile(
                    "^(?<part1>[0-9]?[0-9](?<part1sub2>[0-9][0-9])?)\\-(?<part2>[0-9]?[0-9])\\-(?<part3>[0-9]?[0-9](?<part3sub2>[0-9][0-9])?)$");

    /** DateTimeFormatter for use when interpreting two digit years (we use Java's rules). */
    private static final DateTimeFormatter TWO_DIGIT_YR_FORMAT = DateTimeFormatter.ofPattern("yy");

    /**
     * Matches LocalTime literals. Note these must begin with "L" to avoid ambiguity with the older
     * TIME_AND_DURATION_PATTERN
     */
    private static final Pattern LOCAL_TIME_PATTERN =
            Pattern.compile("^L([0-9][0-9]):?([0-9][0-9])?:?([0-9][0-9])?(\\.([0-9]{1,9}))?");

    /**
     * Matches long values.
     */
    private static final Pattern LONG_PATTERN = Pattern.compile("^-?\\d{1,19}$");

    /**
     * Matches dates with time zones.
     */
    private static final Pattern DATE_TZ_PATTERN = Pattern.compile(
            "(?<date>[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])(?<t>[tT]?) (?<timezone>[a-zA-Z_/]+)");

    /**
     * Matches time durations.
     */
    private static final Pattern TIME_DURATION_PATTERN = Pattern.compile(
            "\\-?([0-9]+):([0-9]+)(:[0-9]+)?(\\.[0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?)?");

    /**
     * Matches date times.
     */
    private static final Pattern CAPTURING_DATETIME_PATTERN = Pattern.compile(
            "(([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])T?)?(([0-9][0-9]?)(?::([0-9][0-9])(?::([0-9][0-9]))?(?:\\.([0-9][0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?))?)?)?( [a-zA-Z]+)?");

    /**
     * Default date style.
     */
    private static final DateStyle DEFAULT_DATE_STYLE = DateStyle
            .valueOf(Configuration.getInstance().getStringWithDefault("DateTimeUtils.dateStyle", DateStyle.MDY.name()));

    // endregion

    // region Constants

    /**
     * A zero length array of date times.
     */
    public static final Instant[] ZERO_LENGTH_INSTANT_ARRAY = new Instant[0];

    /**
     * One microsecond in nanoseconds.
     */
    public static final long MICRO = 1_000;

    /**
     * One millisecond in nanoseconds.
     */
    public static final long MILLI = 1_000_000;

    /**
     * One second in nanoseconds.
     */
    public static final long SECOND = 1_000_000_000;

    /**
     * One minute in nanoseconds.
     */
    public static final long MINUTE = 60 * SECOND;

    /**
     * One hour in nanoseconds.
     */
    public static final long HOUR = 60 * MINUTE;

    /**
     * One day in nanoseconds.
     */
    public static final long DAY = 24 * HOUR;

    /**
     * One week in nanoseconds.
     */
    public static final long WEEK = 7 * DAY;

    /**
     * One year (365 days) in nanoseconds.
     */
    public static final long YEAR = 365 * DAY;

    /**
     * Maximum time in microseconds that can be converted to a date time without overflow.
     */
    private static final long MAX_CONVERTIBLE_MICROS = Long.MAX_VALUE / 1_000L;

    /**
     * Maximum time in milliseconds that can be converted to a date time without overflow.
     */
    private static final long MAX_CONVERTIBLE_MILLIS = Long.MAX_VALUE / 1_000_000L;

    /**
     * Maximum time in seconds that can be converted to a date time without overflow.
     */
    private static final long MAX_CONVERTIBLE_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    /**
     * Number of seconds per nanosecond.
     */
    public static final double SECONDS_PER_NANO = 1. / (double) SECOND;

    /**
     * Number of minutes per nanosecond.
     */
    public static final double MINUTES_PER_NANO = 1. / (double) MINUTE;

    /**
     * Number of hours per nanosecond.
     */
    public static final double HOURS_PER_NANO = 1. / (double) HOUR;

    /**
     * Number of days per nanosecond.
     */
    public static final double DAYS_PER_NANO = 1. / (double) DAY;

    /**
     * Number of years per nanosecond.
     */
    public static final double YEARS_PER_NANO = 1. / (double) YEAR;

    // endregion

    // region Overflow / Underflow

    /**
     * A type of RuntimeException thrown when operations resulting in date time values would exceed the range available
     * by max or min long nanoseconds.
     */
    public static class DateTimeOverflowException extends RuntimeException {
        /**
         * Creates a new overflow exception.
         */
        @SuppressWarnings("unused")
        private DateTimeOverflowException() {
            super("Operation failed due to overflow");
        }

        /**
         * Creates a new overflow exception.
         *
         * @param cause cause of the overflow.
         */
        private DateTimeOverflowException(@NotNull final Throwable cause) {
            super("Operation failed due to overflow", cause);
        }

        /**
         * Creates a new overflow exception.
         *
         * @param message error string.
         */
        private DateTimeOverflowException(@NotNull final String message) {
            super(message);
        }

        /**
         * Creates a new overflow exception.
         *
         * @param message error message.
         * @param cause cause of the overflow.
         */
        @SuppressWarnings("unused")
        private DateTimeOverflowException(@NotNull final String message, @NotNull final Throwable cause) {
            super(message, cause);
        }
    }

    // + can only result in flow if both positive or both negative
    @SuppressWarnings("SameParameterValue")
    private static long checkOverflowPlus(final long l1, final long l2, final boolean minusOperation) {
        if (l1 > 0 && l2 > 0 && Long.MAX_VALUE - l1 < l2) {
            final String message = minusOperation
                    ? "Subtracting " + -l2 + " nanos from " + l1 + " would overflow"
                    : "Adding " + l2 + " nanos to " + l1 + " would overflow";
            throw new DateTimeOverflowException(message);
        }

        if (l1 < 0 && l2 < 0) {
            return checkUnderflowMinus(l1, -l2, false);
        }

        return l1 + l2;
    }

    // - can only result in flow if one is positive and one is negative
    private static long checkUnderflowMinus(final long l1, final long l2, final boolean minusOperation) {
        if (l1 < 0 && l2 > 0 && Long.MIN_VALUE + l2 > -l1) {
            final String message = minusOperation
                    ? "Subtracting " + l2 + " nanos from " + l1 + " would underflow"
                    : "Adding " + -l2 + " nanos to " + l1 + " would underflow";
            throw new DateTimeOverflowException(message);
        }

        if (l1 > 0 && l2 < 0) {
            return checkOverflowPlus(l1, -l2, true);
        }

        return l1 - l2;
    }

    private static long safeComputeNanos(final long epochSecond, final long nanoOfSecond) {
        if (epochSecond >= MAX_CONVERTIBLE_SECONDS) {
            throw new DateTimeOverflowException("Numeric overflow detected during conversion of " + epochSecond
                    + " to nanoseconds");
        }

        return epochSecond * 1_000_000_000L + nanoOfSecond;
    }

    // endregion

    // region Clock

    // TODO(deephaven-core#3044): Improve scaffolding around full system replay
    /**
     * Clock used to compute the current time. This allows a custom clock to be used instead of the current system
     * clock. This is mainly used for replay simulations.
     */
    private static Clock clock;

    /**
     * Set the clock used to compute the current time. This allows a custom clock to be used instead of the current
     * system clock. This is mainly used for replay simulations.
     *
     * @param clock clock used to compute the current time. Null uses the system clock.
     */
    @ScriptApi
    public static void setClock(@Nullable final Clock clock) {
        DateTimeUtils.clock = clock;
    }

    /**
     * Returns the clock used to compute the current time. This may be the current system clock, or it may be an
     * alternative clock used for replay simulations.
     *
     * @see #setClock(Clock)
     * @return current clock.
     */
    @NotNull
    @ScriptApi
    public static Clock currentClock() {
        return Objects.requireNonNullElse(clock, Clock.system());
    }

    /**
     * Provides the current date time according to the current clock. Under most circumstances, this method will return
     * the current system time, but during replay simulations, this method can return the replay time.
     *
     * @see #currentClock()
     * @see #setClock(Clock)
     * @see #nowSystem()
     * @return the current date time according to the current clock.
     */
    @ScriptApi
    @NotNull
    public static Instant now() {
        return currentClock().instantNanos();
    }

    /**
     * Provides the current date time, with millisecond resolution, according to the current clock. Under most
     * circumstances, this method will return the current system time, but during replay simulations, this method can
     * return the replay time.
     *
     * @see #currentClock()
     * @see #setClock(Clock)
     * @see #nowSystemMillisResolution()
     * @return the current date time, with millisecond resolution, according to the current clock.
     */
    @ScriptApi
    @NotNull
    public static Instant nowMillisResolution() {
        return currentClock().instantMillis();
    }

    /**
     * Provides the current date time according to the system clock. Note that the system time may not be desirable
     * during replay simulations.
     *
     * @see #now()
     * @return the current date time according to the system clock.
     */
    @ScriptApi
    @NotNull
    public static Instant nowSystem() {
        return Clock.system().instantNanos();
    }

    /**
     * Provides the current date time, with millisecond resolution, according to the system clock. Note that the system
     * time may not be desirable during replay simulations.
     *
     * @see #nowMillisResolution()
     * @return the current date time, with millisecond resolution, according to the system clock.
     */
    @ScriptApi
    @NotNull
    public static Instant nowSystemMillisResolution() {
        return Clock.system().instantMillis();
    }

    /**
     * A cached date in a specific timezone. The cache is invalidated when the current clock indicates the next day has
     * arrived.
     */
    private abstract static class CachedDate {

        final ZoneId timeZone;
        String value;
        long valueExpirationTimeMillis;

        private CachedDate(@NotNull final ZoneId timeZone) {
            this.timeZone = timeZone;
        }

        @SuppressWarnings("unused")
        private ZoneId getTimeZone() {
            return timeZone;
        }

        public String get() {
            return get(currentClock().currentTimeMillis());
        }

        public synchronized String get(final long currentTimeMillis) {
            if (currentTimeMillis >= valueExpirationTimeMillis) {
                update(currentTimeMillis);
            }
            return value;
        }

        abstract void update(long currentTimeMillis);
    }

    private static class CachedCurrentDate extends CachedDate {

        private CachedCurrentDate(@NotNull final ZoneId timeZone) {
            super(timeZone);
        }

        @Override
        void update(final long currentTimeMillis) {
            value = formatDate(epochMillisToInstant(currentTimeMillis), timeZone);
            valueExpirationTimeMillis =
                    epochMillis(atMidnight(epochNanosToInstant(millisToNanos(currentTimeMillis) + DAY), timeZone));
        }
    }

    private static class CachedDateKey<CACHED_DATE_TYPE extends CachedDate>
            extends KeyedObjectKey.Basic<ZoneId, CACHED_DATE_TYPE> {

        @Override
        public ZoneId getKey(final CACHED_DATE_TYPE cachedDate) {
            return cachedDate.timeZone;
        }
    }

    private static final KeyedObjectHashMap<ZoneId, CachedCurrentDate> cachedCurrentDates =
            new KeyedObjectHashMap<>(new CachedDateKey<>());

    /**
     * Provides the current date string according to the current clock. Under most circumstances, this method will
     * return the date according to current system time, but during replay simulations, this method can return the date
     * according to replay time.
     *
     * @param timeZone time zone.
     * @see #currentClock()
     * @see #setClock(Clock)
     * @return the current date according to the current clock and time zone formatted as "yyyy-MM-dd".
     */
    @ScriptApi
    @NotNull
    public static String today(@NotNull final ZoneId timeZone) {
        return cachedCurrentDates.putIfAbsent(timeZone, CachedCurrentDate::new).get();
    }

    /**
     * Provides the current date string according to the current clock and the default time zone. Under most
     * circumstances, this method will return the date according to current system time, but during replay simulations,
     * this method can return the date according to replay time.
     *
     * @see #currentClock()
     * @see #setClock(Clock)
     * @see ZoneId#systemDefault()
     * @return the current date according to the current clock and default time zone formatted as "yyyy-MM-dd".
     */
    @ScriptApi
    @NotNull
    public static String today() {
        return today(ZoneId.systemDefault());
    }

    // endregion

    // region Time Zone

    /**
     * Gets the time zone for a time zone name.
     *
     * @param timeZone time zone name.
     * @return time zone.
     * @throws RuntimeException if the time zone does not exist.
     */
    public static ZoneId timeZone(@NotNull String timeZone) {
        return TimeZoneAliases.zoneId(timeZone);
    }

    /**
     * Gets the sysetm default time zone.
     *
     * @return time zone.
     * @see ZoneId#systemDefault()
     */
    public static ZoneId timeZone() {
        return ZoneId.systemDefault();
    }

    // endregion

    // region Conversions: Time Units

    /**
     * Converts microseconds to nanoseconds.
     *
     * @param micros microseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         microseconds converted to nanoseconds.
     */
    @ScriptApi
    public static long microsToNanos(final long micros) {
        if (micros == NULL_LONG) {
            return NULL_LONG;
        }
        if (Math.abs(micros) > MAX_CONVERTIBLE_MICROS) {
            throw new DateTimeOverflowException("Converting " + micros + " micros to nanos would overflow");
        }
        return micros * 1000;
    }

    /**
     * Converts milliseconds to nanoseconds.
     *
     * @param millis milliseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         milliseconds converted to nanoseconds.
     */
    @ScriptApi
    public static long millisToNanos(final long millis) {
        if (millis == NULL_LONG) {
            return NULL_LONG;
        }
        if (Math.abs(millis) > MAX_CONVERTIBLE_MILLIS) {
            throw new DateTimeOverflowException("Converting " + millis + " millis to nanos would overflow");
        }
        return millis * 1000000;
    }

    /**
     * Converts seconds to nanoseconds.
     *
     * @param seconds seconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         seconds converted to nanoseconds.
     */
    @ScriptApi
    public static long secondsToNanos(final long seconds) {
        if (seconds == NULL_LONG) {
            return NULL_LONG;
        }
        if (Math.abs(seconds) > MAX_CONVERTIBLE_SECONDS) {
            throw new DateTimeOverflowException("Converting " + seconds + " seconds to nanos would overflow");
        }

        return seconds * 1000000000L;
    }

    /**
     * Converts nanoseconds to microseconds.
     *
     * @param nanos nanoseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         nanoseconds converted to microseconds, rounded down.
     */
    @ScriptApi
    public static long nanosToMicros(final long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }
        return nanos / MICRO;
    }

    /**
     * Converts milliseconds to microseconds.
     *
     * @param millis milliseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         milliseconds converted to microseconds, rounded down.
     */
    @ScriptApi
    public static long millisToMicros(final long millis) {
        if (millis == NULL_LONG) {
            return NULL_LONG;
        }
        return millis * 1000L;
    }

    /**
     * Converts seconds to microseconds.
     *
     * @param seconds seconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         seconds converted to microseconds, rounded down.
     */
    @ScriptApi
    public static long secondsToMicros(final long seconds) {
        if (seconds == NULL_LONG) {
            return NULL_LONG;
        }
        return seconds * 1_000_000;
    }

    /**
     * Converts nanoseconds to milliseconds.
     *
     * @param nanos nanoseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         nanoseconds converted to milliseconds, rounded down.
     */
    @ScriptApi
    public static long nanosToMillis(final long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }

        return nanos / MILLI;
    }

    /**
     * Converts microseconds to milliseconds.
     *
     * @param micros microseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         microseconds converted to milliseconds, rounded down.
     */
    @ScriptApi
    public static long microsToMillis(final long micros) {
        if (micros == NULL_LONG) {
            return NULL_LONG;
        }

        return micros / 1000L;
    }

    /**
     * Converts seconds to milliseconds.
     *
     * @param seconds nanoseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         seconds converted to milliseconds, rounded down.
     */
    @ScriptApi
    public static long secondsToMillis(final long seconds) {
        if (seconds == NULL_LONG) {
            return NULL_LONG;
        }

        return seconds * 1_000L;
    }

    /**
     * Converts nanoseconds to seconds.
     *
     * @param nanos nanoseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         nanoseconds converted to seconds, rounded down.
     */
    @ScriptApi
    public static long nanosToSeconds(final long nanos) {
        if (nanos == NULL_LONG) {
            return NULL_LONG;
        }
        return nanos / SECOND;
    }

    /**
     * Converts microseconds to seconds.
     *
     * @param micros microseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         microseconds converted to seconds, rounded down.
     */
    @ScriptApi
    public static long microsToSeconds(final long micros) {
        if (micros == NULL_LONG) {
            return NULL_LONG;
        }
        return micros / 1_000_000L;
    }

    /**
     * Converts milliseconds to seconds.
     *
     * @param millis milliseconds to convert.
     * @return {@link QueryConstants#NULL_LONG} if the input is {@link QueryConstants#NULL_LONG}; otherwise the input
     *         milliseconds converted to seconds, rounded down.
     */
    @ScriptApi
    public static long millisToSeconds(final long millis) {
        if (millis == NULL_LONG) {
            return NULL_LONG;
        }
        return millis / 1_000L;
    }

    // endregion

    // region Conversions: Date Time Types


    /**
     * Converts a date time to an {@link Instant}.
     *
     * @param dateTime date time to convert.
     * @return {@link Instant}, or null if dateTime is null.
     */
    @ScriptApi
    @Nullable
    public static Instant toInstant(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return dateTime.toInstant();
    }

    /**
     * Converts a date, time, and time zone to an {@link Instant}.
     *
     * @param date date.
     * @param time local time.
     * @param timeZone time zone.
     * @return {@link Instant}, or null if any input is null.
     */
    @ScriptApi
    @Nullable
    public static Instant toInstant(@Nullable final LocalDate date, @Nullable final LocalTime time,
            @Nullable ZoneId timeZone) {
        if (date == null || time == null || timeZone == null) {
            return null;
        }

        return time // LocalTime
                .atDate(date) // LocalDateTime
                .atZone(timeZone) // ZonedDateTime
                .toInstant(); // Instant
    }

    /**
     * Converts a date time to an {@link Instant}.
     *
     * @param dateTime date time to convert.
     * @return {@link Instant}, or null if any input is null.
     */
    @ScriptApi
    @Nullable
    @Deprecated
    public static Instant toInstant(@Nullable final Date dateTime) {
        if (dateTime == null) {
            return null;
        }

        return epochMillisToInstant(dateTime.getTime());
    }

    /**
     * Converts a date time to a {@link ZonedDateTime}.
     *
     * @param dateTime date time to convert.
     * @param timeZone time zone.
     * @return {@link ZonedDateTime}, or null if any input is null.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime toZonedDateTime(@Nullable final Instant dateTime, @Nullable ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return ZonedDateTime.ofInstant(dateTime, timeZone);
    }

    /**
     * Converts a local date, local time, and time zone to a {@link ZonedDateTime}.
     *
     * @param date date.
     * @param time local time.
     * @param timeZone time zone.
     * @return {@link ZonedDateTime}, or null if any input is null.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime toZonedDateTime(@Nullable final LocalDate date, @Nullable final LocalTime time,
            @Nullable ZoneId timeZone) {
        if (date == null || time == null || timeZone == null) {
            return null;
        }

        return time // LocalTime
                .atDate(date) // LocalDateTime
                .atZone(timeZone); // ZonedDateTime
    }

    /**
     * Converts a date time to a {@link LocalDate} with the specified time zone.
     *
     * @param dateTime date time to convert.
     * @param timeZone time zone.
     * @return {@link LocalDate}, or null if any input is null.
     */
    @Nullable
    public static LocalDate toLocalDate(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return toZonedDateTime(dateTime, timeZone).toLocalDate();
    }

    /**
     * Converts a date time to a {@link LocalDate} with the time zone in the {@link ZonedDateTime}.
     *
     * @param dateTime date time to convert.
     * @return {@link LocalDate}, or null if any input is null.
     */
    @Nullable
    public static LocalDate toLocalDate(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return dateTime.toLocalDate();
    }

    /**
     * Converts a date time to a {@link LocalTime} with the specified time zone.
     *
     * @param dateTime date time to convert.
     * @param timeZone time zone.
     * @return {@link LocalTime}, or null if any input is null.
     */
    @Nullable
    public static LocalTime toLocalTime(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }
        return toZonedDateTime(dateTime, timeZone).toLocalTime();
    }

    /**
     * Converts a date time to a {@link LocalTime} with the time zone in the {@link ZonedDateTime}.
     *
     * @param dateTime date time to convert.
     * @return {@link LocalTime}, or null if any input is null.
     */
    @Nullable
    public static LocalTime toLocalTime(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return dateTime.toLocalTime();
    }


    /**
     * Converts a date time to a {@link Date}. The date time will be truncated to millisecond resolution.
     *
     * @param dateTime date time to convert.
     * @return {@link Date}, or null if any input is null.
     * @deprecated
     */
    @Deprecated
    @Nullable
    public static Date toDate(@Nullable final Instant dateTime) {
        if (dateTime == null) {
            return null;
        }
        return new Date(epochMillis(dateTime));
    }

    /**
     * Converts a date time to a {@link Date}. The date time will be truncated to millisecond resolution.
     *
     * @param dateTime date time to convert.
     * @return {@link Date}, or null if any input is null.
     * @deprecated
     */
    @Deprecated
    @Nullable
    public static Date toDate(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        return new Date(epochMillis(dateTime));
    }

    // endregion

    // region Conversions: Epoch

    /**
     * Returns nanoseconds from the Epoch for a date time value.
     *
     * @param dateTime date time to compute the Epoch offset for.
     * @return nanoseconds since Epoch, or a NULL_LONG value if the date time is null.
     */
    @ScriptApi
    public static long epochNanos(@Nullable final Instant dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return safeComputeNanos(dateTime.getEpochSecond(), dateTime.getNano());
    }

    /**
     * Returns nanoseconds from the Epoch for a date time value.
     *
     * @param dateTime date time to compute the Epoch offset for.
     * @return nanoseconds since Epoch, or a NULL_LONG value if the date time is null.
     */
    @ScriptApi
    public static long epochNanos(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return safeComputeNanos(dateTime.toEpochSecond(), dateTime.getNano());
    }

    /**
     * Returns microseconds from the Epoch for a date time value.
     *
     * @param dateTime date time to compute the Epoch offset for.
     * @return microseconds since Epoch, or a NULL_LONG value if the date time is null.
     */
    @ScriptApi
    public static long epochMicros(@Nullable final Instant dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return nanosToMicros(epochNanos(dateTime));
    }

    /**
     * Returns microseconds from the Epoch for a date time value.
     *
     * @param dateTime date time to compute the Epoch offset for.
     * @return microseconds since Epoch, or a NULL_LONG value if the date time is null.
     */
    @ScriptApi
    public static long epochMicros(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return nanosToMicros(epochNanos(dateTime));
    }

    /**
     * Returns milliseconds from the Epoch for a date time value.
     *
     * @param dateTime date time to compute the Epoch offset for.
     * @return milliseconds since Epoch, or a NULL_LONG value if the date time is null.
     */
    @ScriptApi
    public static long epochMillis(@Nullable final Instant dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.toEpochMilli();
    }

    /**
     * Returns milliseconds from the Epoch for a date time value.
     *
     * @param dateTime date time to compute the Epoch offset for.
     * @return milliseconds since Epoch, or a NULL_LONG value if the date time is null.
     */
    @ScriptApi
    public static long epochMillis(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return nanosToMillis(epochNanos(dateTime));
    }

    /**
     * Returns seconds since from the Epoch for a date time value.
     *
     * @param dateTime date time to compute the Epoch offset for.
     * @return seconds since Epoch, or a NULL_LONG value if the date time is null.
     */
    @ScriptApi
    public static long epochSeconds(@Nullable final Instant dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.getEpochSecond();
    }

    /**
     * Returns seconds since from the Epoch for a date time value.
     *
     * @param dateTime date time to compute the Epoch offset for.
     * @return seconds since Epoch, or a NULL_LONG value if the date time is null.
     */
    @ScriptApi
    public static long epochSeconds(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.toEpochSecond();
    }

    /**
     * Converts nanoseconds from the Epoch to an {@link Instant}.
     *
     * @param nanos nanoseconds since Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input nanoseconds from the Epoch
     *         converted to an {@link Instant}.
     */
    @ScriptApi
    @Nullable
    public static Instant epochNanosToInstant(final long nanos) {
        return nanos == NULL_LONG ? null : Instant.ofEpochSecond(nanos / 1_000_000_000L, nanos % 1_000_000_000L);
    }

    /**
     * Converts microseconds from the Epoch to an {@link Instant}.
     *
     * @param micros microseconds since Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input microseconds from the Epoch
     *         converted to an {@link Instant}.
     */
    @ScriptApi
    @Nullable
    public static Instant epochMicrosToInstant(final long micros) {
        return micros == NULL_LONG ? null : Instant.ofEpochSecond(micros / 1_000_000L, (micros % 1_000_000L) * 1_000L);
    }

    /**
     * Converts milliseconds from the Epoch to an {@link Instant}.
     *
     * @param millis milliseconds since Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input milliseconds from the Epoch
     *         converted to an {@link Instant}.
     */
    @ScriptApi
    @Nullable
    public static Instant epochMillisToInstant(final long millis) {
        return millis == NULL_LONG ? null : Instant.ofEpochSecond(millis / 1_000L, (millis % 1_000L) * 1_000_000L);
    }

    /**
     * Converts seconds from the Epoch to an {@link Instant}.
     *
     * @param seconds seconds since Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input seconds from the Epoch
     *         converted to an {@link Instant}.
     */
    @ScriptApi
    @Nullable
    public static Instant epochSecondsToInstant(final long seconds) {
        return seconds == NULL_LONG ? null : Instant.ofEpochSecond(seconds, 0);
    }

    /**
     * Converts nanoseconds from the Epoch to a {@link ZonedDateTime}.
     *
     * @param nanos nanoseconds since Epoch.
     * @param timeZone time zone.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input nanoseconds from the Epoch
     *         converted to a {@link ZonedDateTime}.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochNanosToZonedDateTime(final long nanos, final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        // noinspection ConstantConditions
        return nanos == NULL_LONG ? null : ZonedDateTime.ofInstant(epochNanosToInstant(nanos), timeZone);
    }

    /**
     * Converts microseconds from the Epoch to a {@link ZonedDateTime}.
     *
     * @param micros microseconds since Epoch.
     * @param timeZone time zone.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input microseconds from the Epoch
     *         converted to a {@link ZonedDateTime}.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochMicrosToZonedDateTime(final long micros, @Nullable ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        // noinspection ConstantConditions
        return micros == NULL_LONG ? null : ZonedDateTime.ofInstant(epochMicrosToInstant(micros), timeZone);
    }

    /**
     * Converts milliseconds from the Epoch to a {@link ZonedDateTime}.
     *
     * @param millis milliseconds since Epoch.
     * @param timeZone time zone.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input milliseconds from the Epoch
     *         converted to a {@link ZonedDateTime}.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochMillisToZonedDateTime(final long millis, final @Nullable ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        // noinspection ConstantConditions
        return millis == NULL_LONG ? null : ZonedDateTime.ofInstant(epochMillisToInstant(millis), timeZone);
    }

    /**
     * Converts seconds from the Epoch to a {@link ZonedDateTime}.
     *
     * @param seconds seconds since Epoch.
     * @param timeZone time zone.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input seconds from the Epoch
     *         converted to a {@link ZonedDateTime}.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochSecondsToZonedDateTime(final long seconds, final @Nullable ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }
        // noinspection ConstantConditions
        return seconds == NULL_LONG ? null : ZonedDateTime.ofInstant(epochSecondsToInstant(seconds), timeZone);
    }

    /**
     * Converts an offset from the Epoch to a nanoseconds from the Epoch. The offset can be in milliseconds,
     * microseconds, or nanoseconds. Expected date ranges are used to infer the units for the offset.
     *
     * @param epochOffset time offset from the Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input offset from the Epoch
     *         converted to nanoseconds from the Epoch.
     */
    @ScriptApi
    public static long epochAutoToEpochNanos(final long epochOffset) {
        if (epochOffset == NULL_LONG) {
            return epochOffset;
        }
        final long absEpoch = Math.abs(epochOffset);
        if (absEpoch > 1000 * TimeConstants.MICROTIME_THRESHOLD) { // Nanoseconds
            return epochOffset;
        }
        if (absEpoch > TimeConstants.MICROTIME_THRESHOLD) { // Microseconds
            return 1000 * epochOffset;
        }
        if (absEpoch > TimeConstants.MICROTIME_THRESHOLD / 1000) { // Milliseconds
            return 1000 * 1000 * epochOffset;
        }
        // Seconds
        return 1000 * 1000 * 1000 * epochOffset;
    }

    /**
     * Converts an offset from the Epoch to an {@link Instant}. The offset can be in milliseconds, microseconds, or
     * nanoseconds. Expected date ranges are used to infer the units for the offset.
     *
     * @param epochOffset time offset from the Epoch.
     * @return null if the input is {@link QueryConstants#NULL_LONG}; otherwise the input offset from the Epoch
     *         converted to an {@link Instant}.
     */
    @ScriptApi
    @Nullable
    public static Instant epochAutoToInstant(final long epochOffset) {
        if (epochOffset == NULL_LONG) {
            return null;
        }
        return epochNanosToInstant(epochAutoToEpochNanos(epochOffset));
    }

    /**
     * Converts an offset from the Epoch to a {@link ZonedDateTime}. The offset can be in milliseconds, microseconds, or
     * nanoseconds. Expected date ranges are used to infer the units for the offset.
     *
     * @param epochOffset time offset from the Epoch.
     * @param timeZone time zone.
     * @return null if any input is null or {@link QueryConstants#NULL_LONG}; otherwise the input offset from the Epoch
     *         converted to a {@link ZonedDateTime}.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime epochAutoToZonedDateTime(final long epochOffset, @Nullable ZoneId timeZone) {
        if (epochOffset == NULL_LONG || timeZone == null) {
            return null;
        }
        return epochNanosToZonedDateTime(epochAutoToEpochNanos(epochOffset), timeZone);
    }

    // endregion

    // region Conversions: Excel

    private static double epochMillisToExcelTime(final long millis, final ZoneId timeZone) {
        return (double) (millis + java.util.TimeZone.getTimeZone(timeZone).getOffset(millis)) / 86400000 + 25569;
    }

    private static long excelTimeToEpochMillis(final double excel, final ZoneId timeZone) {
        final java.util.TimeZone tz = java.util.TimeZone.getTimeZone(timeZone);

        final long mpo = (long) ((excel - 25569) * 86400000);
        final long o = tz.getOffset(mpo);
        final long m = mpo - o;
        final long o2 = tz.getOffset(m);
        return mpo - o2;
    }

    /**
     * Converts a date time to an Excel time represented as a double.
     *
     * @param dateTime date time to convert.
     * @param timeZone time zone to use when interpreting the date time.
     * @return 0.0 if either input is null; otherwise, the input date time converted to an Excel time represented as a
     *         double.
     */
    @ScriptApi
    public static double toExcelTime(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return 0.0;
        }

        return epochMillisToExcelTime(epochMillis(dateTime), timeZone);
    }

    /**
     * Converts a date time to an Excel time represented as a double.
     *
     * @param dateTime date time to convert.
     * @return 0.0 if either input is null; otherwise, the input date time converted to an Excel time represented as a
     *         double.
     */
    @ScriptApi
    public static double toExcelTime(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return 0.0;
        }

        return toExcelTime(toInstant(dateTime), dateTime.getZone());
    }

    /**
     * Converts an Excel time represented as a double to an {@link Instant}.
     *
     * @param excel excel time represented as a double.
     * @param timeZone time zone to use when interpreting the Excel time.
     * @return null if timeZone is null; otherwise, the input Excel time converted to an {@link Instant}.
     */
    @ScriptApi
    @Nullable
    public static Instant excelToInstant(final double excel, @Nullable final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        return epochMillisToInstant(excelTimeToEpochMillis(excel, timeZone));
    }

    /**
     * Converts an Excel time represented as a double to a {@link ZonedDateTime}.
     *
     * @param excel excel time represented as a double.
     * @param timeZone time zone to use when interpreting the Excel time.
     * @return null if timeZone is null; otherwise, the input Excel time converted to a {@link ZonedDateTime}.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime excelToZonedDateTime(final double excel, @Nullable final ZoneId timeZone) {
        if (timeZone == null) {
            return null;
        }

        return epochMillisToZonedDateTime(excelTimeToEpochMillis(excel, timeZone), timeZone);
    }

    // endregion

    // region Arithmetic

    /**
     * Adds nanoseconds to a date time.
     *
     * @param dateTime starting date time value.
     * @param nanos number of nanoseconds to add.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time plus
     *         the specified number of nanoseconds.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static Instant plus(@Nullable final Instant dateTime, final long nanos) {
        if (dateTime == null || nanos == NULL_LONG) {
            return null;
        }

        try {
            return dateTime.plusNanos(nanos);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds nanoseconds to a date time.
     *
     * @param dateTime starting date time value.
     * @param nanos number of nanoseconds to add.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time plus
     *         the specified number of nanoseconds.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime plus(@Nullable final ZonedDateTime dateTime, final long nanos) {
        if (dateTime == null || nanos == NULL_LONG) {
            return null;
        }

        try {
            return dateTime.plusNanos(nanos);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to a date time.
     *
     * @param dateTime starting date time value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time plus
     *         the specified time period.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static Instant plus(@Nullable final Instant dateTime, @Nullable final Duration period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.plus(period);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to a date time.
     *
     * @param dateTime starting date time value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time plus
     *         the specified time period.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static Instant plus(@Nullable final Instant dateTime, @Nullable final Period period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.plus(period);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to a date time.
     *
     * @param dateTime starting date time value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time plus
     *         the specified time period.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime plus(@Nullable final ZonedDateTime dateTime, @Nullable final Duration period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.plus(period);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Adds a time period to a date time.
     *
     * @param dateTime starting date time value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time plus
     *         the specified time period.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime plus(@Nullable final ZonedDateTime dateTime, @Nullable final Period period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.plus(period);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts nanoseconds from a date time.
     *
     * @param dateTime starting date time value.
     * @param nanos number of nanoseconds to subtract.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time minus
     *         the specified number of nanoseconds.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static Instant minus(@Nullable final Instant dateTime, final long nanos) {
        if (dateTime == null || nanos == NULL_LONG) {
            return null;
        }

        try {
            return dateTime.minusNanos(nanos);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts nanoseconds from a date time.
     *
     * @param dateTime starting date time value.
     * @param nanos number of nanoseconds to subtract.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time minus
     *         the specified number of nanoseconds.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime minus(@Nullable final ZonedDateTime dateTime, final long nanos) {
        if (dateTime == null || nanos == NULL_LONG) {
            return null;
        }

        try {
            return dateTime.minusNanos(nanos);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period to a date time.
     *
     * @param dateTime starting date time value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time minus
     *         the specified time period.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static Instant minus(@Nullable final Instant dateTime, @Nullable final Duration period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.minus(period);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period to a date time.
     *
     * @param dateTime starting date time value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time minus
     *         the specified time period.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static Instant minus(@Nullable final Instant dateTime, @Nullable final Period period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.minus(period);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period to a date time.
     *
     * @param dateTime starting date time value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time minus
     *         the specified time period.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime minus(@Nullable final ZonedDateTime dateTime, @Nullable final Duration period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.minus(period);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtracts a time period to a date time.
     *
     * @param dateTime starting date time value.
     * @param period time period.
     * @return null if either input is null or {@link QueryConstants#NULL_LONG}; otherwise the starting date time minus
     *         the specified time period.
     * @throws DateTimeOverflowException if the resultant date time exceeds the supported range.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime minus(@Nullable final ZonedDateTime dateTime, @Nullable final Period period) {
        if (dateTime == null || period == null) {
            return null;
        }

        try {
            return dateTime.minus(period);
        } catch (Exception ex) {
            throw new DateTimeOverflowException(ex);
        }
    }

    /**
     * Subtract one date time from another and return the difference in nanoseconds.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in dateTime1 and
     *         dateTime2 in nanoseconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static long minus(@Nullable final Instant dateTime1, @Nullable final Instant dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return NULL_LONG;
        }

        return checkUnderflowMinus(epochNanos(dateTime1), epochNanos(dateTime2), true);
    }

    /**
     * Subtract one date time from another and return the difference in nanoseconds.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in dateTime1 and
     *         dateTime2 in nanoseconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static long minus(@Nullable final ZonedDateTime dateTime1, @Nullable final ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return NULL_LONG;
        }

        return checkUnderflowMinus(epochNanos(dateTime1), epochNanos(dateTime2), true);
    }

    /**
     * Returns the difference in nanoseconds between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in
     *         nanoseconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static long diffNanos(@Nullable final Instant start, @Nullable final Instant end) {
        return minus(end, start);
    }

    /**
     * Returns the difference in nanoseconds between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in
     *         nanoseconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static long diffNanos(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        return minus(end, start);
    }

    /**
     * Returns the difference in microseconds between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in
     *         microseconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static long diffMicros(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMicros(diffNanos(start, end));
    }

    /**
     * Returns the difference in microseconds between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in
     *         microseconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static long diffMicros(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMicros(diffNanos(start, end));
    }

    /**
     * Returns the difference in milliseconds between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in
     *         milliseconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static long diffMillis(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMillis(diffNanos(start, end));
    }

    /**
     * Returns the difference in milliseconds between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in start and end in
     *         milliseconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static long diffMillis(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_LONG;
        }

        return nanosToMillis(diffNanos(start, end));
    }

    /**
     * Returns the difference in seconds between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in
     *         seconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static double diffSeconds(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / SECOND;
    }

    /**
     * Returns the difference in seconds between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in
     *         seconds.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static double diffSeconds(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / SECOND;
    }

    /**
     * Returns the difference in minutes between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in
     *         minutes.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static double diffMinutes(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / MINUTE;
    }

    /**
     * Returns the difference in minutes between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in
     *         minutes.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static double diffMinutes(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / MINUTE;
    }

    /**
     * Returns the difference in days between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in
     *         days.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static double diffDays(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / DAY;
    }

    /**
     * Returns the difference in days between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in
     *         days.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static double diffDays(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) / DAY;
    }

    /**
     * Returns the difference in years (365-days) between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in
     *         years.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static double diffYears(@Nullable final Instant start, @Nullable final Instant end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) * YEARS_PER_NANO;
    }

    /**
     * Returns the difference in years (365-days) between two date time values.
     *
     * @param start start time.
     * @param end end time.
     * @return {@link QueryConstants#NULL_DOUBLE} if either input is null; otherwise the difference in start and end in
     *         years.
     * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
     */
    @ScriptApi
    public static double diffYears(@Nullable final ZonedDateTime start, @Nullable final ZonedDateTime end) {
        if (start == null || end == null) {
            return io.deephaven.util.QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNanos(start, end) * YEARS_PER_NANO;
    }

    // endregion

    // region Comparisons

    /**
     * Evaluates whether one date time value is before a second date time value.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return true if dateTime1 is before dateTime2; otherwise, false if either value is null or if dateTime2 is equal
     *         to or before dateTime1.
     */
    @ScriptApi
    public static boolean isBefore(@Nullable final Instant dateTime1, @Nullable final Instant dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isBefore(dateTime2);
    }

    /**
     * Evaluates whether one date time value is before a second date time value.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return true if dateTime1 is before dateTime2; otherwise, false if either value is null or if dateTime2 is equal
     *         to or before dateTime1.
     */
    @ScriptApi
    public static boolean isBefore(@Nullable final ZonedDateTime dateTime1, @Nullable final ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isBefore(dateTime2);
    }

    /**
     * Evaluates whether one date time value is before or equal to a second date time value.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return true if dateTime1 is before or equal to dateTime2; otherwise, false if either value is null or if
     *         dateTime2 is before dateTime1.
     */
    @ScriptApi
    public static boolean isBeforeOrEqual(@Nullable final Instant dateTime1, @Nullable final Instant dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isBefore(dateTime2) || dateTime1.equals(dateTime2);
    }

    /**
     * Evaluates whether one date time value is before or equal to a second date time value.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return true if dateTime1 is before or equal to dateTime2; otherwise, false if either value is null or if
     *         dateTime2 is before dateTime1.
     */
    @ScriptApi
    public static boolean isBeforeOrEqual(@Nullable final ZonedDateTime dateTime1,
            @Nullable final ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isBefore(dateTime2) || dateTime1.equals(dateTime2);
    }

    /**
     * Evaluates whether one date time value is after a second date time value.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return true if dateTime1 is after dateTime2; otherwise, false if either value is null or if dateTime2 is equal
     *         to or after dateTime1.
     */
    @ScriptApi
    public static boolean isAfter(@Nullable final Instant dateTime1, @Nullable final Instant dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isAfter(dateTime2);
    }

    /**
     * Evaluates whether one date time value is after a second date time value.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return true if dateTime1 is after dateTime2; otherwise, false if either value is null or if dateTime2 is equal
     *         to or after dateTime1.
     */
    @ScriptApi
    public static boolean isAfter(@Nullable final ZonedDateTime dateTime1, @Nullable final ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isAfter(dateTime2);
    }

    /**
     * Evaluates whether one date time value is after or equal to a second date time value.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return true if dateTime1 is after or equal to dateTime2; otherwise, false if either value is null or if
     *         dateTime2 is after dateTime1.
     */
    @ScriptApi
    public static boolean isAfterOrEqual(@Nullable final Instant dateTime1, @Nullable final Instant dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isAfter(dateTime2) || dateTime1.equals(dateTime2);
    }

    /**
     * Evaluates whether one date time value is after or equal to a second date time value.
     *
     * @param dateTime1 first date time.
     * @param dateTime2 second date time.
     * @return true if dateTime1 is after or equal to dateTime2; otherwise, false if either value is null or if
     *         dateTime2 is after dateTime1.
     */
    @ScriptApi
    public static boolean isAfterOrEqual(@Nullable final ZonedDateTime dateTime1, @Nullable ZonedDateTime dateTime2) {
        if (dateTime1 == null || dateTime2 == null) {
            return false;
        }

        return dateTime1.isAfter(dateTime2) || dateTime1.equals(dateTime2);
    }

    // endregion

    // region Chronology

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the millisecond.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if the input is null; otherwise, number of nanoseconds that have elapsed
     *         since the top of the millisecond.
     */
    @ScriptApi
    public static int nanosOfMilli(@Nullable final Instant dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) (epochNanos(dateTime) % 1000000);
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the millisecond.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if the input is null; otherwise, number of nanoseconds that have elapsed
     *         since the top of the millisecond.
     */
    @ScriptApi
    public static int nanosOfMilli(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return nanosOfMilli(toInstant(dateTime));
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the millisecond. Nanoseconds are rounded,
     * not dropped -- '20:41:39.123456700' has 457 micros, not 456.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if the input is null; otherwise, number of microseconds that have elapsed
     *         since the top of the millisecond.
     */
    @ScriptApi
    public static int microsOfMilli(@Nullable final Instant dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) Math.round((epochNanos(dateTime) % 1000000) / 1000d);
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the millisecond. Nanoseconds are rounded,
     * not dropped -- '20:41:39.123456700' has 457 micros, not 456.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if the input is null; otherwise, number of microseconds that have elapsed
     *         since the top of the millisecond.
     */
    @ScriptApi
    public static int microsOfMilli(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return microsOfMilli(toInstant(dateTime));
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise, number of nanoseconds that have
     *         elapsed since the top of the second.
     */
    @ScriptApi
    public static long nanosOfSecond(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return NULL_LONG;
        }

        return nanosOfSecond(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise, number of nanoseconds that have
     *         elapsed since the top of the second.
     */
    @ScriptApi
    public static long nanosOfSecond(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.getNano();
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise, number of microseconds that have
     *         elapsed since the top of the second.
     */
    @ScriptApi
    public static long microsOfSecond(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return NULL_LONG;
        }

        return nanosToMicros(nanosOfSecond(dateTime, timeZone));
    }

    /**
     * Returns the number of microseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise, number of microseconds that have
     *         elapsed since the top of the second.
     */
    @ScriptApi
    public static long microsOfSecond(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return nanosToMicros(nanosOfSecond(dateTime));
    }

    /**
     * Returns the number of milliseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of milliseconds that have
     *         elapsed since the top of the second.
     */
    @ScriptApi
    public static int millisOfSecond(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) nanosToMillis(nanosOfSecond(dateTime, timeZone));
    }

    /**
     * Returns the number of milliseconds that have elapsed since the top of the second.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of milliseconds that have
     *         elapsed since the top of the second.
     */
    @ScriptApi
    public static int millisOfSecond(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) nanosToMillis(nanosOfSecond(dateTime));
    }

    /**
     * Returns the number of seconds that have elapsed since the top of the minute.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of seconds that have elapsed
     *         since the top of the minute.
     */
    @ScriptApi
    public static int secondOfMinute(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return toZonedDateTime(dateTime, timeZone).getSecond();
    }

    /**
     * Returns the number of seconds that have elapsed since the top of the minute.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of seconds that have elapsed
     *         since the top of the minute.
     */
    @ScriptApi
    public static int secondOfMinute(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getSecond();
    }

    /**
     * Returns the number of minutes that have elapsed since the top of the hour.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of minutes that have elapsed
     *         since the top of the hour.
     */
    @ScriptApi
    public static int minuteOfHour(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return toZonedDateTime(dateTime, timeZone).getMinute();
    }

    /**
     * Returns the number of minutes that have elapsed since the top of the hour.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of minutes that have elapsed
     *         since the top of the hour.
     */
    @ScriptApi
    public static int minuteOfHour(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getMinute();
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of nanoseconds that have
     *         elapsed since the top of the day.
     */
    @ScriptApi
    public static long nanosOfDay(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return NULL_LONG;
        }

        return nanosOfDay(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns the number of nanoseconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of nanoseconds that have
     *         elapsed since the top of the day.
     */
    @ScriptApi
    public static long nanosOfDay(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return NULL_LONG;
        }

        return dateTime.getHour() * HOUR + dateTime.getMinute() * MINUTE + dateTime.getSecond() * SECOND
                + dateTime.getNano();
    }

    /**
     * Returns the number of milliseconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of milliseconds that have
     *         elapsed since the top of the day.
     */
    @ScriptApi
    public static int millisOfDay(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) nanosToMillis(nanosOfDay(dateTime, timeZone));
    }

    /**
     * Returns the number of milliseconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of milliseconds that have
     *         elapsed since the top of the day.
     */
    @ScriptApi
    public static int millisOfDay(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) nanosToMillis(nanosOfDay(dateTime));
    }

    /**
     * Returns the number of seconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of seconds that have elapsed
     *         since the top of the day.
     */
    @ScriptApi
    public static int secondOfDay(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) nanosToSeconds(nanosOfDay(dateTime, timeZone));
    }

    /**
     * Returns the number of seconds that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of seconds that have elapsed
     *         since the top of the day.
     */
    @ScriptApi
    public static int secondOfDay(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return (int) nanosToSeconds(nanosOfDay(dateTime));
    }

    /**
     * Returns the number of minutes that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of minutes that have elapsed
     *         since the top of the day.
     */
    @ScriptApi
    public static int minuteOfDay(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return secondOfDay(dateTime, timeZone) / 60;
    }

    /**
     * Returns the number of minutes that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of minutes that have elapsed
     *         since the top of the day.
     */
    @ScriptApi
    public static int minuteOfDay(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return secondOfDay(dateTime) / 60;
    }

    /**
     * Returns the number of hours that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of hours that have elapsed
     *         since the top of the day.
     */
    @ScriptApi
    public static int hourOfDay(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return hourOfDay(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns the number of hours that have elapsed since the top of the day.
     *
     * @param dateTime time.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, number of hours that have elapsed
     *         since the top of the day.
     */
    @ScriptApi
    public static int hourOfDay(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getHour();
    }

    /**
     * Returns a 1-based int value of the day of the week for a date time in the specified time zone, with 1 being
     * Monday and 7 being Sunday.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the week.
     */
    @ScriptApi
    public static int dayOfWeek(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dayOfWeek(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns a 1-based int value of the day of the week for a date time in the specified time zone, with 1 being
     * Monday and 7 being Sunday.
     *
     * @param dateTime time to find the day of the month of.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the week.
     */
    @ScriptApi
    public static int dayOfWeek(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getDayOfWeek().getValue();
    }

    /**
     * Returns a 1-based int value of the day of the month for a date time and specified time zone. The first day of the
     * month returns 1, the second day returns 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return A {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the month.
     */
    @ScriptApi
    public static int dayOfMonth(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dayOfMonth(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns a 1-based int value of the day of the month for a date time and specified time zone. The first day of the
     * month returns 1, the second day returns 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @return A {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the month.
     */
    @ScriptApi
    public static int dayOfMonth(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getDayOfMonth();
    }

    /**
     * Returns a 1-based int value of the day of the year (Julian date) for a date time in the specified time zone. The
     * first day of the year returns 1, the second day returns 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the year.
     */
    @ScriptApi
    public static int dayOfYear(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dayOfYear(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns a 1-based int value of the day of the year (Julian date) for a date time in the specified time zone. The
     * first day of the year returns 1, the second day returns 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the day of the year.
     */
    @ScriptApi
    public static int dayOfYear(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getDayOfYear();
    }

    /**
     * Returns a 1-based int value of the month of the year (Julian date) for a date time in the specified time zone.
     * January is 1, February is 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the month of the year.
     */
    @ScriptApi
    public static int monthOfYear(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return monthOfYear(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns a 1-based int value of the month of the year (Julian date) for a date time in the specified time zone.
     * January is 1, February is 2, etc.
     *
     * @param dateTime time to find the day of the month of.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the month of the year.
     */
    @ScriptApi
    public static int monthOfYear(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getMonthValue();
    }

    /**
     * Returns the year for a date time in the specified time zone.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the year.
     */
    @ScriptApi
    public static int year(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return year(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns the year for a date time in the specified time zone.
     *
     * @param dateTime time to find the day of the month of.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the year.
     */
    @ScriptApi
    public static int year(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return dateTime.getYear();
    }

    /**
     * Returns the year of the century (two-digit year) for a date time in the specified time zone.
     *
     * @param dateTime time to find the day of the month of.
     * @param timeZone time zone.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the year of the century (two-digit
     *         year).
     */
    @ScriptApi
    public static int yearOfCentury(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return year(dateTime, timeZone) % 100;
    }

    /**
     * Returns the year of the century (two-digit year) for a date time in the specified time zone.
     *
     * @param dateTime time to find the day of the month of.
     * @return {@link QueryConstants#NULL_INT} if either input is null; otherwise, the year of the century (two-digit
     *         year).
     */
    @ScriptApi
    public static int yearOfCentury(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return io.deephaven.util.QueryConstants.NULL_INT;
        }

        return year(dateTime) % 100;
    }

    /**
     * Returns a date time for the prior midnight in the specified time zone.
     *
     * @param dateTime time to compute the prior midnight for.
     * @param timeZone time zone.
     * @return null if either input is null; otherwise a date time representing the prior midnight in the specified time
     *         zone.
     */
    @ScriptApi
    @Nullable
    public static Instant atMidnight(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return toInstant(atMidnight(toZonedDateTime(dateTime, timeZone)));
    }

    /**
     * Returns a date time for the prior midnight in the specified time zone.
     *
     * @param dateTime time to compute the prior midnight for.
     * @return null if either input is null; otherwise a date time representing the prior midnight in the specified time
     *         zone.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime atMidnight(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return dateTime.toLocalDate().atStartOfDay(dateTime.getZone());
    }

    // endregion

    // region Binning

    /**
     * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
     * five-minute window that contains the input date time.
     *
     * @param dateTime date time for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @return null if either input is null; otherwise, a date time representing the start of the window.
     */
    @ScriptApi
    @Nullable
    public static Instant lowerBin(@Nullable final Instant dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return epochNanosToInstant(Numeric.lowerBin(epochNanos(dateTime), intervalNanos));
    }

    /**
     * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
     * five-minute window that contains the input date time.
     *
     * @param dateTime date time for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @return null if either input is null; otherwise, a date time representing the start of the window.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime lowerBin(@Nullable final ZonedDateTime dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return epochNanosToZonedDateTime(Numeric.lowerBin(epochNanos(dateTime), intervalNanos), dateTime.getZone());
    }

    /**
     * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
     * five-minute window that contains the input date time.
     *
     * @param dateTime date time for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return null if either input is null; otherwise, a date time representing the start of the window.
     */
    @ScriptApi
    @Nullable
    public static Instant lowerBin(final @Nullable Instant dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == NULL_LONG || offset == NULL_LONG) {
            return null;
        }

        return epochNanosToInstant(Numeric.lowerBin(epochNanos(dateTime) - offset, intervalNanos) + offset);
    }

    /**
     * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
     * five-minute window that contains the input date time.
     *
     * @param dateTime date time for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return null if either input is null; otherwise, a date time representing the start of the window.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime lowerBin(final @Nullable ZonedDateTime dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == NULL_LONG || offset == NULL_LONG) {
            return null;
        }

        return epochNanosToZonedDateTime(Numeric.lowerBin(epochNanos(dateTime) - offset, intervalNanos) + offset,
                dateTime.getZone());
    }

    /**
     * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
     * five-minute window that contains the input date time.
     *
     * @param dateTime date time for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @return null if either input is null; otherwise, a date time representing the end of the window.
     */
    @ScriptApi
    @Nullable
    public static Instant upperBin(final @Nullable Instant dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return epochNanosToInstant(Numeric.upperBin(epochNanos(dateTime), intervalNanos));
    }

    /**
     * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
     * five-minute window that contains the input date time.
     *
     * @param dateTime date time for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @return null if either input is null; otherwise, a date time representing the end of the window.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime upperBin(final @Nullable ZonedDateTime dateTime, long intervalNanos) {
        if (dateTime == null || intervalNanos == NULL_LONG) {
            return null;
        }

        return epochNanosToZonedDateTime(Numeric.upperBin(epochNanos(dateTime), intervalNanos), dateTime.getZone());
    }

    /**
     * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
     * five-minute window that contains the input date time.
     *
     * @param dateTime date time for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return null if either input is null; otherwise, a date time representing the end of the window.
     */
    @ScriptApi
    @Nullable
    public static Instant upperBin(@Nullable final Instant dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == NULL_LONG
                || offset == NULL_LONG) {
            return null;
        }

        return epochNanosToInstant(Numeric.upperBin(epochNanos(dateTime) - offset, intervalNanos) + offset);
    }

    /**
     * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
     * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
     * five-minute window that contains the input date time.
     *
     * @param dateTime date time for which to evaluate the start of the containing window.
     * @param intervalNanos size of the window in nanoseconds.
     * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
     *        one minute.
     * @return null if either input is null; otherwise, a date time representing the end of the window.
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime upperBin(@Nullable final ZonedDateTime dateTime, long intervalNanos, long offset) {
        if (dateTime == null || intervalNanos == NULL_LONG
                || offset == NULL_LONG) {
            return null;
        }

        return epochNanosToZonedDateTime(Numeric.upperBin(epochNanos(dateTime) - offset, intervalNanos) + offset,
                dateTime.getZone());
    }

    // endregion

    // region Format

    /**
     * Pads a string with zeros.
     *
     * @param str string.
     * @param length desired time string length.
     * @return input string padded with zeros to the desired length. If the input string is longer than the desired
     *         length, the input string is returned.
     */
    @NotNull
    static String padZeros(@NotNull final String str, final int length) {
        if (length <= str.length()) {
            return str;
        }
        return "0".repeat(length - str.length()) + str;
    }

    /**
     * Returns a nanosecond duration formatted as a "hhh:mm:ss.nnnnnnnnn" string.
     *
     * @param nanos nanoseconds, or null if the input is {@link QueryConstants#NULL_LONG}.
     * @return the nanosecond duration formatted as a "hhh:mm:ss.nnnnnnnnn" string.
     */
    @ScriptApi
    @Nullable
    public static String formatNanos(long nanos) {
        if (nanos == NULL_LONG) {
            return null;
        }

        StringBuilder buf = new StringBuilder(25);

        if (nanos < 0) {
            buf.append('-');
            nanos = -nanos;
        }

        int hours = (int) (nanos / 3600000000000L);

        nanos %= 3600000000000L;

        int minutes = (int) (nanos / 60000000000L);

        nanos %= 60000000000L;

        int seconds = (int) (nanos / 1000000000L);

        nanos %= 1000000000L;

        buf.append(hours).append(':').append(padZeros(String.valueOf(minutes), 2)).append(':')
                .append(padZeros(String.valueOf(seconds), 2));

        if (nanos != 0) {
            buf.append('.').append(padZeros(String.valueOf(nanos), 9));
        }

        return buf.toString();
    }

    /**
     * Returns a date time formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.
     *
     * @param dateTime time to format as a string.
     * @param timeZone time zone to use when formatting the string.
     * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ"
     *         string.
     */
    @ScriptApi
    @Nullable
    public static String formatDateTime(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return formatDateTime(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns a date time formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.
     *
     * @param dateTime time to format as a string.
     * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ"
     *         string.
     */
    @ScriptApi
    @Nullable
    public static String formatDateTime(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        final String timeZone = TimeZoneAliases.zoneName(dateTime.getZone());
        final String ldt = ISO_LOCAL_DATE_TIME.format(dateTime);
        final StringBuilder sb = new StringBuilder();

        sb.append(ldt);

        int pad = 29 - ldt.length();

        if (ldt.length() == 19) {
            sb.append(".");
            pad--;
        }

        return sb.append("0".repeat(Math.max(0, pad))).append(" ").append(timeZone).toString();
    }

    /**
     * Returns a date time formatted as a "yyyy-MM-dd" string.
     *
     * @param dateTime time to format as a string.
     * @param timeZone time zone to use when formatting the string.
     * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-dd" string.
     */
    @ScriptApi
    @Nullable
    public static String formatDate(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
        if (dateTime == null || timeZone == null) {
            return null;
        }

        return formatDate(toZonedDateTime(dateTime, timeZone));
    }

    /**
     * Returns a date time formatted as a "yyyy-MM-dd" string.
     *
     * @param dateTime time to format as a string.
     * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-dd" string.
     */
    @ScriptApi
    @Nullable
    public static String formatDate(@Nullable final ZonedDateTime dateTime) {
        if (dateTime == null) {
            return null;
        }

        return ISO_LOCAL_DATE.format(dateTime);
    }

    // endregion

    // region Parse

    /**
     * Parses the string argument as a time zone.
     *
     * @param s string to be converted
     * @return a {@link ZoneId} represented by the input string.
     * @throws RuntimeException if the string cannot be converted.
     * @see ZoneId
     * @see TimeZoneAliases
     */
    @ScriptApi
    @NotNull
    public static ZoneId parseTimeZone(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new RuntimeException("Cannot parse time zone ID (null): " + s);
        }

        return TimeZoneAliases.zoneId(s);
    }

    /**
     * Parses the string argument as a time zone.
     *
     * @param s string to be converted
     * @return a {@link ZoneId} represented by the input string, or null if the string can not be parsed.
     * @see ZoneId
     * @see TimeZoneAliases
     */
    @ScriptApi
    @Nullable
    public static ZoneId parseTimeZoneQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseTimeZone(s);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Converts a String of digits of any length to a nanoseconds long value. Will ignore anything longer than 9 digits,
     * and will throw a NumberFormatException if any non-numeric character is found. Strings shorter than 9 digits will
     * be interpreted as sub-second values to the right of the decimal point.
     *
     * @param s string to convert.
     * @return value in nanoseconds.
     * @throws NumberFormatException if any non-numeric character is found.
     */
    @ScriptApi
    private static long parseNanosInternal(@NotNull final String s) {
        long result = 0;
        for (int i = 0; i < 9; i++) {
            result *= 10;
            final int digit;
            if (i >= s.length()) {
                digit = 0;
            } else {
                digit = Character.digit(s.charAt(i), 10);
                if (digit < 0) {
                    throw new NumberFormatException("Invalid character for nanoseconds conversion: " + s.charAt(i));
                }
            }
            result += digit;
        }
        return result;
    }

    /**
     * Parses the string argument as a time duration in nanoseconds.
     * <p>
     * Time duration strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]} or as a duration string formatted as
     * {@code [-]PnDTnHnMn.nS}.
     *
     * @param s string to be converted.
     * @return the number of nanoseconds represented by the string.
     * @throws RuntimeException if the string cannot be parsed.
     * @see #parseDuration(String)
     * @see #parseDurationQuiet(String)
     */
    @ScriptApi
    public static long parseDuratioNanos(@NotNull String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new RuntimeException("Cannot parse time: " + s);
        }

        try {
            if (TIME_DURATION_PATTERN.matcher(s).matches()) {
                long multiplier = 1;
                long dayNanos = 0;
                long subsecondNanos = 0;

                if (s.charAt(0) == '-') {
                    multiplier = -1;

                    s = s.substring(1);
                }

                int decimalIndex = s.indexOf('.');

                if (decimalIndex != -1) {
                    subsecondNanos = parseNanosInternal(s.substring(decimalIndex + 1));

                    s = s.substring(0, decimalIndex);
                }

                String[] tokens = s.split(":");

                if (tokens.length == 2) { // hh:mm
                    return multiplier
                            * (1000000000L * (3600L * Integer.parseInt(tokens[0]) + 60L * Integer.parseInt(tokens[1]))
                                    + dayNanos + subsecondNanos);
                } else if (tokens.length == 3) { // hh:mm:ss
                    return multiplier
                            * (1000000000L * (3600L * Integer.parseInt(tokens[0]) + 60L * Integer.parseInt(tokens[1])
                                    + Integer.parseInt(tokens[2])) + dayNanos + subsecondNanos);
                }
            }

            return parseDuration(s).toNanos();
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse time : " + s, e);
        }
    }

    /**
     * Parses the string argument as a time duration in nanoseconds.
     * <p>
     * Time duration strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]} or as a duration string formatted as
     * {@code [-]PnDTnHnMn.nS}.
     *
     * @param s string to be converted.
     * @return the number of nanoseconds represented by the string, or {@link QueryConstants#NULL_LONG} if the string
     *         cannot be parsed.
     * @see #parseDuration(String)
     * @see #parseDurationQuiet(String)
     */
    @ScriptApi
    public static long parseDurationNanosQuiet(@Nullable String s) {
        if (s == null || s.length() <= 1) {
            return NULL_LONG;
        }

        try {
            return parseDuratioNanos(s);
        } catch (Exception e) {
            return NULL_LONG;
        }
    }

    /**
     * Parses the string argument as a period, which is a unit of time in terms of calendar time (days, weeks, months,
     * years, etc.).
     * <p>
     * Period strings are formatted according to the ISO-8601 duration format as {@code PnYnMnD} and {@code PnW}, where
     * the coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin
     * with a negative sign.
     * <p>
     * Examples:
     * 
     * <pre>
     *   "P2Y"             -- Period.ofYears(2)
     *   "P3M"             -- Period.ofMonths(3)
     *   "P4W"             -- Period.ofWeeks(4)
     *   "P5D"             -- Period.ofDays(5)
     *   "P1Y2M3D"         -- Period.of(1, 2, 3)
     *   "P1Y2M3W4D"       -- Period.of(1, 2, 25)
     *   "P-1Y2M"          -- Period.of(-1, 2, 0)
     *   "-P1Y2M"          -- Period.of(-1, -2, 0)
     * </pre>
     *
     * @param s period string.
     * @return the period.
     * @throws RuntimeException if the string cannot be parsed.
     * @see Period#parse(CharSequence)
     */
    @ScriptApi
    @NotNull
    public static Period parsePeriod(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new RuntimeException("Cannot parse period (null): " + s);
        }

        try {
            return Period.parse(s);
        } catch (Exception ex) {
            throw new RuntimeException("Cannot parse period: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a period, which is a unit of time in terms of calendar time (days, weeks, months,
     * years, etc.).
     * <p>
     * Period strings are formatted according to the ISO-8601 duration format as {@code PnYnMnD} and {@code PnW}, where
     * the coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin
     * with a negative sign.
     * <p>
     * Examples:
     * 
     * <pre>
     *   "P2Y"             -- Period.ofYears(2)
     *   "P3M"             -- Period.ofMonths(3)
     *   "P4W"             -- Period.ofWeeks(4)
     *   "P5D"             -- Period.ofDays(5)
     *   "P1Y2M3D"         -- Period.of(1, 2, 3)
     *   "P1Y2M3W4D"       -- Period.of(1, 2, 25)
     *   "P-1Y2M"          -- Period.of(-1, 2, 0)
     *   "-P1Y2M"          -- Period.of(-1, -2, 0)
     * </pre>
     *
     * @param s period string.
     * @return the period, or null if the string can not be parsed.
     * @see Period#parse(CharSequence)
     */
    @ScriptApi
    @Nullable
    public static Period parsePeriodQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parsePeriod(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as a duration, which is a unit of time in terms of clock time (24-hour days, hours,
     * minutes, seconds, and nanoseconds).
     * <p>
     * Duration strings are formatted according to the ISO-8601 duration format as {@code [-]PnDTnHnMn.nS}, where the
     * coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin with
     * a negative sign.
     * <p>
     * Examples:
     * 
     * <pre>
     *    "PT20.345S" -- parses as "20.345 seconds"
     *    "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
     *    "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
     *    "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
     *    "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
     *    "PT-6H3M"    -- parses as "-6 hours and +3 minutes"
     *    "-PT6H3M"    -- parses as "-6 hours and -3 minutes"
     *    "-PT-6H+3M"  -- parses as "+6 hours and -3 minutes"
     * </pre>
     *
     * @param s duration string.
     * @return the duration.
     * @throws RuntimeException if the string cannot be parsed.
     * @see Duration#parse(CharSequence)
     */
    @ScriptApi
    @NotNull
    public static Duration parseDuration(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new RuntimeException("Cannot parse duration (null): " + s);
        }

        try {
            return Duration.parse(s);
        } catch (Exception ex) {
            throw new RuntimeException("Cannot parse duration: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a duration, which is a unit of time in terms of clock time (24-hour days, hours,
     * minutes, seconds, and nanoseconds).
     * <p>
     * Duration strings are formatted according to the ISO-8601 duration format as {@code [-]PnDTnHnMn.nS}, where the
     * coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin with
     * a negative sign.
     * <p>
     * Examples:
     * 
     * <pre>
     *    "PT20.345S" -- parses as "20.345 seconds"
     *    "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
     *    "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
     *    "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
     *    "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
     *    "PT-6H3M"    -- parses as "-6 hours and +3 minutes"
     *    "-PT6H3M"    -- parses as "-6 hours and -3 minutes"
     *    "-PT-6H+3M"  -- parses as "+6 hours and -3 minutes"
     * </pre>
     *
     * @param s duration string.
     * @return the duration, or null if the string can not be parsed.
     * @see Duration#parse(CharSequence)
     */
    @ScriptApi
    @Nullable
    public static Duration parseDurationQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseDuration(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as nanoseconds since the Epoch.
     *
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others. Additionally, date time strings can be integer values that
     * are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.
     *
     * @param s date time string.
     * @return a date time represented by the input string.
     * @throws RuntimeException if the string cannot be parsed.
     * @see #epochAutoToEpochNanos
     * @see #parseInstant(String)
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    public static long parseEpochNanos(@NotNull final String s) {
        if (LONG_PATTERN.matcher(s).matches()) {
            return epochAutoToEpochNanos(Long.parseLong(s));
        }

        return epochNanos(parseZonedDateTime(s));
    }

    /**
     * Parses the string argument as a date time.
     *
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others. Additionally, date time strings can be integer values that
     * are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.
     *
     * @param s date time string.
     * @return a date time represented by the input string, or null if the string can not be parsed.
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    public static long parseEpochNanosQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return NULL_LONG;
        }

        try {
            return parseEpochNanos(s);
        } catch (Exception e) {
            return NULL_LONG;
        }
    }

    /**
     * Parses the string argument as a date time.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others. Additionally, date time strings can be integer values that
     * are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.
     *
     * @param s date time string.
     * @return a date time represented by the input string.
     * @throws RuntimeException if the string cannot be parsed.
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    @NotNull
    public static Instant parseInstant(@NotNull final String s) {
        if (LONG_PATTERN.matcher(s).matches()) {
            final long nanos = epochAutoToEpochNanos(Long.parseLong(s));
            // noinspection ConstantConditions
            return epochNanosToInstant(nanos);
        }

        return parseZonedDateTime(s).toInstant();
    }

    /**
     * Parses the string argument as a date time.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others. Additionally, date time strings can be integer values that
     * are nanoseconds, milliseconds, or seconds from the Epoch. Expected date ranges are used to infer the units.
     *
     * @param s date time string.
     * @return a date time represented by the input string, or null if the string can not be parsed.
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    @Nullable
    public static Instant parseInstantQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseInstant(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as a date time.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
     *
     * @param s date time string.
     * @return a date time represented by the input string.
     * @throws RuntimeException if the string cannot be parsed.
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    @NotNull
    public static ZonedDateTime parseZonedDateTime(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new RuntimeException("Cannot parse datetime (null): " + s);
        }

        try {
            return ZonedDateTime.parse(s);
        } catch (DateTimeParseException e) {
            // ignore
        }

        try {
            final Matcher dtzMatcher = DATE_TZ_PATTERN.matcher(s);
            if (dtzMatcher.matches()) {
                final String dateString = dtzMatcher.group("date");
                final String timeZoneString = dtzMatcher.group("timezone");
                final ZoneId timeZone = parseTimeZoneQuiet(timeZoneString);

                if (timeZone == null) {
                    throw new RuntimeException("No matching time zone: '" + timeZoneString + "'");
                }

                return LocalDate.parse(dateString, FORMATTER_ISO_LOCAL_DATE).atTime(LocalTime.of(0, 0))
                        .atZone(timeZone);
            }

            int spaceIndex = s.indexOf(' ');
            if (spaceIndex == -1) {
                throw new RuntimeException("No time zone provided");
            }

            final String dateTimeString = s.substring(0, spaceIndex);
            final String timeZoneString = s.substring(spaceIndex + 1);
            final ZoneId timeZone = parseTimeZoneQuiet(timeZoneString);

            if (timeZone == null) {
                throw new RuntimeException("No matching time zone: " + timeZoneString);
            }

            return LocalDateTime.parse(dateTimeString, FORMATTER_ISO_LOCAL_DATE_TIME).atZone(timeZone);
        } catch (Exception ex) {
            throw new RuntimeException("Cannot parse datetime: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a date time.
     * <p>
     * Date time strings are formatted according to the ISO 8601 date time format
     * {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
     *
     * @param s date time string.
     * @return a date time represented by the input string, or null if the string can not be parsed.
     * @see DateTimeFormatter#ISO_INSTANT
     */
    @ScriptApi
    @Nullable
    public static ZonedDateTime parseZonedDateTimeQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseZonedDateTime(s);
        } catch (Exception e) {
            return null;
        }
    }

    private enum DateGroupId {
        // Date(1),
        Year(2, ChronoField.YEAR), Month(3, ChronoField.MONTH_OF_YEAR), Day(4, ChronoField.DAY_OF_MONTH),
        // Tod(5),
        Hours(6, ChronoField.HOUR_OF_DAY), Minutes(7, ChronoField.MINUTE_OF_HOUR), Seconds(8,
                ChronoField.SECOND_OF_MINUTE), Fraction(9, ChronoField.MILLI_OF_SECOND);
        // TODO MICRO and NANOs are not supported! -- fix and unit test!

        final int id;
        final ChronoField field;

        DateGroupId(int id, @NotNull ChronoField field) {
            this.id = id;
            this.field = field;
        }
    }

    /**
     * Returns a {@link ChronoField} indicating the level of precision in a time or datetime string.
     *
     * @param s time string.
     * @return {@link ChronoField} for the finest units in the string (e.g. "10:00:00" would yield SecondOfMinute).
     * @throws RuntimeException if the string cannot be parsed.
     */
    @ScriptApi
    @NotNull
    public static ChronoField parseTimePrecision(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new RuntimeException("Cannot parse time precision (null): " + s);
        }

        try {
            Matcher dtMatcher = CAPTURING_DATETIME_PATTERN.matcher(s);
            if (dtMatcher.matches()) {
                DateGroupId[] parts = DateGroupId.values();
                for (int i = parts.length - 1; i >= 0; i--) {
                    String part = dtMatcher.group(parts[i].id);
                    if (part != null && !part.isEmpty()) {
                        return parts[i].field;
                    }
                }
            }

            throw new RuntimeException("Time precision does not match expected pattern");
        } catch (Exception ex) {
            throw new RuntimeException("Cannot parse time precision: " + s, ex);
        }
    }

    /**
     * Returns a {@link ChronoField} indicating the level of precision in a time or datetime string.
     *
     * @param s time string.
     * @return null if the time string cannot be parsed; otherwise, a {@link ChronoField} for the finest units in the
     *         string (e.g. "10:00:00" would yield SecondOfMinute).
     * @throws RuntimeException if the string cannot be parased.
     */
    @ScriptApi
    @Nullable
    public static ChronoField parseTimePrecisionQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseTimePrecision(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Format style for a date string.
     */
    @ScriptApi
    public enum DateStyle {
        /**
         * Month, day, year date format.
         */
        MDY,
        /**
         * Day, month, year date format.
         */
        DMY,
        /**
         * Year, month, day date format.
         */
        YMD
    }

    // see if we can match one of the slash-delimited styles, the interpretation of which requires knowing the
    // system date style setting (for example Europeans often write dates as d/m/y).
    @NotNull
    private static LocalDate matchLocalDate(final Matcher matcher, final DateStyle dateStyle) {
        final String yearGroup, monthGroup, dayGroup, yearFinal2DigitsGroup;
        // note we have nested groups which allow us to detect 2 vs 4 digit year
        // (groups 2 and 5 are the optional last 2 digits)
        switch (dateStyle) {
            case MDY:
                dayGroup = "part2";
                monthGroup = "part1";
                yearGroup = "part3";
                yearFinal2DigitsGroup = "part3sub2";
                break;
            case DMY:
                dayGroup = "part1";
                monthGroup = "part2";
                yearGroup = "part3";
                yearFinal2DigitsGroup = "part3sub2";
                break;
            case YMD:
                dayGroup = "part3";
                monthGroup = "part2";
                yearGroup = "part1";
                yearFinal2DigitsGroup = "part1sub2";
                break;
            default:
                throw new RuntimeException("Unsupported DateStyle: " + DEFAULT_DATE_STYLE);
        }
        final int year;
        // for 2 digit years, lean on java's standard interpretation
        if (matcher.group(yearFinal2DigitsGroup) == null) {
            year = Year.parse(matcher.group(yearGroup), TWO_DIGIT_YR_FORMAT).getValue();
        } else {
            year = Integer.parseInt(matcher.group(yearGroup));
        }
        final int month = Integer.parseInt(matcher.group(monthGroup));
        final int dayOfMonth = Integer.parseInt(matcher.group(dayGroup));
        return LocalDate.of(year, month, dayOfMonth);
    }

    /**
     * Converts a string into a local date. A local date is a date without a time or time zone.
     * <p>
     * The ideal date format is YYYY-MM-DD since it's the least ambiguous, but other formats are supported.
     * <p>
     * Supported formats: - YYYY-MM-DD - YYYYMMDD - YYYY/MM/DD - MM/DD/YYYY - MM-DD-YYYY - DD/MM/YYYY - DD-MM-YYYY -
     * YY/MM/DD - YY-MM-DD - MM/DD/YY - MM-DD-YY - DD/MM/YY - DD-MM-YY
     * <p>
     * If the format matches the ISO YYYY-MM-DD or YYYYMMDD formats, the date style is ignored.
     *
     * @param s date string.
     * @param dateStyle style the date string is formatted in.
     * @return local date.
     * @throws RuntimeException if the string cannot be parsed.
     */
    @ScriptApi
    @NotNull
    public static LocalDate parseLocalDate(@NotNull final String s, @Nullable final DateStyle dateStyle) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new RuntimeException("Cannot parse date (null): " + s);
        }

        try {
            final Matcher stdMatcher = STD_DATE_PATTERN.matcher(s);
            if (stdMatcher.matches()) {
                final int year = Integer.parseInt(stdMatcher.group("year"));
                final int month = Integer.parseInt(stdMatcher.group("month"));
                final int dayOfMonth = Integer.parseInt(stdMatcher.group("day"));
                return LocalDate.of(year, month, dayOfMonth);
            }

            final Matcher nosepMatcher = NOSEP_DATE_PATTERN.matcher(s);
            if (nosepMatcher.matches()) {
                final int year = Integer.parseInt(nosepMatcher.group("year"));
                final int month = Integer.parseInt(nosepMatcher.group("month"));
                final int dayOfMonth = Integer.parseInt(nosepMatcher.group("day"));
                return LocalDate.of(year, month, dayOfMonth);
            }

            if (dateStyle == null) {
                throw new RuntimeException("Cannot parse date (null style): " + s);
            }

            final Matcher dashMatcher = DASH_DATE_PATTERN.matcher(s);
            if (dashMatcher.matches()) {
                return matchLocalDate(dashMatcher, dateStyle);
            }

            final Matcher slashMatcher = SLASH_DATE_PATTERN.matcher(s);
            if (slashMatcher.matches()) {
                return matchLocalDate(slashMatcher, dateStyle);
            }

            throw new RuntimeException("Date does not match expected pattern");
        } catch (Exception ex) {
            throw new RuntimeException("Cannot parse date: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a local date, which is a date without a time or time zone.
     * <p>
     * The ideal date format is {@code YYYY-MM-DD} since it's the least ambiguous, but other formats are supported.
     * <p>
     * Supported formats: - {@code YYYY-MM-DD} - {@code YYYYMMDD} - {@code YYYY/MM/DD} - {@code MM/DD/YYYY} -
     * {@code MM-DD-YYYY} - {@code DD/MM/YYYY} - {@code DD-MM-YYYY} - {@code YY/MM/DD} - {@code YY-MM-DD} -
     * {@code MM/DD/YY} - {@code MM-DD-YY} - {@code DD/MM/YY} - {@code DD-MM-YY}
     *
     * If the format matches the ISO {@code YYYY-MM-DD} or {@code YYYYMMDD} formats, the date style is ignored.
     * <p>
     *
     * @param s date string.
     * @param dateStyle style the date string is formatted in.
     * @return local date, or null if the string can not be parsed.
     */
    @ScriptApi
    @Nullable
    public static LocalDate parseLocalDateQuiet(@Nullable final String s, @Nullable final DateStyle dateStyle) {
        if (s == null || s.length() <= 1 || dateStyle == null) {
            return null;
        }

        try {
            return parseLocalDate(s, dateStyle);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Parses the string argument as a local date, which is a date without a time or time zone.
     * <p>
     * The ideal date format is {@code YYYY-MM-DD} since it's the least ambiguous, but other formats are supported.
     * <p>
     * Supported formats: - {@code YYYY-MM-DD} - {@code YYYYMMDD} - {@code YYYY/MM/DD} - {@code MM/DD/YYYY} -
     * {@code MM-DD-YYYY} - {@code DD/MM/YYYY} - {@code DD-MM-YYYY} - {@code YY/MM/DD} - {@code YY-MM-DD} -
     * {@code MM/DD/YY} - {@code MM-DD-YY} - {@code DD/MM/YY} - {@code DD-MM-YY}
     *
     * If the format matches the ISO {@code YYYY-MM-DD} or {@code YYYYMMDD} formats, the date style is ignored.
     * <p>
     *
     * @param s date string.
     * @return local date parsed according to the default date style.
     * @throws RuntimeException if the string cannot be parsed.
     */
    @ScriptApi
    @NotNull
    public static LocalDate parseLocalDate(@NotNull final String s) {
        return parseLocalDate(s, DEFAULT_DATE_STYLE);
    }

    /**
     * Parses the string argument as a local date, which is a date without a time or time zone.
     * <p>
     * The ideal date format is {@code YYYY-MM-DD} since it's the least ambiguous, but other formats are supported.
     * <p>
     * Supported formats: - {@code YYYY-MM-DD} - {@code YYYYMMDD} - {@code YYYY/MM/DD} - {@code MM/DD/YYYY} -
     * {@code MM-DD-YYYY} - {@code DD/MM/YYYY} - {@code DD-MM-YYYY} - {@code YY/MM/DD} - {@code YY-MM-DD} -
     * {@code MM/DD/YY} - {@code MM-DD-YY} - {@code DD/MM/YY} - {@code DD-MM-YY}
     *
     * If the format matches the ISO {@code YYYY-MM-DD} or {@code YYYYMMDD} formats, the date style is ignored.
     * <p>
     *
     * @param s date string.
     * @return local date parsed according to the default date style, or null if the string can not be parsed.
     */
    @ScriptApi
    @Nullable
    public static LocalDate parseLocalDateQuiet(@Nullable final String s) {
        return parseLocalDateQuiet(s, DEFAULT_DATE_STYLE);
    }


    /**
     * Parses the string argument as a local time, which is the time that would be read from a clock and does not have a
     * date or timezone.
     * <p>
     * Local time strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]}.
     *
     * @param s string to be converted
     * @return a {@link LocalTime} represented by the input string.
     * @throws RuntimeException if the string cannot be converted, otherwise a {@link LocalTime} from the parsed string.
     */
    @ScriptApi
    @NotNull
    public static LocalTime parseLocalTime(@NotNull final String s) {
        // noinspection ConstantConditions
        if (s == null) {
            throw new RuntimeException("Cannot parse local time (null): " + s);
        }

        try {
            final Matcher matcher = LOCAL_TIME_PATTERN.matcher(s);
            if (matcher.matches()) {
                final int hour = Integer.parseInt(matcher.group(1)); // hour is the only required field
                final int minute = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
                final int second = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
                final int nanos;
                if (matcher.group(4) != null) {
                    final String fractionStr = matcher.group(5); // group 5 excludes the decimal pt
                    nanos = Integer.parseInt(fractionStr) * (int) Math.pow(10, 9 - fractionStr.length());
                } else {
                    nanos = 0;
                }
                return LocalTime.of(hour, minute, second, nanos);
            }

            throw new RuntimeException("Local time does not match expected pattern");
        } catch (Exception ex) {
            throw new RuntimeException("Cannot parse local time: " + s, ex);
        }
    }

    /**
     * Parses the string argument as a local time, which is the time that would be read from a clock and does not have a
     * date or timezone.
     * <p>
     * Local time strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]}.
     *
     * @param s string to be converted
     * @return a {@link LocalTime} represented by the input string, or null if the string can not be parsed.
     */
    @ScriptApi
    @Nullable
    public static LocalTime parseLocalTimeQuiet(@Nullable final String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }

        try {
            return parseLocalTime(s);
        } catch (Exception e) {
            return null;
        }
    }

    // endregion

}
