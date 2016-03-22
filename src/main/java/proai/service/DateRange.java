package proai.service;

import org.joda.time.DateTime;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A DateRange defines a intervall between to dates. The intervall constraints
 * are exclusive. The class provides a set of functions to create DateRanges.
 * Each function expectes a iso8601 String: "yyyy-MM-dd'T'HH:mm:ss'.'SSS'Z'".
 * The provided strings can vary in granularity. e.g. the following ist
 * possible: From 2013 -- Until 2013-09 or From 2013-09 -- Until 2014.
 *
 * @author Jan Schnasse schnasse@hbz-nrw.de
 */
public class DateRange {

    String from = null;
    String until = null;

    private DateRange(String fromDate, String untilDate) {

        from = fromDate;
        until = untilDate;
    }

    /**
     * Creates a range where the given constraints are inclusive
     *
     * @param fromDate  a date in iso8601 format
     * @param untilDate a date in iso8601 format
     * @return a DateRange
     */
    public static DateRange getRangeInclIncl(String fromDate, String untilDate)

    {
        if (fromDate == null || fromDate.isEmpty()) {
            fromDate = null;
        }
        if (untilDate == null || untilDate.isEmpty()) {
            untilDate = null;
        }
        if (!validate(fromDate))
            throw new DateRangeParseException("\"" + fromDate
                    + "\" is not a valid ISO8601 string.");
        if (!validate(untilDate))
            throw new DateRangeParseException(untilDate
                    + " is not a valid ISO8601 string.");
        if (fromIsAfterUntil(fromDate, untilDate))
            throw new DateRangeParseException(untilDate
                    + " (until) is before " + fromDate + "(from). Must result in empty response!");

        fromDate = decrement(fromDate);
        untilDate = increment(untilDate);
        fromDate = unify(fromDate, untilDate);
        untilDate = unify(untilDate, fromDate);
        return new DateRange(fromDate, untilDate);
    }

    /**
     * @param date converts a Date object to a iso8601 string
     * @return a iso8601 string
     */
    private static String dateToIso8601(Date date) {
        if (date == null)
            return null;
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.'SSS'Z'")
                .format(date);
    }

    /**
     * @param date a is08601 date
     * @return true if this class can handle the string, false if not.
     */
    private static boolean validate(String date) {
        if (date == null) return true;
        String pattern = "^(-?(?:[1-9][0-9]*)?[0-9]{4})(-(1[0-2]|0[1-9]))?(-(3[0-1]|0[1-9]|[1-2][0-9]))?(T(2[0-3]|[0-1][0-9])(:([0-5][0-9])(:([0-5][0-9]))?((\\.[0-9]+))?)?Z)?";
        return date.matches(pattern);
    }

    private static boolean fromIsAfterUntil(String fromDate, String untilDate) {
        if (untilDate == null) return false;
        if (fromDate == null) return false;

        Date from = iso8601ToDate(fromDate);
        Date until = iso8601ToDate(untilDate);

        return from.after(until);
    }

    /**
     * Decrements a date on the provided granularity.
     *
     * @param date a date object as a iso8601 String
     * @return a iso8601 String decremented by 1 unit.
     */
    private static String decrement(final String date) {
        if (date == null) return null;
        try {
            DateTime decDate = new DateTime(iso8601ToDate(date));
            decDate = decDate.minusMillis(1);
            return dateToIso8601(decDate.toDate());
        } catch (Exception e) {
            throw new DateRangeParseException(e);
        }
    }

    /**
     * Increments a date on the provided granularity.
     *
     * @param date a date object as a iso8601 String
     * @return a iso8601 String incremented by 1 unit.
     */
    private static String increment(final String date) {

        if (date == null) return null;
        try {
            int len = date.length();
            DateTime incDate = new DateTime(iso8601ToDate(date));

            if (len == 4) {
                incDate = incDate.plusYears(1);
            } else if (len == 7) {
                incDate = incDate.plusMonths(1);
            } else if (len == 10) {
                incDate = incDate.plusDays(1);
            } else if (len == 14) {
                incDate = incDate.plusHours(1);
            } else if (len == 17) {
                incDate = incDate.plusMinutes(1);
            } else if (len == 20) {
                incDate = incDate.plusSeconds(1);
            } else if (len == 24) {
                incDate = incDate.plusMillis(1);
            }
            return dateToIso8601(incDate.toDate());

        } catch (Exception e) {
            throw new DateRangeParseException(e);
        }
    }

    /**
     * @param unifyMe      a iso8601 string which should fit to the same granularity as
     *                     the likeExpected string
     * @param likeExpected a is08601 string with the intended granularity
     * @return a version of the passed unifyMe String that is on the same
     * granularity as the likeExpected string.
     */
    private static String unify(final String unifyMe, final String likeExpected) {

        if (unifyMe == null) return null;
        if (likeExpected == null) return unifyMe;

        String str = unifyMe;
        if (str.length() >= likeExpected.length())
            return str;

        if (likeExpected.length() >= 7 && str.length() < 7) {
            str = str + "-01";
        }

        if (likeExpected.length() >= 10 && str.length() < 10) {
            str = str + "-01";
        }

        if (likeExpected.length() >= 14 && str.length() < 14) {
            str = str + "T00Z";
        }
        if (likeExpected.length() >= 17 && str.length() < 17) {
            str = str.substring(0, str.length() - 1) + ":00Z";
        }
        if (likeExpected.length() >= 20 && str.length() < 20) {
            str = str.substring(0, str.length() - 1) + ":00Z";
        }
        if (likeExpected.length() >= 24 && str.length() < 24) {
            str = str.substring(0, str.length() - 1) + ".000Z";
        }

        return str;
    }

    /**
     * @param str converts a string object to a a iso8601 string
     * @return a Date object
     */
    public static Date iso8601ToDate(String str) {
        if (str == null)
            return null;
        try {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'.'SSS'Z'")
                    .parse(str);
        } catch (ParseException e0) {
            try {
                return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
                        .parse(str);
            } catch (ParseException e) {
                try {
                    return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
                            .parse(str);
                } catch (ParseException e1) {
                    try {
                        return new SimpleDateFormat("yyyy-MM-dd'T'HH'Z'")
                                .parse(str);
                    } catch (ParseException e2) {
                        try {
                            return new SimpleDateFormat("yyyy-MM-dd")
                                    .parse(str);
                        } catch (ParseException e3) {
                            try {
                                return new SimpleDateFormat("yyyy-MM")
                                        .parse(str);
                            } catch (ParseException e4) {

                                try {
                                    return new SimpleDateFormat("yyyy")
                                            .parse(str);
                                } catch (ParseException e5) {
                                    throw new DateRangeParseException(e5);
                                }

                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Creates a range where the given from constraint is exclusive and the
     * given Until constraint is inclusive
     *
     * @param fromDate  a date in iso8601 format
     * @param untilDate a date in iso8601 format
     * @return a DateRange
     */
    public static DateRange getRangeExclIncl(Date fromDate, Date untilDate) {
        return getRangeExclIncl(dateToIso8601(fromDate),
                dateToIso8601(untilDate));
    }

    /**
     * Creates a range where the given from constraint is exclusive and the
     * given Until constraint is inclusive
     *
     * @param fromDate  a date in iso8601 format
     * @param untilDate a date in iso8601 format
     * @return a DateRange
     */
    public static DateRange getRangeExclIncl(String fromDate, String untilDate) {
        if (fromDate == null || fromDate.isEmpty()) {
            fromDate = null;
        }
        if (untilDate == null || untilDate.isEmpty()) {
            untilDate = null;
        }
        if (!validate(fromDate))
            throw new DateRangeParseException(fromDate
                    + " is not a valid ISO8601 string.");
        if (!validate(untilDate))
            throw new DateRangeParseException(untilDate
                    + " is not a valid ISO8601 string.");
        if (fromIsAfterUntil(fromDate, untilDate))
            throw new DateRangeParseException(untilDate
                    + " (until) is before " + fromDate + "(from). Must result in empty response!");

        untilDate = increment(untilDate);
        fromDate = unify(fromDate, untilDate);
        untilDate = unify(untilDate, fromDate);
        return new DateRange(fromDate, untilDate);
    }

    /**
     * @return a iso8601 String
     */
    public String getFrom() {
        return from;
    }

    /**
     * @param from a iso8601 String
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * @return a iso8601 String
     */
    public String getUntil() {
        return until;
    }

    public String toString() {
        return from + " - " + until;
    }

}
