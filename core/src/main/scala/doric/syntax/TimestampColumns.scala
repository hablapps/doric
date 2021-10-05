package doric
package syntax

import cats.implicits._
import doric.types.TimestampType
import org.apache.spark.sql.catalyst.expressions.{DateFormatClass, MonthsBetween, NextDay, TruncDate}
import org.apache.spark.sql.{Column, functions => f}

import java.sql.Timestamp

private[syntax] trait TimestampColumns {

  /**
    * Returns the current timestamp at the start of query evaluation as a timestamp column.
    * All calls of current_timestamp within the same query return the same value.
    *
    * @group Timestamp Type
    */
  def currentTimestamp(): TimestampColumn = f.current_timestamp().asDoric[Timestamp]

  implicit class TimestampColumnLikeSyntax[T: TimestampType](
      column: DoricColumn[T]
  ) {

    /**
      * Adds to the date the number of months
      *
      * @group Timestamp Type
      * @param nMonths
      *   the number of months to add, can be negative to substract.
      * @return
      *   DoricColumn of the same type as the original with the month changed.
      */
    def addMonths(nMonths: IntegerColumn): DoricColumn[T] =
      (column.elem, nMonths.elem).mapN(f.add_months).toDC

    /**
      * Returns the date that is `days` days after `start`
      *
      * @param days A column of the number of days to add to `start`, can be negative to subtract days
      * @group Timestamp Type
      */
    def addDays(days: IntegerColumn): DoricColumn[T] =
      (column.elem, days.elem).mapN(f.date_add).toDC

    /**
      * Private common function for [[format]] & [[truncate]] doric functions
      *
      * @param format pattern
      */
    private def truncateFormat(format: StringColumn): Doric[Column] =
      (column.elem, format.elem)
        .mapN((c, fmt) => {
          new Column(DateFormatClass(c.expr, fmt.expr))
        })

    /**
      * Converts a date/timestamp/string to a value of string in the format specified by the date
      * format given by the second argument.
      *
      * @param format A pattern `dd.MM.yyyy` would return a string like `18.03.1993`
      * @note Use specialized functions like [[year]] whenever possible as they benefit from a
      *       specialized implementation.
      * @group Timestamp Type
      */
    def format(format: StringColumn): StringColumn = truncateFormat(format).toDC

    /**
      * Returns the date that is `days` days before `start`
      *
      * @param days A column of the number of days to subtract from `start`, can be negative to add
      *             days
      * @group Timestamp Type
      */
    def subDays(days: IntegerColumn): DoricColumn[T] =
      (column.elem, days.elem).mapN(f.date_sub).toDC

    /**
      * Returns date truncated to the unit specified by the format.
      *
      * @param format: 'year', 'yyyy', 'yy' to truncate by year,
      *               or 'month', 'mon', 'mm' to truncate by month
      *               Other options are: 'week', 'quarter'
      * @group Timestamp Type
      */
    def truncate(format: StringColumn): TimestampColumn = truncateFormat(
      format
    ).toDC

    /**
      * Returns the number of days from `start` to `end`.
      *
      * @param dateCol A date, timestamp or string. If a string, the data must be in a format that
      *                can be cast to a date, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
      * @group Timestamp Type
      */
    def diff(dateCol: DateColumn): IntegerColumn =
      (column.elem, dateCol.elem)
        .mapN((end, start) => f.datediff(end, start))
        .toDC

    /**
      * Extracts the day of the month as an integer from a given date/timestamp/string.
      *
      * @group Timestamp Type
      */
    def dayOfMonth(): IntegerColumn = column.elem.map(f.dayofmonth).toDC

    /**
      * Extracts the day of the week as an integer from a given date/timestamp/string.
      * Ranges from 1 for a Sunday through to 7 for a Saturday
      *
      * @group Timestamp Type
      */
    def dayOfWeek(): IntegerColumn = column.elem.map(f.dayofweek).toDC

    /**
      * Extracts the day of the year as an integer from a given date/timestamp/string.
      *
      * @group Timestamp Type
      */
    def dayOfYear(): IntegerColumn = column.elem.map(f.dayofyear).toDC

    /**
      * @group Timestamp Type
      * @return
      *   the hour of the time.
      */
    def hour: IntegerColumn = column.elem.map(f.hour).toDC

    /**
      * Sets the moment to the last day of the same month.
      * @group Timestamp Type
      */
    def endOfMonth: DoricColumn[T] = lastDayOfMonth()

    /**
      * Returns the last day of the month which the given date belongs to.
      * For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the
      * month in July 2015.
      *
      * @group Timestamp Type
      */
    def lastDayOfMonth(): DoricColumn[T] = column.elem.map(f.last_day).toDC

    /**
      * Extracts the minutes as an integer from a given date/timestamp/string.
      *
      * @group Timestamp Type
      */
    def minute(): DoricColumn[T] = column.elem.map(f.minute).toDC

    /**
      * Extracts the month as an integer from a given date/timestamp/string.
      *
      * @group Timestamp Type
      */
    def month(): DoricColumn[T] = column.elem.map(f.month).toDC

    /**
      * Returns number of months between dates `start` and `end`.
      *
      * A whole number is returned if both inputs have the same day of month or both are the last day
      * of their respective months. Otherwise, the difference is calculated assuming 31 days per month.
      *
      * For example:
      * {{{
      * months_between("2017-11-14", "2017-07-14")  // returns 4.0
      * months_between("2017-01-01", "2017-01-10")  // returns 0.29032258
      * months_between("2017-06-01", "2017-06-16 12:00:00")  // returns -0.5
      * }}}
      *
      * @param dateCol A date, timestamp or string. If a string, the data must be in a format that can
      *                cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
      * @group Timestamp Type
      */
    def monthsBetween(dateCol: DateColumn): DoubleColumn =
      (column.elem, dateCol.elem).mapN(f.months_between).toDC

    /**
      * Returns number of months between dates `end` and `start`.
      *
      * @param dateCol A date, timestamp or string. If a string, the data must be in a format that can
      *                cast to a timestamp, such as `yyyy-MM-dd` or `yyyy-MM-dd HH:mm:ss.SSSS`
      * @param roundOff If `roundOff` is set to true, the result is rounded off to 8 digits;
      *                 it is not rounded otherwise.
      * @group Timestamp Type
      */
    def monthsBetween(
                       dateCol: DateColumn,
                       roundOff: BooleanColumn
                     ): DoubleColumn =
      (column.elem, dateCol.elem, roundOff.elem)
        .mapN((c, d, r) => {
          new Column(new MonthsBetween(c.expr, d.expr, r.expr))
        })
        .toDC

    /**
      * Returns the first date which is later than the value of the `date` column that is on the
      * specified day of the week.
      *
      * For example, `next_day('2015-07-27', "Sunday")` returns 2015-08-02 because that is the first
      * Sunday after 2015-07-27.
      *
      * @param dayOfWeek Case insensitive, and accepts: "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"
      * @group Timestamp Type
      */
    def nextDay(dayOfWeek: StringColumn): DoricColumn[T] =
      (column.elem, dayOfWeek.elem)
        .mapN((c, dow) => {
          new Column(NextDay(c.expr, dow.expr))
        })
        .toDC

    /**
      * Extracts the quarter as an integer from a given date/timestamp/string.
      *
      * @group Timestamp Type
      */
    def quarter(): IntegerColumn = column.elem.map(f.quarter).toDC

    /**
      * Extracts the seconds as an integer from a given date/timestamp/string.
      *
      * @group Timestamp Type
      */
    def second(): IntegerColumn = column.elem.map(f.second).toDC

    /**
      * Returns date truncated to the unit specified by the format.
      *
      * For example, `trunc("2018-11-19 12:01:19", "year")` returns 2018-01-01
      *
      * @param format 'year', 'yyyy', 'yy' to truncate by year,
      *               or 'month', 'mon', 'mm' to truncate by month
      *               Other options are: 'week', 'quarter'
      * @group Timestamp Type
      */
    def trunc(format: StringColumn): DoricColumn[T] =
      (column.elem, format.elem)
        .mapN((c, fmt) => {
          new Column(TruncDate(c.expr, fmt.expr))
        })
        .toDC

    /**
      * Extracts the week number as an integer from a given date/timestamp/string.
      *
      * A week is considered to start on a Monday and week 1 is the first week with more than 3 days,
      * as defined by ISO 8601
      *
      * @group Timestamp Type
      */
    def weekOfYear(): IntegerColumn = column.elem.map(f.weekofyear).toDC

    /**
      * Extracts the year as an integer from a given date/timestamp/string.
      *
      * @group Timestamp Type
      */
    def year(): IntegerColumn = column.elem.map(f.year).toDC

    /**
      * @group Timestamp Type
      * @return
      *   a Date Column without the hour
      */
    def toDate: DateColumn = column.elem.map(f.to_date).toDC

    /**
      * @group Timestamp Type
      * @return
      *   a LocalDate Column without the hour
      */
    def toLocalDate: LocalDateColumn = column.elem.map(f.to_date).toDC
  }
}
