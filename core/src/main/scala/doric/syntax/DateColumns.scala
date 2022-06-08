package doric
package syntax

import cats.implicits._
import doric.DoricColumn.sparkFunction
import doric.types.{DateType, SparkType}

import java.sql.Date
import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.{AddMonths, DateAdd, DateFormatClass, DateSub, MonthsBetween, NextDay, TruncDate, TruncTimestamp}

private[syntax] trait DateColumns {

  /**
    * Returns the current date at the start of query evaluation as a date column.
    * All calls of current_date within the same query return the same value.
    *
    * @group Date Type
    * @see [[org.apache.spark.sql.functions.current_date]]
    */
  def currentDate(): DateColumn = f.current_date().asDoric[Date]

  /**
    * Returns the current date at the start of query evaluation as a date column typed with the provided T.
    * All calls of current_date within the same query return the same value.
    *
    * @group Date Type
    * @see [[org.apache.spark.sql.functions.current_date]]
    */
  def currentDateT[T: DateType: SparkType](): DoricColumn[T] = f.current_date().asDoric[T]

  implicit class DateColumnLikeSyntax[T: DateType: SparkType](
      column: DoricColumn[T]
  ) {

    /**
      * @group Comparable Type
      */
    def >=(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ >= _)

    /**
      * @group Comparable Type
      */
    def <=(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ <= _)

    /**
      * @group Comparable Type
      */
    def >(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ > _)

    /**
      * @group Comparable Type
      */
    def <(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ < _)

    /**
      * Adds to the Date or Timestamp column the number of months
      *
      * @group Date & Timestamp Type
      * @param nMonths
      * the number of months to add, can be negative to subtract.
      * @return
      * Date column after adding months
      * @note
      * Timestamp columns will be truncated to Date column
      */
    def addMonths(nMonths: IntegerColumn): DateColumn =
      (column.elem, nMonths.elem)
        .mapN((x, y) => new Column(AddMonths(x.expr, y.expr)))
        .toDC

    /**
      * Returns the date that is `days` days after date column
      *
      * @param days
      * A column of the number of days to add to date column, can be negative to subtract days
      * @note
      * Timestamp columns will be truncated to Date column
      * @group Date & Timestamp Type
      */
    def addDays(days: IntegerColumn): DateColumn =
      (column.elem, days.elem)
        .mapN((x, y) => new Column(DateAdd(x.expr, y.expr)))
        .toDC

    /**
      * Converts a date to a value of string in the format specified by the date
      * format given by the second argument.
      *
      * @param format
      *   A pattern `dd.MM.yyyy` would return a string like `18.03.1993`
      * @throws java.lang.IllegalArgumentException if malformed format
      * @note
      *   Use specialized functions like 'year' whenever possible as they benefit from a
      *   specialized implementation.
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.date_format]]
      */
    def format(format: StringColumn): StringColumn =
      (column.elem, format.elem)
        .mapN((c, fmt) => {
          new Column(DateFormatClass(c.expr, fmt.expr))
        })
        .toDC

    /**
      * Returns the date that is `days` days before date column
      *
      * @param days
      * A column of the number of days to subtract from date column, can be negative to add days
      * @note
      * Timestamp columns will be truncated to Date column
      * @group Date & Timestamp Type
      */
    def subDays(days: IntegerColumn): DateColumn =
      (column.elem, days.elem)
        .mapN((x, y) => new Column(DateSub(x.expr, y.expr)))
        .toDC

    /**
      * Returns the number of days from date column to `dateCol`.
      *
      * @param dateCol
      *   A Date or Timestamp column
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.datediff]]
      */
    def diff(dateCol: DoricColumn[T]): IntegerColumn =
      (column.elem, dateCol.elem)
        .mapN((end, start) => f.datediff(end, start))
        .toDC

    /**
      * Extracts the day of the month as an integer from a given date.
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.dayofmonth]]
      */
    def dayOfMonth: IntegerColumn = column.elem.map(f.dayofmonth).toDC

    /**
      * Extracts the day of the week as an integer from a given date.
      * Ranges from 1 for a Sunday through to 7 for a Saturday
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.dayofweek]]
      */
    def dayOfWeek: IntegerColumn = column.elem.map(f.dayofweek).toDC

    /**
      * Extracts the day of the year as an integer from a given date.
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.dayofyear]]
      */
    def dayOfYear: IntegerColumn = column.elem.map(f.dayofyear).toDC

    /**
      * Sets the moment to the last day of the same month.
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.last_day]]
      */
    def endOfMonth: DateColumn = lastDayOfMonth

    /**
      * Returns the last day of the month which the given date belongs to.
      * For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the
      * month in July 2015.
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.last_day]]
      */
    def lastDayOfMonth: DateColumn = column.elem.map(f.last_day).toDC

    /**
      * Extracts the month as an integer from a given date.
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.month]]
      */
    def month: IntegerColumn = column.elem.map(f.month).toDC

    /**
      * Returns number of months between dates date column and `dateCol`.
      *
      * A whole number is returned if both inputs have the same day of month or both are the last day
      * of their respective months. Otherwise, the difference is calculated assuming 31 days per month.
      *
      * For example:
      * @example {{{
      * Date("2017-11-14").monthsBetween(Date("2017-07-14"))                              // returns 4.0
      * Date("2017-01-01").monthsBetween(Date("2017-01-10"))                              // returns 0.29032258
      * Timestamp("2017-06-01 00:00:00").monthsBetween(Timestamp("2017-06-16 12:00:00"))  // returns -0.5
      * }}}
      *
      * @param dateCol
      *   Date or Timestamp column
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.months_between(end:org\.apache\.spark\.sql\.Column,start:org\.apache\.spark\.sql\.Column):* org.apache.spark.sql.functions.months_between]]
      */
    def monthsBetween(dateCol: DoricColumn[T]): DoubleColumn =
      (column.elem, dateCol.elem).mapN(f.months_between).toDC

    /**
      * Returns number of months between dates `dateCol` and date column.
      *
      * @param dateCol
      *   Date or Timestamp column
      * @param roundOff
      *   If `roundOff` is set to true, the result is rounded off to 8 digits;
      *   it is not rounded otherwise.
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.months_between(end:org\.apache\.spark\.sql\.Column,start:org\.apache\.spark\.sql\.Column,roundOff:* org.apache.spark.sql.functions.months_between]]
      */
    def monthsBetween(
        dateCol: DoricColumn[T],
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
      * @example For example, `Date("2015-07-27").nextDay("Sunday")` returns Date("2015-08-02")
      * because that is the first Sunday after 2015-07-27.
      *
      * @param dayOfWeek
      *   Case insensitive, and accepts: "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"
      * @note
      *   Timestamp columns will be truncated to Date column
      * @group Date & Timestamp Type
      * @see org.apache.spark.sql.functions.next_day
      * @todo scaladoc link (issue #135)
      */
    def nextDay(dayOfWeek: StringColumn): DateColumn =
      (column.elem, dayOfWeek.elem)
        .mapN((c, dow) => {
          new Column(NextDay(c.expr, dow.expr))
        })
        .toDC

    /**
      * Extracts the quarter as an integer from a given date.
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.quarter]]
      */
    def quarter: IntegerColumn = column.elem.map(f.quarter).toDC

    /**
      * Returns date truncated to the unit specified by the format.
      *
      * @example For example, `Timestamp("2018-11-19 12:01:19").trunc("year")` returns Date("2018-01-01")
      *
      * @param format
      *   - if <b>date</b>:
      *     - 'year', 'yyyy', 'yy' to truncate by year,
      *     - 'month', 'mon', 'mm' to truncate by month
      *     - __Other options are__: 'week', 'quarter'
      *   - if <b>timestamp</b>:
      *     - 'year', 'yyyy', 'yy' to truncate by year,
      *     - 'month', 'mon', 'mm' to truncate by month,
      *     - 'day', 'dd' to truncate by day,
      *     - __Other options are__:  'microsecond', 'millisecond', 'second', 'minute', 'hour', 'week', 'quarter'
      * @note
      *   Timestamp columns will be truncated to Date column
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.trunc]]
      * @see [[org.apache.spark.sql.functions.date_trunc]]
      */
    def truncate(format: StringColumn): DoricColumn[T] =
      (column.elem, format.elem)
        .mapN((c, fmt) => {
          new Column(SparkType[T].dataType match {
            case org.apache.spark.sql.types.DateType =>
              TruncDate(c.expr, fmt.expr)
            case org.apache.spark.sql.types.TimestampType =>
              TruncTimestamp(fmt.expr, c.expr)
          })
        })
        .toDC

    /**
      * Converts date/timestamp to Unix timestamp (in seconds),
      * using the default timezone and the default locale.
      *
      * @return
      *   A long
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.unix_timestamp(s:org\.apache\.spark\.sql\.Column):* org.apache.spark.sql.functions.unix_timestamp]]
      */
    def unixTimestamp: LongColumn = column.elem.map(f.unix_timestamp).toDC

    /**
      * Extracts the week number as an integer from a given date.
      *
      * A week is considered to start on a Monday and week 1 is the first week with more than 3 days,
      * as defined by ISO 8601
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.weekofyear]]
      */
    def weekOfYear: IntegerColumn = column.elem.map(f.weekofyear).toDC

    /**
      * Extracts the year as an integer from a given date.
      *
      * @group Date & Timestamp Type
      * @see [[org.apache.spark.sql.functions.year]]
      */
    def year: IntegerColumn = column.elem.map(f.year).toDC

    /**
      * Transform date to timestamp
      *
      * @group Date Type
      * @see [[org.apache.spark.sql.functions.to_timestamp(s:org\.apache\.spark\.sql\.Column):* org.apache.spark.sql.functions.to_timestamp]]
      */
    def toTimestamp: TimestampColumn = column.elem.map(f.to_timestamp).toDC

    /**
      * Transform date to Instant
      *
      * @group Date Type
      * @see [[org.apache.spark.sql.functions.to_timestamp(s:org\.apache\.spark\.sql\.Column):* org.apache.spark.sql.functions.to_timestamp]]
      */
    def toInstant: InstantColumn = column.elem.map(f.to_timestamp).toDC
  }
}
