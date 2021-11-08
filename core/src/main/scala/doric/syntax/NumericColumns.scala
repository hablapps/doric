package doric
package syntax

import cats.implicits.catsSyntaxTuple2Semigroupal
import doric.DoricColumn.sparkFunction
import doric.types.NumericType
import org.apache.spark.sql.Column
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.catalyst.expressions.{FormatNumber, FromUnixTime}

private[syntax] trait NumericColumns {

  def unixTimestamp(): LongColumn = {
    DoricColumn(f.unix_timestamp())
  }

  implicit class NumericOperationsSyntax[T: NumericType](
      column: DoricColumn[T]
  ) {

    /**
      * @group Numeric Type
      */
    def +(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ + _)

    /**
      * @group Numeric Type
      */
    def -(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ - _)

    /**
      * @group Numeric Type
      */
    def *(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ * _)

    /**
      * @group Numeric Type
      */
    def /(other: DoricColumn[T]): DoricColumn[Double] =
      sparkFunction(column, other, _ / _)

    /**
      * @group Numeric Type
      */
    def %(other: DoricColumn[T]): DoricColumn[T] =
      sparkFunction(column, other, _ % _)

    /**
      * @group Comparable Type
      */
    def >(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ > _)

    /**
      * @group Comparable Type
      */
    def >=(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ >= _)

    /**
      * @group Comparable Type
      */
    def <(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ < _)

    /**
      * @group Comparable Type
      */
    def <=(other: DoricColumn[T]): BooleanColumn =
      sparkFunction[T, Boolean](column, other, _ <= _)

    /**
      * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places
      * with HALF_EVEN round mode, and returns the result as a string column.
      *
      * If d is 0, the result has no decimal point or fractional part.
      * If d is less than 0, the result will be null.
      *
      * @group Numeric Type
      */
    def formatNumber(decimals: IntegerColumn): StringColumn =
      (column.elem, decimals.elem)
        .mapN((c, d) => {
          new Column(FormatNumber(c.expr, d.expr))
        })
        .toDC

    /**
      * Creates timestamp from the number of seconds since UTC epoch.
      *
      * @group Numeric Type
      */
    def timestampSeconds: TimestampColumn =
      column.elem.map(f.timestamp_seconds).toDC

  }

  implicit class LongOperationsSyntax(
      column: LongColumn
  ) {

    /**
      * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
      * representing the timestamp of that moment in the current system time zone in the
      * yyyy-MM-dd HH:mm:ss format.
      *
      * @group Numeric Type
      */
    def fromUnixTime: StringColumn = column.elem.map(f.from_unixtime).toDC

    /**
      * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
      * representing the timestamp of that moment in the current system time zone in the given
      * format.
      *
      * @note
      *   An IllegalArgumentException will be thrown if invalid pattern
      *
      * @group Numeric Type
      */
    def fromUnixTime(format: StringColumn): StringColumn =
      (column.elem, format.elem)
        .mapN((c, f) => {
          new Column(FromUnixTime(c.expr, f.expr))
        })
        .toDC

  }

}
