package doric
package syntax

import cats.implicits.catsSyntaxTuple2Semigroupal
import doric.DoricColumn.sparkFunction
import doric.types.{NumericDecimalsType, NumericType}
import org.apache.spark.sql.Column
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.catalyst.expressions.{FormatNumber, FromUnixTime, Rand, Randn}

private[syntax] trait NumericColumns {

  /**
    * Returns the current Unix timestamp (in seconds) as a long.
    *
    * @note All calls of `unix_timestamp` within the same query return the same value
    * (i.e. the current timestamp is calculated at the start of query evaluation).
    *
    * @group Numeric Type
    * @see [[org.apache.spark.sql.functions.unix_timestamp()* org.apache.spark.sql.functions.unix_timestamp]]
    */
  def unixTimestamp(): LongColumn = {
    DoricColumn(f.unix_timestamp())
  }

  /**
    * Generate a random column with independent and identically distributed (i.i.d.) samples
    * uniformly distributed in [0.0, 1.0).
    *
    * @note The function is non-deterministic in general case.
    * @group Numeric Type
    * @see [[org.apache.spark.sql.functions.rand()* org.apache.spark.sql.functions.rand]]
    */
  def random(): DoubleColumn = DoricColumn(f.rand())

  /**
    * Generate a random column with independent and identically distributed (i.i.d.) samples
    * uniformly distributed in [0.0, 1.0).
    *
    * @note The function is non-deterministic in general case.
    * @group Numeric Type
    * @see [[org.apache.spark.sql.functions.rand(seed* org.apache.spark.sql.functions.rand]]
    */
  def random(seed: LongColumn): DoubleColumn =
    seed.elem.map(s => new Column(Rand(s.expr))).toDC

  /**
    * Generate a column with independent and identically distributed (i.i.d.) samples from
    * the standard normal distribution.
    *
    * @note The function is non-deterministic in general case.
    * @group Numeric Type
    * @see [[org.apache.spark.sql.functions.randn()* org.apache.spark.sql.functions.randn]]
    */
  def randomN(): DoubleColumn = DoricColumn(f.randn())

  /**
    * Generate a column with independent and identically distributed (i.i.d.) samples from
    * the standard normal distribution.
    *
    * @note The function is non-deterministic in general case.
    * @group Numeric Type
    * @see [[org.apache.spark.sql.functions.randn(seed* org.apache.spark.sql.functions.randn]]
    */
  def randomN(seed: LongColumn): DoubleColumn =
    seed.elem.map(s => new Column(Randn(s.expr))).toDC

  /**
    * Partition ID.
    *
    * @note This is non-deterministic because it depends on data partitioning and task scheduling.
    * @group Numeric Type
    * @see [[org.apache.spark.sql.functions.spark_partition_id]]
    */
  def sparkPartitionId(): IntegerColumn = DoricColumn(f.spark_partition_id)

  /**
    * A column expression that generates monotonically increasing 64-bit integers.
    *
    * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    * The current implementation puts the partition ID in the upper 31 bits, and the record number
    * within each partition in the lower 33 bits. The assumption is that the data frame has
    * less than 1 billion partitions, and each partition has less than 8 billion records.
    *
    * @example consider a `DataFrame` with two partitions, each with 3 records.
    * This expression would return the following IDs:
    *
    * {{{
    * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
    * }}}
    *
    * @group Numeric Type
    * @see [[org.apache.spark.sql.functions.monotonically_increasing_id]]
    */
  def monotonicallyIncreasingId(): LongColumn =
    DoricColumn(f.monotonically_increasing_id())

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
      * @see [[org.apache.spark.sql.functions.format_number]]
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
      * @see [[org.apache.spark.sql.functions.timestamp_seconds]]
      */
    def timestampSeconds: TimestampColumn =
      column.elem.map(f.timestamp_seconds).toDC

  }

  implicit class NumericDecimalsOpsSyntax[T: NumericDecimalsType](
      column: DoricColumn[T]
  ) {

    /**
      * Checks if the value of the column is not a number
      * @group All Types
      * @return
      *   Boolean DoricColumn
      */
    def isNaN: BooleanColumn = column.elem.map(_.isNaN).toDC
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
      * @see [[org.apache.spark.sql.functions.from_unixtime(ut:org\.apache\.spark\.sql\.Column):* org.apache.spark.sql.functions.from_unixtime]]
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
      * @see [[org.apache.spark.sql.functions.from_unixtime(ut:org\.apache\.spark\.sql\.Column,f* org.apache.spark.sql.functions.from_unixtime]]
      */
    def fromUnixTime(format: StringColumn): StringColumn =
      (column.elem, format.elem)
        .mapN((c, f) => {
          new Column(FromUnixTime(c.expr, f.expr))
        })
        .toDC

  }

}
