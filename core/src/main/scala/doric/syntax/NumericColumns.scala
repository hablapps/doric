package doric
package syntax

import cats.implicits._
import doric.DoricColumn.sparkFunction
import doric.types.NumericType
import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.{BRound, FormatNumber, FromUnixTime, Rand, Randn, Round, ShiftLeft, ShiftRight}

private[syntax] trait NumericColumns {

  /**
    * Returns the current Unix timestamp (in seconds) as a long.
    *
    * @note All calls of `unix_timestamp` within the same query return the same value
    *       (i.e. the current timestamp is calculated at the start of query evaluation).
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

  /**
    * GENERIC NUMERIC OPERATIONS
    */
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
      * Checks if the value of the column is not a number
      * @group All Types
      * @return
      *   Boolean DoricColumn
      */
    def isNaN: BooleanColumn = column.elem.map(_.isNaN).toDC

    /**
      * Computes the absolute value of a numeric value.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.abs]]
      */
    def abs: DoricColumn[T] = column.elem.map(f.abs).toDC

    /**
      * Inverse cosine of `column` in radians, as if computed by `java.lang.Math.acos`
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.abs]]
      */
    def acos: DoubleColumn = column.elem.map(f.acos).toDC

    def acosh: DoubleColumn = column.elem.map(f.acosh).toDC

    def asin: DoubleColumn = column.elem.map(f.asin).toDC

    def asinh: DoubleColumn = column.elem.map(f.asinh).toDC

    def atan: DoubleColumn = column.elem.map(f.atan).toDC

    def atan2(yCoordinates: DoricColumn[T]): DoubleColumn =
      (yCoordinates.elem, column.elem).mapN(f.atan2).toDC

    def bin: DoubleColumn = column.elem.map(f.bin).toDC

    def cbrt: DoubleColumn = column.elem.map(f.cbrt).toDC

    def cos: DoubleColumn = column.elem.map(f.cos).toDC

    def cosh: DoubleColumn = column.elem.map(f.cosh).toDC

    def degrees: DoubleColumn = column.elem.map(f.degrees).toDC

    def exp: DoubleColumn = column.elem.map(f.exp).toDC

    def expm1: DoubleColumn = column.elem.map(f.expm1).toDC

    def factorial: LongColumn = column.elem.map(f.factorial).toDC

    def hypot(right: DoricColumn[T]): DoubleColumn =
      (column.elem, right.elem).mapN(f.hypot).toDC

    def log: DoubleColumn = column.elem.map(f.log).toDC

    def log10: DoubleColumn = column.elem.map(f.log10).toDC

    def log1p: DoubleColumn = column.elem.map(f.log1p).toDC

    def log2: DoubleColumn = column.elem.map(f.log2).toDC

    def log2(right: DoricColumn[T]): DoubleColumn =
      (column.elem, right.elem).mapN(f.pow).toDC

    def pMod(divisor: DoricColumn[T]): DoricColumn[T] =
      (column.elem, divisor.elem).mapN(f.pmod).toDC

    def radians: DoubleColumn = column.elem.map(f.radians).toDC

    def rint: DoubleColumn = column.elem.map(f.rint).toDC

    def signum: DoubleColumn = column.elem.map(f.signum).toDC

    def sin: DoubleColumn = column.elem.map(f.sin).toDC

    def sinh: DoubleColumn = column.elem.map(f.sinh).toDC

    def sqrt: DoubleColumn = column.elem.map(f.sqrt).toDC

    def tan: DoubleColumn = column.elem.map(f.tan).toDC

    def tanh: DoubleColumn = column.elem.map(f.tanh).toDC
  }

  /**
    * LONG OPERATIONS
    */
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
      * @throws java.lang.IllegalArgumentException if invalid format
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

    // LongType, BinaryType, StringType
    def hex: StringColumn = column.elem.map(f.hex).toDC

    // IN => Int/Long => Returns the same
    def shiftLeft(numBits: IntegerColumn): DoubleColumn = (column.elem, numBits.elem)
      .mapN((c, n) => new Column(ShiftLeft(c.expr, n.expr)))
      .toDC

    // IN => Int/Long => Returns the same
    def shiftRight(numBits: IntegerColumn): DoubleColumn = (column.elem, numBits.elem)
      .mapN((c, n) => new Column(ShiftRight(c.expr, n.expr)))
      .toDC
  }

  /**
    * INTEGER OPERATIONS
    */
  implicit class IntegerOperationsSyntax(
      column: IntegerColumn
  ) {

    /**
      * Generate a sequence of integers from start to stop, incrementing by step.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sequence(start:org\.apache\.spark\.sql\.Column,stop:org\.apache\.spark\.sql\.Column,step* org.apache.spark.sql.functions.sequence]]
      */
    def sequence(to: IntegerColumn, step: IntegerColumn): ArrayColumn[Int] =
      (column.elem, to.elem, step.elem).mapN(f.sequence).toDC

    /**
      * Generate a sequence of integers from start to stop, incrementing by 1
      * if start is less than or equal to stop, otherwise -1.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sequence(start:org\.apache\.spark\.sql\.Column,stop:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.sequence]]
      */
    def sequence(to: IntegerColumn): ArrayColumn[Int] =
      (column.elem, to.elem).mapN(f.sequence).toDC
  }

  /**
    * DOUBLE OPERATIONS
    */
  implicit class DoubleOperationsSyntax(
      column: DoubleColumn
  ) {

    def atanh: DoubleColumn = column.elem.map(f.atanh).toDC

    def bRound: DoubleColumn = column.elem.map(f.bround).toDC

    def bRound(scale: IntegerColumn): DoubleColumn =
      (column.elem, scale.elem)
        .mapN((c, s) => new Column(BRound(c.expr, s.expr)))
        .toDC

    // DoubleType, DecimalType // TODO RETURN DOUBLE OR LONG
    def ceil: DoubleColumn = column.elem.map(f.ceil).toDC

    // DoubleType, DecimalType // TODO RETURN DOUBLE OR LONG
    def floor: DoubleColumn = column.elem.map(f.floor).toDC

    // returns decimal type or T??
    def round: DoubleColumn = column.elem.map(f.round).toDC

    def round(scale: IntegerColumn): DoubleColumn = (column.elem, scale.elem)
      .mapN((c, s) => new Column(Round(c.expr, s.expr)))
      .toDC
  }

}
