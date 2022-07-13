package doric
package syntax

import cats.implicits._
import doric.DoricColumn.sparkFunction
import doric.types.{CollectionType, NumericType}
import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions.{BRound, FormatNumber, FromUnixTime, Rand, Randn, Round, ShiftLeft, ShiftRight, ShiftRightUnsigned}

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
      * @see [[org.apache.spark.sql.functions.acos(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.acos]]
      */
    def acos: DoubleColumn = column.elem.map(f.acos).toDC

    /**
      * Inverse sine of the column in radians, as if computed by java.lang.Math.asin
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.asin(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.asin]]
      */
    def asin: DoubleColumn = column.elem.map(f.asin).toDC

    /**
      * Inverse tangent of the column as if computed by java.lang.Math.atan
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.atan(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.atan]]
      */
    def atan: DoubleColumn = column.elem.map(f.atan).toDC

    /**
      * The theta component of the point (r, theta) in polar coordinates that corresponds to the point
      * (x, y) in Cartesian coordinates, as if computed by java.lang.Math.atan2
      *
      * The column corresponds to yCoordinates
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.atan2(y:org\.apache\.spark\.sql\.Column,x:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.atan2]]
      */
    def atan2(xCoordinates: DoricColumn[T]): DoubleColumn =
      (column.elem, xCoordinates.elem).mapN(f.atan2).toDC

    /**
      * An expression that returns the string representation of the binary value of the given long column.
      * For example, bin("12") returns "1100".
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.bin(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.bin]]
      */
    def bin: StringColumn = column.elem.map(f.bin).toDC

    /**
      * Computes the cube-root of the given value
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.cbrt(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.cbrt]]
      */
    def cbrt: DoubleColumn = column.elem.map(f.cbrt).toDC

    /**
      * Cosine of the angle, as if computed by java.lang.Math.cos
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.cos(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.cos]]
      */
    def cos: DoubleColumn = column.elem.map(f.cos).toDC

    /**
      * Hyperbolic cosine of the angle, as if computed by java.lang.Math.cosh
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.cosh(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.cosh]]
      */
    def cosh: DoubleColumn = column.elem.map(f.cosh).toDC

    /**
      * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.degrees(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.degrees]]
      */
    def degrees: DoubleColumn = column.elem.map(f.degrees).toDC

    /**
      * Computes the exponential of the given column
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.exp(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.exp]]
      */
    def exp: DoubleColumn = column.elem.map(f.exp).toDC

    /**
      * Computes the exponential of the given value minus one.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.expm1(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.expm1]]
      */
    def expm1: DoubleColumn = column.elem.map(f.expm1).toDC

    /**
      * Computes the factorial of the given value.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.factorial]]
      */
    def factorial: LongColumn = column.elem.map(f.factorial).toDC

    /**
      * Computes sqrt(a^2^ + b^2^) without intermediate overflow or underflow.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.hypot(l:org\.apache\.spark\.sql\.Column,r:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.hypot]]
      */
    def hypot(right: DoricColumn[T]): DoubleColumn =
      (column.elem, right.elem).mapN(f.hypot).toDC

    /**
      * Computes the natural logarithm of the given value.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.log(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.log]]
      */
    def log: DoubleColumn = column.elem.map(f.log).toDC

    /**
      * Computes the logarithm of the given value in base 10.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.log10(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.log10]]
      */
    def log10: DoubleColumn = column.elem.map(f.log10).toDC

    /**
      * Computes the natural logarithm of the given value plus one.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.log1p(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.log1p]]
      */
    def log1p: DoubleColumn = column.elem.map(f.log1p).toDC

    /**
      * Computes the logarithm of the given value in base 2.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.log2(expr:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.log2]]
      */
    def log2: DoubleColumn = column.elem.map(f.log2).toDC

    /**
      * Returns the value of the first argument raised to the power of the second argument.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.pow(l:org\.apache\.spark\.sql\.Column,r:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.pow]]
      */
    def pow(right: DoricColumn[T]): DoubleColumn =
      (column.elem, right.elem).mapN(f.pow).toDC

    /**
      * Returns the positive value of dividend mod divisor.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.pmod]]
      */
    def pMod(divisor: DoricColumn[T]): DoricColumn[T] =
      (column.elem, divisor.elem).mapN(f.pmod).toDC

    /**
      * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.radians(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.radians]]
      */
    def radians: DoubleColumn = column.elem.map(f.radians).toDC

    /**
      * Returns the double value that is closest in value to the argument and is equal to a mathematical integer.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.rint(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.rint]]
      */
    def rint: DoubleColumn = column.elem.map(f.rint).toDC

    /**
      * Computes the signum of the given value.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.signum(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.signum]]
      */
    def signum: DoubleColumn = column.elem.map(f.signum).toDC

    /**
      * Sine of the angle, as if computed by java.lang.Math.sin
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sin(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.sin]]
      */
    def sin: DoubleColumn = column.elem.map(f.sin).toDC

    /**
      * Hyperbolic sine of the given value, as if computed by java.lang.Math.sinh
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sinh(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.sinh]]
      */
    def sinh: DoubleColumn = column.elem.map(f.sinh).toDC

    /**
      * Computes the square root of the specified float value
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sqrt(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.sqrt]]
      */
    def sqrt: DoubleColumn = column.elem.map(f.sqrt).toDC

    /**
      * Tangent of the given value, as if computed by java.lang.Math.tan
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.tan(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.tan]]
      */
    def tan: DoubleColumn = column.elem.map(f.tan).toDC

    /**
      * Hyperbolic tangent of the given value, as if computed by java.lang.Math.tanh
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.tanh(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.tanh]]
      */
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
  }

  /**
    * INTEGRAL OPERATIONS
    */
  implicit class IntegralOperationsSyntax[T: IntegralType](
      column: DoricColumn[T]
  ) {

    /**
      * Generate a sequence of integers from start to stop, incrementing by 1
      * if start is less than or equal to stop, otherwise -1.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sequence(start:org\.apache\.spark\.sql\.Column,stop:org\.apache\.spark\.sql\.Column,step* org.apache.spark.sql.functions.sequence]]
      */
    def sequence(stop: IntegerColumn): ArrayColumn[T] = sequenceT(stop)

    /**
      * Generate a sequence of integers from start to stop, incrementing by step.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sequence(start:org\.apache\.spark\.sql\.Column,stop:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.sequence]]
      */
    def sequence(stop: IntegerColumn, step: IntegerColumn): ArrayColumn[T] =
      sequenceT(stop, step)

    /**
      * Generate a sequence of integers from start to stop, incrementing by 1
      * if start is less than or equal to stop, otherwise -1.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sequence(start:org\.apache\.spark\.sql\.Column,stop:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.sequence]]
      */
    def sequenceT[G[_]: CollectionType](
        stop: IntegerColumn
    ): DoricColumn[G[T]] =
      (column.elem, stop.elem).mapN(f.sequence).toDC

    /**
      * Generate a sequence of integers from start to stop, incrementing by step.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.sequence(start:org\.apache\.spark\.sql\.Column,stop:org\.apache\.spark\.sql\.Column,step* org.apache.spark.sql.functions.sequence]]
      */
    def sequenceT[G[_]: CollectionType](
        stop: IntegerColumn,
        step: IntegerColumn
    ): DoricColumn[G[T]] =
      (column.elem, stop.elem, step.elem).mapN(f.sequence).toDC

    /**
      * Computes hex value of the given column
      *
      * group Numeric Type
      * @see [[org.apache.spark.sql.functions.hex]]
      */
    def hex: StringColumn = column.elem.map(f.hex).toDC
  }

  /**
    * NUM WITH DECIMALS OPERATIONS
    */
  implicit class NumWithDecimalsOperationsSyntax[T: NumWithDecimalsType](
      column: DoricColumn[T]
  ) {

    /**
      * Returns the value of the column rounded to 0 decimal places with HALF_EVEN round mode
      *
      * @todo decimal type
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.bround(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.bround]]
      */
    def bRound: DoricColumn[T] = column.elem.map(f.bround).toDC

    /**
      * Round the value of e to scale decimal places with HALF_EVEN round mode if scale is greater than
      * or equal to 0 or at integral part when scale is less than 0.
      *
      * @todo decimal type
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.bround(e:org\.apache\.spark\.sql\.Column,scale:* org.apache.spark.sql.functions.bround]]
      */
    def bRound(scale: IntegerColumn): DoricColumn[T] =
      (column.elem, scale.elem)
        .mapN((c, s) => new Column(BRound(c.expr, s.expr)))
        .toDC

    /**
      * Computes the ceiling of the given value.
      *
      * @todo decimal type
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.ceil(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.ceil]]
      */
    def ceil: LongColumn = column.elem.map(f.ceil).toDC

    /**
      * Computes the floor of the given value
      *
      * @todo decimal type
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.floor(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.floor]]
      */
    def floor: LongColumn = column.elem.map(f.floor).toDC

    /**
      * Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode
      *
      * @todo decimal type
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.round(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.round]]
      */
    def round: DoricColumn[T] = column.elem.map(f.round).toDC

    /**
      * Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode.
      *
      * @todo decimal type
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.round(e:org\.apache\.spark\.sql\.Column,scale:* org.apache.spark.sql.functions.round]]
      */
    def round(scale: IntegerColumn): DoricColumn[T] = (column.elem, scale.elem)
      .mapN((c, s) => new Column(Round(c.expr, s.expr)))
      .toDC
  }

}
