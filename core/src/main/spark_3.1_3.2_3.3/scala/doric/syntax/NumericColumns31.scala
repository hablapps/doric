package doric
package syntax

import doric.types.NumericType
import org.apache.spark.sql.{functions => f}

private[syntax] trait NumericColumns31 {
  implicit class NumericOperationsSyntax31[T: NumericType](
      column: DoricColumn[T]
  ) {

    /**
      * Creates timestamp from the number of seconds since UTC epoch.
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.timestamp_seconds]]
      */
    def timestampSeconds: TimestampColumn =
      column.elem.map(f.timestamp_seconds).toDC

    /**
      * Returns inverse hyperbolic cosine of the column
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.acosh(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.acosh]]
      */
    def acosh: DoubleColumn = column.elem.map(f.acosh).toDC

    /**
      * Inverse hyperbolic sine of the column
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.asinh(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.asinh]]
      */
    def asinh: DoubleColumn = column.elem.map(f.asinh).toDC
  }

  /**
    * NUM WITH DECIMALS OPERATIONS
    */
  implicit class NumWithDecimalsOperationsSyntax31[T: NumWithDecimalsType](
      column: DoricColumn[T]
  ) {

    /**
      * Returns inverse hyperbolic tangent of the column
      *
      * @group Numeric Type
      * @see [[org.apache.spark.sql.functions.atanh(e:org\.apache\.spark\.sql\.Column)* org.apache.spark.sql.functions.atanh]]
      */
    def atanh: DoubleColumn = column.elem.map(f.atanh).toDC
  }

}
