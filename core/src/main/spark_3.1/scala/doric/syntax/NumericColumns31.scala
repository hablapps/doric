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
  }

}
